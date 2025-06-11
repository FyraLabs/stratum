//! Mount helpers for managing mountpoints
pub mod composefs;
use nix::mount::{MntFlags, umount2};
use rustix::{
    fs::CWD,
    mount::{
        FsMountFlags, FsOpenFlags, MountAttrFlags, MoveMountFlags, fsconfig_create,
        fsconfig_set_string, fsmount, fsopen, move_mount,
    },
};
use std::{
    collections::HashSet,
    io::Result,
    os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd}, // Removed IntoRawFd
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub enum FsHandle {
    Fd(OwnedFd),
    Path(PathBuf),
}

impl FsHandle {
    // Renamed from open_raw_fs to new_from_fs_name for clarity
    pub fn new_from_fs_name(name: &str) -> Result<FsHandle> {
        Ok(FsHandle::Fd(fsopen(name, FsOpenFlags::FSOPEN_CLOEXEC)?))
    }

    pub fn path(&self) -> Option<&Path> {
        match self {
            FsHandle::Path(p) => Some(p.as_path()),
            FsHandle::Fd(_) => None,
        }
    }

    // Note: into_fd is removed due to Drop trait conflicts
    // Use as_fd() for borrowing or handle the enum variants directly
}

impl AsFd for FsHandle {
    fn as_fd(&self) -> BorrowedFd {
        match self {
            FsHandle::Fd(fd) => fd.as_fd(),
            // This is tricky. A Path variant doesn't have an FD in the same way.
            // This might panic or be incorrect depending on usage.
            // Consider if AsFd is appropriate for a Path variant or if users
            // should explicitly handle the Fd case.
            // For now, to satisfy existing usages that might expect an FD (like Drop),
            // we'll make it panic if it's a Path. This needs careful review.
            FsHandle::Path(p) => panic!(
                "Cannot call as_fd on an FsHandle::Path variant for path: {:?}",
                p
            ),
        }
    }
}

impl Drop for FsHandle {
    fn drop(&mut self) {
        match self {
            FsHandle::Fd(fd) => {
                // OwnedFd handles closing automatically. The read loop was likely for debugging or specific error handling.
                // For a generic FsHandle, just ensuring the FD is closed is sufficient.
                // The AsRawFd trait is available on OwnedFd directly.
                tracing::debug!(
                    "FsHandle::Fd dropped, fd {} will be closed.",
                    fd.as_raw_fd()
                );
            }
            FsHandle::Path(path_buf) => {
                tracing::debug!(
                    "FsHandle::Path dropped, attempting to unmount {:?}",
                    path_buf
                );
                // Attempt to unmount the path
                // MNT_DETACH is often useful for cleanup
                if let Err(e) = umount2(path_buf.as_path(), MntFlags::MNT_DETACH) {
                    tracing::warn!(
                        "Failed to unmount {:?} during FsHandle drop: {}",
                        path_buf,
                        e
                    );
                } else {
                    tracing::debug!(
                        "Successfully unmounted {:?} during FsHandle drop.",
                        path_buf
                    );
                }
            }
        }
    }
}

pub fn mount_at(fs_fd: impl AsFd, dirfd: impl AsFd, path: impl AsRef<Path>) -> Result<()> {
    move_mount(
        fs_fd.as_fd(),
        "",
        dirfd.as_fd(),
        path.as_ref(),
        MoveMountFlags::MOVE_MOUNT_F_EMPTY_PATH,
    )?;
    Ok(())
}

pub trait EphemeralMount {
    fn get_mountpoint(&self) -> &PathBuf;
}

pub struct TempOvlMount {
    pub mountpoint: PathBuf,
    /// List of lower base directories for the overlay mount.
    /// the lowerdir list here will be reversed when mounting,
    /// so the last entry will be the topmost layer, instead
    /// of the usual kernel order of first-is-topmost order
    pub lowerdirs: HashSet<PathBuf>,
    /// Upper directory for the overlay mount.
    /// This is where changes will be written.
    /// This directory will be created if it does not exist.
    pub upperdir: PathBuf,
    /// Work directory for the overlay mount.
    ///
    /// if not provided, a temporary directory will be created.
    pub workdir: Option<PathBuf>,
}

impl TempOvlMount {
    pub fn new(
        mountpoint: PathBuf,
        lowerdirs: HashSet<PathBuf>,
        upperdir: PathBuf,
        workdir: Option<PathBuf>,
    ) -> Self {
        TempOvlMount {
            mountpoint,
            lowerdirs,
            upperdir,
            workdir,
        }
    }

    pub fn mount(&self) -> Result<()> {
        // Create the workdir if needed
        let workdir_str = match &self.workdir {
            Some(path) => path.display().to_string(),
            None => {
                let temp_dir = tempfile::tempdir()?;
                // We need to keep the temp dir alive, but for now use its path
                temp_dir.path().display().to_string()
            }
        };

        // Build the lowerdir vector
        let mut lowerdirs: Vec<&str> = self.lowerdirs.iter().map(|p| p.to_str().unwrap()).collect();
        lowerdirs.reverse();

        // Use the convenience function to mount
        mount_overlay_at(
            &lowerdirs,
            self.upperdir.to_str().unwrap(),
            &workdir_str,
            &self.mountpoint,
        )
    }
}

impl EphemeralMount for TempOvlMount {
    fn get_mountpoint(&self) -> &PathBuf {
        &self.mountpoint
    }
}

impl Drop for TempOvlMount {
    fn drop(&mut self) {
        // First unmount the filesystem
        // First try with DETACH, which is less forceful
        if let Err(e) = umount2(self.get_mountpoint(), MntFlags::empty()) {
            tracing::error!(
                "Failed to unmount {}: {}",
                self.get_mountpoint().display(),
                e
            );
        }

        // Now that the mountpoint is unmounted, sync the remaining directories
        // Note: we exclude the mountpoint since it's already unmounted
        let mut dirs_to_sync = vec![&self.upperdir];
        if let Some(workdir) = &self.workdir {
            dirs_to_sync.push(workdir);
        }
        dirs_to_sync.extend(self.lowerdirs.iter());

        // Sync all directories
        for dir in dirs_to_sync {
            if let Err(e) = fsync_dir(dir) {
                tracing::error!("Failed to fsync {}: {}", dir.display(), e);
            }
        }
    }
}

#[tracing::instrument(level = "trace", name = "fsync_dir")]
pub fn fsync_dir(path: &Path) -> Result<()> {
    tracing::trace!("running fsync");
    let file = std::fs::File::open(path)?;
    rustix::fs::fsync(&file)?;
    Ok(())
}

pub fn overlay_fsmount(lowerdirs: &[&str], upperdir: &str, workdir: &str) -> Result<OwnedFd> {
    let overlayfs = FsHandle::new_from_fs_name("overlay")?;

    let lowerdirs_str = lowerdirs.join(":");
    fsconfig_set_string(overlayfs.as_fd(), "lowerdir", &lowerdirs_str)?;
    fsconfig_set_string(overlayfs.as_fd(), "upperdir", upperdir)?;
    fsconfig_set_string(overlayfs.as_fd(), "workdir", workdir)?;
    fsconfig_create(overlayfs.as_fd())?;

    Ok(fsmount(
        overlayfs.as_fd(),
        FsMountFlags::FSMOUNT_CLOEXEC,
        MountAttrFlags::empty(),
    )?)
}

pub fn mount_overlay_at(
    lowerdirs: &[&str],
    upperdir: &str,
    workdir: &str,
    mountpoint: impl AsRef<Path>,
) -> Result<()> {
    let mnt = overlay_fsmount(lowerdirs, upperdir, workdir)?;
    mount_at(mnt, CWD, mountpoint)
}

/// Mount a composefs image with an optional upperdir for writable layers
pub fn mount_composefs_with_upperdir(
    image_path: impl AsRef<Path>,
    name: &str,
    basedir: Option<impl AsRef<Path>>,
    upperdir: Option<impl AsRef<Path>>,
    mountpoint: impl AsRef<Path>,
) -> Result<()> {
    mount_composefs_with_upperdir_and_source(image_path, name, basedir, upperdir, mountpoint, None)
}

/// Mount a composefs image with optional upperdir and custom source name
pub fn mount_composefs_with_upperdir_and_source(
    image_path: impl AsRef<Path>,
    name: &str,
    basedir: Option<impl AsRef<Path>>,
    upperdir: Option<impl AsRef<Path>>,
    mountpoint: impl AsRef<Path>,
    source_name: Option<&str>,
) -> Result<()> {
    use std::fs::File;

    let image_fd = File::open(image_path)?.into();
    let mut config = if let Some(upperdir) = upperdir {
        composefs::ComposeFsConfig::writable(
            image_fd,
            name.to_string(),
            upperdir.as_ref().to_path_buf(),
            None, // Auto-generate workdir
        )
    } else {
        composefs::ComposeFsConfig::read_only(image_fd, name.to_string())
    };

    if let Some(basedir) = basedir {
        config = config.with_basedir(basedir.as_ref().to_path_buf());
    }

    if let Some(source_name) = source_name {
        config = config.with_source_name(source_name.to_string());
    }

    composefs::mount_composefs_at(&config, mountpoint.as_ref())?;
    Ok(())
}

/// Create a managed composefs mount that can be easily controlled
pub fn create_composefs_mount(
    image_path: impl AsRef<Path>,
    name: &str,
    basedir: Option<impl AsRef<Path>>,
    upperdir: Option<impl AsRef<Path>>,
    mountpoint: impl AsRef<Path>,
) -> Result<composefs::ComposeFsMount> {
    use std::fs::File;

    let image_fd = File::open(image_path)?.into();
    let config = if let Some(upperdir) = upperdir {
        composefs::ComposeFsConfig::writable(
            image_fd,
            name.to_string(),
            upperdir.as_ref().to_path_buf(),
            None, // Auto-generate workdir
        )
    } else {
        composefs::ComposeFsConfig::read_only(image_fd, name.to_string())
    };

    let config = if let Some(basedir) = basedir {
        config.with_basedir(basedir.as_ref().to_path_buf())
    } else {
        config
    };

    Ok(composefs::ComposeFsMount::new(
        config,
        mountpoint.as_ref().to_path_buf(),
    ))
}

/// Mount a composefs image persistently (mount survives after function returns)
/// This creates a persistent mount that will remain active until manually unmounted
/// or the system is rebooted. The mount is not automatically cleaned up.
pub fn mount_composefs_persistent(
    image_path: impl AsRef<Path>,
    name: &str,
    basedir: Option<impl AsRef<Path>>,
    upperdir: Option<impl AsRef<Path>>,
    mountpoint: impl AsRef<Path>,
) -> Result<()> {
    mount_composefs_persistent_with_source(image_path, name, basedir, upperdir, mountpoint, None)
}

/// Mount a composefs image persistently with custom source name
/// This creates a persistent mount that will remain active until manually unmounted
/// or the system is rebooted. The mount is not automatically cleaned up.
pub fn mount_composefs_persistent_with_source(
    image_path: impl AsRef<Path>,
    name: &str,
    basedir: Option<impl AsRef<Path>>,
    upperdir: Option<impl AsRef<Path>>,
    mountpoint: impl AsRef<Path>,
    source_name: Option<&str>,
) -> Result<()> {
    use std::fs::File;
    use std::mem;

    let image_fd = File::open(image_path)?.into();
    let mut config = if let Some(upperdir) = upperdir {
        composefs::ComposeFsConfig::writable(
            image_fd,
            name.to_string(),
            upperdir.as_ref().to_path_buf(),
            None, // Auto-generate workdir
        )
    } else {
        composefs::ComposeFsConfig::read_only(image_fd, name.to_string())
    };

    if let Some(basedir) = basedir {
        config = config.with_basedir(basedir.as_ref().to_path_buf());
    }

    if let Some(source_name) = source_name {
        config = config.with_source_name(source_name.to_string());
    }

    // Ensure mountpoint exists
    std::fs::create_dir_all(mountpoint.as_ref())?;

    // Ensure upperdir exists if specified
    if let Some(upperdir_path) = config.upperdir.as_ref() {
        std::fs::create_dir_all(upperdir_path)?;
    }

    // Create the mount and intentionally "leak" the handle to make it persistent
    let fs_handle = composefs::mount_composefs_at(&config, mountpoint.as_ref())?;

    // Forget the handle so it doesn't get dropped and unmounted
    // This makes the mount persistent until manually unmounted
    mem::forget(fs_handle);

    Ok(())
}

/// Unmount a persistent composefs mount at the specified path
/// This can be used to clean up mounts created with mount_composefs_persistent
pub fn unmount_composefs_persistent(mountpoint: impl AsRef<Path>) -> Result<()> {
    use nix::mount::{MntFlags, umount2};
    umount2(mountpoint.as_ref(), MntFlags::empty())?;
    Ok(())
}
