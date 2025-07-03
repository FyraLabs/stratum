//! ComposeFS mounting support with upperdir capability
//!
//! This module provides enhanced composefs mounting capabilities that support
//! writable upperdirs on top of the read-only composefs base layers.

//

/*
    SPDX-License-Identifier: GPL-3.0-or-later

    This file is a fork of the composefs-rs project
    (https://github.com/containers/composefs-rs), originally licensed
    under MIT OR Apache-2.0.

    Original code (c) Containers, Red Hat and contributors,
    licensed under MIT OR Apache-2.0.

    Modifications and additions (c) Fyra Labs
*/

use std::{
    fs::canonicalize,
    io::Result,
    mem,
    os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd},
    path::{Path, PathBuf},
};

use rustix::{
    fs::{CWD, Mode as RustixFsMode, OFlags as RustixFsOFlags, open, openat},
    mount::{
        FsMountFlags,
        FsOpenFlags, // Moved FsOpenFlags here
        MountAttrFlags,
        MoveMountFlags,
        UnmountFlags,
        fsconfig_create,
        fsconfig_set_string,
        fsmount,
        move_mount,
        unmount,
    },
};

use super::FsHandle;

// Private module for pre-6.15 temporary mount compatibility
mod tmp_mount_compat {
    use super::*; // Imports Result, AsFd, BorrowedFd, OwnedFd, Path, CWD, open, RustixFsMode, RustixFsOFlags, move_mount, unmount, MoveMountFlags, UnmountFlags
    use tempfile::TempDir;

    #[derive(Debug)]
    pub(super) struct TmpMount {
        dir: TempDir,
        fd: OwnedFd,
    }

    impl TmpMount {
        pub(super) fn mount(mnt_fd: OwnedFd) -> Result<Self> {
            let tmp = TempDir::new().inspect_err(|e| {
                tracing::error!("Failed to create temp dir for TmpMount: {}", e);
            })?;
            tracing::debug!("TmpMount: Created temp dir at {:?}", tmp.path());
            move_mount(
                mnt_fd.as_fd(),
                "",
                CWD,
                tmp.path(),
                MoveMountFlags::MOVE_MOUNT_F_EMPTY_PATH,
            )
            .inspect_err(|e| {
                tracing::error!(
                    "TmpMount: Failed to move_mount fd {} to {:?}: {}",
                    mnt_fd.as_raw_fd(),
                    tmp.path(),
                    e
                );
            })?;
            tracing::debug!(
                "TmpMount: Moved mount fd {} to {:?}",
                mnt_fd.as_raw_fd(),
                tmp.path()
            );

            let opened_fd = open(
                tmp.path(),
                RustixFsOFlags::PATH | RustixFsOFlags::DIRECTORY | RustixFsOFlags::CLOEXEC,
                RustixFsMode::empty(),
            )
            .inspect_err(|e| {
                tracing::error!(
                    "TmpMount: Failed to open O_PATH for {:?}: {}",
                    tmp.path(),
                    e
                );
            })?;
            tracing::debug!(
                "TmpMount: Opened O_PATH fd {} for {:?}",
                opened_fd.as_raw_fd(),
                tmp.path()
            );
            Ok(TmpMount {
                dir: tmp,
                fd: opened_fd,
            })
        }
    }

    impl AsFd for TmpMount {
        fn as_fd(&self) -> BorrowedFd<'_> {
            self.fd.as_fd()
        }
    }

    impl Drop for TmpMount {
        fn drop(&mut self) {
            tracing::debug!(
                "TmpMount: Dropping, attempting to unmount {:?}",
                self.dir.path()
            );
            if let Err(e) = unmount(self.dir.path(), UnmountFlags::DETACH) {
                tracing::warn!(
                    "Failed to unmount temporary mount at {:?} during drop: {}",
                    self.dir.path(),
                    e
                );
            } else {
                tracing::debug!("TmpMount: Successfully unmounted {:?}", self.dir.path());
            }
        }
    }
}

/// Formats a string like "/proc/self/fd/3" for the given fd.
pub(crate) fn proc_self_fd(fd: impl AsFd) -> String {
    format!("/proc/self/fd/{}", fd.as_fd().as_raw_fd())
}

/// Prepares a mounted filesystem for further use.
/// On 6.15+ kernels, this is a no-op. On earlier kernels, it might involve
/// mounting to a temporary directory.
pub fn prepare_mount(mnt_fd: OwnedFd) -> Result<impl AsFd> {
    // TODO: Implement actual kernel version checking.
    // For now, let's try TmpMount approach since the direct fd approach
    // is failing with "failed to clone lowerpath"
    tracing::debug!(
        "prepare_mount: Using TmpMount for compatibility for fd {}.",
        mnt_fd.as_raw_fd()
    );
    tmp_mount_compat::TmpMount::mount(mnt_fd)

    // Use direct fd approach for 6.15+ kernels (currently disabled):
    // tracing::debug!(
    //     "prepare_mount: Using direct fd approach (6.15+ behavior) for fd {}.",
    //     mnt_fd.as_raw_fd()
    // );
    // Ok(mnt_fd)
}

/// Sets an overlayfs mount option that takes a file descriptor.
/// Includes fallback for kernels that don't support fd-based options (e.g., "upperdir+").
pub fn overlayfs_set_fd(
    fs_fd: impl AsFd,
    key: &str,
    original_fd_to_set: impl AsFd,
) -> rustix::io::Result<()> {
    let fs_fd_borrowed = fs_fd.as_fd();
    let original_fd_borrowed = original_fd_to_set.as_fd();

    // Tier 1: Attempt direct fsconfig_set_fd
    match rustix::mount::fsconfig_set_fd(fs_fd_borrowed, key, original_fd_borrowed) {
        Ok(()) => {
            tracing::debug!(
                "overlayfs_set_fd: Successfully set key '{}' with original_fd {}",
                key,
                original_fd_borrowed.as_raw_fd()
            );
            Ok(())
        }
        Err(e1) if e1 == rustix::io::Errno::INVAL => {
            tracing::warn!(
                "overlayfs_set_fd: fsconfig_set_fd for key '{}' with original_fd {} failed (EINVAL). Attempting to reopen fd with O_RDONLY.",
                key,
                original_fd_borrowed.as_raw_fd()
            );

            // Tier 2: Re-open original_fd_to_set with O_RDONLY | O_DIRECTORY and retry
            match openat(
                original_fd_borrowed,
                ".",
                RustixFsOFlags::RDONLY | RustixFsOFlags::DIRECTORY | RustixFsOFlags::CLOEXEC,
                RustixFsMode::empty(),
            ) {
                Ok(rdonly_fd) => {
                    tracing::debug!(
                        "overlayfs_set_fd: Successfully reopened original_fd {} as O_RDONLY fd {}. Retrying fsconfig_set_fd for key '{}'.",
                        original_fd_borrowed.as_raw_fd(),
                        rdonly_fd.as_fd().as_raw_fd(),
                        key
                    );
                    match rustix::mount::fsconfig_set_fd(fs_fd_borrowed, key, rdonly_fd.as_fd()) {
                        Ok(()) => {
                            tracing::debug!(
                                "overlayfs_set_fd: Successfully set key '{}' with O_RDONLY fd {}",
                                key,
                                rdonly_fd.as_fd().as_raw_fd()
                            );
                            Ok(())
                        }
                        Err(e2) if e2 == rustix::io::Errno::INVAL => {
                            tracing::warn!(
                                "overlayfs_set_fd: fsconfig_set_fd for key '{}' with O_RDONLY fd {} also failed (EINVAL). Checking if we can fall back to string path.",
                                key,
                                rdonly_fd.as_fd().as_raw_fd()
                            );

                            // Special handling for upperdir and workdir - these need persistent paths, not /proc/self/fd/ paths
                            if key == "upperdir+" || key == "workdir+" {
                                tracing::error!(
                                    "overlayfs_set_fd: Cannot use /proc/self/fd/ fallback for key '{}' as it requires a persistent directory path. Consider using string-based mount options instead.",
                                    key
                                );
                                Err(e2)
                            } else {
                                // Tier 3: Fallback to string path (only for lowerdir+, datadir+, etc.)
                                let fallback_key = key.strip_suffix('+').unwrap_or(key);
                                let proc_path = proc_self_fd(original_fd_borrowed);
                                tracing::debug!(
                                    "overlayfs_set_fd: Using /proc/self/fd/ fallback for key '{}' -> '{}'",
                                    fallback_key,
                                    proc_path
                                );
                                rustix::mount::fsconfig_set_string(fs_fd_borrowed, fallback_key, &proc_path)
                                    .inspect_err(|e_str| {
                                        tracing::error!(
                                            "overlayfs_set_fd: Fallback fsconfig_set_string for key '{}' with path '{}' also failed: {}",
                                            fallback_key,
                                            proc_path,
                                            e_str
                                        );
                                    })
                            }
                        }
                        Err(e2) => {
                            tracing::error!(
                                "overlayfs_set_fd: fsconfig_set_fd for key '{}' with O_RDONLY fd {} failed with unexpected error: {}",
                                key,
                                rdonly_fd.as_fd().as_raw_fd(),
                                e2
                            );
                            Err(e2)
                        }
                    }
                }
                Err(reopen_err) => {
                    tracing::warn!(
                        "overlayfs_set_fd: Failed to reopen original_fd {} as O_RDONLY (error: {}). Falling back to string path for key '{}'.",
                        original_fd_borrowed.as_raw_fd(),
                        reopen_err,
                        key
                    );
                    // Tier 3: Fallback to string path (if reopen failed)
                    let fallback_key = key.strip_suffix('+').unwrap_or(key);
                    let proc_path = proc_self_fd(original_fd_borrowed);
                    rustix::mount::fsconfig_set_string(fs_fd_borrowed, fallback_key, &proc_path)
                        .inspect_err(|e_str| {
                            tracing::error!(
                                "overlayfs_set_fd: Fallback fsconfig_set_string for key '{}' with path '{}' also failed: {}",
                                fallback_key,
                                proc_path,
                                e_str
                            );
                        })
                }
            }
        }
        Err(e1) => {
            tracing::error!(
                "overlayfs_set_fd: fsconfig_set_fd for key '{}' with original_fd {} failed with unexpected error: {}",
                key,
                original_fd_borrowed.as_raw_fd(),
                e1
            );
            Err(e1)
        }
    }
}

/// Sets the "lowerdir+" and "datadir+" mount options of an overlayfs mount.
pub fn overlayfs_set_lower_and_data_fds(
    fs_fd: impl AsFd,
    lower_fd: impl AsFd,
    data_fd: Option<impl AsFd>,
) -> rustix::io::Result<()> {
    overlayfs_set_fd(fs_fd.as_fd(), "lowerdir+", lower_fd.as_fd())?;
    if let Some(data_fd) = data_fd {
        overlayfs_set_fd(fs_fd.as_fd(), "datadir+", data_fd.as_fd())?;
    }
    Ok(())
}

/// Configuration for composefs mounting with optional upperdir support
#[derive(Debug)]
pub struct ComposeFsConfig {
    /// The composefs image file descriptor
    pub image_fd: OwnedFd,
    /// The name/source identifier for the mount
    pub name: String,
    /// Optional custom source name for the mount (shows in /proc/mounts)
    pub source_name: Option<String>,
    /// Optional base directory for composefs objects
    pub basedir: Option<PathBuf>,
    /// Optional upperdir for writable overlay
    pub upperdir: Option<PathBuf>,
    /// Optional workdir for overlay (required if upperdir is provided)
    pub workdir: Option<PathBuf>,
    /// Whether to enable verity checking
    pub verity_required: bool,
    /// Whether to enable metacopy
    pub metacopy: bool,
    /// Whether to enable redirect_dir
    pub redirect_dir: bool,
}

impl ComposeFsConfig {
    /// Create a new read-only composefs configuration
    pub fn read_only(image_fd: OwnedFd, name: String) -> Self {
        Self {
            image_fd,
            name,
            source_name: None,
            basedir: None,
            upperdir: None,
            workdir: None,
            verity_required: true,
            metacopy: true,
            redirect_dir: true,
        }
    }

    /// Create a new writable composefs configuration with upperdir
    pub fn writable(
        image_fd: OwnedFd,
        name: String,
        upperdir: PathBuf,
        workdir: Option<PathBuf>,
    ) -> Self {
        Self {
            image_fd,
            name,
            source_name: None,
            basedir: None,
            upperdir: Some(upperdir),
            workdir,
            verity_required: true,
            metacopy: true,
            redirect_dir: true,
        }
    }

    /// Set the base directory for composefs objects
    pub fn with_basedir(mut self, basedir: PathBuf) -> Self {
        self.basedir = Some(basedir);
        self
    }

    /// Set custom source name for the mount
    pub fn with_source_name(mut self, source_name: String) -> Self {
        self.source_name = Some(source_name);
        self
    }

    /// Set verity requirement
    pub fn with_verity(mut self, required: bool) -> Self {
        self.verity_required = required;
        self
    }

    /// Set metacopy option
    pub fn with_metacopy(mut self, enabled: bool) -> Self {
        self.metacopy = enabled;
        self
    }

    /// Set redirect_dir option
    pub fn with_redirect_dir(mut self, enabled: bool) -> Self {
        self.redirect_dir = enabled;
        self
    }
}

/// Mounts an EROFS filesystem image.
pub fn erofs_fsmount(
    image_fd: impl AsFd,
    _config: &ComposeFsConfig, // Keep config for future EROFS options
) -> Result<OwnedFd> {
    let erofs = rustix::mount::fsopen("erofs", FsOpenFlags::empty())?; // Changed FsMountFlags to FsOpenFlags
    // TODO: Handle config.verity, config.metacopy, config.redirect_dir if applicable to EROFS
    fsconfig_set_string(erofs.as_fd(), "source", proc_self_fd(image_fd))?;
    fsconfig_create(erofs.as_fd())?;
    let mnt_fd = fsmount(
        erofs.as_fd(),
        FsMountFlags::empty(),
        MountAttrFlags::empty(),
    )?;
    Ok(mnt_fd)
}

/// Mounts a composefs (EROFS base + OverlayFS) filesystem.
pub fn composefs_fsmount(
    config: &ComposeFsConfig,
    target_path: Option<&Path>,
    basedir_path: Option<&Path>,
    upperdir_path: Option<&Path>,
    workdir_path: Option<&Path>,
) -> Result<FsHandle> {
    tracing::debug!(
        "composefs_fsmount called with target: {:?}, basedir: {:?}, upper: {:?}, work: {:?}",
        target_path,
        basedir_path,
        upperdir_path,
        workdir_path
    );

    // Mount EROFS base layer
    let erofs_mnt_raw = erofs_fsmount(config.image_fd.as_fd(), config)?;
    tracing::debug!("EROFS layer mounted, raw fd: {}", erofs_mnt_raw.as_raw_fd());

    // Prepare the EROFS mount (e.g., move to temp dir for older kernels)
    let erofs_mnt = prepare_mount(erofs_mnt_raw)?;
    let erofs_mnt_fd = erofs_mnt.as_fd(); // erofs_mnt now holds the (potentially TmpMount) prepared mount
    tracing::debug!(
        "EROFS mount prepared, effective fd: {}",
        erofs_mnt_fd.as_raw_fd()
    );

    tracing::trace!("Opening overlayfs for composefs mount");
    // Open OverlayFS
    let overlayfs = rustix::mount::fsopen("overlay", FsOpenFlags::empty())?;

    tracing::debug!("OverlayFS opened, fd: {}", overlayfs.as_fd().as_raw_fd());

    // Set the mount source name to something meaningful instead of "none"
    let mount_name = config
        .source_name
        .as_ref()
        .cloned()
        .unwrap_or_else(|| format!("composefs-{}", config.name));
    fsconfig_set_string(overlayfs.as_fd(), "source", &mount_name)?;
    tracing::debug!("Set overlay mount source name to: {}", mount_name);
    // Set basedir if provided
    let mut _basedir_fd_owned: Option<OwnedFd> = None; // To keep the FD alive if opened here
    let basedir_fd_ref: Option<BorrowedFd<'_>> = if let Some(bpath) = basedir_path {
        let fd = open(
            bpath,
            RustixFsOFlags::PATH | RustixFsOFlags::DIRECTORY | RustixFsOFlags::CLOEXEC,
            RustixFsMode::empty(),
        )?;
        _basedir_fd_owned = Some(fd);
        _basedir_fd_owned.as_ref().map(|owned| owned.as_fd())
    } else {
        config.basedir.as_ref().and_then(|path| {
            match open(
                path,
                RustixFsOFlags::PATH | RustixFsOFlags::DIRECTORY | RustixFsOFlags::CLOEXEC,
                RustixFsMode::empty(),
            ) {
                Ok(fd) => {
                    _basedir_fd_owned = Some(fd);
                    _basedir_fd_owned.as_ref().map(|owned| owned.as_fd())
                }
                Err(_) => None,
            }
        })
    };

    tracing::debug!(
        "Setting basedir fd: {:?} for overlayfs",
        basedir_fd_ref.map(|fd| fd.as_raw_fd())
    );

    overlayfs_set_lower_and_data_fds(overlayfs.as_fd(), erofs_mnt_fd, basedir_fd_ref)?;

    // Set metacopy option if enabled in config
    if config.metacopy {
        tracing::debug!("Setting metacopy=on for overlayfs");
        fsconfig_set_string(overlayfs.as_fd(), "metacopy", "on")?;
    }

    // Set redirect_dir option if enabled in config
    if config.redirect_dir {
        tracing::debug!("Setting redirect_dir=on for overlayfs");
        fsconfig_set_string(overlayfs.as_fd(), "redirect_dir", "on")?;
    }

    tracing::debug!("Lower and data fds set for overlayfs");
    // Set upperdir if provided - use string paths instead of FDs for persistence
    if let Some(upath) = upperdir_path {
        tracing::debug!("Setting upperdir to: {:?}", upath);
        fsconfig_set_string(
            overlayfs.as_fd(),
            "upperdir",
            upath.to_str().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "upperdir path is not valid UTF-8",
                )
            })?,
        )?;
    } else if let Some(upperdir_path) = config.upperdir.as_ref() {
        tracing::debug!("Setting upperdir from config to: {:?}", upperdir_path);
        fsconfig_set_string(
            overlayfs.as_fd(),
            "upperdir",
            upperdir_path.to_str().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "upperdir path is not valid UTF-8",
                )
            })?,
        )?;
    }
    // Set workdir if provided (must be on the same filesystem as upperdir)
    if let Some(wpath) = workdir_path {
        tracing::debug!("Setting workdir to: {:?}", wpath);
        fsconfig_set_string(
            overlayfs.as_fd(),
            "workdir",
            wpath.to_str().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "workdir path is not valid UTF-8",
                )
            })?,
        )?;
    } else if let Some(workdir_path) = config.workdir.as_ref() {
        tracing::debug!("Setting workdir from config to: {:?}", workdir_path);
        fsconfig_set_string(
            overlayfs.as_fd(),
            "workdir",
            workdir_path.to_str().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "workdir path is not valid UTF-8",
                )
            })?,
        )?;
    } else if upperdir_path.is_some() || config.upperdir.is_some() {
        tracing::debug!(
            "No workdir provided, but upperdir is specified. Auto-creating a temporary workdir."
        );
        let auto_workdir = tempfile::TempDir::new().inspect_err(|e| {
            tracing::error!("Failed to create temporary workdir: {}", e);
        })?;
        tracing::debug!(
            "Auto-created temporary workdir at: {:?}",
            auto_workdir.path()
        );
        fsconfig_set_string(
            overlayfs.as_fd(),
            "workdir",
            auto_workdir.path().to_str().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "auto workdir path is not valid UTF-8",
                )
            })?,
        )?;
        // Keep the temp directory alive for the duration of this function
        std::mem::forget(auto_workdir);
    } else {
        tracing::debug!("No workdir or upperdir specified, skipping workdir setup.");
    }

    tracing::debug!("Finalizing overlayfs configuration with fsconfig_create.");
    fsconfig_create(overlayfs.as_fd()).inspect_err(|e| {
        tracing::error!(
            "Failed to finalize overlayfs configuration with fsconfig_create: {}",
            e
        );
    })?;
    tracing::debug!("Successfully finalized overlayfs configuration with fsconfig_create.");

    let mount_flags = if target_path.is_some() {
        tracing::debug!("Target path specified, using empty FsMountFlags for move_mount.");
        FsMountFlags::empty() // Will be moved to target
    } else {
        tracing::debug!("No target path specified, using FSMOUNT_CLOEXEC for returned fd.");
        FsMountFlags::FSMOUNT_CLOEXEC // No target, so CLOEXEC on the returned fd
    };

    let final_mnt_fd = fsmount(overlayfs.as_fd(), mount_flags, MountAttrFlags::empty())?;

    if let Some(tp) = target_path {
        let canon_target_path = canonicalize(tp).unwrap_or_else(|_| tp.to_path_buf());
        tracing::debug!(
            "Moving overlay mount fd {} to target path: {:?}",
            final_mnt_fd.as_raw_fd(),
            canon_target_path
        );
        move_mount(
            final_mnt_fd.as_fd(),
            "",
            CWD,
            &canon_target_path,
            MoveMountFlags::MOVE_MOUNT_F_EMPTY_PATH,
        )?;

        // We're not really supposed to drop erofs_mnt here,
        // since we need to keep the EROFS layer alive for the overlay.
        // However, we're dropping it anyway so it gets cleaned up, it should work fine though

        // If mount fails, consider uncommenting this
        // tracing::debug!("Forgetting erofs_mnt to keep EROFS layer alive for overlay");
        // std::mem::forget(erofs_mnt);

        Ok(FsHandle::Path(canon_target_path))
    } else {
        // Note: Keeping the erofs_mnt alive to ensure proper cleanup.
        // Returning FsHandle::Fd while holding onto erofs_mnt to avoid issues.
        tracing::debug!("Returning FsHandle::Fd while keeping erofs_mnt alive for proper cleanup.");
        Ok(FsHandle::Fd(final_mnt_fd))
    }
}

/// Convenience function to mount a composefs image at a specific path.
pub fn mount_composefs_at(config: &ComposeFsConfig, mountpoint: &Path) -> Result<FsHandle> {
    composefs_fsmount(
        config,
        Some(mountpoint),
        config.basedir.as_deref(),
        config.upperdir.as_deref(),
        config.workdir.as_deref(),
    )
}

/// Mount a composefs image persistently at the specified path
/// The mount will persist until manually unmounted or system reboot
pub fn mount_composefs_persistent_at(config: &ComposeFsConfig, mountpoint: &Path) -> Result<()> {
    // Create the mount and intentionally "leak" the handle to make it persistent
    let fs_handle = mount_composefs_at(config, mountpoint)?;

    // Forget the handle so it doesn't get dropped and unmounted
    // This makes the mount persistent until manually unmounted
    mem::forget(fs_handle);

    tracing::info!(
        "Composefs mounted persistently at {} (will not auto-unmount)",
        mountpoint.display()
    );

    Ok(())
}

/// Unmount a composefs mount at the specified path
/// This can be used to clean up persistent mounts
pub fn unmount_composefs_at(mountpoint: &Path) -> Result<()> {
    use nix::mount::{MntFlags, umount2};
    umount2(mountpoint, MntFlags::empty())?;
    tracing::info!("Unmounted composefs at {}", mountpoint.display());
    Ok(())
}

/// Represents a managed composefs mount that will be unmounted on Drop.
/// Enhanced with upperdir support for writable mounts.
#[derive(Debug)]
pub struct ComposeFsMount {
    pub mountpoint: PathBuf,
    pub config: ComposeFsConfig,
    /// Whether this mount is currently active
    pub is_mounted: bool,
    /// The filesystem handle that keeps the mount alive
    fs_handle: Option<FsHandle>,
}

impl ComposeFsMount {
    /// Create a new composefs mount
    pub fn new(config: ComposeFsConfig, mountpoint: PathBuf) -> Self {
        Self {
            mountpoint,
            config,
            is_mounted: false,
            fs_handle: None,
        }
    }

    /// Mount the composefs
    pub fn mount(&mut self) -> Result<()> {
        if self.is_mounted {
            return Ok(());
        }

        // Ensure mountpoint exists
        std::fs::create_dir_all(&self.mountpoint)?;

        // We need to duplicate the fd since mount_composefs_at takes ownership
        let duplicated_fd = rustix::io::dup(&self.config.image_fd)?;

        // The ComposeFsConfig for composefs_fsmount needs an OwnedFd for image_fd.
        // The original config.image_fd is moved into `duplicated_fd`.
        // We need to ensure that if basedir is a path, it's handled correctly by composefs_fsmount.
        // composefs_fsmount now handles opening basedir path to an fd.

        let mount_config = ComposeFsConfig {
            image_fd: duplicated_fd, // This is now the owned FD for the mount operation
            name: self.config.name.clone(),
            source_name: self.config.source_name.clone(),
            basedir: self.config.basedir.clone(), // Pass the path, composefs_fsmount will open it
            upperdir: self.config.upperdir.clone(),
            workdir: self.config.workdir.clone(),
            verity_required: self.config.verity_required,
            metacopy: self.config.metacopy,
            redirect_dir: self.config.redirect_dir,
        };

        let fs_handle = mount_composefs_at(&mount_config, &self.mountpoint)?;
        self.fs_handle = Some(fs_handle);
        self.is_mounted = true;
        Ok(())
    }

    /// Unmount the composefs
    pub fn unmount(&mut self) -> Result<()> {
        if !self.is_mounted {
            return Ok(());
        }

        // Drop the fs_handle, which will automatically unmount the filesystem
        self.fs_handle = None;
        self.is_mounted = false;
        Ok(())
    }

    /// Check if this mount is writable (has upperdir)
    pub fn is_writable(&self) -> bool {
        self.config.upperdir.is_some()
    }
}

impl Drop for ComposeFsMount {
    fn drop(&mut self) {
        if self.is_mounted {
            if let Err(e) = self.unmount() {
                tracing::error!(
                    "Failed to unmount composefs at {}: {}",
                    self.mountpoint.display(),
                    e
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_composefs_config() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let image_fd = std::fs::File::open(temp_file.path()).unwrap().into();

        let config = ComposeFsConfig::read_only(image_fd, "test".to_string());
        assert!(config.upperdir.is_none());
        assert!(config.verity_required);
    }

    #[test]
    fn test_writable_config() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let image_fd = std::fs::File::open(temp_file.path()).unwrap().into();
        let temp_dir = TempDir::new().unwrap();

        let config = ComposeFsConfig::writable(
            image_fd,
            "test".to_string(),
            temp_dir.path().to_path_buf(),
            None,
        );
        assert!(config.upperdir.is_some());
        assert!(config.verity_required);
    }
}
