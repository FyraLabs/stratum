//! Stratum store module
//!
//! This module provides a type for managing and storing a stratum store, including
//! functionality for loading and saving the store to disk.
//!
//! This is similar to composefs-rs' `Repository` type.
pub mod chunks;
#[cfg(test)]
pub mod tests;

use tempfile::TempDir;

use crate::{
    commit::{StratumRef, Worktree},
    mount::EphemeralMount,
    object::ObjectDatabase,
    state::StateManager,
    util::{calculate_total_size, copy_dir_all, fsync_all_walk},
};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

pub struct Store {
    /// The path to the store's base directory, so we know where the store actually is
    pub base_path: String,
    object_database: ObjectDatabase,
    state_manager: StateManager,
}

impl Store {
    const OBJECTS_DIR: &'static str = "objects";
    const COMMITS_DIR: &'static str = "commits";
    const REFS_DIR: &'static str = "refs";
    const TAGS_DIR: &'static str = "tags";
    const WORKTREES_DIR: &'static str = "worktrees";
    const TEMP_DIR: &'static str = "temp";

    // // Default worktree name
    // const DEFAULT_WORKTREE: &'static str = "main";

    // Worktree subdirectories
    const UPPERDIR: &'static str = "upperdir";
    const WORKDIR: &'static str = "workdir";
    const WORKTREE_META_FILE: &'static str = "meta.toml";

    const COMMIT_META_FILE: &'static str = "metadata.toml";
    const COMMIT_FILE: &'static str = "commit.cfs";

    pub fn new(base_path: String) -> Self {
        std::fs::create_dir_all(&base_path).ok();

        // Sync the base directory to ensure it's written to disk
        if let Some(parent) = Path::new(&base_path).parent() {
            if let Err(e) = fsync_all_walk(parent) {
                tracing::warn!(
                    "Failed to fsync store base parent directory {}: {}",
                    parent.display(),
                    e
                );
            }
        }

        let object_database =
            ObjectDatabase::new(&base_path).expect("Failed to initialize object database");
        let state_manager = StateManager::new().expect("Failed to initialize state manager");
        Store {
            base_path,
            object_database,
            state_manager,
        }
    }

    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    pub fn temp_path(&self) -> String {
        let path = format!("{}/{}", self.base_path, Self::TEMP_DIR);
        std::fs::create_dir_all(&path).ok();
        path
    }

    pub fn new_tempdir(&self) -> TempDir {
        tempfile::Builder::new()
            .prefix("stratum_temp_")
            .tempdir_in(self.temp_path())
            .expect("Failed to create temporary directory")
    }

    /// Returns the path to the given ref's directory
    fn ref_path(&self, label: &str) -> String {
        format!("{}/{}/{}", self.base_path, Self::REFS_DIR, label)
    }

    /// Returns the path to the commits directory
    fn commits_path(&self) -> String {
        let path = format!("{}/{}", self.base_path, Self::COMMITS_DIR);
        std::fs::create_dir_all(&path).ok();
        path
    }

    /// Returns the path to a specific commit directory
    fn commit_path(&self, commit_id: &str) -> String {
        std::fs::create_dir_all(self.commits_path()).ok();
        format!("{}/{}", self.commits_path(), commit_id)
    }

    /// Returns the path to the tags directory for a given ref
    fn tags_path(&self, label: &str) -> String {
        // refs/<label>/tags/<tag>
        std::fs::create_dir_all(self.ref_path(label)).ok();
        format!("{}/{}", self.ref_path(label), Self::TAGS_DIR)
    }

    /// Returns the path to the objects directory
    fn objects_path(&self) -> String {
        let path = format!("{}/{}", self.base_path, Self::OBJECTS_DIR);
        std::fs::create_dir_all(&path).ok();
        path
    }

    /// Returns the path to the metadata file for a given ref and tag
    fn metadata_path(&self, label: &str, tag: &str) -> String {
        format!("{}/{tag}/{}", self.ref_path(label), Self::COMMIT_META_FILE)
    }

    fn ref_commit_path(&self, label: &str, tag: &str) -> String {
        format!("{}/{tag}", self.ref_path(label))
    }

    fn ref_commit_file_path(&self, label: &str, tag: &str) -> String {
        format!("{}/{tag}/{}", self.ref_path(label), Self::COMMIT_FILE)
    }

    /// Returns the path to the worktrees directory for a label
    fn worktrees_path(&self, label: &str) -> String {
        std::fs::create_dir_all(self.ref_path(label)).ok();
        format!("{}/{}", self.ref_path(label), Self::WORKTREES_DIR)
    }

    /// Returns the path to a specific worktree
    fn worktree_path(&self, label: &str, worktree: &str) -> String {
        std::fs::create_dir_all(self.worktrees_path(label)).ok();
        format!("{}/{}", self.worktrees_path(label), worktree)
    }

    /// Returns the path to a worktree's upperdir
    fn worktree_upperdir(&self, label: &str, worktree: &str) -> String {
        let worktree_path = self.worktree_path(label, worktree);
        std::fs::create_dir_all(&worktree_path).ok();
        format!("{}/{}", worktree_path, Self::UPPERDIR)
    }

    /// Returns the path to a worktree's workdir
    fn worktree_workdir(&self, label: &str, worktree: &str) -> String {
        let worktree_path = self.worktree_path(label, worktree);
        std::fs::create_dir_all(&worktree_path).ok();
        format!("{}/{}", worktree_path, Self::WORKDIR)
    }

    /// Returns the path to a worktree's metadata file
    fn worktree_meta_path(&self, label: &str, worktree: &str) -> String {
        format!(
            "{}/{}",
            self.worktree_path(label, worktree),
            Self::WORKTREE_META_FILE
        )
    }

    /// Mount a reference at a given mountpoint.
    ///
    /// This keeps track of currently-mounted points using [`crate::state::StateManager`],
    /// preventing the same worktree to be mounted mutably concurrently.
    ///
    /// If you're creating an ephemeral mount for the purpose of staging new commits,
    /// consider [`Self::mount_ref_ephemeral`] instead.
    ///
    /// # Arguments
    /// * `sref` - The stratum reference to mount
    /// * `mountpoint` - The path where to mount the filesystem
    /// * `worktree` - Optional worktree name. If provided, creates writable mount with upperdir. If None, creates read-only mount.
    pub fn mount_ref(
        &self,
        sref: &StratumRef,
        mountpoint: &str,
        worktree: Option<&str>,
    ) -> Result<(), String> {
        let cid = sref
            .resolve_commit_id(self)
            .map_err(|e| format!("Failed to resolve commit ID: {}", e))?;

        let actual_mountpoint = PathBuf::from(mountpoint);

        // Check if the destination is already a mountpoint
        let already_mountpoint = match mountpoints::mountpaths() {
            Ok(mountpaths) => {
                let canonical_mountpoint = Path::new(mountpoint).canonicalize().map_err(|e| {
                    format!("Failed to canonicalize mountpoint {}: {}", mountpoint, e)
                })?;
                mountpaths.iter().any(|p| p == &canonical_mountpoint)
            }
            Err(e) => return Err(format!("Failed to get current mountpoints: {}", e)),
        };

        // If it's already a mount point, unmount it first
        if already_mountpoint {
            tracing::info!(
                "Target {} is already a mountpoint, unmounting first",
                mountpoint
            );
            // Try to unmount using the standard unmount method
            if let Err(e) = self.unmount_ref(mountpoint) {
                tracing::warn!("Failed to unmount existing mount at {}: {}", mountpoint, e);
                // If unmount fails due to state manager issues, try a force unmount
                tracing::info!("Attempting force unmount at {}", mountpoint);
                if let Err(e) = crate::mount::composefs::unmount_composefs_at(Path::new(mountpoint))
                {
                    return Err(format!(
                        "Failed to force unmount existing mount at {}: {}",
                        mountpoint, e
                    ));
                }
            }
        }

        // After unmounting (if needed), we can use the actual mountpoint directly
        let mounted_mp = actual_mountpoint.clone();

        // Create mountpoint if it doesn't exist
        std::fs::create_dir_all(mountpoint)
            .map_err(|e| format!("Failed to create mountpoint {}: {}", mountpoint, e))?;

        // Canonicalize the mount path for consistent storage
        let canonical_mountpoint = std::fs::canonicalize(&mounted_mp).map_err(|e| {
            format!(
                "Failed to canonicalize mountpoint {}: {}",
                mounted_mp.display(),
                e
            )
        })?;

        // Check if already mounted
        if self.is_mounted(&mounted_mp.to_string_lossy())? {
            tracing::info!("Already mounted at {}", mounted_mp.display());
            return Ok(());
        }

        // Get the composefs file for this commit
        let commit_file = format!("{}/commit.cfs", self.commit_path(&cid));
        if !Path::new(&commit_file).exists() {
            return Err(format!("Commit file not found: {}", commit_file));
        }

        // Open the composefs image file
        let image_file = std::fs::File::open(&commit_file)
            .map_err(|e| format!("Failed to open composefs image {}: {}", commit_file, e))?;

        // Extract label for source name construction and worktree operations
        let label = match sref {
            StratumRef::Worktree { label, .. } => label,
            _ => &sref.to_string(),
        };

        match worktree {
            Some(worktree_name) => {
                // Writable mount with worktree upperdir
                tracing::debug!(
                    "Mounting ref {:?} (commit: {}) at {} with worktree: {}",
                    sref,
                    cid,
                    mounted_mp.display(),
                    worktree_name
                );

                // Ensure the worktree exists
                if !self.worktree_exists(label, worktree_name) {
                    return Err(format!(
                        "Worktree {}:{} does not exist",
                        label, worktree_name
                    ));
                }

                let source_name = format!("stratum:{}+{}", label, worktree_name);

                // Create composefs configuration with worktree upperdir
                let upperdir = self.worktree_upperdir(label, worktree_name);
                let workdir = self.worktree_workdir(label, worktree_name);
                let config = crate::mount::composefs::ComposeFsConfig::writable(
                    image_file.into(),
                    source_name.clone(),
                    std::path::PathBuf::from(upperdir),
                    Some(std::path::PathBuf::from(workdir)),
                );

                let config = config
                    .with_basedir(std::path::PathBuf::from(self.objects_path()))
                    .with_source_name(source_name);

                // Mount using native implementation
                tracing::debug!("Mounting writable composefs at {}", mounted_mp.display());
                crate::mount::composefs::mount_composefs_persistent_at(&config, &mounted_mp)
                    .map_err(|e| format!("Failed to mount composefs: {}", e))?;

                // Update state manager with mount information using canonical path
                let mounted_stratum = crate::state::MountedStratum {
                    stratum_ref: crate::state::StratumMountRef::Worktree {
                        label: sref.to_string(),
                        worktree: worktree_name.to_string(),
                    },
                    mount_point: canonical_mountpoint.clone(),
                    read_only: false, // Worktrees are always writable
                    // Base commit of the worktree, useful for safety checks
                    base_commit: cid.clone(),
                };
                self.state_manager
                    .add_mount(canonical_mountpoint.clone(), mounted_stratum)?;

                tracing::info!(
                    "Successfully mounted {} at {:?} using native implementation (worktree: {})",
                    cid,
                    mounted_mp,
                    worktree_name
                );
            }
            None => {
                // Read-only mount without worktree
                tracing::debug!(
                    "Mounting ref {:?} (commit: {}) at {:?} as read-only",
                    sref,
                    cid,
                    mounted_mp
                );

                let source_name = format!("stratum:{}", sref);

                // Create read-only composefs configuration
                let config = crate::mount::composefs::ComposeFsConfig::read_only(
                    image_file.into(),
                    source_name.clone(),
                );

                let config = config
                    .with_basedir(std::path::PathBuf::from(self.objects_path()))
                    .with_source_name(source_name);

                // Mount using native implementation
                tracing::debug!("Mounting read-only composefs at {:?}", mounted_mp);
                crate::mount::composefs::mount_composefs_persistent_at(&config, &mounted_mp)
                    .map_err(|e| format!("Failed to mount composefs: {}", e))?;

                // Update state manager with mount information using canonical path
                let mounted_stratum = crate::state::MountedStratum {
                    stratum_ref: crate::state::StratumMountRef::Snapshot(sref.clone()),
                    mount_point: canonical_mountpoint.clone(),
                    base_commit: cid.clone(),
                    read_only: true, // Read-only snapshots
                };
                self.state_manager
                    .add_mount(canonical_mountpoint, mounted_stratum)?;

                tracing::info!(
                    "Successfully mounted {} at {:?} using native implementation (read-only)",
                    cid,
                    mounted_mp
                );
            }
        }

        Ok(())
    }

    /// Temporarily mounts a stratum commit at a mountpoint using an ephemeral mount.
    /// Returns a [`crate::mount::FsHandle`] for the mounted filesystem, which are
    /// automatically unmounted when dropped from memory.
    ///
    /// # Safety
    ///
    /// You may use [`std::mem::forget`] to prevent the handle from unmounting the filesystem,
    /// but at that point you should use [`Self::mount_ref`] instead, as it is safer and tracks states
    /// inside [`crate::state::StateManager`]
    pub fn mount_ref_ephemeral(
        &self,
        sref: &StratumRef,
        mountpoint: &str,
    ) -> Result<crate::mount::FsHandle, String> {
        // Deny worktree mounts for ephemeral mounts
        if let StratumRef::Worktree { .. } = sref {
            return Err("Ephemeral mounts do not support worktrees".to_string());
        }

        // Create mountpoint if it doesn't exist
        std::fs::create_dir_all(mountpoint)
            .map_err(|e| format!("Failed to create mountpoint {}: {}", mountpoint, e))?;

        // Get the commit ID for the stratum reference
        let cid = sref
            .resolve_commit_id(self)
            .map_err(|e| format!("Failed to resolve commit ID: {}", e))?;

        // Get the composefs file for this commit
        let commit_file = format!("{}/commit.cfs", self.commit_path(&cid));
        if !Path::new(&commit_file).exists() {
            return Err(format!("Commit file not found: {}", commit_file));
        }

        // Open the composefs image file
        let image_file = std::fs::File::open(&commit_file)
            .map_err(|e| format!("Failed to open composefs image {}: {}", commit_file, e))?;

        // Create composefs configuration for ephemeral mount
        let config = crate::mount::composefs::ComposeFsConfig::read_only(
            image_file.into(),
            sref.to_string(),
        );

        tracing::trace!("Creating mount config for stratum ref: {}", sref);
        let config = config
            .with_basedir(std::path::PathBuf::from(self.objects_path()))
            .with_source_name(sref.to_string());

        // Mount using native implementation
        tracing::debug!("Mounting read-only composefs at {}", mountpoint);
        let handle = crate::mount::composefs::mount_composefs_at(&config, Path::new(mountpoint))
            .map_err(|e| format!("Failed to mount composefs: {}", e))?;

        Ok(handle)
    }

    /// Check if a path is already mounted
    fn is_mounted(&self, path: &str) -> Result<bool, String> {
        let mounts = std::fs::read_to_string("/proc/mounts")
            .map_err(|e| format!("Failed to read /proc/mounts: {}", e))?;

        let canonical_path = std::fs::canonicalize(path)
            .map_err(|e| format!("Failed to canonicalize path {}: {}", path, e))?;

        for line in mounts.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let mount_point = parts[1];
                if mount_point == canonical_path.to_string_lossy() {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// List all ref names in the store
    pub fn list_all_refs(&self) -> Result<Vec<String>, String> {
        let refs_path = format!("{}/{}", self.base_path, Self::REFS_DIR);

        if !Path::new(&refs_path).exists() {
            return Ok(Vec::new());
        }

        let entries = std::fs::read_dir(&refs_path).map_err(|e| e.to_string())?;

        let mut refs = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();

            if path.is_dir() {
                refs.push(entry.file_name().to_string_lossy().to_string());
            }
        }

        refs.sort();
        Ok(refs)
    }

    /// Unmount a composefs mount using native Rust implementation
    pub fn unmount_ref(&self, mountpoint: &str) -> Result<(), String> {
        // Canonicalize the mount path for consistent storage and comparison
        let canonical_mountpoint = std::fs::canonicalize(mountpoint)
            .map_err(|e| format!("Failed to canonicalize mountpoint {}: {}", mountpoint, e))?;

        if !self.is_mounted(&canonical_mountpoint.to_string_lossy())? {
            tracing::info!("Not mounted at {}", canonical_mountpoint.display());
            return Ok(());
        }

        // Safety check: verify the mount is registered in state manager
        let state = self.state_manager.load_state()?;

        if !state.mounts.contains_key(&canonical_mountpoint) {
            tracing::warn!(
                "Mount at {} not found in state manager, but appears to be mounted",
                canonical_mountpoint.display()
            );
            return Err(format!(
                "Mount at {} is not managed by stratum",
                canonical_mountpoint.display()
            ));
        }

        // Use native Rust composefs unmounting implementation
        crate::mount::composefs::unmount_composefs_at(&canonical_mountpoint)
            .map_err(|e| format!("Failed to unmount composefs: {}", e))?;

        // Remove mount from state manager
        self.state_manager.remove_mount(&canonical_mountpoint)?;

        tracing::info!("Successfully unmounted {}", canonical_mountpoint.display());
        Ok(())
    }

    /// Register objects in the object database for a commit
    #[tracing::instrument(skip(self, commit_id, commit_file), level = "trace")]
    pub fn register_objects(&self, commit_id: &str, commit_file: &str) -> Result<(), String> {
        tracing::debug!(
            "Registering objects for commit {} from file {}",
            commit_id,
            commit_file
        );

        // Get the list of objects in the commit file
        let objects = self.composefs_info_objects(commit_file)?;

        // Register each object in the object database
        for object_id in objects {
            self.register_object(commit_id, &object_id)?;
        }

        Ok(())
    }

    pub fn delete_commit(&self, commit_id: &str) -> Result<(), String> {
        // todo: safety check if commit is still mounted
        if self.state_manager.get_commit_mounted(commit_id)? {
            return Err(format!(
                "Cannot delete commit {}: it is currently mounted",
                commit_id
            ));
        }

        tracing::debug!("Deleting commit {}", commit_id);

        // Unregister all objects for this commit (don't fail if objects aren't registered)
        if let Err(e) = self.unregister_objects(commit_id) {
            tracing::warn!(
                "Failed to unregister objects for commit {}: {}",
                commit_id,
                e
            );
        }

        // Remove the commit directory
        let commit_path = self.commit_path(commit_id);
        if Path::new(&commit_path).exists() {
            std::fs::remove_dir_all(&commit_path)
                .map_err(|e| format!("Failed to delete commit directory {}: {}", commit_path, e))?;
        } else {
            tracing::warn!("Commit directory {} does not exist", commit_path);
        }

        Ok(())
    }

    /// Register a single object in the object database
    #[tracing::instrument(skip(self, commit_id, object_id), level = "trace")]
    pub fn register_object(&self, commit_id: &str, object_id: &str) -> Result<(), String> {
        // Register a single object in the object database

        tracing::debug!("Registering object {} for commit {}", object_id, commit_id);
        let object_file_meta =
            std::fs::metadata(Path::new(&self.objects_path()).join(object_id))
                .map_err(|e| format!("Failed to get metadata for object {}: {}", object_id, e))?;
        self.object_database
            .register_object(object_id, object_file_meta.len(), Some(commit_id));
        Ok(())
    }

    /// Unregister a single object from the object database
    #[tracing::instrument(skip(self, object_id, commit_id), level = "trace")]
    pub fn unregister_object(&self, object_id: &str, commit_id: &str) -> Result<(), String> {
        tracing::debug!(
            "Unregistering object {} for commit {}",
            object_id,
            commit_id
        );
        self.object_database
            .unregister_object(object_id, commit_id)
            .map_err(|e| format!("Failed to unregister object {}: {}", object_id, e))?;
        Ok(())
    }

    /// Unregister all objects for a commit from the object database
    #[tracing::instrument(skip(self, commit_id), level = "trace")]
    pub fn unregister_objects(&self, commit_id: &str) -> Result<(), String> {
        tracing::debug!("Unregistering all objects for commit {}", commit_id);

        let commit_file = format!("{}/commit.cfs", self.commit_path(commit_id));
        if !Path::new(&commit_file).exists() {
            return Err(format!("Commit file not found: {}", commit_file));
        }

        // Get the list of objects in the commit file
        let objects = self.composefs_info_objects(&commit_file)?;

        // Unregister each object from the object database
        for object_id in objects {
            self.unregister_object(&object_id, commit_id)?;
        }

        Ok(())
    }

    /// Import a directory and create a content-addressed commit
    ///
    /// Optionally has a `parent_commit` field that specifies
    /// how this commit was built from. Does not do anything other than metadata.
    /// See [`Self::union_patch_commit`] for actually merging commits
    ///
    /// # Arguments
    /// * `label` - The label (namespace) for this commit
    /// * `dir_path` - Path to directory to import
    /// * `parent_commit` - Optional parent commit for layered/bare imports
    ///
    /// # Returns
    /// Returns the commit ID (metadata hash)
    pub fn commit_directory_bare(
        &self,
        label: &str,
        dir_path: &str,
        parent_commit: Option<&str>,
        transient: bool,
    ) -> Result<String, String> {
        // Ensure the base path exists
        std::fs::create_dir_all(&self.base_path).map_err(|e| e.to_string())?;

        // Collect file data for merkle tree
        let file_chunks = chunks::collect_file_chunks(dir_path)?;

        // Generate merkle root for cryptographic verification
        let file_refs: Vec<&[u8]> = file_chunks.iter().map(|v| v.as_slice()).collect();
        let merkle_root = crate::util::build_merkle_root(&file_refs);

        // Generate fast metadata hash (this becomes the commit ID)
        let metadata_hash = crate::util::hash_directory_tree(Path::new(dir_path))
            .map_err(|e| format!("Failed to hash directory: {}", e))?;
        let commit_id = hex::encode(metadata_hash);

        // todo: if commit has a parent (that means it's not a base commit)
        // Create a new commit by:
        // - creating ephemeral overlayfs mount with the bare import + RO copy of parent commit
        //   as lowerdirs
        // - build composefs commit out of that
        tracing::debug!("Importing directory for label: {}", label);
        tracing::debug!("Commit ID (metadata hash): {}", commit_id);
        tracing::debug!("Merkle root: {}", hex::encode(merkle_root));
        if let Some(parent) = parent_commit {
            tracing::debug!("Parent commit: {}", parent);
        }

        // Create commit directory using commit ID
        let commit_path = self.commit_path(&commit_id);
        std::fs::create_dir_all(&commit_path).map_err(|e| e.to_string())?;

        // Sync the commit directory to ensure it's written to disk
        if let Some(parent) = Path::new(&commit_path).parent() {
            if let Err(e) = fsync_all_walk(parent) {
                tracing::warn!(
                    "Failed to fsync commit parent directory {}: {}",
                    parent.display(),
                    e
                );
            }
        }

        // Create the commit object
        let commit = crate::commit::Commit {
            commit: crate::commit::CommitInfo {
                merkle_root: hex::encode(merkle_root),
                metadata_hash: commit_id.clone(),
                timestamp: chrono::Utc::now(),
                parent_commit: parent_commit.map(|s| s.to_string()),
            },
            files: crate::commit::FileStats {
                count: file_chunks.len() as u64,
                total_size: calculate_total_size(dir_path)?,
            },
            merkle: crate::commit::MerkleInfo {
                leaf_count: file_chunks.len(),
                tree_depth: if file_chunks.is_empty() {
                    0
                } else {
                    (file_chunks.len() as f64).log2().ceil() as u32
                },
            },
        };

        // Create composefs file in commit directory
        let file = self.create_composefs_file(&commit_id, dir_path)?;

        if !transient {
            self.register_objects(&commit_id, &file)?;
        }

        // Store commit metadata
        self.store_commit(&commit_id, &commit)?;

        // Sync the commit directory after writing metadata
        if let Err(e) = fsync_all_walk(Path::new(&commit_path)) {
            tracing::warn!("Failed to fsync commit directory {}: {}", commit_path, e);
        }
        // Register objects in the object database

        // Ensure ref directory exists
        std::fs::create_dir_all(self.ref_path(label)).map_err(|e| e.to_string())?;

        // Sync the ref directory to ensure it's written to disk
        if let Err(e) = fsync_all_walk(Path::new(&self.ref_path(label))) {
            tracing::warn!(
                "Failed to fsync ref directory {}: {}",
                self.ref_path(label),
                e
            );
        }

        Ok(commit_id)
    }

    /// Tag a commit with a human-readable name using symlinks
    pub fn tag_commit(&self, label: &str, commit_id: &str, tag: &str) -> Result<(), String> {
        // Verify commit exists
        if !self.commit_exists(commit_id) {
            return Err(format!("Commit {} does not exist", commit_id));
        }

        // Create tags directory
        let tags_path = self.tags_path(label);
        std::fs::create_dir_all(&tags_path).map_err(|e| e.to_string())?;

        // Create symlink pointing to commit directory
        let tag_symlink = format!("{}/{}", tags_path, tag);

        // Create relative path from tags directory to commits directory
        // tags path is: base_path/refs/label/tags
        // commits path is: base_path/commits/commit_id
        // so relative path is: ../../../commits/commit_id
        let relative_commit_path = format!("../../../commits/{}", commit_id);

        // Remove existing tag if it exists (handles both valid and broken symlinks)
        let _ = std::fs::remove_file(&tag_symlink);

        // Create relative symlink to commit directory
        std::os::unix::fs::symlink(&relative_commit_path, &tag_symlink)
            .map_err(|e| e.to_string())?;

        tracing::info!("Tagged commit {} as {}:{}", commit_id, label, tag);
        Ok(())
    }

    pub fn untag(&self, tag: &str, label: &str) -> Result<(), String> {
        let tag_symlink = format!("{}/{}", Self::TAGS_DIR, tag);

        if !Path::new(&tag_symlink).exists() {
            return Err(format!("Tag {}:{} does not exist", label, tag));
        }

        // Remove the symlink
        std::fs::remove_file(&tag_symlink)
            .map_err(|e| format!("Failed to remove tag symlink: {}", e))?;

        tracing::info!("Untagged {}:{}", label, tag);
        Ok(())
    }

    /// Resolve a tag to a commit ID using symlinks
    pub fn resolve_tag(&self, label: &str, tag: &str) -> Result<String, String> {
        let tag_symlink = format!("{}/{}", self.tags_path(label), tag);

        if !Path::new(&tag_symlink).exists() {
            return Err(format!("Tag {}:{} does not exist", label, tag));
        }

        // Read the symlink target (commit directory path)
        let target_path = std::fs::read_link(&tag_symlink)
            .map_err(|e| format!("Failed to read tag symlink: {}", e))?;

        // Extract commit ID from the path
        let commit_id = target_path
            .file_name()
            .ok_or("Invalid symlink target")?
            .to_string_lossy()
            .to_string();

        Ok(commit_id)
    }

    /// List all tags for a label (works with symlinks)
    pub fn list_tags(&self, label: &str) -> Result<Vec<String>, String> {
        let tags_path = self.tags_path(label);

        if !Path::new(&tags_path).exists() {
            return Ok(Vec::new());
        }

        let entries = std::fs::read_dir(&tags_path).map_err(|e| e.to_string())?;

        let mut tags = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| e.to_string())?;
            let file_type = entry.file_type().map_err(|e| e.to_string())?;
            // Check for symlinks (tags are now symlinks to commit directories)
            if file_type.is_symlink() {
                tags.push(entry.file_name().to_string_lossy().to_string());
            }
        }

        tags.sort();
        Ok(tags)
    } // === Worktree Management ===

    /// Create a new worktree based on a commit
    ///
    /// This creates the worktree directory structure and metadata file.
    /// The worktree allows mutable changes on top of the base commit.
    pub fn create_worktree(
        &self,
        label: &str,
        worktree_name: &str,
        base_commit: &str,
        description: Option<String>,
    ) -> Result<(), String> {
        // Verify the base commit exists
        if !self.commit_exists(base_commit) {
            return Err(format!("Base commit {} does not exist", base_commit));
        }

        // Check if worktree already exists
        let meta_path = self.worktree_meta_path(label, worktree_name);
        if Path::new(&meta_path).exists() {
            return Err(format!(
                "Worktree {}:{} already exists",
                label, worktree_name
            ));
        }

        // Create worktree directories
        let upperdir = self.worktree_upperdir(label, worktree_name);
        let workdir = self.worktree_workdir(label, worktree_name);

        std::fs::create_dir_all(&upperdir)
            .map_err(|e| format!("Failed to create upperdir {}: {}", upperdir, e))?;

        std::fs::create_dir_all(&workdir)
            .map_err(|e| format!("Failed to create workdir {}: {}", workdir, e))?;

        // Sync the worktree directories to ensure they're written to disk
        if let Some(parent) = Path::new(&upperdir).parent() {
            if let Err(e) = fsync_all_walk(parent) {
                tracing::warn!(
                    "Failed to fsync worktree parent directory {}: {}",
                    parent.display(),
                    e
                );
            }
        }

        // Create worktree metadata
        let worktree = crate::commit::Worktree::new(
            worktree_name.to_string(),
            base_commit.to_string(),
            description,
        );

        // Save worktree metadata
        self.save_worktree_metadata(label, &worktree)?;

        // Sync the worktree metadata directory
        if let Some(parent) = Path::new(&self.worktree_meta_path(label, worktree_name)).parent() {
            if let Err(e) = fsync_all_walk(parent) {
                tracing::warn!(
                    "Failed to fsync worktree metadata directory {}: {}",
                    parent.display(),
                    e
                );
            }
        }

        tracing::info!(
            "Created worktree {}:{} based on commit {}",
            label,
            worktree_name,
            base_commit
        );

        Ok(())
    }

    /// Check if a worktree exists
    pub fn worktree_exists(&self, label: &str, worktree_name: &str) -> bool {
        Path::new(&self.worktree_meta_path(label, worktree_name)).exists()
    }

    /// List all worktrees for a label
    pub fn list_worktrees(&self, label: &str) -> Result<Vec<(String, Worktree)>, String> {
        let worktrees_path = self.worktrees_path(label);

        if !Path::new(&worktrees_path).exists() {
            return Ok(Vec::new());
        }

        let entries = std::fs::read_dir(&worktrees_path).map_err(|e| e.to_string())?;

        let mut worktrees = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();

            if path.is_dir() {
                let worktree_name = entry.file_name().to_string_lossy().to_string();
                // Verify it has a meta.toml file to confirm it's a valid worktree
                if Path::new(&self.worktree_meta_path(label, &worktree_name)).exists() {
                    let worktree = self.load_worktree(label, &worktree_name)?;
                    worktrees.push((format!("{}+{}", label, worktree.name()), worktree));
                }
            }
        }

        worktrees.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(worktrees)
    }

    /// List all worktrees across all labels in the store
    pub fn list_all_worktrees(&self) -> Result<Vec<(String, Worktree)>, String> {
        let mut all_worktrees = Vec::new();
        let labels = self.list_all_refs()?;

        for label in labels {
            let worktrees = self.list_worktrees(&label)?;
            all_worktrees.extend(worktrees);
        }

        all_worktrees.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(all_worktrees)
    }

    /// Load worktree metadata from storage
    pub fn load_worktree(
        &self,
        label: &str,
        worktree_name: &str,
    ) -> Result<crate::commit::Worktree, String> {
        let meta_path = self.worktree_meta_path(label, worktree_name);

        if !Path::new(&meta_path).exists() {
            return Err(format!(
                "Worktree {}:{} does not exist",
                label, worktree_name
            ));
        }

        let toml_content = std::fs::read_to_string(&meta_path)
            .map_err(|e| format!("Failed to read worktree metadata: {}", e))?;

        let worktree: crate::commit::Worktree = toml::from_str(&toml_content)
            .map_err(|e| format!("Failed to parse worktree metadata: {}", e))?;

        Ok(worktree)
    }

    /// Save worktree metadata to storage
    pub fn save_worktree_metadata(
        &self,
        label: &str,
        worktree: &crate::commit::Worktree,
    ) -> Result<(), String> {
        let meta_path = self.worktree_meta_path(label, worktree.name());
        let toml_content = toml::to_string(worktree).map_err(|e| e.to_string())?;

        // Ensure parent directory exists
        if let Some(parent) = Path::new(&meta_path).parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }

        std::fs::write(&meta_path, toml_content).map_err(|e| e.to_string())?;

        // Sync the parent directory to ensure the metadata file is written to disk
        if let Some(parent) = Path::new(&meta_path).parent() {
            if let Err(e) = fsync_all_walk(parent) {
                tracing::warn!(
                    "Failed to fsync worktree metadata parent directory {}: {}",
                    parent.display(),
                    e
                );
            }
        }

        tracing::debug!("Saved worktree metadata at: {}", meta_path);
        Ok(())
    }

    /// Remove a worktree (must be unmounted first)
    pub fn remove_worktree(&self, label: &str, worktree_name: &str) -> Result<(), String> {
        // Check if worktree exists
        if !self.worktree_exists(label, worktree_name) {
            return Err(format!(
                "Worktree {}:{} does not exist",
                label, worktree_name
            ));
        }

        // Check if worktree is currently mounted using state manager
        if self.is_worktree_mounted(label, worktree_name)? {
            return Err(format!(
                "Worktree {}:{} is currently mounted. Unmount it first.",
                label, worktree_name
            ));
        }

        // Remove the entire worktree directory
        let worktree_path = self.worktree_path(label, worktree_name);
        std::fs::remove_dir_all(&worktree_path).map_err(|e| {
            format!(
                "Failed to remove worktree directory {}: {}",
                worktree_path, e
            )
        })?;

        tracing::info!("Removed worktree {}:{}", label, worktree_name);
        Ok(())
    }

    /// Update state manager with mount information after unmounting
    pub fn remove_mount_from_state(&self, mountpoint: &str) -> Result<(), String> {
        self.state_manager.remove_mount(Path::new(mountpoint))?;
        Ok(())
    }

    /// Check if a worktree is currently mounted using state manager
    pub fn is_worktree_mounted(&self, label: &str, worktree_name: &str) -> Result<bool, String> {
        self.state_manager.is_worktree_mounted(label, worktree_name)
    }

    /// Get the mount path for a worktree from state manager
    pub fn get_worktree_mount_path(
        &self,
        label: &str,
        worktree_name: &str,
    ) -> Result<Option<PathBuf>, String> {
        self.state_manager
            .find_mount_by_worktree(label, worktree_name)
    }

    /// Mark a worktree as committed and save metadata
    pub fn mark_worktree_committed(&self, label: &str, worktree_name: &str) -> Result<(), String> {
        let mut worktree = self.load_worktree(label, worktree_name)?;
        worktree.mark_committed();
        self.save_worktree_metadata(label, &worktree)?;
        Ok(())
    }

    /// Check if a worktree has uncommitted changes
    pub fn worktree_has_changes(&self, label: &str, worktree_name: &str) -> Result<bool, String> {
        let worktree = self.load_worktree(label, worktree_name)?;
        let upperdir_string = self.worktree_upperdir(label, worktree_name);
        let upperdir_path = Path::new(&upperdir_string);
        Ok(worktree.has_uncommitted_changes(upperdir_path))
    }

    /// Get the worktree name for a mount point using the state manager
    pub fn find_worktree_by_mount(
        &self,
        label: &str,
        mount_path: &str,
    ) -> Result<Option<String>, String> {
        // Use the state manager to find which worktree is mounted at this path
        let state = self.state_manager.load_state()?;

        for (mount_point, mounted_stratum) in state.mounts.iter() {
            if mount_point.to_string_lossy() == mount_path {
                if let crate::state::StratumMountRef::Worktree {
                    label: mount_label,
                    worktree,
                } = &mounted_stratum.stratum_ref
                {
                    if mount_label == label {
                        return Ok(Some(worktree.clone()));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Rebase a worktree to a new base commit
    pub fn rebase_worktree(
        &self,
        label: &str,
        worktree_name: &str,
        new_base_commit: &StratumRef,
    ) -> Result<(), String> {
        // Load the existing worktree metadata
        let mut current_worktree = self.load_worktree(label, worktree_name)?;

        let new_base = match new_base_commit {
            StratumRef::Tag(id) => self.resolve_tag(label, id)?,
            StratumRef::Commit(id) => id.clone(),
            StratumRef::Worktree { label, worktree } => {
                tracing::warn!(
                    "Worktree is selected as rebase target, will use that worktree's base commit instead"
                );

                let loaded_worktree = self.load_worktree(label, worktree)?;

                if current_worktree == loaded_worktree {
                    return Err("cannot rebase a worktree onto itself".to_string());
                }
                loaded_worktree.base_commit().to_string()
            }
        };

        // Check if worktree is being mounted, if so remember the mount point and unmount it
        let mut mount_path = None;
        if self.is_worktree_mounted(label, worktree_name)? {
            // Get the mount path before unmounting
            mount_path = self.get_worktree_mount_path(label, worktree_name)?;

            if let Some(path) = &mount_path {
                tracing::info!(
                    "Worktree {}:{} is currently mounted at {}, will unmount and remount after rebase",
                    label,
                    worktree_name,
                    path.display()
                );

                // Unmount the worktree
                self.unmount_ref(&path.to_string_lossy())?;
            }
        }

        // Verify the new base commit exists
        if !self.commit_exists(&new_base) {
            return Err(format!("New base commit {} does not exist", new_base));
        }

        // Update the worktree's base commit
        current_worktree.set_base_commit(new_base.to_string());

        self.save_worktree_metadata(label, &current_worktree)?;
        tracing::info!(
            "Rebased worktree {}:{} to new base commit {}",
            label,
            worktree_name,
            new_base
        );

        // Remount the worktree if it was mounted before
        if let Some(path) = mount_path {
            tracing::info!(
                "Remounting worktree {}:{} at {}",
                label,
                worktree_name,
                path.display()
            );

            // Create a StratumRef for the worktree
            let sref = StratumRef::Worktree {
                label: label.to_string(),
                worktree: worktree_name.to_string(),
            };

            // Remount the worktree at the same path
            self.mount_ref(&sref, &path.to_string_lossy(), Some(worktree_name))?;
        }

        Ok(())
    }

    // == End Worktree Management ==

    /*
       /// Import a bare directory as a new commit on top of an existing base commit
       ///
       /// This is used for creating a union commit that layers a new directory.
       /// Or merging multiple commits into a new one.
       pub fn union_patch_commit_old(
           &self,
           label: &str,
           dir_path: &str, // the upperdir to patch on top
           base_commit: &str,
           transient: bool,
       ) -> Result<String, String> {
           // Verify the base commit exists
           if !self.commit_exists(base_commit) {
               return Err(format!("Base commit {} does not exist", base_commit));
           }

           tracing::info!(
               "Creating union patch commit for label: {}, base commit: {}, from dir: {}",
               label,
               base_commit,
               dir_path
           );

           // Try to create temporary directories on the same filesystem as the source directory
           // This avoids copying large files into RAM
           let dir_path_buf = Path::new(dir_path);
           let parent_dir = dir_path_buf.parent().unwrap_or(Path::new("/tmp"));
           // remind to self: don't put this tempfile inside the scope of tempdir_in
           // because it will be dropped before the overlayfs mount is created

           let ovl_mountpoint = tempfile::tempdir()
               .map_err(|e| format!("Failed to create temporary overlayfs mount: {}", e))?;

           // First try creating temporary directory adjacent to the source directory
           let (_ovl_tmp, temp_upperdir, overlayfs_workdir, overlayfs_mountpoint) =
               match tempfile::tempdir_in(parent_dir) {
                   Ok(tmp_dir) => {
                       tracing::debug!(
                           "Created temporary directory on the same filesystem as the source: {}",
                           tmp_dir.path().display()
                       );

                       // Create just the necessary subdirectories
                       let mount_point = ovl_mountpoint.path().to_path_buf();
                       let work_dir = tmp_dir.path().join("workdir");

                       std::fs::create_dir_all(&mount_point)
                           .map_err(|e| format!("Failed to create overlayfs mountpoint: {}", e))?;
                       std::fs::create_dir_all(&work_dir)
                           .map_err(|e| format!("Failed to create overlayfs workdir: {}", e))?;

                       // Sync the parent directories to ensure they're written to disk
                       if let Some(parent) = mount_point.parent() {
                           if let Err(e) = fsync_all_walk(parent) {
                               tracing::warn!(
                                   "Failed to fsync overlayfs mount parent directory {}: {}",
                                   parent.display(),
                                   e
                               );
                           }
                       }
                       if let Some(parent) = work_dir.parent() {
                           if let Err(e) = fsync_all_walk(parent) {
                               tracing::warn!(
                                   "Failed to fsync overlayfs work parent directory {}: {}",
                                   parent.display(),
                                   e
                               );
                           }
                       }

                       // Use the original directory as the upperdir directly
                       // without creating any extra directories or copying files
                       tracing::debug!("Using original directory as upperdir: {}", dir_path);
                       (tmp_dir, dir_path_buf.to_path_buf(), work_dir, mount_point)
                   }
                   Err(e) => {
                       // Fall back to the original approach with copying if creating tempdir in parent fails
                       tracing::warn!(
                           "Failed to create temporary directory adjacent to source, falling back to system temp: {}",
                           e
                       );

                       let tmp_dir = tempfile::tempdir().map_err(|e| {
                           format!("Failed to create temporary overlayfs mount: {}", e)
                       })?;

                       let mount_point = tmp_dir.path().join("overlayfs_mount");
                       let upper_dir = tmp_dir.path().join("upperdir");
                       let work_dir = tmp_dir.path().join("workdir");

                       std::fs::create_dir_all(&mount_point)
                           .map_err(|e| format!("Failed to create overlayfs mountpoint: {}", e))?;
                       std::fs::create_dir_all(&upper_dir)
                           .map_err(|e| format!("Failed to create temporary upperdir: {}", e))?;
                       std::fs::create_dir_all(&work_dir)
                           .map_err(|e| format!("Failed to create overlayfs workdir: {}", e))?;

                       // Sync the parent directories to ensure they're written to disk
                       if let Some(parent) = mount_point.parent() {
                           if let Err(e) = fsync_all_walk(parent) {
                               tracing::warn!(
                                   "Failed to fsync fallback overlayfs mount parent directory {}: {}",
                                   parent.display(),
                                   e
                               );
                           }
                       }
                       if let Some(parent) = upper_dir.parent() {
                           if let Err(e) = fsync_all_walk(parent) {
                               tracing::warn!(
                                   "Failed to fsync fallback overlayfs upper parent directory {}: {}",
                                   parent.display(),
                                   e
                               );
                           }
                       }
                       if let Some(parent) = work_dir.parent() {
                           if let Err(e) = fsync_all_walk(parent) {
                               tracing::warn!(
                                   "Failed to fsync fallback overlayfs work parent directory {}: {}",
                                   parent.display(),
                                   e
                               );
                           }
                       }

                       // Need to copy in this case since we're on different filesystems
                       tracing::debug!(
                           "Copying {} to temporary upperdir {}",
                           dir_path,
                           upper_dir.display()
                       );
                       copy_dir_all(dir_path, &upper_dir)
                           .map_err(|e| format!("Failed to copy to temporary upperdir: {}", e))?;

                       // Sync the copied directory to ensure it's written to disk
                       if let Err(e) = fsync_all_walk(&upper_dir) {
                           tracing::warn!(
                               "Failed to fsync copied upperdir {}: {}",
                               upper_dir.display(),
                               e
                           );
                       }

                       (tmp_dir, upper_dir, work_dir, mount_point)
                   }
               };

           let commit_id = {
               let base_sref = StratumRef::Commit(base_commit.to_string());
               let basecommit_mountpoint = tempfile::tempdir().map_err(|e| {
                   format!("Failed to create temporary mountpoint for base commit: {e}")
               })?;

               tracing::debug!(
                   "Mounting base commit {} at {}",
                   base_commit,
                   basecommit_mountpoint.path().to_string_lossy()
               );
               // this will be dropped after the commit is created
               let _base_commit_mount = self
                   .mount_ref_ephemeral(&base_sref, &basecommit_mountpoint.path().to_string_lossy())
                   .map_err(|e| format!("Failed to mount base commit {base_commit}: {e}"))?;

               tracing::debug!(
                   "Creating overlayfs mount at {} with base commit {}",
                   overlayfs_mountpoint.to_string_lossy(),
                   base_commit
               );
               let ovl_mount = crate::mount::TempOvlMount::new(
                   overlayfs_mountpoint,
                   HashSet::from([basecommit_mountpoint.path().to_path_buf()]),
                   temp_upperdir,
                   Some(overlayfs_workdir),
               );

               ovl_mount
                   .mount()
                   .map_err(|e| format!("Failed to mount overlayfs: {}", e))?;

               // Import the overlayfs mountpoint as a new commit with the base as parent
               // This will capture both the base commit and the patched upperdir content
               let result = self.commit_directory_bare(
                   label,
                   &ovl_mount.get_mountpoint().to_string_lossy(),
                   Some(base_commit),
                   transient,
               );

               // Explicitly drop the ovl_mount to ensure it's unmounted before we return
               drop(ovl_mount);

               result?
           };

           tracing::info!("Imported bare directory on top of commit {}", base_commit);
           tracing::info!("New commit: {}", commit_id);

           Ok(commit_id)
       }
    */

    /// Create an optimized union patch commit using hash derivation
    ///
    /// This optimized version of union_patch_commit derives the new commit hash
    /// from the existing base commit instead of recalculating everything from scratch.
    /// This significantly improves performance for patchset operations.
    ///
    /// # Arguments
    /// * `label` - The label (namespace) for this commit
    /// * `dir_path` - Path to the patch directory (upperdir)
    /// * `base_commit` - The base commit to patch on top of
    /// * `transient` - Whether this is a transient commit
    ///
    /// # Returns
    /// Returns the new commit ID (derived metadata hash)
    pub fn union_patch_commit(
        &self,
        label: &str,
        dir_path: &str,
        base_commit: &str,
        transient: bool,
    ) -> Result<String, String> {
        // Verify the base commit exists
        if !self.commit_exists(base_commit) {
            return Err(format!("Base commit {} does not exist", base_commit));
        }

        tracing::info!(
            "Creating optimized union patch commit for label: {}, base commit: {}, from dir: {}",
            label,
            base_commit,
            dir_path
        );

        // Try to create temporary directories on the same filesystem as the source directory
        // This avoids copying large files into RAM
        let dir_path_buf = Path::new(dir_path);
        let parent_dir = dir_path_buf.parent().unwrap_or(Path::new("/tmp"));

        let ovl_mountpoint = tempfile::tempdir()
            .map_err(|e| format!("Failed to create temporary overlayfs mount: {}", e))?;

        // First try creating temporary directory adjacent to the source directory
        let (_ovl_tmp, temp_upperdir, overlayfs_workdir, overlayfs_mountpoint) =
            match tempfile::tempdir_in(parent_dir) {
                Ok(tmp_dir) => {
                    tracing::debug!(
                        "Created temporary directory on the same filesystem as the source: {}",
                        tmp_dir.path().display()
                    );

                    // Create just the necessary subdirectories
                    let mount_point = ovl_mountpoint.path().to_path_buf();
                    let work_dir = tmp_dir.path().join("workdir");

                    std::fs::create_dir_all(&mount_point)
                        .map_err(|e| format!("Failed to create overlayfs mountpoint: {}", e))?;
                    std::fs::create_dir_all(&work_dir)
                        .map_err(|e| format!("Failed to create overlayfs workdir: {}", e))?;

                    // Sync the parent directories to ensure they're written to disk
                    if let Some(parent) = mount_point.parent() {
                        if let Err(e) = fsync_all_walk(parent) {
                            tracing::warn!(
                                "Failed to fsync overlayfs mount parent directory {}: {}",
                                parent.display(),
                                e
                            );
                        }
                    }
                    if let Some(parent) = work_dir.parent() {
                        if let Err(e) = fsync_all_walk(parent) {
                            tracing::warn!(
                                "Failed to fsync overlayfs work parent directory {}: {}",
                                parent.display(),
                                e
                            );
                        }
                    }

                    // Use the original directory as the upperdir directly
                    // without creating any extra directories or copying files
                    tracing::debug!("Using original directory as upperdir: {}", dir_path);
                    (tmp_dir, dir_path_buf.to_path_buf(), work_dir, mount_point)
                }
                Err(e) => {
                    // Fall back to the original approach with copying if creating tempdir in parent fails
                    tracing::warn!(
                        "Failed to create temporary directory adjacent to source, falling back to system temp: {}",
                        e
                    );

                    let tmp_dir = tempfile::tempdir().map_err(|e| {
                        format!("Failed to create temporary overlayfs mount: {}", e)
                    })?;

                    let mount_point = tmp_dir.path().join("overlayfs_mount");
                    let upper_dir = tmp_dir.path().join("upperdir");
                    let work_dir = tmp_dir.path().join("workdir");

                    std::fs::create_dir_all(&mount_point)
                        .map_err(|e| format!("Failed to create overlayfs mountpoint: {}", e))?;
                    std::fs::create_dir_all(&upper_dir)
                        .map_err(|e| format!("Failed to create temporary upperdir: {}", e))?;
                    std::fs::create_dir_all(&work_dir)
                        .map_err(|e| format!("Failed to create overlayfs workdir: {}", e))?;

                    // Sync the parent directories to ensure they're written to disk
                    if let Some(parent) = mount_point.parent() {
                        if let Err(e) = fsync_all_walk(parent) {
                            tracing::warn!(
                                "Failed to fsync fallback overlayfs mount parent directory {}: {}",
                                parent.display(),
                                e
                            );
                        }
                    }
                    if let Some(parent) = upper_dir.parent() {
                        if let Err(e) = fsync_all_walk(parent) {
                            tracing::warn!(
                                "Failed to fsync fallback overlayfs upper parent directory {}: {}",
                                parent.display(),
                                e
                            );
                        }
                    }
                    if let Some(parent) = work_dir.parent() {
                        if let Err(e) = fsync_all_walk(parent) {
                            tracing::warn!(
                                "Failed to fsync fallback overlayfs work parent directory {}: {}",
                                parent.display(),
                                e
                            );
                        }
                    }

                    // Need to copy in this case since we're on different filesystems
                    tracing::debug!(
                        "Copying {} to temporary upperdir {}",
                        dir_path,
                        upper_dir.display()
                    );
                    copy_dir_all(dir_path, &upper_dir)
                        .map_err(|e| format!("Failed to copy to temporary upperdir: {}", e))?;

                    // Sync the copied directory to ensure it's written to disk
                    if let Err(e) = fsync_all_walk(&upper_dir) {
                        tracing::warn!(
                            "Failed to fsync copied upperdir {}: {}",
                            upper_dir.display(),
                            e
                        );
                    }

                    (tmp_dir, upper_dir, work_dir, mount_point)
                }
            };

        let commit_id = {
            let base_sref = StratumRef::Commit(base_commit.to_string());

            let basecommit_mountpoint = tempfile::tempdir().map_err(|e| {
                format!("Failed to create temporary mountpoint for base commit: {e}")
            })?;

            tracing::debug!(
                "Mounting base commit {} at {}",
                base_commit,
                basecommit_mountpoint.path().to_string_lossy()
            );
            // this will be dropped after the commit is created
            let _base_commit_mount = self
                .mount_ref_ephemeral(&base_sref, &basecommit_mountpoint.path().to_string_lossy())
                .map_err(|e| format!("Failed to mount base commit {base_commit}: {e}"))?;

            tracing::debug!(
                "Creating overlayfs mount at {} with base commit {}",
                overlayfs_mountpoint.to_string_lossy(),
                base_commit
            );
            let ovl_mount = crate::mount::TempOvlMount::new(
                overlayfs_mountpoint,
                HashSet::from([basecommit_mountpoint.path().to_path_buf()]),
                temp_upperdir,
                Some(overlayfs_workdir),
            );

            ovl_mount
                .mount()
                .map_err(|e| format!("Failed to mount overlayfs: {}", e))?;

            // OPTIMIZATION: Derive merkle root and file chunks from existing data
            // instead of reading all files from the combined mount
            let (combined_merkle_root, combined_file_chunks) =
                self.derive_combined_merkle_data(base_commit, dir_path)?;

            // Derive the new commit hash using our optimization
            let derived_commit_id = self.derive_commit_hash(
                base_commit,
                dir_path,
                combined_merkle_root,
                &combined_file_chunks,
            )?;

            // Create commit using the optimized path with pre-computed data
            let result = self.commit_from_existing_data(
                label,
                &ovl_mount.get_mountpoint().to_string_lossy(),
                derived_commit_id,
                combined_merkle_root,
                combined_file_chunks,
                Some(base_commit),
                transient,
            );

            // Explicitly drop the ovl_mount to ensure it's unmounted before we return
            drop(ovl_mount);

            result?
        };

        tracing::info!("Imported bare directory on top of commit {}", base_commit);
        tracing::info!("New commit: {}", commit_id);

        Ok(commit_id)
    }

    /// Derive combined merkle data from base commit and patch files with real content hashing
    ///
    /// This optimization hashes the real file content from the patch directory and combines
    /// those hashes with the base commit's existing hashes, avoiding reading all files from
    /// the combined overlayfs mount while maintaining cryptographic integrity.
    ///
    /// # Arguments
    /// * `base_commit` - The base commit ID
    /// * `patch_dir` - Path to the patch directory containing changed files
    ///
    /// # Returns
    /// Returns a tuple of (derived_merkle_root, combined_file_chunks)
    fn derive_combined_merkle_data(
        &self,
        base_commit: &str,
        patch_dir: &str,
    ) -> Result<([u8; 32], Vec<Vec<u8>>), String> {
        // Load base commit to get its existing data
        let base_commit_obj = self.load_commit(base_commit)?;
        let base_merkle_root = base_commit_obj
            .merkle_root_bytes()
            .map_err(|e| format!("Failed to decode base commit merkle root: {}", e))?;

        // Hash the actual patch files to get real content hashes
        let mut patch_chunks = Vec::new();
        crate::store::chunks::collect_chunks_recursive(Path::new(patch_dir), &mut patch_chunks)
            .map_err(|e| format!("Failed to collect patch file chunks: {}", e))?;

        tracing::debug!("Collected {} real patch file chunks", patch_chunks.len());

        // For the combined merkle root, we need to combine the base commit's
        // merkle root with the patch file hashes in a deterministic way
        use sha2::{Digest, Sha256};

        // Create a hash of all patch chunks to represent the patch's contribution
        let mut patch_hash = Sha256::new();
        patch_hash.update(b"PATCH_CHUNKS");
        for chunk in &patch_chunks {
            patch_hash.update(chunk);
        }
        let patch_contribution = patch_hash.finalize();

        // Derive new merkle root by combining base merkle root with patch contribution
        let mut combined_hasher = Sha256::new();
        combined_hasher.update(b"COMBINED_MERKLE_ROOT");
        combined_hasher.update(base_merkle_root);
        combined_hasher.update(patch_contribution);
        let derived_merkle_root_bytes = combined_hasher.finalize();

        let mut derived_merkle_root = [0u8; 32];
        derived_merkle_root.copy_from_slice(&derived_merkle_root_bytes);

        // The combined chunks include both base file count and real patch chunks
        // We use the base commit's file count to represent its contribution
        // and add the actual patch chunks for files that changed
        let base_chunk_count = base_commit_obj.files.count as usize;
        let patch_chunk_count = patch_chunks.len(); // Get count before moving
        let total_chunk_count = base_chunk_count + patch_chunk_count;

        let mut combined_chunks = Vec::with_capacity(total_chunk_count);

        // Add synthetic chunks to represent the base commit's unchanged files
        let base_hash = base_commit_obj.metadata_hash_bytes().unwrap_or([0u8; 32]);
        for i in 0..base_chunk_count {
            let mut chunk_hasher = Sha256::new();
            chunk_hasher.update(b"BASE_FILE_CHUNK");
            chunk_hasher.update(base_hash);
            chunk_hasher.update((i as u64).to_le_bytes());
            combined_chunks.push(chunk_hasher.finalize().to_vec());
        }

        // Add the real patch file chunks (these have actual file content hashes)
        combined_chunks.extend(patch_chunks);

        tracing::debug!(
            "Combined {} base chunks + {} patch chunks = {} total chunks",
            base_chunk_count,
            patch_chunk_count,
            total_chunk_count
        );

        Ok((derived_merkle_root, combined_chunks))
    }

    /// Derive a new commit hash from an existing base commit and real patch file chunks
    ///
    /// This optimization avoids recalculating the entire directory tree hash by
    /// combining the base commit hash with real patch file content hashes using
    /// a cryptographic derivation function.
    ///
    /// # Arguments
    /// * `base_commit` - The base commit ID
    /// * `patch_dir` - Path to the patch directory (for generating patch hash)
    /// * `combined_merkle_root` - Merkle root of the combined filesystem
    /// * `combined_file_chunks` - File chunks from the combined filesystem
    ///
    /// # Returns
    /// Returns the derived commit ID (hex-encoded hash)
    fn derive_commit_hash(
        &self,
        base_commit: &str,
        patch_dir: &str,
        combined_merkle_root: [u8; 32],
        combined_file_chunks: &[Vec<u8>],
    ) -> Result<String, String> {
        use sha2::{Digest, Sha256};

        // Load the base commit to get its metadata hash
        let base_commit_hash = if let Ok(base_commit_obj) = self.load_commit(base_commit) {
            base_commit_obj.metadata_hash_bytes().unwrap_or([0u8; 32])
        } else {
            // Fallback: decode the commit ID directly
            hex::decode(base_commit)
                .ok()
                .and_then(|bytes| {
                    if bytes.len() == 32 {
                        let mut array = [0u8; 32];
                        array.copy_from_slice(&bytes);
                        Some(array)
                    } else {
                        None
                    }
                })
                .unwrap_or([0u8; 32])
        };

        // Generate a hash of the patch directory for deterministic commit derivation
        let patch_metadata_hash = crate::util::hash_directory_tree(Path::new(patch_dir))
            .map_err(|e| format!("Failed to hash patch directory: {}", e))?;

        // Create a deterministic hash by combining:
        // 1. Base commit hash
        // 2. Patch metadata hash (directory structure + file metadata)
        // 3. Combined merkle root (includes real file content hashes)
        // 4. Number of file chunks (for structural integrity)
        let mut hasher = Sha256::new();

        hasher.update(b"DERIVED_COMMIT"); // Type marker
        hasher.update(base_commit_hash);
        hasher.update(patch_metadata_hash);
        hasher.update(combined_merkle_root);
        hasher.update((combined_file_chunks.len() as u64).to_le_bytes());

        let result = hasher.finalize();
        Ok(hex::encode(result))
    }

    /// Create a commit from pre-computed data without recalculating hashes
    ///
    /// This optimized function bypasses the normal commit creation process by using
    /// pre-computed data, significantly improving performance for union patch operations.
    ///
    /// # Arguments
    /// * `label` - The label (namespace) for this commit
    /// * `dir_path` - Path to the directory (for creating composefs file)
    /// * `commit_id` - Pre-computed commit ID
    /// * `merkle_root` - Pre-computed merkle root
    /// * `file_chunks` - Pre-computed file chunks
    /// * `parent_commit` - Optional parent commit
    /// * `transient` - Whether this is a transient commit
    ///
    /// # Returns
    /// Returns the commit ID
    pub fn commit_from_existing_data(
        &self,
        label: &str,
        dir_path: &str,
        commit_id: String,
        merkle_root: [u8; 32],
        file_chunks: Vec<Vec<u8>>,
        parent_commit: Option<&str>,
        transient: bool,
    ) -> Result<String, String> {
        tracing::debug!("Creating commit from existing data: {}", commit_id);
        tracing::debug!("Merkle root: {}", hex::encode(merkle_root));
        if let Some(parent) = parent_commit {
            tracing::debug!("Parent commit: {}", parent);
        }

        // Create commit directory using the provided commit ID
        let commit_path = self.commit_path(&commit_id);
        std::fs::create_dir_all(&commit_path).map_err(|e| e.to_string())?;

        // Sync the commit directory to ensure it's written to disk
        if let Some(parent) = Path::new(&commit_path).parent() {
            if let Err(e) = crate::util::fsync_all_walk(parent) {
                tracing::warn!(
                    "Failed to fsync commit parent directory {}: {}",
                    parent.display(),
                    e
                );
            }
        }

        // Calculate total size from the directory
        let total_size = crate::util::calculate_total_size(dir_path)?;

        // Create the commit object with pre-computed data
        let commit = crate::commit::Commit {
            commit: crate::commit::CommitInfo {
                merkle_root: hex::encode(merkle_root),
                metadata_hash: commit_id.clone(),
                timestamp: chrono::Utc::now(),
                parent_commit: parent_commit.map(|s| s.to_string()),
            },
            files: crate::commit::FileStats {
                count: file_chunks.len() as u64,
                total_size,
            },
            merkle: crate::commit::MerkleInfo {
                leaf_count: file_chunks.len(),
                tree_depth: if file_chunks.is_empty() {
                    0
                } else {
                    (file_chunks.len() as f64).log2().ceil() as u32
                },
            },
        };

        // Create composefs file in commit directory
        let file = self.create_composefs_file(&commit_id, dir_path)?;

        if !transient {
            self.register_objects(&commit_id, &file)?;
        }

        // Store commit metadata
        self.store_commit(&commit_id, &commit)?;

        // Sync the commit directory after writing metadata
        if let Err(e) = crate::util::fsync_all_walk(Path::new(&commit_path)) {
            tracing::warn!("Failed to fsync commit directory {}: {}", commit_path, e);
        }

        // Ensure ref directory exists
        std::fs::create_dir_all(self.ref_path(label)).map_err(|e| e.to_string())?;

        // Sync the ref directory to ensure it's written to disk
        if let Err(e) = crate::util::fsync_all_walk(Path::new(&self.ref_path(label))) {
            tracing::warn!(
                "Failed to fsync ref directory {}: {}",
                self.ref_path(label),
                e
            );
        }

        tracing::info!("Created optimized commit: {}", commit_id);
        Ok(commit_id)
    }

    /// Check if a commit exists
    fn commit_exists(&self, commit_id: &str) -> bool {
        Path::new(&self.commit_path(commit_id)).exists()
    }

    /// Store a commit object as TOML metadata
    fn store_commit(&self, commit_id: &str, commit: &crate::commit::Commit) -> Result<(), String> {
        let metadata_path = format!("{}/{}", self.commit_path(commit_id), Self::COMMIT_META_FILE);
        let toml_content = toml::to_string(commit).map_err(|e| e.to_string())?;
        std::fs::write(&metadata_path, toml_content).map_err(|e| e.to_string())?;
        tracing::debug!("Stored commit metadata at: {}", metadata_path);
        Ok(())
    }

    // -- composefs wrappers --
    // todo: port to native composefs-rs API

    /// Create composefs file for a commit
    #[tracing::instrument(skip_all)]
    fn create_composefs_file(&self, commit_id: &str, dir_path: &str) -> Result<String, String> {
        tracing::info!("Creating ComposeFS file for commit {}", commit_id);
        fsync_all_walk(
            &Path::new(dir_path).canonicalize().map_err(|e| {
                format!("Failed to canonicalize directory path {}: {}", dir_path, e)
            })?,
        )
        .map_err(|e| format!("Failed to fsync directory {}: {}", dir_path, e))?;
        let commit_file = format!("{}/{}", self.commit_path(commit_id), Self::COMMIT_FILE);

        let output = std::process::Command::new("mkcomposefs")
            .arg(format!("--digest-store={}", self.objects_path()))
            .arg(dir_path)
            .arg(&commit_file)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .map_err(|e| e.to_string())?;

        if !output.status.success() {
            return Err(format!(
                "mkcomposefs failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        // fsync_all_walk(Path::new(&commit_file))
        // .map_err(|e| format!("Failed to fsync composefs file {}: {}", commit_file, e))?;

        fsync_all_walk(Path::new(&self.objects_path()))
            .map_err(|e| format!("Failed to fsync composefs file {}: {}", commit_file, e))?;

        tracing::debug!("Created composefs file: {}", commit_file);
        Ok(commit_file)
    }

    /// Calls `composefs-info objects` to get lists of objects in a commit
    fn composefs_info_objects(&self, file: &str) -> Result<Vec<String>, String> {
        let output = std::process::Command::new("composefs-info")
            .arg("objects")
            .arg(file)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .map_err(|e| e.to_string())?;

        if !output.status.success() {
            return Err(format!(
                "composefs-info failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        let stdout = String::from_utf8(output.stdout).map_err(|e| e.to_string())?;
        let objects: Vec<String> = stdout
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    Some(trimmed.to_string())
                } else {
                    None
                }
            })
            .collect();
        tracing::debug!("Found {} objects in commit: {}", objects.len(), file);
        Ok(objects)
    }

    /// Calls `composefs-info missing-objects` to get lists of missing objects in a commit
    fn composefs_info_missing_objects(&self, file: &str) -> Result<Vec<String>, String> {
        let output = std::process::Command::new("composefs-info")
            .arg(format!("--basedir={}", self.objects_path()))
            .arg("missing-objects")
            .arg(file)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .map_err(|e| e.to_string())?;

        if !output.status.success() {
            return Err(format!(
                "composefs-info missing-objects failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        let stdout = String::from_utf8(output.stdout).map_err(|e| e.to_string())?;
        let objects: Vec<String> = stdout
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    Some(trimmed.to_string())
                } else {
                    None
                }
            })
            .collect();
        tracing::debug!(
            "Found {} missing objects in commit: {}",
            objects.len(),
            file
        );
        Ok(objects)
    }

    /// Verify objects in a commit using composefs-info and find missing objects
    pub fn verify_commit_objects(&self, commit_id: &str) -> Result<Vec<String>, String> {
        // Verify objects in a commit using composefs-info
        let commit_file = format!("{}/{}", self.commit_path(commit_id), Self::COMMIT_FILE);
        if !Path::new(&commit_file).exists() {
            return Err(format!("Commit file not found: {}", commit_file));
        }

        let missing_objects = self.composefs_info_missing_objects(&commit_file)?;
        if missing_objects.is_empty() {
            tracing::info!("All objects for commit {} are present", commit_id);
            Ok(vec![])
        } else {
            tracing::warn!(
                "Missing objects for commit {}: {}",
                commit_id,
                missing_objects.join(", ")
            );
            Ok(missing_objects)
        }
    }

    // -- end composefs wrappers --

    /// Load a commit from storage
    pub fn load_commit(&self, commit_id: &str) -> Result<crate::commit::Commit, String> {
        let metadata_path = format!("{}/metadata.toml", self.commit_path(commit_id));
        let toml_content = std::fs::read_to_string(&metadata_path)
            .map_err(|e| format!("Failed to read commit metadata: {}", e))?;

        let commit: crate::commit::Commit = toml::from_str(&toml_content)
            .map_err(|e| format!("Failed to parse commit metadata: {}", e))?;

        Ok(commit)
    }

    /// Verify a commit using its merkle root
    pub fn verify_commit(&self, commit_id: &str) -> Result<bool, String> {
        let commit = self.load_commit(commit_id)?;
        let stored_merkle_root = commit.merkle_root();

        // Check if commit exists and metadata is valid
        if !self.commit_exists(commit_id) || stored_merkle_root.is_empty() {
            return Ok(false);
        }

        // Verify all objects for this commit are present
        let missing_objects = self.verify_commit_objects(commit_id)?;
        if !missing_objects.is_empty() {
            tracing::warn!(
                "Commit {} has missing objects: {}",
                commit_id,
                missing_objects.join(", ")
            );
            return Ok(false);
        }

        // Recalculate merkle root from the original directory structure
        // Note: This would need the original directory or reconstructed from composefs
        // For now, just verify the commit exists, metadata is valid, and all objects are present
        Ok(true)
    }
}
