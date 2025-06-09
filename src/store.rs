//! Stratum store module
//!
//! This module provides a type for managing and storing a stratum store, including
//! functionality for loading and saving the store to disk.
//!
//! This is similar to composefs-rs' `Repository` type.

use crate::{commit::StratumRef, object::ObjectDatabase};
use composefs::repository::Repository as ComposeFSRepo;
use std::path::Path;

pub struct Store {
    /// The path to the store's base directory, so we know where the store actually is
    pub base_path: String,
    object_database: ObjectDatabase,
}

impl Store {
    const OBJECTS_DIR: &'static str = "objects";
    const COMMITS_DIR: &'static str = "commits";
    const REFS_DIR: &'static str = "refs";
    const TAGS_DIR: &'static str = "tags";
    const WORKTREES_DIR: &'static str = "worktrees";

    // Default worktree name
    const DEFAULT_WORKTREE: &'static str = "main";

    // Worktree subdirectories
    const UPPERDIR: &'static str = "upperdir";
    const WORKDIR: &'static str = "workdir";
    const WORKTREE_META_FILE: &'static str = "meta.toml";

    const METADATA_FILE: &'static str = "metadata.toml";
    const COMMIT_FILE: &'static str = "commit.cfs";

    pub fn new(base_path: String) -> Self {
        std::fs::create_dir_all(&base_path).ok();
        let object_database =
            ObjectDatabase::new(&base_path).expect("Failed to initialize object database");
        Store {
            base_path,
            object_database,
        }
    }

    pub fn base_path(&self) -> &str {
        &self.base_path
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
        format!("{}/{tag}/{}", self.ref_path(label), Self::METADATA_FILE)
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

    /// Mount a reference using native Rust composefs implementation
    ///
    /// # Arguments
    /// * `sref` - The stratum reference to mount
    /// * `mountpoint` - The path where to mount the filesystem
    /// * `worktree` - Optional worktree name. If provided, the mount will be associated with this worktree
    pub fn mount_ref(
        &self,
        sref: &StratumRef,
        mountpoint: &str,
        worktree: Option<&str>,
    ) -> Result<(), String> {
        let cid = sref
            .resolve_commit_id(self)
            .map_err(|e| format!("Failed to resolve commit ID: {}", e))?;

        let worktree_info = match worktree {
            Some(wt) => format!(" (worktree: {})", wt),
            None => String::new(),
        };

        tracing::debug!(
            "Mounting ref {:?} (commit: {}) at {}{}",
            sref,
            cid,
            mountpoint,
            worktree_info
        );

        // Get the composefs file for this commit
        let commit_file = format!("{}/commit.cfs", self.commit_path(&cid));
        if !Path::new(&commit_file).exists() {
            return Err(format!("Commit file not found: {}", commit_file));
        }

        // Create mountpoint if it doesn't exist
        std::fs::create_dir_all(mountpoint)
            .map_err(|e| format!("Failed to create mountpoint {}: {}", mountpoint, e))?;

        // Check if already mounted
        if self.is_mounted(mountpoint)? {
            tracing::info!("Already mounted at {}", mountpoint);
            return Ok(());
        }

        // Use native Rust composefs mounting implementation
        tracing::debug!("Using native Rust composefs mounting for {}", commit_file);

        // Open the composefs image file
        let image_file = std::fs::File::open(&commit_file)
            .map_err(|e| format!("Failed to open composefs image {}: {}", commit_file, e))?;

        let source_name = {
            if let Some(worktree) = worktree {
                format!("stratum:{}+{})", sref, worktree)
            } else {
                format!("stratum:{}", sref)
            }
        };

        // Create composefs configuration
        let config = if let Some(wt) = worktree {
            let upperdir = self.worktree_upperdir(&sref.to_string(), wt);
            let workdir = self.worktree_workdir(&sref.to_string(), wt);
            crate::mount::composefs::ComposeFsConfig::writable(
                image_file.into(),
                source_name.clone(),
                std::path::PathBuf::from(upperdir),
                Some(std::path::PathBuf::from(workdir)),
            )
        } else {
            crate::mount::composefs::ComposeFsConfig::read_only(
                image_file.into(),
                source_name.clone(),
            )
        };

        let config = config
            .with_basedir(std::path::PathBuf::from(self.objects_path()))
            .with_source_name(source_name);

        // Mount using native implementation
        tracing::debug!("Mounting composefs at {}", mountpoint);
        crate::mount::composefs::mount_composefs_persistent_at(&config, Path::new(mountpoint))
            .map_err(|e| format!("Failed to mount composefs: {}", e))?;

        tracing::info!(
            "Successfully mounted {} at {} using native implementation{}",
            cid,
            mountpoint,
            worktree_info
        );
        Ok(())
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

    /// Unmount a composefs mount using native Rust implementation
    pub fn unmount_ref(&self, mountpoint: &str) -> Result<(), String> {
        if !self.is_mounted(mountpoint)? {
            tracing::info!("Not mounted at {}", mountpoint);
            return Ok(());
        }

        // Use native Rust composefs unmounting implementation
        crate::mount::composefs::unmount_composefs_at(Path::new(mountpoint))
            .map_err(|e| format!("Failed to unmount composefs: {}", e))?;

        tracing::info!("Successfully unmounted {}", mountpoint);
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

    /// Import a directory and create a content-addressed commit
    ///
    /// # Arguments
    /// * `label` - The label (namespace) for this commit
    /// * `dir_path` - Path to directory to import
    /// * `parent_commit` - Optional parent commit for layered/bare imports
    ///
    /// # Returns
    /// Returns the commit ID (metadata hash)
    pub fn import_directory(
        &self,
        label: &str,
        dir_path: &str,
        parent_commit: Option<&str>,
    ) -> Result<String, String> {
        // Ensure the base path exists
        std::fs::create_dir_all(&self.base_path).map_err(|e| e.to_string())?;

        // Collect file data for merkle tree
        let file_chunks = self.collect_file_chunks(dir_path)?;

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
                total_size: self.calculate_total_size(dir_path)?,
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
        self.register_objects(&commit_id, &file)?;

        // Store commit metadata
        self.store_commit(&commit_id, &commit)?;
        // Register objects in the object database

        // Ensure ref directory exists
        std::fs::create_dir_all(self.ref_path(label)).map_err(|e| e.to_string())?;

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
    }

    // === Worktree Management ===
    // (Implementation to be added later)

    //

    /// Import a bare directory on top of an existing commit (for layering/patches)
    /// This is useful for applying deltas, patches, or mods on top of existing commits
    pub fn import_bare(
        &self,
        label: &str,
        dir_path: &str,
        base_commit: &str,
    ) -> Result<String, String> {
        // Verify the base commit exists
        if !self.commit_exists(base_commit) {
            return Err(format!("Base commit {} does not exist", base_commit));
        }

        // Import the bare directory as a new commit with the base as parent
        let commit_id = self.import_directory(label, dir_path, Some(base_commit))?;

        tracing::info!("Imported bare directory on top of commit {}", base_commit);
        tracing::info!("New commit: {}", commit_id);

        Ok(commit_id)
    }

    /// Collects file chunks from a directory for merkle tree construction
    fn collect_file_chunks(&self, dir_path: &str) -> Result<Vec<Vec<u8>>, String> {
        let mut chunks = Vec::new();
        let path = Path::new(dir_path);

        if !path.is_dir() {
            return Err("Path is not a directory".to_string());
        }

        self.collect_chunks_recursive(path, &mut chunks)?;

        // Sort by path to ensure consistent ordering
        chunks.sort();

        Ok(chunks)
    }

    /// Recursively collect file chunks from directory
    fn collect_chunks_recursive(
        &self,
        dir: &Path,
        chunks: &mut Vec<Vec<u8>>,
    ) -> Result<(), String> {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;

        // Sort for consistent ordering
        entries.sort_by_key(|entry| entry.file_name());

        for entry in entries {
            let path = entry.path();
            let relative_path = path.strip_prefix(dir).unwrap_or(&path);

            if path.is_dir() {
                self.collect_chunks_recursive(&path, chunks)?;
            } else if path.is_file() {
                // Create chunk from path + metadata (for consistency with hash_directory_tree)
                let mut chunk = Vec::new();
                chunk.extend_from_slice(relative_path.to_string_lossy().as_bytes());

                let metadata = std::fs::metadata(&path).map_err(|e| e.to_string())?;
                chunk.extend_from_slice(&metadata.len().to_le_bytes());

                let mtime = metadata
                    .modified()
                    .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                chunk.extend_from_slice(&mtime.to_le_bytes());

                chunks.push(chunk);
            }
        }

        Ok(())
    }

    /// Check if a commit exists
    fn commit_exists(&self, commit_id: &str) -> bool {
        Path::new(&self.commit_path(commit_id)).exists()
    }

    /// Store a commit object as TOML metadata
    fn store_commit(&self, commit_id: &str, commit: &crate::commit::Commit) -> Result<(), String> {
        let metadata_path = format!("{}/metadata.toml", self.commit_path(commit_id));
        let toml_content = toml::to_string(commit).map_err(|e| e.to_string())?;
        std::fs::write(&metadata_path, toml_content).map_err(|e| e.to_string())?;
        tracing::debug!("Stored commit metadata at: {}", metadata_path);
        Ok(())
    }

    // -- composefs wrappers --
    // todo: port to native composefs-rs API

    /// Create composefs file for a commit
    fn create_composefs_file(&self, commit_id: &str, dir_path: &str) -> Result<String, String> {
        let commit_file = format!("{}/commit.cfs", self.commit_path(commit_id));

        let output = std::process::Command::new("mkcomposefs")
            .arg(format!("--digest-store={}", self.objects_path()))
            .arg(dir_path)
            .arg(&commit_file)
            .output()
            .map_err(|e| e.to_string())?;

        if !output.status.success() {
            return Err(format!(
                "mkcomposefs failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        tracing::debug!("Created composefs file: {}", commit_file);
        Ok(commit_file)
    }

    /// Calls `composefs-info objects` to get lists of objects in a commit
    fn composefs_info_objects(&self, file: &str) -> Result<Vec<String>, String> {
        let output = std::process::Command::new("composefs-info")
            .arg("objects")
            .arg(file)
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

    // -- end composefs wrappers --

    /// Calculate total size of directory
    fn calculate_total_size(&self, dir_path: &str) -> Result<u64, String> {
        let path = Path::new(dir_path);
        if !path.is_dir() {
            return Err("Path is not a directory".to_string());
        }

        let mut total_size = 0u64;
        Self::calculate_size_recursive(path, &mut total_size)?;
        Ok(total_size)
    }

    /// Recursively calculate directory size
    fn calculate_size_recursive(dir: &Path, total_size: &mut u64) -> Result<(), String> {
        let entries = std::fs::read_dir(dir).map_err(|e| e.to_string())?;

        for entry in entries {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();

            if path.is_dir() {
                Self::calculate_size_recursive(&path, total_size)?;
            } else if path.is_file() {
                let metadata = std::fs::metadata(&path).map_err(|e| e.to_string())?;
                *total_size += metadata.len();
            }
        }

        Ok(())
    }

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

        // Recalculate merkle root from the original directory structure
        // Note: This would need the original directory or reconstructed from composefs
        // For now, just verify the commit exists and metadata is valid
        Ok(self.commit_exists(commit_id) && !stored_merkle_root.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_store_creation() {
        let temp_dir = TempDir::new().unwrap();
        let store_path = temp_dir.path().join("test_store");
        let store = Store::new(store_path.to_string_lossy().to_string());

        assert!(store_path.exists());
        assert_eq!(store.base_path(), store_path.to_string_lossy());
    }

    #[test]
    fn test_oci_style_commit_and_tagging() {
        let temp_dir = TempDir::new().unwrap();
        let source_path = temp_dir.path().join("source");
        let store_path = temp_dir.path().join("test_store");

        // Create test directory
        fs::create_dir_all(&source_path).unwrap();
        fs::write(source_path.join("file1.txt"), "content1").unwrap();
        fs::write(source_path.join("file2.txt"), "content2").unwrap();

        let store = Store::new(store_path.to_string_lossy().to_string());

        // Import creates a commit
        let commit_id = store
            .import_directory("myapp", &source_path.to_string_lossy(), None)
            .unwrap();

        assert!(!commit_id.is_empty());

        // Verify the directory structure:
        // store/
        //   commits/<commit_id>/
        //     metadata.toml
        //     commit.cfs
        //   refs/myapp/
        //     tags/
        let commits_dir = store_path.join("commits").join(&commit_id);
        assert!(commits_dir.exists());
        assert!(commits_dir.join("metadata.toml").exists());

        let refs_dir = store_path.join("refs").join("myapp");
        assert!(refs_dir.exists());

        // Tag the commit
        store.tag_commit("myapp", &commit_id, "v1.0").unwrap();
        store.tag_commit("myapp", &commit_id, "latest").unwrap();

        // Verify tags exist as symlinks
        let tags_dir = refs_dir.join("tags");
        assert!(tags_dir.exists());
        let v1_tag = tags_dir.join("v1.0");
        let latest_tag = tags_dir.join("latest");
        assert!(v1_tag.exists());
        assert!(latest_tag.exists());

        // Verify they are symlinks
        assert!(v1_tag.symlink_metadata().unwrap().file_type().is_symlink());
        assert!(
            latest_tag
                .symlink_metadata()
                .unwrap()
                .file_type()
                .is_symlink()
        );

        // Resolve tags
        let resolved_v1 = store.resolve_tag("myapp", "v1.0").unwrap();
        let resolved_latest = store.resolve_tag("myapp", "latest").unwrap();
        assert_eq!(resolved_v1, commit_id);
        assert_eq!(resolved_latest, commit_id);

        // List tags
        let tags = store.list_tags("myapp").unwrap();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&"v1.0".to_string()));
        assert!(tags.contains(&"latest".to_string()));
    }

    #[test]
    fn test_worktree_paths() {
        let temp_dir = TempDir::new().unwrap();
        let store_path = temp_dir.path().join("test_store");
        let store = Store::new(store_path.to_string_lossy().to_string());

        // Test worktree path methods
        let worktrees_path = store.worktrees_path("myapp");
        let main_worktree_path = store.worktree_path("myapp", "main");
        let upperdir_path = store.worktree_upperdir("myapp", "main");
        let workdir_path = store.worktree_workdir("myapp", "main");
        let meta_path = store.worktree_meta_path("myapp", "main");

        // Verify path structure
        assert!(worktrees_path.contains("refs/myapp/worktrees"));
        assert!(main_worktree_path.contains("refs/myapp/worktrees/main"));
        assert!(upperdir_path.contains("refs/myapp/worktrees/main/upperdir"));
        assert!(workdir_path.contains("refs/myapp/worktrees/main/workdir"));
        assert!(meta_path.contains("refs/myapp/worktrees/main/meta.toml"));

        // Verify parent directories are created by the path methods
        assert!(Path::new(&worktrees_path).exists());
        assert!(Path::new(&main_worktree_path).exists());

        // Note: upperdir and workdir themselves aren't created by path methods,
        // only their parent worktree directory is created
        assert!(Path::new(&main_worktree_path).exists());
    }
}
