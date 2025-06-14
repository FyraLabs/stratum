use serde::{Deserialize, Serialize};
mod commit_ref;
pub use commit_ref::*;
/// Metadata for a worktree, representing a named workspace for development
///
/// Worktrees provide isolated workspaces where you can make changes on top of a base commit.
/// Each worktree has its own upperdir (for changes) and workdir (for overlayfs working directory).
///
/// Example usage:
/// ```
/// use stratum::commit::Worktree;
///
/// // Create a new worktree based on a commit
/// let mut worktree = Worktree::new(
///     "feature-x".to_string(),
///     "abc123commit".to_string(),
///     Some("Working on feature X".to_string()),
/// );
///
/// // Mount it somewhere
/// worktree.set_mounted_at(Some("/mnt/feature-x".to_string()));
/// assert!(worktree.is_mounted());
///
/// // Check for uncommitted changes (requires upperdir path)
/// let upperdir = std::path::Path::new("/store/refs/myapp/worktrees/feature-x/upperdir");
/// let has_changes = worktree.has_uncommitted_changes(upperdir);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Worktree {
    /// actual worktree data
    pub worktree: WorktreeInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorktreeInfo {
    /// The name of this worktree
    pub name: String,
    /// The ID of the base commit this worktree is based on
    pub base_commit: String,
    /// When this worktree was created
    pub created: chrono::DateTime<chrono::Utc>,
    /// Last time this worktree was modified
    pub last_modified: chrono::DateTime<chrono::Utc>,
    /// Last time this worktree was committed into a new commit
    ///
    /// Optional because a worktree may be a fresh overlay that has not ever been committed yet
    pub last_committed: Option<chrono::DateTime<chrono::Utc>>,
    /// Optional description of what this worktree is for
    pub description: Option<String>,
}

/// Main commit structure that maps directly to TOML sections
///
/// This follows an OCI-style approach where:
/// - Commits are identified by their `metadata_hash` (content-addressable)
/// - Tags serve as human-readable aliases pointing to commit hashes
/// - No commit messages - tags provide the semantic meaning
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Commit {
    /// [commit] section - core commit information
    pub commit: CommitInfo,
    /// [files] section - file statistics
    pub files: FileStats,
    /// [merkle] section - merkle tree information
    pub merkle: MerkleInfo,
}

impl Commit {
    /// Creates a new commit with the given information
    pub fn new(
        metadata_hash: [u8; 32],
        merkle_root: [u8; 32],
        file_count: u64,
        total_size: u64,
        leaf_count: usize,
        tree_depth: u32,
        parent_id: Option<String>,
    ) -> Self {
        Self {
            commit: CommitInfo {
                merkle_root: hex::encode(merkle_root),
                metadata_hash: hex::encode(metadata_hash),
                timestamp: chrono::Utc::now(),
                parent_commit: parent_id,
            },
            files: FileStats {
                count: file_count,
                total_size,
            },
            merkle: MerkleInfo {
                leaf_count,
                tree_depth,
            },
        }
    }

    /// Returns the commit ID (metadata hash)
    pub fn id(&self) -> &str {
        &self.commit.metadata_hash
    }

    /// Returns the metadata hash as hex string
    pub fn metadata_hash(&self) -> &str {
        &self.commit.metadata_hash
    }

    /// Returns the merkle root hash as hex string
    pub fn merkle_root(&self) -> &str {
        &self.commit.merkle_root
    }

    /// Returns the metadata hash as bytes
    pub fn metadata_hash_bytes(&self) -> Result<[u8; 32], hex::FromHexError> {
        let decoded = hex::decode(&self.commit.metadata_hash)?;
        let mut array = [0u8; 32];
        array.copy_from_slice(&decoded);
        Ok(array)
    }

    /// Returns the merkle root hash as bytes
    pub fn merkle_root_bytes(&self) -> Result<[u8; 32], hex::FromHexError> {
        let decoded = hex::decode(&self.commit.merkle_root)?;
        let mut array = [0u8; 32];
        array.copy_from_slice(&decoded);
        Ok(array)
    }
}

/// [commit] section - core commit information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitInfo {
    /// Cryptographic merkle root for verification
    pub merkle_root: String,
    /// Fast metadata-based hash (primary identifier)
    pub metadata_hash: String,
    /// When this commit was created
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Optional parent commit ID for history tracking
    pub parent_commit: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileStats {
    /// Total number of files
    pub count: u64,
    /// Total size in bytes
    pub total_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MerkleInfo {
    /// Number of leaves in the merkle tree
    pub leaf_count: usize,
    /// Depth of the merkle tree
    pub tree_depth: u32,
}

impl Worktree {
    /// Creates a new worktree with the given name and base commit
    pub fn new(name: String, base_commit: String, description: Option<String>) -> Self {
        let now = chrono::Utc::now();
        Self {
            worktree: WorktreeInfo {
                name,
                base_commit,
                created: now,
                last_modified: now,
                last_committed: None,
                description,
            },
        }
    }

    /// Returns the name of this worktree
    pub fn name(&self) -> &str {
        &self.worktree.name
    }

    /// Returns the base commit ID
    pub fn base_commit(&self) -> &str {
        &self.worktree.base_commit
    }

    pub fn set_base_commit(&mut self, base_commit: String) {
        self.worktree.base_commit = base_commit;
    }

    /// Updates the last committed timestamp
    pub fn mark_committed(&mut self) {
        self.worktree.last_committed = Some(chrono::Utc::now());
    }

    /// Updates the last modified timestamp (only call when explicitly needed)
    pub fn touch(&mut self) {
        self.worktree.last_modified = chrono::Utc::now();
    }

    /// Returns whether this worktree has uncommitted changes by checking the upperdir
    ///
    /// This requires the upperdir path to be passed in since the worktree metadata
    /// doesn't store filesystem paths directly
    pub fn has_uncommitted_changes(&self, upperdir_path: &std::path::Path) -> bool {
        if !upperdir_path.exists() {
            return false; // No upperdir means no changes
        }

        // Check if upperdir has any files/directories
        match std::fs::read_dir(upperdir_path) {
            Ok(mut entries) => entries.next().is_some(), // Has at least one entry
            Err(_) => false,                             // Can't read directory, assume no changes
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_creation() {
        let metadata_hash = [0xab; 32];
        let merkle_root = [0xcd; 32];
        let commit = Commit::new(metadata_hash, merkle_root, 100, 1024 * 1024, 100, 10, None);

        // serialize to TOML
        let toml_str = toml::to_string(&commit).expect("Failed to serialize commit");
        println!("Serialized commit:\n{toml_str}");
    }

    #[test]
    fn test_worktree_creation() {
        let worktree = Worktree::new(
            "main".to_owned(),
            "abc123".to_owned(),
            Some("Main development branch".to_owned()),
        );

        assert_eq!(worktree.name(), "main");
        assert_eq!(worktree.base_commit(), "abc123");
        assert_eq!(
            worktree.worktree.description,
            Some("Main development branch".to_owned())
        );
    }

    #[test]
    fn test_worktree_uncommitted_changes() {
        use std::fs;
        use tempfile::TempDir;

        let worktree = Worktree::new("test".to_owned(), "xyz789".to_owned(), None);

        let temp_dir = TempDir::new().unwrap();
        let upperdir = temp_dir.path().join("upperdir");

        // Non-existent upperdir should return false
        assert!(!worktree.has_uncommitted_changes(&upperdir));

        // Empty upperdir should return false
        fs::create_dir_all(&upperdir).unwrap();
        assert!(!worktree.has_uncommitted_changes(&upperdir));

        // Upperdir with files should return true
        fs::write(upperdir.join("test.txt"), "content").unwrap();
        assert!(worktree.has_uncommitted_changes(&upperdir));
    }
}
