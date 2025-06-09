use serde::{Deserialize, Serialize};
mod commit_ref;
pub use commit_ref::*;
/// A special metadata for HEAD, for storing data about the underlying header
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Head {
    /// actual head data
    pub head: HeadInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeadInfo {
    /// The ID of the last commit, used as lowerdir image for layering
    pub last_commit: String,
    /// Last time this head was committed into a new commit
    /// 
    /// Optional because HEAD may be a fresh overlay that has not ever been committed yet
    pub last_committed: Option<chrono::DateTime<chrono::Utc>>,
}

/// Main commit structure that maps directly to TOML sections
///
/// This follows an OCI-style approach where:
/// - Commits are identified by their metadata_hash (content-addressable)
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
        println!("Serialized commit:\n{}", toml_str);
    }
}
