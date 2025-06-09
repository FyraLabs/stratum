use clap::ValueEnum;
use rs_merkle::{Hasher, MerkleTree};
use sha2::{Digest, Sha256};
use std::fs;
use std::io;
use std::path::Path;
use std::time::SystemTime;

// Parses and OCI-style tag into a tuple of name and optional tag.
pub fn parse_label(label: &str) -> Result<(String, Option<String>), String> {
    label
        .split_once(':')
        .map(|(name, tag)| (name.to_string(), Some(tag.to_string())))
        .or_else(|| Some((label.to_string(), None)))
        .ok_or_else(|| format!("Invalid label format: {}", label))
}

/// Derives a new hash by combining two byte arrays using SHA256.
///
/// This function is designed for use in merkle tree construction where
/// parent nodes are computed by hashing the concatenation of their children.
/// The order of inputs matters: `derive_hash(a, b) != derive_hash(b, a)`.
///
/// # Arguments
///
/// * `left` - The left child hash or data
/// * `right` - The right child hash or data
///
/// # Returns
///
/// A 32-byte SHA256 hash of the concatenated inputs
///
/// # Examples
///
/// ```rust
/// use stratum::util::derive_hash;
///
/// let left = b"left_data";
/// let right = b"right_data";
/// let parent_hash = derive_hash(left, right);
/// ```
pub fn derive_hash(left: &[u8], right: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(left);
    hasher.update(right);
    let result = hasher.finalize();

    // Convert to fixed-size array more efficiently
    let mut hash_array = [0u8; 32];
    hash_array.copy_from_slice(result.as_slice());
    hash_array
}

/// Calculates a SHA256 hash for the entire directory tree rooted at `dir_path`.
///
/// The hash is derived from the names of files and directories, file metadata
/// (size and modification time), and the structure of the directory tree.
/// This approach is much faster than hashing file contents for large trees.
/// Entries are processed in alphabetical order by name to ensure hash consistency.
///
/// Symlinks are followed; if they point to a directory, it's traversed, if to a file,
/// its metadata is read. This could lead to issues with symlink loops in complex scenarios.
///
/// # Arguments
///
/// * `dir_path`: The path to the directory to hash.
///
/// # Errors
///
/// Returns `std::io::Error` if the path is not a directory, or if any I/O error
/// occurs during traversal or metadata reading.
#[tracing::instrument(level = "trace")]
pub fn hash_directory_tree(dir_path: &Path) -> io::Result<[u8; 32]> {
    if !dir_path.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Input path is not a directory",
        ));
    }

    tracing::trace!(?dir_path, "Calculating metadata hash for directory tree");
    calculate_dir_hash(dir_path)
}

/// Helper function to recursively calculate the hash of a directory's metadata.
fn calculate_dir_hash(dir_path: &Path) -> io::Result<[u8; 32]> {
    let mut hasher = Sha256::new();

    let mut entries = fs::read_dir(dir_path)?.collect::<Result<Vec<_>, io::Error>>()?;

    // Sort entries by file name to ensure consistent hash results
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let file_name = entry.file_name();

        // Hash the entry's name
        tracing::trace!(?file_name, ?path, "Hashing entry name");
        hasher.update(file_name.to_string_lossy().as_bytes());

        let metadata = fs::metadata(&path)?; // Follows symlinks

        if metadata.is_dir() {
            tracing::trace!(?path, "Processing directory");
            hasher.update(b"DIR"); // Type marker for directory
            let subdir_hash = calculate_dir_hash(&path)?; // Recursive call for subdirectory
            tracing::trace!(?path, hash = %hex::encode(subdir_hash), "Subdirectory hash");
            hasher.update(subdir_hash); // Update with the hash of the subdirectory
        } else if metadata.is_file() {
            tracing::trace!(?path, "Processing file metadata");
            hasher.update(b"FILE"); // Type marker for file

            // Hash file size
            hasher.update(metadata.len().to_le_bytes());

            // Hash modification time (fallback to 0 if unavailable)
            let mtime = metadata
                .modified()
                .unwrap_or(SystemTime::UNIX_EPOCH)
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            hasher.update(mtime.to_le_bytes());

            tracing::trace!(?path, size = metadata.len(), mtime, "Hashed file metadata");
        } else {
            tracing::trace!(?path, "Ignoring non-file, non-directory entry");
        }
    }

    let result = hasher.finalize();
    let mut hash_array = [0u8; 32];
    hash_array.copy_from_slice(result.as_slice());
    tracing::trace!(?dir_path, hash = %hex::encode(hash_array), "Directory hash calculated");
    Ok(hash_array)
}

/// Calculates the hash of a single leaf node in a merkle tree.
///
/// This function prepends a leaf marker (0x00) to distinguish leaf nodes
/// from internal nodes in the merkle tree, preventing second preimage attacks.
///
/// # Arguments
///
/// * `data` - The data to hash as a leaf node
///
/// # Returns
///
/// A 32-byte SHA256 hash with leaf node prefix
pub fn hash_leaf(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([0x00]); // Leaf node prefix
    hasher.update(data);
    let result = hasher.finalize();

    let mut hash_array = [0u8; 32];
    hash_array.copy_from_slice(result.as_slice());
    hash_array
}

/// Calculates the hash of an internal node in a merkle tree.
///
/// This function prepends an internal node marker (0x01) to distinguish
/// internal nodes from leaf nodes, preventing second preimage attacks.
///
/// # Arguments
///
/// * `left_hash` - The hash of the left child node
/// * `right_hash` - The hash of the right child node
///
/// # Returns
///
/// A 32-byte SHA256 hash with internal node prefix
pub fn hash_internal_node(left_hash: &[u8; 32], right_hash: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([0x01]); // Internal node prefix
    hasher.update(left_hash);
    hasher.update(right_hash);
    let result = hasher.finalize();

    let mut hash_array = [0u8; 32];
    hash_array.copy_from_slice(result.as_slice());
    hash_array
}

/// SHA256 hasher implementation for rs-merkle
#[derive(Clone)]
pub struct Sha256Hasher;

impl Hasher for Sha256Hasher {
    type Hash = [u8; 32];

    fn hash(data: &[u8]) -> Self::Hash {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();

        let mut hash_array = [0u8; 32];
        hash_array.copy_from_slice(result.as_slice());
        hash_array
    }
}

/// Builds a merkle tree root hash from a list of data chunks using rs-merkle.
///
/// This function uses the robust rs-merkle crate for constructing merkle trees
/// with proper security guarantees and optimized performance.
///
/// # Arguments
///
/// * `data_chunks` - A slice of data chunks to build the merkle tree from
///
/// # Returns
///
/// The root hash of the merkle tree, or an empty hash if input is empty
///
/// # Examples
///
/// ```rust
/// use stratum::util::build_merkle_root;
///
/// let chunks = vec![b"chunk1".as_slice(), b"chunk2".as_slice(), b"chunk3".as_slice()];
/// let root_hash = build_merkle_root(&chunks);
/// ```
pub fn build_merkle_root(data_chunks: &[&[u8]]) -> [u8; 32] {
    if data_chunks.is_empty() {
        return [0u8; 32]; // Empty tree hash
    }

    // Convert data chunks to hashes for the merkle tree
    let leaves: Vec<[u8; 32]> = data_chunks
        .iter()
        .map(|chunk| Sha256Hasher::hash(chunk))
        .collect();

    // Build merkle tree using rs-merkle
    let merkle_tree = MerkleTree::<Sha256Hasher>::from_leaves(&leaves);

    // Return the root hash, or empty hash if tree is empty
    merkle_tree.root().unwrap_or([0u8; 32])
}

/// Generates a merkle proof for a specific leaf in the tree.
///
/// This function creates a cryptographic proof that a specific piece of data
/// is included in the merkle tree without revealing the entire tree.
///
/// # Arguments
///
/// * `data_chunks` - All data chunks in the merkle tree
/// * `target_index` - Index of the chunk to generate proof for
///
/// # Returns
///
/// A merkle proof that can be used to verify inclusion
pub fn generate_merkle_proof(data_chunks: &[&[u8]], target_index: usize) -> Option<Vec<[u8; 32]>> {
    if data_chunks.is_empty() || target_index >= data_chunks.len() {
        return None;
    }

    // Convert data chunks to hashes
    let leaves: Vec<[u8; 32]> = data_chunks
        .iter()
        .map(|chunk| Sha256Hasher::hash(chunk))
        .collect();

    // Build merkle tree
    let merkle_tree = MerkleTree::<Sha256Hasher>::from_leaves(&leaves);

    // Generate proof for the target index
    merkle_tree
        .proof(&[target_index])
        .proof_hashes()
        .to_vec()
        .into()
}

/// Verifies a merkle proof against a root hash.
///
/// This function validates that a piece of data is included in a merkle tree
/// with the given root hash using the provided proof.
///
/// # Arguments
///
/// * `proof` - The merkle proof hashes
/// * `root_hash` - The expected root hash of the merkle tree
/// * `leaf_data` - The data to verify inclusion for
/// * `leaf_index` - The index of the leaf in the original tree
/// * `tree_size` - The total number of leaves in the original tree
///
/// # Returns
///
/// True if the proof is valid, false otherwise
pub fn verify_merkle_proof(
    proof: &[[u8; 32]],
    root_hash: &[u8; 32],
    leaf_data: &[u8],
    leaf_index: usize,
    tree_size: usize,
) -> bool {
    if proof.is_empty() {
        return false;
    }

    let leaf_hash = Sha256Hasher::hash(leaf_data);

    // Create a merkle proof from the provided hashes
    let merkle_proof = rs_merkle::MerkleProof::<Sha256Hasher>::new(proof.to_vec());

    // Verify the proof
    merkle_proof.verify(*root_hash, &[leaf_index], &[leaf_hash], tree_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_hash() {
        let left = b"left_data";
        let right = b"right_data";
        let hash1 = derive_hash(left, right);
        let hash2 = derive_hash(left, right);
        let hash3 = derive_hash(right, left);

        // Same inputs should produce same hash
        assert_eq!(hash1, hash2);
        // Different order should produce different hash
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_merkle_tree_operations() {
        let data = vec![
            b"file1_content".as_slice(),
            b"file2_content".as_slice(),
            b"file3_content".as_slice(),
        ];

        // Build merkle root
        let root = build_merkle_root(&data);
        assert_ne!(root, [0u8; 32]); // Should not be empty

        // Generate proof for middle element
        let proof = generate_merkle_proof(&data, 1);
        assert!(proof.is_some());

        let proof_hashes = proof.unwrap();

        // Verify the proof
        let is_valid = verify_merkle_proof(&proof_hashes, &root, b"file2_content", 1, data.len());
        assert!(is_valid);

        // Verify with wrong data should fail
        let is_invalid = verify_merkle_proof(&proof_hashes, &root, b"wrong_content", 1, data.len());
        assert!(!is_invalid);
    }

    #[test]
    fn test_leaf_vs_internal_node_hashing() {
        let data = b"test_data";
        let leaf_hash = hash_leaf(data);
        let internal_hash = hash_internal_node(&[0u8; 32], &[1u8; 32]);

        // Different prefixes should produce different hashes even with same data
        assert_ne!(leaf_hash, internal_hash);
    }

    #[test]
    fn test_empty_merkle_tree() {
        let empty_data: Vec<&[u8]> = vec![];
        let root = build_merkle_root(&empty_data);
        assert_eq!(root, [0u8; 32]); // Empty tree should have zero hash
    }
}
