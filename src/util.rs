use rs_merkle::{Hasher, MerkleTree};
use sha2::{Digest, Sha256};
use std::fs;
use std::io;
use std::path::Path;

// Parses and OCI-style tag into a tuple of name and optional tag.
pub fn parse_label(label: &str) -> Result<(String, Option<String>), String> {
    label
        .split_once(':')
        .map(|(name, tag)| (name.to_owned(), Some(tag.to_owned())))
        .or_else(|| Some((label.to_owned(), None)))
        .ok_or_else(|| format!("Invalid label format: {label}"))
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
/// The hash is derived from file content, metadata (size, permissions, ownership),
/// relative paths, and the structure of the directory tree. This provides strong
/// integrity guarantees by hashing actual file content along with metadata.
/// Entries are processed in alphabetical order by name to ensure hash consistency.
///
/// SAFETY MEASURES:
/// - Symlinks are NOT followed; only the symlink target path is hashed, not the content
///   it points to. This prevents issues with circular symlinks and infinite loops.
/// - Special files (block devices, character devices, FIFOs, sockets) have only their
///   metadata hashed, NEVER their contents. This prevents dangerous operations like
///   reading from `/dev/sda` or blocking on `/dev/zero`.
/// - Only regular files have their content read and hashed.
///
/// # Arguments
///
/// * `dir_path`: The path to the directory to hash.
///
/// # Errors
///
/// Returns `std::io::Error` if the path is not a directory, or if any I/O error
/// occurs during traversal, metadata reading, or file content reading.
#[tracing::instrument(level = "trace")]
pub fn hash_directory_tree(dir_path: &Path) -> io::Result<[u8; 32]> {
    // Use symlink_metadata to avoid following symlinks when checking if it's a directory
    let metadata = std::fs::symlink_metadata(dir_path)?;
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Input path is not a directory",
        ));
    }

    tracing::trace!(
        ?dir_path,
        "Calculating content-based hash for directory tree"
    );
    calculate_dir_hash(dir_path, dir_path)
}

/// Helper function to recursively calculate the hash of a directory's content and metadata.
///
/// This function now hashes file content along with metadata, relative paths, and attributes
/// to provide stronger integrity guarantees for commit IDs.
pub fn calculate_dir_hash(dir_path: &Path, root_path: &Path) -> io::Result<[u8; 32]> {
    use std::os::unix::fs::MetadataExt;

    let mut hasher = Sha256::new();

    let mut entries = fs::read_dir(dir_path)?.collect::<Result<Vec<_>, io::Error>>()?;

    // Sort entries by file name to ensure consistent hash results
    entries.sort_by_key(std::fs::DirEntry::file_name);

    for entry in entries {
        let path = entry.path();
        let file_name = entry.file_name();

        // Calculate relative path from the root directory
        let relative_path = path.strip_prefix(root_path).unwrap_or(&path);

        // Hash the relative path for unique identification within the tree
        tracing::trace!(?file_name, ?path, ?relative_path, "Hashing entry");
        hasher.update(relative_path.to_string_lossy().as_bytes());
        hasher.update([0]); // Separator between path and content

        let metadata = fs::symlink_metadata(&path)?; // Does NOT follow symlinks

        if metadata.is_dir() {
            tracing::trace!(?path, "Processing directory");
            hasher.update(b"DIR"); // Type marker for directory

            // Hash directory metadata
            hasher.update(metadata.mode().to_le_bytes()); // Permissions
            hasher.update(metadata.uid().to_le_bytes()); // Owner UID
            hasher.update(metadata.gid().to_le_bytes()); // Owner GID

            let subdir_hash = calculate_dir_hash(&path, root_path)?; // Recursive call for subdirectory
            tracing::trace!(?path, hash = %hex::encode(subdir_hash), "Subdirectory hash");
            hasher.update(subdir_hash); // Update with the hash of the subdirectory
        } else if metadata.is_file() {
            tracing::trace!(?path, "Processing file content and metadata");
            hasher.update(b"FILE"); // Type marker for file

            // Hash file metadata (permissions, ownership, size)
            hasher.update(metadata.mode().to_le_bytes()); // Permissions
            hasher.update(metadata.uid().to_le_bytes()); // Owner UID
            hasher.update(metadata.gid().to_le_bytes()); // Owner GID
            hasher.update(metadata.len().to_le_bytes()); // File size

            // Hash the actual file content for stronger integrity
            let file_content = fs::read(&path)?;
            hasher.update(&file_content);

            tracing::trace!(
                ?path,
                size = metadata.len(),
                content_size = file_content.len(),
                mode = format!("{:o}", metadata.mode()),
                uid = metadata.uid(),
                gid = metadata.gid(),
                "Hashed file content and metadata"
            );
        } else if metadata.file_type().is_symlink() {
            tracing::trace!(?path, "Processing symlink");
            hasher.update(b"SYMLINK"); // Type marker for symlink

            // Hash symlink metadata (permissions, ownership)
            hasher.update(metadata.mode().to_le_bytes());
            hasher.update(metadata.uid().to_le_bytes());
            hasher.update(metadata.gid().to_le_bytes());

            // Hash the symlink target (not the content it points to)
            match fs::read_link(&path) {
                Ok(target) => {
                    hasher.update(target.to_string_lossy().as_bytes());
                    tracing::trace!(?path, ?target, "Hashed symlink target");
                }
                Err(e) => {
                    tracing::warn!(?path, error = ?e, "Failed to read symlink target, skipping");
                    hasher.update(b"BROKEN_SYMLINK");
                }
            }
        } else {
            // Handle special files (devices, fifos, sockets, etc.) by hashing ONLY their metadata
            // Never attempt to read their contents as that could be dangerous or infinite

            // Use mode bits to detect file types (Unix-compatible)
            const S_IFBLK: u32 = 0o060000; // block device
            const S_IFCHR: u32 = 0o020000; // character device  
            const S_IFIFO: u32 = 0o010000; // FIFO (named pipe)
            const S_IFSOCK: u32 = 0o140000; // socket
            const S_IFMT: u32 = 0o170000; // file type mask

            let file_type_bits = metadata.mode() & S_IFMT;

            tracing::trace!(
                ?path,
                mode = format!("{:o}", metadata.mode()),
                "Processing special file - metadata only"
            );

            if file_type_bits == S_IFBLK {
                hasher.update(b"BLOCK_DEVICE");
            } else if file_type_bits == S_IFCHR {
                hasher.update(b"CHAR_DEVICE");
            } else if file_type_bits == S_IFIFO {
                hasher.update(b"FIFO");
            } else if file_type_bits == S_IFSOCK {
                hasher.update(b"SOCKET");
            } else {
                hasher.update(b"OTHER_SPECIAL");
            }

            // Hash only metadata for special files - NEVER read their contents
            hasher.update(metadata.mode().to_le_bytes());
            hasher.update(metadata.uid().to_le_bytes());
            hasher.update(metadata.gid().to_le_bytes());
            hasher.update(metadata.len().to_le_bytes()); // Size (if meaningful)

            tracing::trace!(
                ?path,
                file_type_bits = format!("{:o}", file_type_bits),
                "Hashed special file metadata only"
            );
        }
    }

    let result = hasher.finalize();
    let mut hash_array = [0u8; 32];
    hash_array.copy_from_slice(result.as_slice());
    tracing::trace!(?dir_path, hash = %hex::encode(hash_array), "Directory content hash calculated");
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

/// Copy a directory recursively, preserving all metadata including permissions,
/// ownership, timestamps, and extended attributes.
pub fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    let src_path = src.as_ref();
    let dst_path = dst.as_ref();

    tracing::debug!(
        "Copying directory from {} to {} (preserving all metadata)",
        src_path.display(),
        dst_path.display()
    );

    // Create destination directory and copy its metadata
    fs::create_dir_all(&dst)?;
    copy_metadata(src_path, dst_path)?;

    for entry in fs::read_dir(src_path)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let path = entry.path();
        let filename = path.file_name().unwrap();
        let target = dst.as_ref().join(filename);

        if ty.is_dir() {
            copy_dir_all(&path, &target)?;
        } else if ty.is_file() {
            tracing::trace!("Copying file {} to {}", path.display(), target.display());
            copy_file_with_metadata(&path, &target)?;
        } else if ty.is_symlink() {
            tracing::trace!("Copying symlink {} to {}", path.display(), target.display());
            copy_symlink(&path, &target)?;
        } else {
            tracing::warn!("Skipping special file: {}", path.display());
        }
    }

    tracing::debug!(
        "Finished copying directory from {} to {}",
        src_path.display(),
        dst_path.display()
    );
    Ok(())
}

/// Copy a file and preserve all its metadata
fn copy_file_with_metadata(src: &Path, dst: &Path) -> io::Result<()> {
    // Copy file content
    fs::copy(src, dst)?;

    // Copy all metadata
    copy_metadata(src, dst)?;

    Ok(())
}

/// Copy a symlink and preserve its metadata
fn copy_symlink(src: &Path, dst: &Path) -> io::Result<()> {
    let link_target = fs::read_link(src)?;

    // Remove destination if it exists
    _ = fs::remove_file(dst);

    // Create the symlink
    std::os::unix::fs::symlink(&link_target, dst)?;

    // Note: We don't copy metadata for symlinks as it's usually not needed
    // and can cause issues. The symlink itself inherits the metadata.

    Ok(())
}

/// Copy all metadata from source to destination, including permissions, ownership,
/// timestamps, and extended attributes
fn copy_metadata(src: &Path, dst: &Path) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let metadata = src.metadata()?;

    // Copy permissions
    let permissions = fs::Permissions::from_mode(metadata.mode());
    fs::set_permissions(dst, permissions)?;

    // Copy timestamps (access time and modification time)
    if let (Ok(atime), Ok(mtime)) = (metadata.accessed(), metadata.modified()) {
        // Use libc to set both times atomically
        let atime_spec = timespec_from_systemtime(atime);
        let mtime_spec = timespec_from_systemtime(mtime);

        unsafe {
            let path_cstr = std::ffi::CString::new(dst.as_os_str().as_encoded_bytes())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid path"))?;

            let times = [atime_spec, mtime_spec];
            if libc::utimensat(libc::AT_FDCWD, path_cstr.as_ptr(), times.as_ptr(), 0) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }

    // Copy ownership (requires appropriate privileges)
    // This will silently fail if we don't have permission, which is usually fine
    unsafe {
        let path_cstr = std::ffi::CString::new(dst.as_os_str().as_encoded_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid path"))?;

        // Try to set ownership, but don't fail if we can't
        let _ = libc::chown(path_cstr.as_ptr(), metadata.uid(), metadata.gid());
    }

    // Copy extended attributes
    copy_xattrs(src, dst)?;

    Ok(())
}

/// Copy extended attributes from source to destination
fn copy_xattrs(src: &Path, dst: &Path) -> io::Result<()> {
    // List all extended attributes on the source
    let src_cstr = std::ffi::CString::new(src.as_os_str().as_encoded_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid source path"))?;
    let dst_cstr = std::ffi::CString::new(dst.as_os_str().as_encoded_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid destination path"))?;

    unsafe {
        // Get the size of the attribute list
        let list_size = libc::listxattr(src_cstr.as_ptr(), std::ptr::null_mut(), 0);
        if list_size < 0 {
            let err = io::Error::last_os_error();
            // ENOTSUP means the filesystem doesn't support xattrs, which is fine
            if err.raw_os_error() == Some(libc::ENOTSUP) {
                return Ok(());
            }
            return Err(err);
        }

        if list_size == 0 {
            return Ok(()); // No extended attributes
        }

        // Get the attribute list
        let mut attr_list = vec![0u8; list_size as usize];
        let actual_size = libc::listxattr(
            src_cstr.as_ptr(),
            attr_list.as_mut_ptr().cast::<i8>(),
            list_size as usize,
        );
        if actual_size < 0 {
            return Err(io::Error::last_os_error());
        }

        // Parse and copy each attribute
        let mut offset = 0;
        while offset < actual_size as usize {
            // Find the null terminator for this attribute name
            let name_end = attr_list[offset..]
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(0)
                + offset;

            if name_end > offset {
                let attr_name = std::ffi::CStr::from_bytes_with_nul(&attr_list[offset..=name_end])
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "Invalid xattr name")
                    })?;

                // Get the attribute value size
                let value_size = libc::getxattr(
                    src_cstr.as_ptr(),
                    attr_name.as_ptr(),
                    std::ptr::null_mut(),
                    0,
                );
                if value_size >= 0 {
                    if value_size == 0 {
                        // Empty attribute value
                        let result = libc::setxattr(
                            dst_cstr.as_ptr(),
                            attr_name.as_ptr(),
                            std::ptr::null(),
                            0,
                            0,
                        );
                        if result < 0 {
                            let err = io::Error::last_os_error();
                            tracing::warn!(
                                "Failed to copy xattr {}: {}",
                                attr_name.to_string_lossy(),
                                err
                            );
                        }
                    } else {
                        // Get and set the attribute value
                        let mut value = vec![0u8; value_size as usize];
                        let actual_value_size = libc::getxattr(
                            src_cstr.as_ptr(),
                            attr_name.as_ptr(),
                            value.as_mut_ptr().cast::<libc::c_void>(),
                            value_size as usize,
                        );

                        if actual_value_size >= 0 {
                            let result = libc::setxattr(
                                dst_cstr.as_ptr(),
                                attr_name.as_ptr(),
                                value.as_ptr().cast::<libc::c_void>(),
                                actual_value_size as usize,
                                0,
                            );
                            if result < 0 {
                                let err = io::Error::last_os_error();
                                tracing::warn!(
                                    "Failed to copy xattr {}: {}",
                                    attr_name.to_string_lossy(),
                                    err
                                );
                            }
                        }
                    }
                }
            }

            offset = name_end + 1;
        }
    }

    Ok(())
}

/// Convert `SystemTime` to `libc::timespec`
pub fn timespec_from_systemtime(time: std::time::SystemTime) -> libc::timespec {
    match time.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => libc::timespec {
            tv_sec: duration.as_secs() as libc::time_t,
            tv_nsec: libc::c_long::from(duration.subsec_nanos()),
        },
        Err(_) => libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        },
    }
}
/// Calculate total size of directory
pub fn calculate_total_size(dir_path: &str) -> Result<u64, String> {
    let path = Path::new(dir_path);

    // Use symlink_metadata to avoid following symlinks when checking if it's a directory
    let metadata = std::fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if !metadata.is_dir() {
        return Err("Path is not a directory".to_owned());
    }

    let mut total_size = 0u64;
    calculate_size_recursive(path, &mut total_size)?;
    Ok(total_size)
}

/// Recursively calculate directory size
///
/// This function safely handles symlinks by not following them - it will only
/// count the size of the symlink itself, not its target. This prevents issues
/// with recursive symlinks and ensures consistent behavior.
pub fn calculate_size_recursive(dir: &Path, total_size: &mut u64) -> Result<(), String> {
    let entries = std::fs::read_dir(dir).map_err(|e| e.to_string())?;

    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        // Use symlink_metadata to avoid following symlinks
        let metadata = std::fs::symlink_metadata(&path).map_err(|e| e.to_string())?;

        if metadata.is_dir() {
            // Only recurse into actual directories, not symlinked directories
            calculate_size_recursive(&path, total_size)?;
        } else if metadata.is_file() {
            *total_size += metadata.len();
        } else if metadata.file_type().is_symlink() {
            // Count symlink size (typically very small)
            *total_size += metadata.len();
        }
        // Ignore other special file types (devices, FIFOs, etc.)
    }

    Ok(())
}
pub fn fsync_all_walk(dir: &Path) -> io::Result<()> {
    tracing::trace!("Running fsync() on {}", dir.display());

    // Configure jwalk to not follow symlinks for safety
    // This prevents issues with recursive symlinks like Wine's pfx directories
    let walker = jwalk::WalkDir::new(dir).follow_links(false);

    for entry in walker {
        let entry = entry?;
        let path = entry.path();

        // Use symlink_metadata to check file type without following symlinks
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) => {
                // Only attempt to sync regular files and directories
                if metadata.is_file() || metadata.is_dir() {
                    match std::fs::File::open(&path) {
                        Ok(file) => {
                            if let Err(e) = rustix::fs::fsync(&file) {
                                // Some files can't be synced (e.g., special mounts, device files)
                                // Log as debug rather than warning to reduce noise
                                tracing::debug!("Failed to sync {}: {}", path.display(), e);
                            }
                        }
                        Err(e) => {
                            tracing::debug!("Failed to open {} for sync: {}", path.display(), e);
                        }
                    }
                } else {
                    // Skip symlinks, device files, FIFOs, sockets, etc.
                    tracing::trace!("Skipping non-regular file for sync: {}", path.display());
                }
            }
            Err(e) => {
                tracing::debug!("Failed to get metadata for {}: {}", path.display(), e);
            }
        }
    }
    Ok(())
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
