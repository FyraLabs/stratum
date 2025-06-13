use std::path::Path;

/// Recursively collect file chunks from directory
///
/// This function now hashes actual file content instead of just metadata
/// to provide stronger integrity guarantees and better deduplication.
/// Note: This makes importing slower as all file content must be read.
///
/// SAFETY MEASURES:
/// - Symlinks are not followed to avoid issues with circular references.
/// - Special files (block devices, character devices, FIFOs, sockets) have only their
///   metadata hashed, NEVER their contents. This prevents dangerous operations like
///   reading from devices or blocking on special files.
/// - Only regular files have their content read and hashed.
pub fn collect_chunks_recursive(dir: &Path, chunks: &mut Vec<Vec<u8>>) -> Result<(), String> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;

    // Sort for consistent ordering
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let relative_path = path.strip_prefix(dir).unwrap_or(&path);

        // Use symlink_metadata to avoid following symlinks
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|e| format!("Failed to read metadata for {}: {}", path.display(), e))?;

        if metadata.is_dir() {
            collect_chunks_recursive(&path, chunks)?;
        } else if metadata.is_file() {
            // Create chunk from path + file content hash for stronger integrity
            let mut chunk = Vec::new();

            // Include the relative path for identification
            chunk.extend_from_slice(relative_path.to_string_lossy().as_bytes());
            chunk.push(0); // separator between path and content

            // Read and hash the actual file content
            let file_content = std::fs::read(&path)
                .map_err(|e| format!("Failed to read file {}: {}", path.display(), e))?;

            // Use SHA256 to hash the file content
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&file_content);
            let content_hash = hasher.finalize();

            // Append the content hash to the chunk
            chunk.extend_from_slice(&content_hash);

            chunks.push(chunk);

            tracing::debug!(
                path = %path.display(),
                size = file_content.len(),
                content_hash = %hex::encode(content_hash),
                "Hashed file content for merkle tree"
            );
        } else if metadata.file_type().is_symlink() {
            // Handle symlinks by hashing their target, not following them
            let mut chunk = Vec::new();

            // Include the relative path for identification
            chunk.extend_from_slice(relative_path.to_string_lossy().as_bytes());
            chunk.push(0); // separator between path and content

            // Hash the symlink target instead of following it
            match std::fs::read_link(&path) {
                Ok(target) => {
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(b"SYMLINK:");
                    hasher.update(target.to_string_lossy().as_bytes());
                    let content_hash = hasher.finalize();

                    chunk.extend_from_slice(&content_hash);
                    chunks.push(chunk);

                    tracing::debug!(
                        path = %path.display(),
                        target = %target.display(),
                        content_hash = %hex::encode(content_hash),
                        "Hashed symlink target for merkle tree"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "Failed to read symlink target, skipping"
                    );
                }
            }
        } else {
            // Handle special files (devices, fifos, sockets, etc.) by hashing ONLY their metadata
            // NEVER attempt to read their contents as that could be dangerous, infinite, or block forever

            // Use mode bits to detect file types (Unix-compatible)
            const S_IFBLK: u32 = 0o060000; // block device
            const S_IFCHR: u32 = 0o020000; // character device  
            const S_IFIFO: u32 = 0o010000; // FIFO (named pipe)
            const S_IFSOCK: u32 = 0o140000; // socket
            const S_IFMT: u32 = 0o170000; // file type mask

            let file_type_bits = metadata.mode() & S_IFMT;

            let mut chunk = Vec::new();

            // Include the relative path for identification
            chunk.extend_from_slice(relative_path.to_string_lossy().as_bytes());
            chunk.push(0); // separator between path and content

            use sha2::{Digest, Sha256};
            use std::os::unix::fs::MetadataExt;
            let mut hasher = Sha256::new();

            // Identify the specific type of special file
            if file_type_bits == S_IFBLK {
                hasher.update(b"BLOCK_DEVICE:");
                // Include device number for block devices (but never read content!)
                hasher.update(metadata.rdev().to_le_bytes());
            } else if file_type_bits == S_IFCHR {
                hasher.update(b"CHAR_DEVICE:");
                // Include device number for character devices (but never read content!)
                hasher.update(metadata.rdev().to_le_bytes());
            } else if file_type_bits == S_IFIFO {
                hasher.update(b"FIFO:");
            } else if file_type_bits == S_IFSOCK {
                hasher.update(b"SOCKET:");
            } else {
                hasher.update(b"OTHER_SPECIAL:");
            }

            // Hash only metadata - never the contents
            hasher.update(metadata.mode().to_le_bytes());
            hasher.update(metadata.uid().to_le_bytes());
            hasher.update(metadata.gid().to_le_bytes());
            hasher.update(metadata.len().to_le_bytes());

            let content_hash = hasher.finalize();
            chunk.extend_from_slice(&content_hash);
            chunks.push(chunk);

            tracing::debug!(
                path = %path.display(),
                file_type_bits = format!("{:o}", file_type_bits),
                mode = format!("{:o}", metadata.mode()),
                content_hash = %hex::encode(content_hash),
                "Hashed special file metadata only (no content read)"
            );
        }
    }

    Ok(())
}

/// Collects file chunks from a directory for merkle tree construction
pub fn collect_file_chunks(dir_path: &str) -> Result<Vec<Vec<u8>>, String> {
    let mut chunks = Vec::new();
    let path = Path::new(dir_path);

    // Use symlink_metadata to avoid following symlinks when checking if it's a directory
    let metadata = std::fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if !metadata.is_dir() {
        return Err("Path is not a directory".to_string());
    }

    collect_chunks_recursive(path, &mut chunks)?;

    // Sort by path to ensure consistent ordering
    chunks.sort();

    Ok(chunks)
}
