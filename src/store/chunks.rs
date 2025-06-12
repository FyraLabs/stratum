use std::path::Path;

/// Recursively collect file chunks from directory
///
/// This function now hashes actual file content instead of just metadata
/// to provide stronger integrity guarantees and better deduplication.
/// Note: This makes importing slower as all file content must be read.
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

        if path.is_dir() {
            collect_chunks_recursive(&path, chunks)?;
        } else if path.is_file() {
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
        }
    }

    Ok(())
}

/// Collects file chunks from a directory for merkle tree construction
pub fn collect_file_chunks(dir_path: &str) -> Result<Vec<Vec<u8>>, String> {
    let mut chunks = Vec::new();
    let path = Path::new(dir_path);

    if !path.is_dir() {
        return Err("Path is not a directory".to_string());
    }

    collect_chunks_recursive(path, &mut chunks)?;

    // Sort by path to ensure consistent ordering
    chunks.sort();

    Ok(chunks)
}
