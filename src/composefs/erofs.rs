//! adapter for composefs-rs EROFS functionality
//!
//! So we can create, read and write our own EROFS images
//!
//! important for ComposeFS itself, since it uses EROFS to store the directory tree
//! and we want to be able to read and write them
use std::io::Read;
use composefs::erofs::reader::InodeOps;
use composefs::{erofs::reader::DirectoryEntry, fsverity::FsVerityHashValue};
use rustix::path::Arg;
use tracing::trace;

pub struct ErofsImage<'i> {
    i: composefs::erofs::reader::Image<'i>,
}

impl<'i> ErofsImage<'i> {
    pub fn from_bytes(bytes: &'i [u8]) -> Self {
        Self {
            i: composefs::erofs::reader::Image::open(bytes),
        }
    }

    pub fn root_nid(&self) -> u64 {
        self.i.sb.root_nid.get() as u64
    }

    pub fn list_files(
        &self,
        inode: composefs::erofs::reader::InodeType,
    ) -> impl Iterator<Item = composefs::erofs::reader::DirectoryEntry<'_>> {
        self.i.directory_block(inode.blocks(self.i.blkszbits).start).entries()
    }
    
    pub fn test(&self) -> Result<(), String> {
        // This is just a test function to ensure the ErofsImage can be created and read.
        // You can implement more specific tests as needed.
        let root_nid = self.root_nid();
        
        let root = self.i.root();
        
        println!("root: {:?}", root);
        
        Ok(())
    }
    
    // https://github.com/ToolmanP/erofs-rs/blob/fb5301b3cc46b909de32f86ea246eb9ecff741cf/erofs-sys/src/operations.rs#L33-L59
    pub fn get_nid_from_path(
        &self,
        path: &str,
    ) -> Result<u64, String> {
        let mut nid = self.root_nid();
        for part in path.as_bytes().split(|&b| b == b'/') {
            if part.is_empty() {
                continue;
            }
            let entry = self.list_files(self.i.inode(nid)).find(|entry| entry.name == part).ok_or_else(|| format!("Path '{path}' not found in EROFS image"))?;
            nid = entry.header.inode_offset.get();
        }
        Ok(nid)
    }

    /// Check if a DirectoryEntry is an OverlayFS whiteout or an opaque directory.
    ///
    /// If it is, return the given argument.
    ///
    /// if not, return None.
    pub fn is_whiteout(&self, entry: DirectoryEntry) -> Option<bool> {
        // from kernel documentation:
        //
        // > In order to support rm and rmdir without changing the lower filesystem, an overlay
        // > filesystem needs to record in the upper filesystem that files have been removed.
        // > This is done using whiteouts and opaque directories (non-directories are always opaque).
        // > A whiteout is created as a character device with 0/0 device number or as a zero-size regular file with the xattr
        // > “trusted.overlay.whiteout”.
        // > When a whiteout is found in the upper level of a merged directory, any matching name in the lower level is ignored,
        // > and the whiteout itself is also hidden.
        // > A directory is made opaque by setting the xattr “trusted.overlay.opaque” to “y”.
        // > Where the upper filesystem contains an opaque directory, any directory in the
        // > lower filesystem with the same name is ignored.
        // >
        // > An opaque directory should not contain any whiteouts, because they do not serve any purpose.
        // > A merge directory containing regular files with the xattr “trusted.overlay.whiteout”,
        // > should be additionally marked by setting the xattr “trusted.overlay.opaque” to “x” on the
        // > merge directory itself. This is needed to avoid the overhead of checking the “trusted.overlay.whiteout” on
        // > all entries during readdir in the common case.
        //
        //
        //
        let xattrs = self.i.inode(entry.header.inode_offset.get()).xattrs();
        // xattrs.

        todo!()
    }
}

/// Merge two EROFS images into one.
///
/// Based on the lower and upper images, will try to create a new EROFS image where:
/// - files from the upper image will override files from the lower image.
/// - both files from the lower and upper images will be present in the new image.
/// - if a file exists in the lower image but not in the upper image, it will still be present in the new image unless there's an
///   OverlayFS whiteout file for it.
pub fn merge_erofs_image<'i>(
    lower: &ErofsImage,
    upper: &ErofsImage,
) -> Result<ErofsImage<'i>, String> {
    // let mut excluded_files = Vec::new();
    let mut final_image = ErofsImage::from_bytes(&[]); // Create an empty EROFS image to start with
    // Iterate over the lower image files
    for entry in lower.list_files(lower.i.root()) {
        let path = entry.name.to_string_lossy();
        trace!("Processing lower image entry: {path}");

    }


    todo!()
}

pub fn merge_images<'i>(mut images: Vec<ErofsImage<'i>>) -> Result<ErofsImage<'i>, String> {
    if images.is_empty() {
        return Err("No images to merge".to_string());
    }

    let mut merged_image = images.remove(0);

    for image in images {
        merged_image = merge_erofs_image(&merged_image, &image)?;
    }

    Ok(merged_image)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use erofs_sys::{data::backends::uncompressed::UncompressedBackend, superblock::FileSystem};

    fn manifest_dir() -> String {
        env!("CARGO_MANIFEST_DIR").to_string()
    }

    // relative to workspace root
    const EROFS_IMAGE_PATH: &str = "/test/erofs/commit.cfs";
    const EROFS_TEST2_IMAGE_PATH: &str = "/test/erofs/test2.cfs";

    fn get_test_file() -> std::fs::File {
        std::fs::File::open(manifest_dir() + EROFS_IMAGE_PATH).expect("Failed to open test file")
    }
    
    fn get_test2_file() -> std::fs::File {
        std::fs::File::open(manifest_dir() + EROFS_TEST2_IMAGE_PATH).expect("Failed to open test2 file")
    }
    
    #[test]
    fn read_test_file() {
        let mut file = get_test2_file();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        assert!(!buf.is_empty(), "Test file is empty");
        let image = ErofsImage::from_bytes(&buf);
        
        let root_nid = image.root_nid();
        
        image.test().unwrap();
        println!("Root NID: {}", root_nid);
        for entry in image.list_files(image.i.root()) {
            println!("Found Entry: {}", entry.name.to_string_lossy())
        }
    }
    
}