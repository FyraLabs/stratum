//! adapter for composefs-rs EROFS functionality
//!
//! So we can create, read and write our own EROFS images
//!
//! important for ComposeFS itself, since it uses EROFS to store the directory tree
//! and we want to be able to read and write them
use composefs::erofs::reader::{DirectoryEntry, InodeHeader, InodeOps, InodeType};
use rustix::path::Arg;
use tracing::trace;
use zerocopy::FromBytes;

pub struct ErofsImage<'i> {
    pub i: composefs::erofs::reader::Image<'i>,
}

/// Get the inline data of an inode.
pub fn inode_inline<'img>(inode: &composefs::erofs::reader::InodeType<'img>) -> &'img [u8] {
    match inode {
        composefs::erofs::reader::InodeType::Compact(inode) => {
            &inode.data[inode.header.xattr_size()..]
        }
        composefs::erofs::reader::InodeType::Extended(inode) => {
            &inode.data[inode.header.xattr_size()..]
        }
    }
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

    pub fn list_recursive_files<'a>(
        &'a self,
        inode: InodeType<'a>,
        yield_dir: bool,
    ) -> impl Iterator<Item = DirectoryEntry<'a>> {
        assert!(inode.mode().is_dir());
        self.list_files_with_owned_inode(inode)
            .filter(|entry| entry.name != b"." && entry.name != b"..")
            .flat_map(move |entry| {
                let child_inode = self.i.inode(entry.header.inode_offset.get());
                if child_inode.mode().is_dir() {
                    // For directories, yield the entry and then recurse
                    if yield_dir {
                        Box::new(
                            std::iter::once(entry)
                                .chain(self.list_recursive_files(child_inode, yield_dir)),
                        )
                    } else {
                        Box::new(self.list_recursive_files(child_inode, yield_dir))
                            as Box<dyn Iterator<Item = DirectoryEntry<'a>>>
                    }
                } else {
                    // For files, just yield the entry
                    Box::new(std::iter::once(entry))
                }
            })
    }

    pub fn list_files_with_owned_inode<'a>(
        &'a self,
        inode: InodeType<'a>,
    ) -> impl Iterator<Item = DirectoryEntry<'a>> {
        assert!(inode.mode().is_dir());
        match (inode.data_layout(), inode.size()) {
            (composefs::erofs::format::DataLayout::ChunkBased, 0) if inode.inline().is_empty() => {
                Box::new(std::iter::empty()) as Box<dyn Iterator<Item = DirectoryEntry<'_>>>
            }
            (composefs::erofs::format::DataLayout::ChunkBased, 0) => Box::new(
                composefs::erofs::reader::DirectoryBlock::ref_from_bytes(inode_inline(&inode))
                    .unwrap()
                    .entries(),
            ),
            _ => {
                // Directory uses external blocks
                Box::new(
                    inode
                        .blocks(self.i.sb.blkszbits)
                        .map(|blkid| self.i.directory_block(blkid))
                        .flat_map(|dirblk| dirblk.entries()),
                )
            }
        }
    }

    pub fn list_files<'a, 'b>(
        &'a self,
        inode: &'b InodeType<'a>,
    ) -> impl Iterator<Item = DirectoryEntry<'a>> {
        assert!(inode.mode().is_dir());
        match (inode.data_layout(), inode.size()) {
            (composefs::erofs::format::DataLayout::ChunkBased, 0) if inode.inline().is_empty() => {
                Box::new(std::iter::empty()) as Box<dyn Iterator<Item = DirectoryEntry<'a>>>
            }
            (composefs::erofs::format::DataLayout::ChunkBased, 0) => Box::new(
                composefs::erofs::reader::DirectoryBlock::ref_from_bytes(inode_inline(inode))
                    .unwrap()
                    .entries(),
            ),
            _ => {
                // Directory uses external blocks
                Box::new(
                    inode
                        .blocks(self.i.sb.blkszbits)
                        .map(|blkid| self.i.directory_block(blkid))
                        .flat_map(|dirblk| dirblk.entries()),
                )
            }
        }
    }

    pub fn test(&self) -> Result<(), String> {
        // This is just a test function to ensure the ErofsImage can be created and read.
        // You can implement more specific tests as needed.
        let root_nid = self.root_nid();

        let root = self.i.root();

        println!("root: {:?}", root);

        Ok(())
    }

    /// Get the value of `trusted.overlay.redirect` xattr from the root inode.
    ///
    ///
    /// Returns the path to the object in the store, relative to the store's
    /// digest store path, or None if the xattr is not set.
    ///
    /// example: 00/123467890deadbeef
    pub fn get_overlay_redirect(&self, nid: u64) -> Result<Option<String>, String> {
        let inode = self.i.inode(nid);
        let xattrs = inode.xattrs();
        if let Some(xattrs) = xattrs {
            let mut l = xattrs.local();

            if let Some(attr) = l.find(|attr| attr.suffix().to_string_lossy() == "overlay.redirect")
            {
                return Ok(Some(attr.value().to_string_lossy().to_string()));
            }
        }
        Ok(None)
    }

    pub fn get_xattrs(
        &self,
        nid: u64,
    ) -> Result<std::collections::HashMap<String, Vec<u8>>, String> {
        let inode = self.i.inode(nid);
        let xattrs = inode.xattrs();
        if let Some(xattrs) = xattrs {
            let mut map = std::collections::HashMap::new();
            for attr in xattrs.local() {
                map.insert(
                    attr.suffix().to_string_lossy().to_string(),
                    attr.value().to_vec(),
                );
            }
            Ok(map)
        } else {
            Err(format!("No xattrs found for inode {nid}"))
        }
    }

    // https://github.com/ToolmanP/erofs-rs/blob/fb5301b3cc46b909de32f86ea246eb9ecff741cf/erofs-sys/src/operations.rs#L33-L59
    pub fn get_nid_from_path(&self, path: &str) -> Result<u64, String> {
        let mut nid = self.root_nid();
        for part in path.as_bytes().split(|&b| b == b'/') {
            if part.is_empty() {
                continue;
            }
            let inode = self.i.inode(nid);
            let entry = self
                .list_files(&inode)
                .find(|entry| entry.name == part)
                .ok_or_else(|| format!("Path '{path}' not found in EROFS image"))?;
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
    lower: &ErofsImage<'i>,
    upper: &ErofsImage<'i>,
) -> Result<ErofsImage<'i>, String> {
    // let mut excluded_files = Vec::new();
    let mut final_image = lower;
    // - clone the lower image to start with
    // - for each file in the upper image:
    //   - check if it exists in the lower image
    //   - if so, change the xattrs to whatever exists in the upper image
    //   - if not, add the file from the upper image to the final image somehow
    //   - if the file is a whiteout, remove the file from the final image
    // -
    // Iterate over the upper image files
    //
    // todo: recursive now that the list_files function is in fact, not recursive
    for entry in upper.list_files(&upper.i.root()) {
        let path = entry.name.to_string_lossy();
        trace!("Processing upper image entry: {path}");
    }
    todo!()
    // Ok(final_image.clone())
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
    use erofs_sys::{data::backends::uncompressed::UncompressedBackend, superblock::FileSystem};
    use std::collections::HashMap;
    use zerocopy::FromBytes;

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
        std::fs::File::open(manifest_dir() + EROFS_TEST2_IMAGE_PATH)
            .expect("Failed to open test2 file")
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
        for entry in image.list_files(&image.i.root()) {
            println!("Found Entry: {}", entry.name.to_string_lossy())
        }
        let app_nid = image
            .get_nid_from_path("/App")
            .expect("Failed to get NID for /App");
        println!("NID for /App: {}", app_nid);

        for entry in image.list_files(&image.i.inode(app_nid)) {
            println!(
                "Entry: {} (NID: {})",
                entry.name.to_string_lossy(),
                entry.header.inode_offset.get()
            );
        }

        const TEST_NID: u64 = 1845;
        let inode = image.i.inode(TEST_NID);
        // Test get_overlay_redirect on this inode
        let redirect = image.get_overlay_redirect(TEST_NID).unwrap();
        println!("trusted.overlay.redirect: {:?}", redirect);
        let xattrs = image.get_xattrs(TEST_NID).unwrap();
        println!("Xattrs for NID {TEST_NID}:");
        for (key, value) in xattrs {
            println!("{}: {:x?}", key, value);
        }
    }
}
