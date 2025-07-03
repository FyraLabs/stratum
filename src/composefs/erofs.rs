//! adapter for composefs-rs EROFS functionality
//!
//! So we can create, read and write our own EROFS images
//!
//! important for ComposeFS itself, since it uses EROFS to store the directory tree
//! and we want to be able to read and write them
use composefs::erofs::reader::InodeHeader;
use composefs::erofs::reader::InodeOps;
use composefs::{erofs::reader::DirectoryEntry, fsverity::FsVerityHashValue};
use libc::_SC_CHILD_MAX;
use libc::B0;
use rustix::path::Arg;
use std::io::Read;
use std::rc::Rc;
use tracing::trace;
use zerocopy::FromBytes;

pub struct ErofsImage<'i> {
    i: composefs::erofs::reader::Image<'i>,
}
/// Iterator for recursively traversing all files and directories in a directory tree.
pub struct IterRecursiveFiles<'a> {
    image: &'a ErofsImage<'a>,
    stack: Vec<(
        Rc<composefs::erofs::reader::InodeType<'a>>,
        Box<dyn Iterator<Item = composefs::erofs::reader::DirectoryEntry<'a>> + 'a>,
    )>,
}

impl<'a> IterRecursiveFiles<'a> {
    pub fn new(image: &'a ErofsImage<'a>, inode: composefs::erofs::reader::InodeType<'a>) -> Self {
        let inode = Rc::new(inode);
        let mut stack = Vec::new();
        if inode.mode().is_dir() {
            let iter = image
                .list_files_rc(Rc::clone(&inode))
                .filter(|entry| entry.name != b"." && entry.name != b"..");
            stack.push((Rc::clone(&inode), Box::new(iter) as _));
        }
        IterRecursiveFiles { image, stack }
    }
}

impl<'a> Iterator for IterRecursiveFiles<'a> {
    type Item = composefs::erofs::reader::DirectoryEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((_, iter)) = self.stack.last_mut() {
            if let Some(entry) = iter.next() {
                let child_inode = self.image.i.inode(entry.header.inode_offset.get());
                let child_inode = Rc::new(child_inode);
                if child_inode.mode().is_dir() {
                    let sub_iter = self
                        .image
                        .list_files_rc(Rc::clone(&child_inode))
                        .filter(|entry| entry.name != b"." && entry.name != b"..");
                    self.stack
                        .push((Rc::clone(&child_inode), Box::new(sub_iter)));
                }
                return Some(entry);
            } else {
                self.stack.pop();
            }
        }
        None
    }
}

pub struct IterFiles<'a> {
    _inode: Rc<composefs::erofs::reader::InodeType<'a>>,
    state: IterFilesState<'a>,
}

enum IterFilesState<'a> {
    Empty,
    Inline(std::vec::IntoIter<DirectoryEntry<'a>>),
    ExternalBlocks {
        mut_blocks: Box<dyn Iterator<Item = &'a composefs::erofs::reader::DirectoryBlock> + 'a>,
        cur_entries: Option<composefs::erofs::reader::DirectoryEntries<'a>>,
    },
}

impl<'a> IterFiles<'a> {
    pub fn new(
        image: &'a ErofsImage<'a>,
        inode: Rc<composefs::erofs::reader::InodeType<'a>>,
    ) -> Self {
        use IterFilesState::*;
        use composefs::erofs::format::DataLayout;

        let _inode = Rc::clone(&inode);

        let state = match (inode.data_layout(), inode.size()) {
            (DataLayout::ChunkBased, 0) if inode.inline().is_empty() => Empty,
            (DataLayout::ChunkBased, 0) => {
                // Extract entries BEFORE constructing the struct
                let entries: Vec<_> = {
                    let block =
                        composefs::erofs::reader::DirectoryBlock::ref_from_bytes(inode.inline())
                            .unwrap();
                    block.entries().collect()
                };
                Inline(entries.into_iter())
            }
            _ => {
                let blocks = inode
                    .blocks(image.i.sb.blkszbits)
                    .map(move |blkid| image.i.directory_block(blkid));
                ExternalBlocks {
                    mut_blocks: Box::new(blocks),
                    cur_entries: None,
                }
            }
        };
        Self { _inode, state }
    }
}

impl<'a> Iterator for IterFiles<'a> {
    type Item = composefs::erofs::reader::DirectoryEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        use IterFilesState::*;
        match &mut self.state {
            Empty => None,
            Inline(iter) => iter.next(),
            ExternalBlocks {
                mut_blocks,
                cur_entries,
            } => loop {
                if let Some(entries) = cur_entries {
                    if let Some(entry) = entries.next() {
                        return Some(entry);
                    }
                }
                // Advance to next block
                match mut_blocks.next() {
                    Some(block) => {
                        *cur_entries = Some(block.entries());
                    }
                    None => return None,
                }
            },
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
        inode: composefs::erofs::reader::InodeType<'a>,
        // yield_dir: bool,
    ) -> IterRecursiveFiles<'a> {
        IterRecursiveFiles::new(self, inode)
    }

    pub fn list_files_rc<'a>(
        &'a self,
        inode: Rc<composefs::erofs::reader::InodeType<'a>>,
    ) -> IterFiles<'a> {
        assert!(inode.mode().is_dir());
        IterFiles::new(self, inode)
    }

    pub fn list_files<'a>(
        &'a self,
        inode: &'a composefs::erofs::reader::InodeType<'a>,
    ) -> impl Iterator<Item = composefs::erofs::reader::DirectoryEntry<'a>> + use<'a> {
        assert!(inode.mode().is_dir());
        match (inode.data_layout(), inode.size()) {
            (composefs::erofs::format::DataLayout::ChunkBased, 0) if inode.inline().is_empty() => {
                Box::new(std::iter::empty())
                    as Box<dyn Iterator<Item = composefs::erofs::reader::DirectoryEntry<'a>>>
            }
            (composefs::erofs::format::DataLayout::ChunkBased, 0) => Box::new(
                composefs::erofs::reader::DirectoryBlock::ref_from_bytes(inode.inline())
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
