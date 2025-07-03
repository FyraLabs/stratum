use erofs_sys::data::Buffer;
use erofs_sys::file::ImageFileSystem;
use erofs_sys::inode::{Inode, InodeCollection, InodeInfo};
use erofs_sys::superblock::{FileSystem, SuperBlock};
use erofs_sys::xattrs::XAttrSharedEntries;
use erofs_sys::{Nid, PosixResult, operations};
use nix::unistd::read;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::BufRead;
use std::{
    io::{BufReader, Read},
    os::unix::fs::FileExt,
};

// todo:
// - read erofs images and its superblocks
// - get their xattrs so we know where the `trusted.overlayfs.*` points to
// - write our own erofs images with the same data (hopefully)
// - completely replace `mkcomposefs` with them
// - properly implement `fs-verity` hashing functionality (see fsverity.rs)

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ErofsError {
    #[error("EROFS Error: {}", *(.0) as i32)]
    Erofs(erofs_sys::errnos::Errno),
}

impl From<erofs_sys::errnos::Errno> for ErofsError {
    fn from(err: erofs_sys::errnos::Errno) -> Self {
        ErofsError::Erofs(err)
    }
}

// test/erofs/commit.cfs
pub struct ErofsImage {
    // ifs: ImageFileSystem<StFile>,
    sbi: SimpleBufferedFileSystem,
}

impl ErofsImage {
    pub fn from_backend<B: erofs_sys::superblock::FileSystem<SimpleInode> + 'static>(
        backend: B,
    ) -> PosixResult<Self> {
        Ok(Self {
            sbi: erofs_sys::superblock::SuperblockInfo::new(
                Box::new(backend),
                StCollection::default(),
                (),
            ),
        })
    }
    pub fn get_inode_from_path(&mut self, path: &str) -> PosixResult<&mut SimpleInode> {
        operations::lookup(
            &*self.sbi.filesystem,
            &mut self.sbi.inodes,
            self.sbi.filesystem.superblock().root_nid as Nid,
            path,
        )
    }
    
    unsafe fn into_filesystem(self) -> Box<SimpleBufferedFileSystem> {
        unsafe {
            Box::from_raw(Box::into_raw(self.sbi.filesystem).cast::<SimpleBufferedFileSystem>())
        }
    }
    
    /// Write the EROFS image to disk
    pub fn write(&mut self, buf: &mut &[u8]) -> Result<(), ErofsError> {
        todo!()
    }
}

pub struct StFile {
    pub f: std::fs::File,
}

impl erofs_sys::data::Backend for StFile {
    fn fill(
        &self,
        data: &mut [u8],
        device_id: i32,
        offset: erofs_sys::Off,
    ) -> erofs_sys::PosixResult<u64> {
        let i = self
            .f
            .read_at(data, offset as u64)
            .expect("Failed to read from file");
        Ok(i as u64)
    }
}

pub struct RawBuffer {
    pub buf: Vec<u8>,
}

impl erofs_sys::data::Backend for RawBuffer {
    fn fill(
        &self,
        data: &mut [u8],
        device_id: i32,
        offset: erofs_sys::Off,
    ) -> erofs_sys::PosixResult<u64> {
        let start = offset as usize;
        let len = std::cmp::min(data.len(), self.buf.len() - start);
        data.copy_from_slice(&self.buf[start..start + len]);
        Ok(len as u64)
    }
}

impl erofs_sys::data::FileBackend for StFile {}
impl erofs_sys::data::FileBackend for RawBuffer {}

/// File metadata structure, unified from InodeInfo
pub struct DirEntry {
    /// Mode of the file
    pub mode: u16,
    /// File size in bytes
    pub size: u64,
    /// UID of the file owner
    pub uid: u32,
    /// GID of the file owner
    pub gid: u32,
    /// Last modification time
    pub mtime: u32,
    /// Last modification time in nanoseconds
    pub mtime_nsec: Option<u32>,
    /// Number of hard links to this file
    pub nlink: Option<u32>,
    /// Extended attributes shared entries
    pub xattr_shared_entries: XAttrSharedEntries,
}


pub struct SimpleInode {
    pub info: InodeInfo,
    pub xattr_shared_entries: XAttrSharedEntries,
    pub nid: Nid,
}

impl SimpleInode {
    fn file_size(&self) -> u64 {
        self.info.file_size()
    }
    fn is_empty(&self) -> bool {
        self.info.file_size() == 0
    }
    fn is_dir(&self) -> bool {
        self.info.inode_type() == erofs_sys::inode::Type::Directory
    }
}

impl Inode for SimpleInode {
    fn new(_sb: &SuperBlock, info: InodeInfo, nid: Nid, xattr_header: XAttrSharedEntries) -> Self {
        Self {
            info,
            xattr_shared_entries: xattr_header,
            nid,
        }
    }
    fn xattrs_shared_entries(&self) -> &XAttrSharedEntries {
        &self.xattr_shared_entries
    }
    fn nid(&self) -> Nid {
        self.nid
    }
    fn info(&self) -> &InodeInfo {
        &self.info
    }
}

#[derive(Default)]
struct StCollection {
    map: HashMap<Nid, SimpleInode>,
}

impl InodeCollection for StCollection {
    type I = SimpleInode;
    fn iget(&mut self, nid: Nid, f: &dyn FileSystem<Self::I>) -> PosixResult<&mut Self::I> {
        match self.map.entry(nid) {
            Entry::Vacant(v) => {
                let info = f.read_inode_info(nid)?;
                let xattrs_header = f.read_inode_xattrs_shared_entries(nid, &info)?;
                Ok(v.insert(Self::I::new(f.superblock(), info, nid, xattrs_header)))
            }
            Entry::Occupied(o) => Ok(o.into_mut()),
        }
    }
    fn release(&mut self, nid: Nid) {
        self.map.remove_entry(&nid);
    }
}

type SimpleBufferedFileSystem =
    erofs_sys::superblock::SuperblockInfo<SimpleInode, StCollection, ()>;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use erofs_sys::{data::backends::uncompressed::UncompressedBackend, superblock::FileSystem};

    fn manifest_dir() -> String {
        env!("CARGO_MANIFEST_DIR").to_string()
    }

    use super::*;
    // relative to workspace root
    const EROFS_IMAGE_PATH: &str = "../../test/erofs/commit.cfs";

    #[test]
    fn erofs_raw_bytes() {
        let raw_bytes = include_bytes!("../../test/erofs/commit.cfs");
        let stfile = RawBuffer {
            buf: raw_bytes.to_vec(),
        };
        let image = ImageFileSystem::try_new(stfile).unwrap();
        let mut sbi: SimpleBufferedFileSystem = erofs_sys::superblock::SuperblockInfo::new(
            Box::new(image),
            StCollection::default(),
            (),
        );
        let inode = operations::lookup(
            &*sbi.filesystem,
            &mut sbi.inodes,
            sbi.filesystem.superblock().root_nid as Nid,
            "/",
        )
        .unwrap();

        println!("Root Inode NID: {}", inode.nid());
    }

    #[test]
    fn erofs_stfile() {
        let stfile = StFile {
            f: std::fs::File::open(manifest_dir() + "/test/erofs/commit.cfs").unwrap(),
        };
        let image = ImageFileSystem::try_new(stfile).unwrap();
        let mut sbi: SimpleBufferedFileSystem = erofs_sys::superblock::SuperblockInfo::new(
            Box::new(image),
            StCollection::default(),
            (),
        );
        let inode = operations::lookup(
            &*sbi.filesystem,
            &mut sbi.inodes,
            sbi.filesystem.superblock().root_nid as Nid,
            "/",
        )
        .unwrap();

        println!("Root Inode NID: {}", inode.nid());
    }
}
