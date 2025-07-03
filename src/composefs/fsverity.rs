//! our own fs-verity implementation for Stratum/ComposeFS
//! hopefully should be compatible with upstream composefs
use sha2::Digest;
// use composefs::fsverity::FsVerityHashValue;
#[derive(Debug)]
pub struct VerityLayer {
    // digest is context
    pub context: Vec<u8>,
    remaining: usize,
}
