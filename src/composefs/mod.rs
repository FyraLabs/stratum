//! ComposeFS-rs high-level adapters
//! based on `composefs-rs` crate, minus the higher level `Repository` module.
//!
//! rationale: we can't depend on their high-level `Repository` structure, because
//! our `stratum::Store` is simply just different.
//!
//! All we want is simply just the EROFS editing functionality and the
//! Digest Store functionality, which is what this module should port stuff for

// analogies:
// - `Repository` in `composefs-rs` is like `Store` in stratum
// - `images` in `Repository` is like `Commit` in stratum, their version of images are just erofs images we use for storing commit data iirc
//
// todo: completely rewrite our merkel tree hashing algo to use FsVerityHash instead of our own algo
// implications:
// theoretically commits should(can) be backward-compatible when reading pre-FsVerity commits, but creating new commits will not use our merkle algo anymore so
// new commits will now have different hashes if this is implemented, which is fine
// this way when getting commit IDs it will now be uniform from reading `composefs-info measure-file` instead of it being completely different (current behavior)
//
// todo:
// - re-implement their fs-verity hashing functionality
// - make our own composefs-erofs images
// - read them
// - merge them with commits
// - write them back to disk

use composefs::fsverity::FsVerityHashValue;
pub mod erofs_old;
pub mod erofs;
pub mod fsverity;

// todo: Actually re-implement `mkcomposefs --digest-store` functionality
// todo: move mount/composefs.rs to here
pub struct DigestStore {
    pub path: String,
}

impl DigestStore {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}
