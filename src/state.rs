//! Ephemeral state management for Stratum.
//! 
//! This keeps track of which stratum is being mounted, and where/how it was being mounted,
//! in order to support unmounting and garbage collection.
//! 
//! This state should be stored in tmpfs, as mounts are ephemeral and should not persist
//! across reboots.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use bincode::{Decode, Encode};
use crate::commit::StratumRef;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum StratumMountRef {
    /// A writable worktree
    Worktree(String),
    /// A read-only snapshot of a stratum
    Snapshot(StratumRef),
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct MountedStratum {
    /// The stratum reference being mounted
    /// 
    /// This has to be a name to a stratus ref (i.e )
    pub stratum_ref: StratumMountRef,
    /// The mount point where the stratum is mounted
    pub mount_point: PathBuf,
    /// Whether the mount is read-only
    /// 
    /// Note: there should only be one read-write mount per stratum,
    /// mounted snapshots should always be read-only.
    pub read_only: bool,
}