//! Ephemeral state management for Stratum.
//!
//! This keeps track of which stratum is being mounted, and where/how it was being mounted,
//! in order to support unmounting and garbage collection.
//!
//! This state should be stored in tmpfs, as mounts are ephemeral and should not persist
//! across reboots.

use crate::commit::StratumRef;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub enum StratumMountRef {
    /// A writable worktree
    Worktree { label: String, worktree: String },
    /// A read-only snapshot of a stratum
    Snapshot(StratumRef),
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct MountedStratum {
    /// The stratum reference being mounted
    pub stratum_ref: StratumMountRef,
    /// The mount point where the stratum is mounted
    pub mount_point: PathBuf,
    /// Whether the mount is read-only
    ///
    /// Note: there should only be one read-write mount per stratum,
    /// mounted snapshots should always be read-only.
    pub read_only: bool,
    /// Resolved path to the stratum commit
    pub base_commit: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode, Default)]
pub struct StratumState {
    /// Map of mount points to mounted strata
    pub mounts: HashMap<PathBuf, MountedStratum>,
}

pub struct StateManager {
    state_file: PathBuf,
}

impl StateManager {
    const STATE_DIR: &'static str = "/run/stratum";
    const STATE_FILE: &'static str = "state";

    pub fn new() -> Result<Self, String> {
        let state_dir = Path::new(Self::STATE_DIR);
        std::fs::create_dir_all(state_dir)
            .map_err(|e| format!("Failed to create state directory: {}", e))?;

        let state_file = state_dir.join(Self::STATE_FILE);

        Ok(StateManager { state_file })
    }

    /// Check if any commit with  the given ID is currently mounted
    pub fn get_commit_mounted(
        &self,
        commit_id: &str,
    ) -> Result<bool, String> {
        let state = self.load_state()?;
        Ok(state.mounts.values().any(|m| m.base_commit == commit_id))
    }

    /// Load the current state from disk
    pub fn load_state(&self) -> Result<StratumState, String> {
        if !self.state_file.exists() {
            return Ok(StratumState::default());
        }

        let content = std::fs::read(&self.state_file)
            .map_err(|e| format!("Failed to read state file: {}", e))?;

        let state: StratumState = bincode::decode_from_slice(&content, bincode::config::standard())
            .map_err(|e| format!("Failed to parse state file: {}", e))?
            .0;

        Ok(state)
    }

    /// Save the current state to disk
    pub fn save_state(&self, state: &StratumState) -> Result<(), String> {
        let content = bincode::encode_to_vec(state, bincode::config::standard())
            .map_err(|e| format!("Failed to serialize state: {}", e))?;

        std::fs::write(&self.state_file, content)
            .map_err(|e| format!("Failed to write state file: {}", e))?;

        Ok(())
    }

    /// Add a mounted stratum to the state
    pub fn add_mount(
        &self,
        mount_point: PathBuf,
        mounted_stratum: MountedStratum,
    ) -> Result<(), String> {
        let mut state = self.load_state()?;
        state.mounts.insert(mount_point, mounted_stratum);
        self.save_state(&state)?;
        Ok(())
    }

    /// Remove a mounted stratum from the state
    pub fn remove_mount(&self, mount_point: &Path) -> Result<(), String> {
        let mut state = self.load_state()?;
        state.mounts.remove(mount_point);
        self.save_state(&state)?;
        Ok(())
    }

    /// Find a mounted stratum by worktree
    pub fn find_mount_by_worktree(
        &self,
        label: &str,
        worktree: &str,
    ) -> Result<Option<PathBuf>, String> {
        let state = self.load_state()?;

        for (mount_point, mounted_stratum) in state.mounts.iter() {
            if let StratumMountRef::Worktree {
                label: mount_label,
                worktree: mount_worktree,
            } = &mounted_stratum.stratum_ref
            {
                if mount_label == label && mount_worktree == worktree {
                    return Ok(Some(mount_point.clone()));
                }
            }
        }

        Ok(None)
    }

    /// Find a mounted stratum by mount point
    pub fn find_mount_by_path(&self, mount_point: &Path) -> Result<Option<MountedStratum>, String> {
        let state = self.load_state()?;
        Ok(state.mounts.get(mount_point).cloned())
    }

    /// Check if a worktree is currently mounted
    pub fn is_worktree_mounted(&self, label: &str, worktree: &str) -> Result<bool, String> {
        Ok(self.find_mount_by_worktree(label, worktree)?.is_some())
    }

    /// Get all current mounts
    pub fn get_all_mounts(&self) -> Result<HashMap<PathBuf, MountedStratum>, String> {
        let state = self.load_state()?;
        Ok(state.mounts)
    }
}
