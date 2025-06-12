//! Stratum Patchsets
//!
//! Patchsets are an easy way to manage a collection of patches on top of a stratum.
//!
//! They work by building a tree of commit references in a deterministic order,
//! allowing you to create full commits consisting of multiple layered commits.
//!
//! To use patchsets:
//!
//! - Create a patchset from a file, usually named `*.patchset.toml`

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{commit::StratumRef, store::Store, util::copy_dir_all};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]

pub struct Patchset {
    pub patchset: PatchsetData,
}

impl Patchset {
    /// Creates a new patchset with the given base commit and patches.
    pub fn new(base_commit: Option<String>, patches: Vec<String>) -> Self {
        Self {
            patchset: PatchsetData {
                base_commit,
                patches,
            },
        }
    }

    /// Returns the base commit of the patchset, if any.
    pub fn base_commit(&self) -> Option<StratumRef> {
        self.patchset
            .base_commit
            .as_ref()
            .map(|into| StratumRef::from(into.as_str()))
    }

    /// Returns the list of patches in the patchset.
    pub fn patches(&self) -> Vec<StratumRef> {
        self.patchset
            .patches
            .iter()
            .map(|p| StratumRef::from(p.as_str()))
            .collect::<Vec<_>>()
    }

    pub fn load_patchset_from_file(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read patchset file: {}", e))?;
        toml::from_str(&content).map_err(|e| format!("Failed to parse patchset: {}", e))
    }

    /// Apply a single patch on top of a base commit
    #[tracing::instrument(skip_all)]
    fn apply_patch(
        &self,
        store: &Store,
        patch: &StratumRef,
        base_commit: &StratumRef,
        label: &str,
        is_transient: bool,
    ) -> Result<String, String> {
        tracing::debug!("Applying patch: {} on base: {}", patch, base_commit);

        let patch_mount = tempfile::Builder::new()
            .prefix("stratum_staging_")
            .tempdir_in(store.base_path())
            .map_err(|e| format!("Failed to create staging directory: {}", e))?;

        let copy_workdir = store.new_tempdir();
        tracing::debug!(
            "Creating ephemeral layer at: {}",
            patch_mount.path().display()
        );

        // To work around OverlayFS limitations (We can't use OverlayFS as upperdir)
        // We have to actually copy the files to disk

        // tmpfs is fine, but we want to be prepared for large patches
        {
            tracing::trace!(
                ?patch,
                "Mounting ephemeral layer to copy into staging directory"
            );
            let _eph_mount =
                store.mount_ref_ephemeral(patch, &patch_mount.path().to_string_lossy())?;

            copy_dir_all(patch_mount.path(), copy_workdir.path())
                .map_err(|e| format!("Failed to copy directory: {}", e))?;
        }

        let base_commit_id = base_commit.resolve_commit_id(store)?;
        tracing::debug!(
            "Union patch commit with base: {} and patch: {}",
            base_commit_id,
            patch
        );

        let new_commit = store.union_patch_commit(
            label,
            &copy_workdir.path().to_string_lossy(),
            &base_commit_id,
            is_transient,
        )?;

        tracing::debug!("New commit created: {}", new_commit);
        Ok(new_commit)
    }
    #[tracing::instrument(skip_all)]
    pub fn generate_commit(&self, store: &Store, label: &str) -> Result<StratumRef, String> {
        // base commit can be either the first patch or a separate base commit
        let base_commit = self.base_commit().or_else(|| {
            tracing::warn!("No base commit specified, using first patch as base");
            self.patches().first().cloned()
        });

        let (label, tag) = crate::util::parse_label(label)
            .map_err(|e| format!("Failed to parse label '{}': {}", label, e))?;
        // Actually check if the stratum with the label exists
        if !Path::new(store.base_path())
            .join("refs")
            .join(&label)
            .exists()
        {
            return Err(format!("Stratum with label '{}' does not exist", label));
        }

        tracing::debug!(
            "Generating commit with base: {:?} and patches: {:?}",
            base_commit,
            self.patches()
        );
        let mut transient_commits_to_clean = Vec::new();

        let mut current_commit: Option<StratumRef> = None;
        let mut patches_to_apply = self.patches().to_vec();
        let last_patch = patches_to_apply.last().cloned();

        // If no base commit and using first patch as base, remove it from patches to apply
        if self.base_commit().is_none() && !patches_to_apply.is_empty() {
            patches_to_apply.remove(0);
        }
        // Remove the last patch from the list to apply
        if !patches_to_apply.is_empty() {
            patches_to_apply.pop();
        }

        let mut final_commit: Option<String> = None;

        // Now, let's finally apply each patch in order
        for patch in patches_to_apply {
            let commit_to_layer = if let Some(current) = &current_commit {
                tracing::debug!("Current commit: {}", current);
                current.clone()
            } else {
                tracing::debug!("No current commit, using base commit");
                StratumRef::Commit(
                    base_commit
                        .as_ref()
                        .ok_or_else(|| "No base commit or patches to apply".to_string())?
                        .resolve_commit_id(store)?,
                )
            };

            let transient_label = format!("transient_{}", ulid::Ulid::new().to_string());
            let new_commit_id = self.apply_patch(
                store,
                &patch,
                &commit_to_layer,
                &transient_label,
                true, // is_transient
            )?;

            current_commit = Some(StratumRef::Commit(new_commit_id.clone()));
            // Delete transient ref after use
            std::fs::remove_dir_all(
                Path::new(store.base_path())
                    .join("refs")
                    .join(&transient_label),
            )
            .map_err(|e| {
                format!(
                    "Failed to remove transient ref '{}': {}",
                    transient_label, e
                )
            })?;
            transient_commits_to_clean.push(new_commit_id); // Store commit ID, not label
        }

        // Now apply the final patch (if there is one) using a non-transient commit
        if let Some(final_patch) = last_patch {
            let commit_to_apply = if let Some(current) = &current_commit {
                current.clone()
            } else {
                tracing::warn!("No current commit, using base commit");
                StratumRef::Commit(
                    base_commit
                        .as_ref()
                        .ok_or_else(|| "No base commit or patches to apply".to_string())?
                        .resolve_commit_id(store)?,
                )
            };

            let commit = self.apply_patch(
                store,
                &final_patch,
                &commit_to_apply,
                &label,
                false, // is_transient
            )?;

            tracing::debug!("Final commit created: {}", commit);
            final_commit.replace(commit);
            // tag
            store
                .tag_commit(
                    &label,
                    final_commit.as_ref().unwrap(),
                    &tag.unwrap_or_else(|| {
                        tracing::warn!("No tag specified, using 'latest' as default");
                        "latest".to_string()
                    }),
                )
                .map_err(|e| {
                    format!(
                        "Failed to tag final commit '{}': {}",
                        final_commit.as_ref().unwrap(),
                        e
                    )
                })?;
        }

        // Clean up transient commits
        for transient_commit in transient_commits_to_clean {
            tracing::debug!("Cleaning up transient commit ID: {}", transient_commit);
            store.delete_commit(&transient_commit).map_err(|e| {
                format!(
                    "Failed to remove transient commit '{}': {}",
                    transient_commit, e
                )
            })?;
        }

        // Return the final commit as a StratumRef
        final_commit
            .map(StratumRef::Commit)
            .ok_or_else(|| "No final commit generated".to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PatchsetData {
    pub base_commit: Option<String>,
    pub patches: Vec<String>,
}
