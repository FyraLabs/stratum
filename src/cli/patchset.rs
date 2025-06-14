use crate::{patchset::Patchset, store::Store};
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
pub enum PatchsetCommand {
    #[clap(name = "build", aliases = &["b"])]
    Build {
        /// Path to the patchset definition file
        #[clap(value_parser)]
        patchset_file: PathBuf,

        /// Stratum tag to link the patchset as
        #[clap(value_parser)]
        tag: String,
    },
}

impl PatchsetCommand {
    /// Execute the patchset command
    pub fn execute(self, store: &Store) -> Result<(), String> {
        match self {
            Self::Build { patchset_file, tag } => {
                let patchset = Patchset::load_patchset_from_file(&patchset_file)?;
                let artifact = patchset
                    .generate_commit(store, &tag)
                    .map_err(|e| format!("Failed to generate commit from patchset: {e}"))?;
                tracing::info!(
                    "Patchset '{}' built successfully with tag '{}'",
                    patchset_file.display(),
                    tag
                );
                // resolve commit from patchset
                let commit_id = artifact.resolve_commit_id(store)?;
                tracing::info!("Patchset commit ID: {}", commit_id);

                println!("{commit_id} ({tag})");
                Ok(())
            }
        }
    }
}
