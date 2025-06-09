use crate::commit::StratumRef;
use crate::util::{self};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
#[clap(version, about, author)]
pub enum Commands {
    /// Import a directory as a new stratum,
    /// optionally tagging it for layering on top of an existing commit.
    ///
    Import {
        /// Label for the import operation.
        ///
        /// This label should be in the format "stratum:tag"
        /// where "stratum" is the name of the stratum and "tag" is an optional tag.
        /// If no tag is provided, it defaults to "latest".
        // positonal arg
        // todo: Import to HEAD, remount and commit as lower dir but still below
        // HEAD's upperdir, may need to create new temp overlayfs
        #[clap(value_parser)]
        label: String,

        /// Directory to import from
        ///
        /// The layering will be relative to this directory.
        #[clap(value_parser)]
        dir: PathBuf,

        /// Optional ref to layer this import on top of.
        #[clap(long)]
        source_ref: Option<StratumRef>,
    },

    Tag {
        /// Source ref to tag
        #[clap(value_parser)]
        source: StratumRef,
        /// Target tag to apply to
        #[clap(value_parser)]
        target: String,
    },

    /// Mount a stratum at a given path.
    Mount {
        /// The reference to mount
        #[clap(value_parser)]
        stratum_ref: StratumRef,

        /// The path to mount the stratum at
        #[clap(value_parser)]
        mountpoint: PathBuf,
    },
}

const BASE_PATH: &str = "dev/stratum";

impl Cli {
    pub fn run(self) -> Result<(), String> {
        let store = crate::store::Store::new(BASE_PATH.to_string());
        tracing::trace!("Running command: {:?}", self.command);
        match self.command {
            Commands::Import {
                dir,
                label,
                source_ref,
            } => {
                if !dir.is_dir() {
                    return Err(format!("{} is not a directory", dir.display()));
                }

                tracing::info!(
                    "Importing directory: {} with label: {}",
                    dir.display(),
                    label
                );

                let (stratum_label, tag) = util::parse_label(&label)
                    .map_err(|e| format!("Failed to parse label '{}': {}", label, e))?;

                // Import the directory and get the commit ID
                let commit_id =
                    store.import_directory(&stratum_label, &dir.to_string_lossy(), None)?;

                // Always tag the commit - use provided tag or default to "latest"
                let tag_name = tag.unwrap_or_else(|| "latest".to_string());
                store
                    .tag_commit(&stratum_label, &commit_id, &tag_name)
                    .map_err(|e| format!("Failed to tag commit '{}': {}", commit_id, e))?;

                tracing::info!(
                    "Tagged commit {} as {}:{}",
                    commit_id,
                    stratum_label,
                    tag_name
                );

                println!("{}  (tagged as {}:{})", commit_id, stratum_label, tag_name);
                Ok(())
            }
            Commands::Tag { source, target } => {
                let (target_label, target_tag) = util::parse_label(&target)
                    .map_err(|e| format!("Failed to parse target label '{}': {}", target, e))?;
                let target_tag = target_tag.unwrap_or("latest".to_string());

                let commit_id = source.resolve_commit_id(&store).map_err(|e| {
                    format!("Failed to resolve source commit ID '{:?}': {}", source, e)
                })?;
                tracing::info!("Tagging commit {} with tag '{}'", commit_id, target);
                store
                    .tag_commit(&target_label, &commit_id, &target_tag)
                    .map_err(|e| format!("Failed to tag commit '{}': {}", commit_id, e))?;
                println!("Tagged commit {} with '{}'", commit_id, target);
                Ok(())
            }
            Commands::Mount {
                stratum_ref,
                mountpoint,
            } => {
                tracing::info!(
                    "Mounting stratum reference {:?} at {}",
                    stratum_ref,
                    mountpoint.display()
                );
                // todo: get or make new head :D
                store.mount_ref(&stratum_ref, &mountpoint.to_string_lossy())
            }
        }
    }
}
