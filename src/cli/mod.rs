mod patchset;
mod worktree;
use crate::commit::StratumRef;
use crate::util::{self};
use clap::{Parser, Subcommand};
use rustix::process::getuid;
use std::path::PathBuf;

#[derive(Parser, Debug)]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
#[clap(version, about, author)]
pub enum Commands {
    /// Import a directory as a new stratum commit
    #[clap(name = "import", aliases = &["i"])]
    Import {
        /// Directory to import from
        #[clap(value_parser)]
        directory: PathBuf,

        /// Name for the stratum (label)
        #[clap(value_parser)]
        name: String,

        /// Import as a bare directory (default behavior)
        #[clap(long)]
        bare: bool,

        /// Import as a patch on top of an existing stratum
        #[clap(long)]
        patch: Option<StratumRef>,
    },
    #[clap(name = "remove", aliases = &["rm", "del", "delete", "r", "d"])]
    /// Remove a stratum reference (commit ID or tag)
    Remove {
        /// Stratum reference to remove (commit ID or tag)
        #[clap(value_parser)]
        stratum_ref: StratumRef,
    },

    /// Create a tag pointing to a specific commit
    #[clap(name = "tag", aliases = &["t"])]
    Tag {
        /// Source stratum reference (commit ID or existing tag)
        #[clap(value_parser)]
        source: StratumRef,

        /// New tag name to create
        #[clap(value_parser)]
        new_tag: String,
    },

    /// Remove a tag from a stratum
    #[clap(name = "untag", aliases = &["ut"])]
    Untag {
        #[clap(value_parser)]
        /// The tag to remove
        tag: String,
    },

    /// Mount a stratum at a given path
    #[clap(name = "mount", aliases = &["mnt", "m"])]
    Mount {
        /// The stratum reference to mount (supports format: stratum_ref:tag or stratum_ref+worktree)
        #[clap(value_parser)]
        stratum_ref: StratumRef,

        /// The path to mount the stratum at (optional, will auto-generate if not provided)
        #[clap(value_parser)]
        mountpoint: Option<PathBuf>,
    },

    #[clap(name = "unmount", aliases = &["umount", "um", "u", "umnt"])]
    /// Unmount a stratum volume from a given path
    Unmount {
        /// The path to unmount the stratum from
        #[clap(value_parser)]
        mountpoint: PathBuf,
    },

    /// Manage worktrees
    #[clap(subcommand, name = "worktree", alias = "wt")]
    Worktree(worktree::WorktreeCommand),

    /// Patchset management commands
    #[clap(subcommand, name = "patchset", alias = "ps")]
    Patchset(patchset::PatchsetCommand),
}

#[cfg(debug_assertions)]
const BASE_PATH: &str = "dev/stratum";
#[cfg(not(debug_assertions))]
const BASE_PATH: &str = "/var/lib/stratum";

impl Cli {
    pub fn run(self) -> Result<(), String> {
        let store = crate::store::Store::new(BASE_PATH.to_string());
        tracing::trace!("Running command: {:?}", self.command);
        match self.command {
            Commands::Import {
                directory,
                name,
                bare, // todo: handle bare import
                patch,
            } => {
                if !bare {
                    unimplemented!(
                        "Export file import is not yet implemented, please use --bare option for now."
                    );
                }

                if !directory.is_dir() {
                    return Err(format!("{} is not a directory", directory.display()));
                }

                tracing::info!(
                    "Importing directory: {} with label: {}",
                    directory.display(),
                    name
                );

                let (stratum_label, tag) = util::parse_label(&name)
                    .map_err(|e| format!("Failed to parse label '{}': {}", name, e))?;

                // Import the directory and get the commit ID
                let parent_commit = if let Some(patch_ref) = patch {
                    let commit_id = patch_ref.resolve_commit_id(&store)?;
                    store.union_patch_commit(
                        &stratum_label,
                        &directory.to_string_lossy(),
                        &commit_id,
                        false,
                    )?
                } else {
                    store.commit_directory_bare(
                        &stratum_label,
                        &directory.to_string_lossy(),
                        None,
                        false,
                    )?
                };

                let commit_id = parent_commit;

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
            Commands::Tag { source, new_tag } => {
                let commit_id = source.resolve_commit_id(&store).map_err(|e| {
                    format!("Failed to resolve source commit ID '{:?}': {}", source, e)
                })?;

                let (target_label, target_tag) = util::parse_label(&new_tag)
                    .map_err(|e| format!("Failed to parse target label '{}': {}", new_tag, e))?;
                let target_tag = target_tag.unwrap_or("latest".to_string());

                tracing::info!("Tagging commit {} with tag '{}'", commit_id, new_tag);
                store
                    .tag_commit(&target_label, &commit_id, &target_tag)
                    .map_err(|e| format!("Failed to tag commit '{}': {}", commit_id, e))?;
                println!("Tagged commit {} with '{}'", commit_id, new_tag);
                Ok(())
            }
            Commands::Untag { tag } => {
                let (label, tag_name) = util::parse_label(&tag)
                    .map_err(|e| format!("Failed to parse tag '{}': {}", tag, e))?;
                let tag_name = tag_name.ok_or_else(|| {
                    "No tag provided, please provide in form of volume:tag".to_string()
                })?;

                tracing::info!("Removing tag '{}'", tag);
                store
                    .untag(&tag_name, &label)
                    .map_err(|e| format!("Failed to remove tag '{}': {}", tag, e))?;
                println!("Removed tag '{}'", tag);
                Ok(())
            }
            Commands::Mount {
                stratum_ref,
                mountpoint,
            } => {
                // Generate mountpoint if not provided
                let mount_path = if let Some(mp) = mountpoint {
                    mp.to_string_lossy().to_string()
                } else {
                    // Generate auto mountpoint based on design: /run/user/<uid>/stratum/<stratum_ref>
                    let uid = getuid();
                    let auto_mountpoint =
                        format!("/run/user/{}/stratum/{}", uid.as_raw(), stratum_ref);
                    std::fs::create_dir_all(&auto_mountpoint).map_err(|e| {
                        format!(
                            "Failed to create auto mountpoint {}: {}",
                            auto_mountpoint, e
                        )
                    })?;
                    println!("{}", auto_mountpoint); // Print auto-generated mountpoint to stdout
                    auto_mountpoint
                };

                tracing::info!(
                    "Mounting stratum reference {:?} at {}",
                    stratum_ref,
                    mount_path
                );

                // Extract worktree from stratum_ref using pattern matching
                let worktree = match &stratum_ref {
                    crate::commit::StratumRef::Worktree { label: _, worktree } => {
                        Some(worktree.as_str())
                    }
                    crate::commit::StratumRef::Tag(_) | crate::commit::StratumRef::Commit(_) => {
                        None
                    }
                };

                store.mount_ref(&stratum_ref, &mount_path, worktree)
            }
            Commands::Unmount { mountpoint } => {
                tracing::info!("Unmounting stratum from {}", mountpoint.display());
                store
                    .unmount_ref(&mountpoint.to_string_lossy())
                    .map_err(|e| {
                        format!(
                            "Failed to unmount stratum from '{}': {}",
                            mountpoint.display(),
                            e
                        )
                    })?;
                println!("Unmounted stratum from {}", mountpoint.display());
                Ok(())
            }
            Commands::Worktree(command) => {
                // Delegate to the worktree command handler
                command
                    .execute(&store)
                    .map_err(|e| format!("Worktree command failed: {}", e))
            }
            Commands::Patchset(command) => {
                // Delegate to the patchset command handler
                command
                    .execute(&store)
                    .map_err(|e| format!("Patchset command failed: {}", e))
            }
            Commands::Remove { stratum_ref } => {
                // todo: safety check: duplicate commits?
                tracing::info!("Removing stratum reference: {:?}", stratum_ref);
                let commit_id = stratum_ref.resolve_commit_id(&store).map_err(|e| {
                    format!("Failed to resolve commit ID for '{:?}': {}", stratum_ref, e)
                })?;
                store
                    .delete_commit(&commit_id)
                    .map_err(|e| format!("Failed to remove stratum '{}': {}", stratum_ref, e))?;
                println!("Removed stratum reference: {}", stratum_ref);
                Ok(())
            }
        }
    }
}
