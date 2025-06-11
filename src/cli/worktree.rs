use crate::commit::StratumRef;
use crate::store::Store;
use clap::Parser;

#[derive(Parser, Debug)]
pub enum WorktreeCommand {
    /// Create a new worktree
    /// Creates a new worktree with the given name and base commit
    #[clap(
        name = "add",
        aliases = &["create", "new", "init", "c" , "n", "i"],
    )]
    Add {
        /// Name of the worktree
        ///
        /// This only accepts a Stratum reference in the format: `label+worktree_name`
        /// where `label` is the name of the stratum and `worktree_name` is the name of the worktree.
        name: StratumRef,
        /// Base commit ID to use for this worktree
        base_commit: StratumRef,
        /// Optional description for the worktree
        #[clap(long, value_parser)]
        description: Option<String>,
    },
    /// List all worktrees
    #[clap(name = "list", aliases = &["ls", "l"])]
    List {
        /// Optional filter to show only worktrees of this stratum
        #[clap(long, value_parser)]
        stratum_name: Option<String>,
    },

    /// Remove a worktree
    #[clap(name = "remove", aliases = &["rm", "del", "delete", "r", "d"])]
    Remove {
        /// Name of the worktree to remove
        name: StratumRef,
    },
}
impl WorktreeCommand {
    /// Execute the worktree command
    pub fn execute(self, store: &Store) -> Result<(), String> {
        match self {
            WorktreeCommand::Add {
                name,
                base_commit,
                description,
            } => {
                let (label, worktree_name) = match name {
                    StratumRef::Worktree { label, worktree } => (label, worktree),
                    _ => {
                        return Err(
                            "Invalid worktree name format. Use 'label+worktree_name'.".to_string()
                        );
                    }
                };
                let commit_id = base_commit.resolve_commit_id(store)?;
                store.create_worktree(&label, &worktree_name, &commit_id, description)?;
                Ok(())
            }
            WorktreeCommand::List { stratum_name } => {
                if let Some(ref name) = stratum_name {
                    let worktrees = store.list_worktrees(name)?;
                    for (worktree_ref, worktree) in worktrees {
                        println!(
                            "{}: {}",
                            worktree_ref,
                            worktree
                                .worktree
                                .description
                                .as_deref()
                                .unwrap_or("No description")
                        );
                    }
                } else {
                    let worktrees = store.list_all_worktrees()?;
                    for (worktree_ref, worktree) in worktrees {
                        println!(
                            "{}: {}",
                            worktree_ref,
                            worktree
                                .worktree
                                .description
                                .as_deref()
                                .unwrap_or("No description")
                        );
                    }
                }
                Ok(())
            }
            WorktreeCommand::Remove { name } => {
                let (label, worktree_name) = match name {
                    StratumRef::Worktree { label, worktree } => (label, worktree),
                    _ => {
                        return Err(
                            "Invalid worktree name format. Use 'label+worktree_name'.".to_string()
                        );
                    }
                };
                store.remove_worktree(&label, &worktree_name)
            }
        }
    }
}
