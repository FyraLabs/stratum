use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::util::parse_label;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub enum StratumRef {
    /// A commit reference, typically a commit ID or hash
    Commit(String),
    /// A tag reference, typically a human-readable label
    Tag(String),

    /// A worktree reference, which is a combination of a label and a worktree name
    Worktree { label: String, worktree: String },
}

impl std::str::FromStr for StratumRef {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(s))
    }
}

impl From<&str> for StratumRef {
    fn from(s: &str) -> Self {
        if s.contains('+') {
            let parts: Vec<&str> = s.splitn(2, '+').collect();
            if parts.len() != 2 {
                return Self::Tag(s.to_owned());
            }
            Self::Worktree {
                label: parts[0].to_owned(),
                worktree: parts[1].to_owned(),
            }
        } else if is_sha256_hash(s) {
            Self::Commit(s.to_owned())
        } else {
            Self::Tag(s.to_owned())
        }
    }
}

impl fmt::Display for StratumRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Commit(id) => write!(f, "{id}"),
            Self::Tag(tag) => write!(f, "{tag}"),
            Self::Worktree { label, worktree } => write!(f, "{label}+{worktree}"),
        }
    }
}

impl StratumRef {
    pub fn resolve_commit_id(&self, store: &crate::store::Store) -> Result<String, String> {
        match self {
            Self::Commit(id) => Ok(id.clone()),
            Self::Tag(tag) => {
                let (label, tag) = parse_label(tag)
                    .map_err(|e| format!("Failed to parse tag '{tag}': {e}"))?;
                let tag = tag.unwrap_or_else(|| "latest".to_owned());

                store
                    .resolve_tag(&label, &tag)
                    .map_err(|e| format!("Failed to resolve tag '{label}: {tag}': {e}"))
            }
            Self::Worktree { label, worktree } => {
                let worktree_obj = store.load_worktree(label, worktree).map_err(|e| {
                    format!("Failed to load worktree '{label}+{worktree}': {e}")
                })?;
                Ok(worktree_obj.base_commit().to_owned())
            }
        }
    }
}

fn is_sha256_hash(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}
