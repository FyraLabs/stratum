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
}

impl std::str::FromStr for StratumRef {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(StratumRef::from(s))
    }
}

impl From<&str> for StratumRef {
    fn from(s: &str) -> Self {
        if is_sha256_hash(s) {
            StratumRef::Commit(s.to_string())
        } else {
            StratumRef::Tag(s.to_string())
        }
    }
}

impl fmt::Display for StratumRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StratumRef::Commit(id) => write!(f, "{}", id),
            StratumRef::Tag(tag) => write!(f, "{}", tag),
        }
    }
}

impl StratumRef {
    pub fn resolve_commit_id(&self, store: &crate::store::Store) -> Result<String, String> {
        match self {
            StratumRef::Commit(id) => Ok(id.clone()),
            StratumRef::Tag(tag) => {
                let (label, tag) = parse_label(tag)
                    .map_err(|e| format!("Failed to parse tag '{}': {}", tag, e))?;
                let tag = tag.unwrap_or_else(|| "latest".to_string());

                store
                    .resolve_tag(&label, &tag)
                    .map_err(|e| format!("Failed to resolve tag '{}: {}': {}", label, tag, e))
            }
        }
    }
}

fn is_sha256_hash(s: &str) -> bool {
    s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit())
}
