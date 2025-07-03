//! Objects module
//! This module helps keep track of objects in the system, allowing garbage collection
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, path::Path};

pub struct ObjectDatabase {
    db: sled::Db,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Encode, Decode)]
pub struct ObjectMetadata {
    pub size: u64,
    pub commit_refs: HashSet<String>,
    #[bincode(with_serde)]
    pub first_seen: chrono::DateTime<chrono::Utc>,
}

impl ObjectMetadata {
    pub fn new(size: u64) -> Self {
        ObjectMetadata {
            size,
            commit_refs: HashSet::new(),
            first_seen: chrono::Utc::now(),
        }
    }
}

impl ObjectDatabase {
    pub fn new(state_dir: &str) -> Result<Self, String> {
        let db = sled::open(Path::new(state_dir).join("objects.db"))
            .map_err(|e| format!("Failed to open object database: {}", e))?;
        Ok(ObjectDatabase { db })
    }

    pub fn register_object(&self, object_id: &str, size: u64, commit_ref: Option<&str>) {
        let key = object_id.as_bytes();

        let mut metadata = if let Ok(Some(existing_data)) = self.db.get(key) {
            // Object exists, decode existing metadata
            bincode::decode_from_slice(&existing_data, bincode::config::standard())
                .map(|(meta, _)| meta)
                .unwrap_or_else(|_| ObjectMetadata::new(size))
        } else {
            // Object doesn't exist, create new metadata
            ObjectMetadata::new(size)
        };

        // Add commit ref if provided
        if let Some(commit_ref) = commit_ref {
            metadata.commit_refs.insert(commit_ref.to_string());
        }

        // Save updated metadata
        self.db
            .insert(
                key,
                bincode::encode_to_vec(&metadata, bincode::config::standard()).unwrap(),
            )
            .unwrap();
    }

    pub fn unregister_object(&self, object_id: &str, commit_ref: &str) -> Result<(), String> {
        let key = object_id.as_bytes();

        if let Ok(Some(existing_data)) = self.db.get(key) {
            let mut metadata: ObjectMetadata =
                bincode::decode_from_slice(&existing_data, bincode::config::standard())
                    .map(|(meta, _)| meta)
                    .map_err(|e| format!("Failed to decode metadata for '{}': {}", object_id, e))?;

            // Remove the commit ref
            metadata.commit_refs.remove(commit_ref);

            // If no more commit refs, remove the object entirely
            if metadata.commit_refs.is_empty() {
                self.db
                    .remove(key)
                    .map_err(|e| format!("Failed to remove object '{}': {}", object_id, e))?;
            } else {
                // Update the metadata with the removed commit ref
                let encoded = bincode::encode_to_vec(&metadata, bincode::config::standard())
                    .map_err(|e| format!("Failed to encode metadata for '{}': {}", object_id, e))?;
                self.db
                    .insert(key, encoded)
                    .map_err(|e| format!("Failed to update metadata for '{}': {}", object_id, e))?;
            }
        }

        Ok(())
    }

    pub fn get_object_metadata(&self, object_id: &str) -> Result<Option<ObjectMetadata>, String> {
        let key = object_id.as_bytes();
        match self.db.get(key) {
            Ok(Some(data)) => {
                let metadata: ObjectMetadata =
                    bincode::decode_from_slice(&data, bincode::config::standard())
                        .map(|(meta, _)| meta)
                        .map_err(|e| {
                            format!("Failed to decode metadata for '{}': {}", object_id, e)
                        })?;
                Ok(Some(metadata))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(format!("Failed to get metadata for '{}': {}", object_id, e)),
        }
    }

    pub fn remove_object(&self, object_id: &str) -> Result<(), String> {
        let key = object_id.as_bytes();
        self.db
            .remove(key)
            .map_err(|e| format!("Failed to remove object '{}': {}", object_id, e))?;
        Ok(())
    }
}
