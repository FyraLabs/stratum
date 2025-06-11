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