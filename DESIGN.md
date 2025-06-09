# Stratum - A Layered State Management System

stratum should be defined by tags similar to OCI, in a `label:tag` format.

## Storage Architecture

The store follows an OCI-style content-addressable design:

- **Commits** are immutable and stored by their content hash (metadata hash for fast operations)
- **Tags** are mutable pointers/aliases that reference specific commits
- **HEAD** is a special persistent mutable upperdir for layering changes before commit
- **Dual hash system**: metadata hash (fast) for commit IDs, merkle root (secure) for verification

The store should be something like composefs + overlayfs, writable upperdir that can be committed and re-mounted on the fly using mount --move or similar
a special tag called `HEAD` should be used to indicate the current state of the store, including the writable upperdir,
committing should be done by doing something like `mkcomposefs --digest-store=$STORE $CURRENT_MOUNTPOINT $COMMIT_FILE.cfs`

## Directory Structure

The
directory structure following OCI-style content-addressable storage:

```text
stratum/
|-- objects/                    # composefs digest store, contains all the blobs
|-- commits/                    # content-addressed commit storage
|   |-- a1b2c3d4e5f6.../       # commit directory (named by metadata hash)
|   |   |-- metadata.toml       # commit metadata with both hashes and stats
|   |   |-- commit.cfs          # EROFS commit file from mkcomposefs
|   |-- f7g8h9i0j1k2.../       # another commit
|       |-- metadata.toml
|       |-- commit.cfs
|-- refs/                       # label namespaces with tags and HEAD
    |-- <label>/                # application/project namespace
        |-- head.toml           # metadata for current HEAD (base commit + changes)
        |-- head/               # persistent mutable upperdir for layering
        |   |-- ...             # user changes layered on top of base commit
        |-- tags/               # human-readable tag aliases
            |-- latest          # file containing commit hash: "a1b2c3d4e5f6..."
            |-- v1.0            # file containing commit hash: "f7g8h9i0j1k2..."
            |-- stable          # file containing commit hash: "a1b2c3d4e5f6..."
```

## Commit Metadata Format

Each commit stores metadata in TOML format with the following structure:

```toml
[commit]
merkle_root = "a1b2c3d4..."     # Cryptographic proof for verification
metadata_hash = "e5f6g7h8..."   # Fast comparison hash (commit ID)
timestamp = "2025-06-09T10:30:00Z"
parent_commit = "previous_hash" # Optional parent for history

[files]
count = 1523                    # Total number of files
total_size = 1048576           # Total size in bytes

[merkle]
leaf_count = 1523              # Number of leaves in merkle tree
tree_depth = 11                # Depth of the merkle tree
```

## Workflow Examples

```bash
# Import directory as new commit
stratum import /path/to/app --label myapp
# Returns: "myapp:a1b2c3d4e5f6..."

# Create human-readable tags
stratum tag myapp:a1b2c3d4... v1.0
stratum tag myapp:a1b2c3d4... latest

# Mount with persistent HEAD for development
stratum mount myapp:latest /mnt/myapp
# Changes go to refs/myapp/head/ (persistent upperdir)

# Commit HEAD changes as new version
stratum commit myapp:v1.1
# Creates new commit, updates HEAD base

# Mount read-only tagged version
stratum mount myapp:v1.0 /mnt/readonly
# No upperdir, completely read-only
```

`/run/stratum/state` - temporary state file for the current state of the stratum, used for mounting/unmounting, won't persist across reboots

`/run/user/<uid>/stratum/<label>` - fallback mountpoint for ephemeral mounts, used for temporary mounts when no mountpoint is specified

`/run/user/<uid>/stratum/<label:tag>` - read-only mountpoint for a specific tag, no writable upperdir

note: consider using advisory locks to lock the state file and prevent concurrent commits

## Planned CLI reference

- `stratum init <label> <mountpoint>` - initialize a new empty state, mounts it to a mountpoint, optionally taking `--migrate` to migrate an existing directory
- `stratum import <directory> --label <label>` - import a directory as a new commit, returns the commit hash
- `stratum mount <label:optional_tag> <optional_mountpoint>` - mount a stratum by label and tag, if no tag is specified, the `HEAD` should be used,
  if no mountpoint is specified, a temporary mountpoint should be created somewhere in `/run/user/<uid>/stratum/<label:optional_tag>`
  and returned in stdout
- `stratum commit <label:optional_tag>` - commit current HEAD state to a new tag, if no tag is specified, it should hash the current state
  and use that as a tag, if the tag already exists, it should fail with an error  
- `stratum tag <label:commit_hash> <new_tag>` - create a tag pointing to a specific commit hash
- `stratum list <label>` - list all tags for a given label, if no label is specified, list all existing labels
- `stratum remove <label:tag>` - remove a tag from a label, if no tag is specified, remove the whole label and all its tags
- `stratum status <label:tag>` - show the current status of a stratum, including the current mountpoint, last commit + timestamp, and other metadata
- `stratum export <label:tag> <file>` - export a stratum to a file, if the tag does not exist, it should fail with an error
- `stratum apply <label:tag> <mountpoint>` - apply a stratum to a mountpoint, reverting the current state to a snapshot tag,
  if an existing mountpoint is specified, it will re-mount that stratum on top of the current state
- `stratum import --bare <directory> <label:optional_tag>` - Import a bare directory to layer onto a stratum
  Applying a bare directory with a similar structure on top of an existing stratum. Useful for deltas and/or applying patches/mods

  ```text
     bare_dir/
     |-- new_subdir/
     ^ -- merged into the stratum, commit as new tag
  ```
  
- `stratum tag <label:tag> <new_tag>` - copies a tag to a new tag, maybe consider --move to delete/rename the old tag

stratum should also garbage collect unused blobs when tags are removed, also HEAD will always be the current state so there can only be one
writable HEAD per label, custom tag mounts should be read-only, and only HEAD will be writable
