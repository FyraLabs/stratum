# Stratum - A Layered State Management System

stratum should be defined by tags similar to OCI, in a `stratum_ref` format.

## Storage Architecture

The store follows an OCI-style content-addressable design:

- **Commits** are immutable and stored by their content hash (metadata hash for fast operations)
- **Tags** are mutable pointers/aliases that reference specific commits
- **Worktrees** are multiple persistent mutable upperdirs for parallel development (similar to git worktrees)
- **Dual hash system**: metadata hash (fast) for commit IDs, merkle root (secure) for verification

The store should be something like composefs + overlayfs, with multiple named worktrees that can be committed and re-mounted on the fly using mount --move or similar. Each worktree has its own persistent upperdir for layering changes, allowing parallel development workflows.

## Key Concepts

### Refs/Strata

A **stratum**, or internally a **ref**, is an individual volume managed by Stratum. It can be thought of as a single unit of state, similar to a Docker image or an OSTree commit. Each stratum can have multiple tags and worktrees associated with it.

A stratum is defined by its volume name, usually followed by a colon and a tag or worktree name, e.g. `myapp:latest` or `myapp+feature-branch`. However these are mutually exclusive, you may either refer to a stratum by its tag, commit hash, or its worktree reference, not combined.

### Commits

**Commits** are Immutable snapshots of the filesystem, stored as EROFS images (using `mkcomposefs`).

Unlike Git, each Stratum commit is a full snapshot of the filesystem state represented as a ComposeFS image. Commits do not store diffs, but can optionally reference a parent commit for metadata or provenance. Changes between commits are expressed by comparing Merkle roots or by looking at each commit's parent commit individually.

Patch application in Stratum uses additive directory merges (OverlayFS upperdirs), optionally using whiteout files (`.wh.*`) to indicate deletions. This allows efficient binary delta updates or mod layering without recomputing full diffs.

### Tags

**Tags** are human-readable aliases for commits, allowing easy reference and management. They are mutable pointers to specific commits, similar to Git tags. Tags can be used to mark important points in the commit history, such as releases or stable versions.

### Worktrees

**Worktrees** are Named mutable upperdirs that allow mutable changes on top of a base commit, similar to git worktrees. Each worktree has its own upperdir and working directory. This allows for layering changes without actually committing them, enabling data like game saves or configuration files to be modified without affecting the base commit.

Stratum prevents multiple mounts of the same worktree concurrently via a runtime advisory lock. This avoids conflicts arising from concurrent mutable overlays on a single worktree. However, mounting distinct worktrees in parallel is fully supported.

These restrictions are soft and enforced only by Stratum’s tooling; manual ComposeFS mounts bypass these checks and may lead to conflicts if used improperly.

```bash
# Create a new worktree
stratum worktree add myapp+profile-1 myapp:latest
stratum worktree add myapp+profile-2 myapp:latest

# Mount the worktree
stratum mount myapp+profile-1 /mnt/profile-1
stratum mount myapp+profile-2 /mnt/profile-2


# You cannot mount the same worktree twice, this will fail:
stratum mount myapp+profile-1 /tmp/profile-1-duplicate # Error: worktree already mounted

stratum unmount /mnt/profile-1
# Now you can mount it again
stratum mount myapp+profile-1 /tmp/profile-1-duplicate


# === DANGER ZONE ===

# You may however, force a mount, but this is ***NOT RECOMMENDED***

mount.composefs -o basedir=/path/to/stratum-store/objects,upperdir=/path/to/stratum-store/refs/myapp/worktrees/profile-1/upperdir,workdir=/path/to/stratum-store/refs/myapp/worktrees/profile-1/workdir /path/to/stratum-store/refs/myapp/tags/latest/commit.cfs /mnt/profile-1-force

# This is actually what Stratum does under the hood, but you may do it yourself, But be aware that you assume full responsibility for the state of the worktree and its upperdir when doing unmanaged mounts.

stratum unmount /mnt/profile-1-force # This will fail because stratum isn't aware of this mount, you must manage it manually!!

# === END DANGER ZONE ===

```

## Stratum Store

The stratum store is a directory structure that contains all the strata, commits, tags, and worktrees. It is designed to be content-addressable, allowing efficient storage and retrieval of strata.

The store is structured as follows:

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
|-- refs/                       # strata namespaces with tags and worktrees
    |-- <label>/                # application/project namespace
        |-- worktrees/          # multiple named worktrees for parallel development
        |   |-- main/           # default worktree (replaces old HEAD)
        |   |   |-- upperdir/   # mutable changes layered on base commit
        |   |   |-- workdir/    # overlayfs working directory
        |   |   |-- meta.toml   # worktree metadata (base commit, created, etc.)
        |   |-- feature-x/      # feature development worktree
        |   |   |-- upperdir/
        |   |   |-- workdir/
        |   |   |-- meta.toml
        |   |-- hotfix/         # hotfix worktree based on different commit
        |       |-- upperdir/
        |       |-- workdir/
        |       |-- meta.toml
        |-- tags/               # human-readable tag aliases (symlinks to commits)
            |-- latest -> ../../commits/a1b2c3d4e5f6.../
            |-- v1.0 -> ../../commits/f7g8h9i0j1k2.../
            |-- stable -> ../../commits/a1b2c3d4e5f6.../
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

## Worktree Metadata Format

Each worktree stores metadata in TOML format:

```toml
[worktree]
name = "main"                   # Worktree name
base_commit = "a1b2c3d4..."    # Base commit this worktree is based on
created = "2025-06-10T10:30:00Z"
last_modified = "2025-06-10T11:15:00Z"
description = "Main development worktree"  # Optional description
```

## Patchsets

Stratum supports the composition of multiple independent patches into a single deterministic snapshot via patchsets.

Patchsets allows users to defined an ordered list of commits, which are then merged sequentially onto a base commit (or an empty root). This is especially useful for managing mod-like layers (e.g., in games), configuration overlays, or update deltas.

Each patch in a patchset should be a **bare commit**, a minimal snapshot containing the delta (new/changed files, whiteouts, etc.) Without relying on an existing commit. These patches do not need to share the same base commit and are merged using a ***last-write-wins*** strategy.

> [!NOTE]
> You may still import a commit with an existing base commit, but note that this will copy-up all files, slightly degrading performance on patchset application. However it is a **no-op** in the end due to the nature of
> ComposeFS, which manages each file as a content-addressable, deduplicated blob.

### Patchset File Format

A patchset is a TOML file that defines how to apply a series of patches to create the final commit. They are usually named `*.patchset.toml`, and contain the following sections:

```toml
[patchset]
base = "myapp:stable" # Optional base commit, if not defined will base from first patch instead

patches = [
  "myapp:modloader",
  "myapp:patch-fixes",
  "myapp:patch-ui",
  "myapp:patch-localization",
  "myapp:content-mod-x",
  "myapp:content-mod-y",
  # If a patch conflicts with another, the last
  # applied patch wins, so in this case, it's mod Z that gets applied in the end, combined with mod Y's unique files, if any
  "myapp:content-mod-z-conflicts-with-y"
  # You may even use commit hashes directly here, if you know the hash
  "a1b2c3d4e5f6...", # Direct commit hash
]
```

### How the patchset is applied

When you use a patchset, Stratum will:

1. Start with the base commit if exists (or the first patch if not defined)
2. For each patch, create transient OverlayFS mount with the base, and and the next patch as the upperdir
3. Create a **transient commit** to be used as the base for the next patch
4. Finally, after the final patch, it generates a new commit with the final filesystem view, which can then be tagged or rebased for a new worktree.

### Transient Commits

During patchset application, Stratum creates **transient commits** that won't register new objects in the store.
These commits exist solely during orchestration and are immediately discarded after the final commit is created.

This avoids unnecessary I/O overhead from registering intermediate states, ensuring that object references remain meaningful and minimal.

## Workflow Examples

```bash
# Import directory as new commit
stratum import --bare /path/to/app myapp
# Returns: "a1b2c3d4e5f6...", also addressable as myapp:latest
# You can also use --patch to import a directory as a patch on top of an existing commit.
# Import a patch on top of an existing commit
stratum import --bare --patch myapp:latest myapp:feature-x /path/to/patchtree
# Import a Stratum export file
stratum import /path/to/export.stratum.tar myapp

# Create human-readable tags
stratum tag myapp:a1b2c3d4... v1.0
stratum tag myapp:a1b2c3d4... latest
stratum tag a1b2c3d4e5f6... stable  # Tagging a commit directly by hash

# Create and work with worktrees
stratum worktree add myapp:latest feature-branch  # Create new worktree
stratum worktree add myapp:v1.0 hotfix           # Worktree from different base

# Mount specific worktrees (writable)
stratum mount myapp+main /mnt/main
stratum mount myapp+feature-branch /mnt/feature
stratum mount myapp /mnt/main # Default to main worktree if no worktree specified

# Mount a specific tag (read-only)
stratum mount myapp:v1.0 /mnt/readonly
# No upperdir, completely read-only

# Note that worktrees can only be mounted once per worktree to avoid conflicts
# You can't really specify both worktrees and tags in the same mount command,
# as it would be ambiguous, so you should use either worktree or tag.


# Commit from specific worktrees
stratum commit myapp:feature-branch new-feature
stratum commit myapp:main latest

#... or from an existing mount
stratum commit /mnt/main new-tag

# List all strata
stratum list myapp
```

`/run/stratum/state` - temporary state file for the current state of the stratum, used for mounting/unmounting, won't persist across reboots

`/run/user/<uid>/stratum/<stratum_ref>` - fallback mountpoint for ephemeral mounts, used for temporary mounts when no mountpoint is specified

`/run/user/<uid>/stratum/<stratum_ref>` - read-only mountpoint for a specific tag, no writable upperdir

note: consider using advisory locks to lock the state file and prevent concurrent commits

## Planned CLI reference

- `stratum worktree add <stratum_ref> <worktree_name>` - create a new worktree based on a commit
- `stratum worktree list <stratum_ref>` - list all worktrees for a stratum
- `stratum worktree remove <stratum_ref+worktree>` - remove a worktree (must be unmounted first)
- `stratum worktree switch <stratum_ref+worktree> </mountpoint>` - switch to a different worktree (for shell integration)
- `stratum init <stratum_ref> <mountpoint>` - initialize a new empty state, mounts it to a mountpoint, optionally taking `--migrate` to migrate an existing directory
- `stratum mount <stratum_ref:optional_worktree_or_tag> <optional_mountpoint>` - mount a stratum by stratum_ref and worktree/tag, if no worktree is specified, the `main` worktree should be used,
  if no mountpoint is specified, a temporary mountpoint should be created somewhere in `/run/user/<uid>/stratum/<stratum_ref:optional_worktree_or_tag>`
  and returned in stdout
- `stratum commit <stratum_ref+optional_worktree> <optional_tag>` - commit current worktree state to a new tag, if no worktree is specified, use `main`,
  if no tag is specified, it should hash the current state and use that as a tag, if the tag already exists, it should fail with an error  
- `stratum tag <stratum_ref> <new_tag>` - create a tag pointing to a specific commit hash
- `stratum list <stratum_ref>` - list all tags for a given stratum_ref, if no stratum_ref is specified, list all existing strata
- `stratum remove <stratum_ref>` - remove a tag from a stratum_ref, if no tag is specified, remove the whole stratum_ref and all its tags
- `stratum status <stratum_ref>` - show the current status of a stratum, including the current mountpoint, last commit + timestamp, and other metadata
- `stratum reset <mountpoint> <stratum_ref>` - reset a stratum at a mounted worktree to a specific tag
  If no tag is specified, it should reset to the latest commit of the main worktree
- `stratum rebase <mountpoint> <stratum_ref>` - rebase the current worktree state onto a new stratum while preserving the upperdir changes.
  if an existing mountpoint is specified, it will re-mount that stratum on top of the current state
- `stratum export <stratum_ref> <file>` - export a stratum to a file, if the tag does not exist, it should fail with an error
- `stratum import <path> <name>` - import a stratum export file to a new stratum, with the specified name.
- `stratum import --bare <directory> <name>` - Import a bare directory to a new commit
- `stratum import --patch <path> <stratum_ref>` - Import a new layer as a patch on top of an existing commit,
  Can also be used alongside `--bare` to import a directory as a patch on top of an existing stratum.
  This is useful for applying delta or mods, given the directory structure is similar to the existing stratum.
  Note that the merge will be additive, meaning it will not remove files that are not present in the patch.
  Whiteouts may be used to remove files though, as these are simply meant to be OverlayFS upperdirs.

  ```text
     bare_dir/
     |-- .wh.removed_file.txt  # Whiteout file (deletion marker)
     |-- new_subdir/
     ^ -- merged into the stratum, commit as new tag


  ```
  
- `stratum tag <stratum_ref> <new_tag>` - copies a tag to a new tag, maybe consider --move to delete/rename the old tag

## Additional Notes

Stratum’s ComposeFS implementation is a fork of the Rust ComposeFS project, re-implemented to enable writable OverlayFS upperdir support—a feature upstream lacks due to its tarball-based, read-only OSTree-style Repository model.

The upstream Rust ComposeFS project is explicitly experimental, described by its author as a fast-iteration playground and learning project, not intended for immediate production use. It is expected to eventually merge into more complete systems, like Stratum as seen here.

While the upstream C ComposeFS mount helper supports writable OverlayFS upperdirs, the official Rust ComposeFS implementation does not. To maintain a fully Rust-native userspace workflow without shelling out, Stratum re-implements ComposeFS to add this capability directly.
