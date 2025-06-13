# Hacking on Stratum

As of `Tue Jun 10 02:07:05 AM +07 2025`, Stratum runs as a single Rust binary, requiring priviledged
access to mount filesystems. The plan is to eventually support unprivileged mounts, by using
FUSE, or a privileged helper daemon that mounts filesystems on behalf of the user similar to how containerd works.

## Known Issues and Limitations

Stratum is still currently in alpha, and there are various known issues and limitations with the current implementation:

- FUSE support is not yet implemented, so Stratum requires root privileges to mount filesystems.
- When merging commits, Stratum currently does not handle merging EROFS filesystems zero-copy as of yet, so it will do:
  - Mount a the base commit as an OverlayFS lower layer, and a transient upper layer on disk
  - Mount the commit to be merged as a separate OverlayFS mount
  - Copy the contents of the commit to be merged into the upper layer of the base commit
  - Run `mkcomposefs` to create a new ComposeFS layer from the upper layer
  
  This causes performance issues as it will copy the contents of the commit, and then proceed to recreate a new ComposeFS layer from the unified view, so performance will be `O(N_total * log N_total + S_total)`, where:
  - N_total = total number of files across all commits being merged
  - S_total = total size (in bytes) of all files across all commits
  - M = number of commits being merged

  There are plans to implement a dedicated EROFS-aware ComposeFS merge engine that will read and merge EROFS layers directly,
  but this is still a work in progress.
- Live rebases do not work as they require FUSE support, or ability to move OverlayFS mounts around, which is not supported in upstream OverlayFS. Rebases will simply just unmount and remount the layers in the correct order, which is not ideal for live applications.

## Building Stratum

You require the following pre-requisites to build Stratum:

- Rust toolchain (stable or nightly)
- libfuse-dev (for FUSE support)
- Linux kernel version 6.12 or later (for OverlayFS and ComposeFS support + Loopback-less EROFS support)
- `composefs-tools`, for `mkcomposefs` and `composefs-info` binary wrappers
- `util-linux` (for `mount` and `umount` commands)
- `pkg-config` for finding libraries
- And other basic build dependencies like GCC, Make, Clang, etc.

To build Stratum, you need to have the Rust toolchain installed. You can install it using [rustup](https://rustup.rs/).

Once you have Rust installed, you can clone the Stratum repository and build it:

```bash
cargo build
```

There is also a helper Justfile that can be used to quickly run Stratum under root using `sudo`:

```bash
just run-dev <arguments>
```
