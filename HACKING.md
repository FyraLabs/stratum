# Hacking on Stratum

As of `Tue Jun 10 02:07:05 AM +07 2025`, Stratum runs as a single Rust binary, requiring priviledged
access to mount filesystems. The plan is to eventually support unprivileged mounts, by using
FUSE, or a privileged helper daemon that mounts filesystems on behalf of the user similar to how containerd works.

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
