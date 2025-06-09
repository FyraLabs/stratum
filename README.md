# Stratum

> Fearless Statefulness for Stateful Applications
>
> or: A modern, versioned mod manager for Linux

**Stratum** is a content-addressable, overlay-based system for managing modifiable and immutable application data, snapshots, and runtime state — without sacrificing performance or portability.

Stratum allows you to create, mount, and commit layers of filesystems, allowing application state to be managed and tracked in a way that is easily content-addressable, efficient, yet robust. It allows you to mount multiple layers of filesystems as a single unified view, yet still retain the ability to keep track of changes, snapshots, and easily roll back to previous states if needed.

## So, what *is* Stratum exactly?

Think of Stratum as:

- **Git**, but for binary artifacts, or
- **OSTree**, but mountable and support for staging untracked changes.
- **Mod Organizer 2**, but it uses native Linux kernel features.
- Docker layers, if they were fully mountable directories with live mutation support.

Stratum is a filesystem layer manager, like a mod manager on steroids. Designed to version, stack, and snapshot entire trees of runtime or stateful data. You can use Stratum to:

- Manage game mods and patches, allowing you to keep track of different mod profiles/modpacks, configurations, and even game saves, while saving space by only storing unique changed files. (This is the main and initial inspiration for Stratum!)
- Manage application state and configuration files, allowing you to version and track changes to these files over time, and easily roll back to previous versions if needed. Think managing dotfiles, wineprefixes or straight up massive asset trees and versioning them.
- Copy-on-write (CoW) directories, allowing you to create lightweight copies of large directories without duplicating the entire tree, and only storing the changes made to the original tree, saving space and time without using full-blown CoW filesystems like Btrfs or ZFS.

### Real-world use cases

- 🎮 Versioned mod profiles for games like Elder Scrolls, Fallout, Cyperpunk 2077, keeping the vanilla game pristine while allowing mods to be layered on top.
- 💾 Backing up fragile, self-modifying applications/games like *The Sims 2*, allowing you to easily roll back to a previous state if the game breaks or gets corrupted.
- 🍷 Managing Wineprefixes for different applications, without duplicating the entire Wineprefix for each of them. No more having 100+ copies of Visual C++ redistributables!
- ⏳ Seamless filesystem snapshots with live rollback support, allowing you to roll-back entire directories with zero downtime.

## Features

- Stackable, de-duplicated filesystem layers and snapshots, powered by OverlayFS and ComposeFS
- Writable persistent upper layer, able to be committed as a new layer live
- Content-addressable layer metadata
- Docker/OCI-like tagging system for easy versioning and management
- Live rollbacks and snapshots

## Why Stratum?

Layered union filesystems aren't new — but **Stratum** builds on them with a developer and user-focused orchestration layer:

| System                    | Key Differences                                                                                                                                        |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Docker / Podman / OCI** | Focused on immutable, stateless images and sandboxed runtimes. Stratum is focused purely on state management and modifiable data trees. Use both together — Stratum manages the data; Docker/Podman/K8s runs the app. Think of Stratum as a more smart version of `podman mount`.|
| **ComposeFS / OverlayFS** | ComposeFS and OverlayFS provide the low-level filesystem features. Stratum is an orchestration layer that structures these into addressable, layered, stateful hierarchies.                                                                                          |
| **Git**                   | Git is optimized for source control, not runtime state or binary assets. Stratum is purpose-built for application assets, mod directories, and runtime data. If you're doing version control on asset blobs in a codebase, consider Git LFS.|
| **OSTree**                | Stratum's design and purpose slightly overlaps with OSTree, but Stratum is focused on managing mutable application state and large binary files, rather than OS images. If you care about integrity and immutability *on deployment*, consider OSTree for that purpose. |
| **ZFS / Btrfs**           | These are full filesystems with snapshots and CoW. Stratum is filesystem-agnostic and operates on top of existing filesystems. It may integrate snapshot drivers from ZFS/Btrfs in future releases.                                                                  |
| **Mod Organizer 2**       | MO2 pioneered layered game modding, but relies on fragile Windows VFS hooks. Stratum uses native Linux filesystems, is unprivileged-friendly, and is general-purpose. Initially inspired by MO2, Stratum aims to go beyond games with cleaner UX/DX and CLI support. |
| **Flatpak / Snap** | These are sandboxed application formats with their own runtime environments. Stratum is not a packaging format but a state management layer. It may be used alongside Flatpak/Snap to manage application state and mod directories without modifying the original files but is not a replacement for them. |
| **systemd-sysext** | systemd-sysext provides a way to extend system images with additional layers, but is focused on system-level extensions, Stratum is more focused on application-level state management, but can be used alongside it for application-specific layers. |

## Attributions

Stratum builds on the work of many open-source projects, we couldn't exist without them!

- [Rust](https://www.rust-lang.org/) — implementation language
- [OverlayFS](https://docs.kernel.org/filesystems/overlayfs.html) — for layered filesystems
- [ComposeFS](https://github.com/composefs/composefs) — for optimized readonly images
- [systemd-sysext](https://www.freedesktop.org/software/systemd/man/systemd-sysext.html) — inspiration for extensible layers
- [Mod Organizer 2](https://github.com/ModOrganizer2/modorganizer) — the original inspiration for per-layer modding
- [The Linux Kernel](https://www.kernel.org/), and countless contributors
- The modern gaming/modding community, for inspiring us to create a better way to manage patches.
