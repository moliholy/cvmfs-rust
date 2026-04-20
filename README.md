# cvmfs-rust

[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-BSD%203--Clause-blue.svg)](LICENSE)

A [CernVM-FS](https://github.com/cvmfs/cvmfs) client implementation written in Rust. This project provides a
modern, memory-safe alternative to the original C++ implementation, allowing users to mount
remote CernVM-FS repositories as local filesystems via FUSE.

## Features

- Native Rust implementation of the CernVM-FS client
- FUSE integration for filesystem mounting via `fuse_mt`
- Transparent zlib decompression of repository objects
- SHA-1 signature verification of repository root files
- SQLite-based catalog and history database handling
- Local caching of downloaded objects
- Support for nested catalogs and chunked files
- HTTP/HTTPS retrieval from Stratum-1 servers

## Prerequisites

- Rust (stable)
- FUSE libraries:
    - **macOS**: [macFUSE](https://macfuse.github.io/) (`brew install --cask macfuse`)
    - **Linux**: `libfuse-dev` / `fuse-devel`

## Installation

```bash
git clone https://github.com/Moliholy/cvmfs-rust.git
cd cvmfs-rust
cargo build --release
```

The binary is located at `target/release/cvmfs-cli`.

## Usage

```bash
cvmfs-cli <repository_url> <mount_point> [cache_directory]
```

### Example

```bash
mkdir -p /tmp/cvmfs_mount
cvmfs-cli http://cvmfs-stratum-one.cern.ch/opt/boss /tmp/cvmfs_mount /tmp/cvmfs_cache
```

### Arguments

| Argument          | Required | Description                                          |
|-------------------|----------|------------------------------------------------------|
| `repository_url`  | Yes      | URL of the CernVM-FS repository                      |
| `mount_point`     | Yes      | Local directory to mount (must exist)                |
| `cache_directory` | No       | Directory for cached data (defaults to `/tmp/cvmfs`) |

### Unmounting

```bash
# macOS
umount /tmp/cvmfs_mount
# Linux
fusermount -u /tmp/cvmfs_mount
```

### Logging

Enable logging with the `RUST_LOG` environment variable:

```bash
RUST_LOG=info cvmfs-cli http://cvmfs-stratum-one.cern.ch/opt/boss /tmp/cvmfs_mount
```

## Development

```bash
cargo test
cargo clippy --workspace --all-targets -- -D warnings
cargo +nightly fmt --all
```

## License

This project is licensed under the BSD 3-Clause License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- The original [CernVM-FS project](https://github.com/cvmfs/cvmfs)
