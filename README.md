# cvmfs-rust

[![Rust](https://github.com/Moliholy/cvmfs-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/Moliholy/cvmfs-rust/actions/workflows/rust.yml)
[![codecov](https://codecov.io/gh/moliholy/cvmfs-rust/graph/badge.svg)](https://codecov.io/gh/moliholy/cvmfs-rust)
[![License](https://img.shields.io/badge/license-BSD%203--Clause-blue.svg)](LICENSE)

A pure Rust implementation of the [CernVM-FS](https://cernvm.cern.ch/fs) client. Mount remote CVMFS repositories as local filesystems via FUSE, with full content verification and transparent decompression.

## Why Rust?

The original CernVM-FS client is written in C++. This project rewrites the client in Rust to get:

- **Memory safety** without garbage collection
- **Fearless concurrency** for multi-threaded FUSE operations
- **Modern tooling**: cargo, clippy, built-in testing, dependency management
- **Smaller binary**: single static binary, no shared library dependencies beyond FUSE

## Features

- FUSE filesystem mounting via `fuse_mt` (multi-threaded)
- Transparent zlib decompression of content-addressed objects
- RSA-PKCS1v15 signature verification of repository manifests
- SQLite catalog traversal with nested catalog support
- Chunked file reassembly for large files
- Local object caching with content-addressed storage
- HTTP/HTTPS retrieval from Stratum-1 replica servers

## Quick Start

### Prerequisites

- Rust (stable)
- FUSE 3 libraries:
  - **macOS**: [macFUSE](https://macfuse.github.io/) (`brew install --cask macfuse`)
  - **Linux**: `sudo apt install libfuse3-dev` (Debian/Ubuntu) or `sudo dnf install fuse3-devel` (Fedora)

### Build

```bash
git clone https://github.com/Moliholy/cvmfs-rust.git
cd cvmfs-rust
cargo build --release
```

### Mount a Repository

```bash
mkdir -p /tmp/cvmfs_mount
./target/release/cvmfs-cli http://cvmfs-stratum-one.cern.ch/opt/boss /tmp/cvmfs_mount
```

Then browse `/tmp/cvmfs_mount` like any local directory. Unmount with:

```bash
# macOS
umount /tmp/cvmfs_mount

# Linux
fusermount -u /tmp/cvmfs_mount
```

### CLI Reference

```
cvmfs-cli <repository_url> <mount_point> [cache_directory]
```

| Argument          | Required | Default        | Description                     |
|-------------------|----------|----------------|---------------------------------|
| `repository_url`  | Yes      |                | URL of the CernVM-FS repository |
| `mount_point`     | Yes      |                | Local directory to mount        |
| `cache_directory` | No       | `/tmp/cvmfs`   | Directory for cached objects    |

### Logging

```bash
RUST_LOG=info cvmfs-cli http://cvmfs-stratum-one.cern.ch/opt/boss /tmp/cvmfs_mount
```

## Library Usage

`cvmfs-rust` exposes a library crate for programmatic access:

```rust
use cvmfs::{fetcher::Fetcher, repository::Repository};

let fetcher = Fetcher::new("http://cvmfs-stratum-one.cern.ch/opt/boss", "/tmp/cache", true)?;
let mut repo = Repository::new(fetcher)?;

// List root directory
for entry in repo.list_directory("/")? {
    println!("{} ({})", entry.name, if entry.is_directory() { "dir" } else { "file" });
}

// Read a file
let mut file = repo.get_file("/testfile")?;
let mut contents = String::new();
file.read_to_string(&mut contents)?;
```

## Development

```bash
cargo test --workspace -- --test-threads=1
cargo clippy --workspace --all-targets -- -D warnings
cargo +nightly fmt --all
```

## License

BSD 3-Clause. See [LICENSE](LICENSE).

## Acknowledgments

- [CernVM-FS](https://github.com/cvmfs/cvmfs) (the original C++ implementation)
- [CERN](https://home.cern/) for maintaining public Stratum-1 servers
