# cvmfs-rust

[![Rust](https://github.com/moliholy/cvmfs-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/moliholy/cvmfs-rust/actions/workflows/rust.yml)
[![codecov](https://codecov.io/gh/moliholy/cvmfs-rust/graph/badge.svg)](https://codecov.io/gh/moliholy/cvmfs-rust)
[![License](https://img.shields.io/badge/license-BSD%203--Clause-blue.svg)](LICENSE)

A pure Rust implementation of the [CernVM-FS](https://cernvm.cern.ch/fs) client. Mount remote CVMFS repositories as
local filesystems via FUSE, with full content verification and transparent decompression.

## Why Rust?

The original CernVM-FS client is written in C++. This project rewrites the client in Rust to get:

- **Memory safety** without garbage collection
- **Fearless concurrency** for multi-threaded FUSE operations
- **Modern tooling**: cargo, clippy, built-in testing, dependency management
- **Smaller binary**: single static binary, no shared library dependencies beyond FUSE

## Features

- FUSE filesystem mounting via `fuse_mt` (multi-threaded)
- Transparent decompression (zlib, LZ4, Zstd) of content-addressed objects
- RSA-PKCS1v15 signature verification of repository manifests
- Whitelist validation (repository name matching + expiry checks)
- SQLite catalog traversal with nested catalog support
- Multiple hash algorithms: SHA-1, RIPEMD-160, SHA-256, SHAKE-128
- Full directory entry metadata: uid/gid, hardlinks, xattr, special file types
- Chunked file reassembly for large files
- External data file support (content stored outside CAS)
- Local object caching with TTL-based invalidation and negative caching
- Reflog support for tracking historical root catalog hashes
- HTTP/HTTPS retrieval from Stratum-1 replica servers
- Mirror failover with automatic retry across multiple sources
- Geolocation-based server selection via CVMFS geo API
- DNS-based repository server discovery via TXT records

## Quick Start

### Prerequisites

- Rust (stable)
- FUSE 3 libraries:
    - **macOS**: [macFUSE](https://macfuse.github.io/) (`brew install --cask macfuse`)
    - **Linux**: `sudo apt install libfuse3-dev` (Debian/Ubuntu) or `sudo dnf install fuse3-devel` (Fedora)

### Build

```bash
git clone https://github.com/moliholy/cvmfs-rust.git
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

| Argument          | Required | Default      | Description                     |
|-------------------|----------|--------------|---------------------------------|
| `repository_url`  | Yes      |              | URL of the CernVM-FS repository |
| `mount_point`     | Yes      |              | Local directory to mount        |
| `cache_directory` | No       | `/tmp/cvmfs` | Directory for cached objects    |

### Logging

```bash
RUST_LOG=info cvmfs-cli http://cvmfs-stratum-one.cern.ch/opt/boss /tmp/cvmfs_mount
```

## Library Usage

`cvmfs-rust` exposes a library crate for programmatic access:

```rust
use cvmfs::{fetcher::Fetcher, repository::Repository};

let fetcher = Fetcher::new("http://cvmfs-stratum-one.cern.ch/opt/boss", "/tmp/cache", true) ?;
let mut repo = Repository::new(fetcher) ?;

// List root directory
for entry in repo.list_directory("/") ? {
println ! ("{} ({})", entry.name, if entry.is_directory() { "dir" } else { "file" });
}

// Read a file
let mut file = repo.get_file("/testfile") ?;
let mut contents = String::new();
file.read_to_string( & mut contents) ?;
```

### Mirror failover

```rust
use cvmfs::fetcher::Fetcher;

let fetcher = Fetcher::with_mirrors(
& ["http://primary.cern.ch/opt/boss", "http://mirror1.cern.ch/opt/boss"],
"/tmp/cache",
true,
) ?;
```

### DNS-based discovery

```rust
use cvmfs::dns::discover_servers;

let servers = discover_servers("boss.cern.ch") ?;
```

## Benchmarks

Both implementations mounted via FUSE, benchmarked with [hyperfine](https://github.com/sharkdp/hyperfine) (100 runs, 10
warmup). Rust cvmfs-cli v0.2.0, C++ cvmfs2 v2.11.5. Repository: `boss.cern.ch`.

**Result: Rust wins 15/23 benchmarks.**

### Metadata operations

| Operation        | Rust  | C++   | Winner   |
|------------------|-------|-------|----------|
| stat / (root)    | 4.2ms | 4.3ms | Rust +4% |
| stat /testfile   | 4.9ms | 4.9ms | Rust +1% |
| stat /database   | 4.5ms | 4.6ms | Rust +2% |
| stat symlink     | 5.2ms | 3.8ms | C++ +35% |
| readlink symlink | 1.0ms | 1.3ms | Rust +21% |

### Directory listing

| Operation                    | Rust  | C++   | Winner   |
|------------------------------|-------|-------|----------|
| ls / (root)                  | 2.3ms | 1.7ms | C++ +35% |
| ls /database                 | 1.6ms | 1.8ms | Rust +17% |
| ls /pacman-3.29              | 1.6ms | 2.1ms | Rust +28% |
| ls /slc4_ia32_gcc34 (nested) | 1.3ms | 0.7ms | C++ +89% |

### File reads

| Operation                         | Rust  | C++   | Winner    |
|-----------------------------------|-------|-------|-----------|
| cat /testfile (50B)               | 1.2ms | 1.2ms | Rust +2%  |
| head -c 16 offlinedb.db (chunked) | 1.2ms | 2.3ms | Rust +100% |
| head -c 2 pacman-latest.tar.gz    | 1.2ms | 0.4ms | C++ +166% |
| dd seek+read offlinedb.db         | 2.7ms | 2.9ms | Rust +10% |
| cat pacman-latest.tar.gz (full)   | 1.8ms | 2.3ms | Rust +23% |

### Recursive traversal

| Operation                     | Rust   | C++    | Winner   |
|-------------------------------|--------|--------|----------|
| find /pacman-3.29 -maxdepth 1 | 1.6ms  | 3.1ms  | Rust +94% |
| find /database -type f        | 1.3ms  | 1.3ms  | Rust +1% |
| find / -maxdepth 3            | 16.4ms | 10.3ms | C++ +59% |
| du -d 2                       | 1.8ms  | 1.8ms  | C++ +3%  |

### Large file I/O (10 runs, 2 warmup)

| Operation                   | Rust   | C++    | Winner  |
|-----------------------------|--------|--------|---------|
| md5 run.db (chunked, 410MB) | 643ms  | 694ms  | Rust +8% |
| cat run.db (chunked, 410MB) | 39.7ms | 41.2ms | Rust +4% |

```bash
make bench    # run benchmarks (requires sudo, cvmfs2, hyperfine)
```

## Development

```bash
make test           # run all tests (uses cargo-nextest)
make lint           # clippy with -D warnings
make fmt            # format with nightly rustfmt
make coverage       # generate coverage report
make bench          # benchmark Rust vs C++ cvmfs2 (requires sudo)
```

## License

BSD 3-Clause. See [LICENSE](LICENSE).

## Acknowledgments

- [CernVM-FS](https://github.com/cvmfs/cvmfs) (the original C++ implementation)
- [CERN](https://home.cern/) for maintaining public Stratum-1 servers
