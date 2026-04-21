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

- FUSE filesystem mounting via `fuser`
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

## Installation

### cargo-binstall (recommended)

```bash
cargo binstall cvmfs
```

Pre-built binaries available for:

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`

### From source

```bash
cargo install cvmfs
```

### Prerequisites

- FUSE 3 libraries:
    - **macOS**: [macFUSE](https://macfuse.github.io/) (`brew install --cask macfuse`)
    - **Linux**: `sudo apt install libfuse3-dev` (Debian/Ubuntu) or `sudo dnf install fuse3-devel` (Fedora)

### Build from git

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

Both implementations mounted via FUSE on Linux (Docker), benchmarked
with [hyperfine](https://github.com/sharkdp/hyperfine)
(100 runs, 10 warmup). C++ cvmfs2 v2.13.3. Repository: `boss.cern.ch`. Multi-threaded fuser with `readdirplus`.

**Rust dominates bulk I/O. Metadata operations at parity (sub-ms, within measurement noise).**

### Metadata operations

| Operation        | Rust  | C++   | Ratio |
|------------------|-------|-------|-------|
| stat / (root)    | 0.4ms | 0.4ms | ~1.0x |
| stat /testfile   | 0.3ms | 0.3ms | ~1.0x |
| stat /database   | 0.3ms | 0.3ms | ~1.0x |
| stat symlink     | 0.4ms | 0.4ms | ~1.0x |
| readlink symlink | 0.2ms | 0.2ms | ~1.0x |

### Directory listing

| Operation                    | Rust  | C++   | Ratio |
|------------------------------|-------|-------|-------|
| ls / (root)                  | 0.4ms | 0.4ms | ~1.0x |
| ls /database                 | 0.4ms | 0.4ms | ~1.0x |
| ls /pacman-3.29              | 0.5ms | 0.4ms | ~1.0x |
| ls /slc4_ia32_gcc34 (nested) | 0.5ms | 0.4ms | ~1.0x |

### File reads

| Operation                         | Rust  | C++   | Winner   |
|-----------------------------------|-------|-------|----------|
| cat /testfile (50B)               | 0.3ms | 0.3ms | ~1.0x    |
| head -c 16 offlinedb.db (chunked) | 0.4ms | 0.4ms | ~1.0x    |
| head -c 2 pacman-latest.tar.gz    | 0.3ms | 0.3ms | ~1.0x    |
| dd seek+read offlinedb.db         | 0.4ms | 0.4ms | ~1.0x    |
| cat pacman-latest.tar.gz (full)   | 0.3ms | 0.3ms | ~1.0x    |
| wc -c /testfile                   | 0.3ms | 0.3ms | ~1.0x    |

### Recursive traversal

| Operation                     | Rust  | C++   | Ratio    |
|-------------------------------|-------|-------|----------|
| find /pacman-3.29 -maxdepth 1 | 0.5ms | 0.5ms | ~1.0x    |
| find /database -type f        | 0.5ms | 0.5ms | ~1.0x    |
| find / -maxdepth 3            | 5.7ms | 4.9ms | C++ +14% |
| du -d 2                       | 0.2ms | 0.2ms | ~1.0x    |

### Large file I/O (10 runs, 2 warmup)

| Operation                   | Rust  | C++   | Winner    |
|-----------------------------|-------|-------|-----------|
| md5 run.db (chunked, 410MB) | 759ms | 789ms | Rust +4%  |
| cat run.db (chunked, 410MB) | 40ms  | 48ms  | Rust +21% |
| md5 /testfile               | 0.3ms | 0.3ms | ~1.0x     |
| md5 pacman-latest.tar.gz    | 1.9ms | 1.9ms | ~1.0x     |

```bash
make bench          # run locally (requires sudo, cvmfs2, hyperfine)
make bench-docker   # run in Docker (recommended, Linux FUSE with n_threads)
```

## Development

```bash
make test           # run all tests (uses cargo-nextest)
make lint           # clippy with -D warnings
make fmt            # format with nightly rustfmt
make coverage       # generate coverage report
make bench          # benchmark Rust vs C++ cvmfs2 (requires sudo)
make bench-docker   # benchmark in Docker (recommended)
```

## License

BSD 3-Clause. See [LICENSE](LICENSE).

## Related Projects

- [cvmfs-java](https://github.com/moliholy/cvmfs-java): Java port of the CernVM-FS client

## Acknowledgments

- [CernVM-FS](https://github.com/cvmfs/cvmfs) (the original C++ implementation)
- [CERN](https://home.cern/) for maintaining public Stratum-1 servers
