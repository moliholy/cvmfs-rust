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

Both implementations mounted via FUSE, same shell commands, median timing. Rust cvmfs-cli v0.2.0, C++ cvmfs2 v2.11.5.
Repository: `boss.cern.ch`.

### Light operations (50 iterations)

| Operation | Rust | C++ cvmfs2 | Winner |
|---|---|---|---|
| stat / (root) | 6.70ms | 6.62ms | C++ +1% |
| stat /testfile | 6.99ms | 6.64ms | C++ +5% |
| stat /database | 6.66ms | 6.64ms | C++ +0% |
| stat symlink | 6.66ms | 6.70ms | Rust +1% |
| ls / (root) | 5.00ms | 4.45ms | C++ +12% |
| ls /database | 5.48ms | 4.47ms | C++ +23% |
| ls /pacman-3.29 | 5.21ms | 4.48ms | C++ +16% |
| ls /slc4_ia32_gcc34 (nested) | 5.02ms | 4.48ms | C++ +12% |
| readlink symlink | 4.33ms | 4.20ms | C++ +3% |
| cat /testfile (50B) | 4.40ms | 4.18ms | C++ +5% |
| head -c 16 offlinedb.db (chunked) | 5.32ms | 4.34ms | C++ +23% |
| head -c 2 pacman-latest.tar.gz | 4.46ms | 4.45ms | C++ +0% |
| dd seek+read offlinedb.db | 5.40ms | 4.39ms | C++ +23% |
| find /pacman-3.29 | 5.50ms | 4.56ms | C++ +21% |
| find /database -type f | 6.06ms | 4.58ms | C++ +32% |
| wc -c /testfile | 4.46ms | 4.25ms | C++ +5% |
| wc -c /pacman-latest.tar.gz | 4.47ms | 4.20ms | C++ +6% |
| md5 /testfile | 4.55ms | 4.34ms | C++ +5% |

Timings dominated by shell subprocess overhead (~4ms), not FUSE performance.

### Heavy operations (10 iterations)

*Run `make bench` to populate.*

```bash
make bench    # run benchmarks (requires sudo, cvmfs2 installed)
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
