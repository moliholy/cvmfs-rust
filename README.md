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

Both implementations mounted via FUSE, benchmarked with [hyperfine](https://github.com/sharkdp/hyperfine) (100 runs, 10
warmup). Rust cvmfs-cli v0.3.0, C++ cvmfs2 v2.11.5. Repository: `boss.cern.ch`.

**Result: Rust wins 7/23 benchmarks. Near-parity on most operations.**

### Metadata operations

| Operation        | Rust   | C++    | Winner    |
|------------------|--------|--------|-----------|
| stat / (root)    | 3.7ms  | 4.1ms  | Rust +11% |
| stat /testfile   | 3.8ms  | 3.8ms  | Tie       |
| stat /database   | 4.0ms  | 4.0ms  | Tie       |
| stat symlink     | 3.7ms  | 3.7ms  | Tie       |
| readlink symlink | 806µs  | 817µs  | Rust +1%  |

### Directory listing

| Operation                     | Rust  | C++   | Winner    |
|-------------------------------|-------|-------|-----------|
| ls / (root)                   | 1.6ms | 1.4ms | C++ +13%  |
| ls /database                  | 1.6ms | 1.3ms | C++ +19%  |
| ls /pacman-3.29               | 1.4ms | 1.3ms | C++ +4%   |
| ls /slc4_ia32_gcc34 (nested)  | 1.4ms | 1.4ms | Rust +3%  |

### File reads

| Operation                          | Rust  | C++   | Winner    |
|------------------------------------|-------|-------|-----------|
| cat /testfile (50B)                | 1.1ms | 1.1ms | Tie       |
| head -c 16 offlinedb.db (chunked) | 1.7ms | 1.2ms | C++ +39%  |
| head -c 2 pacman-latest.tar.gz    | 1.2ms | 1.5ms | Rust +25% |
| dd seek+read offlinedb.db         | 2.0ms | 1.9ms | C++ +9%   |
| cat pacman-latest.tar.gz (full)   | 1.7ms | 1.8ms | Rust +9%  |
| wc -c /testfile                   | 1.4ms | 1.6ms | Rust +15% |

### Recursive traversal

| Operation                     | Rust    | C++    | Winner    |
|-------------------------------|---------|--------|-----------|
| find /pacman-3.29 -maxdepth 1 | 1.7ms   | 1.6ms  | C++ +6%   |
| find /database -type f        | 1.8ms   | 1.6ms  | C++ +15%  |
| find / -maxdepth 3            | 19.1ms  | 10.3ms | C++ +87%  |
| du -d 2                       | 2.0ms   | 1.7ms  | C++ +16%  |

### Large file I/O (10 runs, 2 warmup)

| Operation                    | Rust   | C++    | Winner   |
|------------------------------|--------|--------|----------|
| md5 run.db (chunked, 410MB)  | 645ms  | 664ms  | Rust +3% |
| cat run.db (chunked, 410MB)  | 39.1ms | 38.1ms | C++ +2%  |
| md5 /testfile                | 1.3ms  | 1.3ms  | Tie      |
| md5 pacman-latest.tar.gz     | 2.3ms  | 2.2ms  | C++ +7%  |

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

## Related Projects

- [cvmfs-java](https://github.com/moliholy/cvmfs-java): Java port of the CernVM-FS client

## Acknowledgments

- [CernVM-FS](https://github.com/cvmfs/cvmfs) (the original C++ implementation)
- [CERN](https://home.cern/) for maintaining public Stratum-1 servers
