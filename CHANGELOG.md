# Changelog

## [0.4.2] - 2026-05-05

### Tests
- Raise coverage above 90% across all modules (94% region / 97% line total)
- HTTP mock-based unit tests for `geo`, `fetcher` (retries, decompress, mirrors, verified retrieval)
- Parser-level tests for `dns` TXT record handling

### Docs
- Add crates.io link and extra package categories

## [0.4.1] - 2026-04-21

### Fixes
- Bundle SQLite3 for aarch64-linux cross-compilation support
- Use macOS-14 runner with `PKG_CONFIG_ALLOW_CROSS` for x86_64-apple-darwin builds

## [0.4.0] - 2026-04-21

### Performance
- Migrate from `fuse_mt` to `fuser` 0.17 with multi-threaded dispatch
- `readdirplus` support (Linux): combines readdir + lookup + getattr in a single FUSE op
- Shared `Arc<Fetcher>` across chunked file handles (eliminates per-open HTTP client creation)
- Chunk-aware lookup cache: pre-loaded chunks skip SQLite on repeat opens
- Replace `std::sync` locks with `parking_lot` (lower overhead, no Result wrapping)
- Remove redundant `RwLock<Repository>` wrapper (fuser 0.17 uses `&self`)

### CI
- Add `workflow_dispatch` trigger for binary build testing
- Docker-based benchmark environment (`make bench-docker`)

### Docs
- Benchmark results from Linux Docker: Rust vs C++ cvmfs2 v2.13.3

## [0.3.0] - 2026-04-20

### Features
- FUSE filesystem mounting via `fuse_mt`
- Mirror failover with automatic retry across multiple sources
- Geolocation-based server selection via CVMFS geo API
- DNS-based repository server discovery via TXT records
- TTL-based cache invalidation and negative caching
- Reflog support for tracking historical root catalog hashes
- External data file support (content stored outside CAS)
- Special file types: symlinks, hardlinks, character/block devices
- Full directory entry metadata: uid/gid, hardlinks, xattr
- LZ4 and Zstd decompression (alongside zlib)
- SHA-256 and SHAKE-128 hash algorithms
- Whitelist parsing and validation
- Certificate fingerprint and revision blacklist support
- Download integrity verification with exponential backoff
- Cache quota enforcement with LRU eviction
- Offline mode with stale cache fallback
- Breadcrumb persistence for fast remount
- Magic extended attributes in FUSE layer
- Lookup and readdir caches with kernel attr TTL

### Performance
- Chunked file prefetch with lock-free design
- Thread-safe `RegularFile` and cached statfs
- Pre-populate lookup cache from readdir results
- `FOPEN_KEEP_CACHE` for kernel content caching
- Cache all chunk file handles with thread-local read buffer
- Interior mutability for FUSE performance

### Testing
- Comprehensive integration tests with serialized execution
- nextest single-threaded config
- Codecov coverage reporting

### CI
- crates.io release workflow
- Benchmark suite comparing Rust vs C++ cvmfs2 (hyperfine)

## [0.1.0] - 2025-02-02

### Features
- Initial CernVM-FS client implementation in Rust
- RSA-PKCS1v15 manifest signature verification
- SQLite catalog traversal with nested catalog support
- Transparent zlib decompression of content-addressed objects
- Chunked file reassembly for large files
- HTTP/HTTPS retrieval from Stratum-1 replica servers
- Basic FUSE filesystem with read-only directory and file access
- Local object caching
