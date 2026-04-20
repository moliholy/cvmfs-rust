# cvmfs-rust

CernVM-FS client in Rust. Mounts remote CVMFS repositories as local FUSE filesystems.

## Architecture

- `src/main.rs`: CLI entry point. Parses args, creates Fetcher/Repository/CernvmFileSystem, mounts via `fuse_mt`.
- `src/fetcher.rs`: Downloads objects from HTTP repo, caches locally, decompresses zlib.
- `src/repository.rs`: High-level repo wrapper. Manages catalogs, history, file retrieval.
- `src/catalog.rs`: SQLite-backed catalog database. Stores directory entries, nested catalog refs, statistics.
- `src/file_system.rs`: `FilesystemMT` trait impl. Translates FUSE ops to repo lookups.
- `src/directory_entry.rs`: File/directory metadata. Content hashing, flags, chunks.
- `src/common.rs`: Shared types (`CvmfsError`, `ChunkedFile`, `FileLike`), path utilities.
- `src/root_file.rs`: Parses signed root files (`.cvmfspublished`, `.cvmfswhitelist`).
- `src/manifest.rs`: Wraps `.cvmfspublished` key-value data.
- `src/history.rs`: SQLite history database for revision tags.
- `src/cache.rs`: Local cache with `data/XX/` two-level hash directory structure.
- `src/certificate.rs`: X.509 certificate parsing (verification not yet implemented).

## Build

```bash
cargo build --release
```

Requires FUSE: macFUSE on macOS (`brew install --cask macfuse`), `libfuse-dev` on Linux.

## Test

```bash
cargo test --workspace
```

## Lint

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo +nightly fmt --all
```

## Key patterns

- All SQLite access is read-only. `unsafe impl Sync` on `DatabaseObject`/`Catalog`/`History` relies on
  `SQLITE_OPEN_FULL_MUTEX`.
- `ChunkedFile` implements `Read + Seek + AsRawFd` for large files split across multiple content-addressed chunks.
- Catalogs form a hierarchy: root catalog + nested catalogs at subdirectory mount points.
- Content addressing uses SHA-1 (default) or RIPEMD-160 hashes with two-level `data/XX/` paths.
- `RwLock<Repository>` protects mutable catalog cache from concurrent FUSE threads.
