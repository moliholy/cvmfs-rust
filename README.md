# cvmfs-rust

[![Rust](https://img.shields.io/badge/rust-1.87.0%2B-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-BSD%203--Clause-blue.svg)](LICENSE)

A [CernVM-FS](https://github.com/cvmfs/cvmfs) client implementation written in Rust. This project aims to provide a
modern, secure, and performant alternative to the original C++ implementation.

## Features

- Native Rust implementation of the CernVM-FS client.
- Improved performance and memory safety.
- FUSE integration for filesystem mounting.
- Support for compression and decompression.
- Cryptographic verification of repository content.
- SQLite-based catalog handling.

## Prerequisites

- Rust 1.87.0 or higher.
- FUSE libraries for your operating system.

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/Moliholy/cvmfs-rust.git
cd cvmfs-rust

# Build the project
cargo build --release

# Install the binary (optional)
cargo install --path .
```

## Usage

```bash
# Mount a CernVM-FS repository
cvmfs-cli mount repo.example.org /cvmfs/repo.example.org

# Unmount the repository
fusermount -u /cvmfs/repo.example.org
```

## Development

### Dependencies

This project uses the following key dependencies:

- sha1 (0.10) - SHA-1 hashing.
- log (0.4) - Logging infrastructure.
- fuse_mt (0.6) - FUSE filesystem integration.
- rusqlite (0.37+) - SQLite bindings.
- x509-certificate (0.24+) - Certificate handling.
- reqwest (0.12+) - HTTP client.

### Running Tests

```bash
cargo test
```

## License

This project is licensed under the BSD 3-Clause License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- The original [CernVM-FS project](https://github.com/cvmfs/cvmfs).
- All contributors and maintainers.
