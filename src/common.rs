//! # Common Types and Utilities
//!
//! This module provides common types, error handling, and utility functions used
//! throughout the CVMFS client implementation. It includes:
//! - Core type definitions and traits
//! - Error handling infrastructure
//! - File system utilities and path manipulation
//! - Constants for configuration and file naming

use std::fmt::Debug;
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::os::fd::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};

use crate::directory_entry::{Chunk, PathHash};
use crate::fetcher::Fetcher;

/// Path to repository configuration files.
pub const REPO_CONFIG_PATH: &str = "/etc/cvmfs/repositories.d";
/// Name of the server configuration file.
pub const SERVER_CONFIG_NAME: &str = "server.conf";
/// REST API endpoint name for control operations.
pub const REST_CONNECTOR: &str = "control";
/// Name of the repository whitelist file.
pub const WHITELIST_NAME: &str = ".cvmfswhitelist";
/// Name of the repository manifest file.
pub const MANIFEST_NAME: &str = ".cvmfspublished";
/// Name of the file containing the last replication timestamp.
pub const LAST_REPLICATION_NAME: &str = ".cvmfs_last_snapshot";
/// Name of the file indicating ongoing replication.
pub const REPLICATING_NAME: &str = ".cvmfs_is_snapshotting";

pub type CvmfsResult<R> = Result<R, CvmfsError>;
pub trait FileLike: Debug + Read + Seek + AsRawFd + Send + Sync {}

impl FileLike for File {}

/// Represents a file split into multiple chunks for efficient storage and transfer
///
/// A ChunkedFile maintains metadata about file chunks and provides implementations
/// for standard file operations like reading and seeking across chunks.
#[derive(Debug)]
pub struct ChunkedFile {
    /// Total size of the file in bytes.
    size: u64,
    /// Vector of chunk paths and their metadata.
    chunks: Vec<(String, Chunk)>,
    /// Current position in the file.
    position: u64,
    /// Fetcher instance for retrieving chunks.
    fetcher: Fetcher,
}

impl ChunkedFile {
    pub(crate) fn new(chunks: Vec<(String, Chunk)>, size: u64, fetcher: Fetcher) -> Self {
        Self {
            chunks,
            position: 0,
            size,
            fetcher,
        }
    }
}

impl Read for ChunkedFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut currently_read = 0;
        let mut index = self
            .chunks
            .iter()
            .position(|(_, chunk)| {
                let offset = chunk.offset as u64;
                let size = chunk.size as u64;
                self.position >= offset && self.position < offset + size
            })
            .unwrap_or(usize::MAX);
        while currently_read < buf.len() && index < self.chunks.len() {
            let (path, chunk) = &self.chunks[index];
            let chunk_position = self.position - chunk.offset as u64;
            let remaining_in_chunk = (chunk.size as u64).saturating_sub(chunk_position);
            let local_path = self
                .fetcher
                .retrieve_file(path.as_str())
                .map_err(|_| ErrorKind::Unsupported)?;
            let mut file = File::open(local_path).map_err(|_| ErrorKind::NotFound)?;
            file.seek(SeekFrom::Start(chunk_position))
                .map_err(|_| ErrorKind::NotSeekable)?;
            let max_to_read =
                (buf.len() - currently_read).min(remaining_in_chunk as usize);
            let mut chunk_bytes_read = 0;
            while chunk_bytes_read < max_to_read {
                let end = currently_read + max_to_read.min(chunk_bytes_read + buf.len());
                let bytes_read =
                    file.read(&mut buf[currently_read + chunk_bytes_read..end])?;
                if bytes_read == 0 {
                    break;
                }
                chunk_bytes_read += bytes_read;
            }
            currently_read += chunk_bytes_read;
            index += 1;
        }
        self.position += currently_read as u64;
        Ok(currently_read)
    }
}

impl Seek for ChunkedFile {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let position: i64 = match pos {
            SeekFrom::Start(p) => p as i64,
            SeekFrom::End(p) => self.size as i64 + p,
            SeekFrom::Current(p) => self.position as i64 + p,
        };
        if position < 0 {
            return Err(ErrorKind::UnexpectedEof.into());
        }
        self.position = position as u64;
        Ok(self.position)
    }
}

impl AsRawFd for ChunkedFile {
    fn as_raw_fd(&self) -> RawFd {
        let hash_concat = self
            .chunks
            .iter()
            .fold(String::new(), |mut acc, (_, chunk)| {
                acc.push_str(&chunk.content_hash);
                acc
            });
        let hash = md5::compute(hash_concat.as_bytes()).0;
        let (int_bytes, _) = hash.as_slice().split_at(size_of::<u64>());
        u64::from_le_bytes(int_bytes.try_into().expect("Casting to u64 should work")) as RawFd
    }
}

impl FileLike for ChunkedFile {}

/// Represents errors that can occur during CVMFS operations
///
/// This enum covers various error conditions that may arise during repository
/// access, file operations, and other CVMFS-related tasks.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum CvmfsError {
    #[error("Invalid Certificate")]
    Certificate,
    #[error("IO error")]
    IO(String),
    #[error("Incomplete root file signature")]
    IncompleteRootFileSignature,
    #[error("Invalid root file signature")]
    InvalidRootFileSignature,
    #[error("Cache directory not found")]
    CacheDirectoryNotFound,
    #[error("DatabaseError")]
    DatabaseError(String),
    #[error("Catalog initialization")]
    CatalogInitialization,
    #[error("File not found")]
    FileNotFound,
    #[error("History not found")]
    HistoryNotFound,
    #[error("Revision not found")]
    RevisionNotFound,
    #[error("Invalid timestamp")]
    InvalidTimestamp,
    #[error("Parse error")]
    ParseError,
    #[error("Synchronization error")]
    Sync,
    #[error("Catalog not found")]
    CatalogNotFound,
    #[error("Tag not found")]
    TagNotFound,
    #[error("Generic error")]
    Generic(String),
    #[error("The path is not a file")]
    NotAFile,
}

impl From<String> for CvmfsError {
    fn from(value: String) -> Self {
        CvmfsError::Generic(value)
    }
}

impl From<&str> for CvmfsError {
    fn from(value: &str) -> Self {
        CvmfsError::Generic(value.to_string())
    }
}

impl From<CvmfsError> for i32 {
    fn from(_: CvmfsError) -> Self {
        libc::ENOSYS
    }
}

impl From<reqwest::Error> for CvmfsError {
    fn from(e: reqwest::Error) -> Self {
        CvmfsError::IO(format!("{:?}", e))
    }
}

impl From<std::io::Error> for CvmfsError {
    fn from(e: std::io::Error) -> Self {
        CvmfsError::IO(format!("{:?}", e))
    }
}

impl From<rusqlite::Error> for CvmfsError {
    fn from(e: rusqlite::Error) -> Self {
        CvmfsError::DatabaseError(format!("{:?}", e))
    }
}

/// Converts a path string to its canonical form
///
/// If canonicalization fails, returns the original path unchanged.
///
/// # Arguments
/// * `path` - The path string to canonicalize
///
/// # Returns
/// A PathBuf containing either the canonical path or the original path
pub fn canonicalize_path(path: &str) -> PathBuf {
    PathBuf::from(path)
        .canonicalize()
        .unwrap_or(PathBuf::from(path))
}

/// Splits an MD5 digest into two 64-bit components for path hashing
///
/// # Arguments
/// * `md5_digest` - The 16-byte MD5 digest to split
///
/// # Returns
/// A PathHash containing the split digest components
pub fn split_md5(md5_digest: &[u8; 16]) -> PathHash {
    let mut hi = 0;
    let mut lo = 0;
    for (i, &byte) in md5_digest[..8].iter().enumerate() {
        lo |= (byte as i64) << (i * 8);
    }
    for (i, &byte) in md5_digest[8..].iter().enumerate() {
        hi |= (byte as i64) << (i * 8);
    }
    PathHash {
        hash1: lo,
        hash2: hi,
    }
}

/// Constructs a path for storing an object based on its hash
///
/// Creates a two-level directory structure using the first two characters
/// of the object hash and appends the provided suffix.
///
/// # Arguments
/// * `object_hash` - The hash of the object
/// * `hash_suffix` - Suffix to append to the hash
///
/// # Returns
/// A PathBuf containing the constructed path
pub fn compose_object_path(object_hash: &str, hash_suffix: &str) -> PathBuf {
    let (first, second) = object_hash.split_at(2);
    Path::new("data")
        .join(first)
        .join(second.to_owned() + hash_suffix)
}
