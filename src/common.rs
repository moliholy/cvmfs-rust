//! # Common Types and Utilities
//!
//! This module provides common types, error handling, and utility functions used
//! throughout the CVMFS client implementation. It includes:
//! - Core type definitions and traits
//! - Error handling infrastructure
//! - File system utilities and path manipulation
//! - Constants for configuration and file naming

use std::{
	fmt::Debug,
	fs::File,
	io::{ErrorKind, Read, Seek, SeekFrom},
	os::fd::{AsRawFd, RawFd},
	path::{Path, PathBuf},
};

use crate::{
	directory_entry::{Chunk, PathHash},
	fetcher::Fetcher,
};

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
		Self { chunks, position: 0, size, fetcher }
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
			let local_path =
				self.fetcher.retrieve_file(path.as_str()).map_err(|_| ErrorKind::Unsupported)?;
			let mut file = File::open(local_path).map_err(|_| ErrorKind::NotFound)?;
			file.seek(SeekFrom::Start(chunk_position)).map_err(|_| ErrorKind::NotSeekable)?;
			let max_to_read = (buf.len() - currently_read).min(remaining_in_chunk as usize);
			let dest = &mut buf[currently_read..currently_read + max_to_read];
			let mut chunk_bytes_read = 0;
			while chunk_bytes_read < max_to_read {
				let bytes_read = file.read(&mut dest[chunk_bytes_read..])?;
				if bytes_read == 0 {
					break;
				}
				chunk_bytes_read += bytes_read;
			}
			currently_read += chunk_bytes_read;
			self.position += chunk_bytes_read as u64;
			index += 1;
		}
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
		let hash_concat = self.chunks.iter().fold(String::new(), |mut acc, (_, chunk)| {
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
	PathBuf::from(path).canonicalize().unwrap_or(PathBuf::from(path))
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
	PathHash { hash1: lo, hash2: hi }
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
	Path::new("data").join(first).join(second.to_owned() + hash_suffix)
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::io::SeekFrom;

	#[test]
	fn split_md5_all_zeros() {
		let digest = [0u8; 16];
		let result = split_md5(&digest);
		assert_eq!(result.hash1, 0);
		assert_eq!(result.hash2, 0);
	}

	#[test]
	fn split_md5_all_ff() {
		let digest = [0xFFu8; 16];
		let result = split_md5(&digest);
		assert_eq!(result.hash1, -1); // all bits set in i64
		assert_eq!(result.hash2, -1);
	}

	#[test]
	fn split_md5_known_value() {
		let mut digest = [0u8; 16];
		// lo bytes: 0x01 at position 0 => lo = 1
		digest[0] = 1;
		// hi bytes: 0x02 at position 8 (first of hi) => hi = 2
		digest[8] = 2;
		let result = split_md5(&digest);
		assert_eq!(result.hash1, 1);
		assert_eq!(result.hash2, 2);
	}

	#[test]
	fn split_md5_asymmetric() {
		// lo = bytes [0..8], hi = bytes [8..16]
		let digest: [u8; 16] = [
			0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60,
			0x70, 0x80,
		];
		let result = split_md5(&digest);
		let expected_lo = i64::from_le_bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
		let expected_hi = i64::from_le_bytes([0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80]);
		assert_eq!(result.hash1, expected_lo);
		assert_eq!(result.hash2, expected_hi);
	}

	#[test]
	fn compose_object_path_no_suffix() {
		let result = compose_object_path("abcdef1234", "");
		assert_eq!(result, PathBuf::from("data/ab/cdef1234"));
	}

	#[test]
	fn compose_object_path_with_suffix() {
		let result = compose_object_path("abcdef1234", "C");
		assert_eq!(result, PathBuf::from("data/ab/cdef1234C"));
	}

	#[test]
	fn compose_object_path_rmd160_suffix() {
		let result = compose_object_path("deadbeef00", "-rmd160");
		assert_eq!(result, PathBuf::from("data/de/adbeef00-rmd160"));
	}

	#[test]
	fn canonicalize_path_nonexistent_returns_original() {
		let path = "/this/path/does/not/exist/at/all";
		let result = canonicalize_path(path);
		assert_eq!(result, PathBuf::from(path));
	}

	#[test]
	fn canonicalize_path_root() {
		let result = canonicalize_path("/");
		assert_eq!(result, PathBuf::from("/"));
	}

	#[test]
	fn cvmfs_error_from_string() {
		let err: CvmfsError = "something broke".to_string().into();
		assert_eq!(err, CvmfsError::Generic("something broke".to_string()));
	}

	#[test]
	fn cvmfs_error_from_str() {
		let err: CvmfsError = "oops".into();
		assert_eq!(err, CvmfsError::Generic("oops".to_string()));
	}

	#[test]
	fn cvmfs_error_to_i32() {
		let code: i32 = CvmfsError::FileNotFound.into();
		assert_eq!(code, libc::ENOSYS);

		let code2: i32 = CvmfsError::ParseError.into();
		assert_eq!(code2, libc::ENOSYS);
	}

	#[test]
	fn chunked_file_seek_start() {
		let tmp = std::env::temp_dir().join(format!("cvmfs_test_seek_{}", std::process::id()));
		std::fs::create_dir_all(&tmp).unwrap();
		let cache_dir = tmp.to_str().unwrap();
		let fetcher = Fetcher::new(cache_dir, cache_dir, true).unwrap();
		let mut cf = ChunkedFile::new(vec![], 1000, fetcher);

		let pos = cf.seek(SeekFrom::Start(500)).unwrap();
		assert_eq!(pos, 500);
		assert_eq!(cf.position, 500);

		std::fs::remove_dir_all(&tmp).ok();
	}

	#[test]
	fn chunked_file_seek_current() {
		let tmp = std::env::temp_dir().join(format!("cvmfs_test_seekcur_{}", std::process::id()));
		std::fs::create_dir_all(&tmp).unwrap();
		let cache_dir = tmp.to_str().unwrap();
		let fetcher = Fetcher::new(cache_dir, cache_dir, true).unwrap();
		let mut cf = ChunkedFile::new(vec![], 1000, fetcher);

		cf.seek(SeekFrom::Start(100)).unwrap();
		let pos = cf.seek(SeekFrom::Current(50)).unwrap();
		assert_eq!(pos, 150);

		std::fs::remove_dir_all(&tmp).ok();
	}

	#[test]
	fn chunked_file_seek_end() {
		let tmp = std::env::temp_dir().join(format!("cvmfs_test_seekend_{}", std::process::id()));
		std::fs::create_dir_all(&tmp).unwrap();
		let cache_dir = tmp.to_str().unwrap();
		let fetcher = Fetcher::new(cache_dir, cache_dir, true).unwrap();
		let mut cf = ChunkedFile::new(vec![], 1000, fetcher);

		let pos = cf.seek(SeekFrom::End(-100)).unwrap();
		assert_eq!(pos, 900);

		std::fs::remove_dir_all(&tmp).ok();
	}

	#[test]
	fn chunked_file_seek_negative_position_errors() {
		let tmp = std::env::temp_dir().join(format!("cvmfs_test_seekneg_{}", std::process::id()));
		std::fs::create_dir_all(&tmp).unwrap();
		let cache_dir = tmp.to_str().unwrap();
		let fetcher = Fetcher::new(cache_dir, cache_dir, true).unwrap();
		let mut cf = ChunkedFile::new(vec![], 100, fetcher);

		let result = cf.seek(SeekFrom::End(-200));
		assert!(result.is_err());

		std::fs::remove_dir_all(&tmp).ok();
	}

	#[test]
	fn chunked_file_read_empty_chunks() {
		let tmp = std::env::temp_dir().join(format!("cvmfs_test_readmt_{}", std::process::id()));
		std::fs::create_dir_all(&tmp).unwrap();
		let cache_dir = tmp.to_str().unwrap();
		let fetcher = Fetcher::new(cache_dir, cache_dir, true).unwrap();
		let mut cf = ChunkedFile::new(vec![], 0, fetcher);

		let mut buf = [0u8; 16];
		let n = cf.read(&mut buf).unwrap();
		assert_eq!(n, 0);

		std::fs::remove_dir_all(&tmp).ok();
	}
}
