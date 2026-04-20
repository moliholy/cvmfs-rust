//! # Common Types and Utilities
//!
//! This module provides common types, error handling, and utility functions used
//! throughout the CVMFS client implementation. It includes:
//! - Core type definitions and traits
//! - Error handling infrastructure
//! - File system utilities and path manipulation
//! - Constants for configuration and file naming

use std::{
	collections::HashSet,
	fmt::Debug,
	fs::File,
	io::{ErrorKind, Read, Seek, SeekFrom},
	os::fd::{AsRawFd, RawFd},
	path::{Path, PathBuf},
	sync::{Arc, Mutex},
	thread,
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
/// Name of the reflog database file.
pub const REFLOG_NAME: &str = ".cvmfsreflog";

pub type CvmfsResult<R> = Result<R, CvmfsError>;

pub trait FileLike: Debug + AsRawFd + Send + Sync {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize>;
	fn file_size(&self) -> u64;
}

impl FileLike for RegularFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
		let mut file = self.inner.lock().map_err(|_| ErrorKind::Other)?;
		file.seek(SeekFrom::Start(offset))?;
		file.read(buf)
	}

	fn file_size(&self) -> u64 {
		self.size
	}
}

#[derive(Debug)]
pub struct RegularFile {
	inner: Mutex<File>,
	size: u64,
}

impl RegularFile {
	pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
		let file = File::open(path.as_ref())?;
		let size = file.metadata()?.len();
		Ok(Self { inner: Mutex::new(file), size })
	}
}

impl AsRawFd for RegularFile {
	fn as_raw_fd(&self) -> RawFd {
		self.inner.lock().map(|f| f.as_raw_fd()).unwrap_or(-1)
	}
}

const PREFETCH_AHEAD: usize = 8;

/// Represents a file split into multiple chunks for efficient storage and transfer.
///
/// Uses binary search for chunk lookup, caches opened file handles, and prefetches
/// upcoming chunks in background threads to hide network latency.
#[derive(Debug)]
pub struct ChunkedFile {
	size: u64,
	chunks: Vec<(String, Chunk)>,
	fetcher: Arc<Fetcher>,
	state: Mutex<ChunkedFileState>,
}

#[derive(Debug)]
struct ChunkedFileState {
	open_handles: Vec<Option<File>>,
	prefetched: HashSet<usize>,
}

impl ChunkedFile {
	pub(crate) fn new(chunks: Vec<(String, Chunk)>, size: u64, fetcher: Fetcher) -> Self {
		let num_chunks = chunks.len();
		Self {
			chunks,
			size,
			fetcher: Arc::new(fetcher),
			state: Mutex::new(ChunkedFileState {
				open_handles: (0..num_chunks).map(|_| None).collect(),
				prefetched: HashSet::new(),
			}),
		}
	}

	fn find_chunk_index(&self, position: u64) -> Option<usize> {
		if self.chunks.is_empty() || position >= self.size {
			return None;
		}
		let idx = self
			.chunks
			.partition_point(|(_, chunk)| (chunk.offset as u64) <= position)
			.saturating_sub(1);
		let (_, chunk) = &self.chunks[idx];
		let end = chunk.offset as u64 + chunk.size as u64;
		if position >= chunk.offset as u64 && position < end { Some(idx) } else { None }
	}

	fn open_chunk(&self, index: usize) -> std::io::Result<File> {
		let (path, _) = &self.chunks[index];
		let local_path =
			self.fetcher.retrieve_file(path.as_str()).map_err(|_| ErrorKind::Unsupported)?;
		File::open(local_path)
	}

	fn prefetch_chunks(&self, from_index: usize, state: &mut ChunkedFileState) {
		for i in from_index + 1..self.chunks.len().min(from_index + 1 + PREFETCH_AHEAD) {
			if state.prefetched.contains(&i) {
				continue;
			}
			state.prefetched.insert(i);
			let fetcher = Arc::clone(&self.fetcher);
			let path = self.chunks[i].0.clone();
			thread::spawn(move || {
				let _ = fetcher.retrieve_file(path.as_str());
			});
		}
	}
}

impl FileLike for ChunkedFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
		if buf.is_empty() || offset >= self.size {
			return Ok(0);
		}

		let mut state = self.state.lock().map_err(|_| ErrorKind::Other)?;
		let mut position = offset;
		let mut currently_read = 0;

		let mut index = match self.find_chunk_index(position) {
			Some(i) => i,
			None => return Ok(0),
		};

		while currently_read < buf.len() && index < self.chunks.len() {
			let (_, chunk) = &self.chunks[index];
			let chunk_start = chunk.offset as u64;
			let chunk_end = chunk_start + chunk.size as u64;

			if position >= chunk_end {
				index += 1;
				continue;
			}

			let chunk_position = position - chunk_start;
			let remaining_in_chunk = chunk_end - position;
			let max_to_read = (buf.len() - currently_read).min(remaining_in_chunk as usize);

			if state.open_handles[index].is_none() {
				self.prefetch_chunks(index, &mut state);
				let file = self.open_chunk(index)?;
				state.open_handles[index] = Some(file);
			}

			let file = state.open_handles[index].as_mut().ok_or(ErrorKind::Other)?;
			file.seek(SeekFrom::Start(chunk_position))?;

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
			position += chunk_bytes_read as u64;
			index += 1;
		}

		Ok(currently_read)
	}

	fn file_size(&self) -> u64 {
		self.size
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
	fn chunked_file_read_at_empty_chunks() {
		let tmp = std::env::temp_dir().join(format!("cvmfs_test_readmt_{}", std::process::id()));
		std::fs::create_dir_all(&tmp).unwrap();
		let cache_dir = tmp.to_str().unwrap();
		let fetcher = Fetcher::new(cache_dir, cache_dir, true).unwrap();
		let cf = ChunkedFile::new(vec![], 0, fetcher);

		let mut buf = [0u8; 16];
		let n = cf.read_at(0, &mut buf).unwrap();
		assert_eq!(n, 0);

		std::fs::remove_dir_all(&tmp).ok();
	}

	#[test]
	fn chunked_file_read_at_past_eof() {
		let tmp = std::env::temp_dir().join(format!("cvmfs_test_eof_{}", std::process::id()));
		std::fs::create_dir_all(&tmp).unwrap();
		let cache_dir = tmp.to_str().unwrap();
		let fetcher = Fetcher::new(cache_dir, cache_dir, true).unwrap();
		let cf = ChunkedFile::new(vec![], 1000, fetcher);

		let mut buf = [0u8; 16];
		let n = cf.read_at(1000, &mut buf).unwrap();
		assert_eq!(n, 0);

		std::fs::remove_dir_all(&tmp).ok();
	}

	#[test]
	fn chunked_file_find_chunk_binary_search() {
		let tmp = std::env::temp_dir().join(format!("cvmfs_test_binsearch_{}", std::process::id()));
		std::fs::create_dir_all(&tmp).unwrap();
		let cache_dir = tmp.to_str().unwrap();
		let fetcher = Fetcher::new(cache_dir, cache_dir, true).unwrap();

		use crate::directory_entry::ContentHashTypes;
		let chunks = vec![
			(
				"chunk0".into(),
				Chunk {
					content_hash: "a".into(),
					content_hash_type: ContentHashTypes::Sha1,
					size: 100,
					offset: 0,
				},
			),
			(
				"chunk1".into(),
				Chunk {
					content_hash: "b".into(),
					content_hash_type: ContentHashTypes::Sha1,
					size: 100,
					offset: 100,
				},
			),
			(
				"chunk2".into(),
				Chunk {
					content_hash: "c".into(),
					content_hash_type: ContentHashTypes::Sha1,
					size: 100,
					offset: 200,
				},
			),
		];
		let cf = ChunkedFile::new(chunks, 300, fetcher);

		assert_eq!(cf.find_chunk_index(0), Some(0));
		assert_eq!(cf.find_chunk_index(99), Some(0));
		assert_eq!(cf.find_chunk_index(100), Some(1));
		assert_eq!(cf.find_chunk_index(200), Some(2));
		assert_eq!(cf.find_chunk_index(299), Some(2));
		assert_eq!(cf.find_chunk_index(300), None);

		std::fs::remove_dir_all(&tmp).ok();
	}

	#[test]
	fn chunked_file_file_size() {
		let tmp = std::env::temp_dir().join(format!("cvmfs_test_size_{}", std::process::id()));
		std::fs::create_dir_all(&tmp).unwrap();
		let cache_dir = tmp.to_str().unwrap();
		let fetcher = Fetcher::new(cache_dir, cache_dir, true).unwrap();
		let cf = ChunkedFile::new(vec![], 42, fetcher);

		assert_eq!(cf.file_size(), 42);

		std::fs::remove_dir_all(&tmp).ok();
	}
}
