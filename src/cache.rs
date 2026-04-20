//! # Local Cache Management for CernVM-FS
//!
//! This module provides functionality for managing the local cache of CernVM-FS objects.
//! The cache stores downloaded repository objects to improve performance and allow
//! offline access to previously accessed content.
//!
//! ## Cache Structure
//!
//! The cache follows a two-level directory structure:
//! - The main cache directory contains a `data` subdirectory
//! - Inside `data`, objects are organized into 256 subdirectories (00-ff) based on the first two
//!   hex characters of their content hash
//!
//! ## Cache Operations
//!
//! The cache supports the following operations:
//! - Initialization: Creating the directory structure
//! - Adding: Determining the path where a file should be stored
//! - Retrieval: Looking up files by their identifier
//! - Eviction: Clearing the cache and rebuilding the structure

use std::{
	collections::HashMap,
	fs::{create_dir_all, remove_dir_all},
	path::{Path, PathBuf},
	sync::Mutex,
	time::{Duration, Instant},
};

use crate::common::{CvmfsError, CvmfsResult};

const DEFAULT_TTL: Duration = Duration::from_secs(60);
const DEFAULT_NEGATIVE_TTL: Duration = Duration::from_secs(5);

/// A cache for storing repository objects locally
///
/// The `Cache` struct manages a local directory structure where CernVM-FS objects
/// are stored. It provides methods for initialization, file lookup, and cache management.
#[derive(Debug)]
pub struct Cache {
	/// The root directory where cache files are stored.
	pub cache_directory: String,
	ttl: Duration,
	negative_ttl: Duration,
	negative_entries: Mutex<HashMap<String, Instant>>,
}

impl Cache {
	/// Creates a new cache instance with the specified root directory.
	///
	/// This constructor creates a new cache that will store files in the specified
	/// directory. It validates that the path can be properly represented as a string.
	///
	/// # Arguments
	///
	/// * `cache_directory` - The path to the root cache directory.
	///
	/// # Returns
	///
	/// Returns a `CvmfsResult<Self>` containing the new cache instance, or an error
	/// if the path is invalid.
	///
	/// # Errors
	///
	/// Returns `CvmfsError::FileNotFound` if the path cannot be converted to a string.
	pub fn new(cache_directory: String) -> CvmfsResult<Self> {
		let path = Path::new(&cache_directory);
		Ok(Self {
			cache_directory: path.to_str().ok_or(CvmfsError::FileNotFound)?.into(),
			ttl: DEFAULT_TTL,
			negative_ttl: DEFAULT_NEGATIVE_TTL,
			negative_entries: Mutex::new(HashMap::new()),
		})
	}

	pub fn with_ttl(mut self, ttl: Duration, negative_ttl: Duration) -> Self {
		self.ttl = ttl;
		self.negative_ttl = negative_ttl;
		self
	}

	/// Initializes the cache directory structure.
	///
	/// This method creates the cache directory structure if it doesn't exist. It creates
	/// a `data` subdirectory with 256 subdirectories (00-ff) to store objects based on
	/// the first two hex characters of their hash.
	///
	/// # Returns
	///
	/// Returns `Ok(())` if initialization is successful, or an error if directory
	/// creation fails.
	///
	/// # Errors
	///
	/// Returns filesystem errors if directory creation fails, or path conversion errors.
	pub fn initialize(&self) -> CvmfsResult<()> {
		let base_path = self.create_directory("data")?;
		for i in 0x00..=0xff {
			let new_folder = format!("{:02x}", i);
			let new_file = Path::join::<&Path>(base_path.as_ref(), new_folder.as_ref());
			create_dir_all(new_file)?;
		}
		Ok(())
	}

	/// Creates a directory within the cache root
	///
	/// This helper method creates a directory at the specified path relative to the
	/// cache root directory, ensuring all parent directories are created as needed.
	///
	/// # Arguments
	///
	/// * `path` - The relative path to create within the cache directory
	///
	/// # Returns
	///
	/// Returns a `CvmfsResult<String>` containing the full path to the created directory,
	/// or an error if directory creation or path conversion fails.
	///
	/// # Errors
	///
	/// Returns filesystem errors if directory creation fails, or `CvmfsError::FileNotFound`
	/// if the path cannot be converted to a string.
	fn create_directory(&self, path: &str) -> CvmfsResult<String> {
		let cache_full_path = Path::new(&self.cache_directory).join(path);
		create_dir_all(cache_full_path.clone())?;
		cache_full_path
			.into_os_string()
			.into_string()
			.map_err(|_| CvmfsError::FileNotFound)
	}

	/// Gets the path where a file would be stored in the cache
	///
	/// This method determines the full path where a file with the given name would
	/// be stored in the cache, without checking if it actually exists.
	///
	/// # Arguments
	///
	/// * `file_name` - The name of the file
	///
	/// # Returns
	///
	/// Returns a `PathBuf` with the full path where the file would be stored.
	pub fn add(&self, file_name: &str) -> CvmfsResult<PathBuf> {
		if file_name.contains("..") || file_name.starts_with('/') {
			return Err(CvmfsError::IO("invalid cache filename".to_string()));
		}
		let path = Path::join(self.cache_directory.as_ref(), file_name);
		Ok(path)
	}

	/// Retrieves the path to a file if it exists in the cache
	///
	/// This method checks if a file with the given name exists in the cache and
	/// returns its path if found.
	///
	/// # Arguments
	///
	/// * `file_name` - The name of the file to look up
	///
	/// # Returns
	///
	/// Returns an `Option<PathBuf>` containing the path to the file if it exists,
	/// or `None` if the file is not in the cache.
	pub fn get(&self, file_name: &str) -> Option<PathBuf> {
		if self.is_negative_cached(file_name) {
			return None;
		}
		let path = self.add(file_name).ok()?;
		if !path.is_file() {
			return None;
		}
		if self.is_expired(&path) {
			std::fs::remove_file(&path).ok();
			return None;
		}
		Some(path)
	}

	pub fn record_negative(&self, file_name: &str) {
		if let Ok(mut entries) = self.negative_entries.lock() {
			entries.insert(file_name.to_string(), Instant::now());
		}
	}

	fn is_negative_cached(&self, file_name: &str) -> bool {
		let Ok(mut entries) = self.negative_entries.lock() else {
			return false;
		};
		let Some(inserted) = entries.get(file_name) else {
			return false;
		};
		if inserted.elapsed() < self.negative_ttl {
			return true;
		}
		entries.remove(file_name);
		false
	}

	fn is_expired(&self, path: &Path) -> bool {
		path.metadata()
			.and_then(|m| m.modified())
			.map(|modified| modified.elapsed().unwrap_or_default() > self.ttl)
			.unwrap_or(true)
	}

	/// Clears the cache and re-initializes the directory structure.
	///
	/// This method removes all cached objects by deleting and recreating the data
	/// directory structure. It's useful for clearing corrupted cache data or freeing
	/// disk space.
	///
	/// # Returns
	///
	/// Returns `Ok(())` if eviction is successful, or an error if directory removal
	/// or reinitialization fails.
	///
	/// # Errors
	///
	/// Returns filesystem errors if directory operations fail.
	pub fn evict(&self) -> CvmfsResult<()> {
		let data_path = Path::new(&self.cache_directory).join("data");
		if data_path.exists() && data_path.is_dir() {
			remove_dir_all(data_path)?;
			self.initialize()?;
		}
		if let Ok(mut entries) = self.negative_entries.lock() {
			entries.clear();
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::fs;

	fn tmp_cache_dir(name: &str) -> PathBuf {
		std::env::temp_dir().join(format!("cvmfs_cache_{}_{}", name, std::process::id()))
	}

	#[test]
	fn cache_new_valid_path() {
		let dir = tmp_cache_dir("new");
		fs::create_dir_all(&dir).unwrap();
		let cache = Cache::new(dir.to_str().unwrap().into()).unwrap();
		assert_eq!(cache.cache_directory, dir.to_str().unwrap());
		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn cache_initialize_creates_data_subdirs() {
		let dir = tmp_cache_dir("init");
		fs::create_dir_all(&dir).unwrap();
		let cache = Cache::new(dir.to_str().unwrap().into()).unwrap();
		cache.initialize().unwrap();

		let data_dir = dir.join("data");
		assert!(data_dir.is_dir());

		// Spot-check a few subdirectories
		assert!(data_dir.join("00").is_dir());
		assert!(data_dir.join("0a").is_dir());
		assert!(data_dir.join("ff").is_dir());
		assert!(data_dir.join("7f").is_dir());

		// Count: should be exactly 256
		let count = fs::read_dir(&data_dir).unwrap().count();
		assert_eq!(count, 256);

		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn cache_add_normal_filename() {
		let dir = tmp_cache_dir("add");
		fs::create_dir_all(&dir).unwrap();
		let cache = Cache::new(dir.to_str().unwrap().into()).unwrap();

		let result = cache.add("data/ab/cdef1234").unwrap();
		assert_eq!(result, dir.join("data/ab/cdef1234"));

		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn cache_add_path_traversal_rejected() {
		let dir = tmp_cache_dir("traversal");
		fs::create_dir_all(&dir).unwrap();
		let cache = Cache::new(dir.to_str().unwrap().into()).unwrap();

		let result = cache.add("../etc/passwd");
		assert!(result.is_err());

		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn cache_add_absolute_path_rejected() {
		let dir = tmp_cache_dir("absolute");
		fs::create_dir_all(&dir).unwrap();
		let cache = Cache::new(dir.to_str().unwrap().into()).unwrap();

		let result = cache.add("/etc/passwd");
		assert!(result.is_err());

		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn cache_get_missing_file_returns_none() {
		let dir = tmp_cache_dir("get_miss");
		fs::create_dir_all(&dir).unwrap();
		let cache = Cache::new(dir.to_str().unwrap().into()).unwrap();
		cache.initialize().unwrap();

		assert!(cache.get("data/ab/nonexistent").is_none());

		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn cache_get_existing_file_returns_some() {
		let dir = tmp_cache_dir("get_hit");
		fs::create_dir_all(&dir).unwrap();
		let cache = Cache::new(dir.to_str().unwrap().into()).unwrap();
		cache.initialize().unwrap();

		// Write a file into the cache
		let file_path = dir.join("data/ab/testfile");
		fs::write(&file_path, b"content").unwrap();

		let result = cache.get("data/ab/testfile");
		assert!(result.is_some());
		assert_eq!(result.unwrap(), file_path);

		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn cache_negative_entry_blocks_lookup() {
		let dir = tmp_cache_dir("neg");
		fs::create_dir_all(&dir).unwrap();
		let cache = Cache::new(dir.to_str().unwrap().into()).unwrap();
		cache.initialize().unwrap();

		cache.record_negative("data/ab/missing");
		assert!(cache.get("data/ab/missing").is_none());

		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn cache_negative_entry_expires() {
		let dir = tmp_cache_dir("neg_expire");
		fs::create_dir_all(&dir).unwrap();
		let cache = Cache::new(dir.to_str().unwrap().into())
			.unwrap()
			.with_ttl(Duration::from_secs(60), Duration::from_millis(1));
		cache.initialize().unwrap();

		cache.record_negative("data/ab/willexpire");
		std::thread::sleep(Duration::from_millis(5));

		let file_path = dir.join("data/ab/willexpire");
		fs::write(&file_path, b"data").unwrap();
		assert!(cache.get("data/ab/willexpire").is_some());

		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn cache_ttl_expired_file_not_returned() {
		let dir = tmp_cache_dir("ttl_exp");
		fs::create_dir_all(&dir).unwrap();
		let cache = Cache::new(dir.to_str().unwrap().into())
			.unwrap()
			.with_ttl(Duration::from_millis(1), Duration::from_secs(5));
		cache.initialize().unwrap();

		let file_path = dir.join("data/ab/expired");
		fs::write(&file_path, b"old data").unwrap();
		std::thread::sleep(Duration::from_millis(5));

		assert!(cache.get("data/ab/expired").is_none());
		assert!(!file_path.exists());

		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn cache_evict_clears_negative_entries() {
		let dir = tmp_cache_dir("evict_neg");
		fs::create_dir_all(&dir).unwrap();
		let cache = Cache::new(dir.to_str().unwrap().into()).unwrap();
		cache.initialize().unwrap();

		cache.record_negative("data/ab/neg");
		cache.evict().unwrap();

		let file_path = dir.join("data/ab/neg");
		fs::write(&file_path, b"data").unwrap();
		assert!(cache.get("data/ab/neg").is_some());

		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn cache_evict_clears_and_recreates() {
		let dir = tmp_cache_dir("evict");
		fs::create_dir_all(&dir).unwrap();
		let cache = Cache::new(dir.to_str().unwrap().into()).unwrap();
		cache.initialize().unwrap();

		// Put a file in the cache
		let file_path = dir.join("data/ab/toevict");
		fs::write(&file_path, b"data").unwrap();
		assert!(file_path.is_file());

		cache.evict().unwrap();

		// File should be gone
		assert!(!file_path.is_file());
		// data/ directory should still exist (re-initialized)
		assert!(dir.join("data").is_dir());
		// Subdirectories should be recreated
		assert!(dir.join("data/ab").is_dir());

		fs::remove_dir_all(&dir).ok();
	}
}
