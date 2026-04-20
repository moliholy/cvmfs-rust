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
//! - Inside `data`, objects are organized into 256 subdirectories (00-ff) based on
//!   the first two hex characters of their content hash
//!
//! ## Cache Operations
//!
//! The cache supports the following operations:
//! - Initialization: Creating the directory structure
//! - Adding: Determining the path where a file should be stored
//! - Retrieval: Looking up files by their identifier
//! - Eviction: Clearing the cache and rebuilding the structure

use std::fs::{create_dir_all, remove_dir_all};
use std::path::{Path, PathBuf};

use crate::common::{CvmfsError, CvmfsResult};

/// A cache for storing repository objects locally
///
/// The `Cache` struct manages a local directory structure where CernVM-FS objects
/// are stored. It provides methods for initialization, file lookup, and cache management.
#[derive(Debug, Clone)]
pub struct Cache {
    /// The root directory where cache files are stored.
    pub cache_directory: String,
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
        })
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
        let path = self.add(file_name).ok()?;
        if path.is_file() {
            return Some(path);
        }
        None
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
        Ok(())
    }
}
