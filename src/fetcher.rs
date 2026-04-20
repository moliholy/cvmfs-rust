//! # Repository Content Fetcher for CernVM-FS
//!
//! This module provides functionality to retrieve files and objects from a CernVM-FS
//! repository. It handles both local cache operations and remote retrieval from the
//! repository server when content isn't cached.
//!
//! ## Features
//!
//! * Local file caching to improve performance and enable offline access
//! * Transparent decompression of repository objects
//! * HTTP/HTTPS downloads from repository servers
//! * Fallback strategies when primary sources are unavailable
//!
//! ## Cache Management
//!
//! The fetcher uses a local cache to store downloaded files, reducing network traffic
//! and enabling faster access to previously retrieved content. When a file is requested,
//! the fetcher first checks if it exists in the cache before attempting to download it.
//!
//! ## Repository URL Structure
//!
//! CernVM-FS repositories are typically accessible via HTTP(S) with a URL structure that
//! includes the repository name and a `.cvmfs` suffix. For example:
//! `http://cvmfs-stratum-one.cern.ch/cvmfs/atlas.cern.ch`

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

use reqwest::blocking::Client;

use crate::cache::Cache;
use crate::common::{CvmfsError, CvmfsResult};
use compress::zlib;

const MAX_DOWNLOAD_SIZE: u64 = 1024 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Manages retrieval of repository content from both cache and remote sources
///
/// The `Fetcher` is responsible for obtaining files from a CernVM-FS repository,
/// handling local caching, and remote downloads. It abstracts away the details of
/// where and how repository objects are stored, providing a simple interface for
/// retrieving content by name or path.
///
/// It supports:
/// * Caching downloaded files to avoid redundant network requests
/// * Automatic decompression of repository content
/// * Fallback to remote sources when files aren't in the cache
#[derive(Debug)]
pub struct Fetcher {
    pub cache: Cache,
    pub source: String,
}

impl Fetcher {
    pub fn new(source: &str, cache_directory: &str, initialize: bool) -> CvmfsResult<Self> {
        let path = Path::new(source);
        let source = if path.exists() && path.is_dir() {
            format!("{}{}", "file://", source)
        } else {
            source.into()
        };
        let cache = Cache::new(cache_directory.into())?;
        if initialize {
            cache.initialize()?;
        }
        Ok(Self { cache, source })
    }

    /// Method to retrieve a file from the cache if exists, or from
    /// the repository if it doesn't. In case it has to be retrieved from
    /// the repository it won't be decompressed.
    pub fn retrieve_raw_file(&self, file_name: &str) -> CvmfsResult<String> {
        let cache_file = self.cache.add(file_name)?;
        let file_url = self.make_file_url(file_name);
        Self::download_content_and_store(
            cache_file.to_str().ok_or(CvmfsError::FileNotFound)?,
            file_url.to_str().ok_or(CvmfsError::FileNotFound)?,
        )?;
        Ok(self
            .cache
            .get(file_name)
            .ok_or(CvmfsError::FileNotFound)?
            .to_str()
            .ok_or(CvmfsError::FileNotFound)?
            .into())
    }

    pub fn retrieve_file(&self, file_name: &str) -> CvmfsResult<String> {
        if let Some(cached_file) = self.cache.get(file_name) {
            return Ok(cached_file.to_str().ok_or(CvmfsError::FileNotFound)?.into());
        }
        self.retrieve_file_from_source(file_name)
    }

    fn make_file_url(&self, file_name: &str) -> PathBuf {
        Path::join(self.source.as_ref(), file_name)
    }

    fn retrieve_file_from_source(&self, file_name: &str) -> CvmfsResult<String> {
        let file_url = self.make_file_url(file_name);
        let cached_file = self.cache.add(file_name)?;
        Self::download_content_and_decompress(
            cached_file.to_str().ok_or(CvmfsError::FileNotFound)?,
            file_url.to_str().ok_or(CvmfsError::FileNotFound)?,
        )?;
        match self.cache.get(file_name) {
            None => Err(CvmfsError::FileNotFound),
            Some(file) => Ok(file.to_str().ok_or(CvmfsError::FileNotFound)?.into()),
        }
    }

    fn validated_get(file_url: &str) -> CvmfsResult<Vec<u8>> {
        let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
        let response = client.get(file_url).send()?;
        if !response.status().is_success() {
            return Err(CvmfsError::IO(format!(
                "HTTP {} for {}",
                response.status(),
                file_url
            )));
        }
        if let Some(len) = response.content_length().filter(|&l| l > MAX_DOWNLOAD_SIZE) {
            return Err(CvmfsError::IO(format!("response too large: {len} bytes")));
        }
        Ok(response.bytes()?.to_vec())
    }

    fn download_content_and_decompress(cached_file: &str, file_url: &str) -> CvmfsResult<()> {
        let file_bytes = Self::validated_get(file_url)?;
        Self::decompress(&file_bytes, cached_file)?;
        Ok(())
    }

    fn download_content_and_store(cached_file: &str, file_url: &str) -> CvmfsResult<()> {
        let content = Self::validated_get(file_url)?;
        fs::write(cached_file, content)?;
        Ok(())
    }

    fn decompress(compressed_bytes: &[u8], cached_file: &str) -> CvmfsResult<()> {
        let mut decompressed = Vec::new();
        zlib::Decoder::new(compressed_bytes).read_to_end(&mut decompressed)?;
        fs::write(cached_file, decompressed)?;
        Ok(())
    }
}
