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

use std::{fs, io::Read, path::Path, thread, time::Duration};

use compress::zlib;
use reqwest::blocking::Client;
use sha1::{Digest, Sha1};

use crate::{
	cache::Cache,
	common::{CvmfsError, CvmfsResult},
	geo::sort_servers_by_geo,
};

const MAX_DOWNLOAD_SIZE: u64 = 1024 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
const BACKOFF_INIT: Duration = Duration::from_secs(2);
const BACKOFF_MAX: Duration = Duration::from_secs(10);
const MAX_RETRIES: u32 = 3;

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
	pub mirrors: Vec<String>,
	pub proxy: Option<String>,
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
		Ok(Self { cache, source, mirrors: Vec::new(), proxy: None })
	}

	pub fn with_mirrors(
		sources: &[&str],
		cache_directory: &str,
		initialize: bool,
	) -> CvmfsResult<Self> {
		if sources.is_empty() {
			return Err(CvmfsError::Generic("at least one source is required".into()));
		}
		let mut fetcher = Self::new(sources[0], cache_directory, initialize)?;
		for &mirror in &sources[1..] {
			let path = Path::new(mirror);
			let url = if path.exists() && path.is_dir() {
				format!("file://{mirror}")
			} else {
				mirror.into()
			};
			fetcher.mirrors.push(url);
		}
		Ok(fetcher)
	}

	pub fn set_proxy(&mut self, proxy_url: &str) {
		self.proxy = Some(proxy_url.to_string());
	}

	pub fn sort_mirrors_by_geo(&mut self, repo_name: &str) {
		let mut all = vec![self.source.clone()];
		all.extend(self.mirrors.clone());
		if let Ok(sorted) = sort_servers_by_geo(&self.source, repo_name, &all) {
			let Some((first, rest)) = sorted.split_first() else {
				return;
			};
			self.source = first.clone();
			self.mirrors = rest.to_vec();
		}
	}

	/// Method to retrieve a file from the cache if exists, or from
	/// the repository if it doesn't. In case it has to be retrieved from
	/// the repository it won't be decompressed.
	pub fn retrieve_raw_file(&self, file_name: &str) -> CvmfsResult<String> {
		let cache_file = self.cache.add(file_name)?;
		let cache_str = cache_file.to_str().ok_or(CvmfsError::FileNotFound)?;
		let mut last_err = None;
		for source in self.all_sources() {
			let file_url = Path::join(source.as_ref(), file_name);
			match self.download_content_and_store(
				cache_str,
				file_url.to_str().ok_or(CvmfsError::FileNotFound)?,
			) {
				Ok(()) => {
					return Ok(self
						.cache
						.get(file_name)
						.ok_or(CvmfsError::FileNotFound)?
						.to_str()
						.ok_or(CvmfsError::FileNotFound)?
						.into());
				}
				Err(e) => last_err = Some(e),
			}
		}
		Err(last_err.unwrap_or(CvmfsError::FileNotFound))
	}

	pub fn retrieve_file(&self, file_name: &str) -> CvmfsResult<String> {
		if let Some(cached_file) = self.cache.get(file_name) {
			return Ok(cached_file.to_str().ok_or(CvmfsError::FileNotFound)?.into());
		}
		self.retrieve_file_from_source(file_name)
	}

	fn retrieve_file_from_source(&self, file_name: &str) -> CvmfsResult<String> {
		let cached_file = self.cache.add(file_name)?;
		let cache_str = cached_file.to_str().ok_or(CvmfsError::FileNotFound)?;
		let mut last_err = None;
		for source in self.all_sources() {
			let file_url = Path::join(source.as_ref(), file_name);
			match self.download_content_and_decompress(
				cache_str,
				file_url.to_str().ok_or(CvmfsError::FileNotFound)?,
			) {
				Ok(()) => {
					return match self.cache.get(file_name) {
						None => Err(CvmfsError::FileNotFound),
						Some(file) => Ok(file.to_str().ok_or(CvmfsError::FileNotFound)?.into()),
					};
				}
				Err(e) => last_err = Some(e),
			}
		}
		Err(last_err.unwrap_or(CvmfsError::FileNotFound))
	}

	fn all_sources(&self) -> impl Iterator<Item = &str> {
		std::iter::once(self.source.as_str()).chain(self.mirrors.iter().map(String::as_str))
	}

	fn build_client(&self) -> CvmfsResult<Client> {
		let mut builder = Client::builder().timeout(REQUEST_TIMEOUT);
		if let Some(proxy_url) = &self.proxy {
			builder = builder.proxy(
				reqwest::Proxy::all(proxy_url)
					.map_err(|e| CvmfsError::IO(format!("invalid proxy: {e}")))?,
			);
		}
		Ok(builder.build()?)
	}

	fn validated_get(&self, file_url: &str) -> CvmfsResult<Vec<u8>> {
		let client = self.build_client()?;
		let mut last_err = None;
		let mut delay = BACKOFF_INIT;
		for _ in 0..=MAX_RETRIES {
			match client.get(file_url).send() {
				Ok(response) => {
					if !response.status().is_success() {
						last_err = Some(CvmfsError::IO(format!(
							"HTTP {} for {}",
							response.status(),
							file_url
						)));
					} else if let Some(len) =
						response.content_length().filter(|&l| l > MAX_DOWNLOAD_SIZE)
					{
						return Err(CvmfsError::IO(format!("response too large: {len} bytes")));
					} else {
						return Ok(response.bytes()?.to_vec());
					}
				}
				Err(e) => last_err = Some(CvmfsError::from(e)),
			}
			thread::sleep(delay);
			delay = (delay * 2).min(BACKOFF_MAX);
		}
		Err(last_err.unwrap_or(CvmfsError::FileNotFound))
	}

	fn download_content_and_decompress(
		&self,
		cached_file: &str,
		file_url: &str,
	) -> CvmfsResult<()> {
		let file_bytes = self.validated_get(file_url)?;
		Self::decompress(&file_bytes, cached_file)?;
		Ok(())
	}

	fn download_content_and_store(&self, cached_file: &str, file_url: &str) -> CvmfsResult<()> {
		let content = self.validated_get(file_url)?;
		fs::write(cached_file, content)?;
		Ok(())
	}

	fn decompress(compressed_bytes: &[u8], cached_file: &str) -> CvmfsResult<()> {
		let decompressed = Self::try_decompress_zlib(compressed_bytes)
			.or_else(|_| Self::try_decompress_zstd(compressed_bytes))
			.or_else(|_| Self::try_decompress_lz4(compressed_bytes))?;
		fs::write(cached_file, decompressed)?;
		Ok(())
	}

	fn try_decompress_zlib(data: &[u8]) -> CvmfsResult<Vec<u8>> {
		let mut decompressed = Vec::new();
		zlib::Decoder::new(data).read_to_end(&mut decompressed)?;
		Ok(decompressed)
	}

	fn try_decompress_zstd(data: &[u8]) -> CvmfsResult<Vec<u8>> {
		let decompressed =
			zstd::stream::decode_all(data).map_err(|e| CvmfsError::IO(format!("zstd: {e}")))?;
		Ok(decompressed)
	}

	pub fn verify_hash(data: &[u8], expected_hash: &str) -> CvmfsResult<()> {
		let mut hasher = Sha1::new();
		hasher.update(data);
		let computed: String = hex::encode(hasher.finalize());
		let expected_clean = expected_hash.split('-').next().unwrap_or(expected_hash);
		if computed != expected_clean {
			return Err(CvmfsError::IO(format!(
				"hash mismatch: expected {expected_clean}, got {computed}"
			)));
		}
		Ok(())
	}

	pub fn retrieve_file_verified(
		&self,
		file_name: &str,
		expected_hash: &str,
	) -> CvmfsResult<String> {
		let path = self.retrieve_file(file_name)?;
		let data = fs::read(&path)?;
		Self::verify_hash(&data, expected_hash)?;
		Ok(path)
	}

	fn try_decompress_lz4(data: &[u8]) -> CvmfsResult<Vec<u8>> {
		let decompressed = lz4_flex::frame::FrameDecoder::new(data);
		let mut buf = Vec::new();
		std::io::BufReader::new(decompressed)
			.read_to_end(&mut buf)
			.map_err(|e| CvmfsError::IO(format!("lz4: {e}")))?;
		Ok(buf)
	}
}
