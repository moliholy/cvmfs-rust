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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Debug)]
pub struct Fetcher {
	pub cache: Cache,
	pub source: String,
	pub mirrors: Vec<String>,
	pub proxy: Option<String>,
	pub offline: AtomicBool,
	pub io_errors: AtomicU64,
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
		Ok(Self {
			cache,
			source,
			mirrors: Vec::new(),
			proxy: None,
			offline: AtomicBool::new(false),
			io_errors: AtomicU64::new(0),
		})
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
		if self.offline.load(Ordering::Relaxed) {
			return Err(CvmfsError::FileNotFound);
		}
		match self.retrieve_file_from_source(file_name) {
			Ok(path) => {
				self.offline.store(false, Ordering::Relaxed);
				Ok(path)
			}
			Err(e) => {
				self.io_errors.fetch_add(1, Ordering::Relaxed);
				if let Some(cached_file) = self.cache.get(file_name) {
					log::warn!("network failed, serving {file_name} from stale cache");
					self.offline.store(true, Ordering::Relaxed);
					return Ok(cached_file.to_str().ok_or(CvmfsError::FileNotFound)?.into());
				}
				Err(e)
			}
		}
	}

	pub fn is_offline(&self) -> bool {
		self.offline.load(Ordering::Relaxed)
	}

	pub fn io_error_count(&self) -> u64 {
		self.io_errors.load(Ordering::Relaxed)
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

#[cfg(test)]
mod tests {
	use super::*;

	fn tmp_cache(name: &str) -> String {
		let dir =
			std::env::temp_dir().join(format!("cvmfs_fetcher_{}_{}", name, std::process::id()));
		fs::create_dir_all(&dir).unwrap();
		dir.to_str().unwrap().to_string()
	}

	#[test]
	fn try_decompress_zlib_valid() {
		use std::io::Write;
		let mut encoder =
			flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
		encoder.write_all(b"hello world").unwrap();
		let compressed = encoder.finish().unwrap();
		let result = Fetcher::try_decompress_zlib(&compressed).unwrap();
		assert_eq!(result, b"hello world");
	}

	#[test]
	fn try_decompress_zlib_invalid() {
		let result = Fetcher::try_decompress_zlib(b"not zlib data");
		assert!(result.is_err());
	}

	#[test]
	fn try_decompress_zstd_valid() {
		let data = b"hello zstd world";
		let compressed = zstd::stream::encode_all(&data[..], 3).unwrap();
		let result = Fetcher::try_decompress_zstd(&compressed).unwrap();
		assert_eq!(result, data);
	}

	#[test]
	fn try_decompress_zstd_invalid() {
		let result = Fetcher::try_decompress_zstd(b"not zstd data");
		assert!(result.is_err());
	}

	#[test]
	fn try_decompress_lz4_valid() {
		use lz4_flex::frame::FrameEncoder;
		let mut encoder = FrameEncoder::new(Vec::new());
		std::io::Write::write_all(&mut encoder, b"hello lz4").unwrap();
		let compressed = encoder.finish().unwrap();
		let result = Fetcher::try_decompress_lz4(&compressed).unwrap();
		assert_eq!(result, b"hello lz4");
	}

	#[test]
	fn try_decompress_lz4_invalid() {
		let result = Fetcher::try_decompress_lz4(b"not lz4 data");
		assert!(result.is_err());
	}

	#[test]
	fn decompress_tries_all_formats() {
		use std::io::Write;
		let cache_dir = tmp_cache("decompress");
		let output = format!("{cache_dir}/output");

		let mut encoder =
			flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
		encoder.write_all(b"zlib content").unwrap();
		let compressed = encoder.finish().unwrap();
		Fetcher::decompress(&compressed, &output).unwrap();
		assert_eq!(fs::read_to_string(&output).unwrap(), "zlib content");

		let compressed = zstd::stream::encode_all(&b"zstd content"[..], 3).unwrap();
		Fetcher::decompress(&compressed, &output).unwrap();
		assert_eq!(fs::read_to_string(&output).unwrap(), "zstd content");

		fs::remove_dir_all(&cache_dir).ok();
	}

	#[test]
	fn decompress_all_formats_fail() {
		let cache_dir = tmp_cache("decompress_fail");
		let output = format!("{cache_dir}/output");
		let result = Fetcher::decompress(b"garbage", &output);
		assert!(result.is_err());
		fs::remove_dir_all(&cache_dir).ok();
	}

	#[test]
	fn verify_hash_correct() {
		let data = b"test data for hashing";
		let mut hasher = Sha1::new();
		hasher.update(data);
		let hash = hex::encode(hasher.finalize());
		Fetcher::verify_hash(data, &hash).unwrap();
	}

	#[test]
	fn verify_hash_with_suffix() {
		let data = b"test";
		let mut hasher = Sha1::new();
		hasher.update(data);
		let hash = format!("{}-rmd160", hex::encode(hasher.finalize()));
		Fetcher::verify_hash(data, &hash).unwrap();
	}

	#[test]
	fn verify_hash_mismatch() {
		let result = Fetcher::verify_hash(b"data", "0000000000000000000000000000000000000000");
		assert!(result.is_err());
	}

	#[test]
	fn set_proxy_stores_value() {
		let cache_dir = tmp_cache("proxy");
		let mut fetcher = Fetcher::new("http://example.com", &cache_dir, false).unwrap();
		assert!(fetcher.proxy.is_none());
		fetcher.set_proxy("http://proxy:8080");
		assert_eq!(fetcher.proxy.as_deref(), Some("http://proxy:8080"));
		fs::remove_dir_all(&cache_dir).ok();
	}

	#[test]
	fn all_sources_includes_mirrors() {
		let cache_dir = tmp_cache("sources");
		let fetcher = Fetcher::with_mirrors(
			&["http://a.com", "http://b.com", "http://c.com"],
			&cache_dir,
			false,
		)
		.unwrap();
		let sources: Vec<&str> = fetcher.all_sources().collect();
		assert_eq!(sources, vec!["http://a.com", "http://b.com", "http://c.com"]);
		fs::remove_dir_all(&cache_dir).ok();
	}

	#[test]
	fn with_mirrors_empty_errors() {
		let cache_dir = tmp_cache("mirrors_empty");
		fs::create_dir_all(&cache_dir).unwrap();
		let result = Fetcher::with_mirrors(&[], &cache_dir, false);
		assert!(result.is_err());
		fs::remove_dir_all(&cache_dir).ok();
	}

	#[test]
	fn build_client_no_proxy() {
		let cache_dir = tmp_cache("client_noproxy");
		let fetcher = Fetcher::new("http://example.com", &cache_dir, false).unwrap();
		let client = fetcher.build_client();
		assert!(client.is_ok());
		fs::remove_dir_all(&cache_dir).ok();
	}

	#[test]
	fn build_client_with_proxy() {
		let cache_dir = tmp_cache("client_proxy");
		let mut fetcher = Fetcher::new("http://example.com", &cache_dir, false).unwrap();
		fetcher.set_proxy("http://proxy:3128");
		let client = fetcher.build_client();
		assert!(client.is_ok());
		fs::remove_dir_all(&cache_dir).ok();
	}

	#[test]
	fn new_with_local_dir() {
		let cache_dir = tmp_cache("local_src");
		let src_dir = tmp_cache("local_repo");
		let fetcher = Fetcher::new(&src_dir, &cache_dir, false).unwrap();
		assert!(fetcher.source.starts_with("file://"));
		fs::remove_dir_all(&cache_dir).ok();
		fs::remove_dir_all(&src_dir).ok();
	}

	#[test]
	fn offline_and_io_errors_default() {
		let cache_dir = tmp_cache("defaults");
		let fetcher = Fetcher::new("http://example.com", &cache_dir, false).unwrap();
		assert!(!fetcher.is_offline());
		assert_eq!(fetcher.io_error_count(), 0);
		fs::remove_dir_all(&cache_dir).ok();
	}

	#[test]
	fn retrieve_file_offline_returns_error() {
		let cache_dir = tmp_cache("offline");
		let fetcher = Fetcher::new("http://example.com", &cache_dir, true).unwrap();
		fetcher.offline.store(true, Ordering::Relaxed);
		let result = fetcher.retrieve_file("nonexistent");
		assert!(result.is_err());
		fs::remove_dir_all(&cache_dir).ok();
	}

	#[test]
	fn retrieve_file_cached_returns_path() {
		let cache_dir = tmp_cache("cached");
		let fetcher = Fetcher::new("http://example.com", &cache_dir, true).unwrap();
		let file_path = format!("{cache_dir}/testfile");
		fs::write(&file_path, b"cached content").unwrap();
		let result = fetcher.retrieve_file("testfile").unwrap();
		assert!(result.contains("testfile"));
		fs::remove_dir_all(&cache_dir).ok();
	}

	#[test]
	fn download_content_and_store_writes_file() {
		let cache_dir = tmp_cache("store");
		let fetcher =
			Fetcher::new("http://cvmfs-stratum-one.cern.ch/opt/boss", &cache_dir, true).unwrap();
		let output = format!("{cache_dir}/.cvmfspublished");
		fetcher
			.download_content_and_store(
				&output,
				"http://cvmfs-stratum-one.cern.ch/opt/boss/.cvmfspublished",
			)
			.unwrap();
		assert!(Path::new(&output).exists());
		assert!(fs::read(&output).unwrap().len() > 10);
		fs::remove_dir_all(&cache_dir).ok();
	}
}
