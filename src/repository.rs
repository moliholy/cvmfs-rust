use std::{collections::HashMap, fs, fs::File, sync::RwLock};

use chrono::{DateTime, Utc};

use crate::{
	catalog::{CATALOG_ROOT_PREFIX, Catalog, Statistics},
	certificate::{CERTIFICATE_ROOT_PREFIX, Certificate},
	common::{
		ChunkedFile, CvmfsError, CvmfsResult, FileLike, LAST_REPLICATION_NAME, MANIFEST_NAME,
		REFLOG_NAME, REPLICATING_NAME, RegularFile, WHITELIST_NAME, compose_object_path,
	},
	directory_entry::{Chunk, DirectoryEntry},
	fetcher::Fetcher,
	history::History,
	manifest::Manifest,
	reflog::Reflog,
	revision_tag::RevisionTag,
	root_file::RootFile,
	whitelist::Whitelist,
};

/// Wrapper around a CVMFS repository representation
#[derive(Debug)]
pub struct Repository {
	pub opened_catalogs: RwLock<HashMap<String, Catalog>>,
	pub manifest: Manifest,
	pub fqrn: String,
	pub repo_type: String,
	pub replicating_since: Option<DateTime<Utc>>,
	pub last_replication: Option<DateTime<Utc>>,
	pub replicating: bool,
	fetcher: Fetcher,
	tag: Option<RevisionTag>,
	catalog_hash_cache: RwLock<HashMap<String, String>>,
}

impl Repository {
	pub fn new(fetcher: Fetcher) -> CvmfsResult<Self> {
		let manifest = Self::read_manifest(&fetcher)?;
		Self::validate_whitelist(&fetcher, &manifest.repository_name)?;
		let last_replication =
			Self::try_to_get_last_replication_timestamp(&fetcher).unwrap_or(None);
		let replicating_since = Self::try_to_get_replication_state(&fetcher).unwrap_or(None);
		let mut obj = Self {
			opened_catalogs: RwLock::new(HashMap::new()),
			fqrn: manifest.repository_name.clone(),
			manifest,
			repo_type: "stratum1".to_string(),
			replicating_since,
			last_replication,
			replicating: replicating_since.is_some(),
			fetcher,
			tag: None,
			catalog_hash_cache: RwLock::new(HashMap::new()),
		};
		obj.tag = Some(obj.get_last_tag()?.clone());
		Ok(obj)
	}

	/// Retrieves an object from the content addressable storage or external source
	pub fn retrieve_object(
		&self,
		dirent: &DirectoryEntry,
		path: &str,
	) -> CvmfsResult<Box<dyn FileLike>> {
		if dirent.is_external_file() {
			let external_path = self.fetcher.retrieve_raw_file(path)?;
			return Ok(Box::new(RegularFile::open(external_path)?));
		}
		let mut dirent = dirent.clone();
		if dirent.chunks.is_empty() {
			self.load_chunks_for_entry(&mut dirent, path)?;
		}
		if dirent.has_chunks() {
			let chunks: CvmfsResult<Vec<(String, Chunk)>> = dirent
				.chunks
				.iter()
				.cloned()
				.map(|chunk| -> CvmfsResult<(String, Chunk)> {
					let path = compose_object_path(chunk.content_hash_string().as_str(), "P")
						.to_str()
						.ok_or(CvmfsError::FileNotFound)?
						.to_string();
					Ok((path, chunk))
				})
				.collect();
			Ok(Box::new(ChunkedFile::new(
				chunks?,
				dirent.size as u64,
				Fetcher::new(
					self.fetcher.source.as_str(),
					self.fetcher.cache.cache_directory.as_str(),
					false,
				)?,
			)))
		} else {
			let path = self.retrieve_object_with_suffix(
				dirent
					.content_hash_string()
					.expect("Content hash must be present if no chunks")
					.as_str(),
				"",
			)?;
			Ok(Box::new(RegularFile::open(path)?))
		}
	}

	fn load_chunks_for_entry(&self, entry: &mut DirectoryEntry, path: &str) -> CvmfsResult<()> {
		let hash = self.resolve_catalog_hash(path)?;
		let catalogs = self.opened_catalogs.read().map_err(|_| CvmfsError::Sync)?;
		let catalog = catalogs.get(&hash).ok_or(CvmfsError::CatalogNotFound)?;
		catalog.load_chunks(entry)
	}

	pub fn retrieve_object_with_suffix(
		&self,
		object_hash: &str,
		hash_suffix: &str,
	) -> CvmfsResult<String> {
		let path = compose_object_path(object_hash, hash_suffix);
		self.fetcher.retrieve_file(path.to_str().ok_or(CvmfsError::FileNotFound)?)
	}

	/// Download and open a catalog from the repository
	fn ensure_catalog_loaded(&self, catalog_hash: &str) -> CvmfsResult<()> {
		if self
			.opened_catalogs
			.read()
			.map_err(|_| CvmfsError::Sync)?
			.contains_key(catalog_hash)
		{
			return Ok(());
		}
		let catalog_file = self.retrieve_object_with_suffix(catalog_hash, CATALOG_ROOT_PREFIX)?;
		let catalog = Catalog::new(catalog_file, catalog_hash.into())?;
		self.opened_catalogs
			.write()
			.map_err(|_| CvmfsError::Sync)?
			.insert(catalog_hash.into(), catalog);
		Ok(())
	}

	pub fn has_history(&self) -> bool {
		self.manifest.has_history()
	}

	pub fn retrieve_history(&self) -> CvmfsResult<History> {
		if !self.has_history() {
			return Err(CvmfsError::HistoryNotFound);
		}
		let history_db = self.retrieve_object_with_suffix(
			self.manifest.history_database.as_ref().ok_or(CvmfsError::HistoryNotFound)?,
			"H",
		)?;
		History::new(&history_db)
	}

	pub fn get_tag(&self, number: u32) -> CvmfsResult<RevisionTag> {
		let history = self.retrieve_history()?;
		let tag = history.get_tag_by_revision(number)?;
		match tag {
			None => Err(CvmfsError::RevisionNotFound),
			Some(tag) => Ok(tag),
		}
	}

	pub fn current_tag(&self) -> CvmfsResult<&RevisionTag> {
		self.tag.as_ref().ok_or(CvmfsError::TagNotFound)
	}

	pub fn set_current_tag(&mut self, number: u32) -> CvmfsResult<()> {
		self.tag = Some(self.get_tag(number)?);
		Ok(())
	}

	pub fn get_last_tag(&self) -> CvmfsResult<RevisionTag> {
		self.get_tag(self.manifest.revision)
	}

	fn read_manifest(fetcher: &Fetcher) -> CvmfsResult<Manifest> {
		let manifest_file = fetcher.retrieve_raw_file(MANIFEST_NAME)?;
		let file = File::open(&manifest_file)?;
		let root_file = RootFile::new(&file)?;
		let manifest = Manifest::new(root_file)?;
		Self::verify_manifest(fetcher, &manifest)?;
		Ok(manifest)
	}

	fn verify_manifest(fetcher: &Fetcher, manifest: &Manifest) -> CvmfsResult<()> {
		let signature =
			manifest.root_file.signature().ok_or(CvmfsError::IncompleteRootFileSignature)?;
		let checksum =
			manifest.root_file.checksum().ok_or(CvmfsError::IncompleteRootFileSignature)?;
		let cert_path = compose_object_path(&manifest.certificate, CERTIFICATE_ROOT_PREFIX);
		let cert_file =
			fetcher.retrieve_file(cert_path.to_str().ok_or(CvmfsError::FileNotFound)?)?;
		let cert_bytes = fs::read(&cert_file)?;
		let certificate = Certificate::try_from(cert_bytes.as_slice())?;
		let valid = certificate.verify(signature, checksum.as_bytes())?;
		if !valid {
			return Err(CvmfsError::InvalidRootFileSignature);
		}
		Ok(())
	}

	fn get_replication_date(
		fetcher: &Fetcher,
		file_name: &str,
	) -> CvmfsResult<Option<DateTime<Utc>>> {
		let file = fetcher.retrieve_raw_file(file_name)?;
		let date_string = fs::read_to_string(&file)?;
		let date = DateTime::parse_from_str(&date_string, "%a %e %h %H:%M:%S %Z %Y");
		match date {
			Ok(date) => Ok(Some(DateTime::from(date))),
			Err(_) => Ok(None),
		}
	}

	fn validate_whitelist(fetcher: &Fetcher, fqrn: &str) -> CvmfsResult<()> {
		let whitelist_file = fetcher.retrieve_raw_file(WHITELIST_NAME)?;
		let content = fs::read(&whitelist_file)?;
		let whitelist = Whitelist::parse(&content)?;
		if !whitelist.matches_repository(fqrn) {
			return Err(CvmfsError::Generic(format!(
				"whitelist repository '{}' does not match '{fqrn}'",
				whitelist.repository_name
			)));
		}
		if whitelist.is_expired() {
			return Err(CvmfsError::Generic("whitelist has expired".into()));
		}
		Ok(())
	}

	fn try_to_get_last_replication_timestamp(
		fetcher: &Fetcher,
	) -> CvmfsResult<Option<DateTime<Utc>>> {
		Self::get_replication_date(fetcher, LAST_REPLICATION_NAME)
	}

	fn try_to_get_replication_state(fetcher: &Fetcher) -> CvmfsResult<Option<DateTime<Utc>>> {
		Self::get_replication_date(fetcher, REPLICATING_NAME)
	}

	pub fn get_revision_number(&self) -> CvmfsResult<i32> {
		Ok(self.current_tag()?.revision)
	}

	pub fn get_root_hash(&self) -> CvmfsResult<&str> {
		Ok(&self.current_tag()?.hash)
	}

	pub fn get_name(&self) -> CvmfsResult<&str> {
		Ok(&self.current_tag()?.name)
	}

	pub fn get_timestamp(&self) -> CvmfsResult<i64> {
		Ok(self.current_tag()?.timestamp)
	}

	pub fn retrieve_current_root_catalog(&self) -> CvmfsResult<()> {
		let root_hash = self.current_tag()?.hash.to_string();
		self.ensure_catalog_loaded(&root_hash)
	}

	/// Recursively walk down the Catalogs and find the best fit hash for a path
	fn resolve_catalog_hash(&self, needle_path: &str) -> CvmfsResult<String> {
		if let Some(cached) =
			self.catalog_hash_cache.read().ok().and_then(|c| c.get(needle_path).cloned())
		{
			return Ok(cached);
		}
		let mut hash = String::from(self.get_root_hash()?);
		loop {
			self.ensure_catalog_loaded(&hash)?;
			let catalogs = self.opened_catalogs.read().map_err(|_| CvmfsError::Sync)?;
			let catalog = catalogs.get(&hash).ok_or(CvmfsError::CatalogNotFound)?;
			match catalog.find_nested_for_path(needle_path) {
				Ok(None) => {
					if let Ok(mut cache) = self.catalog_hash_cache.write() {
						cache.insert(needle_path.into(), hash.clone());
					}
					return Ok(hash);
				}
				Ok(Some(nested_reference)) => {
					let next = nested_reference.catalog_hash.clone();
					drop(catalogs);
					hash = next;
				}
				Err(error) => return Err(error),
			};
		}
	}

	pub fn lookup(&self, path: &str) -> CvmfsResult<DirectoryEntry> {
		let path = if path == "/" { "" } else { path };
		let hash = self.resolve_catalog_hash(path)?;
		let catalogs = self.opened_catalogs.read().map_err(|_| CvmfsError::Sync)?;
		let catalog = catalogs.get(&hash).ok_or(CvmfsError::CatalogNotFound)?;
		catalog.find_directory_entry(path)
	}

	pub fn lookup_with_chunks(&self, path: &str) -> CvmfsResult<DirectoryEntry> {
		let canonical = if path == "/" { "" } else { path };
		let hash = self.resolve_catalog_hash(canonical)?;
		let catalogs = self.opened_catalogs.read().map_err(|_| CvmfsError::Sync)?;
		let catalog = catalogs.get(&hash).ok_or(CvmfsError::CatalogNotFound)?;
		let mut entry = catalog.find_directory_entry(canonical)?;
		catalog.load_chunks(&mut entry)?;
		Ok(entry)
	}

	pub fn get_file(&self, path: &str) -> CvmfsResult<Box<dyn FileLike>> {
		let directory_entry = self.lookup(path)?;
		if !directory_entry.is_file() {
			return Err(CvmfsError::NotAFile);
		}
		self.retrieve_object(&directory_entry, path)
	}

	/// List all the entries in a directory
	pub fn list_directory(&self, path: &str) -> CvmfsResult<Vec<DirectoryEntry>> {
		let path = if path == "/" { "" } else { path };
		let hash = self.resolve_catalog_hash(path)?;
		let catalogs = self.opened_catalogs.read().map_err(|_| CvmfsError::Sync)?;
		let catalog = catalogs.get(&hash).ok_or(CvmfsError::CatalogNotFound)?;
		catalog.list_directory(path)
	}

	pub fn fetcher_source(&self) -> String {
		self.fetcher.source.clone()
	}

	pub fn get_statistics(&self) -> CvmfsResult<Statistics> {
		self.retrieve_current_root_catalog()?;
		let root_hash = self.current_tag()?.hash.to_string();
		let catalogs = self.opened_catalogs.read().map_err(|_| CvmfsError::Sync)?;
		let catalog = catalogs.get(&root_hash).ok_or(CvmfsError::CatalogNotFound)?;
		catalog.get_statistics()
	}

	pub fn retrieve_reflog(&self) -> CvmfsResult<Reflog> {
		let reflog_file = self.fetcher.retrieve_raw_file(REFLOG_NAME)?;
		Reflog::new(&reflog_file)
	}

	pub fn catalog_count(&self) -> usize {
		self.opened_catalogs.read().map(|c| c.len()).unwrap_or(0)
	}

	pub fn retrieve_catalog_for_path(&self, path: &str) -> CvmfsResult<String> {
		self.resolve_catalog_hash(path)
	}

	pub fn with_catalog<F, R>(&self, hash: &str, f: F) -> CvmfsResult<R>
	where
		F: FnOnce(&Catalog) -> CvmfsResult<R>,
	{
		self.ensure_catalog_loaded(hash)?;
		let catalogs = self.opened_catalogs.read().map_err(|_| CvmfsError::Sync)?;
		let catalog = catalogs.get(hash).ok_or(CvmfsError::CatalogNotFound)?;
		f(catalog)
	}
}
