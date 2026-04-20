use crate::{
	common::{CvmfsError, CvmfsResult},
	root_file::RootFile,
};
use chrono::{DateTime, Utc};

/// Wraps information from .cvmfspublished
#[derive(Debug)]
pub struct Manifest {
	pub root_file: RootFile,
	pub root_catalog: String,
	pub root_hash: String,
	pub root_catalog_size: u32,
	pub certificate: String,
	pub history_database: Option<String>,
	pub last_modified: DateTime<Utc>,
	pub ttl: u32,
	pub revision: u32,
	pub repository_name: String,
	pub micro_catalog: String,
	pub garbage_collectable: bool,
	pub allows_alternative_name: bool,
}

impl Manifest {
	pub fn has_history(&self) -> bool {
		self.history_database.is_some()
	}
}

impl Manifest {
	fn parse_boolean(value: &str) -> CvmfsResult<bool> {
		match value {
			"yes" => Ok(true),
			"no" => Ok(false),
			_ => Err(CvmfsError::ParseError),
		}
	}

	pub fn new(root_file: RootFile) -> CvmfsResult<Self> {
		let mut root_catalog = String::new();
		let mut root_hash = String::new();
		let mut root_catalog_size = 0;
		let mut certificate = String::new();
		let mut history_database = None;
		let mut last_modified = DateTime::default();
		let mut ttl = 0;
		let mut revision = 0;
		let mut repository_name = String::new();
		let mut micro_catalog = String::new();
		let mut garbage_collectable = false;
		let mut allows_alternative_name = false;

		for line in root_file.lines() {
			if let Some(key) = line.chars().next() {
				let value = &line[1..];
				match key {
					'C' => root_catalog = value.into(),
					'R' => root_hash = value.into(),
					'B' => root_catalog_size = value.parse().map_err(|_| CvmfsError::ParseError)?,
					'X' => certificate = value.into(),
					'H' => history_database = Some(value.into()),
					'T' => {
						last_modified = DateTime::from_timestamp(
							value.parse().map_err(|_| CvmfsError::InvalidTimestamp)?,
							0,
						)
						.ok_or(CvmfsError::InvalidTimestamp)?
					}
					'D' => ttl = value.parse().map_err(|_| CvmfsError::ParseError)?,
					'S' => revision = value.parse().map_err(|_| CvmfsError::ParseError)?,
					'N' => repository_name = value.into(),
					'L' | 'Y' => micro_catalog = value.into(),
					'G' => garbage_collectable = Self::parse_boolean(value)?,
					'A' => allows_alternative_name = Self::parse_boolean(value)?,
					_ => {}
				}
			}
		}

		Ok(Self {
			root_file,
			root_catalog,
			root_hash,
			root_catalog_size,
			certificate,
			history_database,
			last_modified,
			ttl,
			revision,
			repository_name,
			micro_catalog,
			garbage_collectable,
			allows_alternative_name,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use hex::ToHex;
	use sha1::{Digest, Sha1};
	use std::{
		fs::File,
		io::Write,
		path::PathBuf,
		sync::atomic::{AtomicU64, Ordering},
	};

	static COUNTER: AtomicU64 = AtomicU64::new(0);

	fn tmp_path(prefix: &str) -> PathBuf {
		let id = COUNTER.fetch_add(1, Ordering::Relaxed);
		std::env::temp_dir().join(format!("cvmfs_mf_{}_{}_{}", prefix, std::process::id(), id))
	}

	fn make_root_file(content: &str) -> RootFile {
		let path = tmp_path("manifest");
		{
			let mut f = File::create(&path).unwrap();
			f.write_all(content.as_bytes()).unwrap();
			f.flush().unwrap();
		}
		let file = File::open(&path).unwrap();
		let rf = RootFile::new(&file).unwrap();
		std::fs::remove_file(&path).ok();
		rf
	}

	fn make_signed_root_file(content: &str) -> RootFile {
		let mut hasher = Sha1::new();
		hasher.update(content.as_bytes());
		let hash: String = hasher.finalize().encode_hex();

		let mut data = content.to_string();
		data.push_str("--\n");
		data.push_str(&hash);
		data.push('\n');

		let path = tmp_path("manifest_signed");
		{
			let mut f = File::create(&path).unwrap();
			f.write_all(data.as_bytes()).unwrap();
			f.flush().unwrap();
		}
		let file = File::open(&path).unwrap();
		let rf = RootFile::new(&file).unwrap();
		std::fs::remove_file(&path).ok();
		rf
	}

	#[test]
	fn parse_manifest_all_fields() {
		let content = "Cmy_root_catalog\n\
		               Rmy_root_hash\n\
		               B1024\n\
		               Xmy_certificate\n\
		               Hmy_history_db\n\
		               T1000\n\
		               D3600\n\
		               S42\n\
		               Ntest.repo\n\
		               Lmy_micro_catalog\n\
		               Gyes\n\
		               Ayes\n";
		let rf = make_root_file(content);
		let manifest = Manifest::new(rf).unwrap();

		assert_eq!(manifest.root_catalog, "my_root_catalog");
		assert_eq!(manifest.root_hash, "my_root_hash");
		assert_eq!(manifest.root_catalog_size, 1024);
		assert_eq!(manifest.certificate, "my_certificate");
		assert_eq!(manifest.history_database, Some("my_history_db".into()));
		assert_eq!(manifest.ttl, 3600);
		assert_eq!(manifest.revision, 42);
		assert_eq!(manifest.repository_name, "test.repo");
		assert_eq!(manifest.micro_catalog, "my_micro_catalog");
		assert_eq!(manifest.last_modified.timestamp(), 1000);
		assert!(manifest.garbage_collectable);
		assert!(manifest.allows_alternative_name);
	}

	#[test]
	fn parse_manifest_timestamp_is_seconds() {
		let content = "Ccat\nRhash\nB0\nXcert\nT1713952007\nD0\nS1\nNrepo\nGno\nAno\n";
		let rf = make_root_file(content);
		let manifest = Manifest::new(rf).unwrap();
		assert_eq!(manifest.last_modified.timestamp(), 1713952007);
		assert_eq!(manifest.last_modified.format("%Y").to_string(), "2024");
	}

	#[test]
	fn parse_manifest_y_field_micro_catalog() {
		let content = "Ccat\nRhash\nB0\nXcert\nT0\nD0\nS1\nNrepo\nYmicro_hash\nGno\nAno\n";
		let rf = make_root_file(content);
		let manifest = Manifest::new(rf).unwrap();
		assert_eq!(manifest.micro_catalog, "micro_hash");
	}

	#[test]
	fn parse_manifest_from_signed_root_file() {
		let content =
			"Csigned_catalog\nRsigned_hash\nB512\nXcert\nT2000\nD900\nS10\nNrepo\nLmc\nGno\nAno\n";
		let rf = make_signed_root_file(content);
		let manifest = Manifest::new(rf).unwrap();

		assert_eq!(manifest.root_catalog, "signed_catalog");
		assert_eq!(manifest.revision, 10);
		assert!(!manifest.garbage_collectable);
		assert!(!manifest.allows_alternative_name);
	}

	#[test]
	fn parse_boolean_yes() {
		assert!(Manifest::parse_boolean("yes").unwrap());
	}

	#[test]
	fn parse_boolean_no() {
		assert!(!Manifest::parse_boolean("no").unwrap());
	}

	#[test]
	fn parse_boolean_invalid() {
		let result = Manifest::parse_boolean("maybe");
		assert!(result.is_err());
		assert_eq!(result.unwrap_err(), CvmfsError::ParseError);
	}

	#[test]
	fn has_history_none_returns_false() {
		let content = "Ccat\nRhash\nB0\nXcert\nT0\nD0\nS1\nNrepo\nL\nGno\nAno\n";
		let rf = make_root_file(content);
		let manifest = Manifest::new(rf).unwrap();
		assert!(!manifest.has_history());
	}

	#[test]
	fn has_history_some_returns_true() {
		let content = "Ccat\nRhash\nB0\nXcert\nHhistdb\nT0\nD0\nS1\nNrepo\nL\nGno\nAno\n";
		let rf = make_root_file(content);
		let manifest = Manifest::new(rf).unwrap();
		assert!(manifest.has_history());
	}

	#[test]
	fn unknown_keys_ignored() {
		let content =
			"Ccat\nRhash\nB0\nXcert\nT0\nD0\nS1\nNrepo\nL\nGno\nAno\nZunknown_value\nQanother\n";
		let rf = make_root_file(content);
		let result = Manifest::new(rf);
		assert!(result.is_ok());
	}
}
