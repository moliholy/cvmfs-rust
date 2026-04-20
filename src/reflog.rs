use crate::{
	common::{CvmfsError, CvmfsResult},
	database_object::DatabaseObject,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefType {
	Catalog = 0,
	Certificate = 1,
	History = 2,
	MetaInfo = 3,
}

impl TryFrom<i32> for RefType {
	type Error = CvmfsError;

	fn try_from(value: i32) -> Result<Self, Self::Error> {
		match value {
			0 => Ok(RefType::Catalog),
			1 => Ok(RefType::Certificate),
			2 => Ok(RefType::History),
			3 => Ok(RefType::MetaInfo),
			_ => Err(CvmfsError::ParseError),
		}
	}
}

#[derive(Debug, Clone)]
pub struct RefEntry {
	pub hash: String,
	pub ref_type: RefType,
	pub timestamp: i64,
}

#[derive(Debug)]
pub struct Reflog {
	pub database_object: DatabaseObject,
}

unsafe impl Sync for Reflog {}

const LIST_REFS_QUERY: &str = "SELECT hash, type, timestamp FROM refs ORDER BY timestamp DESC";
const LIST_REFS_BY_TYPE_QUERY: &str =
	"SELECT hash, type, timestamp FROM refs WHERE type = ?1 ORDER BY timestamp DESC";
const COUNT_REFS_QUERY: &str = "SELECT COUNT(*) FROM refs";
const CONTAINS_HASH_QUERY: &str = "SELECT COUNT(*) FROM refs WHERE hash = ?1";

impl Reflog {
	pub fn new(database_file: &str) -> CvmfsResult<Self> {
		let database_object = DatabaseObject::new(database_file)?;
		Ok(Self { database_object })
	}

	pub fn list_refs(&self) -> CvmfsResult<Vec<RefEntry>> {
		let mut statement = self.database_object.create_prepared_statement(LIST_REFS_QUERY)?;
		let rows = statement.query_map([], |row| {
			Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?, row.get::<_, i64>(2)?))
		})?;
		let mut entries = Vec::new();
		for row in rows {
			let (hash, ref_type, timestamp) = row.map_err(CvmfsError::from)?;
			entries.push(RefEntry { hash, ref_type: RefType::try_from(ref_type)?, timestamp });
		}
		Ok(entries)
	}

	pub fn list_refs_by_type(&self, ref_type: RefType) -> CvmfsResult<Vec<RefEntry>> {
		let mut statement =
			self.database_object.create_prepared_statement(LIST_REFS_BY_TYPE_QUERY)?;
		let rows = statement.query_map([ref_type as i32], |row| {
			Ok((row.get::<_, String>(0)?, row.get::<_, i32>(1)?, row.get::<_, i64>(2)?))
		})?;
		let mut entries = Vec::new();
		for row in rows {
			let (hash, rt, timestamp) = row.map_err(CvmfsError::from)?;
			entries.push(RefEntry { hash, ref_type: RefType::try_from(rt)?, timestamp });
		}
		Ok(entries)
	}

	pub fn count_refs(&self) -> CvmfsResult<u64> {
		let mut statement = self.database_object.create_prepared_statement(COUNT_REFS_QUERY)?;
		let count: i64 = statement.query_row([], |row| row.get(0))?;
		Ok(count as u64)
	}

	pub fn contains_hash(&self, hash: &str) -> CvmfsResult<bool> {
		let mut statement = self.database_object.create_prepared_statement(CONTAINS_HASH_QUERY)?;
		let count: i64 = statement.query_row([hash], |row| row.get(0))?;
		Ok(count > 0)
	}
}

#[cfg(test)]
mod tests {
	use rusqlite::Connection;
	use std::path::{Path, PathBuf};

	use super::*;

	fn tmp_db_path(name: &str) -> PathBuf {
		std::env::temp_dir().join(format!("cvmfs_reflog_{}_{}.sqlite", name, std::process::id()))
	}

	fn create_reflog_db(path: &Path, entries: &[(&str, i32, i64)]) {
		let conn = Connection::open(path).unwrap();
		conn.execute(
			"CREATE TABLE refs (hash TEXT NOT NULL, type INTEGER NOT NULL, timestamp INTEGER NOT NULL)",
			[],
		)
		.unwrap();
		for (hash, ref_type, ts) in entries {
			conn.execute(
				"INSERT INTO refs (hash, type, timestamp) VALUES (?1, ?2, ?3)",
				rusqlite::params![hash, ref_type, ts],
			)
			.unwrap();
		}
	}

	#[test]
	fn list_refs_returns_entries() {
		let path = tmp_db_path("list");
		create_reflog_db(&path, &[("abc123", 0, 1000), ("def456", 1, 2000), ("ghi789", 2, 3000)]);
		let reflog = Reflog::new(path.to_str().unwrap()).unwrap();
		let refs = reflog.list_refs().unwrap();
		assert_eq!(refs.len(), 3);
		assert_eq!(refs[0].hash, "ghi789");
		assert_eq!(refs[0].ref_type, RefType::History);
		std::fs::remove_file(&path).ok();
	}

	#[test]
	fn list_refs_empty() {
		let path = tmp_db_path("empty");
		create_reflog_db(&path, &[]);
		let reflog = Reflog::new(path.to_str().unwrap()).unwrap();
		let refs = reflog.list_refs().unwrap();
		assert!(refs.is_empty());
		std::fs::remove_file(&path).ok();
	}

	#[test]
	fn list_refs_by_type_filters() {
		let path = tmp_db_path("bytype");
		create_reflog_db(&path, &[("abc", 0, 100), ("def", 1, 200), ("ghi", 0, 300)]);
		let reflog = Reflog::new(path.to_str().unwrap()).unwrap();
		let catalogs = reflog.list_refs_by_type(RefType::Catalog).unwrap();
		assert_eq!(catalogs.len(), 2);
		assert!(catalogs.iter().all(|r| r.ref_type == RefType::Catalog));
		let certs = reflog.list_refs_by_type(RefType::Certificate).unwrap();
		assert_eq!(certs.len(), 1);
		std::fs::remove_file(&path).ok();
	}

	#[test]
	fn count_refs() {
		let path = tmp_db_path("count");
		create_reflog_db(&path, &[("a", 0, 1), ("b", 1, 2), ("c", 2, 3)]);
		let reflog = Reflog::new(path.to_str().unwrap()).unwrap();
		assert_eq!(reflog.count_refs().unwrap(), 3);
		std::fs::remove_file(&path).ok();
	}

	#[test]
	fn contains_hash_found() {
		let path = tmp_db_path("contains_yes");
		create_reflog_db(&path, &[("abc123", 0, 100)]);
		let reflog = Reflog::new(path.to_str().unwrap()).unwrap();
		assert!(reflog.contains_hash("abc123").unwrap());
		std::fs::remove_file(&path).ok();
	}

	#[test]
	fn contains_hash_not_found() {
		let path = tmp_db_path("contains_no");
		create_reflog_db(&path, &[("abc123", 0, 100)]);
		let reflog = Reflog::new(path.to_str().unwrap()).unwrap();
		assert!(!reflog.contains_hash("xyz999").unwrap());
		std::fs::remove_file(&path).ok();
	}

	#[test]
	fn ref_type_try_from_valid() {
		assert_eq!(RefType::try_from(0).unwrap(), RefType::Catalog);
		assert_eq!(RefType::try_from(1).unwrap(), RefType::Certificate);
		assert_eq!(RefType::try_from(2).unwrap(), RefType::History);
		assert_eq!(RefType::try_from(3).unwrap(), RefType::MetaInfo);
	}

	#[test]
	fn ref_type_try_from_invalid() {
		assert!(RefType::try_from(4).is_err());
		assert!(RefType::try_from(-1).is_err());
	}

	#[test]
	fn invalid_db_path_errors() {
		let result = Reflog::new("/nonexistent/path.db");
		assert!(result.is_err());
	}
}
