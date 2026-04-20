//! # SQLite Database Interface for CernVM-FS
//!
//! This module provides a safe interface for interacting with SQLite databases
//! used by CernVM-FS. It handles database connections and query operations
//! specifically tailored for reading repository metadata.
//!
//! The module implements read-only access to SQLite databases with thread-safe
//! operations, primarily used for accessing catalog and metadata information.

use std::path::Path;

use rusqlite::{Connection, OpenFlags, Statement};

use crate::common::{CvmfsError, CvmfsResult};

/// Represents a thread-safe connection to an SQLite database.
///
/// This struct provides a safe interface for read-only operations on CernVM-FS
/// SQLite databases. It manages the database connection and provides methods
/// for executing prepared statements and querying data.
#[derive(Debug)]
pub struct DatabaseObject {
	connection: Connection,
}

/// Implement Sync for DatabaseObject as the connection is used in a read-only manner
/// and protected by internal SQLite locks.
unsafe impl Sync for DatabaseObject {}

impl DatabaseObject {
	/// Creates a new DatabaseObject instance.
	///
	/// # Arguments
	///
	/// * `database_file` - Path to the SQLite database file.
	///
	/// # Returns
	///
	/// Returns a Result containing the new DatabaseObject instance or an error
	/// if the connection cannot be established.
	pub fn new(database_file: &str) -> CvmfsResult<Self> {
		let path = Path::new(database_file);
		let connection = Self::open_database(path)?;
		Ok(Self { connection })
	}

	fn open_database(path: &Path) -> CvmfsResult<Connection> {
		let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_FULL_MUTEX;
		Ok(Connection::open_with_flags(path, flags)?)
	}

	/// Creates a prepared SQL statement.
	///
	/// # Arguments
	///
	/// * `sql` - The SQL query string to prepare.
	///
	/// # Returns
	///
	/// Returns a Result containing the prepared Statement or an error if preparation fails.
	pub fn create_prepared_statement(&self, sql: &str) -> CvmfsResult<Statement<'_>> {
		Ok(self.connection.prepare(sql)?)
	}

	/// Reads all key-value pairs from the properties table.
	///
	/// # Returns
	///
	/// Returns a Result containing a vector of key-value pairs from the properties
	/// table or an error if the query fails.
	pub fn read_properties_table(&self) -> CvmfsResult<Vec<(String, String)>> {
		let mut statement = self.create_prepared_statement("SELECT key, value FROM properties;")?;
		let iterator = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
		iterator.collect::<Result<Vec<_>, _>>().map_err(CvmfsError::from)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;

	fn tmp_db_path(name: &str) -> PathBuf {
		std::env::temp_dir().join(format!("cvmfs_db_{}_{}.sqlite", name, std::process::id()))
	}

	fn create_test_db(path: &Path, entries: &[(&str, &str)]) {
		let conn = Connection::open(path).unwrap();
		conn.execute("CREATE TABLE properties (key TEXT NOT NULL, value TEXT NOT NULL)", [])
			.unwrap();
		for (k, v) in entries {
			conn.execute("INSERT INTO properties (key, value) VALUES (?1, ?2)", [k, v])
				.unwrap();
		}
	}

	#[test]
	fn read_properties_table_returns_entries() {
		let path = tmp_db_path("props");
		create_test_db(&path, &[("schema", "2.5"), ("revision", "42")]);

		let db = DatabaseObject::new(path.to_str().unwrap()).unwrap();
		let props = db.read_properties_table().unwrap();

		assert_eq!(props.len(), 2);
		assert!(props.contains(&("schema".to_string(), "2.5".to_string())));
		assert!(props.contains(&("revision".to_string(), "42".to_string())));

		std::fs::remove_file(&path).ok();
	}

	#[test]
	fn read_properties_table_empty() {
		let path = tmp_db_path("empty");
		create_test_db(&path, &[]);

		let db = DatabaseObject::new(path.to_str().unwrap()).unwrap();
		let props = db.read_properties_table().unwrap();
		assert!(props.is_empty());

		std::fs::remove_file(&path).ok();
	}

	#[test]
	fn create_prepared_statement_valid_sql() {
		let path = tmp_db_path("stmt");
		create_test_db(&path, &[("key1", "val1")]);

		let db = DatabaseObject::new(path.to_str().unwrap()).unwrap();
		let result = db.create_prepared_statement("SELECT key, value FROM properties");
		assert!(result.is_ok());

		std::fs::remove_file(&path).ok();
	}

	#[test]
	fn create_prepared_statement_invalid_sql() {
		let path = tmp_db_path("badsql");
		create_test_db(&path, &[]);

		let db = DatabaseObject::new(path.to_str().unwrap()).unwrap();
		let result = db.create_prepared_statement("SELECT * FROM nonexistent_table");
		assert!(result.is_err());

		std::fs::remove_file(&path).ok();
	}

	#[test]
	fn new_invalid_path_returns_error() {
		let result = DatabaseObject::new("/nonexistent/path/to/db.sqlite");
		assert!(result.is_err());
	}
}
