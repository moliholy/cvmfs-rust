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
        iterator
            .collect::<Result<Vec<_>, _>>()
            .map_err(CvmfsError::from)
    }
}
