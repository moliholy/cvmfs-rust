//! # Repository History Management
//!
//! This module provides functionality to access and query the repository history database
//! in CernVM-FS. The history database tracks revisions and tags over time, allowing
//! for temporal navigation through repository states.
//!
//! ## History Database
//!
//! The history database is an SQLite file that contains information about all tagged
//! revisions of a repository. It enables users to refer to repository states by:
//! - Named tags (e.g., "production", "testing")
//! - Revision numbers (e.g., r42)
//! - Timestamps/dates
//!
//! ## Usage
//!
//! ```no_run
//! use cvmfs::history::History;
//!
//! // Open a history database
//! let history = History::new("/path/to/history.db").unwrap();
//!
//! // Look up a tag by name
//! let tag = history.get_tag_by_name("production").unwrap();
//!
//! // Look up revision by number
//! let rev = history.get_tag_by_revision(42).unwrap();
//! ```

use crate::{
	common::{CvmfsError, CvmfsResult},
	database_object::DatabaseObject,
	revision_tag::{RevisionTag, SQL_QUERY_DATE, SQL_QUERY_NAME, SQL_QUERY_REVISION},
};

#[derive(Debug)]
pub struct History {
	/// The underlying SQLite database object storing history records.
	///
	/// This field manages connections to the SQLite database and provides
	/// methods for executing queries and reading data.
	pub database_object: DatabaseObject,

	/// The schema version of the history database.
	///
	/// Currently, the supported schema version is "1.0". If this value
	/// doesn't match the expected version, initialization will fail.
	pub schema: String,

	/// The fully qualified repository name (FQRN) this history belongs to.
	///
	/// This field identifies the repository, typically in the form of
	/// "repo.domain.tld" that matches the repository's canonical URL.
	pub fqrn: String,
}

/// Mark History as thread-safe for use with synchronization primitives.
///
/// This implementation indicates that it's safe to share references to History
/// between threads. This enables using History with locks, Arc, and other
/// concurrency primitives.
unsafe impl Sync for History {}

impl History {
	/// Creates a new History instance from a database file path.
	///
	/// This alternative constructor opens the database file at the specified path
	/// and initializes a new History instance for accessing the repository history.
	///
	/// # Arguments
	///
	/// * `database_file` - Path to the history database file.
	///
	/// # Returns
	///
	/// Returns a `CvmfsResult<Self>` containing the initialized History object,
	/// or an error if the database cannot be opened or initialized.
	/// Creates a new History instance by opening and validating a history database file.
	///
	/// This constructor initializes a connection to the specified SQLite history database
	/// and verifies its schema version. It reads essential metadata from the 'properties'
	/// table and ensures the schema is compatible with this client implementation.
	///
	/// # Arguments
	///
	/// * `database_file` - Path to the SQLite history database file
	///
	/// # Returns
	///
	/// * `Ok(History)` - A successfully initialized History instance
	/// * `Err(...)` - If the database cannot be opened or has invalid properties
	///
	/// # Panics
	///
	/// This method will panic if the schema version is not "1.0", as this indicates
	/// an incompatible history database format.
	///
	/// # Example
	///
	/// ```no_run
	/// use cvmfs::history::History;
	///
	/// let history = match History::new("/path/to/history.db") {
	///     Ok(h) => h,
	///     Err(e) => panic!("Failed to open history database: {:?}", e)
	/// };
	///
	/// println!("Successfully opened history for repository: {}", history.fqrn);
	/// ```
	pub fn new(database_file: &str) -> CvmfsResult<Self> {
		let database_object = DatabaseObject::new(database_file)?;
		let properties = database_object.read_properties_table()?;
		let mut schema = String::new();
		let mut fqrn = String::new();
		for (key, value) in properties {
			match key.as_str() {
				"schema" => schema.push_str(&value),
				"fqrn" => fqrn.push_str(&value),
				_ => {},
			}
		}
		if schema.ne("1.0") {
			return Err(CvmfsError::ParseError);
		}
		Ok(Self { database_object, schema, fqrn })
	}

	/// Internal helper to fetch a revision tag using a parameterized SQL query.
	///
	/// This private method is used by the public getter methods to query the database
	/// with specific search criteria. It prepares an SQL statement, executes it with
	/// the provided parameter, and constructs a RevisionTag from the results if found.
	///
	/// # Arguments
	///
	/// * `query` - The SQL query string to execute (must match expected table schema)
	/// * `param` - The parameter value to substitute into the query
	///
	/// # Returns
	///
	/// * `Ok(Some(RevisionTag))` - If a matching tag was found
	/// * `Ok(None)` - If no matching tag exists
	/// * `Err(...)` - If a database error occurred
	fn get_tag_by_query(&self, query: &str, param: &str) -> CvmfsResult<Option<RevisionTag>> {
		let mut statement = self.database_object.create_prepared_statement(query)?;
		let mut rows = statement.query([param])?;
		match rows.next()? {
			None => Ok(None),
			Some(row) => Ok(Some(RevisionTag::new(row)?)),
		}
	}

	/// Retrieves a repository revision tag by its name.
	///
	/// This method searches the history database for a tag with the specified name.
	/// Tag names are typically descriptive identifiers like "production", "testing",
	/// or "latest" that point to specific repository revisions.
	///
	/// # Arguments
	///
	/// * `name` - The name of the tag to look up
	///
	/// # Returns
	///
	/// * `Ok(Some(RevisionTag))` - If a tag with the specified name was found
	/// * `Ok(None)` - If no tag with the given name exists
	/// * `Err(...)` - If a database error occurred
	///
	/// # Example
	///
	/// ```no_run
	/// # use cvmfs::history::History;
	/// # let history = History::new("/path/to/history.db").unwrap();
	/// if let Ok(Some(tag)) = history.get_tag_by_name("production") {
	///     println!("Production tag points to revision {}", tag.revision);
	/// }
	/// ```
	pub fn get_tag_by_name(&self, name: &str) -> CvmfsResult<Option<RevisionTag>> {
		self.get_tag_by_query(SQL_QUERY_NAME, name)
	}

	/// Retrieves a repository revision tag by its revision number.
	///
	/// Each published revision of a CernVM-FS repository has a unique monotonically
	/// increasing revision number. This method allows looking up a tag associated
	/// with a specific revision number.
	///
	/// # Arguments
	///
	/// * `revision` - The numeric revision identifier to look up
	///
	/// # Returns
	///
	/// * `Ok(Some(RevisionTag))` - If a tag with the specified revision was found
	/// * `Ok(None)` - If no tag with the given revision exists
	/// * `Err(...)` - If a database error occurred
	///
	/// # Example
	///
	/// ```no_run
	/// # use cvmfs::history::History;
	/// # let history = History::new("/path/to/history.db").unwrap();
	/// if let Ok(Some(tag)) = history.get_tag_by_revision(42) {
	///     println!("Revision 42 is tagged as '{}'", tag.name);
	/// }
	/// ```
	pub fn get_tag_by_revision(&self, revision: u32) -> CvmfsResult<Option<RevisionTag>> {
		self.get_tag_by_query(SQL_QUERY_REVISION, revision.to_string().as_str())
	}

	/// Retrieves a repository revision tag by a specific date/timestamp.
	///
	/// This method finds the repository tag that was active at the specified point in time.
	/// It's useful for examining the repository state as it existed on a particular date.
	///
	/// # Arguments
	///
	/// * `timestamp` - Unix timestamp (seconds since epoch) representing the point in time
	///
	/// # Returns
	///
	/// * `Ok(Some(RevisionTag))` - If a tag matching the timestamp was found
	/// * `Ok(None)` - If no tag was active at the specified time
	/// * `Err(...)` - If a database error occurred
	///
	/// # Example
	///
	/// ```no_run
	/// # use cvmfs::history::History;
	/// # let history = History::new("/path/to/history.db").unwrap();
	/// // Look up repository state from January 1, 2023
	/// let timestamp = 1672531200; // 2023-01-01 00:00:00 UTC
	/// if let Ok(Some(tag)) = history.get_tag_by_date(timestamp) {
	///     println!("On Jan 1, 2023, repository was at revision {}", tag.revision);
	/// }
	/// ```
	pub fn get_tag_by_date(&self, timestamp: i64) -> CvmfsResult<Option<RevisionTag>> {
		self.get_tag_by_query(SQL_QUERY_DATE, timestamp.to_string().as_str())
	}
}
