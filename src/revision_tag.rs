//! # Revision Tag Management for CernVM-FS
//!
//! This module provides functionality for handling repository revision tags in CernVM-FS.
//! Revision tags are metadata entries that identify specific snapshots of the repository
//! content at different points in time. They allow for temporal navigation through
//! repository history.
//!
//! ## Revision Tags
//!
//! In CernVM-FS, revision tags provide the following capabilities:
//! - Named references to specific repository states (e.g., "production", "testing")
//! - Revision number tracking for sequential versioning
//! - Timestamp information for temporal navigation
//! - Channel assignment for content categorization
//! - Description text for human-readable context
//!
//! ## Tag Queries
//!
//! This module provides SQL queries for retrieving tags by different criteria:
//! - By name: Find a specific named tag
//! - By revision: Find a tag with a specific revision number
//! - By date: Find the tag active at a specific point in time
//! - All tags: List all repository tags in chronological order
//!
//! ## Usage
//!
//! Revision tags are typically accessed through the History module:
//!
//! ```no_run
//! use cvmfs::history::History;
//!
//! let history = History::new("/path/to/history.db").unwrap();
//!
//! // Get tag by name
//! if let Ok(Some(tag)) = history.get_tag_by_name("production") {
//!     println!("Production tag is at revision {}", tag.revision);
//! }
//! ```

use rusqlite::Row;

use crate::common::CvmfsResult;

/// SQL query to retrieve all tags ordered by timestamp (newest first)
///
/// This query returns all tags in the history database, ordered by their
/// timestamp in descending order (newest first). It's useful for presenting
/// a chronological history of repository changes.
pub const SQL_QUERY_ALL: &str = "\
SELECT name, hash, revision, timestamp, channel, description \
FROM tags \
ORDER BY timestamp DESC";

/// SQL query to retrieve a tag by its name
///
/// This query looks up a single tag by its name identifier. Tag names are
/// unique within a repository, so this query returns at most one result.
/// It's commonly used to find specific named tags like "production" or "latest".
pub const SQL_QUERY_NAME: &str = "\
SELECT name, hash, revision, timestamp, channel, description \
FROM tags \
WHERE name = ? \
LIMIT 1";

/// SQL query to retrieve a tag by its revision number
///
/// This query looks up a tag by its numeric revision identifier. Each revision
/// represents a published state of the repository, with higher numbers indicating
/// newer revisions. This query returns at most one result.
pub const SQL_QUERY_REVISION: &str = "\
SELECT name, hash, revision, timestamp, channel, description \
FROM tags \
WHERE revision = ? \
LIMIT 1";

/// SQL query to retrieve the earliest tag after a given timestamp
///
/// This query finds the first tag that was created after the specified timestamp.
/// It orders results by timestamp in ascending order and returns only the first match.
/// This is useful for finding which tag was active at a specific point in time.
pub const SQL_QUERY_DATE: &str = "\
SELECT name, hash, revision, timestamp, channel, description \
FROM tags \
WHERE timestamp > ? \
ORDER BY timestamp ASC \
LIMIT 1";

/// Represents a named tag for a specific repository revision
///
/// A RevisionTag provides metadata about a specific snapshot of the repository.
/// It includes information such as the name of the tag, the content hash it
/// points to, its revision number, when it was created, and descriptive text.
/// Tags are used to mark important states of the repository and to provide
/// navigation points in the repository history.
#[derive(Debug, Clone)]
pub struct RevisionTag {
    /// The unique name of this tag (e.g., "production", "testing").
    pub name: String,
    /// The content hash this tag points to, identifying the exact repository state.
    pub hash: String,
    /// The numeric revision identifier for this tag.
    pub revision: i32,
    /// Unix timestamp (seconds since epoch) when this tag was created.
    pub timestamp: i64,
    /// Channel identifier for this tag (used for categorizing tags).
    pub channel: i32,
    /// Human-readable description of this tag's purpose or contents.
    pub description: String,
}

impl RevisionTag {
    /// Creates a new RevisionTag from a database row.
    ///
    /// This constructor extracts tag information from a database row and creates
    /// a new RevisionTag instance. It assumes the row contains columns in the same
    /// order as specified in the SQL queries defined in this module.
    ///
    /// # Arguments
    ///
    /// * `row` - A database row containing tag information.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<Self>` containing the new RevisionTag if successful,
    /// or an error if the row data cannot be parsed.
    ///
    /// # Errors
    ///
    /// This function will return a database error if any column cannot be extracted
    /// from the row due to type mismatch or missing data.
    pub fn new(row: &Row) -> CvmfsResult<Self> {
        Ok(Self {
            name: row.get(0)?,
            hash: row.get(1)?,
            revision: row.get(2)?,
            timestamp: row.get(3)?,
            channel: row.get(4)?,
            description: row.get(5)?,
        })
    }
}
