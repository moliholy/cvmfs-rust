//! # Catalog Management for CernVM-FS
//!
//! This module provides functionality for working with CernVM-FS catalog databases.
//! Catalogs are SQLite databases that store metadata about files and directories in
//! a repository, allowing efficient lookups, listings, and content addressing.
//!
//! ## Catalog Structure
//!
//! A CernVM-FS repository is organized as a hierarchy of catalogs:
//! - The root catalog contains entries for top-level directories and files
//! - Nested catalogs contain entries for subdirectories, enabling scalability
//! - Each catalog has a unique hash that identifies its content
//!
//! ## Catalog Operations
//!
//! This module supports operations such as:
//! - Looking up file and directory entries by path.
//! - Listing directory contents.
//! - Retrieving file chunks for content-addressed storage.
//! - Navigating nested catalogs for hierarchical browsing.
//! - Gathering repository statistics.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rusqlite::Row;

use crate::common::{CvmfsError, CvmfsResult, canonicalize_path, split_md5};
use crate::database_object::DatabaseObject;
use crate::directory_entry::{DirectoryEntry, PathHash};

/// Prefix used to identify catalog objects in the repository storage.
///
/// This constant defines the standard prefix used for catalog objects
/// in the content-addressed storage system.
pub const CATALOG_ROOT_PREFIX: &str = "C";

/// SQL query to list directory entries by parent directory hash.
///
/// This query retrieves all entries in a directory, identified by the MD5 hash
/// of the parent directory path (split into two 64-bit components).
const LISTING_QUERY: &str = "\
SELECT md5path_1, md5path_2, parent_1, parent_2, hash, flags, size, mode, mtime, name, symlink \
FROM catalog \
WHERE parent_1 = ? AND parent_2 = ? \
ORDER BY name ASC";

/// SQL query to count nested catalogs.
///
/// This query counts the number of nested catalogs referenced in the current catalog.
const NESTED_COUNT: &str = "SELECT count(*) FROM nested_catalogs;";

/// SQL query to retrieve file chunks by path hash.
///
/// This query gets all chunks for a file, identified by the MD5 hash of its path
/// (split into two 64-bit components), ordered by offset to reconstruct the file.
const READ_CHUNK: &str = "\
SELECT md5path_1, md5path_2, offset, size, hash \
FROM chunks \
WHERE md5path_1 = ? AND md5path_2 = ? \
ORDER BY offset ASC";

/// SQL query to find a directory entry by its MD5 path hash.
///
/// This query retrieves a single entry that matches the specified MD5 path hash
/// (split into two 64-bit components).
const FIND_MD5_PATH: &str = "SELECT md5path_1, md5path_2, parent_1, parent_2, hash, flags, size, mode, mtime, name, symlink \
FROM catalog \
WHERE md5path_1 = ? AND md5path_2 = ? \
LIMIT 1;";

/// SQL query to read repository statistics.
///
/// This query retrieves all statistics counters from the catalog, ordered by counter name.
const READ_STATISTICS: &str = "SELECT * FROM statistics ORDER BY counter;";

/// Reference to a nested catalog in the repository hierarchy.
///
/// This struct represents a reference to a nested catalog, including its
/// mount point path, content hash, and size. Nested catalogs are used to
/// organize the repository into manageable sections for efficient browsing
/// and synchronization.
#[derive(Debug)]
pub struct CatalogReference {
    /// The repository path where this catalog is mounted.
    pub root_path: String,
    /// The content hash that uniquely identifies this catalog.
    pub catalog_hash: String,
    /// The size of the catalog in bytes.
    pub catalog_size: u32,
}

/// CernVM-FS catalog database wrapper.
///
/// The `Catalog` struct provides an interface to a CernVM-FS catalog database,
/// which stores metadata about files and directories in a repository. It handles
/// database operations, metadata queries, and navigation between nested catalogs.
///
/// Catalogs are versioned with schema and revision numbers, and they form a
/// hierarchical structure in the repository with root and nested catalogs.
#[derive(Debug)]
pub struct Catalog {
    /// The underlying SQLite database object.
    pub database: DatabaseObject,
    /// The schema version of the catalog (e.g., 1.2).
    pub schema: f32,
    /// The schema revision of the catalog.
    pub schema_revision: f32,
    /// The revision number of this catalog.
    pub revision: i32,
    /// Hash of the previous revision of this catalog.
    pub previous_revision: String,
    /// Content hash that uniquely identifies this catalog.
    pub hash: String,
    /// Timestamp when this catalog was last modified.
    pub last_modified: DateTime<Utc>,
    /// Repository path prefix for entries in this catalog.
    pub root_prefix: String,
}

/// Repository statistics counters.
///
/// This struct contains various statistics about the repository content,
/// such as file counts, directory counts, and size information. These
/// statistics are used for monitoring, reporting, and resource planning.
#[derive(Debug, Default)]
pub struct Statistics {
    /// Number of chunked files (files split into multiple pieces).
    pub chunked: i64,
    /// Total size of all chunked files in bytes.
    pub chunked_size: i64,
    /// Total number of file chunks across all chunked files.
    pub chunks: i64,
    /// Number of directories in the repository.
    pub dir: i64,
    /// Number of external files (referenced but not stored in the repository).
    pub external: i64,
    /// Total size of all external files in bytes.
    pub external_file_size: i64,
    /// Total size of all files in the repository in bytes.
    pub file_size: i64,
    /// Number of nested catalogs in the repository.
    pub nested: i64,
    /// Number of regular (non-chunked) files in the repository.
    pub regular: i64,
    /// Number of special files (devices, sockets, etc.).
    pub special: i64,
    /// Number of symbolic links in the repository.
    pub symlink: i64,
    /// Number of files with extended attributes.
    pub xattr: i64,
}

/// Mark Catalog as thread-safe for use with synchronization primitives.
unsafe impl Sync for Catalog {}

impl Catalog {
    /// Creates a new Catalog instance from a database file.
    ///
    /// This constructor opens a catalog database file and reads its properties
    /// to initialize the Catalog instance. It parses metadata like schema version,
    /// revision number, and timestamps from the properties table.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the catalog database file.
    /// * `hash` - Content hash that uniquely identifies this catalog.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<Self>` containing the initialized Catalog, or an error
    /// if the catalog cannot be opened or has invalid properties.
    ///
    /// # Errors
    ///
    /// Returns `CvmfsError::CatalogInitialization` if required properties are missing,
    /// or other errors if the database cannot be opened or properties are invalid.
    pub fn new(path: String, hash: String) -> CvmfsResult<Self> {
        let database = DatabaseObject::new(&path)?;
        let properties = database.read_properties_table()?;
        let mut revision = 0;
        let mut previous_revision = String::new();
        let mut schema = 0.0;
        let mut schema_revision = 0.0;
        let mut root_prefix = String::from("/");
        let mut last_modified = Default::default();
        for (key, value) in properties {
            match key.as_str() {
                "revision" => revision = value.parse().map_err(|_| CvmfsError::ParseError)?,
                "schema" => schema = value.parse().map_err(|_| CvmfsError::ParseError)?,
                "schema_revision" => {
                    schema_revision = value.parse().map_err(|_| CvmfsError::ParseError)?
                }
                "last_modified" => {
                    last_modified = DateTime::from_timestamp(
                        value.parse().map_err(|_| CvmfsError::ParseError)?,
                        0,
                    )
                    .ok_or(CvmfsError::InvalidTimestamp)?
                }
                "previous_revision" => previous_revision.push_str(&value),
                "root_prefix" => {
                    root_prefix.clear();
                    root_prefix.push_str(&value)
                }
                _ => {}
            }
        }
        if revision == 0 || schema == 0.0 {
            return Err(CvmfsError::CatalogInitialization);
        }
        Ok(Self {
            database,
            schema,
            schema_revision,
            revision,
            hash,
            last_modified,
            root_prefix,
            previous_revision,
        })
    }

    /// Checks if this catalog is the root catalog of the repository.
    ///
    /// The root catalog is identified by having a root prefix of "/". All other
    /// catalogs have a more specific root prefix that indicates their mount point
    /// in the repository hierarchy.
    ///
    /// # Returns
    ///
    /// Returns `true` if this is the root catalog, `false` otherwise.
    pub fn is_root(&self) -> bool {
        self.root_prefix.eq("/")
    }

    /// Checks if this catalog contains any nested catalogs.
    ///
    /// This method determines whether this catalog contains references to any
    /// nested catalogs that would need to be loaded to access certain paths.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<bool>` that is `true` if nested catalogs exist,
    /// `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns database errors if the nested catalog count cannot be determined.
    pub fn has_nested(&self) -> CvmfsResult<bool> {
        Ok(self.nested_count()? > 0)
    }

    /// Returns the number of nested catalogs in the catalog.
    ///
    /// This method counts how many nested catalogs are referenced in this catalog.
    /// Nested catalogs are used to organize the repository hierarchy and improve
    /// performance by segmenting the metadata.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<u32>` containing the count of nested catalogs.
    ///
    /// # Errors
    ///
    /// Returns database errors if the query fails or returns unexpected results.
    pub fn nested_count(&self) -> CvmfsResult<u32> {
        let mut result = self.database.create_prepared_statement(NESTED_COUNT)?;
        let mut row = result.query([])?;
        let next_row = row
            .next()
            .map_err(|e| CvmfsError::DatabaseError(format!("{:?}", e)))?
            .ok_or(CvmfsError::DatabaseError("No rows found".to_string()))?;
        Ok(next_row.get(0)?)
    }

    /// Lists all nested catalogs referenced in this catalog.
    ///
    /// This method retrieves information about all nested catalogs that are mounted
    /// at paths within this catalog. It handles different schema versions, adapting
    /// the query based on the catalog schema version.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<Vec<CatalogReference>>` containing references to all
    /// nested catalogs.
    ///
    /// # Errors
    ///
    /// Returns database errors if the query fails or returns malformed data.
    pub fn list_nested(&self) -> CvmfsResult<Vec<CatalogReference>> {
        let new_version = self.schema <= 1.2 && self.schema_revision > 0.0;
        let sql = if new_version {
            "SELECT path, sha1, size FROM nested_catalogs"
        } else {
            "SELECT path, sha1 FROM nested_catalogs"
        };
        let mut result = self.database.create_prepared_statement(sql)?;
        let iterator = result.query_map([], |row| {
            Ok(CatalogReference {
                root_path: row.get(0)?,
                catalog_hash: row.get(1)?,
                catalog_size: if new_version { row.get(2)? } else { 0 },
            })
        })?;
        Ok(iterator.collect::<Result<Vec<_>, _>>()?)
    }

    /// Checks if a path is properly sanitized relative to a catalog path.
    ///
    /// This helper method verifies that a path is either exactly equal to a catalog
    /// path or is a proper subdirectory (i.e., has a '/' character at the appropriate
    /// position after the catalog path).
    ///
    /// # Arguments
    ///
    /// * `needle_path` - The path being checked
    /// * `catalog_path` - The catalog mount point path
    ///
    /// # Returns
    ///
    /// Returns `true` if the path is properly sanitized, `false` otherwise.
    fn path_sanitized(needle_path: &str, catalog_path: &str) -> bool {
        needle_path.len() == catalog_path.len()
            || (needle_path.len() > catalog_path.len()
                && needle_path.as_bytes()[catalog_path.len()] == b'/')
    }

    /// Finds the best matching nested catalog for a given path.
    ///
    /// This method searches through all nested catalogs to find the one with the
    /// longest matching prefix for the given path. This identifies which nested
    /// catalog should be used to look up the specified path.
    ///
    /// # Arguments
    ///
    /// * `needle_path` - The path to find a matching nested catalog for.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<Option<CatalogReference>>` containing the best matching
    /// nested catalog reference, or None if no matching catalog is found.
    ///
    /// # Errors
    ///
    /// Returns database errors if the nested catalog list cannot be retrieved.
    pub fn find_nested_for_path(&self, needle_path: &str) -> CvmfsResult<Option<CatalogReference>> {
        let catalog_refs = self.list_nested()?;
        let mut best_match = None;
        let mut best_match_score = 0;
        let real_needle_path = canonicalize_path(needle_path);
        for nested_catalog in catalog_refs {
            if real_needle_path.starts_with(&nested_catalog.root_path)
                && nested_catalog.root_path.len() > best_match_score
                && Self::path_sanitized(needle_path, &nested_catalog.root_path)
            {
                best_match_score = nested_catalog.root_path.len();
                best_match = Some(nested_catalog);
            }
        }
        Ok(best_match)
    }

    /// Lists directory entries by parent directory MD5 path hash.
    ///
    /// This method retrieves all entries that have the specified parent directory,
    /// identified by its MD5 path hash split into two 64-bit components. It executes
    /// a SQL query to find matching entries and converts them to DirectoryEntry objects.
    ///
    /// # Arguments
    ///
    /// * `parent_1` - First 64 bits of the parent directory's MD5 path hash.
    /// * `parent_2` - Second 64 bits of the parent directory's MD5 path hash.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<Vec<DirectoryEntry>>` containing all entries in the
    /// specified directory, ordered by name.
    ///
    /// # Errors
    ///
    /// Returns database errors if the query fails or returns malformed data.
    pub fn list_directory_split_md5(
        &self,
        parent_1: i64,
        parent_2: i64,
    ) -> CvmfsResult<Vec<DirectoryEntry>> {
        let mut statement = self.database.create_prepared_statement(LISTING_QUERY)?;
        let mut result = Vec::new();
        let mut rows = statement.query([parent_1, parent_2])?;
        loop {
            match rows.next() {
                Ok(row) => {
                    if let Some(row) = row {
                        result.push(self.make_directory_entry(row)?);
                    } else {
                        break;
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(result)
    }

    /// Lists all entries in a directory specified by path.
    ///
    /// This method retrieves all entries in the directory at the specified path.
    /// It computes the MD5 hash of the path and uses it to query the catalog database.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the directory to list.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<Vec<DirectoryEntry>>` containing all entries in the
    /// specified directory.
    ///
    /// # Errors
    ///
    /// Returns `CvmfsError::FileNotFound` if the path cannot be converted to a string,
    /// or database errors if the query fails.
    pub fn list_directory(&self, path: &str) -> CvmfsResult<Vec<DirectoryEntry>> {
        let mut real_path = canonicalize_path(path);
        if real_path.eq(Path::new("/")) {
            real_path = PathBuf::new();
        }
        let md5_hash = md5::compute(
            real_path
                .to_str()
                .ok_or(CvmfsError::FileNotFound)?
                .bytes()
                .collect::<Vec<u8>>(),
        );
        let parent_hash = split_md5(&md5_hash.0);
        self.list_directory_split_md5(parent_hash.hash1, parent_hash.hash2)
    }

    /// Retrieves repository statistics from the catalog
    ///
    /// This method queries the statistics table in the catalog database and populates
    /// a Statistics struct with the values. These statistics provide information about
    /// the repository content, such as file counts and sizes.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<Statistics>` containing the repository statistics.
    ///
    /// # Errors
    ///
    /// Returns database errors if the statistics cannot be retrieved or parsed.
    pub fn get_statistics(&self) -> CvmfsResult<Statistics> {
        let mut statement = self.database.create_prepared_statement(READ_STATISTICS)?;
        let mut rows = statement.query([])?;
        let mut statistics = Statistics::default();
        while let Some(row) = rows.next()? {
            let name: String = row.get(0)?;
            match name.as_str() {
                "subtree_chunked" => statistics.chunked = row.get(1)?,
                "subtree_chunked_size" => statistics.chunked_size = row.get(1)?,
                "subtree_chunks" => statistics.chunks = row.get(1)?,
                "subtree_dir" => statistics.dir = row.get(1)?,
                "subtree_external" => statistics.external = row.get(1)?,
                "subtree_external_file_size" => statistics.external_file_size = row.get(1)?,
                "subtree_nested" => statistics.nested = row.get(1)?,
                "subtree_regular" => statistics.regular = row.get(1)?,
                "subtree_special" => statistics.special = row.get(1)?,
                "subtree_symlink" => statistics.symlink = row.get(1)?,
                "subtree_xattr" => statistics.xattr = row.get(1)?,
                _ => {}
            }
        }
        Ok(statistics)
    }

    /// Creates a DirectoryEntry from a database row
    ///
    /// This helper method constructs a DirectoryEntry object from a database row
    /// and populates it with chunk information if it represents a chunked file.
    ///
    /// # Arguments
    ///
    /// * `row` - A database row containing directory entry data
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<DirectoryEntry>` containing the constructed entry.
    ///
    /// # Errors
    ///
    /// Returns database errors if the row data is invalid or chunks cannot be read.
    fn make_directory_entry(&self, row: &Row) -> CvmfsResult<DirectoryEntry> {
        let mut directory_entry = DirectoryEntry::new(row)?;
        self.read_chunks(&mut directory_entry)?;
        Ok(directory_entry)
    }

    /// Finds and adds the file chunks to a DirectoryEntry.
    ///
    /// This method retrieves chunk information for a file entry and adds it to the
    /// DirectoryEntry object. Chunks are used for large files that are split into
    /// multiple pieces for efficient storage and transfer.
    ///
    /// # Arguments
    ///
    /// * `directory_entry` - The directory entry to populate with chunk information.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<()>` indicating success or failure.
    ///
    /// # Errors
    ///
    /// Returns database errors if the chunks cannot be retrieved or are invalid.
    fn read_chunks(&self, directory_entry: &mut DirectoryEntry) -> CvmfsResult<()> {
        let mut statement = self.database.create_prepared_statement(READ_CHUNK)?;
        let path_hash = directory_entry.path_hash();
        let iterator = statement.query([path_hash.hash1, path_hash.hash2])?;
        directory_entry.add_chunks(iterator)?;
        Ok(())
    }

    /// Finds a directory entry by its path string.
    ///
    /// This method looks up a file or directory entry by its path in the repository.
    /// It computes the MD5 hash of the path and uses it to query the catalog database.
    ///
    /// # Arguments
    ///
    /// * `root_path` - The path to the file or directory to find.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<DirectoryEntry>` containing the found entry.
    ///
    /// # Errors
    ///
    /// Returns `CvmfsError::FileNotFound` if the entry doesn't exist or the path
    /// cannot be converted to a string, or database errors if the query fails.
    pub fn find_directory_entry(&self, root_path: &str) -> CvmfsResult<DirectoryEntry> {
        let real_path = canonicalize_path(root_path);
        let md5_path = md5::compute(
            real_path
                .to_str()
                .ok_or(CvmfsError::FileNotFound)?
                .bytes()
                .collect::<Vec<u8>>(),
        )
        .0;
        self.find_directory_entry_md5(&md5_path)
    }

    /// Finds a directory entry by its MD5 path hash.
    ///
    /// This method looks up a file or directory entry by the MD5 hash of its path.
    /// It's an alternative to path-based lookup that can be more efficient.
    ///
    /// # Arguments
    ///
    /// * `md5_path` - The 16-byte MD5 hash of the path to find.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<DirectoryEntry>` containing the found entry.
    ///
    /// # Errors
    ///
    /// Returns `CvmfsError::FileNotFound` if the entry doesn't exist,
    /// or database errors if the query fails.
    pub fn find_directory_entry_md5(&self, md5_path: &[u8; 16]) -> CvmfsResult<DirectoryEntry> {
        let path_hash = split_md5(md5_path);
        self.find_directory_entry_split_md5(path_hash)
    }

    /// Finds a directory entry by its split MD5 path hash.
    ///
    /// This method looks up a file or directory entry by the split components of its
    /// MD5 path hash. It's the lowest-level lookup method used by the other find methods.
    ///
    /// # Arguments
    ///
    /// * `path_hash` - The PathHash struct containing the split MD5 hash components.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<DirectoryEntry>` containing the found entry.
    ///
    /// # Errors
    ///
    /// Returns `CvmfsError::FileNotFound` if the entry doesn't exist,
    /// or database errors if the query fails.
    fn find_directory_entry_split_md5(&self, path_hash: PathHash) -> CvmfsResult<DirectoryEntry> {
        let mut statement = self.database.create_prepared_statement(FIND_MD5_PATH)?;
        let mut rows = statement.query([path_hash.hash1, path_hash.hash2])?;
        let row = rows.next()?.ok_or(CvmfsError::FileNotFound)?;
        self.make_directory_entry(row)
    }
}
