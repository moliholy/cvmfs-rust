//! # Directory Entry Management for CernVM-FS
//!
//! This module provides functionality for handling directory entries in CernVM-FS.
//! Directory entries represent files, directories, symlinks, and other filesystem
//! objects within a CernVM-FS repository. They contain metadata such as size,
//! permissions, and content hashes.
//!
//! ## Directory Entries
//!
//! Directory entries in CernVM-FS contain the following information:
//! - Path information (including MD5 hashes of paths for lookup)
//! - File attributes (size, mode, modification time)
//! - Content addressing (hashes for retrieving file content)
//! - Type flags (file, directory, symlink, etc.)
//! - Chunk information for large files
//!
//! ## Content Addressing
//!
//! CernVM-FS uses content-addressed storage where file contents are referenced by
//! cryptographic hashes. This module supports multiple hash types and provides
//! functionality for working with these content references.
//!
//! ## Path Hashing
//!
//! For efficient lookups, paths in CernVM-FS are hashed using MD5. This module
//! provides structures for working with these path hashes, which are split into
//! two 64-bit components for database indexing.

use std::ops::BitAnd;

use hex::ToHex;
use rusqlite::{Row, Rows};

use crate::common::CvmfsResult;

/// Enumeration of supported content hash types.
///
/// CernVM-FS supports multiple cryptographic hash algorithms for content
/// addressing. This enum defines the supported hash types and their
/// corresponding identifier values used in the catalog database.
///
/// Hash types are used to determine how file content is addressed and verified
/// in the content-addressed storage system.
#[derive(Debug, Copy, Clone)]
pub enum ContentHashTypes {
    /// Represents an unknown or unsupported hash type (value: -1).
    Unknown = -1,
    /// SHA-1 hash algorithm, the default hash type (value: 1).
    Sha1 = 1,
    /// RIPEMD-160 hash algorithm (value: 2).
    Ripemd160 = 2,
    /// Upper boundary marker for hash types (value: 3)
    UpperBound = 3,
}

impl ContentHashTypes {
    /// Returns the hash suffix used in CernVM-FS's content-addressed storage (CAS).
    ///
    /// Different hash algorithms require different suffixes in the content-addressed storage
    /// system. This method returns the appropriate suffix for each hash type:
    /// * `Ripemd160` hash type has a "-rmd160" suffix
    /// * All other hash types (including `Sha1`) have an empty suffix
    ///
    /// # Arguments
    ///
    /// * `obj` - A reference to the ContentHashTypes enum value
    ///
    /// # Returns
    ///
    /// A String containing the appropriate suffix for the hash type
    pub fn hash_suffix(obj: &Self) -> String {
        match obj {
            ContentHashTypes::Ripemd160 => "-rmd160".into(),
            _ => "".into(),
        }
    }
}

impl From<u32> for ContentHashTypes {
    fn from(value: u32) -> Self {
        match value {
            1 => ContentHashTypes::Sha1,
            2 => ContentHashTypes::Ripemd160,
            3 => ContentHashTypes::UpperBound,
            _ => ContentHashTypes::Unknown,
        }
    }
}

/// Flags representing the type and properties of directory entries.
///
/// This enum defines bit flags that indicate what type of filesystem object
/// a directory entry represents (file, directory, symlink, etc.) and its special
/// properties (nested catalog points, chunked files, etc.).
///
/// These flags are stored in the catalog database and used to determine how
/// the entry should be handled by the filesystem operations.
#[derive(Debug, Copy, Clone)]
pub enum Flags {
    /// Entry is a directory (value: 1).
    Directory = 1,
    /// Entry is a mountpoint for a nested catalog (value: 2).
    NestedCatalogMountpoint = 2,
    /// Entry is a regular file (value: 4).
    File = 4,
    /// Entry is a symbolic link (value: 8).
    Link = 8,
    /// Entry has file statistics (value: 16).
    FileStat = 16,
    /// Entry is the root of a nested catalog (value: 32).
    NestedCatalogRoot = 32,
    /// Entry represents a chunk of a large file (value: 64).
    FileChunk = 64,
    /// Bitmask for content hash type (values: 256 + 512 + 1024).
    ContentHashTypes = 256 + 512 + 1024,
}

/// Implementation of bitwise AND operation between two Flags enum values.
///
/// This allows using the & operator to check if a particular flag is set
/// by comparing the numeric values of the flags.
impl BitAnd<Flags> for Flags {
    type Output = u32;

    fn bitand(self, rhs: Flags) -> Self::Output {
        self as u32 & rhs
    }
}

impl BitAnd<u32> for Flags {
    type Output = u32;

    fn bitand(self, rhs: u32) -> Self::Output {
        self as u32 & rhs
    }
}

impl BitAnd<Flags> for u32 {
    type Output = u32;

    fn bitand(self, rhs: Flags) -> Self::Output {
        self & rhs as u32
    }
}

/// Represents a chunk of a large file in CernVM-FS.
///
/// In CernVM-FS, large files are split into multiple chunks for efficient storage
/// and transfer. Each chunk has its own content hash and can be retrieved independently.
/// This struct contains the metadata for a single chunk, including its position within
/// the file (offset), size, and content addressing information.
///
/// File chunks allow for random access to parts of large files without downloading
/// the entire file, and enable more efficient delta updates when only parts of a file
/// change.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub offset: i64,
    pub size: i64,
    pub content_hash: String,
    pub content_hash_type: ContentHashTypes,
}

impl Chunk {
    /// Returns the complete content hash string for this chunk.
    ///
    /// This method combines the raw content hash with the appropriate hash suffix
    /// based on the hash type. The resulting string is used to locate the chunk
    /// in the content-addressed storage system.
    ///
    /// # Returns
    ///
    /// A String containing the complete content hash identifier, including any
    /// necessary hash type suffix.
    pub fn content_hash_string(&self) -> String {
        format!(
            "{}{}",
            &self.content_hash,
            ContentHashTypes::hash_suffix(&self.content_hash_type)
        )
    }
}

/// Represents a split MD5 path hash used for efficient catalog lookups.
///
/// In CernVM-FS, paths are hashed using MD5 and the resulting 128-bit hash is
/// split into two 64-bit components (hash1 and hash2) for efficient database indexing
/// and lookup operations.
///
/// This split representation is used throughout the catalog database for path-based
/// operations like finding directory entries and listing directory contents.
#[derive(Debug)]
pub struct PathHash {
    pub hash1: i64,
    pub hash2: i64,
}

/// Wraps a DirectoryEntry with its full path string.
///
/// This struct combines a DirectoryEntry with its string path representation,
/// making it easier to work with directory entries in contexts where both the
/// entry metadata and the full path are needed together.
#[derive(Debug)]
pub struct DirectoryEntryWrapper {
    pub directory_entry: DirectoryEntry,
    pub path: String,
}

/// Represents a file system object in the CernVM-FS repository.
///
/// A DirectoryEntry contains all metadata for a file, directory, or symbolic link
/// in the repository. This includes basic file attributes (size, permissions, timestamps),
/// content addressing information (hashes for retrieving file content), and type flags.
///
/// For large files that are split into chunks, the DirectoryEntry also maintains a list
/// of chunks that make up the complete file.
///
/// Directory entries are stored in the catalog database and are the primary objects
/// used for filesystem operations like listing directories and looking up files.
#[derive(Debug, Clone)]
pub struct DirectoryEntry {
    /// First 64 bits of the MD5 hash of the entry's path.
    pub md5_path_1: i64,
    /// Second 64 bits of the MD5 hash of the entry's path.
    pub md5_path_2: i64,
    /// First 64 bits of the MD5 hash of the parent directory's path.
    pub parent_1: i64,
    /// Second 64 bits of the MD5 hash of the parent directory's path.
    pub parent_2: i64,
    /// Content hash for retrieving the file's content (None for chunked files).
    pub content_hash: Option<String>,
    /// Bit flags indicating the entry type and properties.
    pub flags: u32,
    /// Size of the file in bytes.
    pub size: i64,
    /// Unix file mode/permissions (as a 16-bit value).
    pub mode: u16,
    /// Modification time (Unix timestamp).
    pub mtime: i64,
    /// Base name of the file or directory (without path components).
    pub name: String,
    /// Target path for symbolic links (None for non-symlinks).
    pub symlink: Option<String>,
    /// Type of hash used for content addressing.
    pub content_hash_type: ContentHashTypes,
    /// List of chunks for large files that are split into multiple pieces.
    pub chunks: Vec<Chunk>,
}

impl DirectoryEntry {
    /// Creates a new DirectoryEntry from a database row.
    ///
    /// This method constructs a DirectoryEntry by extracting values from a database row
    /// retrieved from the catalog. It parses various fields including path hashes,
    /// content hash, flags, size, permissions, and other metadata.
    ///
    /// Note that this method doesn't populate the chunks field - for chunked files,
    /// the `add_chunks` method should be called separately to populate chunk information.
    ///
    /// # Arguments
    ///
    /// * `row` - A reference to a SQLite row containing directory entry data.
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<Self>` containing the constructed DirectoryEntry,
    /// or an error if any required field is missing or has an invalid format.
    pub fn new(row: &Row) -> CvmfsResult<Self> {
        let content_hash: Option<Vec<u8>> = row.get(4)?;
        let flags = row.get(5)?;
        Ok(Self {
            md5_path_1: row.get(0)?,
            md5_path_2: row.get(1)?,
            parent_1: row.get(2)?,
            parent_2: row.get(3)?,
            content_hash: content_hash.map(|value| value.encode_hex()),
            flags,
            size: row.get(6)?,
            mode: row.get(7)?,
            mtime: row.get(8)?,
            name: row.get(9)?,
            symlink: row.get(10)?,
            content_hash_type: Self::read_content_hash_type(flags),
            chunks: vec![],
        })
    }

    /// Adds chunk information to a directory entry for chunked files.
    ///
    /// This method populates the chunks vector from database query results. For large files
    /// that are split into multiple chunks, this method processes each chunk row and
    /// adds the chunk metadata to the directory entry.
    ///
    /// # Arguments
    ///
    /// * `rows` - An iterator over database rows containing chunk information.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if chunks were successfully added, or an error if the database
    /// query failed or returned malformed data.
    pub fn add_chunks(&mut self, mut rows: Rows) -> CvmfsResult<()> {
        self.chunks.clear();
        loop {
            match rows.next() {
                Ok(row) => {
                    if let Some(row) = row {
                        let content_hash: Vec<u8> = row.get(4)?;
                        self.chunks.push(Chunk {
                            offset: row.get(2)?,
                            size: row.get(3)?,
                            content_hash: content_hash.encode_hex(),
                            content_hash_type: self.content_hash_type,
                        })
                    } else {
                        break;
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Checks if this entry is a directory.
    ///
    /// This method tests whether the Directory flag is set in the entry's flags.
    ///
    /// # Returns
    ///
    /// Returns `true` if this entry is a directory, `false` otherwise.
    pub fn is_directory(&self) -> bool {
        self.flags & Flags::Directory > 0
    }

    /// Checks if this entry is a mountpoint for a nested catalog.
    ///
    /// Nested catalog mountpoints are special directory entries that indicate the
    /// presence of a nested catalog at that path. When traversing the filesystem
    /// hierarchy, these mountpoints signal that a different catalog database needs
    /// to be loaded to access entries beneath this point.
    ///
    /// # Returns
    ///
    /// Returns `true` if this entry is a nested catalog mountpoint, `false` otherwise.
    pub fn is_nested_catalog_mountpoint(&self) -> bool {
        self.flags & Flags::NestedCatalogMountpoint > 0
    }

    /// Checks if this entry is the root of a nested catalog.
    ///
    /// Nested catalog roots are the top-level directory entries in a nested catalog.
    /// They represent the same filesystem object as a nested catalog mountpoint,
    /// but from within the nested catalog itself rather than from the parent catalog.
    ///
    /// # Returns
    ///
    /// Returns `true` if this entry is a nested catalog root, `false` otherwise.
    pub fn is_nested_catalog_root(&self) -> bool {
        self.flags & Flags::NestedCatalogRoot > 0
    }

    /// Checks if this entry is a regular file.
    ///
    /// This method tests whether the File flag is set in the entry's flags.
    ///
    /// # Returns
    ///
    /// Returns `true` if this entry is a regular file, `false` otherwise.
    pub fn is_file(&self) -> bool {
        self.flags & Flags::File > 0
    }

    /// Checks if this entry is a symbolic link.
    ///
    /// This method tests whether the Link flag is set in the entry's flags.
    /// Symbolic links in CernVM-FS point to other paths in the repository.
    ///
    /// # Returns
    ///
    /// Returns `true` if this entry is a symbolic link, `false` otherwise.
    pub fn is_symlink(&self) -> bool {
        self.flags & Flags::Link > 0
    }

    /// Returns a PathHash struct containing this entry's path hash components.
    ///
    /// This method constructs a PathHash struct from the entry's MD5 path hash
    /// components (md5_path_1 and md5_path_2).
    ///
    /// # Returns
    ///
    /// A PathHash struct containing the two 64-bit components of the path hash.
    pub fn path_hash(&self) -> PathHash {
        PathHash {
            hash1: self.md5_path_1,
            hash2: self.md5_path_2,
        }
    }

    /// Returns a PathHash struct containing this entry's parent directory path hash.
    ///
    /// This method constructs a PathHash struct from the entry's parent directory
    /// MD5 path hash components (parent_1 and parent_2).
    ///
    /// # Returns
    ///
    /// A PathHash struct containing the two 64-bit components of the parent path hash.
    pub fn parent_hash(&self) -> PathHash {
        PathHash {
            hash1: self.parent_1,
            hash2: self.parent_2,
        }
    }

    /// Checks if this entry represents a chunked file.
    ///
    /// Large files in CernVM-FS are split into chunks, and their content_hash field
    /// is None to indicate this. This method checks that condition.
    ///
    /// # Returns
    ///
    /// Returns true if the file is chunked (content_hash is None), false otherwise.
    pub fn has_chunks(&self) -> bool {
        self.content_hash.is_none()
    }

    /// Returns the complete content hash string for this entry.
    ///
    /// This method combines the raw content hash with the appropriate hash type suffix
    /// to form the complete identifier used in content-addressed storage.
    ///
    /// # Returns
    ///
    /// Returns None if this is a chunked file, otherwise returns Some containing
    /// the complete hash string with appropriate suffix.
    pub fn content_hash_string(&self) -> Option<String> {
        self.content_hash.clone().map(|value| {
            format!(
                "{}{}",
                &value,
                ContentHashTypes::hash_suffix(&self.content_hash_type)
            )
        })
    }

    /// Extracts the content hash type from the entry flags.
    ///
    /// This method manipulates the flags bitfield to extract and decode the
    /// content hash type bits. It shifts the ContentHashTypes bits right until
    /// aligned and converts the value to a ContentHashTypes enum.
    ///
    /// # Arguments
    ///
    /// * `flags` - The entry flags containing the hash type bits.
    ///
    /// # Returns
    ///
    /// The ContentHashTypes value encoded in the flags.
    fn read_content_hash_type(flags: u32) -> ContentHashTypes {
        let mut bit_mask = Flags::ContentHashTypes as u32;
        let mut right_shifts = 0;
        while (bit_mask & 1) == 0 {
            bit_mask >>= 1;
            right_shifts += 1;
        }
        (((flags & Flags::ContentHashTypes) >> right_shifts) + 1).into()
    }
}
