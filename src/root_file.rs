//! # Root File Management for CernVM-FS
//!
//! This module provides functionality for handling CernVM-FS repository root files,
//! which serve as entry points into the repository. Root files are signed files
//! containing key-value pairs that define essential repository properties.
//!
//! ## Root File Format
//!
//! Root files follow a specific format:
//! - Each line contains a key-value pair
//! - Keys are represented by a single character at the beginning of each line
//! - Values follow the key character directly
//! - The key-value section is terminated either by EOF or a termination line (--)
//! - After the termination line, a signature may follow, consisting of:
//!   * A SHA-1 hash of the key-value content
//!   * A binary private-key signature
//!
//! ## Root File Types
//!
//! The main root files in CernVM-FS are:
//! - `.cvmfspublished` (manifest): Contains repository metadata and references
//! - `.cvmfswhitelist`: Lists trusted certificates for repository validation

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::str::Split;

use hex::ToHex;
use sha1::{Digest, Sha1};

use crate::common::{CvmfsError, CvmfsResult};

/// Base class for CernVM-FS repository's signed 'root files'.
///
/// A CernVM-FS repository has essential 'root files' that have a defined name and
/// serve as entry points into the repository.
/// Namely, the manifest (.cvmfspublished) and the whitelist (.cvmfswhitelist) that
/// both have class representations inheriting from RootFile and implementing the
/// abstract methods defined here.
///
/// Any 'root file' in CernVM-FS is a signed list of line-by-line key-value pairs
/// where the key is represented by a single character in the beginning of a line
/// directly followed by the value. The key-value part of the file is terminted
/// either by EOF or by a termination line (--) followed by a signature.
///
/// The signature follows directly after the termination line with a hash of the
/// key-value line content (without the termination line) followed by an \n and a
/// binary string containing the private-key signature terminated by EOF.
#[derive(Debug)]
pub struct RootFile {
    /// The SHA-1 checksum of the file content, used to verify the signature.
    /// This will be None if the file is not signed.
    checksum: Option<String>,
    /// The raw content of the file as a string, excluding the signature section.
    /// This contains all the key-value pairs that make up the root file.
    contents: String,
}

impl RootFile {
    /// Checks if this root file has a valid signature.
    ///
    /// This method determines whether the root file contains a signature that has
    /// been verified against the file content. A signed root file provides
    /// authenticity guarantees about the repository metadata.
    ///
    /// # Returns
    ///
    /// Returns `true` if the file has a verified signature, `false` otherwise.
    pub fn has_signature(&self) -> bool {
        self.checksum.is_some()
    }

    /// Provides an iterator over the lines in the root file content.
    ///
    /// This method returns a splitter that allows iterating through each line
    /// of the root file. Each line typically contains a key-value pair where
    /// the first character is the key and the remainder is the value.
    ///
    /// # Returns
    ///
    /// Returns a `Split<char>` iterator that yields each line of the root file.
    pub fn lines(&self) -> Split<'_, char> {
        self.contents.split('\n')
    }

    /// Creates a new RootFile instance from a file.
    ///
    /// This constructor reads and parses a CernVM-FS root file, verifying its
    /// signature if present. It handles the specific format of root files, including
    /// the key-value section and optional signature section.
    ///
    /// # Arguments
    ///
    /// * `file` - A reference to the File object to read from
    ///
    /// # Returns
    ///
    /// Returns a `CvmfsResult<Self>` containing the parsed RootFile if successful,
    /// or an error if the file format is invalid or the signature verification fails.
    ///
    /// # Errors
    ///
    /// This function can return the following errors:
    /// * `CvmfsError::IO` - If there is an error reading the file or the signature has an invalid format
    /// * `CvmfsError::InvalidRootFileSignature` - If the signature hash doesn't match the computed hash
    pub fn new(file: &File) -> CvmfsResult<Self> {
        let mut reader = BufReader::new(file);
        let mut buffer = String::new();
        let mut contents = String::new();
        let mut checksum: Option<String> = None;

        // Read the file line by line until EOF or termination marker
        loop {
            buffer.clear();
            let mut bytes_read = reader.read_line(&mut buffer)?;
            if bytes_read == 0 {
                break; // End of file reached
            }

            if buffer.len() >= 2 && buffer[..2].eq("--") {
                buffer.clear();
                bytes_read = reader.read_line(&mut buffer)?;
                if !(bytes_read == 41 || bytes_read == 42) {
                    return Err(CvmfsError::IO("Input does not have 41 bytes".to_string()));
                }
                if buffer.len() < 40 || !buffer[..40].chars().all(|c| c.is_ascii_hexdigit()) {
                    return Err(CvmfsError::InvalidRootFileSignature);
                }
                checksum = Some(buffer[..40].into());
                break;
            } else {
                contents.push_str(buffer.as_str())
            }
        }

        // Verify signature if present
        if checksum.is_some() {
            let mut hasher = Sha1::new();
            hasher.update(contents.as_bytes());
            let hash = &hasher.finalize()[..];
            let signature: String = hash.encode_hex();

            // Compare computed hash with stored hash
            if signature.ne(checksum
                .as_ref()
                .ok_or(CvmfsError::InvalidRootFileSignature)?)
            {
                return Err(CvmfsError::InvalidRootFileSignature);
            }
        }

        Ok(Self { checksum, contents })
    }
}
