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

use std::{
	fs::File,
	io::{BufRead, BufReader, Read as _},
	str::Split,
};

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
	checksum: Option<String>,
	signature: Option<Vec<u8>>,
	contents: String,
}

impl RootFile {
	pub fn has_signature(&self) -> bool {
		self.checksum.is_some()
	}

	pub fn signature(&self) -> Option<&[u8]> {
		self.signature.as_deref()
	}

	pub fn checksum(&self) -> Option<&str> {
		self.checksum.as_deref()
	}

	pub fn lines(&self) -> Split<'_, char> {
		self.contents.split('\n')
	}

	pub fn new(file: &File) -> CvmfsResult<Self> {
		let mut reader = BufReader::new(file);
		let mut buffer = String::new();
		let mut contents = String::new();
		let mut checksum: Option<String> = None;
		let mut signature: Option<Vec<u8>> = None;

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
				let mut sig_bytes = Vec::new();
				reader.read_to_end(&mut sig_bytes)?;
				if !sig_bytes.is_empty() {
					signature = Some(sig_bytes);
				}
				break;
			} else {
				contents.push_str(buffer.as_str())
			}
		}

		// Verify signature if present
		if checksum.is_some() {
			let mut hasher = Sha1::new();
			hasher.update(contents.as_bytes());
			let computed: String = hasher.finalize().encode_hex();

			// Compare computed hash with stored hash
			if computed.ne(checksum.as_ref().ok_or(CvmfsError::InvalidRootFileSignature)?) {
				return Err(CvmfsError::InvalidRootFileSignature);
			}
		}

		Ok(Self { checksum, signature, contents })
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::{io::Write, path::PathBuf};

	fn tmp_path(name: &str) -> PathBuf {
		std::env::temp_dir().join(format!("cvmfs_rf_{}_{}", name, std::process::id()))
	}

	fn write_tmp_file(name: &str, content: &[u8]) -> File {
		let path = tmp_path(name);
		{
			let mut f = File::create(&path).unwrap();
			f.write_all(content).unwrap();
			f.flush().unwrap();
		}
		File::open(&path).unwrap()
	}

	#[test]
	fn unsigned_file_no_signature() {
		let file = write_tmp_file("unsigned", b"Croot_catalog_hash\nRroot_hash\nBsize\n");
		let rf = RootFile::new(&file).unwrap();
		assert!(!rf.has_signature());
		let lines: Vec<&str> = rf.lines().collect();
		assert_eq!(lines[0], "Croot_catalog_hash");
		assert_eq!(lines[1], "Rroot_hash");
		assert_eq!(lines[2], "Bsize");

		std::fs::remove_file(tmp_path("unsigned")).ok();
	}

	#[test]
	fn signed_file_valid_signature() {
		let content = "Chello\nRworld\n";
		let mut hasher = Sha1::new();
		hasher.update(content.as_bytes());
		let hash: String = hasher.finalize().encode_hex();

		let mut data = content.to_string();
		data.push_str("--\n");
		data.push_str(&hash);
		data.push('\n');

		let file = write_tmp_file("signed_valid", data.as_bytes());
		let rf = RootFile::new(&file).unwrap();
		assert!(rf.has_signature());
		let lines: Vec<&str> = rf.lines().collect();
		assert_eq!(lines[0], "Chello");
		assert_eq!(lines[1], "Rworld");

		std::fs::remove_file(tmp_path("signed_valid")).ok();
	}

	#[test]
	fn signed_file_invalid_signature_returns_error() {
		let content = "Cdata\n";
		let bogus_hash = "aa".repeat(20);

		let mut data = content.to_string();
		data.push_str("--\n");
		data.push_str(&bogus_hash);
		data.push('\n');

		let file = write_tmp_file("signed_invalid", data.as_bytes());
		let result = RootFile::new(&file);
		assert!(result.is_err());
		assert_eq!(result.unwrap_err(), CvmfsError::InvalidRootFileSignature);

		std::fs::remove_file(tmp_path("signed_invalid")).ok();
	}

	#[test]
	fn short_hash_after_terminator_returns_io_error() {
		let data = "Cdata\n--\nshort\n";
		let file = write_tmp_file("short_hash", data.as_bytes());
		let result = RootFile::new(&file);
		assert!(result.is_err());

		std::fs::remove_file(tmp_path("short_hash")).ok();
	}

	#[test]
	fn non_hex_hash_returns_error() {
		let bad_hash = "g".repeat(40);
		let data = format!("Cdata\n--\n{}\n", bad_hash);
		let file = write_tmp_file("non_hex", data.as_bytes());
		let result = RootFile::new(&file);
		assert!(result.is_err());

		std::fs::remove_file(tmp_path("non_hex")).ok();
	}

	#[test]
	fn empty_file_no_signature() {
		let file = write_tmp_file("empty", b"");
		let rf = RootFile::new(&file).unwrap();
		assert!(!rf.has_signature());
		let lines: Vec<&str> = rf.lines().collect();
		assert_eq!(lines, vec![""]);

		std::fs::remove_file(tmp_path("empty")).ok();
	}

	#[test]
	fn single_char_line() {
		let file = write_tmp_file("singlechar", b"X\n");
		let rf = RootFile::new(&file).unwrap();
		assert!(!rf.has_signature());
		let lines: Vec<&str> = rf.lines().collect();
		assert_eq!(lines[0], "X");

		std::fs::remove_file(tmp_path("singlechar")).ok();
	}
}
