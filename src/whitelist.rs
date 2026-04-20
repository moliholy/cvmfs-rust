use chrono::{DateTime, NaiveDateTime, Utc};

use crate::common::{CvmfsError, CvmfsResult};

#[derive(Debug)]
pub struct Whitelist {
	pub created: DateTime<Utc>,
	pub expires: DateTime<Utc>,
	pub repository_name: String,
	pub fingerprints: Vec<String>,
}

impl Whitelist {
	pub fn parse(content: &[u8]) -> CvmfsResult<Self> {
		let text = String::from_utf8_lossy(content);
		let mut lines = text.lines();

		let created_str = lines.next().ok_or(CvmfsError::ParseError)?;
		let created = Self::parse_timestamp(created_str)?;

		let expires_line = lines.next().ok_or(CvmfsError::ParseError)?;
		let expires_str = expires_line.strip_prefix('E').ok_or(CvmfsError::ParseError)?;
		let expires = Self::parse_timestamp(expires_str)?;

		let name_line = lines.next().ok_or(CvmfsError::ParseError)?;
		let repository_name =
			name_line.strip_prefix('N').ok_or(CvmfsError::ParseError)?.to_string();

		let mut fingerprints = Vec::new();
		for line in lines {
			if line.starts_with("--") {
				break;
			}
			let fingerprint = line.split('#').next().unwrap_or("").trim();
			if !fingerprint.is_empty() {
				fingerprints.push(fingerprint.to_string());
			}
		}

		Ok(Self { created, expires, repository_name, fingerprints })
	}

	pub fn is_expired(&self) -> bool {
		Utc::now() > self.expires
	}

	pub fn matches_repository(&self, fqrn: &str) -> bool {
		self.repository_name == fqrn
	}

	fn parse_timestamp(s: &str) -> CvmfsResult<DateTime<Utc>> {
		NaiveDateTime::parse_from_str(s, "%Y%m%d%H%M%S")
			.map(|dt| dt.and_utc())
			.map_err(|_| CvmfsError::ParseError)
	}
}

#[cfg(test)]
mod tests {
	use chrono::Datelike;

	use super::*;

	fn sample_whitelist() -> Vec<u8> {
		b"20260330131219\n\
		  E20260729131219\n\
		  Nboss.cern.ch\n\
		  18:81:35:37:A7:2C:31:DB:4E:2A:6A:96:EC:A8:D4:27:06:31:5E:2F\n\
		  82:B5:70:A7:C7:CD:77:07:62:58:91:0A:E3:5E:F5:5C:1E:72:CF:CF\n\
		  --\n\
		  fakechecksum\n"
			.to_vec()
	}

	#[test]
	fn parse_whitelist() {
		let wl = Whitelist::parse(&sample_whitelist()).unwrap();
		assert_eq!(wl.repository_name, "boss.cern.ch");
		assert_eq!(wl.fingerprints.len(), 2);
		assert!(wl.fingerprints[0].contains("18:81:35:37"));
	}

	#[test]
	fn parse_timestamps() {
		let wl = Whitelist::parse(&sample_whitelist()).unwrap();
		assert_eq!(wl.created.year(), 2026);
		assert_eq!(wl.expires.month(), 7);
	}

	#[test]
	fn matches_repository() {
		let wl = Whitelist::parse(&sample_whitelist()).unwrap();
		assert!(wl.matches_repository("boss.cern.ch"));
		assert!(!wl.matches_repository("other.cern.ch"));
	}

	#[test]
	fn empty_input_errors() {
		let result = Whitelist::parse(b"");
		assert!(result.is_err());
	}

	#[test]
	fn missing_expiry_errors() {
		let result = Whitelist::parse(b"20260330131219\n");
		assert!(result.is_err());
	}

	#[test]
	fn missing_name_errors() {
		let result = Whitelist::parse(b"20260330131219\nE20260729131219\n");
		assert!(result.is_err());
	}

	#[test]
	fn no_fingerprints() {
		let data = b"20260330131219\nE20260729131219\nNtest.repo\n--\nchecksum\n";
		let wl = Whitelist::parse(data).unwrap();
		assert!(wl.fingerprints.is_empty());
		assert_eq!(wl.repository_name, "test.repo");
	}
}
