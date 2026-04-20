use std::{fs, path::Path};

use crate::common::CvmfsResult;

const DEFAULT_BLACKLIST_PATH: &str = "/etc/cvmfs/blacklist";

#[derive(Debug, Default)]
pub struct Blacklist {
	pub fingerprints: Vec<String>,
	pub revisions: Vec<(String, u32)>,
}

impl Blacklist {
	pub fn load_default() -> Self {
		Self::load(DEFAULT_BLACKLIST_PATH).unwrap_or_default()
	}

	pub fn load(path: &str) -> CvmfsResult<Self> {
		let p = Path::new(path);
		if !p.exists() {
			return Ok(Self::default());
		}
		let content = fs::read_to_string(p)?;
		Self::parse(&content)
	}

	pub fn parse(content: &str) -> CvmfsResult<Self> {
		let mut fingerprints = Vec::new();
		let mut revisions = Vec::new();

		for line in content.lines() {
			let line = line.trim();
			if line.is_empty() || line.starts_with('#') {
				continue;
			}
			if let Some(rest) = line.strip_prefix('<') {
				if let Some((repo, rev_str)) = rest.split_once(' ') {
					let rev = rev_str.trim().parse::<u32>().ok();
					if let Some(rev) = rev {
						revisions.push((repo.to_string(), rev));
					}
				}
			} else {
				fingerprints.push(line.to_string());
			}
		}

		Ok(Self { fingerprints, revisions })
	}

	pub fn is_fingerprint_blocked(&self, fingerprint: &str) -> bool {
		self.fingerprints.iter().any(|f| f == fingerprint)
	}

	pub fn is_revision_blocked(&self, repo_name: &str, revision: u32) -> bool {
		self.revisions
			.iter()
			.any(|(repo, max_rev)| repo == repo_name && revision < *max_rev)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_fingerprints() {
		let content = "AA:BB:CC:DD\nEE:FF:00:11\n";
		let bl = Blacklist::parse(content).unwrap();
		assert_eq!(bl.fingerprints.len(), 2);
		assert!(bl.is_fingerprint_blocked("AA:BB:CC:DD"));
		assert!(!bl.is_fingerprint_blocked("XX:YY"));
	}

	#[test]
	fn parse_revision_based() {
		let content = "<repo.cern.ch 100\n<other.cern.ch 50\n";
		let bl = Blacklist::parse(content).unwrap();
		assert_eq!(bl.revisions.len(), 2);
		assert!(bl.is_revision_blocked("repo.cern.ch", 50));
		assert!(!bl.is_revision_blocked("repo.cern.ch", 100));
		assert!(!bl.is_revision_blocked("repo.cern.ch", 200));
	}

	#[test]
	fn parse_mixed() {
		let content = "# comment\nAA:BB\n<repo.cern.ch 10\n\nCC:DD\n";
		let bl = Blacklist::parse(content).unwrap();
		assert_eq!(bl.fingerprints.len(), 2);
		assert_eq!(bl.revisions.len(), 1);
	}

	#[test]
	fn parse_empty() {
		let bl = Blacklist::parse("").unwrap();
		assert!(bl.fingerprints.is_empty());
		assert!(bl.revisions.is_empty());
	}

	#[test]
	fn parse_comments_only() {
		let bl = Blacklist::parse("# just a comment\n# another\n").unwrap();
		assert!(bl.fingerprints.is_empty());
	}

	#[test]
	fn load_nonexistent_returns_default() {
		let bl = Blacklist::load("/nonexistent/blacklist").unwrap();
		assert!(bl.fingerprints.is_empty());
	}
}
