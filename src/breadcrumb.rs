use std::{fs, path::Path};

use crate::common::CvmfsResult;

pub struct Breadcrumb;

impl Breadcrumb {
	pub fn write(cache_dir: &str, fqrn: &str, catalog_hash: &str) -> CvmfsResult<()> {
		let path = Self::path(cache_dir, fqrn);
		fs::write(path, catalog_hash)?;
		Ok(())
	}

	pub fn read(cache_dir: &str, fqrn: &str) -> Option<String> {
		let path = Self::path(cache_dir, fqrn);
		fs::read_to_string(path).ok().map(|s| s.trim().to_string())
	}

	pub fn remove(cache_dir: &str, fqrn: &str) {
		let path = Self::path(cache_dir, fqrn);
		fs::remove_file(path).ok();
	}

	fn path(cache_dir: &str, fqrn: &str) -> String {
		Path::new(cache_dir)
			.join(format!("cvmfschecksum.{fqrn}"))
			.to_string_lossy()
			.to_string()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn tmp_dir(name: &str) -> String {
		let p = std::env::temp_dir().join(format!("cvmfs_bc_{}_{}", name, std::process::id()));
		fs::create_dir_all(&p).unwrap();
		p.to_str().unwrap().to_string()
	}

	#[test]
	fn write_and_read() {
		let dir = tmp_dir("wr");
		Breadcrumb::write(&dir, "repo.cern.ch", "abc123").unwrap();
		assert_eq!(Breadcrumb::read(&dir, "repo.cern.ch"), Some("abc123".to_string()));
		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn read_nonexistent() {
		let dir = tmp_dir("noexist");
		assert_eq!(Breadcrumb::read(&dir, "nope.cern.ch"), None);
		fs::remove_dir_all(&dir).ok();
	}

	#[test]
	fn remove_clears() {
		let dir = tmp_dir("rm");
		Breadcrumb::write(&dir, "repo.cern.ch", "hash").unwrap();
		Breadcrumb::remove(&dir, "repo.cern.ch");
		assert_eq!(Breadcrumb::read(&dir, "repo.cern.ch"), None);
		fs::remove_dir_all(&dir).ok();
	}
}
