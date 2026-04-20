use std::{ffi::OsStr, fs, path::Path};

use cvmfs::{
	common::CvmfsResult, fetcher::Fetcher, file_system::CernvmFileSystem, repository::Repository,
};
use fuse_mt::{FilesystemMT, RequestInfo};
use serial_test::serial;

const REPO_URL: &str = "http://cvmfs-stratum-one.cern.ch/opt/boss";
const CACHE_PATH: &str = "/tmp/cvmfs_test_cache";
const PINNED_REVISION: u32 = 293;

fn create_repo() -> Repository {
	fs::create_dir_all(CACHE_PATH).expect("Failure creating the cache");
	let fetcher = Fetcher::new(REPO_URL, CACHE_PATH, true).expect("Failure creating the fetcher");
	let mut repo = Repository::new(fetcher).expect("Failure creating the repository");
	repo.set_current_tag(PINNED_REVISION).expect("Failure setting pinned revision");
	repo
}

// --- Repository initialization ---

#[test]
#[serial]
fn test_initialization() -> CvmfsResult<()> {
	let repo = create_repo();
	assert_eq!(0, repo.catalog_count());
	assert_eq!("boss.cern.ch", repo.fqrn);
	repo.retrieve_current_root_catalog()?;
	assert_eq!(1, repo.catalog_count());
	Ok(())
}

#[test]
#[serial]
fn test_revision_info() -> CvmfsResult<()> {
	let repo = create_repo();
	let revision = repo.get_revision_number()?;
	assert!(revision > 0);
	let root_hash = repo.get_root_hash()?;
	assert!(!root_hash.is_empty());
	let name = repo.get_name()?;
	assert!(!name.is_empty());
	let timestamp = repo.get_timestamp()?;
	assert!(timestamp > 0);
	Ok(())
}

#[test]
#[serial]
fn test_current_tag() -> CvmfsResult<()> {
	let repo = create_repo();
	let tag = repo.current_tag()?;
	assert!(tag.revision > 0);
	assert!(!tag.hash.is_empty());
	assert!(tag.timestamp > 0);
	Ok(())
}

// --- History and tags ---

#[test]
#[serial]
fn test_has_history() {
	let repo = create_repo();
	assert!(repo.has_history());
}

#[test]
#[serial]
fn test_retrieve_history() -> CvmfsResult<()> {
	let repo = create_repo();
	let history = repo.retrieve_history()?;
	assert_eq!(history.schema, "1.0");
	assert_eq!(history.fqrn, "boss.cern.ch");
	Ok(())
}

#[test]
#[serial]
fn test_get_tag_by_revision() -> CvmfsResult<()> {
	let repo = create_repo();
	let current_rev = repo.get_revision_number()? as u32;
	let tag = repo.get_tag(current_rev)?;
	assert_eq!(tag.revision, current_rev as i32);
	assert!(!tag.hash.is_empty());
	Ok(())
}

#[test]
#[serial]
fn test_get_last_tag() -> CvmfsResult<()> {
	let repo = create_repo();
	let tag = repo.get_last_tag()?;
	assert!(tag.revision > 0);
	assert!(!tag.hash.is_empty());
	Ok(())
}

#[test]
#[serial]
fn test_set_current_tag() -> CvmfsResult<()> {
	let mut repo = create_repo();
	let current_rev = repo.get_revision_number()? as u32;
	repo.set_current_tag(current_rev)?;
	let tag = repo.current_tag()?;
	assert_eq!(tag.revision, current_rev as i32);
	Ok(())
}

// --- History queries ---

#[test]
#[serial]
fn test_history_get_tag_by_name() -> CvmfsResult<()> {
	let repo = create_repo();
	let history = repo.retrieve_history()?;
	let tag = history.get_tag_by_name("trunk")?;
	assert!(tag.is_some());
	let tag = tag.unwrap();
	assert_eq!(tag.name, "trunk");
	Ok(())
}

#[test]
#[serial]
fn test_history_get_tag_by_revision() -> CvmfsResult<()> {
	let repo = create_repo();
	let history = repo.retrieve_history()?;
	let current_rev = repo.current_tag()?.revision as u32;
	let tag = history.get_tag_by_revision(current_rev)?;
	assert!(tag.is_some());
	assert_eq!(tag.unwrap().revision, current_rev as i32);
	Ok(())
}

#[test]
#[serial]
fn test_history_get_tag_by_date() -> CvmfsResult<()> {
	let repo = create_repo();
	let history = repo.retrieve_history()?;
	let tag = history.get_tag_by_date(0)?;
	assert!(tag.is_some());
	Ok(())
}

#[test]
#[serial]
fn test_history_get_nonexistent_name() -> CvmfsResult<()> {
	let repo = create_repo();
	let history = repo.retrieve_history()?;
	let tag = history.get_tag_by_name("this_tag_does_not_exist_at_all")?;
	assert!(tag.is_none());
	Ok(())
}

#[test]
#[serial]
fn test_history_get_nonexistent_revision() -> CvmfsResult<()> {
	let repo = create_repo();
	let history = repo.retrieve_history()?;
	let tag = history.get_tag_by_revision(999_999_999)?;
	assert!(tag.is_none());
	Ok(())
}

// --- Catalog operations ---

#[test]
#[serial]
fn test_retrieve_root_catalog() -> CvmfsResult<()> {
	let repo = create_repo();
	repo.retrieve_current_root_catalog()?;
	let hash = repo.get_root_hash()?.to_string();
	repo.with_catalog(&hash, |catalog| {
		assert!(catalog.is_root());
		assert!(catalog.revision > 0);
		assert!(catalog.schema > 0.0);
		Ok(())
	})
}

#[test]
#[serial]
fn test_catalog_caching() -> CvmfsResult<()> {
	let repo = create_repo();
	let hash = repo.get_root_hash()?.to_string();
	repo.with_catalog(&hash, |_| Ok(()))?;
	assert_eq!(1, repo.catalog_count());
	repo.with_catalog(&hash, |_| Ok(()))?;
	assert_eq!(1, repo.catalog_count());
	Ok(())
}

#[test]
#[serial]
fn test_catalog_has_nested() -> CvmfsResult<()> {
	let repo = create_repo();
	repo.retrieve_current_root_catalog()?;
	let hash = repo.get_root_hash()?.to_string();
	repo.with_catalog(&hash, |catalog| {
		assert!(catalog.has_nested()?);
		let count = catalog.nested_count()?;
		assert!(count > 0);
		Ok(())
	})
}

#[test]
#[serial]
fn test_catalog_list_nested() -> CvmfsResult<()> {
	let repo = create_repo();
	repo.retrieve_current_root_catalog()?;
	let hash = repo.get_root_hash()?.to_string();
	repo.with_catalog(&hash, |catalog| {
		let nested = catalog.list_nested()?;
		assert!(!nested.is_empty());
		for nested_ref in &nested {
			assert!(!nested_ref.root_path.is_empty());
			assert!(!nested_ref.catalog_hash.is_empty());
		}
		Ok(())
	})
}

#[test]
#[serial]
fn test_retrieve_catalog_for_path() -> CvmfsResult<()> {
	let repo = create_repo();
	let root_hash = repo.retrieve_catalog_for_path("")?;
	repo.with_catalog(&root_hash, |catalog| {
		assert!(catalog.is_root());
		Ok(())
	})?;
	let nested_hash = repo.retrieve_catalog_for_path("/slc4_ia32_gcc34")?;
	repo.with_catalog(&nested_hash, |catalog| {
		assert!(!catalog.is_root());
		Ok(())
	})
}

// --- Statistics ---

#[test]
#[serial]
fn test_statistics() -> CvmfsResult<()> {
	let repo = create_repo();
	repo.retrieve_current_root_catalog()?;
	let stats = repo.get_statistics()?;
	assert!(stats.dir > 0);
	assert!(stats.regular > 0);
	assert!(stats.file_size > 0);
	Ok(())
}

// --- Directory listing ---

#[test]
#[serial]
fn test_list_root_directory() -> CvmfsResult<()> {
	let repo = create_repo();
	let entries = repo.list_directory("/")?;
	assert!(!entries.is_empty());
	let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
	assert!(names.contains(&"testfile"));
	assert!(names.contains(&"database"));
	assert!(names.contains(&"pacman-3.29"));
	Ok(())
}

#[test]
#[serial]
fn test_list_subdirectory() -> CvmfsResult<()> {
	let repo = create_repo();
	let entries = repo.list_directory("/database")?;
	assert!(!entries.is_empty());
	let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
	assert!(names.contains(&"offlinedb.db"));
	assert!(names.contains(&"run.db"));
	Ok(())
}

#[test]
#[serial]
fn test_list_nested_catalog_directory() -> CvmfsResult<()> {
	let repo = create_repo();
	let entries = repo.list_directory("/slc4_ia32_gcc34")?;
	assert!(!entries.is_empty());
	Ok(())
}

#[test]
#[serial]
fn test_list_nonexistent_directory() {
	let repo = create_repo();
	let result = repo.list_directory("/nonexistent_path_xyz");
	assert!(result.is_err());
}

// --- Lookup ---

#[test]
#[serial]
fn test_lookup_root() -> CvmfsResult<()> {
	let repo = create_repo();
	let entry = repo.lookup("/")?;
	assert!(entry.is_directory());
	Ok(())
}

#[test]
#[serial]
fn test_lookup_file() -> CvmfsResult<()> {
	let repo = create_repo();
	let entry = repo.lookup("/testfile")?;
	assert!(entry.is_file());
	assert_eq!(entry.name, "testfile");
	assert_eq!(entry.size, 50);
	Ok(())
}

#[test]
#[serial]
fn test_lookup_directory() -> CvmfsResult<()> {
	let repo = create_repo();
	let entry = repo.lookup("/database")?;
	assert!(entry.is_directory());
	assert_eq!(entry.name, "database");
	Ok(())
}

#[test]
#[serial]
fn test_lookup_symlink() -> CvmfsResult<()> {
	let repo = create_repo();
	let entry = repo.lookup("/pacman-3.29/setup.csh")?;
	assert!(entry.is_symlink());
	assert_eq!(entry.symlink.as_deref(), Some("scripts/initialize_setup.csh"));
	Ok(())
}

#[test]
#[serial]
fn test_lookup_nested_catalog_entry() -> CvmfsResult<()> {
	let repo = create_repo();
	let entry = repo.lookup("/slc4_ia32_gcc34")?;
	assert!(entry.is_directory());
	assert!(entry.is_nested_catalog_mountpoint() || entry.is_nested_catalog_root());
	Ok(())
}

#[test]
#[serial]
fn test_lookup_nonexistent() {
	let repo = create_repo();
	let result = repo.lookup("/this/does/not/exist");
	assert!(result.is_err());
}

// --- File reading ---

#[test]
#[serial]
fn test_get_file_regular() -> CvmfsResult<()> {
	let repo = create_repo();
	let file = repo.get_file("/testfile")?;
	let mut buf = vec![0u8; file.file_size() as usize];
	let n = file.read_at(0, &mut buf)?;
	let contents = String::from_utf8_lossy(&buf[..n]);
	assert_eq!(n, 50);
	assert!(contents.contains("slc4_ia32_gcc34"));
	Ok(())
}

#[test]
#[serial]
fn test_get_file_binary() -> CvmfsResult<()> {
	let repo = create_repo();
	let file = repo.get_file("/pacman-latest.tar.gz")?;
	let mut header = [0u8; 2];
	file.read_at(0, &mut header)?;
	assert_eq!(header, [0x1f, 0x8b]); // gzip magic
	Ok(())
}

#[test]
#[serial]
fn test_get_file_not_a_file() {
	let repo = create_repo();
	let result = repo.get_file("/database");
	assert!(result.is_err());
}

#[test]
#[serial]
fn test_get_file_nonexistent() {
	let repo = create_repo();
	let result = repo.get_file("/nonexistent_file");
	assert!(result.is_err());
}

// --- Chunked file reading ---

#[test]
#[serial]
fn test_chunked_file_read_first_bytes() -> CvmfsResult<()> {
	let repo = create_repo();
	let file = repo.get_file("/database/offlinedb.db")?;
	let mut header = [0u8; 16];
	file.read_at(0, &mut header)?;
	assert_eq!(&header[..6], b"SQLite");
	Ok(())
}

#[test]
#[serial]
fn test_chunked_file_seek_and_read() -> CvmfsResult<()> {
	let repo = create_repo();
	let file = repo.get_file("/database/offlinedb.db")?;
	let mut buf1 = [0u8; 64];
	file.read_at(0, &mut buf1)?;
	let mut buf2 = [0u8; 64];
	file.read_at(0, &mut buf2)?;
	assert_eq!(buf1, buf2);
	Ok(())
}

#[test]
#[serial]
fn test_chunked_file_cross_chunk_read() -> CvmfsResult<()> {
	let repo = create_repo();
	let entry = repo.lookup("/database/offlinedb.db")?;
	assert!(entry.has_chunks());
	assert!(!entry.chunks.is_empty());
	let first_chunk_size = entry.chunks[0].size as u64;
	let file = repo.get_file("/database/offlinedb.db")?;
	let offset = first_chunk_size - 32;
	let mut buf = [0u8; 64];
	let n = file.read_at(offset, &mut buf)?;
	assert_eq!(n, 64);
	Ok(())
}

// --- Fetcher ---

#[test]
#[serial]
fn test_fetcher_new_http() -> CvmfsResult<()> {
	let fetcher = Fetcher::new(REPO_URL, CACHE_PATH, true)?;
	assert!(fetcher.source.starts_with("http"));
	Ok(())
}

#[test]
#[serial]
fn test_fetcher_new_local_dir() -> CvmfsResult<()> {
	let tmp = format!("/tmp/cvmfs_test_fetcher_local_{}", std::process::id());
	fs::create_dir_all(&tmp)?;
	let fetcher = Fetcher::new(&tmp, CACHE_PATH, true)?;
	assert!(fetcher.source.starts_with("file://"));
	fs::remove_dir_all(&tmp).ok();
	Ok(())
}

#[test]
#[serial]
fn test_fetcher_retrieve_raw_file() -> CvmfsResult<()> {
	let fetcher = Fetcher::new(REPO_URL, CACHE_PATH, true)?;
	let path = fetcher.retrieve_raw_file(".cvmfspublished")?;
	assert!(fs::metadata(&path).is_ok());
	let content = fs::read(&path)?;
	assert!(!content.is_empty());
	let text = String::from_utf8_lossy(&content);
	assert!(text.contains("boss.cern.ch"));
	Ok(())
}

#[test]
#[serial]
fn test_fetcher_retrieve_file_cached() -> CvmfsResult<()> {
	let fetcher = Fetcher::new(REPO_URL, CACHE_PATH, true)?;
	let path1 = fetcher.retrieve_file("data/08/ade4fa3cf6beb5023967c23f6e1f7d8b7754ec")?;
	let path2 = fetcher.retrieve_file("data/08/ade4fa3cf6beb5023967c23f6e1f7d8b7754ec")?;
	assert_eq!(path1, path2);
	Ok(())
}

// --- DirectoryEntry properties ---

#[test]
#[serial]
fn test_directory_entry_file_attributes() -> CvmfsResult<()> {
	let repo = create_repo();
	let entry = repo.lookup("/testfile")?;
	assert!(entry.is_file());
	assert!(!entry.is_directory());
	assert!(!entry.is_symlink());
	assert!(!entry.has_chunks());
	assert!(entry.content_hash.is_some());
	assert!(entry.content_hash_string().is_some());
	assert!(entry.mode > 0);
	assert!(entry.mtime > 0);
	assert_eq!(entry.uid, 313);
	assert_eq!(entry.gid, 313);
	assert!(entry.nlink() > 0);
	Ok(())
}

#[test]
#[serial]
fn test_directory_entry_chunked_attributes() -> CvmfsResult<()> {
	let repo = create_repo();
	let entry = repo.lookup("/database/offlinedb.db")?;
	assert!(entry.is_file());
	assert!(entry.has_chunks());
	assert!(!entry.chunks.is_empty());
	assert!(entry.content_hash.is_none());
	assert!(entry.content_hash_string().is_none());
	for chunk in &entry.chunks {
		assert!(!chunk.content_hash.is_empty());
		assert!(chunk.size > 0);
	}
	Ok(())
}

#[test]
#[serial]
fn test_directory_entry_symlink_attributes() -> CvmfsResult<()> {
	let repo = create_repo();
	let entry = repo.lookup("/pacman-3.29/setup.csh")?;
	assert!(entry.is_symlink());
	assert!(!entry.is_directory());
	assert!(entry.symlink.is_some());
	Ok(())
}

// --- Catalog direct operations ---

#[test]
#[serial]
fn test_catalog_find_directory_entry() -> CvmfsResult<()> {
	let repo = create_repo();
	repo.retrieve_current_root_catalog()?;
	let hash = repo.get_root_hash()?.to_string();
	repo.with_catalog(&hash, |catalog| {
		let entry = catalog.find_directory_entry("")?;
		assert!(entry.is_directory());
		Ok(())
	})
}

#[test]
#[serial]
fn test_catalog_list_directory() -> CvmfsResult<()> {
	let repo = create_repo();
	repo.retrieve_current_root_catalog()?;
	let hash = repo.get_root_hash()?.to_string();
	repo.with_catalog(&hash, |catalog| {
		let entries = catalog.list_directory("/")?;
		assert!(!entries.is_empty());
		Ok(())
	})
}

#[test]
#[serial]
fn test_catalog_statistics() -> CvmfsResult<()> {
	let repo = create_repo();
	repo.retrieve_current_root_catalog()?;
	let hash = repo.get_root_hash()?.to_string();
	repo.with_catalog(&hash, |catalog| {
		let stats = catalog.get_statistics()?;
		assert!(stats.dir > 0);
		assert!(stats.regular > 0);
		assert!(stats.file_size > 0);
		Ok(())
	})
}

// --- Certificate ---

#[test]
#[serial]
fn test_certificate_verification_runs_on_init() {
	let repo = create_repo();
	assert!(!repo.fqrn.is_empty());
}

// --- CernvmFileSystem ---

fn create_fs() -> CernvmFileSystem {
	let repo = create_repo();
	CernvmFileSystem::new(repo).expect("Failure creating the filesystem")
}

fn dummy_req() -> RequestInfo {
	RequestInfo { unique: 1, uid: 0, gid: 0, pid: std::process::id() }
}

#[test]
#[serial]
fn test_fs_getattr_root() {
	let fs = create_fs();
	let (ttl, attr) = fs.getattr(dummy_req(), Path::new("/"), None).unwrap();
	assert!(ttl.as_secs() > 0);
	assert_eq!(attr.kind, fuse_mt::FileType::Directory);
	assert!(attr.nlink >= 2);
}

#[test]
#[serial]
fn test_fs_getattr_file() {
	let fs = create_fs();
	let (_, attr) = fs.getattr(dummy_req(), Path::new("/testfile"), None).unwrap();
	assert_eq!(attr.kind, fuse_mt::FileType::RegularFile);
	assert_eq!(attr.size, 50);
	assert!(attr.nlink > 0);
	assert_eq!(attr.uid, 313);
	assert_eq!(attr.gid, 313);
	assert_eq!(attr.flags, 0);
	assert_eq!(attr.rdev, 0);
}

#[test]
#[serial]
fn test_fs_getattr_directory() {
	let fs = create_fs();
	let (_, attr) = fs.getattr(dummy_req(), Path::new("/database"), None).unwrap();
	assert_eq!(attr.kind, fuse_mt::FileType::Directory);
	assert_eq!(attr.nlink, 2);
}

#[test]
#[serial]
fn test_fs_getattr_nonexistent() {
	let fs = create_fs();
	let result = fs.getattr(dummy_req(), Path::new("/nonexistent"), None);
	assert!(result.is_err());
}

#[test]
#[serial]
fn test_fs_getattr_symlink() {
	let fs = create_fs();
	let (_, attr) = fs.getattr(dummy_req(), Path::new("/pacman-3.29/setup.csh"), None).unwrap();
	assert_eq!(attr.kind, fuse_mt::FileType::Symlink);
}

#[test]
#[serial]
fn test_fs_readlink() {
	let fs = create_fs();
	let data = fs.readlink(dummy_req(), Path::new("/pacman-3.29/setup.csh")).unwrap();
	let target = String::from_utf8(data).unwrap();
	assert_eq!(target, "scripts/initialize_setup.csh");
}

#[test]
#[serial]
fn test_fs_readlink_not_a_link() {
	let fs = create_fs();
	let result = fs.readlink(dummy_req(), Path::new("/testfile"));
	assert!(result.is_err());
}

#[test]
#[serial]
fn test_fs_open_file() {
	let fs = create_fs();
	let (fh, _flags) = fs.open(dummy_req(), Path::new("/testfile"), 0).unwrap();
	assert!(fh > 0);
}

#[test]
#[serial]
fn test_fs_open_directory_fails() {
	let fs = create_fs();
	let result = fs.open(dummy_req(), Path::new("/database"), 0);
	assert!(result.is_err());
}

#[test]
#[serial]
fn test_fs_release() {
	let fs = create_fs();
	let (fh, _) = fs.open(dummy_req(), Path::new("/testfile"), 0).unwrap();
	let result = fs.release(dummy_req(), Path::new("/testfile"), fh, 0, 0, false);
	assert!(result.is_ok());
}

#[test]
#[serial]
fn test_fs_release_nonexistent() {
	let fs = create_fs();
	let result = fs.release(dummy_req(), Path::new("/not_opened"), 0, 0, 0, false);
	assert!(result.is_err());
}

#[test]
#[serial]
fn test_fs_flush() {
	let fs = create_fs();
	let result = fs.flush(dummy_req(), Path::new("/testfile"), 0, 0);
	assert!(result.is_ok());
}

#[test]
#[serial]
fn test_fs_opendir() {
	let fs = create_fs();
	let (fh, _) = fs.opendir(dummy_req(), Path::new("/"), 0).unwrap();
	assert!(fh > 0);
}

#[test]
#[serial]
fn test_fs_opendir_not_a_dir() {
	let fs = create_fs();
	let result = fs.opendir(dummy_req(), Path::new("/testfile"), 0);
	assert!(result.is_err());
}

#[test]
#[serial]
fn test_fs_readdir() {
	let fs = create_fs();
	let (fh, _) = fs.opendir(dummy_req(), Path::new("/"), 0).unwrap();
	let entries = fs.readdir(dummy_req(), Path::new("/"), fh).unwrap();
	assert!(!entries.is_empty());
	let names: Vec<String> = entries.iter().map(|e| e.name.to_string_lossy().to_string()).collect();
	assert!(names.contains(&"testfile".to_string()));
	assert!(names.contains(&"database".to_string()));
}

#[test]
#[serial]
fn test_fs_readdir_not_a_dir() {
	let fs = create_fs();
	let result = fs.readdir(dummy_req(), Path::new("/testfile"), 0);
	assert!(result.is_err());
}

#[test]
#[serial]
fn test_fs_releasedir() {
	let fs = create_fs();
	let result = fs.releasedir(dummy_req(), Path::new("/"), 0, 0);
	assert!(result.is_ok());
}

#[test]
#[serial]
fn test_fs_statfs() {
	let fs = create_fs();
	let stats = fs.statfs(dummy_req(), Path::new("/")).unwrap();
	assert!(stats.blocks > 0);
	assert!(stats.files > 0);
	assert_eq!(stats.bsize, 512);
	assert_eq!(stats.namelen, 255);
}

#[test]
#[serial]
fn test_fs_getxattr() {
	let fs = create_fs();
	let result = fs.getxattr(dummy_req(), Path::new("/"), OsStr::new("user.test"), 0);
	assert!(result.is_err());
}

#[test]
#[serial]
fn test_fs_access_exists() {
	let fs = create_fs();
	let result = fs.access(dummy_req(), Path::new("/testfile"), 0);
	assert!(result.is_ok());
}

#[test]
#[serial]
fn test_fs_access_nonexistent() {
	let fs = create_fs();
	let result = fs.access(dummy_req(), Path::new("/nonexistent"), 0);
	assert!(result.is_err());
}

#[test]
#[serial]
fn test_fs_destroy() {
	let fs = create_fs();
	fs.open(dummy_req(), Path::new("/testfile"), 0).unwrap();
	fs.destroy();
}
