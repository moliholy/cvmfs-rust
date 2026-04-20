use std::{
	cell::RefCell,
	collections::HashMap,
	ffi::OsStr,
	sync::{
		Arc, Mutex, RwLock,
		atomic::{AtomicU64, Ordering},
	},
	time::{Duration, Instant, SystemTime},
};

use chrono::{DateTime, Utc};
use fuser::{
	FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
	ReplyEntry, ReplyOpen, ReplyStatfs, ReplyXattr, Request,
};

use crate::{
	common::{CvmfsError, CvmfsResult, FileLike},
	directory_entry::DirectoryEntry,
	repository::Repository,
};

thread_local! {
	static READ_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(128 * 1024));
}

const FOPEN_KEEP_CACHE: u32 = 0x02;
const TTL: Duration = Duration::from_secs(3600);
const FUSE_ROOT_ID: u64 = 1;

type DirListing = Vec<(u64, FileType, String)>;

static NEXT_FH: AtomicU64 = AtomicU64::new(1);

#[allow(clippy::unnecessary_cast)]
fn map_dirent_type_to_fs_kind(dirent: &DirectoryEntry) -> FileType {
	if dirent.is_directory() {
		FileType::Directory
	} else if dirent.is_symlink() {
		FileType::Symlink
	} else {
		let mode = dirent.mode as u32;
		let ifmt = libc::S_IFMT as u32;
		match mode & ifmt {
			m if m == libc::S_IFSOCK as u32 => FileType::Socket,
			m if m == libc::S_IFIFO as u32 => FileType::NamedPipe,
			m if m == libc::S_IFBLK as u32 => FileType::BlockDevice,
			m if m == libc::S_IFCHR as u32 => FileType::CharDevice,
			_ => FileType::RegularFile,
		}
	}
}

fn make_file_attr(ino: u64, entry: &DirectoryEntry) -> Option<FileAttr> {
	let time = SystemTime::from(DateTime::<Utc>::from_timestamp(entry.mtime, 0)?);
	let size = entry.size as u64;
	Some(FileAttr {
		ino,
		size,
		blocks: 1 + size / 512,
		atime: time,
		mtime: time,
		ctime: time,
		crtime: time,
		kind: map_dirent_type_to_fs_kind(entry),
		perm: entry.mode & 0o7777,
		nlink: entry.nlink(),
		uid: entry.uid,
		gid: entry.gid,
		rdev: 0,
		blksize: 512,
		flags: 0,
	})
}

#[derive(Debug)]
struct InodeTable {
	next_ino: AtomicU64,
	path_to_ino: RwLock<HashMap<String, u64>>,
	ino_to_path: RwLock<HashMap<u64, String>>,
}

impl InodeTable {
	fn new() -> Self {
		let mut path_to_ino = HashMap::new();
		let mut ino_to_path = HashMap::new();
		path_to_ino.insert(String::new(), FUSE_ROOT_ID);
		ino_to_path.insert(FUSE_ROOT_ID, String::new());
		Self {
			next_ino: AtomicU64::new(2),
			path_to_ino: RwLock::new(path_to_ino),
			ino_to_path: RwLock::new(ino_to_path),
		}
	}

	fn get_or_insert(&self, path: &str) -> u64 {
		if let Some(ino) = self.path_to_ino.read().ok().and_then(|m| m.get(path).copied()) {
			return ino;
		}
		let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
		if let (Ok(mut p2i), Ok(mut i2p)) = (self.path_to_ino.write(), self.ino_to_path.write()) {
			if let Some(&existing) = p2i.get(path) {
				return existing;
			}
			p2i.insert(path.into(), ino);
			i2p.insert(ino, path.into());
		}
		ino
	}

	fn get_path(&self, ino: u64) -> Option<String> {
		self.ino_to_path.read().ok()?.get(&ino).cloned()
	}
}

/// Filesystem statistics cached value
#[derive(Debug, Clone, Copy)]
struct CachedStatfs {
	blocks: u64,
	files: u64,
}

/// FUSE filesystem implementation for CernVM-FS using fuser directly.
#[derive(Debug)]
pub struct CernvmFileSystem {
	repository: RwLock<Repository>,
	inodes: InodeTable,
	opened_files: RwLock<HashMap<u64, Box<dyn FileLike>>>,
	opened_dirs: RwLock<HashMap<u64, DirListing>>,
	lookup_cache: RwLock<HashMap<String, Arc<DirectoryEntry>>>,
	cached_statfs: Mutex<Option<(Instant, CachedStatfs)>>,
}

impl Filesystem for CernvmFileSystem {
	fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
		let name = match name.to_str() {
			Some(n) => n,
			None => return reply.error(libc::ENOENT),
		};
		let parent_path = match self.inodes.get_path(parent) {
			Some(p) => p,
			None => return reply.error(libc::ENOENT),
		};
		let child_path = if parent_path.is_empty() {
			format!("/{name}")
		} else {
			format!("{parent_path}/{name}")
		};
		let entry = match self.cached_lookup(&child_path) {
			Ok(e) => e,
			Err(_) => return reply.error(libc::ENOENT),
		};
		let ino = self.inodes.get_or_insert(&child_path);
		match make_file_attr(ino, &entry) {
			Some(attr) => reply.entry(&TTL, &attr, 0),
			None => reply.error(libc::EIO),
		}
	}

	fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
		let path = match self.inodes.get_path(ino) {
			Some(p) => p,
			None => return reply.error(libc::ENOENT),
		};
		let cvmfs_path = if path.is_empty() { "" } else { &path };
		let entry = match self.cached_lookup(cvmfs_path) {
			Ok(e) => e,
			Err(_) => return reply.error(libc::ENOENT),
		};
		match make_file_attr(ino, &entry) {
			Some(attr) => reply.attr(&TTL, &attr),
			None => reply.error(libc::EIO),
		}
	}

	fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
		let path = match self.inodes.get_path(ino) {
			Some(p) => p,
			None => return reply.error(libc::ENOENT),
		};
		let entry = match self.cached_lookup(&path) {
			Ok(e) => e,
			Err(_) => return reply.error(libc::ENOENT),
		};
		if !entry.is_symlink() {
			return reply.error(libc::EINVAL);
		}
		match &entry.symlink {
			Some(target) => reply.data(target.as_bytes()),
			None => reply.error(libc::EIO),
		}
	}

	fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
		let path = match self.inodes.get_path(ino) {
			Some(p) => p,
			None => return reply.error(libc::ENOENT),
		};
		let entry = match self.cached_lookup(&path) {
			Ok(e) => e,
			Err(_) => return reply.error(libc::ENOENT),
		};
		if !entry.is_file() {
			return reply.error(libc::EISDIR);
		}
		let repo = match self.repository.read() {
			Ok(r) => r,
			Err(_) => return reply.error(libc::EIO),
		};
		let file = match repo.retrieve_object(&entry, &path) {
			Ok(f) => f,
			Err(_) => return reply.error(libc::EIO),
		};
		drop(repo);
		let fh = NEXT_FH.fetch_add(1, Ordering::Relaxed);
		if let Ok(mut files) = self.opened_files.write() {
			files.insert(fh, file);
		}
		reply.opened(fh, FOPEN_KEEP_CACHE);
	}

	fn read(
		&mut self,
		_req: &Request<'_>,
		_ino: u64,
		fh: u64,
		offset: i64,
		size: u32,
		_flags: i32,
		_lock_owner: Option<u64>,
		reply: ReplyData,
	) {
		let opened_files = match self.opened_files.read() {
			Ok(guard) => guard,
			Err(_) => return reply.error(libc::EIO),
		};
		let file = match opened_files.get(&fh) {
			Some(f) => f,
			None => return reply.error(libc::EBADF),
		};
		READ_BUF.with(|buf| {
			let mut data = buf.borrow_mut();
			data.resize(size as usize, 0);
			match file.read_at(offset as u64, &mut data) {
				Ok(n) => reply.data(&data[..n]),
				Err(_) => reply.error(libc::EIO),
			}
		});
	}

	fn release(
		&mut self,
		_req: &Request<'_>,
		_ino: u64,
		fh: u64,
		_flags: i32,
		_lock_owner: Option<u64>,
		_flush: bool,
		reply: ReplyEmpty,
	) {
		if let Ok(mut files) = self.opened_files.write() {
			files.remove(&fh);
		}
		reply.ok();
	}

	fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
		let path = match self.inodes.get_path(ino) {
			Some(p) => p,
			None => return reply.error(libc::ENOENT),
		};
		let cvmfs_path = if path.is_empty() { "" } else { &path };
		let repo = match self.repository.read() {
			Ok(r) => r,
			Err(_) => return reply.error(libc::EIO),
		};
		let entries = match repo.list_directory(cvmfs_path) {
			Ok(e) => e,
			Err(_) => return reply.error(libc::ENOTDIR),
		};
		drop(repo);

		let mut all_entries: Vec<(u64, FileType, String)> = Vec::with_capacity(entries.len() + 2);
		all_entries.push((ino, FileType::Directory, ".".into()));
		let parent_ino = if ino == FUSE_ROOT_ID {
			FUSE_ROOT_ID
		} else {
			let parent_path = path.rsplit_once('/').map_or(String::new(), |(p, _)| p.into());
			self.inodes.get_or_insert(&parent_path)
		};
		all_entries.push((parent_ino, FileType::Directory, "..".into()));

		if let Ok(mut cache) = self.lookup_cache.write() {
			self.evict_if_full(&mut cache, entries.len());
			for entry in &entries {
				let child_path = if path.is_empty() {
					format!("/{}", entry.name)
				} else {
					format!("{}/{}", path, entry.name)
				};
				let child_ino = self.inodes.get_or_insert(&child_path);
				all_entries.push((
					child_ino,
					map_dirent_type_to_fs_kind(entry),
					entry.name.clone(),
				));
				cache.insert(child_path, Arc::new(entry.clone()));
			}
		} else {
			for entry in &entries {
				let child_path = if path.is_empty() {
					format!("/{}", entry.name)
				} else {
					format!("{}/{}", path, entry.name)
				};
				let child_ino = self.inodes.get_or_insert(&child_path);
				all_entries.push((
					child_ino,
					map_dirent_type_to_fs_kind(entry),
					entry.name.clone(),
				));
			}
		}

		let fh = NEXT_FH.fetch_add(1, Ordering::Relaxed);
		if let Ok(mut dirs) = self.opened_dirs.write() {
			dirs.insert(fh, all_entries);
		}
		reply.opened(fh, 0);
	}

	fn readdir(
		&mut self,
		_req: &Request<'_>,
		_ino: u64,
		fh: u64,
		offset: i64,
		mut reply: ReplyDirectory,
	) {
		let dirs = match self.opened_dirs.read() {
			Ok(d) => d,
			Err(_) => return reply.error(libc::EIO),
		};
		let all_entries = match dirs.get(&fh) {
			Some(e) => e,
			None => return reply.error(libc::EBADF),
		};
		for (i, (ino, kind, name)) in all_entries.iter().enumerate().skip(offset as usize) {
			if reply.add(*ino, (i + 1) as i64, *kind, name) {
				break;
			}
		}
		reply.ok();
	}

	fn releasedir(
		&mut self,
		_req: &Request<'_>,
		_ino: u64,
		fh: u64,
		_flags: i32,
		reply: ReplyEmpty,
	) {
		if let Ok(mut dirs) = self.opened_dirs.write() {
			dirs.remove(&fh);
		}
		reply.ok();
	}

	fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
		if let Ok(guard) = self.cached_statfs.lock()
			&& let Some((ts, cached)) = *guard
			&& ts.elapsed() < Duration::from_secs(5)
		{
			return reply.statfs(cached.blocks, 0, 0, cached.files, 0, 512, 255, 0);
		}
		let (blocks, files) = match self.repository.read() {
			Ok(repo) => match repo.get_statistics() {
				Ok(stats) => (1 + stats.file_size as u64 / 512, stats.regular as u64),
				Err(_) => (0, 0),
			},
			Err(_) => return reply.error(libc::EIO),
		};
		if let Ok(mut cache) = self.cached_statfs.lock() {
			*cache = Some((Instant::now(), CachedStatfs { blocks, files }));
		}
		reply.statfs(blocks, 0, 0, files, 0, 512, 255, 0);
	}

	fn getxattr(
		&mut self,
		_req: &Request<'_>,
		_ino: u64,
		name: &OsStr,
		size: u32,
		reply: ReplyXattr,
	) {
		let name = match name.to_str() {
			Some(n) => n,
			None => return reply.error(libc::ENODATA),
		};
		let repo = match self.repository.read() {
			Ok(r) => r,
			Err(_) => return reply.error(libc::EIO),
		};
		let value = match name {
			"user.fqrn" => repo.fqrn.clone(),
			"user.revision" => repo.manifest.revision.to_string(),
			"user.hash" => repo.manifest.root_catalog.clone(),
			"user.host" => repo.fetcher_source(),
			"user.expires" => repo.manifest.last_modified.to_rfc3339(),
			"user.nclg" => repo.opened_catalogs.read().map(|c| c.len()).unwrap_or(0).to_string(),
			_ => return reply.error(libc::ENODATA),
		};
		let bytes = value.into_bytes();
		if size == 0 {
			reply.size(bytes.len() as u32);
		} else {
			reply.data(&bytes);
		}
	}

	fn access(&mut self, _req: &Request<'_>, ino: u64, _mask: i32, reply: ReplyEmpty) {
		let path = match self.inodes.get_path(ino) {
			Some(p) => p,
			None => return reply.error(libc::ENOENT),
		};
		let cvmfs_path = if path.is_empty() { "" } else { &path };
		match self.cached_lookup(cvmfs_path) {
			Ok(_) => reply.ok(),
			Err(_) => reply.error(libc::ENOENT),
		}
	}
}

const LOOKUP_CACHE_CAP: usize = 65_536;

impl CernvmFileSystem {
	fn cached_lookup(&self, path: &str) -> CvmfsResult<Arc<DirectoryEntry>> {
		let lookup_path = if path.is_empty() { "/" } else { path };
		if let Some(entry) = self.lookup_cache.read().ok().and_then(|c| c.get(lookup_path).cloned())
		{
			return Ok(entry);
		}
		let cvmfs_path = if path == "/" { "" } else { path };
		let repo = self.repository.read().map_err(|e| CvmfsError::Generic(format!("{e:?}")))?;
		let entry = Arc::new(repo.lookup(cvmfs_path)?);
		drop(repo);
		if let Ok(mut cache) = self.lookup_cache.write() {
			self.evict_if_full(&mut cache, 1);
			cache.insert(lookup_path.into(), Arc::clone(&entry));
		}
		Ok(entry)
	}

	fn evict_if_full(&self, cache: &mut HashMap<String, Arc<DirectoryEntry>>, incoming: usize) {
		if cache.len() + incoming > LOOKUP_CACHE_CAP {
			cache.clear();
		}
	}

	/// Creates a new `CernvmFileSystem` instance.
	pub fn new(repository: Repository) -> CvmfsResult<Self> {
		Ok(Self {
			repository: RwLock::new(repository),
			inodes: InodeTable::new(),
			opened_files: Default::default(),
			opened_dirs: Default::default(),
			lookup_cache: Default::default(),
			cached_statfs: Mutex::new(None),
		})
	}

	/// Mount the filesystem at the given path.
	pub fn mount(self, mountpoint: &str) -> std::io::Result<()> {
		let options =
			vec![MountOption::RO, MountOption::FSName("cernvmfs".into()), MountOption::NoAtime];
		fuser::mount2(self, mountpoint, &options)
	}

	/// Look up a path and return its attributes. Used by tests.
	pub fn do_lookup(&self, path: &str) -> CvmfsResult<(u64, FileAttr)> {
		let entry = self.cached_lookup(path)?;
		let lookup_path = if path.is_empty() || path == "/" { "/" } else { path };
		let ino = self.inodes.get_or_insert(lookup_path);
		let attr = make_file_attr(ino, &entry).ok_or(CvmfsError::InvalidTimestamp)?;
		Ok((ino, attr))
	}

	/// Read symlink target for a path.
	pub fn do_readlink(&self, path: &str) -> CvmfsResult<String> {
		let entry = self.cached_lookup(path)?;
		if !entry.is_symlink() {
			return Err(CvmfsError::NotASymlink);
		}
		entry.symlink.clone().ok_or(CvmfsError::FileNotFound)
	}

	/// Open a file, returning a file handle.
	pub fn do_open(&self, path: &str) -> CvmfsResult<u64> {
		let entry = self.cached_lookup(path)?;
		if !entry.is_file() {
			return Err(CvmfsError::NotAFile);
		}
		let repo = self.repository.read().map_err(|e| CvmfsError::Generic(format!("{e:?}")))?;
		let file = repo.retrieve_object(&entry, path)?;
		drop(repo);
		let fh = NEXT_FH.fetch_add(1, Ordering::Relaxed);
		self.opened_files.write().map_err(|_| CvmfsError::Sync)?.insert(fh, file);
		Ok(fh)
	}

	/// Release (close) a file handle.
	pub fn do_release(&self, fh: u64) -> CvmfsResult<()> {
		self.opened_files
			.write()
			.map_err(|_| CvmfsError::Sync)?
			.remove(&fh)
			.ok_or(CvmfsError::FileNotFound)?;
		Ok(())
	}

	/// Read data from an open file.
	pub fn do_read(&self, fh: u64, offset: u64, size: u32) -> CvmfsResult<Vec<u8>> {
		let files = self.opened_files.read().map_err(|_| CvmfsError::Sync)?;
		let file = files.get(&fh).ok_or(CvmfsError::FileNotFound)?;
		let mut buf = vec![0u8; size as usize];
		let n = file.read_at(offset, &mut buf).map_err(|_| CvmfsError::FileNotFound)?;
		buf.truncate(n);
		Ok(buf)
	}

	/// List directory entries for a path.
	pub fn do_readdir(&self, path: &str) -> CvmfsResult<Vec<(FileType, String)>> {
		let cvmfs_path = if path == "/" { "" } else { path };
		let entry = self.cached_lookup(cvmfs_path)?;
		if !entry.is_directory() {
			return Err(CvmfsError::FileNotFound);
		}
		let repo = self.repository.read().map_err(|e| CvmfsError::Generic(format!("{e:?}")))?;
		let entries = repo.list_directory(cvmfs_path)?;
		drop(repo);
		Ok(entries.into_iter().map(|e| (map_dirent_type_to_fs_kind(&e), e.name)).collect())
	}

	/// Get filesystem statistics.
	pub fn do_statfs(&self) -> CvmfsResult<(u64, u64)> {
		let repo = self.repository.read().map_err(|e| CvmfsError::Generic(format!("{e:?}")))?;
		let stats = repo.get_statistics()?;
		Ok((1 + stats.file_size as u64 / 512, stats.regular as u64))
	}

	/// Get extended attribute value.
	pub fn do_getxattr(&self, name: &str) -> CvmfsResult<String> {
		let repo = self.repository.read().map_err(|e| CvmfsError::Generic(format!("{e:?}")))?;
		match name {
			"user.fqrn" => Ok(repo.fqrn.clone()),
			"user.revision" => Ok(repo.manifest.revision.to_string()),
			"user.hash" => Ok(repo.manifest.root_catalog.clone()),
			"user.host" => Ok(repo.fetcher_source()),
			"user.expires" => Ok(repo.manifest.last_modified.to_rfc3339()),
			"user.nclg" => {
				Ok(repo.opened_catalogs.read().map(|c| c.len()).unwrap_or(0).to_string())
			}
			_ => Err(CvmfsError::FileNotFound),
		}
	}

	/// Check if path exists (access check).
	pub fn do_access(&self, path: &str) -> CvmfsResult<()> {
		let cvmfs_path = if path == "/" { "" } else { path };
		self.cached_lookup(cvmfs_path).map(|_| ())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::directory_entry::Flags;

	fn make_entry(flags: u32, mode: u16) -> DirectoryEntry {
		DirectoryEntry {
			md5_path_1: 0,
			md5_path_2: 0,
			parent_1: 0,
			parent_2: 0,
			content_hash: None,
			flags,
			size: 0,
			mode,
			mtime: 0,
			name: String::new(),
			symlink: None,
			uid: 0,
			gid: 0,
			xattr: None,
			content_hash_type: crate::directory_entry::ContentHashTypes::Sha1,
			chunks: Vec::new(),
			hardlinks: 0,
		}
	}

	#[test]
	fn map_type_directory() {
		let entry = make_entry(Flags::Directory as u32, 0o40755);
		assert_eq!(map_dirent_type_to_fs_kind(&entry), FileType::Directory);
	}

	#[test]
	fn map_type_symlink() {
		let entry = make_entry(Flags::Link as u32, 0o120777);
		assert_eq!(map_dirent_type_to_fs_kind(&entry), FileType::Symlink);
	}

	#[test]
	fn map_type_regular_file() {
		let entry = make_entry(Flags::File as u32, 0o100644);
		assert_eq!(map_dirent_type_to_fs_kind(&entry), FileType::RegularFile);
	}

	#[test]
	fn map_type_socket() {
		let entry = make_entry(Flags::File as u32, 0o140755);
		assert_eq!(map_dirent_type_to_fs_kind(&entry), FileType::Socket);
	}

	#[test]
	fn map_type_named_pipe() {
		let entry = make_entry(Flags::File as u32, 0o010644);
		assert_eq!(map_dirent_type_to_fs_kind(&entry), FileType::NamedPipe);
	}

	#[test]
	fn map_type_block_device() {
		let entry = make_entry(Flags::File as u32, 0o060660);
		assert_eq!(map_dirent_type_to_fs_kind(&entry), FileType::BlockDevice);
	}

	#[test]
	fn map_type_char_device() {
		let entry = make_entry(Flags::File as u32, 0o020666);
		assert_eq!(map_dirent_type_to_fs_kind(&entry), FileType::CharDevice);
	}

	#[test]
	fn map_type_zero_mode_defaults_to_regular() {
		let entry = make_entry(Flags::File as u32, 0);
		assert_eq!(map_dirent_type_to_fs_kind(&entry), FileType::RegularFile);
	}
}
