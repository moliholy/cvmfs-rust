use std::{
	collections::HashMap,
	ffi::{OsStr, OsString},
	path::Path,
	sync::{
		Mutex, RwLock,
		atomic::{AtomicU64, Ordering},
	},
	time::{Duration, Instant, SystemTime},
};

use chrono::{DateTime, Utc};
use fuse_mt::{
	CallbackResult, DirectoryEntry as FuseDirectoryEntry, FileAttr, FileType, FilesystemMT,
	RequestInfo, ResultData, ResultEmpty, ResultEntry, ResultOpen, ResultReaddir, ResultSlice,
	ResultStatfs, ResultXattr, Statfs,
};

static NEXT_DIR_FD: AtomicU64 = AtomicU64::new(0x1000);

use crate::{
	common::{CvmfsError, CvmfsResult, FileLike},
	directory_entry::DirectoryEntry,
	repository::Repository,
};

/// Time-to-live duration for file attributes in the FUSE interface.
///
/// This constant defines how long the operating system should cache file attributes
/// before requesting them again from the filesystem. A short TTL ensures that changes
/// to the repository are quickly reflected in the mounted filesystem, at the cost of
/// more frequent attribute requests. In CernVM-FS, a 1-second TTL provides a good
/// balance between performance and freshness.
const TTL: Duration = Duration::from_secs(1);

/// Maps a CernVM-FS directory entry type to a FUSE file type
///
/// This function converts from the CernVM-FS internal directory entry type representation
/// to the corresponding FUSE file type. This mapping is necessary to present the correct
/// file type information to the operating system through the FUSE interface.
///
/// # Arguments
///
/// * `dirent` - A reference to a `DirectoryEntry` whose type should be mapped.
///
/// # Returns
///
/// Returns a `FileType` value that corresponds to the type of the directory entry.
/// If the entry type cannot be determined, it defaults to `FileType::RegularFile`.
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

/// FUSE filesystem implementation for CernVM-FS.
///
/// This struct implements the `FilesystemMT` trait from the `fuse_mt` crate,
/// providing filesystem operations for a CernVM-FS repository. It handles operations
/// like reading files, listing directories, and retrieving file attributes by delegating
/// to an underlying `Repository` instance.
///
/// The implementation uses `RwLock` to protect shared data, allowing concurrent read
/// operations while ensuring exclusive access for write operations.
#[derive(Debug)]
pub struct CernvmFileSystem {
	/// The repository instance, protected by a read-write lock.
	///
	/// This field stores the CernVM-FS repository that contains all the file metadata
	/// and content. The `RwLock` allows multiple concurrent readers or a single writer,
	/// enabling thread-safe access to the repository data. The repository handles catalog
	/// management, file content retrieval, and metadata operations.
	repository: RwLock<Repository>,

	/// Map of currently opened files, keyed by path string.
	///
	/// This field maintains a mapping from file paths to their corresponding file handles.
	/// When a file is opened, its FileLike implementation is stored in this map and can be
	/// retrieved for subsequent read operations. The `RwLock` ensures thread-safe access
	/// to the map, allowing multiple threads to safely open and close files concurrently.
	opened_files: RwLock<HashMap<String, Box<dyn FileLike>>>,
	cached_statfs: Mutex<Option<(Instant, Statfs)>>,
}

/// Implementation of the FUSE multi-threaded filesystem interface.
///
/// This implementation translates FUSE filesystem operations into operations on the
/// CernVM-FS repository. It handles operations like reading files, listing directories,
/// retrieving file attributes, and managing file handles.
impl FilesystemMT for CernvmFileSystem {
	/// Cleans up resources when the filesystem is being unmounted.
	///
	/// This method is called when the filesystem is being unmounted. It closes all
	/// open files and performs any necessary cleanup.
	fn destroy(&self) {
		if let Ok(mut f) = self.opened_files.write() {
			f.drain();
		};
	}

	/// Retrieves file attributes for a given path.
	///
	/// This method looks up file attributes (size, permissions, timestamps, etc.) for
	/// the file or directory at the specified path. It translates the repository
	/// metadata into FUSE file attributes that can be presented to the operating system.
	///
	/// # Arguments
	///
	/// * `_req` - Information about the request.
	/// * `path` - The path to the file or directory.
	/// * `_fh` - Optional file handle for an open file.
	///
	/// # Returns
	///
	/// Returns a `ResultEntry` containing the file attributes and TTL, or an error code.
	/// if the operation failed.
	fn getattr(&self, _req: RequestInfo, path: &Path, _fh: Option<u64>) -> ResultEntry {
		let path = path.to_str().ok_or(CvmfsError::FileNotFound)?;
		log::info!("Getting attribute of path: {path}");
		let mut repo =
			self.repository.write().map_err(|e| CvmfsError::Generic(format!("{:?}", e)))?;
		let result = repo.lookup(path)?;
		let date_time: DateTime<Utc> =
			DateTime::from_timestamp(result.mtime, 0).ok_or(CvmfsError::InvalidTimestamp)?;
		let time = SystemTime::from(date_time);
		let size = result.size as u64;
		let nlink = result.nlink();
		let file_attr = FileAttr {
			size,
			blocks: 1 + size / 512,
			atime: time,
			mtime: time,
			ctime: time,
			crtime: time,
			kind: map_dirent_type_to_fs_kind(&result),
			perm: result.mode & 0o7777,
			nlink,
			uid: result.uid,
			gid: result.gid,
			rdev: 0,
			flags: 0,
		};
		Ok((TTL, file_attr))
	}

	/// Reads the target of a symbolic link.
	///
	/// This method retrieves the path that a symbolic link points to. It first verifies
	/// that the specified path is indeed a symbolic link before returning its target.
	///
	/// # Arguments
	///
	/// * `_req` - Information about the request
	/// * `path` - The path to the symbolic link
	///
	/// # Returns
	///
	/// Returns a `ResultData` containing the bytes of the symlink target, or an error
	/// code if the operation failed.
	fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
		let path = path.to_str().ok_or(CvmfsError::FileNotFound)?;
		log::info!("Reading link: {path}");
		let mut repo = self.repository.write().map_err(|_| CvmfsError::Sync)?;
		let result = repo.lookup(path)?;
		if !result.is_symlink() {
			return Err(libc::ENOLINK);
		}
		Ok(result.symlink.ok_or(CvmfsError::FileNotFound)?.into_bytes())
	}

	/// Opens a file and returns a file handle
	///
	/// This method opens a file for reading, returning a file handle that can be used
	/// in subsequent read operations. It verifies that the path refers to a regular file
	/// before opening it.
	///
	/// # Arguments
	///
	/// * `_req` - Information about the request
	/// * `path` - The path to the file to open
	/// * `_flags` - Flags specifying how the file should be opened
	///
	/// # Returns
	///
	/// Returns a `ResultOpen` containing the file handle and flags, or an error code
	/// if the operation failed.
	fn open(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
		let path = path.to_str().ok_or(CvmfsError::FileNotFound)?;
		log::info!("Opening file: {path}");
		let mut repo = self.repository.write().map_err(|_| CvmfsError::Sync)?;
		let result = repo.lookup(path)?;
		if !result.is_file() {
			return Err(libc::ENOENT);
		}
		let file = repo.get_file(path)?;
		let fd = file.as_raw_fd() as u64;
		self.opened_files
			.write()
			.map_err(|_| CvmfsError::Sync)?
			.insert(path.into(), file);
		Ok((fd, 0))
	}

	/// Reads data from an open file.
	///
	/// This method reads a specified amount of data from an open file, starting at the
	/// given offset. It uses the file handle to look up the file object in the opened
	/// files map, then reads the requested data and passes it to the callback function.
	///
	/// # Arguments
	///
	/// * `_req` - Information about the request.
	/// * `path` - The path to the file.
	/// * `_fh` - The file handle returned by `open`.
	/// * `offset` - The offset into the file where reading should begin.
	/// * `size` - The number of bytes to read.
	/// * `callback` - A callback function that will be called with the read data.
	///
	/// # Returns
	///
	/// Returns a `CallbackResult` from the callback function, or an error if the
	/// operation failed.
	fn read(
		&self,
		_req: RequestInfo,
		path: &Path,
		_fh: u64,
		offset: u64,
		size: u32,
		callback: impl FnOnce(ResultSlice<'_>) -> CallbackResult,
	) -> CallbackResult {
		let path = match path.to_str() {
			Some(p) => p,
			None => return callback(Err(libc::ENOENT)),
		};
		log::info!("Reading file: {path}");
		let opened_files = match self.opened_files.read() {
			Ok(guard) => guard,
			Err(e) => {
				log::error!("{:?}", e);
				return callback(Err(libc::EIO));
			}
		};
		let file = match opened_files.get(path) {
			Some(f) => f,
			None => return callback(Err(libc::ENOENT)),
		};

		let mut data = vec![0u8; size as usize];
		let bytes_read = match file.read_at(offset, &mut data) {
			Ok(n) => n,
			Err(e) => {
				log::error!("{:?}", e);
				return callback(Err(match e.raw_os_error() {
					Some(code) => code,
					None => libc::EIO,
				}));
			}
		};

		callback(Ok(&data[0..bytes_read]))
	}

	/// Flushes cached file data to storage.
	///
	/// This method is called when the file system should flush any cached data for a
	/// specific file to storage. Since CernVM-FS is a read-only filesystem, this
	/// operation is essentially a no-op, but we still log it for debugging purposes.
	///
	/// # Arguments
	///
	/// * `_req` - Information about the request.
	/// * `path` - The path to the file.
	/// * `_fh` - The file handle.
	/// * `_lock_owner` - The lock owner ID.
	///
	/// # Returns
	///
	/// Returns `Ok(())` if successful, or an error code otherwise.
	fn flush(&self, _req: RequestInfo, path: &Path, _fh: u64, _lock_owner: u64) -> ResultEmpty {
		let path = path.to_str().ok_or(libc::ENOENT)?;
		log::info!("Flushing file: {path}");
		Ok(())
	}

	/// Releases an open file.
	///
	/// This method is called when a file descriptor is closed. It removes the file from
	/// the opened files map, effectively closing the file and releasing its resources.
	///
	/// # Arguments
	///
	/// * `_req` - Information about the request.
	/// * `path` - The path to the file.
	/// * `_fh` - The file handle.
	/// * `_flags` - The flags the file was opened with.
	/// * `_lock_owner` - The lock owner ID.
	/// * `_flush` - Whether to flush data before releasing.
	///
	/// # Returns
	///
	/// Returns `Ok(())` if successful, or an error code otherwise.
	fn release(
		&self,
		_req: RequestInfo,
		path: &Path,
		_fh: u64,
		_flags: u32,
		_lock_owner: u64,
		_flush: bool,
	) -> ResultEmpty {
		let path = path.to_str().ok_or(libc::ENOENT)?;
		log::info!("Releasing: {path}");
		match self
			.opened_files
			.write()
			.map_err(|e| {
				log::error!("{:?}", e);
				libc::EIO
			})?
			.remove(path)
		{
			None => Err(libc::ENOENT),
			Some(_) => Ok(()),
		}
	}

	/// Opens a directory for reading.
	///
	/// This method verifies that the path refers to a directory and prepares it for
	/// listing. Since directory entries in CernVM-FS are retrieved by path rather than
	/// by a file descriptor, this method mainly just verifies that the directory exists.
	///
	/// # Arguments
	///
	/// * `_req` - Information about the request.
	/// * `path` - The path to the directory.
	/// * `_flags` - The flags the directory should be opened with.
	///
	/// # Returns
	///
	/// Returns a `ResultOpen` containing a file handle and flags, or an error code
	/// if the operation failed.
	fn opendir(&self, _req: RequestInfo, path: &Path, _flags: u32) -> ResultOpen {
		let path = path.to_str().ok_or(libc::ENOENT)?;
		log::info!("Opening directory: {path}");
		let mut repo = match self.repository.write() {
			Ok(repo) => repo,
			Err(e) => {
				log::error!("{:?}", e);
				return Err(libc::EIO);
			}
		};
		let result = repo.lookup(path)?;
		if !result.is_directory() {
			return Err(libc::ENOENT);
		}
		let fd = NEXT_DIR_FD.fetch_add(1, Ordering::Relaxed);
		Ok((fd, 0))
	}

	/// Reads the contents of a directory.
	///
	/// This method retrieves the list of entries in a directory, converting them to
	/// FUSE directory entries that can be presented to the operating system.
	///
	/// # Arguments
	///
	/// * `_req` - Information about the request.
	/// * `path` - The path to the directory.
	/// * `_fh` - The file handle returned by `opendir`.
	///
	/// # Returns
	///
	/// Returns a `ResultReaddir` containing a vector of directory entries, or an error
	/// code if the operation failed.
	fn readdir(&self, _req: RequestInfo, path: &Path, _fh: u64) -> ResultReaddir {
		let path = path.to_str().ok_or(libc::ENOENT)?;
		log::info!("Reading directory: {path}");
		let mut repo = self.repository.write().map_err(|_| libc::EIO)?;
		let result = repo.lookup(path)?;
		if !result.is_directory() {
			log::error!("Path '{path}' is not a directory");
			return Err(libc::ENOENT);
		}
		match repo.list_directory(path) {
			Ok(entries) => Ok(entries
				.into_iter()
				.map(|dirent| FuseDirectoryEntry {
					kind: map_dirent_type_to_fs_kind(&dirent),
					name: OsString::from(dirent.name),
				})
				.collect()),
			Err(e) => {
				log::error!("Could not list directory {path}: {:?}", e);
				Err(e.into())
			}
		}
	}

	/// Releases a directory.
	///
	/// This method is called when a directory handle is closed. Since CernVM-FS doesn't
	/// need to track open directories specifically, this is a no-op.
	///
	/// # Arguments
	///
	/// * `_req` - Information about the request.
	/// * `_path` - The path to the directory.
	/// * `_fh` - The file handle.
	/// * `_flags` - The flags the directory was opened with.
	///
	/// # Returns
	///
	/// Returns `Ok(())` if successful, or an error code otherwise.
	fn releasedir(&self, _req: RequestInfo, _path: &Path, _fh: u64, _flags: u32) -> ResultEmpty {
		Ok(())
	}

	/// Retrieves filesystem statistics.
	///
	/// This method provides information about the filesystem, such as total size,
	/// available space, and number of files. Since CernVM-FS is a read-only filesystem,
	/// some of these values (like free space) are always zero.
	///
	/// # Arguments
	///
	/// * `_req` - Information about the request.
	/// * `_path` - The path for which to get statistics (usually ignored).
	///
	/// # Returns
	///
	/// Returns a `ResultStatfs` containing filesystem statistics, or an error code
	/// if the operation failed.
	fn statfs(&self, _req: RequestInfo, _path: &Path) -> ResultStatfs {
		if let Some((ts, cached)) = *self.cached_statfs.lock().map_err(|_| libc::EIO)? {
			#[allow(clippy::collapsible_if)]
			if ts.elapsed() < Duration::from_secs(5) {
				return Ok(cached);
			}
		}
		log::info!("Refreshing FS statistics");
		let mut repo = self.repository.write().map_err(|_| libc::EIO)?;
		let statistics = repo.get_statistics()?;
		let result = Statfs {
			blocks: 1 + statistics.file_size as u64 / 512,
			bfree: 0,
			bavail: 0,
			files: statistics.regular as u64,
			ffree: 0,
			bsize: 512,
			namelen: 255,
			frsize: 512,
		};
		drop(repo);
		if let Ok(mut cache) = self.cached_statfs.lock() {
			*cache = Some((Instant::now(), result));
		}
		Ok(result)
	}

	/// Retrieves an extended attribute for a file or directory.
	///
	/// This method is called when an extended attribute is requested. Since CernVM-FS
	/// doesn't currently support extended attributes, this always returns ENODATA.
	///
	/// # Arguments
	///
	/// * `_req` - Information about the request.
	/// * `_path` - The path to the file or directory.
	/// * `_name` - The name of the extended attribute.
	/// * `_size` - The size of the buffer for the attribute value.
	///
	/// # Returns
	///
	/// Returns a `ResultXattr` containing the attribute value, or an error code
	/// if the operation failed.
	fn getxattr(&self, _req: RequestInfo, _path: &Path, name: &OsStr, size: u32) -> ResultXattr {
		let name = name.to_str().ok_or(libc::ENODATA)?;
		let repo = self.repository.read().map_err(|_| libc::EIO)?;
		let value = match name {
			"user.fqrn" => repo.fqrn.clone(),
			"user.revision" => repo.manifest.revision.to_string(),
			"user.hash" => repo.manifest.root_catalog.clone(),
			"user.host" => repo.fetcher_source(),
			"user.expires" => repo.manifest.last_modified.to_rfc3339(),
			"user.nclg" => repo.opened_catalogs.len().to_string(),
			_ => return Err(libc::ENODATA),
		};
		let bytes = value.into_bytes();
		if size == 0 {
			return Ok(fuse_mt::Xattr::Size(bytes.len() as u32));
		}
		Ok(fuse_mt::Xattr::Data(bytes))
	}

	/// Checks access permissions for a file or directory.
	///
	/// This method checks whether the calling process has the specified access rights
	/// to the file or directory. In CernVM-FS, this mainly just checks if the path exists.
	///
	/// # Arguments
	///
	/// * `_req` - Information about the request.
	/// * `path` - The path to the file or directory.
	/// * `_mask` - The access rights to check.
	///
	/// # Returns
	///
	/// Returns `Ok(())` if access is allowed, or an error code otherwise.
	fn access(&self, _req: RequestInfo, path: &Path, _mask: u32) -> ResultEmpty {
		let path = path.to_str().ok_or(libc::ENOENT)?;
		log::info!("Accessing: {path}");
		let mut repo = self.repository.write().map_err(|_| libc::EIO)?;
		Ok(repo.lookup(path).map(|_| ())?)
	}
}

impl CernvmFileSystem {
	/// Creates a new CernvmFileSystem instance.
	///
	/// This constructor creates a new filesystem instance that operates on the given
	/// repository. It initializes the filesystem with an empty state and prepares it for
	/// mounting.
	///
	/// # Arguments
	///
	/// * `repository` - The CernVM-FS repository to expose through the filesystem.
	///
	/// # Returns
	///
	/// Returns a `CvmfsResult<Self>` containing the new filesystem instance, or an error
	/// if initialization failed.
	pub fn new(repository: Repository) -> CvmfsResult<Self> {
		Ok(Self {
			repository: RwLock::new(repository),
			opened_files: Default::default(),
			cached_statfs: Mutex::new(None),
		})
	}
}
