//! # CVMFS Client Implementation
//!
//! This is the main entry point for the CVMFS client, which provides access to
//! CernVM-FS repositories through a FUSE mount point. The client allows mounting
//! remote CVMFS repositories as local filesystems.
//!
//! ## Usage
//!
//! ```bash
//! cvmfs-cli <repository_url> <mount_point> [cache_directory]
//! ```
//!
//! ### Arguments
//!
//! * `repository_url` - URL of the CVMFS repository (e.g., "http://cvmfs-stratum-one.cern.ch/opt/boss")
//! * `mount_point` - Local directory where the repository will be mounted
//! * `cache_directory` - (Optional) Directory for storing cached data (defaults to "/tmp/cvmfs")
//!
//! ### Example
//!
//! ```bash
//! cvmfs-cli http://cvmfs-stratum-one.cern.ch/opt/boss /mnt/cvmfs /var/cache/cvmfs
//! ```

use std::{env, path::PathBuf};

use cvmfs::{fetcher::Fetcher, file_system::CernvmFileSystem, repository::Repository};

/// Main entry point for the CVMFS client application.
///
/// This function:
/// 1. Initializes logging system.
/// 2. Parses command line arguments.
/// 3. Creates and configures CVMFS client components.
/// 4. Mounts the repository using FUSE.
///
/// # Panics
///
/// Will panic if:
/// - Required arguments are missing.
/// - Mount point doesn't exist or isn't a directory.
/// - Any component initialization fails.
/// - FUSE mount operation fails.
fn main() {
	env_logger::init();
	let args: Vec<String> = env::args().collect();
	if args.len() < 3 {
		panic!("Please specify url of the repository and the mount point");
	}
	let repo_url = &args[1];
	let mountpoint = PathBuf::from(&args[2]);
	if !mountpoint.exists() {
		panic!("Mount point does not exist");
	}
	if !mountpoint.is_dir() {
		panic!("Mount point is not a directory");
	}
	let repo_cache = if args.len() > 3 { args[3].clone() } else { "/tmp/cvmfs".into() };
	let fetcher = Fetcher::new(repo_url, &repo_cache, true).expect("Failure creating the fetcher");
	let repository = Repository::new(fetcher).expect("Failure creating the repository");
	let file_system = CernvmFileSystem::new(repository).expect("Failure creating the file system");

	file_system
		.mount(mountpoint.to_str().expect("Invalid mount point string"))
		.expect("Could not mount the file system");
}
