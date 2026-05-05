use std::process::Command;

use crate::common::{CvmfsError, CvmfsResult};

const CVMFS_SRV_PREFIX: &str = "_cvmfs._tcp";

pub fn discover_servers(repo_fqrn: &str) -> CvmfsResult<Vec<String>> {
	let domain = extract_domain(repo_fqrn)
		.ok_or_else(|| CvmfsError::Generic("cannot extract domain from FQRN".into()))?;
	discover_servers_for_domain(&domain)
}

pub fn discover_servers_for_domain(domain: &str) -> CvmfsResult<Vec<String>> {
	let query_name = format!("{CVMFS_SRV_PREFIX}.{domain}");
	lookup_txt_records(&query_name)
}

fn lookup_txt_records(query_name: &str) -> CvmfsResult<Vec<String>> {
	let output = Command::new("dig")
		.args(["+short", "TXT", query_name])
		.output()
		.map_err(|e| CvmfsError::IO(format!("failed to run dig: {e}")))?;

	if !output.status.success() {
		return Err(CvmfsError::IO(format!("dig failed with status {}", output.status)));
	}

	let stdout = String::from_utf8_lossy(&output.stdout);
	Ok(parse_dig_output(&stdout))
}

fn parse_dig_output(stdout: &str) -> Vec<String> {
	stdout
		.lines()
		.map(|line| line.trim().trim_matches('"').trim().to_string())
		.filter(|s| !s.is_empty())
		.flat_map(|record| {
			record
				.split(';')
				.map(|s| s.trim().to_string())
				.filter(|s| !s.is_empty())
				.collect::<Vec<_>>()
		})
		.collect()
}

pub fn extract_domain(fqrn: &str) -> Option<String> {
	let parts: Vec<&str> = fqrn.splitn(2, '.').collect();
	if parts.len() == 2 && !parts[1].is_empty() { Some(parts[1].to_string()) } else { None }
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn extract_domain_from_fqrn() {
		assert_eq!(extract_domain("atlas.cern.ch"), Some("cern.ch".to_string()));
	}

	#[test]
	fn extract_domain_simple() {
		assert_eq!(extract_domain("repo.example.com"), Some("example.com".to_string()));
	}

	#[test]
	fn extract_domain_no_dots() {
		assert_eq!(extract_domain("localhost"), None);
	}

	#[test]
	fn extract_domain_trailing_dot_empty() {
		assert_eq!(extract_domain("repo."), None);
	}

	#[test]
	fn discover_servers_empty_fqrn_errors() {
		let result = discover_servers("");
		assert!(result.is_err());
	}

	#[test]
	fn discover_servers_no_dot_errors() {
		let result = discover_servers("nodomain");
		assert!(result.is_err());
	}

	#[test]
	fn lookup_nonexistent_returns_empty() {
		let result = lookup_txt_records("_cvmfs._tcp.nonexistent.invalid.test");
		if let Ok(servers) = result {
			assert!(servers.is_empty());
		}
	}

	#[test]
	fn extract_domain_multiple_dots() {
		assert_eq!(extract_domain("a.b.c.d"), Some("b.c.d".to_string()));
	}

	#[test]
	fn extract_domain_two_parts() {
		assert_eq!(extract_domain("repo.org"), Some("org".to_string()));
	}

	#[test]
	fn discover_servers_for_valid_domain() {
		let result = discover_servers_for_domain("cern.ch");
		assert!(result.is_ok());
	}

	#[test]
	fn discover_servers_valid_fqrn() {
		let result = discover_servers("atlas.cern.ch");
		assert!(result.is_ok());
	}

	#[test]
	fn parse_dig_output_empty() {
		assert!(parse_dig_output("").is_empty());
	}

	#[test]
	fn parse_dig_output_single_record() {
		let out = parse_dig_output("\"http://stratum1.example.com/cvmfs\"\n");
		assert_eq!(out, vec!["http://stratum1.example.com/cvmfs"]);
	}

	#[test]
	fn parse_dig_output_multiple_lines() {
		let out = parse_dig_output("\"http://a.example.com\"\n\"http://b.example.com\"\n");
		assert_eq!(out, vec!["http://a.example.com", "http://b.example.com"]);
	}

	#[test]
	fn parse_dig_output_semicolon_split() {
		let out = parse_dig_output("\"http://a.example.com;http://b.example.com\"\n");
		assert_eq!(out, vec!["http://a.example.com", "http://b.example.com"]);
	}

	#[test]
	fn parse_dig_output_strips_quotes_and_whitespace() {
		let out = parse_dig_output("  \"  http://a.example.com  \"  \n");
		assert_eq!(out, vec!["http://a.example.com"]);
	}

	#[test]
	fn parse_dig_output_skips_empty_segments() {
		let out = parse_dig_output("\";http://a.example.com;;http://b.example.com;\"\n");
		assert_eq!(out, vec!["http://a.example.com", "http://b.example.com"]);
	}

	#[test]
	fn parse_dig_output_skips_blank_lines() {
		let out = parse_dig_output("\n\n\"http://only.example.com\"\n\n");
		assert_eq!(out, vec!["http://only.example.com"]);
	}
}
