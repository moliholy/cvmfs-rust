use std::time::Duration;

use reqwest::blocking::Client;

use crate::common::CvmfsResult;

const GEO_API_PATH: &str = "api/v1.0/geo";
const GEO_TIMEOUT: Duration = Duration::from_secs(5);

pub fn sort_servers_by_geo(
	geo_api_server: &str,
	repo_name: &str,
	servers: &[String],
) -> CvmfsResult<Vec<String>> {
	if servers.len() <= 1 {
		return Ok(servers.to_vec());
	}

	let host_list: Vec<&str> = servers.iter().map(|s| extract_hostname(s)).collect();
	let csv = host_list.join(",");

	let url = format!(
		"{}/{}/{}/me/{}",
		geo_api_server.trim_end_matches('/'),
		GEO_API_PATH,
		repo_name,
		csv
	);

	let client = Client::builder().timeout(GEO_TIMEOUT).build()?;
	let response = client.get(&url).send()?;

	if !response.status().is_success() {
		return Ok(servers.to_vec());
	}

	let body = response.text()?;
	let ordering = parse_geo_response(&body, servers.len());

	let mut sorted: Vec<String> = Vec::with_capacity(servers.len());
	for idx in &ordering {
		if *idx < servers.len() {
			sorted.push(servers[*idx].clone());
		}
	}
	for (i, server) in servers.iter().enumerate() {
		if !ordering.contains(&i) {
			sorted.push(server.clone());
		}
	}

	Ok(sorted)
}

fn extract_hostname(url: &str) -> &str {
	let without_scheme = url
		.strip_prefix("http://")
		.or_else(|| url.strip_prefix("https://"))
		.or_else(|| url.strip_prefix("file://"))
		.unwrap_or(url);
	without_scheme.split('/').next().unwrap_or(without_scheme)
}

fn parse_geo_response(body: &str, server_count: usize) -> Vec<usize> {
	body.lines()
		.next()
		.unwrap_or("")
		.split(',')
		.filter_map(|s| s.trim().parse::<usize>().ok())
		.filter(|&idx| idx > 0 && idx <= server_count)
		.map(|idx| idx - 1)
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn extract_hostname_http() {
		assert_eq!(extract_hostname("http://example.com/cvmfs/repo"), "example.com");
	}

	#[test]
	fn extract_hostname_https() {
		assert_eq!(extract_hostname("https://cvmfs.cern.ch/cvmfs/atlas"), "cvmfs.cern.ch");
	}

	#[test]
	fn extract_hostname_no_scheme() {
		assert_eq!(extract_hostname("example.com/path"), "example.com");
	}

	#[test]
	fn extract_hostname_bare() {
		assert_eq!(extract_hostname("localhost"), "localhost");
	}

	#[test]
	fn parse_geo_response_valid() {
		let ordering = parse_geo_response("3,1,2\n", 3);
		assert_eq!(ordering, vec![2, 0, 1]);
	}

	#[test]
	fn parse_geo_response_empty() {
		let ordering = parse_geo_response("", 3);
		assert!(ordering.is_empty());
	}

	#[test]
	fn parse_geo_response_partial() {
		let ordering = parse_geo_response("2,1\n", 3);
		assert_eq!(ordering, vec![1, 0]);
	}

	#[test]
	fn parse_geo_response_out_of_range() {
		let ordering = parse_geo_response("1,5,2\n", 3);
		assert_eq!(ordering, vec![0, 1]);
	}

	#[test]
	fn parse_geo_response_zero_index_skipped() {
		let ordering = parse_geo_response("0,1,2\n", 3);
		assert_eq!(ordering, vec![0, 1]);
	}

	#[test]
	fn sort_single_server_noop() {
		let servers = vec!["http://example.com".to_string()];
		let result = sort_servers_by_geo("http://geo.api", "repo", &servers).unwrap();
		assert_eq!(result, servers);
	}

	#[test]
	fn sort_empty_servers_noop() {
		let servers: Vec<String> = vec![];
		let result = sort_servers_by_geo("http://geo.api", "repo", &servers).unwrap();
		assert!(result.is_empty());
	}
}
