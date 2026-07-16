use std::borrow::Cow;
use std::collections::BTreeMap;

use anyhow_ext::Context;
use anyhow_ext::Result;
use anyhow_ext::anyhow;
use async_std::fs::File;
use async_std::io::ReadExt;
use async_std::path::Path;
use chrono::Utc;
use concat_string::concat_string;
use crypto::digest::Digest;
use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::sha2::Sha256;
use tracing::info;
use zjhttpc::requestx::Request;

use crate::S3Body;

const EMPTY_BODY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const REGION: &str = "us-east-1";
const SERVICE: &str = "s3";
const TERMINATOR: &str = "aws4_request";
const AWS_ISO8601_FORMAT: &str = "%Y%m%dT%H%M%SZ";
const AWS_AUTH_METHOD: &str = "AWS4-HMAC-SHA256";

pub async fn auth(
	access_key: &str,
	secret_key: &str,
	mut req: Request,
	// timestamp: Option<DateTime<Utc>>,
	timestamp: Option<String>,
	body: Option<S3Body>,
) -> Result<Request> {
	let full_url = req.url.to_string();
	info!(full_url);
	let timestamp = timestamp.unwrap_or_else(|| Utc::now().format(AWS_ISO8601_FORMAT).to_string());
	info!(timestamp);
	req = req.add_header("x-amz-date", &timestamp);

	let body_checksum = if let Some(body) = body {
		match body {
			S3Body::Bytes(data) => {
				req = req.set_body_slice(&data);
				Cow::from(cal_sha256_from_bytes(&data))
			}
			S3Body::Path(path_buf) => {
				req = req.set_body_file(&path_buf).await.dot()?;
				Cow::from(cal_sha256_from_file(path_buf).await.dot()?)
			}
			S3Body::Stream(reader, length) => {
				req = req.set_body_stream(reader, length);
				Cow::from(UNSIGNED_PAYLOAD)
			}
		}
	} else {
		Cow::from(EMPTY_BODY_SHA256)
	};
	req = req.add_header("x-amz-content-sha256", body_checksum.as_ref());
	let host = req.url.host_str().unwrap().to_owned(); // normally surf will insert Host for us but we need to put it into caculation
	req = req.add_header("host", host);
	let method = req.method.to_string();
	let sorted_query_str = req
		.url
		.query_pairs()
		.map(|(k, v)| {
			let mut k1 = String::new();
			url_escape::encode_component_to_string(&k, &mut k1);
			let mut v1 = String::new();
			url_escape::encode_www_form_urlencoded_to_string(&v, &mut v1);
			(k1, v1)
		})
		.collect::<BTreeMap<_, _>>()
		.into_iter()
		.map(|(k, v)| concat_string!(k, "=", v))
		.collect::<Vec<String>>()
		.join("&");
	let uri = req.url.path();
	// let headers = req
	// 	.header_names()
	// 	.map(|name| (name.as_str(), req.header(name).unwrap().as_str()))
	// 	.collect::<BTreeMap<_, _>>();
	let headers = req
		.headers
		.iter()
		.map(|(k, v)| (k, v.first().unwrap()))
		.collect::<BTreeMap<_, _>>();
	let canonical_headers = headers
		.into_iter()
		.filter(|(name, _)| name.to_lowercase() != "authorization")
		.map(|(name, value)| (name.to_lowercase(), value.trim()))
		.collect::<BTreeMap<String, &str>>();
	let canonical_headers_str = gen_canonical_headers_str(&canonical_headers);
	let signed_headers_str = gen_signed_headers_str(&canonical_headers);
	// TODO: what if there is body?
	let canonical_request = format!(
		"{method}\n{uri}\n{sorted_query_str}\n{canonical_headers_str}\n{signed_headers_str}\n{body_checksum}"
	);

	let short_date = timestamp
		.split('T')
		.take(1)
		.next()
		.ok_or(anyhow!("split T failed"))?;
	let scope = format!("{short_date}/{REGION}/{SERVICE}/{TERMINATOR}");

	let mut sha256_hasher = Sha256::new();
	sha256_hasher.input(canonical_request.as_bytes());
	let canonical_request_hash = sha256_hasher.result_str();
	let str_to_sign = format!("{AWS_AUTH_METHOD}\n{timestamp}\n{scope}\n{canonical_request_hash}");
	let sign_key = gen_sign_key(short_date, secret_key, sha256_hasher);
	let signed_bytes = gen_hmac_sha256(&sign_key, &str_to_sign, sha256_hasher);
	let signed_hex = hex::encode(signed_bytes);

	let auth_str = format!(
		"{AWS_AUTH_METHOD} Credential={access_key}/{short_date}/{REGION}/{SERVICE}/{TERMINATOR}, SignedHeaders={signed_headers_str}, Signature={signed_hex}"
	);
	info!(auth_str);
	req = req.add_header("Authorization", auth_str);
	// req.remove_header("host"); // to avoid duplicate host headers
	return Ok(req);
}

fn cal_sha256_from_bytes<B: AsRef<[u8]>>(bytes: B) -> String {
	let mut sha256_hasher = Sha256::new();
	sha256_hasher.input(bytes.as_ref());
	sha256_hasher.result_str()
}

async fn cal_sha256_from_file<P: AsRef<Path>>(p: P) -> Result<String> {
	let mut file = File::open(p).await.dot()?;
	let mut sha256_hasher = Sha256::new();
	let mut buf = vec![0u8; 1024 * 1024];
	loop {
		let n = file.read(&mut buf).await.dot()?;
		if n == 0 {
			break;
		}
		sha256_hasher.input(&buf[..n]);
	}
	Ok(sha256_hasher.result_str())
}

fn gen_sign_key(short_date: &str, secret_key: &str, sha256_hasher: Sha256) -> Vec<u8> {
	let date_key = gen_hmac_sha256(
		concat_string!("AWS4", secret_key).as_bytes(),
		short_date,
		sha256_hasher,
	);
	let date_region_key = gen_hmac_sha256(&date_key, REGION, sha256_hasher);
	let date_region_service_key = gen_hmac_sha256(&date_region_key, SERVICE, sha256_hasher);
	return gen_hmac_sha256(&date_region_service_key, TERMINATOR, sha256_hasher);
}

fn gen_hmac_sha256(key: &[u8], data: &str, mut sha256_hasher: Sha256) -> Vec<u8> {
	sha256_hasher.reset();
	let mut hmac_sha256_hasher = Hmac::new(sha256_hasher, key);
	hmac_sha256_hasher.input(data.as_bytes());
	return hmac_sha256_hasher.result().code().to_owned();
}

fn gen_canonical_headers_str(canonical_headers: &BTreeMap<String, &str>) -> String {
	canonical_headers
		.into_iter()
		.map(|(name, value)| format!("{name}:{value}\n"))
		.fold(String::new(), |a, b| a + &b)
}

fn gen_signed_headers_str(canonical_headers: &BTreeMap<String, &str>) -> String {
	canonical_headers
		.into_iter()
		.map(|(name, _)| name.as_str())
		.collect::<Vec<&str>>()
		.join(";")
}

#[cfg(test)]
mod tests {

	use super::*;
	use async_std::fs::{File, write};
	use async_std::task;
	use tempfile::tempdir;
	use zjhttpc::requestx::Request;
	use zjhttpc::url::Url;

	#[test]
	fn test_auth_basic_request() -> Result<()> {
		let access_key = "AKIAIOSFODNN7EXAMPLE";
		let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
		let timestamp = "20230615T123456Z".to_string();

		let url = Url::parse("https://test-bucket.s3.amazonaws.com/test.txt")?;
		let mut req = Request::new("GET", url).unwrap();

		let signed_req = task::block_on(async {
			auth(access_key, secret_key, req, Some(timestamp), None)
				.await
				.dot()
				.unwrap()
		});

		assert_eq!(
			signed_req
				.headers
				.get("x-amz-date")
				.unwrap()
				.first()
				.unwrap()
				.as_str(),
			"20230615T123456Z"
		);
		assert_eq!(
			signed_req
				.headers
				.get("x-amz-content-sha256")
				.unwrap()
				.first()
				.unwrap()
				.as_str(),
			EMPTY_BODY_SHA256
		);
		// println!("{:?}", signed_req.headers.get("Authorization").unwrap().first().unwrap());
		assert_eq!(
			signed_req
				.headers
				.get("Authorization")
				.unwrap()
				.first()
				.unwrap()
				.as_str(),
			"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=c16688c87adce6773bcc42d4eb56e4c7d67079cb9eeb1d4f55771cd5593c93a2"
		);
		assert!(
			signed_req
				.headers
				.get("host")
				.unwrap()
				.first()
				.unwrap()
				.as_str() == "test-bucket.s3.amazonaws.com"
		);

		Ok(())
	}

	#[test]
	fn test_cal_sha256_in_stream() -> Result<()> {
		task::block_on(async {
			// let dir = tempdir()?;
			// let file_path = dir.path().join("test.txt");
			// let test_content = "Hello, world!";
			// write(&file_path, test_content).await?;

			let hash = cal_sha256_from_file("Cargo.toml").await?;

			// Pre-calculated SHA256 hash of "Hello, world!"
			assert_eq!(
				hash,
				"53be68e5a8a9e15c4b7b91a727aa4da3045f0033a8958f89d4a4d728b76aeda4"
			);

			Ok(())
		})
	}

	#[test]
	fn test_auth_stream_unsigned_payload() -> Result<()> {
		let access_key = "AKIAIOSFODNN7EXAMPLE";
		let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
		let timestamp = "20230615T123456Z".to_string();

		let url = Url::parse("https://test-bucket.s3.amazonaws.com/test.bin")?;
		let req = Request::new("PUT", url)?;

		let body =
			crate::S3Body::Stream(Box::new(async_std::io::Cursor::new(b"hello".to_vec())), 5);

		let signed_req = task::block_on(async {
			auth(access_key, secret_key, req, Some(timestamp), Some(body))
				.await
				.dot()
				.unwrap()
		});

		assert_eq!(
			signed_req
				.headers
				.get("x-amz-content-sha256")
				.unwrap()
				.first()
				.unwrap()
				.as_str(),
			"UNSIGNED-PAYLOAD"
		);
		let auth_header = signed_req
			.headers
			.get("Authorization")
			.unwrap()
			.first()
			.unwrap()
			.as_str()
			.to_owned();
		assert!(auth_header.starts_with(
			"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230615/us-east-1/s3/aws4_request"
		));
		// user-agent 也被 zjhttpc 自动加入 SignedHeaders，只断言关键三个存在。
		assert!(auth_header.contains("host"));
		assert!(auth_header.contains("x-amz-content-sha256"));
		assert!(auth_header.contains("x-amz-date"));
		assert!(auth_header.contains("Signature="));

		Ok(())
	}

	#[test]
	fn test_auth_bytes_signed_payload() -> Result<()> {
		let access_key = "AKIAIOSFODNN7EXAMPLE";
		let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
		let timestamp = "20230615T123456Z".to_string();

		let url = Url::parse("https://test-bucket.s3.amazonaws.com/test.bin")?;
		let req = Request::new("PUT", url)?;

		let payload = b"<CompleteMultipartUpload></CompleteMultipartUpload>".to_vec();
		let body = crate::S3Body::Bytes(payload.clone());

		let signed_req = task::block_on(async {
			auth(access_key, secret_key, req, Some(timestamp), Some(body))
				.await
				.dot()
				.unwrap()
		});

		// x-amz-content-sha256 should be the actual SHA256 of the payload, not UNSIGNED-PAYLOAD.
		let expected_sha = cal_sha256_from_bytes(&payload);
		assert_ne!(expected_sha, "UNSIGNED-PAYLOAD");
		assert_eq!(
			signed_req
				.headers
				.get("x-amz-content-sha256")
				.unwrap()
				.first()
				.unwrap()
				.as_str(),
			expected_sha
		);

		Ok(())
	}
}
