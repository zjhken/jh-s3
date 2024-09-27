use std::collections::BTreeMap;

use anyhow_ext::anyhow;
use anyhow_ext::Result;
use chrono::DateTime;
use chrono::Utc;
use concat_string::concat_string;
use crypto::digest::Digest;
use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::sha2::Sha256;
use surf::Request;
use tracing::info;

const EMPTY_BODY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const REGION: &str = "us-east-1";
const SERVICE: &str = "s3";
const TERMINATOR: &str = "aws4_request";
const AWS_ISO8601_FORMAT: &str = "%Y%m%dT%H%M%SZ";
const AWS_AUTH_METHOD: &str = "AWS4-HMAC-SHA256";

pub fn auth(
	access_key: &str,
	secret_key: &str,
	mut req: Request,
	// timestamp: Option<DateTime<Utc>>,
	timestamp: Option<String>,
) -> Result<Request> {
	let full_url = req.url().to_string();
	info!(full_url);
	let timestamp = timestamp.unwrap_or_else(|| Utc::now().format(AWS_ISO8601_FORMAT).to_string());
	info!(timestamp);
	req.insert_header("x-amz-date", &timestamp);
	// TODO: what if there is body?
	req.insert_header("x-amz-content-sha256", EMPTY_BODY_SHA256);
	let host = req.url().host_str().unwrap().to_owned(); // normally surf will insert Host for us but we need to put it into caculation
	req.insert_header("host", host);
	let method = req.method().to_string();
	let sorted_query_str = req
		.url()
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
	let uri = req.url().path();
	let headers = req
		.header_names()
		.map(|name| (name.as_str(), req.header(name).unwrap().as_str()))
		.collect::<BTreeMap<_, _>>();
	let canonical_headers = headers
		.into_iter()
		.filter(|(name, _)| name.to_lowercase() != "authorization")
		.map(|(name, value)| (name.to_lowercase(), value.trim()))
		.collect::<BTreeMap<String, &str>>();
	let canonical_headers_str = gen_canonical_headers_str(&canonical_headers);
	let signed_headers_str = gen_signed_headers_str(&canonical_headers);
	// TODO: what if there is body?
	let canonical_request = format!("{method}\n{uri}\n{sorted_query_str}\n{canonical_headers_str}\n{signed_headers_str}\n{EMPTY_BODY_SHA256}");

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

	let auth_str = format!("{AWS_AUTH_METHOD} Credential={access_key}/{short_date}/{REGION}/{SERVICE}/{TERMINATOR}, SignedHeaders={signed_headers_str}, Signature={signed_hex}");
	info!(auth_str);
	req.insert_header("Authorization", auth_str);
	// req.remove_header("host"); // to avoid duplicate host headers
	return Ok(req);
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
