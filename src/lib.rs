mod aws_sig_v4;
// mod aws_sig_v4_surf;
pub mod bucket;
pub mod object;

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow_ext::Context;
use anyhow_ext::Result;
use anyhow_ext::anyhow;
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;
use zjhttpc::client::ZJHttpClient;
use zjhttpc::requestx::Request;
use zjhttpc::response::Response;
use zjhttpc::url::Url;

#[derive(Debug)]
pub struct S3Client {
	pub endpoint: String,
	pub bucket: String,
	pub access_key: String,
	pub secret_key: String,
	pub httpc: ZJHttpClient,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct S3Config {
	pub endpoint: String,
	pub bucket: String,
	pub access_key: String,
	pub secret_key: String,
	pub trust_cert_path: Option<String>,
}

impl S3Client {
	pub fn new(
		endpoint: String,
		bucket: String,
		access_key: String,
		secret_key: String,
		trust_cert_path: Option<String>,
	) -> Result<Self> {
		let mut builder = ZJHttpClient::builder();
		if let Some(cert_path) = trust_cert_path {
			builder.set_global_trust_store_pem(zjhttpc::misc::TrustStorePem::Path(PathBuf::from(
				cert_path,
			)));
		}
		let httpc = builder.build().map_err(|e| anyhow!(e.to_string()))?;
		Ok(S3Client {
			endpoint,
			bucket,
			access_key,
			secret_key,
			httpc,
		})
	}
	pub fn from_toml_config<P>(path: P) -> Result<Self>
	where
		P: AsRef<Path>,
	{
		let txt = fs::read_to_string(path)?;
		let c: S3Config = toml::from_str(&txt)?;
		Self::new(
			c.endpoint,
			c.bucket,
			c.access_key,
			c.secret_key,
			c.trust_cert_path,
		)
	}
	pub async fn send(
		&self,
		path: Option<&str>,
		method: &'static str,
		queries: Option<&impl Serialize>,
		headers: Option<HashMap<String, String>>,
		body: Option<S3Body>,
	) -> Result<Response> {
		let mut url = format!("{}/{}", self.endpoint, self.bucket);
		if let Some(p) = path {
			url.push_str("/");
			url.push_str(p);
		}
		let url = Url::parse(&url)?;
		let mut req = Request::new(method, url).dot()?;
		if let Some(headers) = headers {
			req = req.set_headers_nondup(headers);
		}
		if let Some(queries) = queries {
			req = req.set_queries_serde(queries).dot()?;
		}
		req = crate::aws_sig_v4::auth(&self.access_key, &self.secret_key, req, None, body)
			.await
			.dot()?;
		let resp = self
			.httpc
			.send(&mut req)
			.await
			.map_err(|err| anyhow!(err.to_string()))?;
		Ok(resp)
	}

	// pub async fn send_via_surf(
	// 	&self,
	// 	path: Option<&str>,
	// 	method: surf::http::Method,
	// 	queries: Option<&impl Serialize>,
	// 	headers: Option<HashMap<String, String>>,
	// 	body: Option<impl Into<surf::Body>>,
	// ) -> Result<surf::Response> {
	// 	let mut url = format!("{}/{}", self.endpoint, self.bucket);
	// 	if let Some(p) = path {
	// 		url.push_str("/");
	// 		url.push_str(p);
	// 	}
	// 	let url = Url::parse(&url)?;
	// 	let mut builder = surf::RequestBuilder::new(method, url)
	// 		.query(&queries)
	// 		.map_err(|err| anyhow!(err.to_string()))?;
	// 	if let Some(map) = headers {
	// 		for (key, value) in map.into_iter() {
	// 			let key = surf::http::headers::HeaderName::from_bytes(key.into_bytes()).unwrap();
	// 			builder = builder.header(&key, value);
	// 		}
	// 	}
	// 	if let Some(body) = body {
	// 		builder = builder.body(body);
	// 	}
	// 	let req = builder.build();
	// 	let req = crate::aws_sig_v4_surf::auth(&self.access_key, &self.secret_key, req, None)?;
	// 	let surf_config = surf::Config::new();
	// 	let http_client: surf::Client = surf_config.try_into().dot()?;
	// 	let resp = http_client
	// 		.send(req)
	// 		.await
	// 		.map_err(|err| anyhow!(err.to_string()))?;
	// 	Ok(resp)
	// }
}

pub enum S3Body {
	Bytes(Vec<u8>),
	Path(PathBuf),
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct S3Error {
	code: String,
	message: String,
	resource: Option<String>,
	request_id: Option<String>,
}

#[cfg(test)]
mod test {
	use crate::S3Client;
	use anyhow_ext::Result;

	#[test]
	fn test_s3_client() {
		// let s3client = S3Client::new(endpoint, bucket, access_key, secret_key, trust_cert_path)
	}
}
