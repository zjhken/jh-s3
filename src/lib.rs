mod aws_sig_v4;
// mod aws_sig_v4_surf;
pub mod bucket;
pub mod error;
pub mod multipart;
pub mod object;

pub use error::{Error, Result};

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::PathBuf;

use error::{ConfigSnafu, HttpClientBuilderSnafu, HttpSnafu, IoSnafu};
use serde::Deserialize;
use serde::Serialize;
use snafu::prelude::*;
use std::path::Path;
use zjhttpc::client::ZJHttpClient;
use zjhttpc::requestx::Request;
use zjhttpc::response::Response;

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
		let httpc = builder.build().context(HttpClientBuilderSnafu)?;
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
		let txt = fs::read_to_string(path).context(IoSnafu)?;
		let c: S3Config = toml::from_str(&txt).context(ConfigSnafu)?;
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
		let mut req = Request::new(method, &url).context(HttpSnafu)?;
		if let Some(headers) = headers {
			req = req.set_headers_nondup(headers);
		}
		if let Some(queries) = queries {
			req = req.set_queries_serde(queries).context(HttpSnafu)?;
		}
		req = crate::aws_sig_v4::auth(&self.access_key, &self.secret_key, req, None, body).await?;
		let resp = self.httpc.send(&mut req).await.context(HttpSnafu)?;
		Ok(resp)
	}
}

pub enum S3Body {
	Bytes(Vec<u8>),
	Path(PathBuf),
	/// 已知长度的流式 body。签名走 UNSIGNED-PAYLOAD（无法预计算 SHA256）。
	Stream(Box<dyn async_std::io::Read + Unpin + Send + Sync>, u64),
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct S3Error {
	pub code: String,
	pub message: String,
	pub resource: Option<String>,
	pub request_id: Option<String>,
}

impl fmt::Display for S3Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "code={} message=\"{}\"", self.code, self.message)?;
		if let Some(r) = self.resource.as_ref() {
			write!(f, " resource=\"{}\"", r)?;
		}
		if let Some(r) = self.request_id.as_ref() {
			write!(f, " request_id=\"{}\"", r)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod test {
	#[test]
	fn test_s3_client() {
		// let s3client = S3Client::new(endpoint, bucket, access_key, secret_key, trust_cert_path)
	}
}
