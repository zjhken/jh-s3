mod aws_sig_v4;
pub mod bucket;
pub mod object_api;

use std::collections::HashMap;

use anyhow_ext::anyhow;
use anyhow_ext::Result;
use derive_builder::Builder;
use serde::Deserialize;
use serde::Serialize;
use surf::http::headers::HeaderName;
use surf::{http::Method, Body, Response, Url};

#[derive(Serialize, Default, Builder, Debug)]
#[builder(setter(into))]
// #[builder(pattern = "owned")]
#[builder(default)]
pub struct S3Client {
	pub endpoint: String,
	pub bucket: String,
	pub access_key: String,
	pub secret_key: String,
	// pub trust_cert_path: String,
	#[serde(skip)]
	pub http_client: surf::Client,
}

impl S3Client {
	pub async fn send(
		&self,
		path: Option<&str>,
		method: Method,
		queries: Option<&impl Serialize>,
		headers: Option<HashMap<String, String>>,
		body: Option<impl Into<Body>>,
	) -> Result<Response> {
		let mut url = format!("{}/{}", self.endpoint, self.bucket);
		if let Some(p) = path {
			url.push_str("/");
			url.push_str(p);
		}
		let url = Url::parse(&url)?;
		let mut builder = surf::RequestBuilder::new(method, url)
			.query(&queries)
			.map_err(|err| anyhow!(err.to_string()))?;
		if let Some(map) = headers {
			for (key, value) in map.into_iter() {
				let key = HeaderName::from_bytes(key.into_bytes()).unwrap();
				builder = builder.header(&key, value);
			}
		}
		if let Some(body) = body {
			builder = builder.body(body);
		}
		let req = builder.build();
		let req = crate::aws_sig_v4::auth(&self.access_key, &self.secret_key, req, None)?;
		let resp = self
			.http_client
			.send(req)
			.await
			.map_err(|err| anyhow!(err.to_string()))?;
		Ok(resp)
	}
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct S3Error {
	code: String,
	message: String,
	resource: Option<String>,
	request_id: String,
}
