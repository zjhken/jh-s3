mod aws_sig_v4;
pub mod bucket;
pub mod object_api;

use std::collections::HashMap;
use std::fs;

use anyhow_ext::anyhow;
use anyhow_ext::Result;
use derive_builder::Builder;
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;
use surf::http::headers::HeaderName;
use surf::{http::Method, Body, Response, Url};
use tracing::info;

#[derive(Serialize, Default, Builder, Debug)]
#[builder(setter(into))]
// #[builder(pattern = "owned")]
#[builder(default)]
pub struct S3Client {
	pub endpoint: String,
	pub bucket: String,
	pub access_key: String,
	pub secret_key: String,
	#[serde(skip)]
	pub http_client: surf::Client,
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
	) -> Self {
		let http_client: surf::Client = match trust_cert_path {
			Some(path) => {
				todo!()
			}
			None => {
				let surf_config = surf::Config::new();
				surf_config.try_into().unwrap()
			}
		};
		return S3Client {
			endpoint,
			bucket,
			access_key,
			secret_key,
			http_client,
		};
	}
	pub fn from_toml_config<P>(path: P) -> Result<Self>
	where
		P: AsRef<Path>,
	{
		let txt = fs::read_to_string(path)?;
		let c: S3Config = toml::from_str(&txt)?;
		return Ok(Self::new(
			c.endpoint,
			c.bucket,
			c.access_key,
			c.secret_key,
			c.trust_cert_path,
		));
	}
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
		let req = crate::aws_sig_v4::auth(
			&self.access_key,
			&self.secret_key,
			req,
			Some("20240927T011346Z".to_string()),
		)?;
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
	request_id: Option<String>,
}

#[cfg(test)]
mod test {
	use crate::{
		object_api::{ListObjectParams, ListObjectParamsBuilder},
		S3Client,
	};
	use anyhow_ext::Result;
	use async_std::task;
	use tracing::info;

	#[test]
	#[tracing_test::traced_test]
	fn test_list_object() -> Result<()> {
		let s3 = S3Client::from_toml_config("config.toml")?;
		task::block_on(async {
			let x = s3
				.list_object(
					ListObjectParamsBuilder::default()
						.prefix(Some("/".to_owned()))
						.delimiter(Some("/".to_owned()))
						.build()
						.unwrap(),
				)
				.await
				.unwrap();
			info!(?x);
		});
		return Ok(());
	}
}
