use crate::object::ListBucketResult;

use std::path::Path;

use anyhow_ext::Context;
use anyhow_ext::{Result, anyhow};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use tracing::trace;
use zjhttpc::misc::Body;

use crate::{S3Client, S3Error};

impl S3Client {
	pub async fn list_bucket(&self, params: ListBucketParams) -> Result<ListBucketResult> {
		let mut resp = self
			.send(
				None,
				"GET",
				// surf::http::Method::Get,
				Some(&params),
				None,
				None, // TODO: streaming upload
				// None::<String>,
			)
			.await
			.dot()?;
		let xml = resp
			.body_string()
			.await
			.map_err(|err| anyhow!(err.to_string()))
			.dot()?;
		trace!(xml);
		if resp.is_success() {
			return Ok(serde_xml_rs::from_reader(xml.as_bytes()).dot()?);
		} else {
			let error: S3Error = serde_xml_rs::from_reader(xml.as_bytes()).dot()?;
			return Err(anyhow!("s3 error = {:?}", error));
		}
	}
}

#[derive(Serialize, Default, Builder, Debug)]
#[builder(setter(into))]
// #[builder(pattern = "owned")]
#[builder(default)]
pub struct ListBucketParams {
	#[serde(rename(serialize = "list-type"))]
	#[builder(default = "2u8")]
	list_type: u8, // 2 means the V2 ListObjectsV2
	prefix: Option<String>,
	delimiter: Option<String>,
	#[serde(rename(serialize = "continuation-token"))]
	continuation_token: Option<String>,
	#[serde(rename(serialize = "max-keys"))]
	max_keys: Option<String>,
}

#[cfg(test)]
mod test {
	use async_std::{fs::File, task};

	use anyhow_ext::Result;
	use async_std::io::BufReader;
	use tracing::info;
	use tracing_test::traced_test;

	use crate::{S3Client, bucket::ListBucketParamsBuilder};

	#[test]
	#[traced_test]
	fn test_builder() -> Result<()> {
		let req = ListBucketParamsBuilder::default()
			.prefix(Some("/".to_owned()))
			.delimiter(Some("/".to_owned()))
			.build()?;
		println!("{:?}", req);
		Ok(())
	}

	#[test]
	#[tracing_test::traced_test]
	fn test_list_bucket() -> Result<()> {
		let s3 = S3Client::from_toml_config("config.toml")?;
		task::block_on(async {
			let x = s3
				.list_bucket(
					ListBucketParamsBuilder::default()
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
