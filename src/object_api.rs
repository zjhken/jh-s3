use anyhow_ext::Context;
use anyhow_ext::{anyhow, Result};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{S3Client, S3Error};

impl S3Client {
	pub async fn list_object(&self, params: ListObjectParams) -> Result<ListBucketResult> {
		let mut resp = self
			.send(
				None,
				surf::http::Method::Get,
				Some(&params),
				None,
				None::<&str>,
			)
			.await
			.dot()?;
		let status_code = resp.status();
		let xml = resp
			.body_string()
			.await
			.map_err(|err| anyhow!(err.to_string()))
			.dot()?;
		trace!(xml);
		if status_code.is_success() {
			return Ok(serde_xml_rs::from_reader(xml.as_bytes()).dot()?);
		} else {
			let error: S3Error = serde_xml_rs::from_reader(xml.as_bytes()).dot()?;
			return Err(anyhow!("s3 error = {:?}", error));
		}
	}
}

// pub async fn put_object(self, )

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ListBucketResult {
	pub name: String,
	pub prefix: Option<String>,
	pub key_count: Option<u16>,
	pub max_keys: u16,
	pub delimiter: Option<String>,
	pub is_truncated: bool,
	pub next_continuation_token: Option<String>,
	pub server_side_encryption_enabled: Option<bool>,
	pub common_prefixes: Option<Vec<CommonPrefexes>>, // if there is no file with a prefix, then show this field to indicate user to use a longer prefix
	pub object_matches: Option<ObjectMatches>,        // if use metadata search, then show
	pub contents: Option<Vec<Content>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct CommonPrefexes {
	pub prefix: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMatches {
	pub object: Option<Vec<Object>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Content {
	pub object: Option<Vec<Object>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Object {
	pub object_name: Option<String>,
	pub object_id: Option<String>,
	pub version_id: Option<String>,
	pub query_mds: Option<Vec<QueryMds>>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryMds {
	r#type: QueryMetadataType,
	md_map: MdMap,
}
#[derive(Deserialize, Debug)]
pub enum QueryMetadataType {
	SYSMD,
	USERMD,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MdMap {
	entry: Vec<Entry>,
}

#[derive(Deserialize, Debug)]
pub struct Entry {
	key: String,
	value: String,
}

#[derive(Serialize, Default, Builder, Debug)]
#[builder(setter(into))]
// #[builder(pattern = "owned")]
#[builder(default)]
pub struct ListObjectParams {
	#[serde(rename(serialize = "list-type"))]
	#[builder(default = "2u8")]
	list_type: u8, // 2 means the V2 ListObjectsV2
	prefix: String,
	delimiter: String,
	#[serde(rename(serialize = "continuation-token"))]
	continuation_token: String,
	#[serde(rename(serialize = "max-keys"))]
	max_keys: String,
}

#[cfg(test)]
mod test {
	use anyhow_ext::Result;
	use tracing_test::traced_test;

	use crate::object_api::ListObjectParamsBuilder;

	#[test]
	#[traced_test]
	fn test_builder() -> Result<()> {
		let mut req = ListObjectParamsBuilder::default()
			// .prefix("/")
			.delimiter("/")
			.build()?;
		println!("{:?}", req);
		Ok(())
	}
}
