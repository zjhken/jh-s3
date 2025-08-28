use std::path::Path;

use anyhow_ext::Context;
use anyhow_ext::{Result, anyhow};
use std::path::PathBuf;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{S3Client, S3Error};

impl S3Client {
	pub async fn put_object<P>(&self, key: &str, path: P) -> Result<()>
	where
		P: AsRef<Path>,
	{
		let S3Client {
			endpoint,
			bucket,
			access_key,
			secret_key,
			..
		} = self;
		let file = async_std::fs::File::open(path.as_ref()).await.dot()?;
		let resp = self.send(
			Some(key),
			"PUT",
			None::<&u64>,
			None,
			Some(crate::S3Body::Path(PathBuf::from(path.as_ref()))),
		).await.dot();

		Ok(())
	}
}

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
