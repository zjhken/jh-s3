use std::path::Path;

use anyhow_ext::Context;
use anyhow_ext::Result;
use std::path::PathBuf;
use tracing::info;
use zjhttpc::response::Response;

use crate::S3Client;

impl S3Client {
	pub async fn put_object<P>(&self, key: &str, path: P) -> Result<()>
	where
		P: AsRef<Path>,
	{
		let mut resp = self
			.send(
				Some(key),
				"PUT",
				None::<&u64>,
				None,
				Some(crate::S3Body::Path(PathBuf::from(path.as_ref()))),
			)
			.await
			.dot()?;

		info!(resp.status_code);
		let body = resp.body_string().await.dot()?;
		info!(body);
		Ok(())
	}

	pub async fn get_object(&self, key: &str) -> Result<Response> {
		let resp = self
			.send(Some(key), "GET", None::<&u64>, None, None)
			.await
			.dot()?;
		return Ok(resp);
	}
}

#[cfg(test)]
mod tests {
	use async_std::task;

	use super::*;

	#[test]
	#[tracing_test::traced_test]
	fn test_list_bucket() -> Result<()> {
		let s3 = S3Client::from_toml_config("config.toml")?;
		task::block_on(async {
			s3.put_object("test.bin", "Cargo.toml").await.unwrap();
		});
		return Ok(());
	}
}
