use std::path::Path;

use anyhow_ext::Context;
use anyhow_ext::Result;
use std::path::PathBuf;
use tracing::info;
use zjhttpc::response::Response;

use crate::S3Client;

impl S3Client {
	pub async fn put_object<P>(&self, key: &str, path: P) -> Result<Response>
	where
		P: AsRef<Path>,
	{
		let resp = self
			.send(
				Some(key),
				"PUT",
				None::<&u64>,
				None,
				Some(crate::S3Body::Path(PathBuf::from(path.as_ref()))),
			)
			.await
			.dot()?;

		Ok(resp)
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
use tracing::error;

	use super::*;

	#[test]
	#[tracing_test::traced_test]
	fn test_put_object() -> Result<()> {
		let s3 = S3Client::from_toml_config("config.toml")?;
		task::block_on(async {
			s3.put_object("test.bin", "Cargo.toml").await.unwrap();
		});
		return Ok(());
	}

	#[test]
	#[tracing_test::traced_test]
	fn test_get_object() -> Result<()> {
		let s3 = S3Client::from_toml_config("config.toml")?;
		task::block_on(async {
			let mut resp = s3.get_object("test.bin").await.unwrap();
			if resp.is_success() {
				let text = resp.body_string().await.unwrap();
				info!(text);
			} else {
				let msg = resp.body_string().await.unwrap();
				error!(resp.status_code, msg)

			}
		});
		return Ok(());
	}
}
