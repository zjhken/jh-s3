use std::collections::HashMap;
use std::path::Path;

use anyhow_ext::Context;
use anyhow_ext::Result;
use std::path::PathBuf;
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

	/// 流式上传：接收任意 `async_std::io::Read`，不把整个 body 读进内存。
	/// 需要已知 `content_length`（zjhttpc 的 stream body 要求）。
	/// 签名走 `UNSIGNED-PAYLOAD`，body 不参与签名计算。
	pub async fn put_object_in_stream<R>(
		&self,
		key: &str,
		reader: R,
		content_length: u64,
		headers: Option<HashMap<String, String>>,
	) -> Result<Response>
	where
		R: async_std::io::Read + Unpin + Send + Sync + 'static,
	{
		let resp = self
			.send(
				Some(key),
				"PUT",
				None::<&u64>,
				headers,
				Some(crate::S3Body::Stream(Box::new(reader), content_length)),
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

	pub async fn delete_object(&self, key: &str) -> Result<Response> {
		let resp = self
			.send(Some(key), "DELETE", None::<&u64>, None, None)
			.await
			.dot()?;
		Ok(resp)
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
	fn test_put_object_in_stream() -> Result<()> {
		let s3 = S3Client::from_toml_config("config.toml")?;
		task::block_on(async {
			let data = b"hello streaming upload via put_object_in_stream".to_vec();
			let cursor = async_std::io::Cursor::new(data.clone());
			let resp = s3
				.put_object_in_stream("test_stream.bin", cursor, data.len() as u64, None)
				.await
				.unwrap();
			assert!(resp.is_success());

			let mut get = s3.get_object("test_stream.bin").await.unwrap();
			assert!(get.is_success());
			let body = get.body_bytes().await.unwrap();
			assert_eq!(body, data);

			s3.delete_object("test_stream.bin").await.unwrap();
		});
		Ok(())
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
