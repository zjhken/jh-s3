use std::collections::HashMap;

use async_std::io::ReadExt;
use serde::Deserialize;
use serde::Serialize;
use snafu::prelude::*;
use tracing::info;
use tracing::warn;
use zjhttpc::response::Response;

use crate::S3Client;
use crate::S3Error;
use crate::error::{
	HttpSnafu, InvalidPartSizeSnafu, IoSnafu, MissingEtagSnafu, Result, S3ApiSnafu, XmlSnafu,
};

const S3_MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

impl S3Client {
	/// POST /{key}?uploads —— 发起一次 multipart upload。
	/// `headers` 中的 `Content-Type` 等会成为最终对象的元数据。
	pub async fn initiate_multipart_upload(
		&self,
		key: &str,
		headers: Option<HashMap<String, String>>,
	) -> Result<InitiateMultipartUploadResult> {
		#[derive(Serialize)]
		struct InitiateQuery {
			uploads: bool,
		}
		let q = InitiateQuery { uploads: true };
		let mut resp = self
			.send(Some(key), "POST", Some(&q), headers, None)
			.await?;
		let xml = resp.body_string().await.context(HttpSnafu)?;
		info!(xml);
		if resp.is_success() {
			Ok(serde_xml_rs::from_reader(xml.as_bytes()).context(XmlSnafu)?)
		} else {
			let err: S3Error = serde_xml_rs::from_reader(xml.as_bytes()).context(XmlSnafu)?;
			S3ApiSnafu { error: err }.fail()
		}
	}

	/// PUT /{key}?partNumber=N&uploadId=... —— 上传单个 part，返回 ETag（已去引号）。
	pub async fn upload_part(
		&self,
		key: &str,
		upload_id: &str,
		part_number: u32,
		body: Vec<u8>,
	) -> Result<String> {
		#[derive(Serialize)]
		struct PartQuery<'a> {
			#[serde(rename(serialize = "partNumber"))]
			part_number: u32,
			#[serde(rename(serialize = "uploadId"))]
			upload_id: &'a str,
		}
		let q = PartQuery {
			part_number,
			upload_id,
		};
		let mut resp = self
			.send(
				Some(key),
				"PUT",
				Some(&q),
				None,
				Some(crate::S3Body::Bytes(body)),
			)
			.await?;
		if !resp.is_success() {
			let xml = resp.body_string().await.context(HttpSnafu)?;
			let err: S3Error = serde_xml_rs::from_reader(xml.as_bytes()).context(XmlSnafu)?;
			return S3ApiSnafu { error: err }.fail();
		}
		let etag = resp
			.header_one("ETag")
			.context(MissingEtagSnafu)?
			.trim_matches('"')
			.to_owned();
		Ok(etag)
	}

	/// POST /{key}?uploadId=... —— 提交所有 parts，完成 multipart upload。
	pub async fn complete_multipart_upload(
		&self,
		key: &str,
		upload_id: &str,
		parts: &[UploadedPart],
	) -> Result<CompleteMultipartUploadResult> {
		#[derive(Serialize)]
		struct UploadIdQuery<'a> {
			#[serde(rename(serialize = "uploadId"))]
			upload_id: &'a str,
		}
		let q = UploadIdQuery { upload_id };
		let xml_body = build_complete_xml(parts);
		let mut resp = self
			.send(
				Some(key),
				"POST",
				Some(&q),
				None,
				Some(crate::S3Body::Bytes(xml_body.into_bytes())),
			)
			.await?;
		let xml = resp.body_string().await.context(HttpSnafu)?;
		info!(xml);
		if resp.is_success() {
			Ok(serde_xml_rs::from_reader(xml.as_bytes()).context(XmlSnafu)?)
		} else {
			let err: S3Error = serde_xml_rs::from_reader(xml.as_bytes()).context(XmlSnafu)?;
			S3ApiSnafu { error: err }.fail()
		}
	}

	/// DELETE /{key}?uploadId=... —— 取消进行中的 multipart upload，释放已上传的 parts。
	pub async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> Result<Response> {
		#[derive(Serialize)]
		struct UploadIdQuery<'a> {
			#[serde(rename(serialize = "uploadId"))]
			upload_id: &'a str,
		}
		let q = UploadIdQuery { upload_id };
		let resp = self.send(Some(key), "DELETE", Some(&q), None, None).await?;
		Ok(resp)
	}

	/// 高层 helper：把 reader 按 `part_size` 切块顺序上传，失败时自动 abort。
	/// `total_length` 必须已知且与 reader 实际可读字节数一致。
	pub async fn multipart_upload<R>(
		&self,
		key: &str,
		mut reader: R,
		total_length: u64,
		part_size: u64,
		headers: Option<HashMap<String, String>>,
	) -> Result<CompleteMultipartUploadResult>
	where
		R: async_std::io::Read + Unpin + Send + Sync + 'static,
	{
		if part_size == 0 {
			return InvalidPartSizeSnafu.fail();
		}
		if part_size < S3_MIN_PART_SIZE && total_length > part_size {
			warn!(
				part_size,
				"part_size below S3 5 MiB minimum; non-final parts may be rejected by strict S3 implementations"
			);
		}

		let init = self.initiate_multipart_upload(key, headers).await?;
		let upload_id = init.upload_id.clone();

		let inner: Result<CompleteMultipartUploadResult> = async {
			let mut parts: Vec<UploadedPart> = Vec::new();
			let mut part_number: u32 = 1;
			let mut remaining = total_length;

			while remaining > 0 {
				let to_read = remaining.min(part_size) as usize;
				let mut buf = vec![0u8; to_read];
				reader.read_exact(&mut buf).await.context(IoSnafu)?;
				let etag = self.upload_part(key, &upload_id, part_number, buf).await?;
				parts.push(UploadedPart { part_number, etag });
				part_number += 1;
				remaining = remaining.saturating_sub(to_read as u64);
			}

			if parts.is_empty() {
				let etag = self.upload_part(key, &upload_id, 1, Vec::new()).await?;
				parts.push(UploadedPart {
					part_number: 1,
					etag,
				});
			}

			self.complete_multipart_upload(key, &upload_id, &parts)
				.await
		}
		.await;

		match inner {
			Ok(r) => Ok(r),
			Err(e) => {
				let _ = self.abort_multipart_upload(key, &upload_id).await;
				Err(e)
			}
		}
	}
}

fn build_complete_xml(parts: &[UploadedPart]) -> String {
	let mut s = String::from("<CompleteMultipartUpload>");
	for p in parts {
		let etag = p.etag.trim_matches('"');
		s.push_str(&format!(
			"<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
			p.part_number, etag
		));
	}
	s.push_str("</CompleteMultipartUpload>");
	s
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct InitiateMultipartUploadResult {
	pub bucket: String,
	pub key: String,
	pub upload_id: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct CompleteMultipartUploadResult {
	pub location: Option<String>,
	pub bucket: String,
	pub key: String,
	pub etag: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UploadedPart {
	pub part_number: u32,
	pub etag: String,
}

#[cfg(test)]
mod tests {
	use async_std::task;
	use tracing::info;

	use super::*;
	use crate::S3Client;

	#[test]
	#[tracing_test::traced_test]
	fn test_multipart_roundtrip() -> Result<()> {
		// NOTE: 1 MiB parts 低于 AWS S3 5 MiB 最小限制；Dell ECS 非生产环境通常接受。
		// 跑严格 AWS S3 时需把 PART_SIZE 调到 5_242_880 并加大 TOTAL。
		const PART_SIZE: u64 = 1024 * 1024;
		const TOTAL: u64 = PART_SIZE * 3 - 17;
		let s3 = S3Client::from_toml_config("config.toml")?;
		task::block_on(async {
			let data = vec![0xABu8; TOTAL as usize];
			let cursor = async_std::io::Cursor::new(data.clone());

			let result = s3
				.multipart_upload("test_mpu.bin", cursor, TOTAL, PART_SIZE, None)
				.await
				.unwrap();
			info!(?result);

			let mut get = s3.get_object("test_mpu.bin").await.unwrap();
			assert!(get.is_success());
			let body = get.body_bytes().await.unwrap();
			assert_eq!(body.len() as u64, TOTAL);
			assert_eq!(body[0], 0xAB);
			assert_eq!(body[TOTAL as usize - 1], 0xAB);

			s3.delete_object("test_mpu.bin").await.unwrap();
		});
		Ok(())
	}

	#[test]
	#[tracing_test::traced_test]
	fn test_multipart_empty_object() -> Result<()> {
		let s3 = S3Client::from_toml_config("config.toml")?;
		task::block_on(async {
			let cursor = async_std::io::Cursor::new(Vec::<u8>::new());
			let _ = s3
				.multipart_upload("test_empty.bin", cursor, 0, 5 * 1024 * 1024, None)
				.await
				.unwrap();
			let mut get = s3.get_object("test_empty.bin").await.unwrap();
			assert!(get.is_success());
			let body = get.body_bytes().await.unwrap();
			assert!(body.is_empty());
			s3.delete_object("test_empty.bin").await.unwrap();
		});
		Ok(())
	}

	#[test]
	#[tracing_test::traced_test]
	fn test_abort_multipart_upload() -> Result<()> {
		let s3 = S3Client::from_toml_config("config.toml")?;
		task::block_on(async {
			let init = s3
				.initiate_multipart_upload("test_abort.bin", None)
				.await
				.unwrap();
			s3.abort_multipart_upload("test_abort.bin", &init.upload_id)
				.await
				.unwrap();
			let get = s3.get_object("test_abort.bin").await.unwrap();
			assert!(!get.is_success());
		});
		Ok(())
	}
}
