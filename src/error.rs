use crate::S3Error;
use snafu::prelude::*;
use std::io;
use zjhttpc::client::ZJHttpClientBuilderError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
	#[snafu(display("http error at {location}: {source}"))]
	Http {
		#[snafu(source)]
		source: zjhttpc::ZjhttpcError,
		#[snafu(implicit)]
		location: snafu::Location,
	},

	#[snafu(display("http client builder failed at {location}: {source}"))]
	HttpClientBuilder {
		#[snafu(source)]
		source: ZJHttpClientBuilderError,
		#[snafu(implicit)]
		location: snafu::Location,
	},

	#[snafu(display("io error at {location}: {source}"))]
	Io {
		#[snafu(source)]
		source: io::Error,
		#[snafu(implicit)]
		location: snafu::Location,
	},

	#[snafu(display("invalid config TOML at {location}: {source}"))]
	Config {
		#[snafu(source)]
		source: toml::de::Error,
		#[snafu(implicit)]
		location: snafu::Location,
	},

	#[snafu(display("xml error at {location}: {source}"))]
	Xml {
		#[snafu(source)]
		source: serde_xml_rs::Error,
		#[snafu(implicit)]
		location: snafu::Location,
	},

	#[snafu(display("s3 api error at {location}: {error}"))]
	S3Api {
		error: S3Error,
		#[snafu(implicit)]
		location: snafu::Location,
	},

	#[snafu(display("invalid part_size at {location}: part_size must be > 0"))]
	InvalidPartSize {
		#[snafu(implicit)]
		location: snafu::Location,
	},

	#[snafu(display("missing ETag response header at {location}"))]
	MissingEtag {
		#[snafu(implicit)]
		location: snafu::Location,
	},

	#[snafu(display("timestamp split on 'T' failed at {location}"))]
	TimestampSplitFailed {
		#[snafu(implicit)]
		location: snafu::Location,
	},
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_io_error_message_includes_location() {
		let result: Result<()> = (|| {
			std::fs::read_to_string("/nonexistent/path/for/test").context(IoSnafu)?;
			Ok(())
		})();
		let e = result.unwrap_err();
		let msg = e.to_string();
		assert!(
			msg.starts_with("io error at src/error.rs:"),
			"missing location prefix: {msg}"
		);
		assert!(
			msg.contains("No such file or directory"),
			"missing io source text: {msg}"
		);
	}
}
