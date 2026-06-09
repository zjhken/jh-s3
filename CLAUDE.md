# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

```bash
cargo build                # Build the library
cargo test                 # Run all tests
cargo test test_auth_basic_request   # Run a single test by name
cargo test -- --nocapture  # Run tests with stdout visible (for tracing/logging output)
cargo fmt                  # Format code (uses hard_tabs = true)
```

Note: Integration tests in `bucket.rs` and `object.rs` require a `config.toml` in the project root (see `config.example.toml` for format) and a running S3-compatible endpoint. Unit tests in `aws_sig_v4.rs` run without external dependencies.

## Architecture

This is a Rust library crate (`jh-s3`) implementing an S3 protocol client targeting Dell ECS. It uses `async-std` as the async runtime and `zjhttpc` as the HTTP client.

**Module layout:**
- `lib.rs` — `S3Client` and `S3Config` structs, client initialization (`new`, `from_toml_config`), core `send()` method that dispatches authenticated requests. `S3Body` enum for request payloads (`Bytes` or `Path`).
- `aws_sig_v4.rs` — AWS Signature V4 implementation. The `auth()` function takes a `Request`, computes the canonical request, signs it, and returns the signed request with the `Authorization` header. Supports both byte and file body checksums (SHA256).
- `bucket.rs` — `list_bucket()` with `ListBucketParams` builder. Parses XML responses including Dell ECS extensions (metadata queries: `SYSMD`, `USERMD`).
- `object.rs` — `put_object()` (file upload) and `get_object()`.

**Key pattern:** All S3 operations are `impl S3Client` blocks spread across modules. The `send()` method in `lib.rs` is the single entry point — it builds the URL, attaches auth via `aws_sig_v4::auth()`, and dispatches through `zjhttpc`.

**Signature constants:** Region is hardcoded to `us-east-1`, service to `s3` (in `aws_sig_v4.rs`).

**Error handling:** Uses `anyhow_ext` throughout (`anyhow_ext::Result`, `.dot()` for chaining).

**Config:** TOML-based with fields: `endpoint`, `bucket`, `access_key`, `secret_key`, optional `trust_cert_path` for custom CA certs.

## Formatting

Uses hard tabs (`rustfmt.toml` sets `hard_tabs = true`).
