//! S3-compatible HTTP gateway over the Nucleus blob store.
//!
//! *"S3 that dedupes, verifies, and runs as part of your database."*
//!
//! Serves the S3 REST API against the disk-tiered, content-addressed,
//! BLAKE3-verified blob store: SigV4 auth (header + presigned URLs +
//! aws-chunked streaming), buckets, objects with Range reads, ListObjects
//! V1/V2, batch delete, server-side copy, and multipart uploads.
//! CompleteMultipartUpload and CopyObject are zero-copy — they compose
//! manifests over existing content-addressed chunks.
//!
//! What it deliberately is NOT: a MinIO replacement for raw throughput or
//! erasure-coded scale-out. Versioning, lifecycle rules, object locks, ACLs
//! and IAM policies are not implemented; every request must be signed by the
//! single configured credential pair.
//!
//! Enable with `nucleus serve --s3-port 9000` plus the
//! `NUCLEUS_S3_ACCESS_KEY` / `NUCLEUS_S3_SECRET_KEY` environment variables.
//! Clients must use path-style addressing (for the AWS CLI:
//! `aws s3 --endpoint-url http://host:9000 ...`).

pub mod handlers;
pub mod http;
pub mod server;
pub mod sigv4;

pub use handlers::{S3Config, S3Handler};
pub use server::{S3ServerConfig, start_s3_server, start_s3_server_with_config};
