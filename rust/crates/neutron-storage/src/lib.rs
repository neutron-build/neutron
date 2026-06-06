//! S3-compatible object storage for the Neutron web framework.
//!
//! Supports AWS S3, Cloudflare R2, Google Cloud Storage (HMAC), and any
//! S3-compatible endpoint (MinIO, Ceph, etc.).
//!
//! Uses AWS Signature Version 4 signing with existing workspace TLS primitives —
//! no heavy AWS SDK dependency required.
//!
//! # Quick start
//!
//! ```rust,ignore
//! use neutron_storage::{StorageClient, StorageConfig};
//!
//! // AWS S3
//! let client = StorageClient::new(
//!     StorageConfig::s3("us-east-1", "my-bucket")
//!         .credentials(access_key, secret_key),
//! );
//!
//! // Upload
//! client.put("uploads/photo.jpg", image_bytes, "image/jpeg").await?;
//!
//! // Download
//! let bytes = client.get("uploads/photo.jpg").await?;
//!
//! // Presigned URL (browser-friendly)
//! let url = client.presign_get("uploads/photo.jpg", 3600);
//!
//! // List
//! let objects = client.list("uploads/").await?;
//! for obj in objects {
//!     println!("{}: {} bytes", obj.key, obj.size);
//! }
//!
//! // Delete
//! client.delete("uploads/photo.jpg").await?;
//! ```
//!
//! # Router integration
//!
//! ```rust,ignore
//! use neutron::router::Router;
//! use neutron::data::Data;
//! use neutron_storage::{StorageClient, StorageConfig};
//!
//! let storage = StorageClient::new(
//!     StorageConfig::r2("account-id", "assets")
//!         .credentials(r2_access_key, r2_secret),
//! );
//!
//! let app = Router::new()
//!     .post("/upload", upload_handler)
//!     .state(Data::new(storage));
//!
//! async fn upload_handler(
//!     Data(storage): Data<StorageClient>,
//!     /* multipart body … */
//! ) -> impl IntoResponse {
//!     storage.put("file.jpg", bytes, "image/jpeg").await.ok();
//!     let url = storage.presign_get("file.jpg", 86400);
//!     url
//! }
//! ```

pub(crate) mod client;

pub mod config;
pub mod error;
pub mod sign;
pub mod storage;

pub use config::{Provider, StorageConfig};
pub use error::StorageError;
pub use storage::{ObjectInfo, ObjectMeta, StorageClient};
