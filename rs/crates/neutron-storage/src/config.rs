//! Storage provider configuration.

/// Which S3-compatible provider to target.
#[derive(Debug, Clone)]
pub enum Provider {
    /// Amazon S3 — host is `s3.{region}.amazonaws.com`.
    S3 { region: String },
    /// Cloudflare R2 — host is `{account_id}.r2.cloudflarestorage.com`.
    R2 { account_id: String },
    /// Google Cloud Storage via S3-compatible XML API.
    /// Use HMAC credentials from the GCS console.
    Gcs,
    /// Custom endpoint (MinIO, Ceph, etc.).
    Custom { host: String, region: String },
}

/// Configuration for connecting to an S3-compatible object store.
///
/// # Examples
///
/// ```rust,ignore
/// // AWS S3
/// let cfg = StorageConfig::s3("us-east-1", "my-bucket")
///     .credentials("ACCESS_KEY_ID", "SECRET_ACCESS_KEY");
///
/// // Cloudflare R2
/// let cfg = StorageConfig::r2("acct-id", "my-bucket")
///     .credentials("R2_ACCESS_KEY", "R2_SECRET");
///
/// // GCS (HMAC)
/// let cfg = StorageConfig::gcs("my-bucket")
///     .credentials("HMAC_ACCESS_KEY", "HMAC_SECRET");
///
/// // MinIO (local dev)
/// let cfg = StorageConfig::custom("localhost:9000", "us-east-1", "dev-bucket")
///     .credentials("minioadmin", "minioadmin")
///     .http();
/// ```
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub(crate) provider:   Provider,
    pub(crate) bucket:     String,
    pub(crate) access_key: String,
    pub(crate) secret_key: String,
    /// Use plain HTTP instead of HTTPS (for local dev/MinIO).
    pub(crate) use_http:   bool,
}

impl StorageConfig {
    /// AWS S3 in `region`, storing objects in `bucket`.
    pub fn s3(region: impl Into<String>, bucket: impl Into<String>) -> Self {
        StorageConfig {
            provider:   Provider::S3 { region: region.into() },
            bucket:     bucket.into(),
            access_key: String::new(),
            secret_key: String::new(),
            use_http:   false,
        }
    }

    /// Cloudflare R2 under `account_id`, storing objects in `bucket`.
    pub fn r2(account_id: impl Into<String>, bucket: impl Into<String>) -> Self {
        StorageConfig {
            provider:   Provider::R2 { account_id: account_id.into() },
            bucket:     bucket.into(),
            access_key: String::new(),
            secret_key: String::new(),
            use_http:   false,
        }
    }

    /// Google Cloud Storage (S3-compatible XML API, HMAC credentials).
    pub fn gcs(bucket: impl Into<String>) -> Self {
        StorageConfig {
            provider:   Provider::Gcs,
            bucket:     bucket.into(),
            access_key: String::new(),
            secret_key: String::new(),
            use_http:   false,
        }
    }

    /// Custom S3-compatible endpoint (MinIO, Ceph, etc.).
    pub fn custom(
        host:   impl Into<String>,
        region: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Self {
        StorageConfig {
            provider:   Provider::Custom { host: host.into(), region: region.into() },
            bucket:     bucket.into(),
            access_key: String::new(),
            secret_key: String::new(),
            use_http:   false,
        }
    }

    /// Set credentials (access key ID + secret access key).
    pub fn credentials(
        mut self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        self.access_key = access_key.into();
        self.secret_key = secret_key.into();
        self
    }

    /// Use plain HTTP instead of HTTPS.  Useful for local MinIO.
    pub fn http(mut self) -> Self {
        self.use_http = true;
        self
    }

    // -----------------------------------------------------------------------
    // Derived fields used by the client
    // -----------------------------------------------------------------------

    /// Returns `(host, region)` for signing/connecting.
    pub(crate) fn host_and_region(&self) -> (String, String) {
        match &self.provider {
            Provider::S3 { region } => (
                format!("s3.{region}.amazonaws.com"),
                region.clone(),
            ),
            Provider::R2 { account_id } => (
                format!("{account_id}.r2.cloudflarestorage.com"),
                "auto".to_string(),
            ),
            Provider::Gcs => (
                "storage.googleapis.com".to_string(),
                "auto".to_string(),
            ),
            Provider::Custom { host, region } => (host.clone(), region.clone()),
        }
    }

    /// Returns the scheme prefix for URLs.
    pub(crate) fn scheme(&self) -> &'static str {
        if self.use_http { "http" } else { "https" }
    }

    /// Returns the port to connect to (443 or 80).
    pub(crate) fn port(&self) -> u16 {
        if self.use_http { 80 } else { 443 }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_host_and_region() {
        let cfg = StorageConfig::s3("eu-west-1", "my-bucket").credentials("K", "S");
        let (host, region) = cfg.host_and_region();
        assert_eq!(host, "s3.eu-west-1.amazonaws.com");
        assert_eq!(region, "eu-west-1");
    }

    #[test]
    fn r2_host_and_region() {
        let cfg = StorageConfig::r2("abc123", "my-bucket").credentials("K", "S");
        let (host, region) = cfg.host_and_region();
        assert_eq!(host, "abc123.r2.cloudflarestorage.com");
        assert_eq!(region, "auto");
    }

    #[test]
    fn gcs_host_and_region() {
        let cfg = StorageConfig::gcs("my-bucket").credentials("K", "S");
        let (host, region) = cfg.host_and_region();
        assert_eq!(host, "storage.googleapis.com");
        assert_eq!(region, "auto");
    }

    #[test]
    fn custom_host() {
        let cfg = StorageConfig::custom("minio.local:9000", "us-east-1", "dev").credentials("a", "b");
        let (host, region) = cfg.host_and_region();
        assert_eq!(host, "minio.local:9000");
        assert_eq!(region, "us-east-1");
    }

    #[test]
    fn http_flag() {
        let cfg = StorageConfig::s3("us-east-1", "b").http();
        assert!(cfg.use_http);
        assert_eq!(cfg.scheme(), "http");
        assert_eq!(cfg.port(), 80);
    }

    #[test]
    fn https_default() {
        let cfg = StorageConfig::s3("us-east-1", "b");
        assert!(!cfg.use_http);
        assert_eq!(cfg.scheme(), "https");
        assert_eq!(cfg.port(), 443);
    }
}
