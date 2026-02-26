use crate::error::OtelError;

/// Transport protocol for OTLP export.
#[derive(Debug, Clone, PartialEq)]
#[derive(Default)]
pub enum OtlpProtocol {
    /// HTTP/JSON — POST to `{endpoint}/v1/traces`.
    #[default]
    HttpJson,
}


/// Configuration for the OTLP exporter.
#[derive(Debug, Clone)]
pub struct OtelConfig {
    /// OTLP collector endpoint, e.g. `http://localhost:4318`.
    pub endpoint: String,
    /// Service name reported in every span's resource.
    pub service_name: String,
    /// Protocol (currently only HTTP/JSON).
    pub protocol: OtlpProtocol,
    /// Maximum number of spans buffered before export.
    pub batch_size: usize,
    /// Export interval in milliseconds.
    pub export_interval_ms: u64,
}

impl Default for OtelConfig {
    fn default() -> Self {
        OtelConfig {
            endpoint:           "http://localhost:4318".to_string(),
            service_name:       "neutron".to_string(),
            protocol:           OtlpProtocol::HttpJson,
            batch_size:         512,
            export_interval_ms: 5_000,
        }
    }
}

impl OtelConfig {
    /// Start building a new config.
    pub fn new(service_name: impl Into<String>) -> Self {
        OtelConfig {
            service_name: service_name.into(),
            ..Default::default()
        }
    }

    /// Override the OTLP endpoint.
    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = url.into();
        self
    }

    /// Override the batch size (max spans per export).
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }

    /// Override the export interval (milliseconds).
    pub fn export_interval_ms(mut self, ms: u64) -> Self {
        self.export_interval_ms = ms;
        self
    }

    /// Validate configuration, returning an error on obvious misconfigurations.
    pub fn validate(&self) -> Result<(), OtelError> {
        if self.endpoint.is_empty() {
            return Err(OtelError::Config("endpoint must not be empty".to_string()));
        }
        if self.service_name.is_empty() {
            return Err(OtelError::Config("service_name must not be empty".to_string()));
        }
        if self.batch_size == 0 {
            return Err(OtelError::Config("batch_size must be > 0".to_string()));
        }
        Ok(())
    }

    /// Returns the full traces endpoint URL.
    pub fn traces_url(&self) -> String {
        format!("{}/v1/traces", self.endpoint.trim_end_matches('/'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let c = OtelConfig::default();
        assert_eq!(c.endpoint, "http://localhost:4318");
        assert_eq!(c.service_name, "neutron");
        assert_eq!(c.batch_size, 512);
        assert_eq!(c.export_interval_ms, 5_000);
    }

    #[test]
    fn builder_endpoint() {
        let c = OtelConfig::new("svc").endpoint("http://collector:4318");
        assert_eq!(c.endpoint, "http://collector:4318");
    }

    #[test]
    fn builder_service_name() {
        let c = OtelConfig::new("my-api");
        assert_eq!(c.service_name, "my-api");
    }

    #[test]
    fn builder_batch_size() {
        let c = OtelConfig::new("svc").batch_size(100);
        assert_eq!(c.batch_size, 100);
    }

    #[test]
    fn builder_export_interval() {
        let c = OtelConfig::new("svc").export_interval_ms(1000);
        assert_eq!(c.export_interval_ms, 1000);
    }

    #[test]
    fn traces_url_no_trailing_slash() {
        let c = OtelConfig::new("svc").endpoint("http://localhost:4318");
        assert_eq!(c.traces_url(), "http://localhost:4318/v1/traces");
    }

    #[test]
    fn traces_url_with_trailing_slash() {
        let c = OtelConfig::new("svc").endpoint("http://localhost:4318/");
        assert_eq!(c.traces_url(), "http://localhost:4318/v1/traces");
    }

    #[test]
    fn validate_ok() {
        assert!(OtelConfig::new("svc").validate().is_ok());
    }

    #[test]
    fn validate_empty_endpoint() {
        let c = OtelConfig::new("svc").endpoint("");
        assert!(matches!(c.validate(), Err(OtelError::Config(_))));
    }

    #[test]
    fn validate_empty_service_name() {
        let c = OtelConfig::new("");
        assert!(matches!(c.validate(), Err(OtelError::Config(_))));
    }

    #[test]
    fn validate_zero_batch_size() {
        let c = OtelConfig::new("svc").batch_size(0);
        assert!(matches!(c.validate(), Err(OtelError::Config(_))));
    }
}
