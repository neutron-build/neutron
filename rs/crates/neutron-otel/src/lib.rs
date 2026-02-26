//! OpenTelemetry OTLP/JSON tracing exporter for Neutron.
//!
//! Exports spans as OTLP/JSON (HTTP) to any OTel-compatible collector
//! (Jaeger, Grafana Tempo, OTel Collector, etc.) — no protobuf dependency.
//!
//! # Quick start
//!
//! ```rust,ignore
//! use neutron_otel::{OtelConfig, OtlpExporter, OtelLayer};
//! use tracing_subscriber::prelude::*;
//!
//! let config   = OtelConfig::new("my-service").endpoint("http://localhost:4318");
//! let exporter = OtlpExporter::new(config).unwrap();
//! let layer    = OtelLayer::new(exporter.clone());
//!
//! tracing_subscriber::registry().with(layer).init();
//!
//! // Spans are now automatically collected and exported
//! let _span = tracing::info_span!("my-operation").entered();
//! ```

pub mod config;
pub mod error;
pub mod exporter;
pub mod id;
pub mod layer;
pub mod span;

pub use config::{OtelConfig, OtlpProtocol};
pub use error::OtelError;
pub use exporter::OtlpExporter;
pub use id::{hex_encode, random_span_id, random_trace_id};
pub use layer::OtelLayer;
pub use span::{AttributeValue, SpanData, SpanStatus};

pub mod prelude {
    pub use crate::{OtelConfig, OtelLayer, OtlpExporter};
}
