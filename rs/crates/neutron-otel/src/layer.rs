use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::span::{Attributes, Record};
use tracing::{Id, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use crate::exporter::OtlpExporter;
use crate::id::{random_span_id, random_trace_id};
use crate::span::{AttributeValue, SpanData, SpanStatus};

/// Per-span data stored in the tracing registry while a span is open.
struct SpanStorage {
    trace_id:      [u8; 16],
    span_id:       [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    name:          String,
    start_ns:      u64,
    attributes:    Vec<(String, AttributeValue)>,
}

fn unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// A [`tracing_subscriber::Layer`] that collects spans and ships them to an
/// OTLP/JSON endpoint via [`OtlpExporter`].
pub struct OtelLayer {
    exporter: Arc<OtlpExporter>,
}

impl OtelLayer {
    pub fn new(exporter: OtlpExporter) -> Self {
        OtelLayer { exporter: Arc::new(exporter) }
    }
}

impl<S> Layer<S> for OtelLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("span must exist");
        let parent_span_id = span
            .parent()
            .and_then(|p| p.extensions().get::<SpanStorage>().map(|s| s.span_id));

        let trace_id = span
            .parent()
            .and_then(|p| p.extensions().get::<SpanStorage>().map(|s| s.trace_id))
            .unwrap_or_else(random_trace_id);

        let storage = SpanStorage {
            trace_id,
            span_id: random_span_id(),
            parent_span_id,
            name: attrs.metadata().name().to_string(),
            start_ns: unix_nanos(),
            attributes: vec![],
        };

        span.extensions_mut().insert(storage);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(storage) = span.extensions_mut().get_mut::<SpanStorage>() {
                // Visit the recorded fields and store string representations
                struct Visitor<'a>(&'a mut Vec<(String, AttributeValue)>);
                impl tracing::field::Visit for Visitor<'_> {
                    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                        self.0.push((field.name().to_string(), AttributeValue::String(value.to_string())));
                    }
                    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
                        self.0.push((field.name().to_string(), AttributeValue::Int(value)));
                    }
                    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                        self.0.push((field.name().to_string(), AttributeValue::Int(value as i64)));
                    }
                    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                        self.0.push((field.name().to_string(), AttributeValue::Bool(value)));
                    }
                    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
                        self.0.push((field.name().to_string(), AttributeValue::Float(value)));
                    }
                    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                        self.0.push((field.name().to_string(), AttributeValue::String(format!("{:?}", value))));
                    }
                }
                values.record(&mut Visitor(&mut storage.attributes));
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let end_ns = unix_nanos();
        if let Some(span) = ctx.span(&id) {
            if let Some(storage) = span.extensions_mut().remove::<SpanStorage>() {
                let span_data = SpanData {
                    trace_id: storage.trace_id,
                    span_id: storage.span_id,
                    parent_span_id: storage.parent_span_id,
                    name: storage.name,
                    start_ns: storage.start_ns,
                    end_ns,
                    status: SpanStatus::Unset,
                    attributes: storage.attributes,
                };
                let exporter = Arc::clone(&self.exporter);
                tokio::spawn(async move {
                    let _ = exporter.push(span_data).await;
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OtelConfig;

    #[test]
    fn unix_nanos_is_nonzero() {
        assert!(unix_nanos() > 0);
    }

    #[test]
    fn unix_nanos_is_monotonic() {
        let a = unix_nanos();
        let b = unix_nanos();
        assert!(b >= a);
    }

    #[test]
    fn otel_layer_constructs() {
        let exp = OtlpExporter::new(OtelConfig::new("svc")).unwrap();
        let _layer = OtelLayer::new(exp);
    }
}
