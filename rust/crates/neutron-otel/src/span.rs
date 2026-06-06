use serde_json::{json, Value};

use crate::id::hex_encode;

/// Status of a completed span.
#[derive(Debug, Clone, PartialEq)]
pub enum SpanStatus {
    /// Not set — default, no explicit status.
    Unset,
    /// Span completed successfully.
    Ok,
    /// Span completed with an error.
    Error(String),
}

/// A single attribute value attached to a span.
#[derive(Debug, Clone)]
pub enum AttributeValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// All data captured for a single completed span.
#[derive(Debug, Clone)]
pub struct SpanData {
    /// 128-bit trace identifier.
    pub trace_id: [u8; 16],
    /// 64-bit span identifier.
    pub span_id: [u8; 8],
    /// Parent span identifier, if this is a child span.
    pub parent_span_id: Option<[u8; 8]>,
    /// Human-readable name of the operation this span represents.
    pub name: String,
    /// Wall-clock start time in Unix nanoseconds.
    pub start_ns: u64,
    /// Wall-clock end time in Unix nanoseconds.
    pub end_ns: u64,
    /// Completion status.
    pub status: SpanStatus,
    /// Key-value attributes attached to this span.
    pub attributes: Vec<(String, AttributeValue)>,
}

impl SpanData {
    /// Serialize this span into OTLP/JSON format as defined by the
    /// OpenTelemetry proto3 JSON mapping.
    pub fn to_otlp_json(&self) -> Value {
        let status_obj = match &self.status {
            SpanStatus::Unset      => json!({ "code": 0 }),
            SpanStatus::Ok         => json!({ "code": 1 }),
            SpanStatus::Error(msg) => json!({ "code": 2, "message": msg }),
        };

        let attrs: Vec<Value> = self
            .attributes
            .iter()
            .map(|(key, val)| {
                let v = match val {
                    AttributeValue::String(s) => json!({ "stringValue": s }),
                    AttributeValue::Int(i)    => json!({ "intValue": i.to_string() }),
                    AttributeValue::Float(f)  => json!({ "doubleValue": f }),
                    AttributeValue::Bool(b)   => json!({ "boolValue": b }),
                };
                json!({ "key": key, "value": v })
            })
            .collect();

        let mut obj = json!({
            "traceId":           hex_encode(&self.trace_id),
            "spanId":            hex_encode(&self.span_id),
            "name":              self.name,
            "startTimeUnixNano": self.start_ns.to_string(),
            "endTimeUnixNano":   self.end_ns.to_string(),
            "status":            status_obj,
            "attributes":        attrs,
        });

        if let Some(parent) = &self.parent_span_id {
            obj["parentSpanId"] = Value::String(hex_encode(parent));
        }

        obj
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{random_span_id, random_trace_id};

    fn make_span() -> SpanData {
        SpanData {
            trace_id: [1u8; 16],
            span_id: [2u8; 8],
            parent_span_id: None,
            name: "test.op".to_string(),
            start_ns: 1_000_000,
            end_ns: 2_000_000,
            status: SpanStatus::Unset,
            attributes: vec![],
        }
    }

    #[test]
    fn otlp_json_trace_id() {
        let json = make_span().to_otlp_json();
        assert_eq!(json["traceId"].as_str().unwrap(), "01010101010101010101010101010101");
    }

    #[test]
    fn otlp_json_span_id() {
        let json = make_span().to_otlp_json();
        assert_eq!(json["spanId"].as_str().unwrap(), "0202020202020202");
    }

    #[test]
    fn otlp_json_no_parent_by_default() {
        let json = make_span().to_otlp_json();
        assert!(json.get("parentSpanId").is_none());
    }

    #[test]
    fn otlp_json_parent_span_id() {
        let mut span = make_span();
        span.parent_span_id = Some([0xabu8; 8]);
        let json = span.to_otlp_json();
        assert_eq!(json["parentSpanId"].as_str().unwrap(), "abababababababab");
    }

    #[test]
    fn otlp_json_status_unset() {
        let json = make_span().to_otlp_json();
        assert_eq!(json["status"]["code"], 0);
    }

    #[test]
    fn otlp_json_status_ok() {
        let mut span = make_span();
        span.status = SpanStatus::Ok;
        let json = span.to_otlp_json();
        assert_eq!(json["status"]["code"], 1);
    }

    #[test]
    fn otlp_json_status_error() {
        let mut span = make_span();
        span.status = SpanStatus::Error("boom".to_string());
        let json = span.to_otlp_json();
        assert_eq!(json["status"]["code"], 2);
        assert_eq!(json["status"]["message"], "boom");
    }

    #[test]
    fn otlp_json_timestamps() {
        let json = make_span().to_otlp_json();
        assert_eq!(json["startTimeUnixNano"].as_str().unwrap(), "1000000");
        assert_eq!(json["endTimeUnixNano"].as_str().unwrap(), "2000000");
    }

    #[test]
    fn otlp_json_string_attribute() {
        let mut span = make_span();
        span.attributes.push(("http.method".to_string(), AttributeValue::String("GET".to_string())));
        let json = span.to_otlp_json();
        assert_eq!(json["attributes"][0]["key"], "http.method");
        assert_eq!(json["attributes"][0]["value"]["stringValue"], "GET");
    }

    #[test]
    fn otlp_json_int_attribute() {
        let mut span = make_span();
        span.attributes.push(("status_code".to_string(), AttributeValue::Int(200)));
        let json = span.to_otlp_json();
        assert_eq!(json["attributes"][0]["value"]["intValue"], "200");
    }

    #[test]
    fn otlp_json_float_attribute() {
        let mut span = make_span();
        span.attributes.push(("lat".to_string(), AttributeValue::Float(3.14)));
        let json = span.to_otlp_json();
        let v = json["attributes"][0]["value"]["doubleValue"].as_f64().unwrap();
        assert!((v - 3.14).abs() < 1e-9);
    }

    #[test]
    fn otlp_json_bool_attribute() {
        let mut span = make_span();
        span.attributes.push(("error".to_string(), AttributeValue::Bool(true)));
        let json = span.to_otlp_json();
        assert_eq!(json["attributes"][0]["value"]["boolValue"], true);
    }

    #[test]
    fn otlp_json_multiple_attributes() {
        let mut span = make_span();
        span.attributes.push(("a".to_string(), AttributeValue::String("x".to_string())));
        span.attributes.push(("b".to_string(), AttributeValue::Int(42)));
        let json = span.to_otlp_json();
        assert_eq!(json["attributes"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn span_clone_is_independent() {
        let span = make_span();
        let mut clone = span.clone();
        clone.name = "other".to_string();
        assert_eq!(span.name, "test.op");
    }

    #[test]
    fn span_status_equality() {
        assert_eq!(SpanStatus::Unset, SpanStatus::Unset);
        assert_eq!(SpanStatus::Ok, SpanStatus::Ok);
        assert_eq!(SpanStatus::Error("e".to_string()), SpanStatus::Error("e".to_string()));
        assert_ne!(SpanStatus::Ok, SpanStatus::Unset);
    }

    #[test]
    fn span_with_random_ids() {
        let span = SpanData {
            trace_id: random_trace_id(),
            span_id: random_span_id(),
            parent_span_id: Some(random_span_id()),
            name: "rand".to_string(),
            start_ns: 100,
            end_ns: 200,
            status: SpanStatus::Ok,
            attributes: vec![("k".to_string(), AttributeValue::String("v".to_string()))],
        };
        let json = span.to_otlp_json();
        assert_eq!(json["traceId"].as_str().unwrap().len(), 32);
        assert_eq!(json["parentSpanId"].as_str().unwrap().len(), 16);
    }
}
