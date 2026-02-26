//! Message codec — encode/decode RPC request and response bodies.

use bytes::Bytes;

/// Error produced by a codec during encoding or decoding.
#[derive(Debug)]
pub struct CodecError(pub String);

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "codec error: {}", self.0)
    }
}

impl std::error::Error for CodecError {}

// ---------------------------------------------------------------------------
// RpcMessage trait
// ---------------------------------------------------------------------------

/// A type that can be encoded to / decoded from a gRPC message body.
///
/// A blanket implementation is provided for all `serde` types, using JSON
/// encoding.  For binary encoding or Protobuf, implement this trait manually.
pub trait RpcMessage: Sized + Send + 'static {
    fn decode(bytes: &[u8]) -> Result<Self, CodecError>;
    fn encode(&self) -> Result<Bytes, CodecError>;
}

// ---------------------------------------------------------------------------
// Blanket JSON impl for serde types
// ---------------------------------------------------------------------------

impl<T> RpcMessage for T
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
{
    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        serde_json::from_slice(bytes)
            .map_err(|e| CodecError(format!("JSON decode: {e}")))
    }

    fn encode(&self) -> Result<Bytes, CodecError> {
        serde_json::to_vec(self)
            .map(Bytes::from)
            .map_err(|e| CodecError(format!("JSON encode: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Greeting { name: String }

    #[test]
    fn json_encode_decode_roundtrip() {
        let g = Greeting { name: "world".to_string() };
        let encoded = g.encode().unwrap();
        let decoded  = Greeting::decode(&encoded).unwrap();
        assert_eq!(decoded, g);
    }

    #[test]
    fn decode_error_on_invalid_json() {
        let result = Greeting::decode(b"not json");
        assert!(result.is_err());
        assert!(result.unwrap_err().0.contains("JSON decode"));
    }
}
