//! Inference response types.

use serde::{Deserialize, Serialize};

/// Why generation stopped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// The model hit its `max_tokens` limit.
    Length,
    /// The model produced an end-of-sequence token.
    Stop,
    /// A stop sequence from the request was matched.
    StopSequence,
}

/// Complete (non-streaming) inference response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResponse {
    /// The generated text.
    pub text: String,
    /// Number of tokens generated.
    pub tokens_generated: u32,
    /// Number of prompt tokens consumed.
    pub prompt_tokens: u32,
    /// Why generation finished.
    pub finish_reason: FinishReason,
    /// Wall-clock time for the request in milliseconds.
    pub latency_ms: u64,
}

/// One chunk of a streaming inference response.
///
/// The client receives a sequence of these followed by a final chunk
/// where `done == true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceChunk {
    /// Incremental text delta — append to build the full response.
    pub delta: String,
    /// Whether this is the last chunk.
    pub done: bool,
    /// Finish reason (only set on the last chunk).
    pub finish_reason: Option<FinishReason>,
}

impl InferenceChunk {
    pub fn data(delta: impl Into<String>) -> Self {
        Self { delta: delta.into(), done: false, finish_reason: None }
    }

    pub fn final_chunk(delta: impl Into<String>, reason: FinishReason) -> Self {
        Self { delta: delta.into(), done: true, finish_reason: Some(reason) }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_done_flag() {
        let c = InferenceChunk::data("hello ");
        assert!(!c.done);
        assert!(c.finish_reason.is_none());

        let f = InferenceChunk::final_chunk("", FinishReason::Stop);
        assert!(f.done);
        assert_eq!(f.finish_reason.unwrap(), FinishReason::Stop);
    }

    #[test]
    fn response_json_roundtrip() {
        let r = InferenceResponse {
            text:             "test".to_string(),
            tokens_generated: 10,
            prompt_tokens:    5,
            finish_reason:    FinishReason::Length,
            latency_ms:       42,
        };
        let json = serde_json::to_string(&r).unwrap();
        let r2: InferenceResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(r2.text, "test");
        assert_eq!(r2.finish_reason, FinishReason::Length);
    }
}
