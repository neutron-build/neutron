//! Inference request types.

use serde::{Deserialize, Serialize};

/// Sampling parameters for text generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingParams {
    /// Sampling temperature — higher = more creative (default: 0.7).
    #[serde(default = "default_temperature")]
    pub temperature: f32,

    /// Top-p nucleus sampling threshold (default: 0.9).
    #[serde(default = "default_top_p")]
    pub top_p: f32,

    /// Top-k sampling (0 = disabled, default).
    #[serde(default)]
    pub top_k: u32,

    /// Repetition penalty (1.0 = none, default).
    #[serde(default = "default_rep_penalty")]
    pub repetition_penalty: f32,
}

fn default_temperature() -> f32 { 0.7 }
fn default_top_p()       -> f32 { 0.9 }
fn default_rep_penalty() -> f32 { 1.0 }

impl Default for SamplingParams {
    fn default() -> Self {
        Self {
            temperature:       default_temperature(),
            top_p:             default_top_p(),
            top_k:             0,
            repetition_penalty: default_rep_penalty(),
        }
    }
}

/// A request to an inference server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceRequest {
    /// The input prompt.
    pub prompt: String,

    /// Maximum number of tokens to generate (default: 256).
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,

    /// Sampling parameters.
    #[serde(default)]
    pub sampling: SamplingParams,

    /// Whether to stream the response token-by-token (default: false).
    #[serde(default)]
    pub stream: bool,

    /// Optional stop sequences — generation halts at any of these strings.
    #[serde(default)]
    pub stop: Vec<String>,
}

fn default_max_tokens() -> u32 { 256 }

impl InferenceRequest {
    pub fn new(prompt: impl Into<String>) -> Self {
        Self {
            prompt:     prompt.into(),
            max_tokens: default_max_tokens(),
            sampling:   SamplingParams::default(),
            stream:     false,
            stop:       Vec::new(),
        }
    }

    pub fn max_tokens(mut self, n: u32) -> Self { self.max_tokens = n; self }
    pub fn temperature(mut self, t: f32) -> Self { self.sampling.temperature = t; self }
    pub fn stream(mut self) -> Self { self.stream = true; self }
    pub fn stop(mut self, seq: impl Into<String>) -> Self { self.stop.push(seq.into()); self }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_sets_fields() {
        let req = InferenceRequest::new("hello")
            .max_tokens(512)
            .temperature(0.5)
            .stream()
            .stop("<|endoftext|>");

        assert_eq!(req.prompt, "hello");
        assert_eq!(req.max_tokens, 512);
        assert!((req.sampling.temperature - 0.5).abs() < 1e-6);
        assert!(req.stream);
        assert_eq!(req.stop, ["<|endoftext|>"]);
    }

    #[test]
    fn json_roundtrip() {
        let req = InferenceRequest::new("test prompt").max_tokens(64);
        let json = serde_json::to_string(&req).unwrap();
        let decoded: InferenceRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.prompt, "test prompt");
        assert_eq!(decoded.max_tokens, 64);
    }
}
