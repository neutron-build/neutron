//! ML inference client and streaming response types for the Neutron framework.
//!
//! Connects to a running [neutron-mojo](https://github.com/nicholasgriffintn/tystack)
//! inference server over HTTP and integrates its output into Neutron handlers.
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use neutron::prelude::*;
//! use neutron_inference::{
//!     InferenceClient, InferenceClientConfig, InferenceRequest, InferStream,
//! };
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = InferenceClient::new(InferenceClientConfig::default());
//!
//!     let router = Router::new()
//!         .state(client)
//!         .post("/generate", generate)
//!         .post("/stream",   stream_generate);
//!
//!     Neutron::new().router(router).listen("0.0.0.0:3000".parse().unwrap()).await.unwrap();
//! }
//!
//! /// Non-streaming: wait for the full response and return JSON.
//! async fn generate(
//!     State(client): State<InferenceClient>,
//!     Json(req): Json<InferenceRequest>,
//! ) -> Result<Json<InferenceResponse>, (StatusCode, String)> {
//!     client.complete(req).await
//!         .map(Json)
//!         .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))
//! }
//!
//! /// Streaming: forward token stream as Server-Sent Events.
//! async fn stream_generate(
//!     State(client): State<InferenceClient>,
//!     Json(req): Json<InferenceRequest>,
//! ) -> InferStream {
//!     InferStream::new(client.stream(req).await)
//! }
//! ```

pub mod client;
pub mod error;
pub mod request;
pub mod response;
pub mod stream;

pub use client::{InferenceClient, InferenceClientConfig};
pub use error::InferError;
pub use request::{InferenceRequest, SamplingParams};
pub use response::{FinishReason, InferenceChunk, InferenceResponse};
pub use stream::InferStream;
