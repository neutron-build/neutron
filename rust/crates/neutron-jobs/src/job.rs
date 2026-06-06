//! Job extractors, context, and result type.

use std::time::{Duration, SystemTime};

use http::{HeaderMap, StatusCode};
use neutron::extract::{FromRequest, FromRequestParts};
use neutron::handler::{IntoResponse, Request, Response};
use serde::de::DeserializeOwned;

// ---------------------------------------------------------------------------
// Job<T> — payload extractor
// ---------------------------------------------------------------------------

/// Extract a deserialized job payload from the request body.
///
/// The `JobWorker` serializes the payload as JSON into the synthetic request
/// body, so `Job<T>` is equivalent to `Json<T>` without the Content-Type check.
///
/// ```rust,ignore
/// #[derive(serde::Deserialize)]
/// struct SendEmail { to: String, subject: String }
///
/// async fn send_email(Job(email): Job<SendEmail>) -> JobResult {
///     // email.to, email.subject
///     JobResult::Ok
/// }
/// ```
pub struct Job<T>(pub T);

impl<T: DeserializeOwned + Send + 'static> FromRequest for Job<T> {
    fn from_request(req: &Request) -> Result<Self, Response> {
        serde_json::from_slice(req.body())
            .map(Job)
            .map_err(|e| {
                (StatusCode::BAD_REQUEST, format!("Invalid job payload: {e}")).into_response()
            })
    }
}

// ---------------------------------------------------------------------------
// JobContext — metadata extractor
// ---------------------------------------------------------------------------

/// Metadata about the currently executing job — set by `JobWorker` before
/// each handler invocation.
///
/// ```rust,ignore
/// async fn my_job(ctx: JobContext, Job(payload): Job<Payload>) -> JobResult {
///     tracing::info!(attempt = ctx.attempt, job_id = %ctx.job_id, "executing");
///     JobResult::Ok
/// }
/// ```
#[derive(Debug, Clone)]
pub struct JobContext {
    pub job_id:       String,
    pub job_type:     String,
    pub attempt:      u32,
    pub max_attempts: u32,
    pub scheduled_at: SystemTime,
    pub queue:        String,
}

impl FromRequestParts for JobContext {
    fn from_parts(req: &Request) -> Result<Self, Response> {
        req.get_extension::<JobContext>()
            .cloned()
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "JobContext not set — was this handler called outside a JobWorker?",
                )
                    .into_response()
            })
    }
}

// ---------------------------------------------------------------------------
// JobResult — return type
// ---------------------------------------------------------------------------

/// The outcome of a job handler — returned to the `JobWorker` to decide
/// what to do next.
#[derive(Debug)]
pub enum JobResult {
    /// Job completed successfully.
    Ok,
    /// Retry this job after `delay`. The worker will re-enqueue it.
    Retry {
        delay:  Duration,
        reason: Option<String>,
    },
    /// Permanent failure — do not retry.
    Dead { reason: String },
}

impl JobResult {
    pub fn retry_after(delay: Duration) -> Self {
        Self::Retry { delay, reason: None }
    }

    pub fn retry_with_reason(delay: Duration, reason: impl Into<String>) -> Self {
        Self::Retry { delay, reason: Some(reason.into()) }
    }

    pub fn dead(reason: impl Into<String>) -> Self {
        Self::Dead { reason: reason.into() }
    }
}

/// `JobResult` serializes into a synthetic HTTP response that the `JobWorker`
/// interprets to decide whether to complete, retry, or discard the job.
///
/// - `Ok`    → 200
/// - `Retry` → 202 + `x-retry-after-ms` / `x-retry-reason` headers
/// - `Dead`  → 500
impl IntoResponse for JobResult {
    fn into_response(self) -> Response {
        match self {
            JobResult::Ok => StatusCode::OK.into_response(),

            JobResult::Retry { delay, reason } => {
                let mut headers = HeaderMap::new();
                if let Ok(v) = delay.as_millis().to_string().parse() {
                    headers.insert("x-retry-after-ms", v);
                }
                if let Some(r) = reason {
                    if let Ok(v) = r.parse() {
                        headers.insert("x-retry-reason", v);
                    }
                }
                (StatusCode::ACCEPTED, headers, "").into_response()
            }

            JobResult::Dead { reason } => {
                (StatusCode::INTERNAL_SERVER_ERROR, reason).into_response()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal: parse a synthetic response back into a job outcome
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub(crate) enum JobOutcome {
    Completed,
    Retry(Duration),
    Failed,
}

pub(crate) fn parse_response(resp: &Response) -> JobOutcome {
    // ACCEPTED (202) must be checked before is_success() — 202 is a 2xx code.
    match resp.status() {
        StatusCode::ACCEPTED => {
            let ms = resp
                .headers()
                .get("x-retry-after-ms")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(5_000);
            JobOutcome::Retry(Duration::from_millis(ms))
        }
        s if s.is_success() => JobOutcome::Completed,
        _ => JobOutcome::Failed,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use http_body_util::BodyExt;
    use neutron::handler::Request;

    // -- JobResult into Response -------------------------------------------

    #[tokio::test]
    async fn job_result_ok_is_200() {
        let resp = JobResult::Ok.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn job_result_retry_is_202_with_header() {
        let resp = JobResult::retry_after(Duration::from_secs(10)).into_response();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(resp.headers().get("x-retry-after-ms").unwrap(), "10000");
    }

    #[tokio::test]
    async fn job_result_retry_with_reason() {
        let resp = JobResult::retry_with_reason(
            Duration::from_millis(500),
            "downstream unavailable",
        )
        .into_response();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(resp.headers().get("x-retry-after-ms").unwrap(), "500");
        assert_eq!(resp.headers().get("x-retry-reason").unwrap(), "downstream unavailable");
    }

    #[tokio::test]
    async fn job_result_dead_is_500() {
        let resp = JobResult::dead("unrecoverable error").into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, "unrecoverable error");
    }

    // -- parse_response ----------------------------------------------------

    #[test]
    fn parse_200_as_completed() {
        let resp = StatusCode::OK.into_response();
        assert!(matches!(parse_response(&resp), JobOutcome::Completed));
    }

    #[test]
    fn parse_202_as_retry_with_delay() {
        let resp = JobResult::retry_after(Duration::from_millis(3000)).into_response();
        match parse_response(&resp) {
            JobOutcome::Retry(d) => assert_eq!(d.as_millis(), 3000),
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    #[test]
    fn parse_500_as_failed() {
        let resp = StatusCode::INTERNAL_SERVER_ERROR.into_response();
        assert!(matches!(parse_response(&resp), JobOutcome::Failed));
    }

    // Helper: unwrap Result<T, Response> without requiring Response: Debug
    fn ok_or_panic<T>(r: Result<T, Response>, msg: &str) -> T {
        match r {
            Ok(v)    => v,
            Err(resp) => panic!("{msg}: HTTP {}", resp.status()),
        }
    }

    // -- Job<T> extractor --------------------------------------------------

    #[test]
    fn job_extractor_deserializes_json_body() {
        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct Payload { id: u64, name: String }

        let body = serde_json::to_vec(&serde_json::json!({"id": 1, "name": "alice"})).unwrap();
        let req = Request::new(
            Method::POST, "/".parse().unwrap(), HeaderMap::new(), Bytes::from(body),
        );

        let Job(p) = ok_or_panic(Job::<Payload>::from_request(&req), "Job extractor failed");
        assert_eq!(p, Payload { id: 1, name: "alice".to_string() });
    }

    #[test]
    fn job_extractor_fails_on_bad_json() {
        let req = Request::new(
            Method::POST,
            "/".parse().unwrap(),
            HeaderMap::new(),
            Bytes::from_static(b"not json"),
        );
        assert!(Job::<serde_json::Value>::from_request(&req).is_err());
    }

    // -- JobContext extractor ----------------------------------------------

    #[test]
    fn job_context_extracted_from_extension() {
        let ctx = JobContext {
            job_id:       "abc-123".to_string(),
            job_type:     "email".to_string(),
            attempt:      2,
            max_attempts: 5,
            scheduled_at: SystemTime::now(),
            queue:        "default".to_string(),
        };

        let mut req = Request::new(
            Method::POST, "/".parse().unwrap(), HeaderMap::new(), Bytes::new(),
        );
        req.set_extension(ctx.clone());

        let extracted = ok_or_panic(JobContext::from_parts(&req), "JobContext extraction failed");
        assert_eq!(extracted.job_id, "abc-123");
        assert_eq!(extracted.attempt, 2);
    }

    #[test]
    fn job_context_fails_without_extension() {
        let req = Request::new(
            Method::POST, "/".parse().unwrap(), HeaderMap::new(), Bytes::new(),
        );
        assert!(JobContext::from_parts(&req).is_err());
    }
}
