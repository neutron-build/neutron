//! End-to-end integration tests for the S3 gateway: a real server on an
//! ephemeral port, driven over TCP with SigV4-signed HTTP requests.
//!
//! The client-side signer here reuses the public sigv4 helpers — the
//! algorithm itself is anchored separately by the AWS documentation test
//! vectors in `src/s3/sigv4.rs`, so these tests exercise the full HTTP
//! parse → auth → route → blob-store → XML path, not the signature math.
#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::s3::sigv4::{
    AuthData, CanonicalRequestInput, UNSIGNED_PAYLOAD, aws_uri_encode, compute_signature,
    percent_decode, sha256_hex,
};
use nucleus::s3::{S3Config, S3ServerConfig};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const ACCESS: &str = "testkey";
const SECRET: &str = "testsecret";

async fn spawn_gateway() -> (u16, Arc<tokio::sync::Notify>) {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let executor = Arc::new(Executor::new(catalog, storage));
    let config = Arc::new(S3Config {
        access_key: ACCESS.to_string(),
        secret_key: SECRET.to_string(),
        max_object_bytes: 64 * 1024 * 1024,
    });
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let shutdown2 = Arc::clone(&shutdown);
    tokio::spawn(async move {
        let _ = nucleus::s3::server::serve(
            listener,
            executor,
            config,
            shutdown2,
            S3ServerConfig::default(),
        )
        .await;
    });
    (port, shutdown)
}

fn amz_now() -> String {
    // Format the current time as YYYYMMDDTHHMMSSZ using only std.
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    // civil-from-days
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    format!(
        "{year:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// Send one signed request and read the response.
async fn request(
    port: u16,
    method: &str,
    path_and_query: &str,
    body: &[u8],
    content_type: Option<&str>,
) -> HttpResponse {
    let (raw_path, raw_query) = match path_and_query.split_once('?') {
        Some((p, q)) => (p, q),
        None => (path_and_query, ""),
    };
    // Canonicalization operates on DECODED parameters (the signer re-encodes).
    let query: Vec<(String, String)> = raw_query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(v)),
            None => (percent_decode(pair), String::new()),
        })
        .collect();

    let host = format!("127.0.0.1:{port}");
    let amz_date = amz_now();
    let payload_hash = sha256_hex(body);
    let scope = format!("{}/local/s3/aws4_request", &amz_date[..8]);

    let mut signed_header_pairs = vec![
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    if let Some(ct) = content_type {
        signed_header_pairs.insert(0, ("content-type".to_string(), ct.to_string()));
    }
    let signed_names: Vec<String> = signed_header_pairs.iter().map(|(k, _)| k.clone()).collect();

    let auth = AuthData {
        access_key: ACCESS.to_string(),
        scope: scope.clone(),
        signed_headers: signed_names.clone(),
        signature: String::new(),
        amz_date: amz_date.clone(),
        expires: None,
        presigned: false,
    };
    let input = CanonicalRequestInput {
        method,
        raw_path,
        query: &query,
        headers: &signed_header_pairs,
        payload_hash: &payload_hash,
    };
    let signature = compute_signature(SECRET, &auth, &input).unwrap();

    let mut req = format!("{method} {path_and_query} HTTP/1.1\r\nHost: {host}\r\n");
    if let Some(ct) = content_type {
        req.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    req.push_str(&format!(
        "x-amz-date: {amz_date}\r\nx-amz-content-sha256: {payload_hash}\r\n\
         Authorization: AWS4-HMAC-SHA256 Credential={ACCESS}/{scope}, \
         SignedHeaders={}, Signature={signature}\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        signed_names.join(";"),
        body.len()
    ));

    send_raw(port, req.into_bytes(), body).await
}

async fn send_raw(port: u16, head: Vec<u8>, body: &[u8]) -> HttpResponse {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.write_all(&head).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    parse_response(&raw)
}

fn parse_response(raw: &[u8]) -> HttpResponse {
    let header_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("no header terminator");
    let head = std::str::from_utf8(&raw[..header_end]).unwrap();
    let mut lines = head.lines();
    let status: u16 = lines
        .next()
        .unwrap()
        .split(' ')
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_lowercase(), v.trim().to_string()))
        .collect();
    let body = raw[header_end + 4..].to_vec();
    HttpResponse {
        status,
        headers,
        body,
    }
}

#[tokio::test]
async fn s3_object_lifecycle() {
    let (port, _shutdown) = spawn_gateway().await;

    // Unknown bucket → 404 NoSuchBucket
    let r = request(port, "GET", "/nope/key", b"", None).await;
    assert_eq!(r.status, 404);
    assert!(r.text().contains("NoSuchBucket"));

    // Create bucket
    let r = request(port, "PUT", "/media", b"", None).await;
    assert_eq!(r.status, 200, "{}", r.text());
    // Duplicate create → 409
    let r = request(port, "PUT", "/media", b"", None).await;
    assert_eq!(r.status, 409);
    // HEAD bucket
    let r = request(port, "HEAD", "/media", b"", None).await;
    assert_eq!(r.status, 200);

    // PUT object (signed payload)
    let data = b"hello, s3 world".repeat(100);
    let r = request(
        port,
        "PUT",
        "/media/docs/hello.txt",
        &data,
        Some("text/plain"),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let etag = r.header("etag").unwrap().to_string();
    assert!(etag.starts_with('"') && etag.ends_with('"'));

    // GET object round-trip
    let r = request(port, "GET", "/media/docs/hello.txt", b"", None).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body, data);
    assert_eq!(r.header("content-type"), Some("text/plain"));
    assert_eq!(r.header("etag").unwrap(), etag);

    // HEAD object: no body, correct advertised length
    let r = request(port, "HEAD", "/media/docs/hello.txt", b"", None).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-length").unwrap(), data.len().to_string());
    assert!(r.body.is_empty());

    // Missing key in existing bucket → NoSuchKey
    let r = request(port, "GET", "/media/docs/none.txt", b"", None).await;
    assert_eq!(r.status, 404);
    assert!(r.text().contains("NoSuchKey"));

    // DELETE object → 204; then gone
    let r = request(port, "DELETE", "/media/docs/hello.txt", b"", None).await;
    assert_eq!(r.status, 204);
    let r = request(port, "GET", "/media/docs/hello.txt", b"", None).await;
    assert_eq!(r.status, 404);
}

#[tokio::test]
async fn s3_range_reads() {
    let (port, _shutdown) = spawn_gateway().await;
    request(port, "PUT", "/rng", b"", None).await;
    let data: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let r = request(port, "PUT", "/rng/blob.bin", &data, None).await;
    assert_eq!(r.status, 200);

    // Signed range GET (Range is an unsigned header — send it raw).
    let mut r =
        request_with_extra_header(port, "GET", "/rng/blob.bin", "Range: bytes=1000-1999").await;
    assert_eq!(r.status, 206);
    assert_eq!(r.body, data[1000..2000].to_vec());
    assert_eq!(r.header("content-range").unwrap(), "bytes 1000-1999/100000");

    r = request_with_extra_header(port, "GET", "/rng/blob.bin", "Range: bytes=-500").await;
    assert_eq!(r.status, 206);
    assert_eq!(r.body, data[99500..].to_vec());

    r = request_with_extra_header(port, "GET", "/rng/blob.bin", "Range: bytes=200000-").await;
    assert_eq!(r.status, 416);
}

/// Signed request with one extra unsigned header line.
async fn request_with_extra_header(
    port: u16,
    method: &str,
    path: &str,
    extra: &str,
) -> HttpResponse {
    let host = format!("127.0.0.1:{port}");
    let amz_date = amz_now();
    let payload_hash = sha256_hex(b"");
    let scope = format!("{}/local/s3/aws4_request", &amz_date[..8]);
    let signed_header_pairs = vec![
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    let auth = AuthData {
        access_key: ACCESS.to_string(),
        scope: scope.clone(),
        signed_headers: vec![
            "host".into(),
            "x-amz-content-sha256".into(),
            "x-amz-date".into(),
        ],
        signature: String::new(),
        amz_date: amz_date.clone(),
        expires: None,
        presigned: false,
    };
    let input = CanonicalRequestInput {
        method,
        raw_path: path,
        query: &[],
        headers: &signed_header_pairs,
        payload_hash: &payload_hash,
    };
    let signature = compute_signature(SECRET, &auth, &input).unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\n{extra}\r\n\
         x-amz-date: {amz_date}\r\nx-amz-content-sha256: {payload_hash}\r\n\
         Authorization: AWS4-HMAC-SHA256 Credential={ACCESS}/{scope}, \
         SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}\r\n\
         Content-Length: 0\r\nConnection: close\r\n\r\n"
    );
    send_raw(port, req.into_bytes(), b"").await
}

#[tokio::test]
async fn s3_listing_and_batch_delete() {
    let (port, _shutdown) = spawn_gateway().await;
    request(port, "PUT", "/files", b"", None).await;
    for key in ["a/1.txt", "a/2.txt", "b/3.txt", "top.txt"] {
        let r = request(port, "PUT", &format!("/files/{key}"), b"x", None).await;
        assert_eq!(r.status, 200);
    }

    // Full V2 list
    let r = request(port, "GET", "/files?list-type=2", b"", None).await;
    assert_eq!(r.status, 200);
    let body = r.text();
    assert!(body.contains("<Key>a/1.txt</Key>"));
    assert!(body.contains("<Key>top.txt</Key>"));
    assert!(body.contains("<KeyCount>4</KeyCount>"));

    // Prefix filter
    let r = request(port, "GET", "/files?list-type=2&prefix=a%2F", b"", None).await;
    let body = r.text();
    assert!(body.contains("a/1.txt") && body.contains("a/2.txt"));
    assert!(!body.contains("b/3.txt"));

    // Delimiter grouping
    let r = request(port, "GET", "/files?delimiter=%2F&list-type=2", b"", None).await;
    let body = r.text();
    assert!(
        body.contains("<Prefix>a/</Prefix>")
            || body.contains("<CommonPrefixes><Prefix>a/</Prefix>")
    );
    assert!(body.contains("<Key>top.txt</Key>"));
    assert!(!body.contains("<Key>a/1.txt</Key>"));

    // Pagination
    let r = request(port, "GET", "/files?list-type=2&max-keys=2", b"", None).await;
    let body = r.text();
    assert!(body.contains("<IsTruncated>true</IsTruncated>"));
    assert!(body.contains("<NextContinuationToken>"));

    // Bucket delete refused while non-empty
    let r = request(port, "DELETE", "/files", b"", None).await;
    assert_eq!(r.status, 409);

    // Batch delete
    let del = "<Delete><Object><Key>a/1.txt</Key></Object><Object><Key>a/2.txt</Key></Object>\
               <Object><Key>b/3.txt</Key></Object><Object><Key>top.txt</Key></Object></Delete>";
    let r = request(port, "POST", "/files?delete", del.as_bytes(), None).await;
    assert_eq!(r.status, 200);
    assert!(r.text().matches("<Deleted>").count() == 4);

    // Now empty → bucket delete OK
    let r = request(port, "DELETE", "/files", b"", None).await;
    assert_eq!(r.status, 204);
}

#[tokio::test]
async fn s3_multipart_and_copy() {
    let (port, _shutdown) = spawn_gateway().await;
    request(port, "PUT", "/big", b"", None).await;

    // Initiate
    let r = request(
        port,
        "POST",
        "/big/video.bin?uploads",
        b"",
        Some("video/mp4"),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let body = r.text();
    let upload_id = body
        .split("<UploadId>")
        .nth(1)
        .unwrap()
        .split("</UploadId>")
        .next()
        .unwrap()
        .to_string();

    // Upload three parts
    let part1 = vec![1u8; 3 * 1024 * 1024];
    let part2 = vec![2u8; 2 * 1024 * 1024];
    let part3 = vec![3u8; 100];
    let mut etags = Vec::new();
    for (n, part) in [(1, &part1), (2, &part2), (3, &part3)] {
        let r = request(
            port,
            "PUT",
            &format!("/big/video.bin?partNumber={n}&uploadId={upload_id}"),
            part,
            None,
        )
        .await;
        assert_eq!(r.status, 200, "part {n}: {}", r.text());
        etags.push(r.header("etag").unwrap().trim_matches('"').to_string());
    }

    // Complete (zero-copy compose)
    let complete_xml = format!(
        "<CompleteMultipartUpload>\
         <Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part>\
         <Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part>\
         <Part><PartNumber>3</PartNumber><ETag>{}</ETag></Part>\
         </CompleteMultipartUpload>",
        etags[0], etags[1], etags[2]
    );
    let r = request(
        port,
        "POST",
        &format!("/big/video.bin?uploadId={upload_id}"),
        complete_xml.as_bytes(),
        None,
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(r.text().contains("-3&quot;")); // multipart etag suffix

    // The assembled object reads back byte-identical.
    let r = request(port, "GET", "/big/video.bin", b"", None).await;
    assert_eq!(r.status, 200);
    let mut expected = part1.clone();
    expected.extend_from_slice(&part2);
    expected.extend_from_slice(&part3);
    assert_eq!(r.body.len(), expected.len());
    assert_eq!(r.body, expected);
    assert_eq!(r.header("content-type"), Some("video/mp4"));

    // Range read across a part boundary.
    let r = request_with_extra_header(
        port,
        "GET",
        "/big/video.bin",
        "Range: bytes=3145700-3145760",
    )
    .await;
    assert_eq!(r.status, 206);
    assert_eq!(r.body, expected[3145700..=3145760].to_vec());

    // Parts are cleaned up (no lingering multipart uploads).
    let r = request(port, "GET", "/big?uploads", b"", None).await;
    assert!(!r.text().contains("<Upload>"));

    // Server-side copy (zero-copy) into another bucket.
    request(port, "PUT", "/copies", b"", None).await;
    let r = request_with_copy_source(port, "/copies/video-copy.bin", "/big/video.bin").await;
    assert_eq!(r.status, 200, "{}", r.text());
    let r = request(port, "GET", "/copies/video-copy.bin", b"", None).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body, expected);
}

/// Signed PUT with an x-amz-copy-source header (signed into the request).
async fn request_with_copy_source(port: u16, dst: &str, src: &str) -> HttpResponse {
    let host = format!("127.0.0.1:{port}");
    let amz_date = amz_now();
    let payload_hash = sha256_hex(b"");
    let scope = format!("{}/local/s3/aws4_request", &amz_date[..8]);
    let headers = vec![
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-copy-source".to_string(), src.to_string()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    let auth = AuthData {
        access_key: ACCESS.to_string(),
        scope: scope.clone(),
        signed_headers: headers.iter().map(|(k, _)| k.clone()).collect(),
        signature: String::new(),
        amz_date: amz_date.clone(),
        expires: None,
        presigned: false,
    };
    let input = CanonicalRequestInput {
        method: "PUT",
        raw_path: dst,
        query: &[],
        headers: &headers,
        payload_hash: &payload_hash,
    };
    let signature = compute_signature(SECRET, &auth, &input).unwrap();
    let req = format!(
        "PUT {dst} HTTP/1.1\r\nHost: {host}\r\nx-amz-copy-source: {src}\r\n\
         x-amz-date: {amz_date}\r\nx-amz-content-sha256: {payload_hash}\r\n\
         Authorization: AWS4-HMAC-SHA256 Credential={ACCESS}/{scope}, \
         SignedHeaders=host;x-amz-content-sha256;x-amz-copy-source;x-amz-date, \
         Signature={signature}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    );
    send_raw(port, req.into_bytes(), b"").await
}

#[tokio::test]
async fn s3_presigned_url() {
    let (port, _shutdown) = spawn_gateway().await;
    request(port, "PUT", "/pre", b"", None).await;
    request(port, "PUT", "/pre/file.txt", b"presigned content", None).await;

    let host = format!("127.0.0.1:{port}");
    let amz_date = amz_now();
    let scope = format!("{}/local/s3/aws4_request", &amz_date[..8]);
    let credential = aws_uri_encode(&format!("{ACCESS}/{scope}"), true);
    let query_pairs = vec![
        (
            "X-Amz-Algorithm".to_string(),
            "AWS4-HMAC-SHA256".to_string(),
        ),
        ("X-Amz-Credential".to_string(), format!("{ACCESS}/{scope}")),
        ("X-Amz-Date".to_string(), amz_date.clone()),
        ("X-Amz-Expires".to_string(), "300".to_string()),
        ("X-Amz-SignedHeaders".to_string(), "host".to_string()),
    ];
    let headers = vec![("host".to_string(), host.clone())];
    let auth = AuthData {
        access_key: ACCESS.to_string(),
        scope: scope.clone(),
        signed_headers: vec!["host".into()],
        signature: String::new(),
        amz_date: amz_date.clone(),
        expires: Some(300),
        presigned: true,
    };
    let input = CanonicalRequestInput {
        method: "GET",
        raw_path: "/pre/file.txt",
        query: &query_pairs,
        headers: &headers,
        payload_hash: UNSIGNED_PAYLOAD,
    };
    let signature = compute_signature(SECRET, &auth, &input).unwrap();

    // No Authorization header — auth entirely in the query string.
    let url = format!(
        "/pre/file.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={credential}\
         &X-Amz-Date={amz_date}&X-Amz-Expires=300&X-Amz-SignedHeaders=host\
         &X-Amz-Signature={signature}"
    );
    let req = format!("GET {url} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    let r = send_raw(port, req.into_bytes(), b"").await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.body, b"presigned content");

    // Tampered signature → 403.
    let bad = url.replace("X-Amz-Signature=", "X-Amz-Signature=0");
    let req = format!("GET {bad} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    let r = send_raw(port, req.into_bytes(), b"").await;
    assert_eq!(r.status, 403);
}

#[tokio::test]
async fn s3_auth_failures() {
    let (port, _shutdown) = spawn_gateway().await;

    // No auth at all
    let req = format!("GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    let r = send_raw(port, req.into_bytes(), b"").await;
    assert_eq!(r.status, 403);

    // Wrong secret: sign with a different key.
    let host = format!("127.0.0.1:{port}");
    let amz_date = amz_now();
    let payload_hash = sha256_hex(b"");
    let scope = format!("{}/local/s3/aws4_request", &amz_date[..8]);
    let headers = vec![
        ("host".to_string(), host.clone()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    let auth = AuthData {
        access_key: ACCESS.to_string(),
        scope: scope.clone(),
        signed_headers: vec![
            "host".into(),
            "x-amz-content-sha256".into(),
            "x-amz-date".into(),
        ],
        signature: String::new(),
        amz_date: amz_date.clone(),
        expires: None,
        presigned: false,
    };
    let input = CanonicalRequestInput {
        method: "GET",
        raw_path: "/",
        query: &[],
        headers: &headers,
        payload_hash: &payload_hash,
    };
    let bad_sig = compute_signature("wrong-secret", &auth, &input).unwrap();
    let req = format!(
        "GET / HTTP/1.1\r\nHost: {host}\r\nx-amz-date: {amz_date}\r\n\
         x-amz-content-sha256: {payload_hash}\r\n\
         Authorization: AWS4-HMAC-SHA256 Credential={ACCESS}/{scope}, \
         SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={bad_sig}\r\n\
         Connection: close\r\n\r\n"
    );
    let r = send_raw(port, req.into_bytes(), b"").await;
    assert_eq!(r.status, 403);
    assert!(r.text().contains("SignatureDoesNotMatch"));

    // Unknown access key
    let r = {
        let auth = AuthData {
            access_key: "intruder".to_string(),
            ..auth
        };
        let sig = compute_signature(SECRET, &auth, &input).unwrap();
        let req = format!(
            "GET / HTTP/1.1\r\nHost: {host}\r\nx-amz-date: {amz_date}\r\n\
             x-amz-content-sha256: {payload_hash}\r\n\
             Authorization: AWS4-HMAC-SHA256 Credential=intruder/{scope}, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}\r\n\
             Connection: close\r\n\r\n"
        );
        send_raw(port, req.into_bytes(), b"").await
    };
    assert_eq!(r.status, 403);
    assert!(r.text().contains("InvalidAccessKeyId"));

    // Tampered payload (hash mismatch)
    let body = b"tampered";
    let wrong_hash = sha256_hex(b"original");
    let req = format!(
        "PUT /any/key HTTP/1.1\r\nHost: {host}\r\nx-amz-date: {amz_date}\r\n\
         x-amz-content-sha256: {wrong_hash}\r\n\
         Authorization: AWS4-HMAC-SHA256 Credential={ACCESS}/{scope}, \
         SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=abc\r\n\
         Content-Length: 8\r\nConnection: close\r\n\r\n"
    );
    let r = send_raw(port, req.into_bytes(), body).await;
    assert_eq!(r.status, 400);
    assert!(r.text().contains("XAmzContentSHA256Mismatch"));
}

#[tokio::test]
async fn s3_list_buckets_and_special_keys() {
    let (port, _shutdown) = spawn_gateway().await;
    request(port, "PUT", "/alpha", b"", None).await;
    request(port, "PUT", "/beta", b"", None).await;
    let r = request(port, "GET", "/", b"", None).await;
    assert_eq!(r.status, 200);
    let body = r.text();
    assert!(body.contains("<Name>alpha</Name>"));
    assert!(body.contains("<Name>beta</Name>"));

    // Keys with spaces and unicode survive the encode/decode round trip.
    let key_path = "/alpha/dir%20name/f%C3%BCn%20file.txt";
    let r = request(port, "PUT", key_path, b"space data", None).await;
    assert_eq!(r.status, 200, "{}", r.text());
    let r = request(port, "GET", key_path, b"", None).await;
    assert_eq!(r.status, 200);
    assert_eq!(r.body, b"space data");
    let r = request(port, "GET", "/alpha?list-type=2", b"", None).await;
    assert!(r.text().contains("dir name/fün file.txt"));
}
