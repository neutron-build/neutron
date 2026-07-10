//! S3 operation handlers over the Nucleus blob store.
//!
//! Layout inside the blob store:
//!   - objects:            `s3/{bucket}/{key}`
//!   - bucket markers:     `s3meta/bucket/{bucket}`
//!   - multipart meta:     `s3mpu/{bucket}/{uploadId}/meta` (data = object key)
//!   - multipart parts:    `s3mpu/{bucket}/{uploadId}/part/{NNNNN}`
//!
//! ETags are BLAKE3 (hex) rather than MD5 — ETag is an opaque string per the
//! S3 contract; multipart ETags get the standard `-{parts}` suffix.
//! CompleteMultipartUpload and CopyObject are **zero-copy**: they compose the
//! target manifest from existing content-addressed chunks.

use std::sync::Arc;

use crate::blob::BlobStore;
use crate::executor::Executor;

use super::http::{Request, Response, http_date, iso8601, xml_escape, xml_extract_all};
use super::sigv4::{
    self, AuthData, CanonicalRequestInput, STREAMING_PAYLOAD, UNSIGNED_PAYLOAD, percent_decode,
    sha256_hex,
};

pub struct S3Config {
    pub access_key: String,
    pub secret_key: String,
    /// Maximum single-request object size (bytes).
    pub max_object_bytes: usize,
}

pub struct S3Handler {
    executor: Arc<Executor>,
    config: Arc<S3Config>,
}

const XML_CT: &str = "application/xml";

fn obj_key(bucket: &str, key: &str) -> String {
    format!("s3/{bucket}/{key}")
}

fn bucket_marker(bucket: &str) -> String {
    format!("s3meta/bucket/{bucket}")
}

fn mpu_meta_key(bucket: &str, upload_id: &str) -> String {
    format!("s3mpu/{bucket}/{upload_id}/meta")
}

fn mpu_part_key(bucket: &str, upload_id: &str, part: u32) -> String {
    format!("s3mpu/{bucket}/{upload_id}/part/{part:05}")
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn error_xml(status: u16, code: &str, message: &str, resource: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code>\
         <Message>{}</Message><Resource>{}</Resource></Error>",
        xml_escape(code),
        xml_escape(message),
        xml_escape(resource)
    );
    Response::with_body(status, XML_CT, body.into_bytes())
}

fn valid_bucket_name(name: &str) -> bool {
    let b = name.as_bytes();
    (3..=63).contains(&b.len())
        && b.iter()
            .all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-' || c == b'.')
        && b[0].is_ascii_alphanumeric()
        && b[b.len() - 1].is_ascii_alphanumeric()
}

/// Blob ETag: prefer the stored tag; otherwise derive one from the manifest
/// (stable — the manifest IS the content).
fn blob_etag(store: &BlobStore, blob_key: &str) -> Option<String> {
    let meta = store.metadata(blob_key)?;
    if let Some(etag) = meta.tags.get("etag") {
        return Some(etag.clone());
    }
    let mut concat = Vec::with_capacity(meta.chunk_hashes.len() * 32);
    for h in &meta.chunk_hashes {
        concat.extend_from_slice(h);
    }
    Some(blake3::hash(&concat).to_hex().to_string())
}

impl S3Handler {
    pub fn new(executor: Arc<Executor>, config: Arc<S3Config>) -> Self {
        Self { executor, config }
    }

    pub fn max_object_bytes(&self) -> usize {
        self.config.max_object_bytes
    }

    /// Authenticate, decode any aws-chunked payload in place, and dispatch.
    pub fn handle(&self, mut req: Request) -> Response {
        match self.authenticate(&mut req) {
            Ok(()) => {}
            Err(resp) => return *resp,
        }

        let decoded_path = percent_decode(&req.raw_path);
        let path = decoded_path.trim_start_matches('/');
        let (bucket, key) = match path.split_once('/') {
            Some((b, k)) => (b.to_string(), k.to_string()),
            None => (path.to_string(), String::new()),
        };

        if bucket.is_empty() {
            return match req.method.as_str() {
                "GET" => self.list_buckets(),
                _ => error_xml(405, "MethodNotAllowed", "unsupported root operation", "/"),
            };
        }
        if !valid_bucket_name(&bucket) {
            return error_xml(400, "InvalidBucketName", "invalid bucket name", &bucket);
        }

        if key.is_empty() {
            return match req.method.as_str() {
                "PUT" => self.create_bucket(&bucket),
                "DELETE" => self.delete_bucket(&bucket),
                "HEAD" => self.head_bucket(&bucket),
                "GET" => self.get_bucket(&bucket, &req),
                "POST" if req.has_query("delete") => self.delete_objects(&bucket, &req),
                _ => error_xml(
                    405,
                    "MethodNotAllowed",
                    "unsupported bucket operation",
                    &bucket,
                ),
            };
        }

        match req.method.as_str() {
            "PUT" if req.has_query("partNumber") && req.has_query("uploadId") => {
                self.upload_part(&bucket, &key, &req)
            }
            "PUT" if req.header("x-amz-copy-source").is_some() => {
                self.copy_object(&bucket, &key, &req)
            }
            "PUT" => self.put_object(&bucket, &key, &req),
            "GET" => self.get_object(&bucket, &key, &req),
            "HEAD" => self.head_object(&bucket, &key, &req),
            "DELETE" if req.has_query("uploadId") => self.abort_multipart(&bucket, &key, &req),
            "DELETE" => self.delete_object(&bucket, &key),
            "POST" if req.has_query("uploads") => self.create_multipart(&bucket, &key, &req),
            "POST" if req.has_query("uploadId") => self.complete_multipart(&bucket, &key, &req),
            _ => error_xml(
                405,
                "MethodNotAllowed",
                "unsupported object operation",
                &key,
            ),
        }
    }

    // ── Authentication ───────────────────────────────────────────────────────

    fn authenticate(&self, req: &mut Request) -> Result<(), Box<Response>> {
        let deny = |status: u16, code: &str, msg: &str| Box::new(error_xml(status, code, msg, "/"));

        let presigned = req.has_query("X-Amz-Signature");
        let auth: AuthData = if presigned {
            sigv4::parse_presigned_query(&req.query).map_err(|e| deny(403, "AccessDenied", &e))?
        } else {
            let header = req
                .header("authorization")
                .ok_or_else(|| deny(403, "AccessDenied", "missing authorization"))?;
            let amz_date = req
                .header("x-amz-date")
                .ok_or_else(|| deny(403, "AccessDenied", "missing x-amz-date"))?
                .to_string();
            sigv4::parse_authorization_header(header, &amz_date)
                .map_err(|e| deny(403, "AccessDenied", &e))?
        };

        if auth.access_key != self.config.access_key {
            return Err(deny(403, "InvalidAccessKeyId", "unknown access key"));
        }

        // Payload hash handling.
        let payload_hash: String = if presigned {
            UNSIGNED_PAYLOAD.to_string()
        } else {
            let declared = req
                .header("x-amz-content-sha256")
                .ok_or_else(|| deny(400, "InvalidRequest", "missing x-amz-content-sha256"))?
                .to_string();
            match declared.as_str() {
                UNSIGNED_PAYLOAD | STREAMING_PAYLOAD => {}
                hex => {
                    if sha256_hex(&req.body) != hex {
                        return Err(deny(
                            400,
                            "XAmzContentSHA256Mismatch",
                            "payload hash mismatch",
                        ));
                    }
                }
            }
            declared
        };

        // Canonical query: presigned requests exclude X-Amz-Signature.
        let query: Vec<(String, String)> = if presigned {
            req.query
                .iter()
                .filter(|(k, _)| k != "X-Amz-Signature")
                .cloned()
                .collect()
        } else {
            req.query.clone()
        };

        let input = CanonicalRequestInput {
            method: &req.method,
            raw_path: &req.raw_path,
            query: &query,
            headers: &req.headers,
            payload_hash: &payload_hash,
        };
        sigv4::verify(&self.config.secret_key, &auth, &input, now_unix())
            .map_err(|e| deny(403, "SignatureDoesNotMatch", &e))?;

        // Decode + verify aws-chunked framing after the seed signature checks
        // out, replacing the body with the actual payload.
        if !presigned && payload_hash == STREAMING_PAYLOAD {
            let decoded = sigv4::decode_aws_chunked(&req.body, &self.config.secret_key, &auth)
                .map_err(|e| deny(403, "SignatureDoesNotMatch", &e))?;
            if let Some(expected) = req.header("x-amz-decoded-content-length")
                && expected.parse::<usize>().ok() != Some(decoded.len())
            {
                return Err(deny(400, "IncompleteBody", "decoded length mismatch"));
            }
            req.body = decoded;
        }
        Ok(())
    }

    // ── Bucket operations ────────────────────────────────────────────────────

    fn list_buckets(&self) -> Response {
        let store = self.executor.blob_store().read();
        let mut names: Vec<String> = store
            .list_prefix("s3meta/bucket/")
            .into_iter()
            .map(|k| k["s3meta/bucket/".len()..].to_string())
            .collect();
        names.sort();
        let mut buckets_xml = String::new();
        for name in &names {
            let created = store
                .metadata(&bucket_marker(name))
                .map(|m| (m.created_at / 1000) as i64)
                .unwrap_or(0);
            buckets_xml.push_str(&format!(
                "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
                xml_escape(name),
                iso8601(created)
            ));
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListAllMyBucketsResult>\
             <Owner><ID>nucleus</ID><DisplayName>nucleus</DisplayName></Owner>\
             <Buckets>{buckets_xml}</Buckets></ListAllMyBucketsResult>"
        );
        Response::with_body(200, XML_CT, body.into_bytes())
    }

    fn create_bucket(&self, bucket: &str) -> Response {
        let mut store = self.executor.blob_store().write();
        let marker = bucket_marker(bucket);
        if store.metadata(&marker).is_some() {
            return error_xml(
                409,
                "BucketAlreadyOwnedByYou",
                "bucket already exists",
                bucket,
            );
        }
        store.put(&marker, b"", None);
        Response::new(200).header("Location", format!("/{bucket}"))
    }

    fn delete_bucket(&self, bucket: &str) -> Response {
        let mut store = self.executor.blob_store().write();
        let marker = bucket_marker(bucket);
        if store.metadata(&marker).is_none() {
            return error_xml(404, "NoSuchBucket", "bucket does not exist", bucket);
        }
        if !store.list_prefix(&obj_key(bucket, "")).is_empty() {
            return error_xml(409, "BucketNotEmpty", "bucket is not empty", bucket);
        }
        store.delete(&marker);
        Response::new(204)
    }

    fn head_bucket(&self, bucket: &str) -> Response {
        if self.bucket_exists(bucket) {
            Response::new(200)
        } else {
            Response::new(404)
        }
    }

    fn bucket_exists(&self, bucket: &str) -> bool {
        self.executor
            .blob_store()
            .read()
            .metadata(&bucket_marker(bucket))
            .is_some()
    }

    fn get_bucket(&self, bucket: &str, req: &Request) -> Response {
        if !self.bucket_exists(bucket) {
            return error_xml(404, "NoSuchBucket", "bucket does not exist", bucket);
        }
        if req.has_query("location") {
            return Response::with_body(
                200,
                XML_CT,
                b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                  <LocationConstraint>nucleus</LocationConstraint>"
                    .to_vec(),
            );
        }
        if req.has_query("uploads") {
            return self.list_multipart_uploads(bucket);
        }
        self.list_objects(bucket, req)
    }

    // ── Listing ──────────────────────────────────────────────────────────────

    fn list_objects(&self, bucket: &str, req: &Request) -> Response {
        let v2 = req.query_param("list-type") == Some("2");
        let prefix = req.query_param("prefix").unwrap_or("").to_string();
        let delimiter = req.query_param("delimiter").unwrap_or("").to_string();
        let max_keys: usize = req
            .query_param("max-keys")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000)
            .min(1000);
        // Resume point: V2 continuation-token/start-after, V1 marker. Our
        // continuation token is simply the last key of the previous page.
        let after = if v2 {
            req.query_param("continuation-token")
                .or(req.query_param("start-after"))
                .unwrap_or("")
        } else {
            req.query_param("marker").unwrap_or("")
        }
        .to_string();

        let store = self.executor.blob_store().read();
        let ns = obj_key(bucket, "");
        let mut keys: Vec<String> = store
            .list_prefix(&format!("{ns}{prefix}"))
            .into_iter()
            .map(|k| k[ns.len()..].to_string())
            .filter(|k| *k > after)
            .collect();
        keys.sort();

        let mut contents = String::new();
        let mut common: Vec<String> = Vec::new();
        let mut count = 0usize;
        let mut truncated = false;
        let mut last_key = String::new();

        for key in &keys {
            if count >= max_keys {
                truncated = true;
                break;
            }
            // Delimiter grouping: everything up to and including the first
            // delimiter after the prefix collapses into a CommonPrefix.
            if !delimiter.is_empty()
                && let Some(idx) = key[prefix.len()..].find(&delimiter)
            {
                let cp = format!("{}{}", &key[..prefix.len() + idx], delimiter);
                if common.last() != Some(&cp) {
                    common.push(cp.clone());
                    count += 1;
                    last_key = cp;
                }
                continue;
            }
            let blob_key = obj_key(bucket, key);
            let (size, modified) = store
                .metadata(&blob_key)
                .map(|m| (m.size, (m.updated_at / 1000) as i64))
                .unwrap_or((0, 0));
            let etag = blob_etag(&store, &blob_key).unwrap_or_default();
            contents.push_str(&format!(
                "<Contents><Key>{}</Key><LastModified>{}</LastModified>\
                 <ETag>&quot;{}&quot;</ETag><Size>{}</Size>\
                 <StorageClass>STANDARD</StorageClass></Contents>",
                xml_escape(key),
                iso8601(modified),
                etag,
                size
            ));
            count += 1;
            last_key = key.clone();
        }

        let mut common_xml = String::new();
        for cp in &common {
            common_xml.push_str(&format!(
                "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                xml_escape(cp)
            ));
        }

        let mut body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult>\
             <Name>{}</Name><Prefix>{}</Prefix><MaxKeys>{}</MaxKeys>\
             <IsTruncated>{}</IsTruncated>",
            xml_escape(bucket),
            xml_escape(&prefix),
            max_keys,
            truncated
        );
        if !delimiter.is_empty() {
            body.push_str(&format!(
                "<Delimiter>{}</Delimiter>",
                xml_escape(&delimiter)
            ));
        }
        if v2 {
            body.push_str(&format!("<KeyCount>{count}</KeyCount>"));
            if truncated {
                body.push_str(&format!(
                    "<NextContinuationToken>{}</NextContinuationToken>",
                    xml_escape(&last_key)
                ));
            }
        } else if truncated {
            body.push_str(&format!(
                "<NextMarker>{}</NextMarker>",
                xml_escape(&last_key)
            ));
        }
        body.push_str(&contents);
        body.push_str(&common_xml);
        body.push_str("</ListBucketResult>");
        Response::with_body(200, XML_CT, body.into_bytes())
    }

    fn list_multipart_uploads(&self, bucket: &str) -> Response {
        let store = self.executor.blob_store().read();
        let ns = format!("s3mpu/{bucket}/");
        let mut uploads = String::new();
        for meta_key in store.list_prefix(&ns) {
            let Some(rest) = meta_key
                .strip_prefix(&ns)
                .and_then(|r| r.strip_suffix("/meta"))
            else {
                continue;
            };
            let object_key = store
                .get(meta_key)
                .map(|d| String::from_utf8_lossy(&d).into_owned())
                .unwrap_or_default();
            uploads.push_str(&format!(
                "<Upload><Key>{}</Key><UploadId>{}</UploadId></Upload>",
                xml_escape(&object_key),
                xml_escape(rest)
            ));
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListMultipartUploadsResult>\
             <Bucket>{}</Bucket><IsTruncated>false</IsTruncated>{uploads}\
             </ListMultipartUploadsResult>",
            xml_escape(bucket)
        );
        Response::with_body(200, XML_CT, body.into_bytes())
    }

    // ── Object operations ────────────────────────────────────────────────────

    fn put_object(&self, bucket: &str, key: &str, req: &Request) -> Response {
        if !self.bucket_exists(bucket) {
            return error_xml(404, "NoSuchBucket", "bucket does not exist", bucket);
        }
        if req.body.len() > self.config.max_object_bytes {
            return error_xml(413, "EntityTooLarge", "object exceeds size limit", key);
        }
        let etag = blake3::hash(&req.body).to_hex().to_string();
        let blob_key = obj_key(bucket, key);
        let content_type = req.header("content-type");
        let mut store = self.executor.blob_store().write();
        store.put(&blob_key, &req.body, content_type);
        store.set_tag(&blob_key, "etag", &etag);
        Response::new(200).header("ETag", format!("\"{etag}\""))
    }

    fn copy_object(&self, bucket: &str, key: &str, req: &Request) -> Response {
        if !self.bucket_exists(bucket) {
            return error_xml(404, "NoSuchBucket", "bucket does not exist", bucket);
        }
        let source = percent_decode(req.header("x-amz-copy-source").unwrap_or(""));
        let source = source.trim_start_matches('/');
        let Some((src_bucket, src_key)) = source.split_once('/') else {
            return error_xml(
                400,
                "InvalidArgument",
                "malformed x-amz-copy-source",
                source,
            );
        };
        let src_blob = obj_key(src_bucket, src_key);
        let dst_blob = obj_key(bucket, key);
        let mut store = self.executor.blob_store().write();
        let (src_ct, src_etag) = match store.metadata(&src_blob) {
            Some(m) => (m.content_type.clone(), blob_etag(&store, &src_blob)),
            None => return error_xml(404, "NoSuchKey", "copy source does not exist", source),
        };
        // Zero-copy: the destination manifest references the source's chunks.
        if !store.compose(&dst_blob, &[&src_blob], src_ct.as_deref()) {
            return error_xml(404, "NoSuchKey", "copy source does not exist", source);
        }
        let etag = src_etag.unwrap_or_default();
        store.set_tag(&dst_blob, "etag", &etag);
        let modified = iso8601(now_unix());
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult>\
             <LastModified>{modified}</LastModified><ETag>&quot;{etag}&quot;</ETag>\
             </CopyObjectResult>"
        );
        Response::with_body(200, XML_CT, body.into_bytes())
    }

    /// Shared GET/HEAD logic. For HEAD the body is never materialized.
    fn object_response(&self, bucket: &str, key: &str, req: &Request, head: bool) -> Response {
        let blob_key = obj_key(bucket, key);
        let store = self.executor.blob_store().read();
        let Some(meta) = store.metadata(&blob_key) else {
            return if self.bucket_exists_locked(&store, bucket) {
                error_xml(404, "NoSuchKey", "object does not exist", key)
            } else {
                error_xml(404, "NoSuchBucket", "bucket does not exist", bucket)
            };
        };
        let size = meta.size;
        let modified = (meta.updated_at / 1000) as i64;
        let content_type = meta
            .content_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string());
        let etag = blob_etag(&store, &blob_key).unwrap_or_default();

        // Range handling.
        let range = req.header("range").and_then(|r| parse_range(r, size));
        let (status, start, len) = match range {
            Some((start, end)) => (206, start, end - start + 1),
            None => (200, 0, size),
        };
        if req.header("range").is_some() && range.is_none() && size > 0 {
            return error_xml(416, "InvalidRange", "range not satisfiable", key)
                .header("Content-Range", format!("bytes */{size}"));
        }

        let mut resp = Response::new(status)
            .header("Content-Type", content_type)
            .header("ETag", format!("\"{etag}\""))
            .header("Last-Modified", http_date(modified))
            .header("Accept-Ranges", "bytes");
        if status == 206 {
            resp = resp.header(
                "Content-Range",
                format!("bytes {}-{}/{}", start, start + len - 1, size),
            );
        }
        if head {
            resp.content_length_override = Some(len);
            return resp;
        }
        let data = if status == 206 {
            store.get_range(&blob_key, start, len)
        } else {
            store.get(&blob_key)
        };
        match data {
            Some(data) => {
                resp.body = data;
                resp
            }
            None => error_xml(500, "InternalError", "object data unreadable", key),
        }
    }

    fn bucket_exists_locked(&self, store: &BlobStore, bucket: &str) -> bool {
        store.metadata(&bucket_marker(bucket)).is_some()
    }

    fn get_object(&self, bucket: &str, key: &str, req: &Request) -> Response {
        self.object_response(bucket, key, req, false)
    }

    fn head_object(&self, bucket: &str, key: &str, req: &Request) -> Response {
        self.object_response(bucket, key, req, true)
    }

    fn delete_object(&self, bucket: &str, key: &str) -> Response {
        let mut store = self.executor.blob_store().write();
        store.delete(&obj_key(bucket, key));
        // S3 returns 204 whether or not the key existed.
        Response::new(204)
    }

    fn delete_objects(&self, bucket: &str, req: &Request) -> Response {
        let body = String::from_utf8_lossy(&req.body);
        let keys = xml_extract_all(&body, "Key");
        let mut store = self.executor.blob_store().write();
        let mut deleted_xml = String::new();
        for key in &keys {
            store.delete(&obj_key(bucket, key));
            deleted_xml.push_str(&format!(
                "<Deleted><Key>{}</Key></Deleted>",
                xml_escape(key)
            ));
        }
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult>{deleted_xml}</DeleteResult>"
        );
        Response::with_body(200, XML_CT, body.into_bytes())
    }

    // ── Multipart upload ─────────────────────────────────────────────────────

    fn create_multipart(&self, bucket: &str, key: &str, req: &Request) -> Response {
        if !self.bucket_exists(bucket) {
            return error_xml(404, "NoSuchBucket", "bucket does not exist", bucket);
        }
        let upload_id: String = {
            use rand::Rng;
            let mut rng = rand::thread_rng();
            (0..32)
                .map(|_| format!("{:x}", rng.gen_range(0..16)))
                .collect()
        };
        let mut store = self.executor.blob_store().write();
        store.put(
            &mpu_meta_key(bucket, &upload_id),
            key.as_bytes(),
            req.header("content-type"),
        );
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult>\
             <Bucket>{}</Bucket><Key>{}</Key><UploadId>{upload_id}</UploadId>\
             </InitiateMultipartUploadResult>",
            xml_escape(bucket),
            xml_escape(key)
        );
        Response::with_body(200, XML_CT, body.into_bytes())
    }

    fn upload_part(&self, bucket: &str, key: &str, req: &Request) -> Response {
        let upload_id = req.query_param("uploadId").unwrap_or("");
        let part: u32 = match req.query_param("partNumber").and_then(|v| v.parse().ok()) {
            Some(p) if (1..=10_000).contains(&p) => p,
            _ => return error_xml(400, "InvalidArgument", "bad partNumber", key),
        };
        if req.body.len() > self.config.max_object_bytes {
            return error_xml(413, "EntityTooLarge", "part exceeds size limit", key);
        }
        let mut store = self.executor.blob_store().write();
        if store.metadata(&mpu_meta_key(bucket, upload_id)).is_none() {
            return error_xml(404, "NoSuchUpload", "upload does not exist", upload_id);
        }
        let etag = blake3::hash(&req.body).to_hex().to_string();
        let part_key = mpu_part_key(bucket, upload_id, part);
        store.put(&part_key, &req.body, None);
        store.set_tag(&part_key, "etag", &etag);
        Response::new(200).header("ETag", format!("\"{etag}\""))
    }

    fn complete_multipart(&self, bucket: &str, key: &str, req: &Request) -> Response {
        let upload_id = req.query_param("uploadId").unwrap_or("").to_string();
        let body = String::from_utf8_lossy(&req.body);
        let part_numbers: Vec<u32> = xml_extract_all(&body, "PartNumber")
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        if part_numbers.is_empty() {
            return error_xml(400, "MalformedXML", "no parts listed", key);
        }
        let mut sorted = part_numbers.clone();
        sorted.sort_unstable();
        sorted.dedup();
        if sorted != part_numbers {
            return error_xml(
                400,
                "InvalidPartOrder",
                "parts must be ascending and unique",
                key,
            );
        }

        let mut store = self.executor.blob_store().write();
        let meta_key = mpu_meta_key(bucket, &upload_id);
        let Some(mpu_meta) = store.metadata(&meta_key) else {
            return error_xml(404, "NoSuchUpload", "upload does not exist", &upload_id);
        };
        let content_type = mpu_meta.content_type.clone();

        let part_keys: Vec<String> = part_numbers
            .iter()
            .map(|n| mpu_part_key(bucket, &upload_id, *n))
            .collect();
        let mut etag_concat = Vec::new();
        for (n, pk) in part_numbers.iter().zip(&part_keys) {
            match blob_etag(&store, pk) {
                Some(etag) => etag_concat.extend_from_slice(etag.as_bytes()),
                None => {
                    return error_xml(400, "InvalidPart", &format!("part {n} not uploaded"), key);
                }
            }
        }

        // Zero-copy assembly: the object manifest references the parts'
        // chunks; no data moves.
        let blob_key = obj_key(bucket, key);
        let sources: Vec<&str> = part_keys.iter().map(|s| s.as_str()).collect();
        if !store.compose(&blob_key, &sources, content_type.as_deref()) {
            return error_xml(400, "InvalidPart", "a part disappeared", key);
        }
        let etag = format!(
            "{}-{}",
            blake3::hash(&etag_concat).to_hex(),
            part_numbers.len()
        );
        store.set_tag(&blob_key, "etag", &etag);

        // Chunks stay alive through the object's references.
        for pk in &part_keys {
            store.delete(pk);
        }
        store.delete(&meta_key);

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult>\
             <Location>/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key>\
             <ETag>&quot;{etag}&quot;</ETag></CompleteMultipartUploadResult>",
            xml_escape(bucket),
            xml_escape(key),
            xml_escape(bucket),
            xml_escape(key)
        );
        Response::with_body(200, XML_CT, body.into_bytes())
    }

    fn abort_multipart(&self, bucket: &str, key: &str, req: &Request) -> Response {
        let _ = key;
        let upload_id = req.query_param("uploadId").unwrap_or("");
        let mut store = self.executor.blob_store().write();
        let prefix = format!("s3mpu/{bucket}/{upload_id}/");
        let keys: Vec<String> = store
            .list_prefix(&prefix)
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        if keys.is_empty() {
            return error_xml(404, "NoSuchUpload", "upload does not exist", upload_id);
        }
        for k in keys {
            store.delete(&k);
        }
        Response::new(204)
    }
}

/// Parse `Range: bytes=a-b` (inclusive), `bytes=a-`, or `bytes=-suffix`.
/// Returns byte positions `(start, end)` inclusive, or None if unsatisfiable.
fn parse_range(header: &str, size: u64) -> Option<(u64, u64)> {
    let spec = header.trim().strip_prefix("bytes=")?;
    if size == 0 {
        return None;
    }
    let (start_s, end_s) = spec.split_once('-')?;
    if start_s.is_empty() {
        // suffix form: last N bytes
        let n: u64 = end_s.parse().ok()?;
        if n == 0 {
            return None;
        }
        let start = size.saturating_sub(n);
        return Some((start, size - 1));
    }
    let start: u64 = start_s.parse().ok()?;
    if start >= size {
        return None;
    }
    let end = if end_s.is_empty() {
        size - 1
    } else {
        end_s.parse::<u64>().ok()?.min(size - 1)
    };
    if end < start {
        return None;
    }
    Some((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_parsing() {
        assert_eq!(parse_range("bytes=0-9", 100), Some((0, 9)));
        assert_eq!(parse_range("bytes=10-", 100), Some((10, 99)));
        assert_eq!(parse_range("bytes=-10", 100), Some((90, 99)));
        assert_eq!(parse_range("bytes=0-200", 100), Some((0, 99)));
        assert_eq!(parse_range("bytes=100-", 100), None);
        assert_eq!(parse_range("bytes=5-3", 100), None);
        assert_eq!(parse_range("bytes=0-0", 100), Some((0, 0)));
        assert_eq!(parse_range("bogus", 100), None);
    }

    #[test]
    fn bucket_name_validation() {
        assert!(valid_bucket_name("my-bucket.01"));
        assert!(!valid_bucket_name("ab"));
        assert!(!valid_bucket_name("UPPER"));
        assert!(!valid_bucket_name("-lead"));
        assert!(!valid_bucket_name("trail-"));
        assert!(!valid_bucket_name("has_underscore"));
        assert!(!valid_bucket_name(&"x".repeat(64)));
    }
}
