//! Opt-in debug HTTP bridge, disabled in release builds.
use crate::bridge::{Request, Response, Router};
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
};
const MAX_HEADERS: usize = 16 * 1024;
const MAX_BODY: usize = 1024 * 1024;

pub fn is_dev_mode() -> bool {
    cfg!(debug_assertions) && std::env::var("NEUTRON_DESKTOP_DEV").unwrap_or_default() == "true"
}
pub fn dev_port() -> u16 {
    std::env::var("NEUTRON_DESKTOP_DEV_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|p| *p != 0)
        .unwrap_or(3001)
}

#[derive(Clone)]
pub struct DevAccess {
    pub token: String,
    pub origin: String,
    host: String,
}
impl DevAccess {
    pub fn new(port: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let mut random = [0u8; 32];
        getrandom::fill(&mut random).map_err(|e| std::io::Error::other(e.to_string()))?;
        let origin = std::env::var("NEUTRON_DESKTOP_DEV_ORIGIN").unwrap_or_else(|_| {
            if cfg!(target_os = "windows") {
                "http://tauri.localhost".into()
            } else {
                "tauri://localhost".into()
            }
        });
        let parsed = url::Url::parse(&origin)?;
        if !(matches!(parsed.scheme(), "http" | "https")
            && parsed.origin().ascii_serialization() == origin
            || origin == "tauri://localhost")
        {
            return Err("Dev origin must be an exact HTTP(S) origin".into());
        }
        Ok(Self {
            token: random.iter().map(|b| format!("{b:02x}")).collect(),
            origin,
            host: format!("127.0.0.1:{port}"),
        })
    }
    fn authorize(&self, headers: &HashMap<String, String>, preflight: bool) -> bool {
        headers.get("host") == Some(&self.host)
            && headers.get("origin") == Some(&self.origin)
            && (preflight || headers.get("x-neutron-dev-token") == Some(&self.token))
    }
}

struct Dispatch {
    sender: std::sync::mpsc::SyncSender<(Request, std::sync::mpsc::SyncSender<Response>)>,
    busy: Arc<std::sync::atomic::AtomicBool>,
}
impl Dispatch {
    fn new(router: Arc<Router>) -> Self {
        let (sender, receiver) =
            std::sync::mpsc::sync_channel::<(Request, std::sync::mpsc::SyncSender<Response>)>(1);
        let busy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_busy = Arc::clone(&busy);
        // Exactly one worker; a timed-out synchronous handler is never replaced
        // with another worker. Its slot remains occupied until it actually exits.
        std::thread::spawn(move || {
            for (request, reply) in receiver {
                let response = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    router.handle_direct(request)
                }))
                .unwrap_or_else(|_| Response::error(500, "Handler failed", "Handler panicked"));
                let _ = reply.try_send(response);
                worker_busy.store(false, std::sync::atomic::Ordering::Release);
            }
        });
        Self { sender, busy }
    }
    fn handle(&self, request: Request, deadline: std::time::Instant) -> Response {
        use std::sync::atomic::Ordering;
        if self
            .busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Response::error(503, "Busy", "Previous handler still owns dispatch");
        }
        let (reply, result) = std::sync::mpsc::sync_channel(1);
        if self.sender.try_send((request, reply)).is_err() {
            self.busy.store(false, Ordering::Release);
            return Response::error(503, "Unavailable", "Dispatch worker unavailable");
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        result.recv_timeout(remaining).unwrap_or_else(|_| {
            Response::error(
                504,
                "Deadline exceeded",
                "Handler may still be running; do not retry mutations blindly",
            )
        })
    }
}

pub fn spawn_dev_server(router: Arc<Router>, port: u16, access: DevAccess) -> std::io::Result<()> {
    if !is_dev_mode() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Debug bridge disabled",
        ));
    }
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    let dispatch = Dispatch::new(router);
    std::thread::spawn(move || {
        // Serial bounded connections avoid an unbounded thread allocation attack.
        for stream in listener.incoming().flatten() {
            if let Err(e) = handle_connection(stream, &dispatch, &access) {
                tracing::debug!("Dev connection: {e}");
            }
        }
    });
    Ok(())
}

fn handle_connection(
    mut stream: TcpStream,
    dispatch: &Dispatch,
    access: &DevAccess,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);

    // Read headers one byte at a time into a hard bounded buffer. No overread of body.
    let mut raw = Vec::with_capacity(MAX_HEADERS);
    while !raw.ends_with(b"\r\n\r\n") {
        if raw.len() == MAX_HEADERS {
            return write_response(
                &mut stream,
                Response::error(431, "Headers too large", "Header limit exceeded"),
                None,
                deadline,
            );
        }
        let mut byte = [0];
        read_before_deadline(&mut stream, &mut byte, deadline)?;
        raw.push(byte[0]);
    }
    let mut slots = [httparse::EMPTY_HEADER; 64];
    let mut parsed = httparse::Request::new(&mut slots);
    parsed.parse(&raw)?;
    let method = parsed
        .method
        .ok_or("Missing method")?
        .parse::<http::Method>()?;
    let target = parsed.path.ok_or("Missing path")?;
    if !target.starts_with('/') || target.starts_with("//") {
        return Err("Invalid request target".into());
    }
    let mut headers = HashMap::new();
    for h in parsed.headers.iter() {
        let name = h.name.to_ascii_lowercase();
        if headers
            .insert(name, std::str::from_utf8(h.value)?.to_string())
            .is_some()
        {
            return Err("Duplicate request header".into());
        }
    }
    let preflight = method == http::Method::OPTIONS;
    if !access.authorize(&headers, preflight) {
        return write_response(
            &mut stream,
            Response::error(403, "Forbidden", "Invalid debug capability, host or origin"),
            None,
            deadline,
        );
    }
    if headers.contains_key("transfer-encoding") {
        return Err("Transfer encoding is unsupported".into());
    }
    let length: usize = headers
        .get("content-length")
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(0);
    if length > MAX_BODY {
        return write_response(
            &mut stream,
            Response::error(413, "Body too large", "Body limit exceeded"),
            Some(&access.origin),
            deadline,
        );
    }
    if preflight {
        if headers
            .get("access-control-request-method")
            .is_none_or(|m| !matches!(m.as_str(), "GET" | "POST" | "PUT" | "DELETE" | "PATCH"))
        {
            return Err("Invalid preflight method".into());
        }
        return write_response(
            &mut stream,
            Response {
                status: 204,
                headers: HashMap::from([
                    (
                        "access-control-allow-methods".into(),
                        "GET, POST, PUT, DELETE, PATCH".into(),
                    ),
                    (
                        "access-control-allow-headers".into(),
                        "Content-Type, X-Neutron-Dev-Token".into(),
                    ),
                ]),
                body: vec![],
            },
            Some(&access.origin),
            deadline,
        );
    }
    let mut body = vec![0; length];
    read_before_deadline(&mut stream, &mut body, deadline)?;
    let (path, query) = parse_path_and_query(target);
    write_response(
        &mut stream,
        dispatch.handle(
            Request {
                method,
                path,
                headers,
                body,
                query,
            },
            deadline,
        ),
        Some(&access.origin),
        deadline,
    )
}
fn read_before_deadline(
    stream: &mut TcpStream,
    mut bytes: &mut [u8],
    deadline: std::time::Instant,
) -> std::io::Result<()> {
    while !bytes.is_empty() {
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "Request deadline exceeded")
            })?;
        stream.set_read_timeout(Some(remaining))?;
        let read = stream.read(bytes)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Truncated request",
            ));
        }
        bytes = &mut bytes[read..];
    }
    Ok(())
}
fn parse_path_and_query(target: &str) -> (String, HashMap<String, String>) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    (
        path.into(),
        url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect(),
    )
}
fn write_response(
    stream: &mut TcpStream,
    response: Response,
    origin: Option<&str>,
    deadline: std::time::Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    if response.body.len() > 8 * 1024 * 1024
        || response
            .headers
            .iter()
            .map(|(name, value)| name.len().saturating_add(value.len()))
            .sum::<usize>()
            > MAX_HEADERS
    {
        return Err("Handler response exceeds bridge transport budget".into());
    }
    let mut headers = response.headers;
    headers.retain(|name, _| !name.eq_ignore_ascii_case("access-control-allow-origin"));
    if let Some(origin) = origin {
        headers.insert("access-control-allow-origin".into(), origin.into());
        headers.insert("vary".into(), "Origin".into());
    }
    let mut encoded = Vec::new();
    write!(
        encoded,
        "HTTP/1.1 {} {}\r\n",
        response.status,
        http::StatusCode::from_u16(response.status)?
            .canonical_reason()
            .unwrap_or("Response")
    )?;
    for (k, v) in headers {
        let name = http::header::HeaderName::from_bytes(k.as_bytes())?;
        let value = http::header::HeaderValue::from_str(&v)?;
        if name == http::header::CONTENT_LENGTH || name == http::header::CONNECTION {
            continue;
        }
        write!(encoded, "{}: {}\r\n", name, value.to_str()?)?;
    }
    write!(
        encoded,
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        response.body.len()
    )?;
    encoded.extend_from_slice(&response.body);
    let mut remaining_bytes = encoded.as_slice();
    while !remaining_bytes.is_empty() {
        let remaining = deadline
            .checked_duration_since(std::time::Instant::now())
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "Response deadline exceeded")
            })?;
        stream.set_write_timeout(Some(remaining))?;
        let written = stream.write(remaining_bytes)?;
        if written == 0 {
            return Err("Response write stopped".into());
        }
        remaining_bytes = &remaining_bytes[written..];
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capability_and_exact_origin() {
        let a = DevAccess {
            token: "secret".into(),
            origin: if cfg!(target_os = "windows") {
                "http://tauri.localhost".into()
            } else {
                "tauri://localhost".into()
            },
            host: "127.0.0.1:3001".into(),
        };
        let mut h = HashMap::from([
            ("host".into(), a.host.clone()),
            ("origin".into(), a.origin.clone()),
            ("x-neutron-dev-token".into(), a.token.clone()),
        ]);
        assert!(a.authorize(&h, false));
        h.remove("x-neutron-dev-token");
        assert!(!a.authorize(&h, false));
        assert!(a.authorize(&h, true));
        h.insert("origin".into(), "https://evil.example".into());
        assert!(!a.authorize(&h, true));
        h.insert("origin".into(), a.origin.clone());
        h.insert("host".into(), "evil.example".into());
        assert!(!a.authorize(&h, true));
    }
}

#[cfg(test)]
mod handler_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn access(port: u16) -> DevAccess {
        DevAccess {
            token: "test-token".into(),
            origin: "https://dev.example".into(),
            host: format!("127.0.0.1:{port}"),
        }
    }
    fn roundtrip(raw: Vec<u8>, dispatch: Dispatch) -> Vec<u8> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let a = access(port);
        let worker = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let _ = handle_connection(stream, &dispatch, &a);
        });
        let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        let raw = String::from_utf8(raw)
            .unwrap()
            .replace("HOST", &format!("127.0.0.1:{port}"));
        let _ = client.write_all(raw.as_bytes());
        client.shutdown(std::net::Shutdown::Write).unwrap();
        let mut bytes = Vec::new();
        let _ = client.read_to_end(&mut bytes);
        worker.join().unwrap();
        bytes
    }
    fn router(count: Arc<AtomicUsize>) -> Arc<Router> {
        Arc::new(Router::new(vec![crate::Route {
            method: http::Method::POST,
            path: "/write".into(),
            handler: Box::new(move |_| {
                count.fetch_add(1, Ordering::SeqCst);
                Response::text("handled")
            }),
        }]))
    }
    #[test]
    fn real_parser_refuses_unauthorized_oversized_duplicate_and_truncated_requests() {
        let count = Arc::new(AtomicUsize::new(0));
        let prefix = "POST /write HTTP/1.1\r\nHost: HOST\r\nOrigin: https://dev.example\r\nX-Neutron-Dev-Token: test-token\r\n";
        for raw in [
            prefix.replace("test-token", "wrong") + "\r\n",
            prefix.to_string() + "Host: duplicate\r\n\r\n",
            prefix.to_string() + "Content-Length: 1048577\r\n\r\n",
            prefix.to_string() + "Transfer-Encoding: chunked\r\n\r\n",
            prefix.to_string() + "Content-Length: 2\r\n\r\nx",
            prefix.to_string() + &"X".repeat(MAX_HEADERS) + "\r\n\r\n",
        ] {
            roundtrip(raw.into_bytes(), Dispatch::new(router(Arc::clone(&count))));
            assert_eq!(count.load(Ordering::SeqCst), 0);
        }
        let bytes = roundtrip(
            (prefix.to_string() + "Content-Length: 1\r\n\r\nx").into_bytes(),
            Dispatch::new(router(Arc::clone(&count))),
        );
        assert!(String::from_utf8_lossy(&bytes).contains("handled"));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn timed_out_handler_keeps_its_single_worker_slot_until_actual_exit() {
        let (release, held) = std::sync::mpsc::sync_channel(1);
        let held = Arc::new(std::sync::Mutex::new(held));
        let router = Arc::new(Router::new(vec![crate::Route {
            method: http::Method::GET,
            path: "/held".into(),
            handler: Box::new(move |_| {
                held.lock().unwrap().recv().unwrap();
                Response::text("released")
            }),
        }]));
        let dispatch = Dispatch::new(router);
        let request = || Request {
            method: http::Method::GET,
            path: "/held".into(),
            headers: HashMap::new(),
            body: vec![],
            query: HashMap::new(),
        };
        assert_eq!(
            dispatch
                .handle(
                    request(),
                    std::time::Instant::now() + std::time::Duration::from_millis(20)
                )
                .status,
            504
        );
        assert_eq!(
            dispatch
                .handle(
                    request(),
                    std::time::Instant::now() + std::time::Duration::from_millis(20)
                )
                .status,
            503
        );
        release.send(()).unwrap();
        for _ in 0..100 {
            if !dispatch.busy.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        panic!("Completed handler did not release dispatch ownership");
    }
}
