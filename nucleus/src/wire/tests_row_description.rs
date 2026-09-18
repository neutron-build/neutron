//! The RowDescription a client receives must describe the DataRows that follow.
//!
//! In the extended query protocol those two come from different places:
//! Describe derives a schema, Execute produces rows. Nucleus derives the schema
//! by executing the statement capped at zero rows, and that cap used to be
//! applied by appending `" LIMIT 0"` to the SQL TEXT. Text-append is not a
//! row cap; it is a second statement that usually resembles the first.
//!
//! Measured against a running server before the fix, over 24 tables read
//! concurrently for 10 seconds: 20,800 of 125,419 statements were answered with
//! a description that did not describe their rows. Every one of the shapes
//! below is a real family from that run.
//!
//! The assertions are on the field NAMES, not just the count. A description
//! that is too NARROW makes a positional client throw, which is loud; one that
//! is too wide, or merely misnamed, decodes a valid row into a row with fields
//! missing and reports nothing at all. The silent half is the one that has to
//! be caught here.

use std::sync::Arc;

use super::{NucleusHandler, NucleusServer, process_socket_closing_on_terminate, zero_row_probe};

// ── raw pgwire client ────────────────────────────────────────────────────────

struct Msg {
    tag: u8,
    payload: Vec<u8>,
}

async fn read_message(stream: &mut tokio::net::TcpStream) -> Option<Msg> {
    use tokio::io::AsyncReadExt;
    let mut header = [0u8; 5];
    stream.read_exact(&mut header).await.ok()?;
    let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut payload = vec![0u8; len.saturating_sub(4)];
    if !payload.is_empty() {
        stream.read_exact(&mut payload).await.ok()?;
    }
    Some(Msg {
        tag: header[0],
        payload,
    })
}

async fn send_message(stream: &mut tokio::net::TcpStream, tag: u8, body: &[u8]) {
    use tokio::io::AsyncWriteExt;
    let mut buf = Vec::with_capacity(body.len() + 5);
    buf.push(tag);
    buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    buf.extend_from_slice(body);
    stream.write_all(&buf).await.unwrap();
}

async fn startup(stream: &mut tokio::net::TcpStream) {
    use tokio::io::AsyncWriteExt;
    let mut body = Vec::new();
    body.extend_from_slice(&196_608i32.to_be_bytes());
    body.extend_from_slice(b"user\0app\0\0");
    stream
        .write_all(&((body.len() + 4) as i32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&body).await.unwrap();
    while let Some(m) = read_message(stream).await {
        if m.tag == b'Z' {
            return;
        }
    }
    panic!("startup never reached ReadyForQuery");
}

/// Column names out of a RowDescription payload.
fn field_names(payload: &[u8]) -> Vec<String> {
    let n = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut out = Vec::with_capacity(n);
    let mut i = 2;
    for _ in 0..n {
        let end = payload[i..].iter().position(|&b| b == 0).unwrap() + i;
        out.push(String::from_utf8_lossy(&payload[i..end]).into_owned());
        i = end + 1 + 18; // NUL + tableOid/attnum/typeOid/typlen/typmod/format
    }
    out
}

/// Number of columns in a DataRow payload.
fn data_row_width(payload: &[u8]) -> usize {
    i16::from_be_bytes([payload[0], payload[1]]) as usize
}

/// The raw value bytes of a DataRow payload (None per NULL column).
fn data_row_values(payload: &[u8]) -> Vec<Option<&[u8]>> {
    let n = data_row_width(payload);
    let mut out = Vec::with_capacity(n);
    let mut i = 2;
    for _ in 0..n {
        let len = i32::from_be_bytes([
            payload[i],
            payload[i + 1],
            payload[i + 2],
            payload[i + 3],
        ]);
        if len < 0 {
            out.push(None);
        } else {
            let len = len as usize;
            out.push(Some(&payload[i + 4..i + 4 + len]));
            i += len;
        }
        i += 4;
    }
    out
}

/// The declared format code of each field in a RowDescription payload
/// (0 = text, 1 = binary).
fn field_formats(payload: &[u8]) -> Vec<i16> {
    let n = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut out = Vec::with_capacity(n);
    let mut i = 2;
    for _ in 0..n {
        let end = payload[i..].iter().position(|&b| b == 0).unwrap() + i;
        // name NUL, then tableOid(4) attnum(2) typeOid(4) typlen(2) typmod(4),
        // then the 2-byte format code.
        let at = end + 1 + 16;
        out.push(i16::from_be_bytes([payload[at], payload[at + 1]]));
        i = end + 1 + 18;
    }
    out
}

struct Answer {
    fields: Option<Vec<String>>,
    row_widths: Vec<usize>,
    errors: Vec<String>,
    /// Declared format codes from the RowDescription, when one was sent.
    formats: Option<Vec<i16>>,
    /// Raw value bytes of every DataRow, in arrival order.
    values: Vec<Vec<Option<Vec<u8>>>>,
}

fn error_text(payload: &[u8]) -> String {
    let mut i = 0;
    while i < payload.len() && payload[i] != 0 {
        let code = payload[i];
        i += 1;
        let end = payload[i..].iter().position(|&b| b == 0).unwrap() + i;
        if code == b'M' {
            return String::from_utf8_lossy(&payload[i..end]).into_owned();
        }
        i = end + 1;
    }
    String::new()
}

async fn drain(stream: &mut tokio::net::TcpStream) -> Answer {
    let mut answer = Answer {
        fields: None,
        row_widths: Vec::new(),
        errors: Vec::new(),
        formats: None,
        values: Vec::new(),
    };
    loop {
        let m = tokio::time::timeout(std::time::Duration::from_secs(30), read_message(stream))
            .await
            .expect("read timeout")
            .expect("eof");
        match m.tag {
            b'T' => {
                answer.fields = Some(field_names(&m.payload));
                answer.formats = Some(field_formats(&m.payload));
            }
            b'n' => answer.fields = Some(Vec::new()),
            b'D' => {
                answer.row_widths.push(data_row_width(&m.payload));
                answer
                    .values
                    .push(data_row_values(&m.payload).into_iter().map(|v| v.map(Into::into)).collect());
            }
            b'E' => answer.errors.push(error_text(&m.payload)),
            b'Z' => return answer,
            _ => {}
        }
    }
}

async fn simple_query(stream: &mut tokio::net::TcpStream, sql: &str) -> Answer {
    let mut body = sql.as_bytes().to_vec();
    body.push(0);
    send_message(stream, b'Q', &body).await;
    drain(stream).await
}

/// Parse / Bind / Describe(PORTAL) / Execute / Sync — the exact sequence
/// node-postgres sends for a parameterized query, and the one where the
/// description and the rows come from two separate derivations.
async fn extended_query(stream: &mut tokio::net::TcpStream, sql: &str) -> Answer {
    extended_query_formats(stream, sql, &[]).await
}

/// The extended sequence with explicit Bind result format codes: `&[0]`
/// forces text, `&[1]` forces binary, `&[]` sends none (server default —
/// the protocol makes that all-text, matching what pgx and node-postgres
/// send when they have no reason to prefer binary).
async fn extended_query_formats(
    stream: &mut tokio::net::TcpStream,
    sql: &str,
    result_formats: &[i16],
) -> Answer {
    let mut parse = vec![0u8];
    parse.extend_from_slice(sql.as_bytes());
    parse.push(0);
    parse.extend_from_slice(&0i16.to_be_bytes());
    send_message(stream, b'P', &parse).await;

    let mut bind = vec![0u8, 0u8];
    bind.extend_from_slice(&0i16.to_be_bytes()); // param formats
    bind.extend_from_slice(&0i16.to_be_bytes()); // params
    bind.extend_from_slice(&(result_formats.len() as i16).to_be_bytes());
    for code in result_formats {
        bind.extend_from_slice(&code.to_be_bytes());
    }
    send_message(stream, b'B', &bind).await;

    send_message(stream, b'D', &[b'P', 0]).await;

    let mut execute = vec![0u8];
    execute.extend_from_slice(&0i32.to_be_bytes());
    send_message(stream, b'E', &execute).await;

    send_message(stream, b'S', &[]).await;
    drain(stream).await
}

// ── the test ─────────────────────────────────────────────────────────────────

/// Every one of these was answered with a description that did not describe
/// its rows before the fix. The comment on each is what the client got.
const SHAPES: &[&str] = &[
    // Control: the shape that always worked.
    "SELECT id, label FROM rd_t",
    // Its own LIMIT: `… LIMIT 2 LIMIT 0` does not parse, so zero fields.
    "SELECT id, label FROM rd_t LIMIT 2",
    "SELECT id, label FROM rd_t ORDER BY id LIMIT 2 OFFSET 1",
    // A lock clause after the appended cap: also not SQL.
    "SELECT id FROM rd_t FOR UPDATE",
    // FETCH FIRST is a row limit the appended one collides with.
    "SELECT id FROM rd_t FETCH FIRST 2 ROWS ONLY",
    // Row-returning statements that do not START with SELECT, and so were
    // routed to the RETURNING-clause describer and given zero fields.
    "(SELECT id, label FROM rd_t)",
    "/* planner hint */ SELECT id, label FROM rd_t",
    // Not a parse failure but a silent relabel: the appended text bound into
    // the variable name, and the client was told the column was called
    // `transaction_isolation.LIMIT`.
    "SHOW transaction_isolation",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn describe_matches_the_rows_it_precedes() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(crate::catalog::Catalog::new());
    let engine =
        crate::storage::DiskEngine::open(&dir.path().join("nucleus.db"), catalog.clone()).unwrap();
    let storage: Arc<dyn crate::storage::StorageEngine> = Arc::new(engine);
    let ex = Arc::new(crate::executor::Executor::new_with_persistence(
        catalog,
        storage,
        None,
        Some(dir.path()),
    ));
    let server = Arc::new(NucleusServer::new(Arc::new(NucleusHandler::new(ex))));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accept = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let server = server.clone();
            tokio::spawn(async move {
                let _ = process_socket_closing_on_terminate(socket, None, server).await;
            });
        }
    });

    let mut client = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    startup(&mut client).await;

    // `id` is the PRIMARY KEY: the FOR UPDATE shape in SHAPES is now honoured
    // as a real row lock, and a keyless table is refused ("no identity to
    // lock with") rather than silently returning unlocked rows.
    let setup = simple_query(
        &mut client,
        "CREATE TABLE rd_t (id INT PRIMARY KEY, label TEXT, n INT)",
    )
    .await;
    assert!(setup.errors.is_empty(), "setup: {:?}", setup.errors);
    for i in 0..5 {
        let r = simple_query(
            &mut client,
            &format!(
                "INSERT INTO rd_t (id, label, n) VALUES ({i}, 'l{i}', {})",
                i * 2
            ),
        )
        .await;
        assert!(r.errors.is_empty(), "insert: {:?}", r.errors);
    }

    let mut failures = Vec::new();
    for sql in SHAPES {
        let simple = simple_query(&mut client, sql).await;
        assert!(simple.errors.is_empty(), "{sql}: {:?}", simple.errors);
        let truth = simple.fields.clone().unwrap_or_default();

        let extended = extended_query(&mut client, sql).await;
        if !extended.errors.is_empty() {
            failures.push(format!("{sql}\n    errored: {:?}", extended.errors));
            continue;
        }
        let described = extended.fields.clone().unwrap_or_default();
        if described != truth {
            failures.push(format!(
                "{sql}\n    described as {described:?} but the statement's columns are {truth:?}"
            ));
            continue;
        }
        // The width check is what a positional client would hit; the name
        // check above is what a by-name client would silently swallow.
        if let Some(bad) = extended.row_widths.iter().find(|w| **w != described.len()) {
            failures.push(format!(
                "{sql}\n    described {} columns but sent a {bad}-column DataRow",
                described.len()
            ));
        }
    }

    drop(client);
    accept.abort();
    assert!(
        failures.is_empty(),
        "the RowDescription did not describe the DataRows for {} of {} shapes:\n  {}",
        failures.len(),
        SHAPES.len(),
        failures.join("\n  ")
    );
}

// ── declared format vs payload (GO-31 / Nucleus finding #35) ────────────────

/// The DataRow payload must carry the format the RowDescription declares.
///
/// Integer columns are the historical offender: an engine deployed to a
/// consumer declared text and shipped big-endian binary, and the consumer's
/// Go client grew a heuristic decoder that guessed text-vs-binary per value
/// (the exact bytes decided whether "-123" read as -123 or ~1.7e9). A live
/// socket pins both directions here: text-declared columns carry ASCII
/// decimal — negatives and i64::MIN included — under format code 0 from the
/// simple protocol, the extended protocol's default, and an explicit text
/// Bind, and a client that explicitly Binds format code 1 gets true
/// big-endian int4/int8 payloads under a binary RowDescription.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn integer_payloads_honor_the_declared_format() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(crate::catalog::Catalog::new());
    let engine =
        crate::storage::DiskEngine::open(&dir.path().join("nucleus.db"), catalog.clone()).unwrap();
    let storage: Arc<dyn crate::storage::StorageEngine> = Arc::new(engine);
    let ex = Arc::new(crate::executor::Executor::new_with_persistence(
        catalog,
        storage,
        None,
        Some(dir.path()),
    ));
    let server = Arc::new(NucleusServer::new(Arc::new(NucleusHandler::new(ex))));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accept = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let server = server.clone();
            tokio::spawn(async move {
                let _ = process_socket_closing_on_terminate(socket, None, server).await;
            });
        }
    });

    let mut client = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    startup(&mut client).await;

    let setup = simple_query(
        &mut client,
        "CREATE TABLE fmt_t (a INT, b BIGINT)",
    )
    .await;
    assert!(setup.errors.is_empty(), "setup: {:?}", setup.errors);
    for (a, b) in [(7i64, i64::MAX), (-123, i64::MIN), (0, 0)] {
        let r = simple_query(
            &mut client,
            &format!("INSERT INTO fmt_t (a, b) VALUES ({a}, {b})"),
        )
        .await;
        assert!(r.errors.is_empty(), "insert: {:?}", r.errors);
    }

    // Ordered so the byte-for-byte row assertions below are exact.
    let sql = "SELECT a, b FROM fmt_t ORDER BY a";

    // Every text-declaring mode must ship ASCII decimal, and nothing else.
    let text_answers = vec![
        ("simple", simple_query(&mut client, sql).await),
        (
            "extended-default",
            extended_query(&mut client, sql).await,
        ),
        (
            "extended-text",
            extended_query_formats(&mut client, sql, &[0]).await,
        ),
    ];
    for (mode, answer) in &text_answers {
        assert!(answer.errors.is_empty(), "{mode}: {:?}", answer.errors);
        let formats = answer.formats.as_ref().expect("RowDescription");
        assert!(
            formats.iter().all(|f| *f == 0),
            "{mode}: declared {formats:?}, want all text"
        );
        assert_eq!(answer.values.len(), 3, "{mode}: row count");
        let expected: [(&[u8], &[u8]); 3] = [
            (b"-123", b"-9223372036854775808"),
            (b"0", b"0"),
            (b"7", b"9223372036854775807"),
        ];
        for (row, (want_a, want_b)) in answer.values.iter().zip(expected) {
            assert_eq!(row[0].as_deref(), Some(want_a), "{mode}: int4 payload");
            assert_eq!(row[1].as_deref(), Some(want_b), "{mode}: int8 payload");
        }
    }

    // A client that asks for binary gets it — declared and shipped.
    let binary = extended_query_formats(&mut client, sql, &[1]).await;
    assert!(binary.errors.is_empty(), "{:?}", binary.errors);
    let formats = binary.formats.as_ref().expect("RowDescription");
    assert!(
        formats.iter().all(|f| *f == 1),
        "declared {formats:?}, want all binary"
    );
    let expected: [(&[u8], &[u8]); 3] = [
        (
            &[0xff, 0xff, 0xff, 0x85],
            &[0x80, 0, 0, 0, 0, 0, 0, 0],
        ),
        (&[0, 0, 0, 0], &[0; 8]),
        (&[0, 0, 0, 7], &[0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]),
    ];
    for (row, (want_a, want_b)) in binary.values.iter().zip(expected) {
        assert_eq!(row[0].as_deref(), Some(want_a), "binary int4 payload");
        assert_eq!(row[1].as_deref(), Some(want_b), "binary int8 payload");
    }

    drop(client);
    accept.abort();
}

// ── zero_row_probe ───────────────────────────────────────────────────────────

fn probe_of(sql: &str) -> String {
    let stmts = crate::sql::parse(sql).expect("parse");
    zero_row_probe(stmts)
        .expect("row-returning")
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

#[test]
fn probe_replaces_an_existing_row_cap_instead_of_adding_one() {
    // Text-append produced `… LIMIT 2 LIMIT 0`, which is not SQL.
    assert!(probe_of("SELECT id FROM t LIMIT 2").ends_with("LIMIT 0"));
    assert!(!probe_of("SELECT id FROM t LIMIT 2").contains("LIMIT 2"));
    assert!(probe_of("SELECT id FROM t LIMIT 2 OFFSET 1").ends_with("LIMIT 0"));
    let fetch = probe_of("SELECT id FROM t FETCH FIRST 2 ROWS ONLY");
    assert!(fetch.ends_with("LIMIT 0"), "{fetch}");
    assert!(!fetch.contains("FETCH"), "{fetch}");
}

#[test]
fn probe_takes_no_locks() {
    let probe = probe_of("SELECT id FROM t FOR UPDATE");
    assert!(!probe.contains("FOR UPDATE"), "{probe}");
    assert!(probe.ends_with("LIMIT 0"), "{probe}");
}

#[test]
fn probe_leaves_alone_what_it_cannot_cap() {
    // SHOW has nowhere to put a LIMIT; appending one renamed the variable.
    assert_eq!(
        probe_of("SHOW transaction_isolation"),
        "SHOW transaction_isolation"
    );
}

#[test]
fn probe_declines_what_has_no_single_row_description() {
    // Multi-statement text: describing the first statement's columns over the
    // last statement's rows is the desync, not a fix for it.
    assert!(zero_row_probe(crate::sql::parse("SELECT 1; SELECT 2").unwrap()).is_none());
    // Writes are never probe-executed.
    assert!(zero_row_probe(crate::sql::parse("INSERT INTO t VALUES (1)").unwrap()).is_none());
    assert!(zero_row_probe(crate::sql::parse("CREATE TABLE t (a INT)").unwrap()).is_none());
}
