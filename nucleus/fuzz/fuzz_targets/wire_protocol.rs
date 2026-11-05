#![no_main]
//! Nucleus's inbound pgwire handling, driven without a socket or executor.
//!
//! Framing, the SSL/GSS/startup/cancel magic numbers and the SASL messages are
//! decoded by the external `pgwire` crate; this target uses that decoder only
//! as a front end and fuzzes what Nucleus does with the decoded content:
//!
//! - Query: KV / SQL fast-path parsers, placeholder counting and the
//!   describe-probe rewrite, COPY ... FROM STDIN detection, statement_timeout
//!   parsing.
//! - Parse: `$N` parameter-type inference.
//! - Bind: every parameter through Nucleus's text/binary decoders (int, float,
//!   numeric, bool, text, bytea, uuid, date/timestamp/interval, arrays, jsonb,
//!   unknown OIDs), both the SQL-text substitution and the cached-AST `Value`
//!   path, then placeholder substitution into the statement.
//! - CopyData/CopyDone: the text, CSV and binary COPY row parsers.
//! - CancelRequest: the cancel-key comparison.
//!
//! The first input byte selects a mode:
//!   0 - wire stream starting at SSLRequest/StartupMessage/CancelRequest
//!   1 - wire stream already past startup (typed messages)
//!   2 - one Bind parameter, unframed: [oid idx][flags][payload]
//!   3 - one COPY payload, unframed: [flags][delimiter][payload]
//!
//! Modes 2 and 3 also check that a text parameter rendered as a SQL literal
//! parses back as exactly that string (no breaking out of the quotes).
//!
//! Run: cargo +nightly fuzz run fuzz_wire_protocol -- -max_len=4096

use std::collections::HashMap;

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use nucleus::types::DataType;
use nucleus::wire::fuzz_api as w;
use nucleus::wire::kv_fast_path;
use pgwire::messages::{
    DecodeContext, PgWireFrontendMessage, ProtocolVersion, SslNegotiationMetaMessage,
};

/// OIDs whose decoding Nucleus implements itself, plus an unknown one.
const OIDS: &[u32] = &[
    0, 16, 17, 18, 19, 20, 21, 23, 25, 114, 700, 701, 1042, 1043, 1082, 1114, 1184, 1186, 1700,
    2950, 3802, 705, 1000, 1001, 1005, 1007, 1009, 1015, 1016, 1021, 1022, 1115, 1182, 1185, 1187,
    1231, 2951, 99999,
];

const COPY_TYPES: &[DataType] = &[
    DataType::Bool,
    DataType::Int32,
    DataType::Int64,
    DataType::Float64,
    DataType::Text,
    DataType::Jsonb,
    DataType::Date,
    DataType::Timestamp,
    DataType::TimestampTz,
    DataType::Numeric,
    DataType::Uuid,
    DataType::Bytea,
    DataType::Interval,
    DataType::Vector(3),
];

fn copy_types(seed: u8) -> Vec<DataType> {
    let n = 1 + (seed as usize % 4);
    (0..n)
        .map(|i| COPY_TYPES[(seed as usize / 4 + i * 5) % COPY_TYPES.len()].clone())
        .collect()
}

/// A text literal produced for a bound parameter must parse back as itself.
fn check_literal_roundtrip(value: &str) {
    use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement, Value};
    let lit = format!("'{}'", w::sanitize_text_literal(value));
    let sql = w::substitute_placeholders("SELECT $1", &[lit]);
    let stmts = match nucleus::sql::parse(&sql) {
        Ok(s) => s,
        // The pre-parse nesting guard counts brackets without lexing, so a
        // literal full of '(' is refused; that is a refusal, not an escape.
        Err(e) if e.to_string().contains("nest") => return,
        Err(e) => panic!("bound text literal failed to parse: {sql:?}: {e}"),
    };
    assert_eq!(stmts.len(), 1, "bound literal split the statement: {sql:?}");
    let Statement::Query(q) = &stmts[0] else {
        panic!("not a query: {sql:?}")
    };
    let SetExpr::Select(sel) = q.body.as_ref() else {
        panic!("not a select: {sql:?}")
    };
    assert_eq!(sel.projection.len(), 1, "bound literal escaped: {sql:?}");
    let SelectItem::UnnamedExpr(Expr::Value(v)) = &sel.projection[0] else {
        panic!("bound literal is not a literal: {sql:?}")
    };
    match &v.value {
        Value::SingleQuotedString(s) => assert_eq!(s, &value.replace('\0', "")),
        other => panic!("bound literal parsed as {other:?}: {sql:?}"),
    }
}

fn copy_rows(data: &[u8], delimiter: u8, is_csv: bool, is_binary: bool, header: bool, types: u8) {
    if is_binary {
        let _ = w::parse_copy_binary_rows(data, &copy_types(types));
    } else {
        let _ = w::parse_copy_rows(data, delimiter, is_csv, header);
    }
}

fn on_query(q: &str) -> Option<(u8, bool, bool, bool)> {
    let _ = kv_fast_path::try_parse_kv(q);
    let _ = kv_fast_path::try_parse_sql_fast_path(q);
    let _ = w::count_placeholders(q);
    let _ = w::replace_placeholders_with_null(q);
    let _ = w::parse_timeout_ms(q);
    if let Some(rest) = q.to_ascii_lowercase().split("statement_timeout").nth(1) {
        let v = rest
            .trim_start_matches([' ', '='])
            .trim_start_matches("to ");
        let _ = w::parse_timeout_ms(v);
    }
    w::detect_copy_from_stdin(q)
}

struct Stmt {
    sql: String,
    oids: Vec<u32>,
}

fn on_bind(stmt: Option<&Stmt>, formats: &[i16], params: &[Option<bytes::Bytes>]) {
    let mut reps = Vec::with_capacity(params.len());
    for (i, p) in params.iter().enumerate() {
        let fmt = match formats.len() {
            0 => 0,
            1 => formats[0],
            _ => formats.get(i).copied().unwrap_or(0),
        };
        let oid = stmt.and_then(|s| s.oids.get(i).copied()).unwrap_or(25);
        let raw = p.as_deref();
        let _ = w::bind_param_value(raw, fmt == 1, oid);
        reps.push(w::bind_param_sql(raw, fmt == 1, oid).unwrap_or_else(|| "NULL".into()));
    }
    if let Some(s) = stmt {
        let _ = w::substitute_placeholders(&s.sql, &reps);
    }
}

thread_local! {
    static IN_PGWIRE_DECODE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// pgwire's own body decoders panic on short frames (unchecked `get_*` /
/// `split_to`). Nucleus's connection loop contains that panic and closes the
/// connection (`process_socket_closing_on_terminate`), so here it ends the
/// stream instead of counting as a crash. libFuzzer's panic hook aborts, so
/// it is bypassed only while pgwire is decoding; a panic anywhere in Nucleus
/// still crashes.
fn decode_frontend(buf: &mut BytesMut, ctx: &DecodeContext) -> Option<PgWireFrontendMessage> {
    static HOOK: std::sync::Once = std::sync::Once::new();
    HOOK.call_once(|| {
        let fuzzer_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !IN_PGWIRE_DECODE.with(|f| f.get()) {
                fuzzer_hook(info);
            }
        }));
    });
    IN_PGWIRE_DECODE.with(|f| f.set(true));
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        PgWireFrontendMessage::decode(buf, ctx)
    }));
    IN_PGWIRE_DECODE.with(|f| f.set(false));
    match res {
        Ok(Ok(Some(m))) => Some(m),
        _ => None,
    }
}

fn wire_stream(data: &[u8], past_startup: bool) {
    let mut buf = BytesMut::from(data);
    let mut ctx = DecodeContext::new(ProtocolVersion::PROTOCOL3_0);
    ctx.awaiting_ssl = !past_startup;
    ctx.awaiting_startup = !past_startup;
    let mut stmts: HashMap<String, Stmt> = HashMap::new();
    let mut copy: Option<((u8, bool, bool, bool), Vec<u8>)> = None;

    for _ in 0..64 {
        let Some(msg) = decode_frontend(&mut buf, &ctx) else {
            return;
        };
        match msg {
            PgWireFrontendMessage::SslNegotiation(m) => {
                ctx.awaiting_ssl = false;
                if !matches!(m, SslNegotiationMetaMessage::None) {
                    continue;
                }
            }
            PgWireFrontendMessage::Startup(s) => {
                ctx.awaiting_startup = false;
                if s.protocol_number_minor == 0 {
                    ctx.protocol_version = ProtocolVersion::PROTOCOL3_0;
                }
            }
            PgWireFrontendMessage::CancelRequest(c) => {
                let key = match &c.secret_key {
                    pgwire::messages::startup::SecretKey::I32(v) => v.to_be_bytes().to_vec(),
                    pgwire::messages::startup::SecretKey::Bytes(b) => b.to_vec(),
                };
                let _ = w::constant_time_eq(&key, &0x1234_5678i32.to_be_bytes());
                return;
            }
            PgWireFrontendMessage::Query(q) => {
                if let Some(info) = on_query(&q.query) {
                    copy = Some((info, Vec::new()));
                }
            }
            PgWireFrontendMessage::Parse(p) => {
                let _ = w::count_placeholders(&p.query);
                let oids = w::infer_parameter_oids(&p.query, &p.type_oids);
                if let Some(info) = w::detect_copy_from_stdin(&p.query) {
                    copy = Some((info, Vec::new()));
                }
                let name = p.name.unwrap_or_default();
                stmts.insert(name, Stmt { sql: p.query, oids });
            }
            PgWireFrontendMessage::Bind(b) => {
                let name = b.statement_name.unwrap_or_default();
                on_bind(stmts.get(&name), &b.parameter_format_codes, &b.parameters);
            }
            PgWireFrontendMessage::CopyData(d) => {
                if let Some((_, acc)) = copy.as_mut() {
                    acc.extend_from_slice(&d.data);
                }
            }
            PgWireFrontendMessage::CopyDone(_) => {
                if let Some(((delim, csv, bin, header), acc)) = copy.take() {
                    copy_rows(&acc, delim, csv, bin, header, acc.len() as u8);
                }
            }
            PgWireFrontendMessage::CopyFail(_) => copy = None,
            PgWireFrontendMessage::Terminate(_) => return,
            _ => {}
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };
    match mode % 4 {
        0 => wire_stream(rest, false),
        1 => wire_stream(rest, true),
        2 => {
            let [idx, flags, payload @ ..] = rest else {
                return;
            };
            let oid = OIDS[*idx as usize % OIDS.len()];
            let is_binary = flags & 1 != 0;
            let raw = (flags & 2 == 0).then_some(payload);
            let _ = w::bind_param_value(raw, is_binary, oid);
            if let Some(sql) = w::bind_param_sql(raw, is_binary, oid)
                && !is_binary
                && let Ok(text) = std::str::from_utf8(payload)
                && sql.starts_with('\'')
            {
                check_literal_roundtrip(text);
            }
        }
        _ => {
            let [flags, delim, payload @ ..] = rest else {
                return;
            };
            copy_rows(
                payload,
                *delim,
                flags & 1 != 0,
                flags & 2 != 0,
                flags & 4 != 0,
                flags >> 3,
            );
            if let Ok(text) = std::str::from_utf8(payload) {
                check_literal_roundtrip(text);
            }
        }
    }
});
