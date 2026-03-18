//! RESP command handler.
//!
//! Maps Redis commands received over the RESP2 protocol to the Nucleus KV store
//! API. Supports string, list, hash, set, sorted set, HyperLogLog, stream,
//! geo, and pub/sub commands.

use std::sync::Arc;

use crate::kv::KvStore;
use crate::resp::encoder;
use crate::resp::pubsub_registry::{PubSubRegistry, Subscription};
use crate::types::Value;

/// Constant-time byte comparison to prevent timing attacks on password checks.
fn constant_time_eq(lhs: &[u8], rhs: &[u8]) -> bool {
    let max_len = lhs.len().max(rhs.len());
    let mut diff = lhs.len() ^ rhs.len();
    for i in 0..max_len {
        let l = lhs.get(i).copied().unwrap_or(0);
        let r = rhs.get(i).copied().unwrap_or(0);
        diff |= (l ^ r) as usize;
    }
    diff == 0
}

/// Handles RESP commands against a shared KV store.
pub struct RespHandler {
    kv: Arc<KvStore>,
    password: Option<String>,
    authenticated: bool,
    /// When Some, we are in a MULTI transaction — commands are queued.
    transaction_queue: Option<Vec<Vec<Vec<u8>>>>,
    /// WATCH'd keys and their version at WATCH time.
    watched_keys: Vec<(String, u64)>,
    /// Shared pub/sub registry.
    pubsub: Arc<PubSubRegistry>,
    /// This connection's subscriber ID (allocated lazily on first SUBSCRIBE).
    pubsub_id: Option<u64>,
    /// Subscription receiver (allocated lazily).
    pubsub_sub: Option<Subscription>,
}

impl RespHandler {
    /// Create a new handler.
    ///
    /// If `password` is `None`, the handler starts in the authenticated state
    /// (no auth required). Otherwise, clients must issue AUTH before any other
    /// command.
    pub fn new(kv: Arc<KvStore>, password: Option<String>, pubsub: Arc<PubSubRegistry>) -> Self {
        let authenticated = password.is_none();
        Self {
            kv,
            password,
            authenticated,
            transaction_queue: None,
            watched_keys: Vec::new(),
            pubsub,
            pubsub_id: None,
            pubsub_sub: None,
        }
    }

    /// Returns true if this connection is in pub/sub subscriber mode.
    pub fn is_in_pubsub_mode(&self) -> bool {
        if let Some(id) = self.pubsub_id {
            self.pubsub.subscription_count(id) > 0
        } else {
            false
        }
    }

    /// Ensure a pub/sub subscriber ID is allocated for this connection.
    fn ensure_pubsub_id(&mut self) {
        if self.pubsub_id.is_none() {
            let (id, sub) = self.pubsub.new_subscriber();
            self.pubsub_id = Some(id);
            self.pubsub_sub = Some(sub);
        }
    }

    /// Clean up pub/sub state on connection close.
    pub fn cleanup_pubsub(&mut self) {
        if let Some(id) = self.pubsub_id.take() {
            self.pubsub.remove_subscriber(id);
        }
        self.pubsub_sub = None;
    }

    /// Receive a pub/sub message (for the server loop to select! on).
    /// Returns the RESP-encoded push message, or None if the channel is closed.
    pub async fn recv_pubsub_message(&mut self) -> Option<Vec<u8>> {
        let sub = self.pubsub_sub.as_mut()?;
        let msg = sub.rx.recv().await?;
        if msg.is_pattern {
            // Pattern message: *4\r\n $8\r\npmessage\r\n $pattern\r\n $channel\r\n $payload\r\n
            let mut resp = encoder::encode_array_header(4);
            resp.extend(encoder::encode_bulk_string(b"pmessage"));
            resp.extend(encoder::encode_bulk_string(msg.channel.as_bytes()));
            resp.extend(encoder::encode_bulk_string(msg.actual_channel.as_bytes()));
            resp.extend(encoder::encode_bulk_string(msg.payload.as_bytes()));
            Some(resp)
        } else {
            // Direct message: *3\r\n $7\r\nmessage\r\n $channel\r\n $payload\r\n
            let mut resp = encoder::encode_array_header(3);
            resp.extend(encoder::encode_bulk_string(b"message"));
            resp.extend(encoder::encode_bulk_string(msg.channel.as_bytes()));
            resp.extend(encoder::encode_bulk_string(msg.payload.as_bytes()));
            Some(resp)
        }
    }

    /// Handle a command while in pub/sub mode. Only SUBSCRIBE, UNSUBSCRIBE,
    /// PSUBSCRIBE, PUNSUBSCRIBE, PING, and QUIT are valid. Returns a list of
    /// response frames (one per channel for SUBSCRIBE/UNSUBSCRIBE).
    pub fn handle_pubsub_command(&mut self, args: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        if args.is_empty() {
            return vec![encoder::encode_error("ERR empty command")];
        }

        let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();

        match cmd.as_str() {
            "SUBSCRIBE" => {
                if args.len() < 2 {
                    return vec![encoder::encode_error(
                        "ERR wrong number of arguments for 'subscribe' command",
                    )];
                }
                self.ensure_pubsub_id();
                let id = self.pubsub_id.unwrap();
                let mut responses = Vec::new();
                for ch_arg in &args[1..] {
                    let channel = String::from_utf8_lossy(ch_arg).to_string();
                    let count = self.pubsub.subscribe(id, &channel);
                    let mut resp = encoder::encode_array_header(3);
                    resp.extend(encoder::encode_bulk_string(b"subscribe"));
                    resp.extend(encoder::encode_bulk_string(channel.as_bytes()));
                    resp.extend(encoder::encode_integer(count as i64));
                    responses.push(resp);
                }
                responses
            }
            "UNSUBSCRIBE" => {
                let id = match self.pubsub_id {
                    Some(id) => id,
                    None => {
                        // Not subscribed to anything — return confirmation with count 0
                        if args.len() < 2 {
                            let mut resp = encoder::encode_array_header(3);
                            resp.extend(encoder::encode_bulk_string(b"unsubscribe"));
                            resp.extend(encoder::encode_null_bulk());
                            resp.extend(encoder::encode_integer(0));
                            return vec![resp];
                        }
                        let mut responses = Vec::new();
                        for ch_arg in &args[1..] {
                            let channel = String::from_utf8_lossy(ch_arg).to_string();
                            let mut resp = encoder::encode_array_header(3);
                            resp.extend(encoder::encode_bulk_string(b"unsubscribe"));
                            resp.extend(encoder::encode_bulk_string(channel.as_bytes()));
                            resp.extend(encoder::encode_integer(0));
                            responses.push(resp);
                        }
                        return responses;
                    }
                };
                if args.len() < 2 {
                    // Unsubscribe from all channels
                    let channels = self.pubsub.unsubscribe_all(id);
                    if channels.is_empty() {
                        let mut resp = encoder::encode_array_header(3);
                        resp.extend(encoder::encode_bulk_string(b"unsubscribe"));
                        resp.extend(encoder::encode_null_bulk());
                        resp.extend(encoder::encode_integer(0));
                        return vec![resp];
                    }
                    let mut responses = Vec::new();
                    for (i, ch) in channels.iter().enumerate() {
                        let remaining = channels.len() - i - 1;
                        let pat_count = self.pubsub.subscription_count(id).saturating_sub(remaining);
                        let _ = pat_count; // count from registry
                        let count = self.pubsub.subscription_count(id);
                        let mut resp = encoder::encode_array_header(3);
                        resp.extend(encoder::encode_bulk_string(b"unsubscribe"));
                        resp.extend(encoder::encode_bulk_string(ch.as_bytes()));
                        resp.extend(encoder::encode_integer(count as i64));
                        responses.push(resp);
                    }
                    responses
                } else {
                    let mut responses = Vec::new();
                    for ch_arg in &args[1..] {
                        let channel = String::from_utf8_lossy(ch_arg).to_string();
                        let count = self.pubsub.unsubscribe(id, &channel);
                        let mut resp = encoder::encode_array_header(3);
                        resp.extend(encoder::encode_bulk_string(b"unsubscribe"));
                        resp.extend(encoder::encode_bulk_string(channel.as_bytes()));
                        resp.extend(encoder::encode_integer(count as i64));
                        responses.push(resp);
                    }
                    responses
                }
            }
            "PSUBSCRIBE" => {
                if args.len() < 2 {
                    return vec![encoder::encode_error(
                        "ERR wrong number of arguments for 'psubscribe' command",
                    )];
                }
                self.ensure_pubsub_id();
                let id = self.pubsub_id.unwrap();
                let mut responses = Vec::new();
                for pat_arg in &args[1..] {
                    let pattern = String::from_utf8_lossy(pat_arg).to_string();
                    let count = self.pubsub.psubscribe(id, &pattern);
                    let mut resp = encoder::encode_array_header(3);
                    resp.extend(encoder::encode_bulk_string(b"psubscribe"));
                    resp.extend(encoder::encode_bulk_string(pattern.as_bytes()));
                    resp.extend(encoder::encode_integer(count as i64));
                    responses.push(resp);
                }
                responses
            }
            "PUNSUBSCRIBE" => {
                let id = match self.pubsub_id {
                    Some(id) => id,
                    None => {
                        if args.len() < 2 {
                            let mut resp = encoder::encode_array_header(3);
                            resp.extend(encoder::encode_bulk_string(b"punsubscribe"));
                            resp.extend(encoder::encode_null_bulk());
                            resp.extend(encoder::encode_integer(0));
                            return vec![resp];
                        }
                        let mut responses = Vec::new();
                        for pat_arg in &args[1..] {
                            let pattern = String::from_utf8_lossy(pat_arg).to_string();
                            let mut resp = encoder::encode_array_header(3);
                            resp.extend(encoder::encode_bulk_string(b"punsubscribe"));
                            resp.extend(encoder::encode_bulk_string(pattern.as_bytes()));
                            resp.extend(encoder::encode_integer(0));
                            responses.push(resp);
                        }
                        return responses;
                    }
                };
                if args.len() < 2 {
                    let patterns = self.pubsub.punsubscribe_all(id);
                    if patterns.is_empty() {
                        let mut resp = encoder::encode_array_header(3);
                        resp.extend(encoder::encode_bulk_string(b"punsubscribe"));
                        resp.extend(encoder::encode_null_bulk());
                        resp.extend(encoder::encode_integer(
                            self.pubsub.subscription_count(id) as i64,
                        ));
                        return vec![resp];
                    }
                    let mut responses = Vec::new();
                    for pat in &patterns {
                        let count = self.pubsub.subscription_count(id);
                        let mut resp = encoder::encode_array_header(3);
                        resp.extend(encoder::encode_bulk_string(b"punsubscribe"));
                        resp.extend(encoder::encode_bulk_string(pat.as_bytes()));
                        resp.extend(encoder::encode_integer(count as i64));
                        responses.push(resp);
                    }
                    responses
                } else {
                    let mut responses = Vec::new();
                    for pat_arg in &args[1..] {
                        let pattern = String::from_utf8_lossy(pat_arg).to_string();
                        let count = self.pubsub.punsubscribe(id, &pattern);
                        let mut resp = encoder::encode_array_header(3);
                        resp.extend(encoder::encode_bulk_string(b"punsubscribe"));
                        resp.extend(encoder::encode_bulk_string(pattern.as_bytes()));
                        resp.extend(encoder::encode_integer(count as i64));
                        responses.push(resp);
                    }
                    responses
                }
            }
            "PING" => {
                if args.len() > 1 {
                    vec![encoder::encode_bulk_string(&args[1])]
                } else {
                    vec![encoder::encode_simple_string("PONG")]
                }
            }
            _ => {
                vec![encoder::encode_error(&format!(
                    "ERR Can't execute '{cmd}': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context"
                ))]
            }
        }
    }

    /// Process a single command and return the RESP2-encoded response bytes.
    pub fn handle_command(&mut self, args: Vec<Vec<u8>>) -> Vec<u8> {
        if args.is_empty() {
            return encoder::encode_error("ERR empty command");
        }

        let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();

        // Auth check: PING, AUTH, and QUIT are always allowed.
        if !self.authenticated && cmd != "AUTH" && cmd != "PING" && cmd != "QUIT" {
            return encoder::encode_error("NOAUTH Authentication required.");
        }

        // MULTI/EXEC/DISCARD/WATCH are handled before the command dispatch
        // because they control the transaction queue.
        match cmd.as_str() {
            "MULTI" => {
                if self.transaction_queue.is_some() {
                    return encoder::encode_error("ERR MULTI calls can not be nested");
                }
                self.transaction_queue = Some(Vec::new());
                return encoder::encode_simple_string("OK");
            }
            "EXEC" => {
                return self.exec_transaction();
            }
            "DISCARD" => {
                if self.transaction_queue.is_none() {
                    return encoder::encode_error("ERR DISCARD without MULTI");
                }
                self.transaction_queue = None;
                self.watched_keys.clear();
                return encoder::encode_simple_string("OK");
            }
            "WATCH" => {
                if self.transaction_queue.is_some() {
                    return encoder::encode_error("ERR WATCH inside MULTI is not allowed");
                }
                if args.len() < 2 {
                    return encoder::encode_error("ERR wrong number of arguments for 'watch' command");
                }
                for arg in &args[1..] {
                    let key = String::from_utf8_lossy(arg).to_string();
                    let version = self.kv.key_version(&key);
                    self.watched_keys.push((key, version));
                }
                return encoder::encode_simple_string("OK");
            }
            "UNWATCH" => {
                self.watched_keys.clear();
                return encoder::encode_simple_string("OK");
            }
            _ => {}
        }

        // If inside a MULTI block, queue the command instead of executing it.
        if let Some(ref mut queue) = self.transaction_queue {
            queue.push(args);
            return encoder::encode_simple_string("QUEUED");
        }

        match cmd.as_str() {
            // ================================================================
            // Connection / meta
            // ================================================================
            "PING" => {
                if args.len() > 1 {
                    encoder::encode_bulk_string(&args[1])
                } else {
                    encoder::encode_simple_string("PONG")
                }
            }
            "AUTH" => {
                if args.len() < 2 {
                    return encoder::encode_error("ERR wrong number of arguments for 'auth' command");
                }
                let provided = String::from_utf8_lossy(&args[1]).to_string();
                match &self.password {
                    Some(pw) if constant_time_eq(pw.as_bytes(), provided.as_bytes()) => {
                        self.authenticated = true;
                        encoder::encode_simple_string("OK")
                    }
                    Some(_) => encoder::encode_error("ERR invalid password"),
                    None => {
                        // No password set -- AUTH is a no-op (client compat).
                        encoder::encode_simple_string("OK")
                    }
                }
            }
            "SELECT" => {
                // Nucleus has a single keyspace; accept and ignore the DB index.
                encoder::encode_simple_string("OK")
            }
            "QUIT" => encoder::encode_simple_string("OK"),

            // ================================================================
            // String commands
            // ================================================================
            "GET" => {
                let key = require_arg!(args, 1);
                match self.kv.get(key) {
                    Some(v) => encode_kv_value(&v),
                    None => encoder::encode_null_bulk(),
                }
            }
            "SET" => {
                let key = require_arg!(args, 1);
                let val = require_arg_bytes!(args, 2);

                // Parse optional EX/PX/EXAT/PXAT modifiers.
                let mut ttl_secs: Option<u64> = None;
                let mut i = 3;
                while i < args.len() {
                    let modifier = String::from_utf8_lossy(&args[i]).to_uppercase();
                    match modifier.as_str() {
                        "EX" => {
                            i += 1;
                            if i >= args.len() {
                                return encoder::encode_error("ERR syntax error");
                            }
                            let s = String::from_utf8_lossy(&args[i]);
                            ttl_secs = s.parse::<u64>().ok();
                        }
                        "PX" => {
                            i += 1;
                            if i >= args.len() {
                                return encoder::encode_error("ERR syntax error");
                            }
                            let s = String::from_utf8_lossy(&args[i]);
                            if let Ok(ms) = s.parse::<u64>() {
                                // Convert milliseconds to seconds (round up).
                                ttl_secs = Some(ms.div_ceil(1000));
                            }
                        }
                        "NX" | "XX" | "KEEPTTL" | "GET" | "EXAT" | "PXAT" => {
                            // Accepted but not fully implemented -- skip.
                        }
                        _ => {}
                    }
                    i += 1;
                }

                let value = Value::Text(String::from_utf8_lossy(val).to_string());
                self.kv.set(key, value, ttl_secs);
                encoder::encode_simple_string("OK")
            }
            "DEL" => {
                if args.len() < 2 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'del' command",
                    );
                }
                let mut count: i64 = 0;
                for arg in &args[1..] {
                    let key = std::str::from_utf8(arg).unwrap_or("");
                    if self.kv.del(key) {
                        count += 1;
                    }
                    // Also try collections
                    if self.kv.collections().del(key) {
                        count += 1;
                    }
                }
                encoder::encode_integer(count)
            }
            "EXISTS" => {
                let key = require_arg!(args, 1);
                let exists =
                    self.kv.exists(key) || self.kv.collections().exists(key);
                encoder::encode_integer(if exists { 1 } else { 0 })
            }
            "INCR" => {
                let key = require_arg!(args, 1);
                match self.kv.incr(key) {
                    Ok(n) => encoder::encode_integer(n),
                    Err(e) => encoder::encode_error(&format!("ERR {e}")),
                }
            }
            "INCRBY" => {
                let key = require_arg!(args, 1);
                let amount = require_i64!(args, 2);
                match self.kv.incr_by(key, amount) {
                    Ok(n) => encoder::encode_integer(n),
                    Err(e) => encoder::encode_error(&format!("ERR {e}")),
                }
            }
            "DECR" => {
                let key = require_arg!(args, 1);
                match self.kv.incr_by(key, -1) {
                    Ok(n) => encoder::encode_integer(n),
                    Err(e) => encoder::encode_error(&format!("ERR {e}")),
                }
            }
            "DECRBY" => {
                let key = require_arg!(args, 1);
                let amount = require_i64!(args, 2);
                match self.kv.incr_by(key, -amount) {
                    Ok(n) => encoder::encode_integer(n),
                    Err(e) => encoder::encode_error(&format!("ERR {e}")),
                }
            }
            "TTL" => {
                let key = require_arg!(args, 1);
                encoder::encode_integer(self.kv.ttl(key))
            }
            "EXPIRE" => {
                let key = require_arg!(args, 1);
                let secs = require_u64!(args, 2);
                let ok = self.kv.expire(key, secs);
                encoder::encode_integer(if ok { 1 } else { 0 })
            }
            "PERSIST" => {
                let key = require_arg!(args, 1);
                let ok = self.kv.persist(key);
                encoder::encode_integer(if ok { 1 } else { 0 })
            }
            "SETNX" => {
                let key = require_arg!(args, 1);
                let val = require_arg_bytes!(args, 2);
                let value = Value::Text(String::from_utf8_lossy(val).to_string());
                let ok = self.kv.setnx(key, value);
                encoder::encode_integer(if ok { 1 } else { 0 })
            }
            "DBSIZE" => encoder::encode_integer(self.kv.dbsize() as i64),
            "FLUSHDB" | "FLUSHALL" => {
                self.kv.flushdb();
                encoder::encode_simple_string("OK")
            }
            "KEYS" => {
                let pattern = if args.len() > 1 {
                    std::str::from_utf8(&args[1]).unwrap_or("*")
                } else {
                    "*"
                };
                let keys = self.kv.keys(pattern);
                let mut out = encoder::encode_array_header(keys.len());
                for k in &keys {
                    out.extend(encoder::encode_bulk_string(k.as_bytes()));
                }
                out
            }
            "MGET" => {
                if args.len() < 2 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'mget' command",
                    );
                }
                let keys: Vec<&str> = args[1..]
                    .iter()
                    .map(|a| std::str::from_utf8(a).unwrap_or(""))
                    .collect();
                let values = self.kv.mget(&keys);
                let mut out = encoder::encode_array_header(values.len());
                for val in &values {
                    match val {
                        Some(v) => out.extend(encode_kv_value(v)),
                        None => out.extend(encoder::encode_null_bulk()),
                    }
                }
                out
            }
            "MSET" => {
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'mset' command",
                    );
                }
                let mut pairs = Vec::new();
                let mut i = 1;
                while i + 1 < args.len() {
                    let key = std::str::from_utf8(&args[i]).unwrap_or("").to_string();
                    let val_str =
                        String::from_utf8_lossy(&args[i + 1]).to_string();
                    pairs.push((key, Value::Text(val_str)));
                    i += 2;
                }
                let refs: Vec<(&str, Value)> =
                    pairs.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
                self.kv.mset(&refs);
                encoder::encode_simple_string("OK")
            }
            "APPEND" => {
                let key = require_arg!(args, 1);
                let val = require_arg_bytes!(args, 2);
                let new_val = match self.kv.get(key) {
                    Some(Value::Text(existing)) => {
                        let mut s = existing;
                        s.push_str(&String::from_utf8_lossy(val));
                        s
                    }
                    Some(_) => {
                        return encoder::encode_error(
                            "ERR value is not a string",
                        );
                    }
                    None => String::from_utf8_lossy(val).to_string(),
                };
                let len = new_val.len() as i64;
                self.kv.set(key, Value::Text(new_val), None);
                encoder::encode_integer(len)
            }
            "STRLEN" => {
                let key = require_arg!(args, 1);
                match self.kv.get(key) {
                    Some(Value::Text(s)) => encoder::encode_integer(s.len() as i64),
                    Some(_) => encoder::encode_error("ERR value is not a string"),
                    None => encoder::encode_integer(0),
                }
            }

            // ================================================================
            // Bitmap commands
            // ================================================================
            "SETBIT" => {
                let key = require_arg!(args, 1);
                let offset: usize = match String::from_utf8_lossy(require_arg_bytes!(args, 2)).parse() {
                    Ok(v) => v,
                    Err(_) => return encoder::encode_error("ERR bit offset is not an integer or out of range"),
                };
                let bit: u8 = match String::from_utf8_lossy(require_arg_bytes!(args, 3)).parse() {
                    Ok(v) if v <= 1 => v,
                    _ => return encoder::encode_error("ERR bit is not an integer or out of range"),
                };
                let old = self.kv.setbit(key, offset, bit == 1);
                encoder::encode_integer(old as i64)
            }
            "GETBIT" => {
                let key = require_arg!(args, 1);
                let offset: usize = match String::from_utf8_lossy(require_arg_bytes!(args, 2)).parse() {
                    Ok(v) => v,
                    Err(_) => return encoder::encode_error("ERR bit offset is not an integer or out of range"),
                };
                let bit = self.kv.getbit(key, offset);
                encoder::encode_integer(bit as i64)
            }
            "BITCOUNT" => {
                let key = require_arg!(args, 1);
                let (start, end) = if args.len() >= 4 {
                    let s: i64 = match String::from_utf8_lossy(&args[2]).parse() {
                        Ok(v) => v,
                        Err(_) => return encoder::encode_error("ERR value is not an integer or out of range"),
                    };
                    let e: i64 = match String::from_utf8_lossy(&args[3]).parse() {
                        Ok(v) => v,
                        Err(_) => return encoder::encode_error("ERR value is not an integer or out of range"),
                    };
                    (Some(s), Some(e))
                } else {
                    (None, None)
                };
                let count = self.kv.bitcount(key, start, end);
                encoder::encode_integer(count as i64)
            }
            "BITOP" => {
                if args.len() < 4 {
                    return encoder::encode_error("ERR wrong number of arguments for 'bitop' command");
                }
                let op = String::from_utf8_lossy(&args[1]).to_uppercase();
                let dest = String::from_utf8_lossy(&args[2]).to_string();
                if op == "NOT" && args.len() != 4 {
                    return encoder::encode_error("ERR BITOP NOT requires one and only one key");
                }
                let src_keys: Vec<&str> = args[3..].iter()
                    .map(|a| std::str::from_utf8(a).unwrap_or(""))
                    .collect();
                let len = self.kv.bitop(&op, &dest, &src_keys);
                encoder::encode_integer(len as i64)
            }
            "BITPOS" => {
                let key = require_arg!(args, 1);
                let bit: u8 = match String::from_utf8_lossy(require_arg_bytes!(args, 2)).parse() {
                    Ok(v) if v <= 1 => v,
                    _ => return encoder::encode_error("ERR bit is not an integer or out of range"),
                };
                let start = if args.len() >= 4 {
                    Some(String::from_utf8_lossy(&args[3]).parse::<i64>().unwrap_or(0))
                } else {
                    None
                };
                let end = if args.len() >= 5 {
                    Some(String::from_utf8_lossy(&args[4]).parse::<i64>().unwrap_or(-1))
                } else {
                    None
                };
                let pos = self.kv.bitpos(key, bit == 1, start, end);
                encoder::encode_integer(pos)
            }

            // ================================================================
            // Stream commands
            // ================================================================
            "XADD" => {
                // XADD key ID field value [field value ...]
                let key = require_arg!(args, 1);
                let id_str = require_arg!(args, 2);
                if args.len() < 5 || !(args.len() - 3).is_multiple_of(2) {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'xadd' command",
                    );
                }
                let mut fields = Vec::new();
                let mut i = 3;
                while i + 1 < args.len() {
                    let field = String::from_utf8_lossy(&args[i]).to_string();
                    let value = String::from_utf8_lossy(&args[i + 1]).to_string();
                    fields.push((field, value));
                    i += 2;
                }
                match self.kv.xadd(key, id_str, fields) {
                    Ok(id) => encoder::encode_bulk_string(id.to_string().as_bytes()),
                    Err(e) => encoder::encode_error(&e),
                }
            }
            "XLEN" => {
                let key = require_arg!(args, 1);
                match self.kv.xlen(key) {
                    Ok(len) => encoder::encode_integer(len as i64),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "XRANGE" => {
                // XRANGE key start end [COUNT count]
                let key = require_arg!(args, 1);
                let start = require_arg!(args, 2);
                let end = require_arg!(args, 3);
                let count = if args.len() >= 6 {
                    let c_str = String::from_utf8_lossy(&args[5]);
                    c_str.parse::<usize>().ok()
                } else {
                    None
                };
                match self.kv.xrange(key, start, end, count) {
                    Ok(entries) => encode_stream_entries(&entries),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "XREVRANGE" => {
                let key = require_arg!(args, 1);
                let end = require_arg!(args, 2);
                let start = require_arg!(args, 3);
                let count = if args.len() >= 6 {
                    let c_str = String::from_utf8_lossy(&args[5]);
                    c_str.parse::<usize>().ok()
                } else {
                    None
                };
                match self.kv.xrevrange(key, end, start, count) {
                    Ok(entries) => encode_stream_entries(&entries),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "XREAD" => {
                // XREAD [COUNT count] STREAMS key [key ...] id [id ...]
                // Simplified: XREAD COUNT count STREAMS key id
                let mut count: Option<usize> = None;
                let mut idx = 1;
                if args.len() > idx && String::from_utf8_lossy(&args[idx]).to_uppercase() == "COUNT" {
                    idx += 1;
                    if idx < args.len() {
                        count = String::from_utf8_lossy(&args[idx]).parse::<usize>().ok();
                        idx += 1;
                    }
                }
                // Expect STREAMS keyword
                if idx >= args.len() || String::from_utf8_lossy(&args[idx]).to_uppercase() != "STREAMS" {
                    return encoder::encode_error("ERR syntax error");
                }
                idx += 1;
                // Remaining args: keys... ids... (split in half)
                let remaining = args.len() - idx;
                if remaining == 0 || !remaining.is_multiple_of(2) {
                    return encoder::encode_error("ERR syntax error");
                }
                let half = remaining / 2;
                let keys: Vec<&str> = (idx..idx + half)
                    .map(|i| std::str::from_utf8(&args[i]).unwrap_or(""))
                    .collect();
                let ids: Vec<&str> = (idx + half..args.len())
                    .map(|i| std::str::from_utf8(&args[i]).unwrap_or(""))
                    .collect();

                // Build response: array of [key, entries] pairs
                let mut resp = format!("*{}\r\n", keys.len()).into_bytes();
                for (key, id) in keys.iter().zip(ids.iter()) {
                    match self.kv.xread(key, id, count) {
                        Ok(entries) => {
                            resp.extend_from_slice(b"*2\r\n");
                            resp.extend_from_slice(&encoder::encode_bulk_string(key.as_bytes()));
                            resp.extend_from_slice(&encode_stream_entries(&entries));
                        }
                        Err(e) => return encode_wrongtype(&e),
                    }
                }
                resp
            }
            "XDEL" => {
                let key = require_arg!(args, 1);
                if args.len() < 3 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'xdel' command",
                    );
                }
                let mut ids = Vec::new();
                for arg in &args[2..] {
                    let id_str = String::from_utf8_lossy(arg);
                    if let Some(id) = crate::kv::streams::StreamId::parse(&id_str) {
                        ids.push(id);
                    }
                }
                match self.kv.xdel(key, &ids) {
                    Ok(n) => encoder::encode_integer(n as i64),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "XTRIM" => {
                // XTRIM key MAXLEN [~] count
                let key = require_arg!(args, 1);
                if args.len() < 4 {
                    return encoder::encode_error("ERR syntax error");
                }
                let maxlen_kw = String::from_utf8_lossy(&args[2]).to_uppercase();
                if maxlen_kw != "MAXLEN" {
                    return encoder::encode_error("ERR syntax error");
                }
                // Skip optional ~ (approximate trimming — we just do exact)
                let count_idx = if args.len() >= 5 && &args[3] == b"~" { 4 } else { 3 };
                let maxlen: usize = match String::from_utf8_lossy(&args[count_idx]).parse() {
                    Ok(v) => v,
                    Err(_) => return encoder::encode_error("ERR value is not an integer"),
                };
                match self.kv.xtrim_maxlen(key, maxlen) {
                    Ok(n) => encoder::encode_integer(n as i64),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "XGROUP" => {
                // XGROUP CREATE key group id
                // XGROUP DESTROY key group
                if args.len() < 3 {
                    return encoder::encode_error("ERR syntax error");
                }
                let subcmd = String::from_utf8_lossy(&args[1]).to_uppercase();
                match subcmd.as_str() {
                    "CREATE" => {
                        if args.len() < 5 {
                            return encoder::encode_error(
                                "ERR wrong number of arguments for 'xgroup create' command",
                            );
                        }
                        let key = std::str::from_utf8(&args[2]).unwrap_or("");
                        let group = std::str::from_utf8(&args[3]).unwrap_or("");
                        let id = std::str::from_utf8(&args[4]).unwrap_or("$");
                        match self.kv.xgroup_create(key, group, id) {
                            Ok(()) => encoder::encode_simple_string("OK"),
                            Err(e) => encoder::encode_error(&e),
                        }
                    }
                    "DESTROY" => {
                        if args.len() < 4 {
                            return encoder::encode_error(
                                "ERR wrong number of arguments for 'xgroup destroy' command",
                            );
                        }
                        let key = std::str::from_utf8(&args[2]).unwrap_or("");
                        let group = std::str::from_utf8(&args[3]).unwrap_or("");
                        match self.kv.xgroup_destroy(key, group) {
                            Ok(true) => encoder::encode_integer(1),
                            Ok(false) => encoder::encode_integer(0),
                            Err(e) => encode_wrongtype(&e),
                        }
                    }
                    _ => encoder::encode_error(&format!(
                        "ERR unknown subcommand '{subcmd}' for 'xgroup'"
                    )),
                }
            }
            "XREADGROUP" => {
                // XREADGROUP GROUP group consumer [COUNT count] STREAMS key id
                if args.len() < 7 {
                    return encoder::encode_error("ERR syntax error");
                }
                let group_kw = String::from_utf8_lossy(&args[1]).to_uppercase();
                if group_kw != "GROUP" {
                    return encoder::encode_error("ERR syntax error");
                }
                let group = std::str::from_utf8(&args[2]).unwrap_or("");
                let consumer = std::str::from_utf8(&args[3]).unwrap_or("");

                let mut idx = 4;
                let mut count: Option<usize> = None;
                if idx < args.len() && String::from_utf8_lossy(&args[idx]).to_uppercase() == "COUNT" {
                    idx += 1;
                    if idx < args.len() {
                        count = String::from_utf8_lossy(&args[idx]).parse::<usize>().ok();
                        idx += 1;
                    }
                }
                if idx >= args.len() || String::from_utf8_lossy(&args[idx]).to_uppercase() != "STREAMS" {
                    return encoder::encode_error("ERR syntax error");
                }
                idx += 1;
                // key id pairs
                let remaining = args.len() - idx;
                if remaining < 2 || !remaining.is_multiple_of(2) {
                    return encoder::encode_error("ERR syntax error");
                }
                let half = remaining / 2;
                let key = std::str::from_utf8(&args[idx]).unwrap_or("");
                let pending_id = std::str::from_utf8(&args[idx + half]).unwrap_or(">");

                match self.kv.xreadgroup(key, group, consumer, pending_id, count) {
                    Ok(entries) => {
                        let mut resp = b"*1\r\n*2\r\n".to_vec();
                        resp.extend_from_slice(&encoder::encode_bulk_string(key.as_bytes()));
                        resp.extend_from_slice(&encode_stream_entries(&entries));
                        resp
                    }
                    Err(e) => encoder::encode_error(&e),
                }
            }
            "XACK" => {
                // XACK key group id [id ...]
                if args.len() < 4 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'xack' command",
                    );
                }
                let key = std::str::from_utf8(&args[1]).unwrap_or("");
                let group = std::str::from_utf8(&args[2]).unwrap_or("");
                let mut ids = Vec::new();
                for arg in &args[3..] {
                    let id_str = String::from_utf8_lossy(arg);
                    if let Some(id) = crate::kv::streams::StreamId::parse(&id_str) {
                        ids.push(id);
                    }
                }
                match self.kv.xack(key, group, &ids) {
                    Ok(n) => encoder::encode_integer(n as i64),
                    Err(e) => encoder::encode_error(&e),
                }
            }

            // ================================================================
            // List commands
            // ================================================================
            "LPUSH" => {
                let key = require_arg!(args, 1);
                if args.len() < 3 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'lpush' command",
                    );
                }
                let mut new_len = 0;
                for arg in &args[2..] {
                    let val = Value::Text(String::from_utf8_lossy(arg).to_string());
                    match self.kv.lpush(key, val) {
                        Ok(len) => new_len = len,
                        Err(e) => return encode_wrongtype(&e),
                    }
                }
                encoder::encode_integer(new_len as i64)
            }
            "RPUSH" => {
                let key = require_arg!(args, 1);
                if args.len() < 3 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'rpush' command",
                    );
                }
                let mut new_len = 0;
                for arg in &args[2..] {
                    let val = Value::Text(String::from_utf8_lossy(arg).to_string());
                    match self.kv.rpush(key, val) {
                        Ok(len) => new_len = len,
                        Err(e) => return encode_wrongtype(&e),
                    }
                }
                encoder::encode_integer(new_len as i64)
            }
            "LPOP" => {
                let key = require_arg!(args, 1);
                match self.kv.lpop(key) {
                    Ok(Some(v)) => encode_kv_value(&v),
                    Ok(None) => encoder::encode_null_bulk(),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "RPOP" => {
                let key = require_arg!(args, 1);
                match self.kv.rpop(key) {
                    Ok(Some(v)) => encode_kv_value(&v),
                    Ok(None) => encoder::encode_null_bulk(),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "LRANGE" => {
                let key = require_arg!(args, 1);
                let start = require_i64!(args, 2);
                let stop = require_i64!(args, 3);
                match self.kv.lrange(key, start, stop) {
                    Ok(vals) => {
                        let mut out = encoder::encode_array_header(vals.len());
                        for v in &vals {
                            out.extend(encode_kv_value(v));
                        }
                        out
                    }
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "LLEN" => {
                let key = require_arg!(args, 1);
                match self.kv.llen(key) {
                    Ok(n) => encoder::encode_integer(n as i64),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "LINDEX" => {
                let key = require_arg!(args, 1);
                let index = require_i64!(args, 2);
                match self.kv.lindex(key, index) {
                    Ok(Some(v)) => encode_kv_value(&v),
                    Ok(None) => encoder::encode_null_bulk(),
                    Err(e) => encode_wrongtype(&e),
                }
            }

            // ================================================================
            // Hash commands
            // ================================================================
            "HSET" => {
                let key = require_arg!(args, 1);
                if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'hset' command",
                    );
                }
                let mut new_count = 0i64;
                let mut i = 2;
                while i + 1 < args.len() {
                    let field = std::str::from_utf8(&args[i]).unwrap_or("");
                    let val = Value::Text(String::from_utf8_lossy(&args[i + 1]).to_string());
                    match self.kv.hset(key, field, val) {
                        Ok(is_new) => {
                            if is_new {
                                new_count += 1;
                            }
                        }
                        Err(e) => return encode_wrongtype(&e),
                    }
                    i += 2;
                }
                encoder::encode_integer(new_count)
            }
            "HGET" => {
                let key = require_arg!(args, 1);
                let field = require_arg!(args, 2);
                match self.kv.hget(key, field) {
                    Ok(Some(v)) => encode_kv_value(&v),
                    Ok(None) => encoder::encode_null_bulk(),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "HDEL" => {
                let key = require_arg!(args, 1);
                if args.len() < 3 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'hdel' command",
                    );
                }
                let mut count = 0i64;
                for arg in &args[2..] {
                    let field = std::str::from_utf8(arg).unwrap_or("");
                    match self.kv.hdel(key, field) {
                        Ok(removed) => {
                            if removed {
                                count += 1;
                            }
                        }
                        Err(e) => return encode_wrongtype(&e),
                    }
                }
                encoder::encode_integer(count)
            }
            "HGETALL" => {
                let key = require_arg!(args, 1);
                match self.kv.hgetall(key) {
                    Ok(pairs) => {
                        let mut out = encoder::encode_array_header(pairs.len() * 2);
                        for (field, val) in &pairs {
                            out.extend(encoder::encode_bulk_string(field.as_bytes()));
                            out.extend(encode_kv_value(val));
                        }
                        out
                    }
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "HLEN" => {
                let key = require_arg!(args, 1);
                match self.kv.hlen(key) {
                    Ok(n) => encoder::encode_integer(n as i64),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "HEXISTS" => {
                let key = require_arg!(args, 1);
                let field = require_arg!(args, 2);
                match self.kv.hexists(key, field) {
                    Ok(exists) => encoder::encode_integer(if exists { 1 } else { 0 }),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "HKEYS" => {
                let key = require_arg!(args, 1);
                match self.kv.hkeys(key) {
                    Ok(keys) => {
                        let mut out = encoder::encode_array_header(keys.len());
                        for k in &keys {
                            out.extend(encoder::encode_bulk_string(k.as_bytes()));
                        }
                        out
                    }
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "HVALS" => {
                let key = require_arg!(args, 1);
                match self.kv.hvals(key) {
                    Ok(vals) => {
                        let mut out = encoder::encode_array_header(vals.len());
                        for v in &vals {
                            out.extend(encode_kv_value(v));
                        }
                        out
                    }
                    Err(e) => encode_wrongtype(&e),
                }
            }

            // ================================================================
            // Set commands
            // ================================================================
            "SADD" => {
                let key = require_arg!(args, 1);
                if args.len() < 3 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'sadd' command",
                    );
                }
                let mut count = 0i64;
                for arg in &args[2..] {
                    let member = std::str::from_utf8(arg).unwrap_or("");
                    match self.kv.sadd(key, member) {
                        Ok(is_new) => {
                            if is_new {
                                count += 1;
                            }
                        }
                        Err(e) => return encode_wrongtype(&e),
                    }
                }
                encoder::encode_integer(count)
            }
            "SREM" => {
                let key = require_arg!(args, 1);
                if args.len() < 3 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'srem' command",
                    );
                }
                let mut count = 0i64;
                for arg in &args[2..] {
                    let member = std::str::from_utf8(arg).unwrap_or("");
                    match self.kv.srem(key, member) {
                        Ok(removed) => {
                            if removed {
                                count += 1;
                            }
                        }
                        Err(e) => return encode_wrongtype(&e),
                    }
                }
                encoder::encode_integer(count)
            }
            "SMEMBERS" => {
                let key = require_arg!(args, 1);
                match self.kv.smembers(key) {
                    Ok(members) => {
                        let mut out = encoder::encode_array_header(members.len());
                        for m in &members {
                            out.extend(encoder::encode_bulk_string(m.as_bytes()));
                        }
                        out
                    }
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "SISMEMBER" => {
                let key = require_arg!(args, 1);
                let member = require_arg!(args, 2);
                match self.kv.sismember(key, member) {
                    Ok(is_member) => encoder::encode_integer(if is_member { 1 } else { 0 }),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "SCARD" => {
                let key = require_arg!(args, 1);
                match self.kv.scard(key) {
                    Ok(n) => encoder::encode_integer(n as i64),
                    Err(e) => encode_wrongtype(&e),
                }
            }

            // ================================================================
            // Sorted set commands
            // ================================================================
            "ZADD" => {
                let key = require_arg!(args, 1);
                if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'zadd' command",
                    );
                }
                let mut count = 0i64;
                let mut i = 2;
                while i + 1 < args.len() {
                    let score_str = std::str::from_utf8(&args[i]).unwrap_or("0");
                    let score: f64 = match score_str.parse() {
                        Ok(s) => s,
                        Err(_) => return encoder::encode_error("ERR value is not a valid float"),
                    };
                    let member = std::str::from_utf8(&args[i + 1]).unwrap_or("");
                    match self.kv.col_zadd(key, member, score) {
                        Ok(is_new) => {
                            if is_new {
                                count += 1;
                            }
                        }
                        Err(e) => return encode_wrongtype(&e),
                    }
                    i += 2;
                }
                encoder::encode_integer(count)
            }
            "ZREM" => {
                let key = require_arg!(args, 1);
                if args.len() < 3 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'zrem' command",
                    );
                }
                let mut count = 0i64;
                for arg in &args[2..] {
                    let member = std::str::from_utf8(arg).unwrap_or("");
                    match self.kv.col_zrem(key, member) {
                        Ok(removed) => {
                            if removed {
                                count += 1;
                            }
                        }
                        Err(e) => return encode_wrongtype(&e),
                    }
                }
                encoder::encode_integer(count)
            }
            "ZRANGE" => {
                let key = require_arg!(args, 1);
                let start = require_i64!(args, 2) as usize;
                let stop = require_i64!(args, 3) as usize;
                // Check for WITHSCORES flag.
                let with_scores = args.len() > 4
                    && String::from_utf8_lossy(&args[4])
                        .to_uppercase()
                        .as_str()
                        == "WITHSCORES";
                match self.kv.col_zrange(key, start, stop) {
                    Ok(entries) => {
                        if with_scores {
                            let mut out = encoder::encode_array_header(entries.len() * 2);
                            for e in &entries {
                                out.extend(encoder::encode_bulk_string(e.member.as_bytes()));
                                out.extend(encoder::encode_bulk_string(
                                    e.score.to_string().as_bytes(),
                                ));
                            }
                            out
                        } else {
                            let mut out = encoder::encode_array_header(entries.len());
                            for e in &entries {
                                out.extend(encoder::encode_bulk_string(e.member.as_bytes()));
                            }
                            out
                        }
                    }
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "ZRANGEBYSCORE" => {
                let key = require_arg!(args, 1);
                let min_str = require_arg!(args, 2);
                let max_str = require_arg!(args, 3);
                let min: f64 = if min_str == "-inf" {
                    f64::NEG_INFINITY
                } else {
                    min_str.parse().unwrap_or(f64::NEG_INFINITY)
                };
                let max: f64 = if max_str == "+inf" || max_str == "inf" {
                    f64::INFINITY
                } else {
                    max_str.parse().unwrap_or(f64::INFINITY)
                };
                match self.kv.col_zrangebyscore(key, min, max) {
                    Ok(entries) => {
                        let mut out = encoder::encode_array_header(entries.len());
                        for e in &entries {
                            out.extend(encoder::encode_bulk_string(e.member.as_bytes()));
                        }
                        out
                    }
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "ZCARD" => {
                let key = require_arg!(args, 1);
                match self.kv.col_zcard(key) {
                    Ok(n) => encoder::encode_integer(n as i64),
                    Err(e) => encode_wrongtype(&e),
                }
            }

            // ================================================================
            // HyperLogLog
            // ================================================================
            "PFADD" => {
                let key = require_arg!(args, 1);
                if args.len() < 3 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'pfadd' command",
                    );
                }
                let mut changed = false;
                for arg in &args[2..] {
                    let element = std::str::from_utf8(arg).unwrap_or("");
                    match self.kv.col_pfadd(key, element) {
                        Ok(c) => {
                            if c {
                                changed = true;
                            }
                        }
                        Err(e) => return encode_wrongtype(&e),
                    }
                }
                encoder::encode_integer(if changed { 1 } else { 0 })
            }
            "PFCOUNT" => {
                let key = require_arg!(args, 1);
                match self.kv.col_pfcount(key) {
                    Ok(n) => encoder::encode_integer(n as i64),
                    Err(e) => encode_wrongtype(&e),
                }
            }

            // ================================================================
            // Info / Meta
            // ================================================================
            "INFO" => {
                let info = format!(
                    "# Server\r\nnucleus_version:{}\r\nredis_version:7.0.0\r\n\
                     # Keyspace\r\ndb0:keys={}\r\n",
                    env!("CARGO_PKG_VERSION"),
                    self.kv.dbsize(),
                );
                encoder::encode_bulk_string(info.as_bytes())
            }
            "COMMAND" => {
                // Compatibility: clients issue COMMAND DOCS / COMMAND COUNT on connect.
                if args.len() > 1 {
                    let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
                    if sub == "COUNT" {
                        return encoder::encode_integer(0);
                    }
                }
                encoder::encode_simple_string("OK")
            }
            "CONFIG" => {
                // Return empty array for CONFIG GET requests.
                encoder::encode_array_header(0).to_vec()
            }
            "ECHO" => {
                if args.len() < 2 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'echo' command",
                    );
                }
                encoder::encode_bulk_string(&args[1])
            }
            "TYPE" => {
                let key = require_arg!(args, 1);
                // Check string KV store first.
                if self.kv.exists(key) {
                    return encoder::encode_simple_string("string");
                }
                // Check collection types.
                match self.kv.collections().key_type(key) {
                    Some(t) => encoder::encode_simple_string(t),
                    None => encoder::encode_simple_string("none"),
                }
            }

            // ================================================================
            // Pub/Sub commands (non-subscriber side)
            // ================================================================
            "PUBLISH" => {
                let channel = require_arg!(args, 1);
                let message = require_arg!(args, 2);
                let count = self.pubsub.publish(channel, message);
                encoder::encode_integer(count as i64)
            }
            // SUBSCRIBE/PSUBSCRIBE in non-pubsub mode are handled by the
            // server loop which calls handle_pubsub_command() directly.
            // If they somehow reach here, redirect.
            "SUBSCRIBE" | "PSUBSCRIBE" | "UNSUBSCRIBE" | "PUNSUBSCRIBE" => {
                encoder::encode_error(
                    "ERR pub/sub commands must be handled by the server connection loop"
                )
            }

            // ================================================================
            // Geo commands
            // ================================================================
            "GEOADD" => {
                // GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
                let key = require_arg!(args, 1);
                if args.len() < 5 || (args.len() - 2) % 3 != 0 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'geoadd' command",
                    );
                }
                let mut count = 0i64;
                let mut i = 2;
                while i + 2 < args.len() {
                    let lon: f64 = match String::from_utf8_lossy(&args[i]).parse() {
                        Ok(v) => v,
                        Err(_) => return encoder::encode_error("ERR value is not a valid float"),
                    };
                    let lat: f64 = match String::from_utf8_lossy(&args[i + 1]).parse() {
                        Ok(v) => v,
                        Err(_) => return encoder::encode_error("ERR value is not a valid float"),
                    };
                    let member = std::str::from_utf8(&args[i + 2]).unwrap_or("");
                    match self.kv.geoadd(key, lon, lat, member) {
                        Ok(is_new) => {
                            if is_new {
                                count += 1;
                            }
                        }
                        Err(e) => return encode_wrongtype(&e),
                    }
                    i += 3;
                }
                encoder::encode_integer(count)
            }
            "GEODIST" => {
                // GEODIST key member1 member2 [m|km|ft|mi]
                let key = require_arg!(args, 1);
                let member1 = require_arg!(args, 2);
                let member2 = require_arg!(args, 3);
                let unit = if args.len() > 4 {
                    std::str::from_utf8(&args[4]).unwrap_or("m")
                } else {
                    "m"
                };
                match self.kv.geodist(key, member1, member2, unit) {
                    Ok(Some(dist)) => encoder::encode_bulk_string(
                        format!("{:.4}", dist).as_bytes(),
                    ),
                    Ok(None) => encoder::encode_null_bulk(),
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "GEOPOS" => {
                // GEOPOS key member [member ...]
                let key = require_arg!(args, 1);
                if args.len() < 3 {
                    return encoder::encode_error(
                        "ERR wrong number of arguments for 'geopos' command",
                    );
                }
                let mut out = encoder::encode_array_header(args.len() - 2);
                for arg in &args[2..] {
                    let member = std::str::from_utf8(arg).unwrap_or("");
                    match self.kv.geopos(key, member) {
                        Ok(Some((lon, lat))) => {
                            out.extend(encoder::encode_array_header(2));
                            out.extend(encoder::encode_bulk_string(
                                format!("{:.6}", lon).as_bytes(),
                            ));
                            out.extend(encoder::encode_bulk_string(
                                format!("{:.6}", lat).as_bytes(),
                            ));
                        }
                        Ok(None) => {
                            out.extend(encoder::encode_null_bulk());
                        }
                        Err(e) => return encode_wrongtype(&e),
                    }
                }
                out
            }
            "GEORADIUS" => {
                // GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [COUNT count] [ASC|DESC]
                let key = require_arg!(args, 1);
                let lon: f64 = match String::from_utf8_lossy(require_arg_bytes!(args, 2)).parse() {
                    Ok(v) => v,
                    Err(_) => return encoder::encode_error("ERR value is not a valid float"),
                };
                let lat: f64 = match String::from_utf8_lossy(require_arg_bytes!(args, 3)).parse() {
                    Ok(v) => v,
                    Err(_) => return encoder::encode_error("ERR value is not a valid float"),
                };
                let radius: f64 = match String::from_utf8_lossy(require_arg_bytes!(args, 4)).parse() {
                    Ok(v) => v,
                    Err(_) => return encoder::encode_error("ERR value is not a valid float"),
                };
                let unit = require_arg!(args, 5);

                // Parse optional flags
                let mut with_dist = false;
                let mut with_coord = false;
                let mut count_limit: Option<usize> = None;
                let mut ascending = true;
                let mut i = 6;
                while i < args.len() {
                    let flag = String::from_utf8_lossy(&args[i]).to_uppercase();
                    match flag.as_str() {
                        "WITHDIST" => with_dist = true,
                        "WITHCOORD" => with_coord = true,
                        "ASC" => ascending = true,
                        "DESC" => ascending = false,
                        "COUNT" => {
                            i += 1;
                            if i < args.len() {
                                count_limit = String::from_utf8_lossy(&args[i]).parse().ok();
                            }
                        }
                        _ => {}
                    }
                    i += 1;
                }

                match self.kv.georadius(key, lon, lat, radius, unit) {
                    Ok(mut results) => {
                        if !ascending {
                            results.reverse();
                        }
                        if let Some(limit) = count_limit {
                            results.truncate(limit);
                        }

                        if with_dist || with_coord {
                            let mut out = encoder::encode_array_header(results.len());
                            for (member, dist) in &results {
                                let mut sub_count = 1; // member name
                                if with_dist { sub_count += 1; }
                                if with_coord { sub_count += 1; }
                                out.extend(encoder::encode_array_header(sub_count));
                                out.extend(encoder::encode_bulk_string(member.as_bytes()));
                                if with_dist {
                                    out.extend(encoder::encode_bulk_string(
                                        format!("{:.4}", dist).as_bytes(),
                                    ));
                                }
                                if with_coord {
                                    if let Ok(Some((mlon, mlat))) = self.kv.geopos(key, member) {
                                        out.extend(encoder::encode_array_header(2));
                                        out.extend(encoder::encode_bulk_string(
                                            format!("{:.6}", mlon).as_bytes(),
                                        ));
                                        out.extend(encoder::encode_bulk_string(
                                            format!("{:.6}", mlat).as_bytes(),
                                        ));
                                    } else {
                                        out.extend(encoder::encode_null_bulk());
                                    }
                                }
                            }
                            out
                        } else {
                            let mut out = encoder::encode_array_header(results.len());
                            for (member, _) in &results {
                                out.extend(encoder::encode_bulk_string(member.as_bytes()));
                            }
                            out
                        }
                    }
                    Err(e) => encode_wrongtype(&e),
                }
            }
            "GEORADIUSBYMEMBER" => {
                // GEORADIUSBYMEMBER key member radius m|km|ft|mi
                let key = require_arg!(args, 1);
                let member = require_arg!(args, 2);
                let radius: f64 = match String::from_utf8_lossy(require_arg_bytes!(args, 3)).parse() {
                    Ok(v) => v,
                    Err(_) => return encoder::encode_error("ERR value is not a valid float"),
                };
                let unit = require_arg!(args, 4);

                // Look up the member's position
                match self.kv.geopos(key, member) {
                    Ok(Some((lon, lat))) => {
                        match self.kv.georadius(key, lon, lat, radius, unit) {
                            Ok(results) => {
                                let mut out = encoder::encode_array_header(results.len());
                                for (m, _) in &results {
                                    out.extend(encoder::encode_bulk_string(m.as_bytes()));
                                }
                                out
                            }
                            Err(e) => encode_wrongtype(&e),
                        }
                    }
                    Ok(None) => encoder::encode_error("ERR could not decode requested zset member"),
                    Err(e) => encode_wrongtype(&e),
                }
            }

            // ================================================================
            // Unknown
            // ================================================================
            _ => encoder::encode_error(&format!("ERR unknown command '{cmd}'")),
        }
    }

    /// Execute all queued commands from a MULTI block atomically.
    fn exec_transaction(&mut self) -> Vec<u8> {
        let queue = match self.transaction_queue.take() {
            Some(q) => q,
            None => return encoder::encode_error("ERR EXEC without MULTI"),
        };

        // Check WATCH'd keys for modifications (optimistic locking).
        for (key, version_at_watch) in &self.watched_keys {
            let current_version = self.kv.key_version(key);
            if current_version != *version_at_watch {
                // A watched key was modified — abort the transaction.
                self.watched_keys.clear();
                return encoder::encode_null_bulk();
            }
        }
        self.watched_keys.clear();

        // Execute all queued commands and collect responses.
        let mut responses = Vec::with_capacity(queue.len());
        for cmd_args in queue {
            let response = self.handle_command(cmd_args);
            responses.push(response);
        }

        // Encode as a RESP array of responses.
        let mut result = encoder::encode_array_header(responses.len());
        for resp in responses {
            result.extend_from_slice(&resp);
        }
        result
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Encode a Nucleus `Value` for RESP, with Text as bulk string.
fn encode_kv_value(v: &Value) -> Vec<u8> {
    encoder::encode_value(v)
}

/// Encode a `WrongTypeError` as a RESP error.
fn encode_wrongtype(e: &crate::kv::collections::WrongTypeError) -> Vec<u8> {
    encoder::encode_error(&format!(
        "WRONGTYPE Operation against a key holding the wrong kind of value (expected {}, got {})",
        e.expected, e.actual
    ))
}

/// Encode a list of stream entries as a RESP array of [id, [field, value, ...]] pairs.
fn encode_stream_entries(entries: &[crate::kv::streams::StreamEntry]) -> Vec<u8> {
    let mut resp = format!("*{}\r\n", entries.len()).into_bytes();
    for entry in entries {
        // Each entry is: *2\r\n $id\r\n *N\r\n field val field val ...
        resp.extend_from_slice(b"*2\r\n");
        resp.extend_from_slice(&encoder::encode_bulk_string(
            entry.id.to_string().as_bytes(),
        ));
        let n_field_vals = entry.fields.len() * 2;
        resp.extend_from_slice(format!("*{n_field_vals}\r\n").as_bytes());
        for (field, value) in &entry.fields {
            resp.extend_from_slice(&encoder::encode_bulk_string(field.as_bytes()));
            resp.extend_from_slice(&encoder::encode_bulk_string(value.as_bytes()));
        }
    }
    resp
}

// ============================================================================
// Argument extraction macros
// ============================================================================

/// Extract a `&str` argument at the given position, returning an error if
/// missing or not valid UTF-8.
macro_rules! require_arg {
    ($args:expr, $idx:expr) => {
        match $args.get($idx) {
            Some(bytes) => match std::str::from_utf8(bytes) {
                Ok(s) => s,
                Err(_) => return encoder::encode_error("ERR invalid argument encoding"),
            },
            None => {
                return encoder::encode_error(&format!(
                    "ERR wrong number of arguments for '{}' command",
                    String::from_utf8_lossy(&$args[0]).to_lowercase()
                ))
            }
        }
    };
}
use require_arg;

/// Extract raw bytes at the given position.
macro_rules! require_arg_bytes {
    ($args:expr, $idx:expr) => {
        match $args.get($idx) {
            Some(bytes) => bytes.as_slice(),
            None => {
                return encoder::encode_error(&format!(
                    "ERR wrong number of arguments for '{}' command",
                    String::from_utf8_lossy(&$args[0]).to_lowercase()
                ))
            }
        }
    };
}
use require_arg_bytes;

/// Parse an i64 from the argument at the given position.
macro_rules! require_i64 {
    ($args:expr, $idx:expr) => {{
        let s = require_arg!($args, $idx);
        match s.parse::<i64>() {
            Ok(n) => n,
            Err(_) => return encoder::encode_error("ERR value is not an integer or out of range"),
        }
    }};
}
use require_i64;

/// Parse a u64 from the argument at the given position.
macro_rules! require_u64 {
    ($args:expr, $idx:expr) => {{
        let s = require_arg!($args, $idx);
        match s.parse::<u64>() {
            Ok(n) => n,
            Err(_) => return encoder::encode_error("ERR value is not an integer or out of range"),
        }
    }};
}
use require_u64;

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build args from string slices.
    fn args(strs: &[&str]) -> Vec<Vec<u8>> {
        strs.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    /// Decode a RESP simple string from the response bytes.
    fn decode_simple(data: &[u8]) -> String {
        let s = String::from_utf8_lossy(data);
        assert!(s.starts_with('+'), "expected simple string, got: {s}");
        s[1..].trim_end_matches("\r\n").to_string()
    }

    /// Decode a RESP integer from the response bytes.
    fn decode_int(data: &[u8]) -> i64 {
        let s = String::from_utf8_lossy(data);
        assert!(s.starts_with(':'), "expected integer, got: {s}");
        s[1..].trim_end_matches("\r\n").parse().unwrap()
    }

    /// Decode a RESP bulk string from the response bytes. Returns None for null.
    fn decode_bulk(data: &[u8]) -> Option<String> {
        let s = String::from_utf8_lossy(data);
        if s.starts_with("$-1\r\n") {
            return None;
        }
        assert!(s.starts_with('$'), "expected bulk string, got: {s}");
        let after_dollar = &s[1..];
        let crlf = after_dollar.find("\r\n").unwrap();
        let len: usize = after_dollar[..crlf].parse().unwrap();
        let body_start = crlf + 2;
        Some(after_dollar[body_start..body_start + len].to_string())
    }

    /// Check if response is a RESP error.
    fn is_error(data: &[u8]) -> bool {
        data.first() == Some(&b'-')
    }

    fn new_handler() -> RespHandler {
        let kv = Arc::new(KvStore::new());
        let pubsub = Arc::new(PubSubRegistry::new());
        RespHandler::new(kv, None, pubsub)
    }

    fn new_handler_with_password(pw: &str) -> RespHandler {
        let kv = Arc::new(KvStore::new());
        let pubsub = Arc::new(PubSubRegistry::new());
        RespHandler::new(kv, Some(pw.to_string()), pubsub)
    }

    #[test]
    fn test_ping() {
        let mut h = new_handler();
        let resp = h.handle_command(args(&["PING"]));
        assert_eq!(decode_simple(&resp), "PONG");

        // PING with argument echoes it back.
        let resp = h.handle_command(args(&["PING", "hello"]));
        assert_eq!(decode_bulk(&resp), Some("hello".to_string()));
    }

    #[test]
    fn test_set_get() {
        let mut h = new_handler();
        let resp = h.handle_command(args(&["SET", "foo", "bar"]));
        assert_eq!(decode_simple(&resp), "OK");

        let resp = h.handle_command(args(&["GET", "foo"]));
        assert_eq!(decode_bulk(&resp), Some("bar".to_string()));
    }

    #[test]
    fn test_del() {
        let mut h = new_handler();
        h.handle_command(args(&["SET", "k1", "v1"]));
        h.handle_command(args(&["SET", "k2", "v2"]));

        let resp = h.handle_command(args(&["DEL", "k1", "k2", "k3"]));
        assert_eq!(decode_int(&resp), 2);

        let resp = h.handle_command(args(&["GET", "k1"]));
        assert_eq!(decode_bulk(&resp), None);
    }

    #[test]
    fn test_incr() {
        let mut h = new_handler();
        let resp = h.handle_command(args(&["INCR", "counter"]));
        assert_eq!(decode_int(&resp), 1);
        let resp = h.handle_command(args(&["INCR", "counter"]));
        assert_eq!(decode_int(&resp), 2);
    }

    #[test]
    fn test_exists() {
        let mut h = new_handler();
        let resp = h.handle_command(args(&["EXISTS", "missing"]));
        assert_eq!(decode_int(&resp), 0);
        h.handle_command(args(&["SET", "present", "yes"]));
        let resp = h.handle_command(args(&["EXISTS", "present"]));
        assert_eq!(decode_int(&resp), 1);
    }

    #[test]
    fn test_expire_ttl() {
        let mut h = new_handler();
        h.handle_command(args(&["SET", "tmp", "val"]));
        let resp = h.handle_command(args(&["EXPIRE", "tmp", "100"]));
        assert_eq!(decode_int(&resp), 1);
        let resp = h.handle_command(args(&["TTL", "tmp"]));
        let ttl = decode_int(&resp);
        assert!(ttl > 0 && ttl <= 100, "TTL should be > 0, got {ttl}");
    }

    #[test]
    fn test_setnx() {
        let mut h = new_handler();
        let resp = h.handle_command(args(&["SETNX", "lock", "1"]));
        assert_eq!(decode_int(&resp), 1);
        let resp = h.handle_command(args(&["SETNX", "lock", "2"]));
        assert_eq!(decode_int(&resp), 0);
    }

    #[test]
    fn test_dbsize_flushdb() {
        let mut h = new_handler();
        h.handle_command(args(&["SET", "a", "1"]));
        h.handle_command(args(&["SET", "b", "2"]));
        h.handle_command(args(&["SET", "c", "3"]));
        let resp = h.handle_command(args(&["DBSIZE"]));
        assert_eq!(decode_int(&resp), 3);

        let resp = h.handle_command(args(&["FLUSHDB"]));
        assert_eq!(decode_simple(&resp), "OK");
        let resp = h.handle_command(args(&["DBSIZE"]));
        assert_eq!(decode_int(&resp), 0);
    }

    #[test]
    fn test_list_ops() {
        let mut h = new_handler();
        // LPUSH creates a list: [c, b, a]
        h.handle_command(args(&["RPUSH", "mylist", "a"]));
        h.handle_command(args(&["RPUSH", "mylist", "b"]));
        h.handle_command(args(&["LPUSH", "mylist", "c"]));

        let resp = h.handle_command(args(&["LLEN", "mylist"]));
        assert_eq!(decode_int(&resp), 3);

        // LRANGE 0 -1 returns all elements
        let resp = h.handle_command(args(&["LRANGE", "mylist", "0", "-1"]));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.starts_with("*3\r\n"), "expected array of 3, got: {s}");

        // LPOP returns the first element.
        let resp = h.handle_command(args(&["LPOP", "mylist"]));
        assert_eq!(decode_bulk(&resp), Some("c".to_string()));
    }

    #[test]
    fn test_hash_ops() {
        let mut h = new_handler();
        let resp = h.handle_command(args(&["HSET", "h", "f1", "v1", "f2", "v2"]));
        assert_eq!(decode_int(&resp), 2);

        let resp = h.handle_command(args(&["HGET", "h", "f1"]));
        assert_eq!(decode_bulk(&resp), Some("v1".to_string()));

        let resp = h.handle_command(args(&["HGETALL", "h"]));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.starts_with("*4\r\n"), "expected array of 4, got: {s}");

        let resp = h.handle_command(args(&["HDEL", "h", "f1"]));
        assert_eq!(decode_int(&resp), 1);

        let resp = h.handle_command(args(&["HLEN", "h"]));
        assert_eq!(decode_int(&resp), 1);
    }

    #[test]
    fn test_set_ops() {
        let mut h = new_handler();
        h.handle_command(args(&["SADD", "s", "a", "b", "c"]));
        let resp = h.handle_command(args(&["SCARD", "s"]));
        assert_eq!(decode_int(&resp), 3);

        let resp = h.handle_command(args(&["SISMEMBER", "s", "b"]));
        assert_eq!(decode_int(&resp), 1);

        let resp = h.handle_command(args(&["SMEMBERS", "s"]));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.starts_with("*3\r\n"), "expected array of 3, got: {s}");

        let resp = h.handle_command(args(&["SREM", "s", "a"]));
        assert_eq!(decode_int(&resp), 1);

        let resp = h.handle_command(args(&["SCARD", "s"]));
        assert_eq!(decode_int(&resp), 2);
    }

    #[test]
    fn test_auth() {
        let mut h = new_handler_with_password("secret");

        // Before AUTH, GET should fail.
        let resp = h.handle_command(args(&["GET", "key"]));
        assert!(is_error(&resp));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.contains("NOAUTH"), "expected NOAUTH, got: {s}");

        // PING is always allowed.
        let resp = h.handle_command(args(&["PING"]));
        assert_eq!(decode_simple(&resp), "PONG");

        // Wrong password.
        let resp = h.handle_command(args(&["AUTH", "wrong"]));
        assert!(is_error(&resp));

        // Correct password.
        let resp = h.handle_command(args(&["AUTH", "secret"]));
        assert_eq!(decode_simple(&resp), "OK");

        // Now GET works.
        let resp = h.handle_command(args(&["GET", "key"]));
        assert_eq!(decode_bulk(&resp), None); // key doesn't exist but no error
    }

    #[test]
    fn test_unknown_command() {
        let mut h = new_handler();
        let resp = h.handle_command(args(&["FOOBAR"]));
        assert!(is_error(&resp));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.contains("unknown command"), "expected unknown command error, got: {s}");
    }

    // ====================================================================
    // MULTI / EXEC / DISCARD / WATCH tests
    // ====================================================================

    #[test]
    fn test_multi_exec_basic() {
        let mut h = new_handler();
        // MULTI starts a transaction
        let resp = h.handle_command(args(&["MULTI"]));
        assert_eq!(decode_simple(&resp), "OK");

        // Commands are queued
        let resp = h.handle_command(args(&["SET", "a", "1"]));
        assert_eq!(decode_simple(&resp), "QUEUED");
        let resp = h.handle_command(args(&["SET", "b", "2"]));
        assert_eq!(decode_simple(&resp), "QUEUED");
        let resp = h.handle_command(args(&["GET", "a"]));
        assert_eq!(decode_simple(&resp), "QUEUED");

        // EXEC runs all commands and returns array of results
        let resp = h.handle_command(args(&["EXEC"]));
        let s = String::from_utf8_lossy(&resp);
        // Should be a RESP array with 3 elements
        assert!(s.starts_with("*3\r\n"), "expected array of 3, got: {s}");

        // Verify the SET commands took effect
        let resp = h.handle_command(args(&["GET", "a"]));
        assert_eq!(decode_bulk(&resp), Some("1".to_string()));
        let resp = h.handle_command(args(&["GET", "b"]));
        assert_eq!(decode_bulk(&resp), Some("2".to_string()));
    }

    #[test]
    fn test_multi_discard() {
        let mut h = new_handler();
        h.handle_command(args(&["SET", "x", "original"]));

        let resp = h.handle_command(args(&["MULTI"]));
        assert_eq!(decode_simple(&resp), "OK");

        h.handle_command(args(&["SET", "x", "changed"]));

        // DISCARD cancels the transaction
        let resp = h.handle_command(args(&["DISCARD"]));
        assert_eq!(decode_simple(&resp), "OK");

        // Value should be unchanged
        let resp = h.handle_command(args(&["GET", "x"]));
        assert_eq!(decode_bulk(&resp), Some("original".to_string()));
    }

    #[test]
    fn test_multi_nested_error() {
        let mut h = new_handler();
        h.handle_command(args(&["MULTI"]));
        let resp = h.handle_command(args(&["MULTI"]));
        assert!(is_error(&resp));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.contains("nested"), "expected nested error, got: {s}");
    }

    #[test]
    fn test_exec_without_multi() {
        let mut h = new_handler();
        let resp = h.handle_command(args(&["EXEC"]));
        assert!(is_error(&resp));
    }

    #[test]
    fn test_discard_without_multi() {
        let mut h = new_handler();
        let resp = h.handle_command(args(&["DISCARD"]));
        assert!(is_error(&resp));
    }

    #[test]
    fn test_watch_exec_no_conflict() {
        let mut h = new_handler();
        h.handle_command(args(&["SET", "watched_key", "v1"]));

        // WATCH the key
        let resp = h.handle_command(args(&["WATCH", "watched_key"]));
        assert_eq!(decode_simple(&resp), "OK");

        // Start transaction (no intervening writes)
        h.handle_command(args(&["MULTI"]));
        h.handle_command(args(&["SET", "watched_key", "v2"]));
        let resp = h.handle_command(args(&["EXEC"]));
        // Should succeed — no conflict
        let s = String::from_utf8_lossy(&resp);
        assert!(s.starts_with("*1\r\n"), "expected array, got: {s}");

        let resp = h.handle_command(args(&["GET", "watched_key"]));
        assert_eq!(decode_bulk(&resp), Some("v2".to_string()));
    }

    #[test]
    fn test_watch_exec_with_conflict() {
        let mut h = new_handler();
        h.handle_command(args(&["SET", "wk", "v1"]));

        // WATCH
        let resp = h.handle_command(args(&["WATCH", "wk"]));
        assert_eq!(decode_simple(&resp), "OK");

        // Simulate a conflicting write (same handler, but version bumps)
        h.handle_command(args(&["SET", "wk", "v_conflict"]));

        // Now try to execute a transaction
        h.handle_command(args(&["MULTI"]));
        h.handle_command(args(&["SET", "wk", "v2"]));
        let resp = h.handle_command(args(&["EXEC"]));
        // Should return null bulk (transaction aborted)
        let s = String::from_utf8_lossy(&resp);
        assert!(s.starts_with("$-1\r\n"), "expected null bulk for aborted tx, got: {s}");

        // Value should be the conflicting write, not v2
        let resp = h.handle_command(args(&["GET", "wk"]));
        assert_eq!(decode_bulk(&resp), Some("v_conflict".to_string()));
    }

    #[test]
    fn test_unwatch() {
        let mut h = new_handler();
        h.handle_command(args(&["SET", "uk", "v1"]));

        h.handle_command(args(&["WATCH", "uk"]));
        let resp = h.handle_command(args(&["UNWATCH"]));
        assert_eq!(decode_simple(&resp), "OK");

        // Write should not cause EXEC to fail since we unwatched
        h.handle_command(args(&["SET", "uk", "v_changed"]));
        h.handle_command(args(&["MULTI"]));
        h.handle_command(args(&["SET", "uk", "v2"]));
        let resp = h.handle_command(args(&["EXEC"]));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.starts_with("*1\r\n"), "expected success after UNWATCH, got: {s}");
    }

    #[test]
    fn test_watch_inside_multi_error() {
        let mut h = new_handler();
        h.handle_command(args(&["MULTI"]));
        let resp = h.handle_command(args(&["WATCH", "key"]));
        assert!(is_error(&resp));
    }

    // ====================================================================
    // Bitmap tests
    // ====================================================================

    #[test]
    fn test_setbit_getbit() {
        let mut h = new_handler();
        // SETBIT on non-existent key
        let resp = h.handle_command(args(&["SETBIT", "bm", "7", "1"]));
        assert_eq!(decode_int(&resp), 0); // old bit was 0

        let resp = h.handle_command(args(&["GETBIT", "bm", "7"]));
        assert_eq!(decode_int(&resp), 1);

        let resp = h.handle_command(args(&["GETBIT", "bm", "0"]));
        assert_eq!(decode_int(&resp), 0);

        // Set bit 7 again — old was 1
        let resp = h.handle_command(args(&["SETBIT", "bm", "7", "1"]));
        assert_eq!(decode_int(&resp), 1);

        // Clear bit 7
        let resp = h.handle_command(args(&["SETBIT", "bm", "7", "0"]));
        assert_eq!(decode_int(&resp), 1); // old was 1

        let resp = h.handle_command(args(&["GETBIT", "bm", "7"]));
        assert_eq!(decode_int(&resp), 0);
    }

    #[test]
    fn test_bitcount() {
        let mut h = new_handler();
        // Set bits 0, 1, 7 (byte 0 = 0b11000001 = 0xC1)
        h.handle_command(args(&["SETBIT", "bc", "0", "1"]));
        h.handle_command(args(&["SETBIT", "bc", "1", "1"]));
        h.handle_command(args(&["SETBIT", "bc", "7", "1"]));
        // Set bit 8 (byte 1, bit 0 in byte 1)
        h.handle_command(args(&["SETBIT", "bc", "8", "1"]));

        // Total bits set = 4
        let resp = h.handle_command(args(&["BITCOUNT", "bc"]));
        assert_eq!(decode_int(&resp), 4);

        // Count only byte 0
        let resp = h.handle_command(args(&["BITCOUNT", "bc", "0", "0"]));
        assert_eq!(decode_int(&resp), 3);

        // Count only byte 1
        let resp = h.handle_command(args(&["BITCOUNT", "bc", "1", "1"]));
        assert_eq!(decode_int(&resp), 1);
    }

    #[test]
    fn test_bitop_and_or_xor() {
        let mut h = new_handler();
        // key1: byte = 0xFF (all bits set)
        for i in 0..8 {
            h.handle_command(args(&["SETBIT", "k1", &i.to_string(), "1"]));
        }
        // key2: byte = 0x0F (lower 4 bits set)
        for i in 4..8 {
            h.handle_command(args(&["SETBIT", "k2", &i.to_string(), "1"]));
        }

        // AND: 0xFF & 0x0F = 0x0F (4 bits)
        h.handle_command(args(&["BITOP", "AND", "dest_and", "k1", "k2"]));
        let resp = h.handle_command(args(&["BITCOUNT", "dest_and"]));
        assert_eq!(decode_int(&resp), 4);

        // OR: 0xFF | 0x0F = 0xFF (8 bits)
        h.handle_command(args(&["BITOP", "OR", "dest_or", "k1", "k2"]));
        let resp = h.handle_command(args(&["BITCOUNT", "dest_or"]));
        assert_eq!(decode_int(&resp), 8);

        // XOR: 0xFF ^ 0x0F = 0xF0 (4 bits)
        h.handle_command(args(&["BITOP", "XOR", "dest_xor", "k1", "k2"]));
        let resp = h.handle_command(args(&["BITCOUNT", "dest_xor"]));
        assert_eq!(decode_int(&resp), 4);
    }

    #[test]
    fn test_bitop_not() {
        let mut h = new_handler();
        // key: 0x00
        h.handle_command(args(&["SETBIT", "src", "7", "0"])); // ensure key exists (1 byte)
        h.handle_command(args(&["BITOP", "NOT", "dest_not", "src"]));
        let resp = h.handle_command(args(&["BITCOUNT", "dest_not"]));
        assert_eq!(decode_int(&resp), 8); // NOT 0x00 = 0xFF
    }

    #[test]
    fn test_bitpos() {
        let mut h = new_handler();
        // Set bit 10 (byte 1, bit 2 in that byte)
        h.handle_command(args(&["SETBIT", "bp", "10", "1"]));

        // Find first 1-bit
        let resp = h.handle_command(args(&["BITPOS", "bp", "1"]));
        assert_eq!(decode_int(&resp), 10);

        // Find first 0-bit (should be 0)
        let resp = h.handle_command(args(&["BITPOS", "bp", "0"]));
        assert_eq!(decode_int(&resp), 0);
    }

    // ====================================================================
    // Stream command tests
    // ====================================================================

    #[test]
    fn test_xadd_xlen() {
        let mut h = new_handler();
        let resp = h.handle_command(args(&["XADD", "mystream", "1-0", "name", "alice"]));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.contains("1-0"), "XADD should return the ID: {s}");

        h.handle_command(args(&["XADD", "mystream", "2-0", "name", "bob"]));

        let resp = h.handle_command(args(&["XLEN", "mystream"]));
        assert_eq!(decode_int(&resp), 2);
    }

    #[test]
    fn test_xrange() {
        let mut h = new_handler();
        h.handle_command(args(&["XADD", "s", "1-0", "k", "v1"]));
        h.handle_command(args(&["XADD", "s", "2-0", "k", "v2"]));
        h.handle_command(args(&["XADD", "s", "3-0", "k", "v3"]));

        let resp = h.handle_command(args(&["XRANGE", "s", "-", "+"]));
        let s = String::from_utf8_lossy(&resp);
        // Should contain all 3 entries
        assert!(s.contains("1-0"), "should contain 1-0: {s}");
        assert!(s.contains("3-0"), "should contain 3-0: {s}");
    }

    #[test]
    fn test_xdel_xtrim() {
        let mut h = new_handler();
        h.handle_command(args(&["XADD", "s", "1-0", "k", "v1"]));
        h.handle_command(args(&["XADD", "s", "2-0", "k", "v2"]));
        h.handle_command(args(&["XADD", "s", "3-0", "k", "v3"]));
        h.handle_command(args(&["XADD", "s", "4-0", "k", "v4"]));

        // Delete one entry
        let resp = h.handle_command(args(&["XDEL", "s", "2-0"]));
        assert_eq!(decode_int(&resp), 1);

        let resp = h.handle_command(args(&["XLEN", "s"]));
        assert_eq!(decode_int(&resp), 3);

        // Trim to 2 entries
        let resp = h.handle_command(args(&["XTRIM", "s", "MAXLEN", "2"]));
        assert_eq!(decode_int(&resp), 1);

        let resp = h.handle_command(args(&["XLEN", "s"]));
        assert_eq!(decode_int(&resp), 2);
    }

    #[test]
    fn test_xgroup_xreadgroup_xack() {
        let mut h = new_handler();
        h.handle_command(args(&["XADD", "s", "1-0", "msg", "hello"]));
        h.handle_command(args(&["XADD", "s", "2-0", "msg", "world"]));

        // Create group
        let resp = h.handle_command(args(&["XGROUP", "CREATE", "s", "g1", "0"]));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.contains("OK"), "XGROUP CREATE should return OK: {s}");

        // Read for consumer alice
        let resp = h.handle_command(args(&[
            "XREADGROUP", "GROUP", "g1", "alice", "STREAMS", "s", ">",
        ]));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.contains("hello"), "should contain hello: {s}");
        assert!(s.contains("world"), "should contain world: {s}");

        // Ack first entry
        let resp = h.handle_command(args(&["XACK", "s", "g1", "1-0"]));
        assert_eq!(decode_int(&resp), 1);

        // Destroy group
        let resp = h.handle_command(args(&["XGROUP", "DESTROY", "s", "g1"]));
        assert_eq!(decode_int(&resp), 1);
    }

    // ====================================================================
    // Geo command tests
    // ====================================================================

    #[test]
    fn test_geoadd_geopos() {
        let mut h = new_handler();
        // Add two locations
        let resp = h.handle_command(args(&[
            "GEOADD", "places", "13.361389", "38.115556", "Palermo",
        ]));
        assert_eq!(decode_int(&resp), 1);

        let resp = h.handle_command(args(&[
            "GEOADD", "places", "15.087269", "37.502669", "Catania",
        ]));
        assert_eq!(decode_int(&resp), 1);

        // GEOPOS should return coordinates
        let resp = h.handle_command(args(&["GEOPOS", "places", "Palermo"]));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.contains("13.361389"), "should contain lon: {s}");
        assert!(s.contains("38.115556"), "should contain lat: {s}");

        // GEOPOS for missing member returns null
        let resp = h.handle_command(args(&["GEOPOS", "places", "NonExistent"]));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.contains("$-1"), "should contain null bulk: {s}");
    }

    #[test]
    fn test_geodist() {
        let mut h = new_handler();
        h.handle_command(args(&[
            "GEOADD", "geo", "13.361389", "38.115556", "Palermo",
        ]));
        h.handle_command(args(&[
            "GEOADD", "geo", "15.087269", "37.502669", "Catania",
        ]));

        // Distance in km
        let resp = h.handle_command(args(&["GEODIST", "geo", "Palermo", "Catania", "km"]));
        let dist_str = decode_bulk(&resp).unwrap();
        let dist: f64 = dist_str.parse().unwrap();
        // Palermo to Catania is approximately 166 km
        assert!(dist > 150.0 && dist < 200.0, "expected ~166km, got {dist}");

        // Non-existent member returns null
        let resp = h.handle_command(args(&["GEODIST", "geo", "Palermo", "Missing", "km"]));
        assert_eq!(decode_bulk(&resp), None);
    }

    #[test]
    fn test_georadius() {
        let mut h = new_handler();
        h.handle_command(args(&[
            "GEOADD", "geo", "13.361389", "38.115556", "Palermo",
        ]));
        h.handle_command(args(&[
            "GEOADD", "geo", "15.087269", "37.502669", "Catania",
        ]));
        h.handle_command(args(&[
            "GEOADD", "geo", "2.349014", "48.864716", "Paris",
        ]));

        // Radius search from Palermo area — 200km should include Catania but not Paris
        let resp = h.handle_command(args(&[
            "GEORADIUS", "geo", "15.0", "37.5", "200", "km",
        ]));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.contains("Catania"), "should contain Catania: {s}");
        assert!(!s.contains("Paris"), "should not contain Paris: {s}");
    }

    #[test]
    fn test_georadius_with_options() {
        let mut h = new_handler();
        h.handle_command(args(&[
            "GEOADD", "geo", "13.361389", "38.115556", "Palermo",
        ]));
        h.handle_command(args(&[
            "GEOADD", "geo", "15.087269", "37.502669", "Catania",
        ]));

        // GEORADIUS WITHDIST
        let resp = h.handle_command(args(&[
            "GEORADIUS", "geo", "15.0", "37.5", "200", "km", "WITHDIST",
        ]));
        let s = String::from_utf8_lossy(&resp);
        // Should be an array of arrays (each [member, dist])
        assert!(s.contains("Catania") || s.contains("Palermo"), "should contain results: {s}");

        // GEORADIUS with COUNT
        let resp = h.handle_command(args(&[
            "GEORADIUS", "geo", "15.0", "37.5", "500", "km", "COUNT", "1",
        ]));
        let s = String::from_utf8_lossy(&resp);
        assert!(s.starts_with("*1\r\n"), "expected 1 result with COUNT 1: {s}");
    }

    #[test]
    fn test_geoadd_multiple() {
        let mut h = new_handler();
        // Add multiple members at once
        let resp = h.handle_command(args(&[
            "GEOADD", "geo",
            "13.361389", "38.115556", "Palermo",
            "15.087269", "37.502669", "Catania",
        ]));
        assert_eq!(decode_int(&resp), 2);

        // Update existing member — should return 0 (not new)
        let resp = h.handle_command(args(&[
            "GEOADD", "geo", "13.361389", "38.115556", "Palermo",
        ]));
        assert_eq!(decode_int(&resp), 0);
    }

    // ====================================================================
    // Pub/Sub command tests
    // ====================================================================

    #[test]
    fn test_publish_no_subscribers() {
        let mut h = new_handler();
        let resp = h.handle_command(args(&["PUBLISH", "channel", "hello"]));
        assert_eq!(decode_int(&resp), 0);
    }

    #[test]
    fn test_publish_with_subscriber() {
        let pubsub = Arc::new(PubSubRegistry::new());
        let kv = Arc::new(KvStore::new());
        let mut publisher = RespHandler::new(Arc::clone(&kv), None, Arc::clone(&pubsub));
        let mut subscriber = RespHandler::new(Arc::clone(&kv), None, Arc::clone(&pubsub));

        // Subscribe
        let responses = subscriber.handle_pubsub_command(args(&["SUBSCRIBE", "news"]));
        assert_eq!(responses.len(), 1);
        let s = String::from_utf8_lossy(&responses[0]);
        assert!(s.contains("subscribe"), "expected subscribe confirmation: {s}");

        // Publish
        let resp = publisher.handle_command(args(&["PUBLISH", "news", "breaking"]));
        assert_eq!(decode_int(&resp), 1);
    }

    #[test]
    fn test_subscribe_multiple_channels() {
        let pubsub = Arc::new(PubSubRegistry::new());
        let kv = Arc::new(KvStore::new());
        let mut h = RespHandler::new(kv, None, pubsub);

        let responses = h.handle_pubsub_command(args(&["SUBSCRIBE", "ch1", "ch2", "ch3"]));
        assert_eq!(responses.len(), 3);

        // Each response should show increasing subscription count
        // Response 1: subscribe ch1 1
        let s = String::from_utf8_lossy(&responses[0]);
        assert!(s.contains("subscribe") && s.contains("ch1"), "resp[0]: {s}");

        // Response 3: subscribe ch3 3
        let s = String::from_utf8_lossy(&responses[2]);
        assert!(s.contains("subscribe") && s.contains("ch3"), "resp[2]: {s}");
    }

    #[test]
    fn test_unsubscribe() {
        let pubsub = Arc::new(PubSubRegistry::new());
        let kv = Arc::new(KvStore::new());
        let mut h = RespHandler::new(kv, None, pubsub);

        h.handle_pubsub_command(args(&["SUBSCRIBE", "ch1", "ch2"]));
        let responses = h.handle_pubsub_command(args(&["UNSUBSCRIBE", "ch1"]));
        assert_eq!(responses.len(), 1);
        let s = String::from_utf8_lossy(&responses[0]);
        assert!(s.contains("unsubscribe"), "expected unsubscribe: {s}");
    }

    #[test]
    fn test_psubscribe_punsubscribe() {
        let pubsub = Arc::new(PubSubRegistry::new());
        let kv = Arc::new(KvStore::new());
        let mut h = RespHandler::new(kv, None, pubsub);

        let responses = h.handle_pubsub_command(args(&["PSUBSCRIBE", "news.*"]));
        assert_eq!(responses.len(), 1);
        let s = String::from_utf8_lossy(&responses[0]);
        assert!(s.contains("psubscribe"), "expected psubscribe: {s}");

        let responses = h.handle_pubsub_command(args(&["PUNSUBSCRIBE", "news.*"]));
        assert_eq!(responses.len(), 1);
        let s = String::from_utf8_lossy(&responses[0]);
        assert!(s.contains("punsubscribe"), "expected punsubscribe: {s}");
    }

    #[test]
    fn test_pubsub_mode_rejects_normal_commands() {
        let pubsub = Arc::new(PubSubRegistry::new());
        let kv = Arc::new(KvStore::new());
        let mut h = RespHandler::new(kv, None, pubsub);

        h.handle_pubsub_command(args(&["SUBSCRIBE", "ch"]));
        assert!(h.is_in_pubsub_mode());

        // Trying to run GET in pub/sub mode
        let responses = h.handle_pubsub_command(args(&["GET", "key"]));
        assert_eq!(responses.len(), 1);
        assert!(is_error(&responses[0]));
    }
}
