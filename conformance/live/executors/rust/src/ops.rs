//! Op dispatch: one arm per op in the spec's vocabulary.
//!
//! Every arm goes through the real `neutron-nucleus` model API. Where the spec
//! asks for something the Rust client has no surface for, the arm returns
//! `NoMapping` rather than reaching for raw SQL — an executor that works around
//! a missing method reports the ENGINE working and hides that the SDK does not.

use std::collections::HashMap;

use base64::Engine as _;
use neutron_nucleus::models::graph::Direction;
use neutron_nucleus::models::timeseries::AggregateFn;
use neutron_nucleus::models::vector::DistanceMetric;
use neutron_nucleus::{NucleusClient, NucleusConfig};
use serde_json::{json, Value};

use crate::{config_from_url, NoMapping, StepError, StepResult, TS_BASE_MS};

// ── argument helpers ─────────────────────────────────────────────────────────
//
// A wrong-shaped argument is a spec/executor mismatch, not an SDK finding, so
// these fail loudly with the op named rather than coercing.

fn s(args: &[Value], i: usize) -> Result<String, StepError> {
    args.get(i)
        .and_then(|v| v.as_str())
        .map(|v| v.to_string())
        .ok_or_else(|| StepError::failed(format!("arg {i} must be a string, got {:?}", args.get(i))))
}

fn i64_at(args: &[Value], i: usize) -> Result<i64, StepError> {
    args.get(i)
        .and_then(|v| v.as_i64())
        .ok_or_else(|| StepError::failed(format!("arg {i} must be an int, got {:?}", args.get(i))))
}

fn f64_at(args: &[Value], i: usize) -> Result<f64, StepError> {
    args.get(i)
        .and_then(|v| v.as_f64())
        .ok_or_else(|| StepError::failed(format!("arg {i} must be a number, got {:?}", args.get(i))))
}

fn obj(args: &[Value], i: usize) -> Result<Value, StepError> {
    args.get(i)
        .cloned()
        .ok_or_else(|| StepError::failed(format!("arg {i} is missing")))
}

fn floats(args: &[Value], i: usize) -> Result<Vec<f32>, StepError> {
    let arr = args
        .get(i)
        .and_then(|v| v.as_array())
        .ok_or_else(|| StepError::failed(format!("arg {i} must be a list of numbers")))?;
    arr.iter()
        .map(|v| {
            v.as_f64()
                .map(|f| f as f32)
                .ok_or_else(|| StepError::failed(format!("arg {i} contains a non-number")))
        })
        .collect()
}

fn err(e: impl std::fmt::Display) -> StepError {
    StepError::failed(format!("client error: {e}"))
}

fn unsupported(op: &str) -> StepError {
    StepError::Unsupported(NoMapping(op.to_string()))
}

pub async fn call(client: &NucleusClient, url: &str, op: &str, args: &[Value]) -> StepResult {
    match op {
        // ── core ─────────────────────────────────────────────────────────────
        "features.isNucleus" => Ok(json!(client.is_nucleus())),

        "connection.closeAndReconnect" => {
            // A fresh client, used once and dropped. The point is that a clean
            // close does not wedge the server for the next connection — the
            // Terminate-handling defect this case exists for showed up as the
            // NEXT connect hanging, not as an error on close.
            let cfg: NucleusConfig = config_from_url(url).map_err(StepError::failed)?;
            let probe = NucleusClient::connect(cfg).await.map_err(err)?;
            probe.ping().await.map_err(err)?;
            drop(probe);
            Ok(json!(true))
        }

        // ── sql ──────────────────────────────────────────────────────────────
        "sql.queryScalar" => {
            let sql = s(args, 0)?;
            let params = args.get(1).cloned().unwrap_or(Value::Array(vec![]));
            sql_scalar(client, &sql, &params).await
        }
        "sql.execute" => {
            let sql = s(args, 0)?;
            let params = args.get(1).cloned().unwrap_or(Value::Array(vec![]));
            sql_execute(client, &sql, &params).await
        }
        "sql.begin" => {
            client.sql().execute("BEGIN", &[]).await.map_err(err)?;
            Ok(Value::Null)
        }
        "sql.rollback" => {
            client.sql().execute("ROLLBACK", &[]).await.map_err(err)?;
            Ok(Value::Null)
        }

        // ── kv ───────────────────────────────────────────────────────────────
        "kv.set" => {
            let ttl = args.get(2).and_then(|v| v.as_i64());
            client
                .kv()
                .set(
                    &s(args, 0)?,
                    &s(args, 1)?,
                    ttl.map(|t| std::time::Duration::from_secs(t as u64)),
                )
                .await
                .map_err(err)?;
            Ok(Value::Null)
        }
        "kv.get" => Ok(opt_str(client.kv().get(&s(args, 0)?).await.map_err(err)?)),
        "kv.delete" => Ok(json!(client.kv().del(&s(args, 0)?).await.map_err(err)?)),
        "kv.exists" => Ok(json!(client.kv().exists(&s(args, 0)?).await.map_err(err)?)),
        "kv.incr" => {
            let by = args.get(1).and_then(|v| v.as_i64());
            Ok(json!(client.kv().incr(&s(args, 0)?, by).await.map_err(err)?))
        }
        "kv.ttl" => Ok(json!(client.kv().ttl(&s(args, 0)?).await.map_err(err)?)),
        "kv.expire" => {
            let secs = i64_at(args, 1)?;
            Ok(json!(client
                .kv()
                .expire(&s(args, 0)?, std::time::Duration::from_secs(secs as u64))
                .await
                .map_err(err)?))
        }
        "kv.rpush" => Ok(json!(client
            .kv()
            .rpush(&s(args, 0)?, &s(args, 1)?)
            .await
            .map_err(err)?)),
        "kv.llen" => Ok(json!(client.kv().llen(&s(args, 0)?).await.map_err(err)?)),
        "kv.lindex" => Ok(opt_str(
            client
                .kv()
                .lindex(&s(args, 0)?, i64_at(args, 1)?)
                .await
                .map_err(err)?,
        )),
        "kv.lrange" => {
            let v = client
                .kv()
                .lrange(&s(args, 0)?, i64_at(args, 1)?, i64_at(args, 2)?)
                .await
                .map_err(err)?;
            Ok(json!(v))
        }
        "kv.hset" => Ok(json!(client
            .kv()
            .hset(&s(args, 0)?, &s(args, 1)?, &s(args, 2)?)
            .await
            .map_err(err)?)),
        "kv.hget" => Ok(opt_str(
            client
                .kv()
                .hget(&s(args, 0)?, &s(args, 1)?)
                .await
                .map_err(err)?,
        )),
        "kv.hdel" => Ok(json!(client
            .kv()
            .hdel(&s(args, 0)?, &s(args, 1)?)
            .await
            .map_err(err)?)),
        "kv.hexists" => Ok(json!(client
            .kv()
            .hexists(&s(args, 0)?, &s(args, 1)?)
            .await
            .map_err(err)?)),
        "kv.hlen" => Ok(json!(client.kv().hlen(&s(args, 0)?).await.map_err(err)?)),
        "kv.hgetall" => {
            let m: HashMap<String, String> =
                client.kv().hgetall(&s(args, 0)?).await.map_err(err)?;
            Ok(json!(m))
        }
        "kv.sadd" => Ok(json!(client
            .kv()
            .sadd(&s(args, 0)?, &s(args, 1)?)
            .await
            .map_err(err)?)),
        "kv.srem" => Ok(json!(client
            .kv()
            .srem(&s(args, 0)?, &s(args, 1)?)
            .await
            .map_err(err)?)),
        "kv.smembers" => Ok(json!(client
            .kv()
            .smembers(&s(args, 0)?)
            .await
            .map_err(err)?)),
        "kv.zadd" => Ok(json!(client
            .kv()
            .zadd(&s(args, 0)?, f64_at(args, 1)?, &s(args, 2)?)
            .await
            .map_err(err)?)),
        "kv.zrange" => Ok(json!(client
            .kv()
            .zrange(&s(args, 0)?, i64_at(args, 1)?, i64_at(args, 2)?)
            .await
            .map_err(err)?)),

        // ── document ─────────────────────────────────────────────────────────
        "document.insert" => Ok(json!(client
            .document()
            .insert_in(&s(args, 0)?, &obj(args, 1)?)
            .await
            .map_err(err)?)),
        "document.get" => Ok(opt_json(
            client
                .document()
                .get(i64_at(args, 0)?)
                .await
                .map_err(err)?,
        )),
        "document.getIn" => Ok(opt_json(
            client
                .document()
                .get_in(&s(args, 0)?, i64_at(args, 1)?)
                .await
                .map_err(err)?,
        )),
        "document.countIn" => Ok(json!(client
            .document()
            .count_in(&s(args, 0)?)
            .await
            .map_err(err)?)),
        "document.find" => Ok(json!(client
            .document()
            .find(&s(args, 0)?, &obj(args, 1)?, 100, 0)
            .await
            .map_err(err)?)),
        "document.findOne" => Ok(opt_json(
            client
                .document()
                .find_one(&s(args, 0)?, &obj(args, 1)?)
                .await
                .map_err(err)?,
        )),
        "document.update" => Ok(json!(client
            .document()
            .update_where(&s(args, 0)?, &obj(args, 1)?, &obj(args, 2)?)
            .await
            .map_err(err)?)),
        "document.delete" => Ok(json!(client
            .document()
            .delete_where(&s(args, 0)?, &obj(args, 1)?)
            .await
            .map_err(err)?)),
        "document.getPathIn" => {
            let coll = s(args, 0)?;
            let id = i64_at(args, 1)?;
            let keys: Vec<String> = args[2..]
                .iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect();
            let refs: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();
            let raw = client
                .document()
                .path_in(&coll, id, &refs)
                .await
                .map_err(err)?;
            // DOC_PATH hands back raw JSON, so a stored string arrives as
            // "\"ada\"". S22 fixed the contract: get_path returns the VALUE.
            Ok(match raw {
                None => Value::Null,
                Some(t) => serde_json::from_str(&t).unwrap_or(Value::String(t)),
            })
        }

        // ── vector ───────────────────────────────────────────────────────────
        "vector.createCollection" => {
            client
                .vector()
                .create_collection(&s(args, 0)?, i64_at(args, 1)? as i32, DistanceMetric::Cosine)
                .await
                .map_err(err)?;
            Ok(Value::Null)
        }
        "vector.insert" => {
            client
                .vector()
                .insert(
                    &s(args, 0)?,
                    &s(args, 1)?,
                    &floats(args, 2)?,
                    &Value::Object(Default::default()),
                )
                .await
                .map_err(err)?;
            Ok(Value::Null)
        }
        "vector.count" => Ok(json!(client
            .vector()
            .count(&s(args, 0)?)
            .await
            .map_err(err)?)),
        "vector.search" => {
            let hits = client
                .vector()
                .search(
                    &s(args, 0)?,
                    &floats(args, 1)?,
                    DistanceMetric::Cosine,
                    i64_at(args, 2)?,
                )
                .await
                .map_err(err)?;
            Ok(Value::Array(
                hits.into_iter()
                    .map(|h| json!({"id": h.id, "metadata": h.metadata, "distance": h.distance}))
                    .collect(),
            ))
        }

        // ── timeseries ───────────────────────────────────────────────────────
        "timeseries.write" => {
            let points = args
                .get(1)
                .and_then(|v| v.as_array())
                .ok_or_else(|| StepError::failed("arg 1 must be a list of points"))?;
            let series = s(args, 0)?;
            for p in points {
                let t = p.get("t").and_then(|v| v.as_i64()).unwrap_or(0);
                let v = p.get("v").and_then(|v| v.as_f64()).unwrap_or(0.0);
                client
                    .timeseries()
                    .insert(&series, TS_BASE_MS + t, v)
                    .await
                    .map_err(err)?;
            }
            Ok(Value::Null)
        }
        "timeseries.count" => Ok(json!(client
            .timeseries()
            .count(&s(args, 0)?)
            .await
            .map_err(err)?)),
        "timeseries.last" => Ok(match client
            .timeseries()
            .last(&s(args, 0)?)
            .await
            .map_err(err)?
        {
            Some(v) => json!(v),
            None => Value::Null,
        }),
        "timeseries.aggregate" => {
            // start/end are OFFSETS from the spec's base instant, like the
            // points written above; the window is a DURATION and is not shifted.
            let out = client
                .timeseries()
                .aggregate(
                    &s(args, 0)?,
                    TS_BASE_MS + i64_at(args, 1)?,
                    TS_BASE_MS + i64_at(args, 2)?,
                    i64_at(args, 3)?,
                    AggregateFn::Avg,
                )
                .await
                .map_err(err)?;
            Ok(Value::Array(
                out.into_iter()
                    .map(|(t, v)| json!({"t": t, "v": v}))
                    .collect(),
            ))
        }
        "timeseries.query" => {
            let pts = client
                .timeseries()
                .range(
                    &s(args, 0)?,
                    TS_BASE_MS + i64_at(args, 1)?,
                    TS_BASE_MS + i64_at(args, 2)?,
                )
                .await
                .map_err(err)?;
            Ok(Value::Array(
                pts.into_iter().map(|(t, v)| json!({"t": t, "v": v})).collect(),
            ))
        }

        // ── blob ─────────────────────────────────────────────────────────────
        // Buckets are a client-side "bucket/key" convention shared with every
        // other SDK; the engine has one flat keyspace.
        "blob.put" => {
            let key = format!("{}/{}", s(args, 0)?, s(args, 1)?);
            let data = base64::engine::general_purpose::STANDARD
                .decode(s(args, 2)?)
                .map_err(|e| StepError::failed(format!("arg 2 is not base64: {e}")))?;
            client.blob().store(&key, &data, None).await.map_err(err)?;
            Ok(Value::Null)
        }
        "blob.get" => {
            let key = format!("{}/{}", s(args, 0)?, s(args, 1)?);
            Ok(match client.blob().get(&key).await.map_err(err)? {
                Some(bytes) => json!(base64::engine::general_purpose::STANDARD.encode(bytes)),
                None => Value::Null,
            })
        }
        "blob.getMeta" => {
            let key = format!("{}/{}", s(args, 0)?, s(args, 1)?);
            Ok(match client.blob().meta(&key).await.map_err(err)? {
                Some(m) => json!({
                    "key": m.key, "size": m.size, "content_type": m.content_type,
                    "created_at": m.created_at, "updated_at": m.updated_at
                }),
                None => Value::Null,
            })
        }
        "blob.exists" => {
            let key = format!("{}/{}", s(args, 0)?, s(args, 1)?);
            Ok(json!(client.blob().exists(&key).await.map_err(err)?))
        }
        "blob.delete" => {
            let key = format!("{}/{}", s(args, 0)?, s(args, 1)?);
            Ok(json!(client.blob().delete(&key).await.map_err(err)?))
        }

        // ── fts ──────────────────────────────────────────────────────────────
        // The spec passes [index, docId, fields]; the Rust client's FTS index is
        // global — `index(doc_id, text)` has no index-name dimension — so the
        // name is dropped and the field map is flattened to the indexed text.
        // Recorded rather than hidden: a per-index FTS surface does not exist
        // in this SDK.
        "fts.indexDoc" => {
            let doc_id: i64 = s(args, 1)?
                .parse()
                .map_err(|e| StepError::failed(format!("doc id must be numeric: {e}")))?;
            let text = match args.get(2) {
                Some(Value::Object(map)) => map
                    .values()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(" "),
                Some(Value::String(s)) => s.clone(),
                other => return Err(StepError::failed(format!("arg 2 shape {other:?}"))),
            };
            Ok(json!(client.fts().index(doc_id, &text).await.map_err(err)?))
        }
        "fts.search" => {
            let hits = client
                .fts()
                .search(&s(args, 1)?, i64_at(args, 2)?)
                .await
                .map_err(err)?;
            Ok(Value::Array(
                hits.into_iter()
                    .map(|h| json!({"doc_id": h.doc_id, "score": h.score}))
                    .collect(),
            ))
        }

        // ── graph ────────────────────────────────────────────────────────────
        "graph.addNode" => {
            // The spec passes a label LIST; the Rust client takes one label, so
            // the first is used. A multi-label node has no Rust surface.
            let labels = args
                .get(0)
                .and_then(|v| v.as_array())
                .ok_or_else(|| StepError::failed("arg 0 must be a list of labels"))?;
            let label = labels
                .first()
                .and_then(|v| v.as_str())
                .ok_or_else(|| StepError::failed("arg 0 must contain at least one label"))?;
            let props = args.get(1).cloned().unwrap_or(Value::Null);
            Ok(json!(client
                .graph()
                .add_node(label, props.as_object().map(|_| &props))
                .await
                .map_err(err)?))
        }
        // The spec orders these [type, from, to]; the client takes
        // (from, to, type). Reading them positionally in the client's order was
        // wrong and produced "arg 0 must be an int".
        "graph.addEdge" => {
            let props = args.get(3).cloned().unwrap_or(Value::Null);
            Ok(json!(client
                .graph()
                .add_edge(
                    i64_at(args, 1)?,
                    i64_at(args, 2)?,
                    &s(args, 0)?,
                    props.as_object().map(|_| &props)
                )
                .await
                .map_err(err)?))
        }
        "graph.deleteNode" => Ok(json!(client
            .graph()
            .delete_node(i64_at(args, 0)?)
            .await
            .map_err(err)?)),
        "graph.neighbors" => {
            let ns = client
                .graph()
                .neighbors(i64_at(args, 0)?, Direction::Both)
                .await
                .map_err(err)?;
            Ok(Value::Array(
                ns.into_iter()
                    .map(|n| {
                        json!({"neighbor_id": n.neighbor_id, "edge_id": n.edge_id,
                               "edge_type": n.edge_type})
                    })
                    .collect(),
            ))
        }
        "graph.shortestPath" => Ok(json!(client
            .graph()
            .shortest_path(i64_at(args, 0)?, i64_at(args, 1)?)
            .await
            .map_err(err)?)),
        "graph.nodeCount" => Ok(json!(client.graph().node_count().await.map_err(err)?)),
        "graph.edgeCount" => Ok(json!(client.graph().edge_count().await.map_err(err)?)),

        // ── streams ──────────────────────────────────────────────────────────
        "streams.xadd" => {
            let fields = args
                .get(1)
                .and_then(|v| v.as_object())
                .ok_or_else(|| StepError::failed("arg 1 must be a field map"))?;
            let owned: Vec<(String, String)> = fields
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.as_str().map(|s| s.to_string()).unwrap_or_else(|| v.to_string()),
                    )
                })
                .collect();
            let refs: Vec<(&str, &str)> =
                owned.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            Ok(json!(client
                .streams()
                .xadd(&s(args, 0)?, &refs)
                .await
                .map_err(err)?))
        }
        "streams.xlen" => Ok(json!(client
            .streams()
            .xlen(&s(args, 0)?)
            .await
            .map_err(err)?)),
        "streams.xrange" => {
            let e = client
                .streams()
                .xrange(&s(args, 0)?, 0, i64::MAX, 100)
                .await
                .map_err(err)?;
            Ok(entries(e))
        }
        "streams.xread" => {
            let e = client
                .streams()
                .xread(&s(args, 0)?, 0, 100)
                .await
                .map_err(err)?;
            Ok(entries(e))
        }
        "streams.xgroupCreate" => Ok(json!(client
            .streams()
            .xgroup_create(&s(args, 0)?, &s(args, 1)?, 0)
            .await
            .map_err(err)?)),
        "streams.xreadgroup" => {
            let e = client
                .streams()
                .xreadgroup(&s(args, 0)?, &s(args, 1)?, &s(args, 2)?, 100)
                .await
                .map_err(err)?;
            Ok(entries(e))
        }
        // xadd returns a single "<ms>-<seq>" string and xack takes the two
        // halves as separate integers, so the two ends of the same API do not
        // compose — which is exactly what this case's xfail records.
        //
        // The first version of this arm split the string here and the case
        // PASSED, reporting an xpass and claiming the defect was fixed. An
        // executor that does the SDK's job proves the engine works and hides
        // that the client does not, which is the one thing this suite must
        // never do. It is left failing, honestly, until the SDK composes.
        "streams.xack" => Err(StepError::failed(
            "xadd returns a single \"<ms>-<seq>\" string and xack takes the two halves \
             as separate integers, so the two ends of the same API do not compose. \
             The executor deliberately does not split the string on the SDK's behalf: \
             doing so made this case pass and report the defect fixed.",
        )),

        // ── datalog ──────────────────────────────────────────────────────────
        "datalog.assertFact" => Ok(json!(client
            .datalog()
            .assert_fact(&s(args, 0)?)
            .await
            .map_err(err)?)),
        "datalog.query" => Ok(json!(client
            .datalog()
            .query(&s(args, 0)?)
            .await
            .map_err(err)?)),
        "datalog.clear" => Ok(json!(client
            .datalog()
            .clear(&s(args, 0)?)
            .await
            .map_err(err)?)),

        // ── cdc ──────────────────────────────────────────────────────────────
        "cdc.read" => {
            let events = client
                .cdc()
                .read(i64_at(args, 0)?, i64_at(args, 1)?)
                .await
                .map_err(err)?;
            Ok(Value::Array(
                events
                    .into_iter()
                    .map(|e| json!({"seq": e.seq, "table": e.table,
                                    "change": e.change, "ts": e.ts}))
                    .collect(),
            ))
        }
        "cdc.count" => Ok(json!(client.cdc().count().await.map_err(err)?)),

        other => Err(unsupported(other)),
    }
}

fn entries(e: Vec<neutron_nucleus::models::streams::StreamEntry>) -> Value {
    Value::Array(
        e.into_iter()
            .map(|x| json!({"id": x.id, "fields": x.fields}))
            .collect(),
    )
}

fn opt_str(v: Option<String>) -> Value {
    match v {
        Some(s) => Value::String(s),
        None => Value::Null,
    }
}

fn opt_json(v: Option<Value>) -> Value {
    v.unwrap_or(Value::Null)
}

// ── raw SQL ──────────────────────────────────────────────────────────────────
//
// The spec binds parameters positionally with JSON-typed values, and
// tokio-postgres needs a concrete Rust type per parameter that matches what the
// server DECLARED for that position. Guessing from the JSON alone is wrong in
// the case these tests exist for: `SELECT $1::int` is declared int4, and
// binding an i64 fails to serialize.
//
// So the statement is prepared first and each value converted to the declared
// type. That is also what makes these cases meaningful — binding everything as
// text is exactly what made the binary-parameter corruption invisible for two
// months.

use tokio_postgres::types::{ToSql, Type};

async fn sql_scalar(client: &NucleusClient, sql: &str, params: &Value) -> StepResult {
    let conn = client.pool().get().await.map_err(err)?;
    let pg = conn.client();
    let stmt = pg.prepare(sql).await.map_err(err)?;
    let owned = bind(params, stmt.params())?;
    let refs: Vec<&(dyn ToSql + Sync)> = owned.iter().map(|p| p.as_sql()).collect();
    let rows = pg.query(&stmt, &refs).await.map_err(err)?;
    match rows.first() {
        None => Ok(Value::Null),
        Some(row) => Ok(row_value(row, 0)),
    }
}

async fn sql_execute(client: &NucleusClient, sql: &str, params: &Value) -> StepResult {
    let conn = client.pool().get().await.map_err(err)?;
    let pg = conn.client();
    let stmt = pg.prepare(sql).await.map_err(err)?;
    let owned = bind(params, stmt.params())?;
    let refs: Vec<&(dyn ToSql + Sync)> = owned.iter().map(|p| p.as_sql()).collect();
    let n = pg.execute(&stmt, &refs).await.map_err(err)?;
    Ok(json!(n))
}

/// A parameter that owns its value, so a borrow outlives the arm that made it.
enum Param {
    Text(String),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
    Json(Value),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Null,
}

impl Param {
    fn as_sql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Param::Text(v) => v,
            Param::I16(v) => v,
            Param::I32(v) => v,
            Param::I64(v) => v,
            Param::F32(v) => v,
            Param::F64(v) => v,
            Param::Bool(v) => v,
            Param::Json(v) => v,
            Param::Bytes(v) => v,
            Param::Uuid(v) => v,
            Param::Null => &Option::<String>::None,
        }
    }
}

/// Convert each JSON argument to the Rust type matching the server's declared
/// parameter type.
fn bind(params: &Value, declared: &[Type]) -> Result<Vec<Param>, StepError> {
    let empty = Vec::new();
    let list = params.as_array().unwrap_or(&empty);
    let mut out = Vec::with_capacity(list.len());

    for (i, v) in list.iter().enumerate() {
        let ty = declared.get(i).cloned().unwrap_or(Type::TEXT);
        out.push(match (v, &ty) {
            (Value::Null, _) => Param::Null,
            (_, &Type::INT2) => Param::I16(as_i64(v, i)? as i16),
            (_, &Type::INT4) => Param::I32(as_i64(v, i)? as i32),
            (_, &Type::INT8) => Param::I64(as_i64(v, i)?),
            (_, &Type::FLOAT4) => Param::F32(as_f64(v, i)? as f32),
            (_, &Type::FLOAT8) => Param::F64(as_f64(v, i)?),
            (Value::Bool(b), &Type::BOOL) => Param::Bool(*b),
            (_, &Type::JSON) | (_, &Type::JSONB) => Param::Json(v.clone()),
            (Value::String(s), &Type::BYTEA) => Param::Bytes(s.clone().into_bytes()),
            // tokio-postgres binds UUID as a real uuid::Uuid; a dashed string
            // is refused outright rather than parsed for you, which is the same
            // strictness Postgrex has and the opposite of asyncpg's.
            (Value::String(s), &Type::UUID) => Param::Uuid(
                s.parse()
                    .map_err(|e| StepError::failed(format!("param {i} is not a uuid: {e}")))?,
            ),
            (Value::String(s), _) => Param::Text(s.clone()),
            (Value::Bool(b), _) => Param::Bool(*b),
            (Value::Number(n), _) if n.is_i64() => Param::I64(n.as_i64().unwrap()),
            (Value::Number(n), _) => Param::F64(n.as_f64().unwrap()),
            (other, _) => Param::Json(other.clone()),
        });
    }
    Ok(out)
}

fn as_i64(v: &Value, i: usize) -> Result<i64, StepError> {
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| StepError::failed(format!("param {i} is not an integer: {v}")))
}

fn as_f64(v: &Value, i: usize) -> Result<f64, StepError> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| StepError::failed(format!("param {i} is not a number: {v}")))
}

/// Decode column `i` without knowing its type ahead of time. Tries the concrete
/// types the spec's cases actually use, in the order that keeps an integer an
/// integer — reading everything as text is what made the binary-parameter
/// corruption invisible for two months.
fn row_value(row: &tokio_postgres::Row, i: usize) -> Value {
    if let Ok(v) = row.try_get::<_, Option<i64>>(i) {
        return v.map(|x| json!(x)).unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<i32>>(i) {
        return v.map(|x| json!(x)).unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<bool>>(i) {
        return v.map(|x| json!(x)).unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<f64>>(i) {
        return v.map(|x| json!(x)).unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<Value>>(i) {
        return v.unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<uuid::Uuid>>(i) {
        return v.map(|u| json!(u.to_string())).unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<Vec<u8>>>(i) {
        return v
            .map(|b| json!(base64::engine::general_purpose::STANDARD.encode(b)))
            .unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<String>>(i) {
        return v.map(Value::String).unwrap_or(Value::Null);
    }
    Value::Null
}
