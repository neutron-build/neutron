// Go executor for the Nucleus live data-model conformance spec.
//
// Reads ../../spec.json, runs every case against a live engine through the real
// in-repo Go client (github.com/neutron-dev/neutron-go/nucleus), and prints one
// JSON result document to stdout. It asserts nothing a mock could assert: only
// that a call reaches the engine, is accepted over the wire, and comes back with
// the right value.
//
//	NEUTRON_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/postgres \
//	    go run .
//
// Exit codes: 0 all cases behaved as the spec says, 1 otherwise. An `xfail` case
// that PASSES is a failure — otherwise a fix lands and the note explaining why
// the case is expected to fail quietly becomes a lie.
//
// Everything on stdout is the report. Diagnostics go to stderr, because the
// orchestrator parses stdout.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/neutron-dev/neutron-go/nucleus"
)

// tsBase is the instant the spec's time-series millisecond offsets are measured
// from. Fixed so the cases are deterministic and comparable across SDKs.
var tsBase = time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)

var fixtureRe = regexp.MustCompile(`@([A-Za-z_][A-Za-z0-9_]*)`)

// stepTimeout bounds a single op. A hang is a finding, but a hung run reports
// nothing at all, so it is turned into a failure with a name attached.
const stepTimeout = 60 * time.Second

// errUnsupported marks an op the Go SDK has no surface for. Undeclared, it is a
// failure; declared in unsupported.json with a reason, it is `unsupported`.
type errUnsupported struct{ op string }

func (e *errUnsupported) Error() string { return "no Go mapping for op " + e.op }

type step struct {
	Op     string         `json:"op"`
	Args   []any          `json:"args"`
	Bind   string         `json:"bind"`
	Expect map[string]any `json:"expect"`
}

type specCase struct {
	ID    string `json:"id"`
	Model string `json:"model"`
	XFail *struct {
		Reason string   `json:"reason"`
		SDKs   []string `json:"sdks"`
	} `json:"xfail"`
	Steps []step `json:"steps"`
}

type spec struct {
	SpecVersion int        `json:"specVersion"`
	Cases       []specCase `json:"cases"`
}

type caseResult struct {
	ID     string `json:"id"`
	Model  string `json:"model"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

// ── argument resolution ──────────────────────────────────────────────────────

// resolve turns a spec argument into a call argument. "@name" is a per-case
// unique fixture (stable within a case, unique across runs); "$name" is a value
// bound by an earlier step; anything else is a literal.
func resolve(v any, fixtures map[string]string, bound map[string]any) (any, error) {
	switch x := v.(type) {
	case string:
		if strings.HasPrefix(x, "$") {
			name := x[1:]
			val, ok := bound[name]
			if !ok {
				return nil, fmt.Errorf("step references $%s before it was bound", name)
			}
			return val, nil
		}
		return fixtureRe.ReplaceAllStringFunc(x, func(m string) string {
			name := m[1:]
			if f, ok := fixtures[name]; ok {
				return f
			}
			f := name + "_" + randHex(5)
			fixtures[name] = f
			return f
		}), nil
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			r, err := resolve(e, fixtures, bound)
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			r, err := resolve(e, fixtures, bound)
			if err != nil {
				return nil, err
			}
			out[k] = r
		}
		return out, nil
	}
	return v, nil
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// ── expectations ─────────────────────────────────────────────────────────────

// check applies the spec's expectation vocabulary to one result. Semantics
// match the Python executor exactly, including that `nonEmpty` is truthiness.
func check(result any, expect map[string]any) error {
	actual := result

	if k, ok := expect["key"]; ok {
		if actual == nil {
			return fmt.Errorf("expected a map with key %v, got nil", k)
		}
		m, isMap := actual.(map[string]any)
		if !isMap {
			return fmt.Errorf("expected a map with key %v, got %T: %s", k, actual, show(actual))
		}
		v, present := m[fmt.Sprint(k)]
		if !present {
			return fmt.Errorf("key %v is absent from %s", k, show(actual))
		}
		actual = v
	}

	if idx, ok := expect["index"]; ok {
		l, isList := actual.([]any)
		if !isList {
			return fmt.Errorf("expected a list to index, got %T: %s", actual, show(actual))
		}
		i := int(toFloat(idx))
		if i < 0 || i >= len(l) {
			return fmt.Errorf("index %d out of range for %d elements: %s", i, len(l), show(actual))
		}
		actual = l[i]
	}

	// Pass a non-string through unchanged. pgx decodes jsonb itself, so a
	// literal implementation would report "expects a string, got map" and hide
	// a value that is in fact correct. The point of the expectation is to
	// compare the VALUE, whichever side of the wire decoded it.
	if b, ok := expect["jsonDecode"].(bool); ok && b {
		if s, isStr := actual.(string); isStr {
			var decoded any
			if err := json.Unmarshal([]byte(s), &decoded); err != nil {
				return fmt.Errorf("jsonDecode failed on %q: %v", s, err)
			}
			actual = decoded
		}
	}

	// Same idea for containers the driver chose on our behalf. pgx decodes uuid
	// to a [16]byte, so the value is right and only the shape differs; render
	// it canonically rather than recording a false disagreement.
	if arr, ok := actual.([16]byte); ok {
		actual = fmt.Sprintf("%x-%x-%x-%x-%x", arr[0:4], arr[4:6], arr[6:8], arr[8:10], arr[10:16])
	}

	if b, ok := expect["notNull"].(bool); ok && b {
		if actual == nil {
			return fmt.Errorf("expected a value, got nil")
		}
	}
	if b, ok := expect["isNull"].(bool); ok && b {
		if actual != nil {
			return fmt.Errorf("expected nil, got %s", show(actual))
		}
	}
	if b, ok := expect["nonEmpty"].(bool); ok && b {
		if !truthy(actual) {
			return fmt.Errorf("expected a non-empty collection, got %s", show(actual))
		}
	}
	if want, ok := expect["length"]; ok {
		n, err := lengthOf(actual)
		if err != nil {
			return err
		}
		if n != int(toFloat(want)) {
			return fmt.Errorf("expected %d elements, got %d: %s", int(toFloat(want)), n, show(actual))
		}
	}
	if want, ok := expect["type"]; ok {
		if err := checkType(actual, fmt.Sprint(want)); err != nil {
			return err
		}
	}
	if want, ok := expect["equals"]; ok {
		eq, err := jsonEqual(actual, want)
		if err != nil {
			return err
		}
		if !eq {
			return fmt.Errorf("expected %s, got %s", show(want), show(actual))
		}
	}
	return nil
}

func checkType(actual any, want string) error {
	ok := false
	switch want {
	case "list":
		_, ok = actual.([]any)
	case "map":
		_, ok = actual.(map[string]any)
	case "string":
		_, ok = actual.(string)
	case "int":
		_, ok = actual.(int64)
	case "float":
		switch actual.(type) {
		case float64, int64:
			ok = true
		}
	case "bool":
		_, ok = actual.(bool)
	case "bytes":
		_, ok = actual.([]byte)
	default:
		return fmt.Errorf("unknown expectation type %q", want)
	}
	if !ok {
		return fmt.Errorf("expected %s, got %T: %s", want, actual, show(actual))
	}
	return nil
}

func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	case int64:
		return x != 0
	case float64:
		return x != 0
	}
	return true
}

func lengthOf(v any) (int, error) {
	switch x := v.(type) {
	case []any:
		return len(x), nil
	case map[string]any:
		return len(x), nil
	case string:
		return len(x), nil
	}
	return 0, fmt.Errorf("expected a collection with a length, got %T: %s", v, show(v))
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case int:
		return float64(x)
	}
	return 0
}

// jsonEqual compares through JSON so that Go's int64/float64/[]string and the
// spec's generic JSON values are compared on the same terms.
func jsonEqual(actual, want any) (bool, error) {
	b, err := json.Marshal(actual)
	if err != nil {
		return false, fmt.Errorf("cannot compare %T: %v", actual, err)
	}
	var normalised any
	if err := json.Unmarshal(b, &normalised); err != nil {
		return false, fmt.Errorf("cannot compare %T: %v", actual, err)
	}
	return reflect.DeepEqual(normalised, want), nil
}

func show(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%#v", v)
	}
	return string(b)
}

// ── ops ──────────────────────────────────────────────────────────────────────

// Ops maps spec op names onto the Go SDK. One flat switch, no cleverness: the
// mapping has to stay auditable, and an assertion that lives in an executor is
// drift waiting to happen.
type Ops struct {
	c   *nucleus.Client
	url string
	tx  *nucleus.Tx // non-nil between sql.begin and sql.rollback
}

// sqlModel routes raw SQL through the open transaction when there is one.
func (o *Ops) sqlModel() *nucleus.SQLModel {
	if o.tx != nil {
		return o.tx.SQL()
	}
	return o.c.SQL()
}

func (o *Ops) call(ctx context.Context, op string, args []any) (any, error) {
	switch op {

	// ── core ────────────────────────────────────────────────────────────
	case "features.isNucleus":
		return o.c.Features().IsNucleus, nil

	case "connection.closeAndReconnect":
		probe, err := nucleus.Connect(ctx, o.url)
		if err != nil {
			return nil, err
		}
		// Hung forever before N25: the server ignored Terminate and never
		// closed the socket. pgxpool.Close blocks until every connection is
		// released, so the timeout turns that hang into a failure.
		done := make(chan struct{})
		go func() { probe.Close(); close(done) }()
		select {
		case <-done:
			return true, nil
		case <-time.After(15 * time.Second):
			return nil, fmt.Errorf("pool close did not return within 15s")
		}

	// ── document ────────────────────────────────────────────────────────
	case "document.insert":
		id, err := o.c.Document().Insert(ctx, argStr(args, 0), argMap(args, 1))
		return id, err

	case "document.get":
		doc, err := o.c.Document().Get(ctx, argInt(args, 0))
		return anyMap(doc), err

	case "document.getIn":
		doc, err := o.c.Document().GetIn(ctx, argStr(args, 0), argInt(args, 1))
		return anyMap(doc), err

	case "document.getPathIn":
		keys := make([]string, 0, len(args)-2)
		for _, a := range args[2:] {
			keys = append(keys, fmt.Sprint(a))
		}
		val, err := o.c.Document().PathIn(ctx, argStr(args, 0), argInt(args, 1), keys...)
		return strPtr(val), err

	case "document.update":
		n, err := o.c.Document().Update(ctx, argStr(args, 0), argMap(args, 1), argMap(args, 2))
		return n, err

	case "document.delete":
		n, err := o.c.Document().Delete(ctx, argStr(args, 0), argMap(args, 1))
		return n, err

	case "document.countIn":
		n, err := o.c.Document().CountIn(ctx, argStr(args, 0))
		return n, err

	case "document.find":
		docs, err := o.c.Document().Find(ctx, argStr(args, 0), argMap(args, 1))
		return mapList(docs), err

	case "document.findOne":
		doc, err := o.c.Document().FindOne(ctx, argStr(args, 0), argMap(args, 1))
		return anyMap(doc), err

	// ── graph ───────────────────────────────────────────────────────────
	case "graph.addNode":
		id, err := o.c.Graph().AddNode(ctx, argStrList(args, 0), argMap(args, 1))
		return id, err

	case "graph.addEdge":
		// Spec order is (type, from, to); the Go signature is (from, to, type).
		id, err := o.c.Graph().AddEdge(ctx, argInt(args, 1), argInt(args, 2), argStr(args, 0), nil)
		return id, err

	case "graph.neighbors":
		dir, err := direction(argStr(args, 1))
		if err != nil {
			return nil, err
		}
		ns, err := o.c.Graph().Neighbors(ctx, argInt(args, 0), "", dir)
		return jsonList(ns), err

	case "graph.shortestPath":
		ids, err := o.c.Graph().ShortestPath(ctx, argInt(args, 0), argInt(args, 1))
		if err != nil || ids == nil {
			return nil, err
		}
		return jsonList(ids), nil

	case "graph.nodeCount":
		n, err := o.c.Graph().NodeCount(ctx)
		return n, err

	case "graph.edgeCount":
		n, err := o.c.Graph().EdgeCount(ctx)
		return n, err

	case "graph.deleteNode":
		ok, err := o.c.Graph().DeleteNode(ctx, argInt(args, 0))
		return ok, err

	// ── key/value ───────────────────────────────────────────────────────
	case "kv.set":
		return nil, o.c.KV().Set(ctx, argStr(args, 0), []byte(argStr(args, 1)))

	case "kv.get":
		v, err := o.c.KV().Get(ctx, argStr(args, 0))
		if err != nil || v == nil {
			return nil, err
		}
		return string(v), nil

	case "kv.exists":
		ok, err := o.c.KV().Exists(ctx, argStr(args, 0))
		return ok, err

	case "kv.delete":
		ok, err := o.c.KV().Delete(ctx, argStr(args, 0))
		return ok, err

	case "kv.expire":
		ok, err := o.c.KV().Expire(ctx, argStr(args, 0), time.Duration(argInt(args, 1))*time.Second)
		return ok, err

	case "kv.ttl":
		n, err := o.c.KV().TTL(ctx, argStr(args, 0))
		return n, err

	case "kv.incr":
		n, err := o.c.KV().Incr(ctx, argStr(args, 0), argInt(args, 1))
		return n, err

	case "kv.rpush":
		n, err := o.c.KV().RPush(ctx, argStr(args, 0), argStr(args, 1))
		return n, err

	case "kv.llen":
		n, err := o.c.KV().LLen(ctx, argStr(args, 0))
		return n, err

	case "kv.lrange":
		v, err := o.c.KV().LRange(ctx, argStr(args, 0), argInt(args, 1), argInt(args, 2))
		return strList(v), err

	case "kv.lindex":
		v, err := o.c.KV().LIndex(ctx, argStr(args, 0), argInt(args, 1))
		return strPtr(v), err

	case "kv.zadd":
		ok, err := o.c.KV().ZAdd(ctx, argStr(args, 0), argFloat(args, 1), argStr(args, 2))
		return ok, err

	case "kv.zrange":
		v, err := o.c.KV().ZRange(ctx, argStr(args, 0), argInt(args, 1), argInt(args, 2))
		return strList(v), err

	case "kv.hset":
		ok, err := o.c.KV().HSet(ctx, argStr(args, 0), argStr(args, 1), argStr(args, 2))
		return ok, err

	case "kv.hget":
		v, err := o.c.KV().HGet(ctx, argStr(args, 0), argStr(args, 1))
		return strPtr(v), err

	case "kv.hexists":
		ok, err := o.c.KV().HExists(ctx, argStr(args, 0), argStr(args, 1))
		return ok, err

	case "kv.hgetall":
		m, err := o.c.KV().HGetAll(ctx, argStr(args, 0))
		if err != nil {
			return nil, err
		}
		out := make(map[string]any, len(m))
		for k, v := range m {
			out[k] = v
		}
		return out, nil

	case "kv.hlen":
		n, err := o.c.KV().HLen(ctx, argStr(args, 0))
		return n, err

	case "kv.hdel":
		ok, err := o.c.KV().HDel(ctx, argStr(args, 0), argStr(args, 1))
		return ok, err

	case "kv.sadd":
		ok, err := o.c.KV().SAdd(ctx, argStr(args, 0), argStr(args, 1))
		return ok, err

	case "kv.srem":
		ok, err := o.c.KV().SRem(ctx, argStr(args, 0), argStr(args, 1))
		return ok, err

	case "kv.smembers":
		v, err := o.c.KV().SMembers(ctx, argStr(args, 0))
		return strList(v), err

	// ── time series ─────────────────────────────────────────────────────
	case "timeseries.write":
		raw, _ := args[1].([]any)
		points := make([]nucleus.TimeSeriesPoint, 0, len(raw))
		for _, p := range raw {
			m, _ := p.(map[string]any)
			points = append(points, nucleus.TimeSeriesPoint{
				Timestamp: tsBase.Add(time.Duration(toFloat(m["t"])) * time.Millisecond),
				Value:     toFloat(m["v"]),
			})
		}
		return nil, o.c.TimeSeries().Write(ctx, argStr(args, 0), points)

	case "timeseries.count":
		n, err := o.c.TimeSeries().Count(ctx, argStr(args, 0))
		return n, err

	case "timeseries.last":
		v, err := o.c.TimeSeries().Last(ctx, argStr(args, 0))
		if err != nil || v == nil {
			return nil, err
		}
		return *v, nil

	case "timeseries.query":
		pts, err := o.c.TimeSeries().Query(ctx, argStr(args, 0), tsAt(args, 1), tsAt(args, 2))
		return jsonList(pts), err

	case "timeseries.aggregate":
		pts, err := o.c.TimeSeries().Aggregate(ctx, argStr(args, 0), tsAt(args, 1), tsAt(args, 2),
			time.Duration(argInt(args, 3))*time.Millisecond, nucleus.Avg)
		return jsonList(pts), err

	// ── streams ─────────────────────────────────────────────────────────
	case "streams.xadd":
		id, err := o.c.Streams().XAdd(ctx, argStr(args, 0), argMap(args, 1))
		return id, err

	case "streams.xlen":
		n, err := o.c.Streams().XLen(ctx, argStr(args, 0))
		return n, err

	case "streams.xrange":
		es, err := o.c.Streams().XRange(ctx, argStr(args, 0), argInt(args, 1), argInt(args, 2), argInt(args, 3))
		return jsonList(es), err

	case "streams.xread":
		es, err := o.c.Streams().XRead(ctx, argStr(args, 0), argInt(args, 1), argInt(args, 2))
		return jsonList(es), err

	case "streams.xgroupCreate":
		ok, err := o.c.Streams().XGroupCreate(ctx, argStr(args, 0), argStr(args, 1), argInt(args, 2))
		return ok, err

	case "streams.xreadgroup":
		es, err := o.c.Streams().XReadGroup(ctx, argStr(args, 0), argStr(args, 1), argStr(args, 2), argInt(args, 3))
		return jsonList(es), err

	case "streams.xack":
		// XAdd hands back one "ms-seq" string; XAck takes (idMs, idSeq) as two
		// integers. Feeding the id straight back is the natural round trip and
		// is what a caller writes, so that is what is measured here — not a
		// split invented by the executor to make the two halves compose.
		entryID := argStr(args, 2)
		idMs, err := strconv.ParseInt(entryID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("XAck takes an integer id_ms; XAdd returned %q: %v", entryID, err)
		}
		n, err := o.c.Streams().XAck(ctx, argStr(args, 0), argStr(args, 1), idMs, 0)
		return n, err

	// ── blobs ───────────────────────────────────────────────────────────
	case "blob.put":
		data, err := base64.StdEncoding.DecodeString(argStr(args, 2))
		if err != nil {
			return nil, err
		}
		return nil, o.c.Blob().Put(ctx, argStr(args, 0), argStr(args, 1), bytes.NewReader(data))

	case "blob.get":
		rc, _, err := o.c.Blob().Get(ctx, argStr(args, 0), argStr(args, 1))
		if err != nil || rc == nil {
			return nil, err
		}
		data, err := io.ReadAll(rc)
		if err != nil {
			return nil, err
		}
		return base64.StdEncoding.EncodeToString(data), nil

	case "blob.getMeta":
		m, err := o.c.Blob().Meta(ctx, argStr(args, 0), argStr(args, 1))
		if err != nil || m == nil {
			return nil, err
		}
		return jsonAny(m)

	case "blob.exists":
		ok, err := o.c.Blob().Exists(ctx, argStr(args, 0), argStr(args, 1))
		return ok, err

	case "blob.delete":
		ok, err := o.c.Blob().Delete(ctx, argStr(args, 0), argStr(args, 1))
		return ok, err

	// ── cdc ─────────────────────────────────────────────────────────────
	case "cdc.read":
		// The Go SDK returns CDC_READ's payload as raw JSON text rather than a
		// decoded list. Returned as-is: what the caller gets is the result.
		raw, err := o.c.CDC().Read(ctx, argInt(args, 0), argInt(args, 1))
		return raw, err

	case "cdc.count":
		n, err := o.c.CDC().Count(ctx)
		return n, err

	// ── datalog ─────────────────────────────────────────────────────────
	case "datalog.assertFact":
		msg, err := o.c.Datalog().Assert(ctx, argStr(args, 0))
		return msg, err

	case "datalog.query":
		// Same shape as cdc.read: the Go SDK hands back the engine's JSON text.
		raw, err := o.c.Datalog().Query(ctx, argStr(args, 0))
		return raw, err

	case "datalog.clear":
		msg, err := o.c.Datalog().Clear(ctx, argStr(args, 0))
		return msg, err

	// ── full-text search ────────────────────────────────────────────────
	case "fts.indexDoc":
		// FTS_INDEX takes (doc_id int, text). The index name has no engine
		// surface — the Python SDK drops it the same way — and the field map is
		// joined into one text blob, also as Python does.
		fields := argMap(args, 2)
		parts := make([]string, 0, len(fields))
		for _, v := range fields {
			parts = append(parts, fmt.Sprint(v))
		}
		docID, err := strconv.ParseInt(argStr(args, 1), 10, 64)
		if err != nil {
			return nil, err
		}
		ok, err := o.c.FTS().Index(ctx, docID, strings.Join(parts, " "))
		return ok, err

	case "fts.search":
		rs, err := o.c.FTS().Search(ctx, argStr(args, 1), nucleus.WithFTSLimit(argInt(args, 2)))
		return jsonList(rs), err

	// ── vector ──────────────────────────────────────────────────────────
	case "vector.createCollection":
		return nil, o.c.Vector().CreateCollection(ctx, argStr(args, 0), int(argInt(args, 1)), nucleus.Cosine)

	case "vector.insert":
		return nil, o.c.Vector().Insert(ctx, argStr(args, 0), argStr(args, 1), argFloat32List(args, 2), nil)

	case "vector.count":
		// VectorModel has no Count; the Python SDK's count is itself a plain
		// SELECT COUNT(*), so the same statement goes through the SQL model.
		n, err := nucleus.QueryOne[any](ctx, o.sqlModel(),
			fmt.Sprintf("SELECT COUNT(*) FROM %s", argStr(args, 0)))
		return n, err

	case "vector.search":
		rs, err := o.c.Vector().Search(ctx, argStr(args, 0), argFloat32List(args, 1),
			nucleus.WithLimit(int(argInt(args, 2))))
		return jsonList(rs), err

	// ── raw sql ─────────────────────────────────────────────────────────
	case "sql.queryScalar":
		v, err := nucleus.QueryOne[any](ctx, o.sqlModel(), argStr(args, 0), argList(args, 1)...)
		return v, err

	case "sql.execute":
		n, err := o.sqlModel().Exec(ctx, argStr(args, 0), argList(args, 1)...)
		return n, err

	case "sql.begin":
		tx, err := o.c.Begin(ctx)
		if err != nil {
			return nil, err
		}
		o.tx = tx
		return nil, nil

	case "sql.rollback":
		if o.tx == nil {
			return nil, fmt.Errorf("sql.rollback with no open transaction")
		}
		tx := o.tx
		o.tx = nil
		return nil, tx.Rollback(ctx)
	}

	return nil, &errUnsupported{op: op}
}

func (o *Ops) cleanup(ctx context.Context) {
	if o.tx != nil {
		_ = o.tx.Rollback(ctx)
		o.tx = nil
	}
}

// ── argument helpers ─────────────────────────────────────────────────────────

func argStr(args []any, i int) string {
	if i >= len(args) || args[i] == nil {
		return ""
	}
	if s, ok := args[i].(string); ok {
		return s
	}
	return fmt.Sprint(args[i])
}

func argInt(args []any, i int) int64 {
	if i >= len(args) {
		return 0
	}
	switch x := args[i].(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

func argFloat(args []any, i int) float64 {
	if i >= len(args) {
		return 0
	}
	return toFloat(args[i])
}

func argMap(args []any, i int) map[string]any {
	if i >= len(args) {
		return nil
	}
	m, _ := args[i].(map[string]any)
	return m
}

func argList(args []any, i int) []any {
	if i >= len(args) {
		return nil
	}
	l, _ := args[i].([]any)
	return l
}

func argStrList(args []any, i int) []string {
	l := argList(args, i)
	out := make([]string, 0, len(l))
	for _, v := range l {
		out = append(out, fmt.Sprint(v))
	}
	return out
}

func argFloat32List(args []any, i int) []float32 {
	l := argList(args, i)
	out := make([]float32, 0, len(l))
	for _, v := range l {
		out = append(out, float32(toFloat(v)))
	}
	return out
}

// tsAt reads a millisecond offset from the spec's fixed time-series base.
func tsAt(args []any, i int) time.Time {
	return tsBase.Add(time.Duration(argInt(args, i)) * time.Millisecond)
}

func direction(s string) (nucleus.Direction, error) {
	switch s {
	case "out":
		return nucleus.Outgoing, nil
	case "in":
		return nucleus.Incoming, nil
	case "both":
		return nucleus.Both, nil
	}
	return nucleus.Outgoing, fmt.Errorf("unknown direction %q", s)
}

// ── result normalisation ─────────────────────────────────────────────────────
//
// Results are normalised to the generic JSON shapes the expectation vocabulary
// is written against, so `key`, `index`, `length` and `equals` mean the same
// thing here as they do in every other executor. Nothing is reinterpreted: a
// nil map stays absent, a nil slice stays an empty collection.

func anyMap(m map[string]any) any {
	if m == nil {
		return nil
	}
	return m
}

func strPtr(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

func strList(v []string) any {
	out := make([]any, 0, len(v))
	for _, s := range v {
		out = append(out, s)
	}
	return out
}

func mapList(v []map[string]any) any {
	out := make([]any, 0, len(v))
	for _, m := range v {
		out = append(out, m)
	}
	return out
}

// jsonList renders a typed slice as []any so length/index/nonEmpty apply.
func jsonList(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out []any
	if err := json.Unmarshal(b, &out); err != nil {
		return v
	}
	if out == nil {
		out = []any{}
	}
	return out
}

func jsonAny(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ── driver ───────────────────────────────────────────────────────────────────

// runCase runs one case and turns any outcome — including a panic inside the
// SDK — into an error. Python records every exception as a case result; a Go
// panic is the same event and must not take the whole run down with it.
func runCase(ctx context.Context, c specCase, client *nucleus.Client, url string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in the Go SDK: %v (at %s)", r, panicSite())
		}
	}()
	return runCaseSteps(ctx, c, client, url)
}

func runCaseSteps(ctx context.Context, c specCase, client *nucleus.Client, url string) error {
	fixtures := map[string]string{}
	bound := map[string]any{}
	ops := &Ops{c: client, url: url}
	defer ops.cleanup(ctx)

	for i, s := range c.Steps {
		args := make([]any, 0, len(s.Args))
		for _, a := range s.Args {
			r, err := resolve(a, fixtures, bound)
			if err != nil {
				return fmt.Errorf("step %d (%s): %v", i, s.Op, err)
			}
			args = append(args, r)
		}

		stepCtx, cancel := context.WithTimeout(ctx, stepTimeout)
		result, err := ops.call(stepCtx, s.Op, args)
		cancel()
		if err != nil {
			var unsup *errUnsupported
			if ok := asUnsupported(err, &unsup); ok {
				return err
			}
			return fmt.Errorf("step %d (%s): %v", i, s.Op, err)
		}
		if s.Bind != "" {
			bound[s.Bind] = result
		}
		if s.Expect != nil {
			if err := check(result, s.Expect); err != nil {
				return fmt.Errorf("step %d (%s): %v", i, s.Op, err)
			}
		}
	}
	return nil
}

// panicSite names the innermost neutron-go frame in the panicking stack, so a
// crash inside the SDK is reported with the file and line that crashed rather
// than only the message.
func panicSite() string {
	var pcs [32]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.Contains(f.File, "/go/nucleus/") || strings.Contains(f.Function, "neutron-go") {
			return fmt.Sprintf("%s:%d in %s", filepath.Base(f.File), f.Line, f.Function)
		}
		if !more {
			break
		}
	}
	return "unknown frame"
}

func asUnsupported(err error, target **errUnsupported) bool {
	u, ok := err.(*errUnsupported)
	if ok {
		*target = u
	}
	return ok
}

// hereDir is this executor's own source directory, so the spec is found the
// same way whatever the working directory is.
func hereDir() string {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Dir(self)
}

func main() {
	os.Exit(run())
}

func run() int {
	url := os.Getenv("NEUTRON_TEST_DATABASE_URL")
	if url == "" {
		fmt.Fprintln(os.Stderr, "::error::NEUTRON_TEST_DATABASE_URL is not set. This suite is only "+
			"meaningful against a live engine; refusing to report a green run for zero executed cases.")
		return 1
	}

	here := hereDir()
	raw, err := os.ReadFile(filepath.Join(here, "..", "..", "spec.json"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "::error::cannot read spec: %v\n", err)
		return 1
	}
	var sp spec
	if err := json.Unmarshal(raw, &sp); err != nil {
		fmt.Fprintf(os.Stderr, "::error::cannot parse spec: %v\n", err)
		return 1
	}

	declared := map[string]string{}
	if b, err := os.ReadFile(filepath.Join(here, "unsupported.json")); err == nil {
		var doc struct {
			Cases map[string]string `json:"cases"`
		}
		if err := json.Unmarshal(b, &doc); err != nil {
			fmt.Fprintf(os.Stderr, "::error::cannot parse unsupported.json: %v\n", err)
			return 1
		}
		declared = doc.Cases
	}

	ctx := context.Background()
	client, err := nucleus.Connect(ctx, url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "::error::cannot connect to %s: %v\n", url, err)
		return 1
	}
	defer client.Close()

	results := make([]caseResult, 0, len(sp.Cases))
	for _, c := range sp.Cases {
		entry := caseResult{ID: c.ID, Model: c.Model}
		// An xfail may be scoped to named SDKs: some engine defects are only
		// observable through one driver strategy, and without scoping every
		// unaffected SDK reports xpass forever and the signal is lost.
		expectedFail := c.XFail != nil
		if expectedFail && len(c.XFail.SDKs) > 0 {
			expectedFail = false
			for _, s := range c.XFail.SDKs {
				if s == "go" {
					expectedFail = true
				}
			}
		}

		err := runCase(ctx, c, client, url)
		switch {
		case err == nil:
			if expectedFail {
				entry.Status = "xpass"
				entry.Detail = "case is marked xfail but passed — the underlying bug is " +
					"fixed and the xfail note is now false"
			} else {
				entry.Status = "pass"
			}
		default:
			var unsup *errUnsupported
			if asUnsupported(err, &unsup) {
				if reason, ok := declared[c.ID]; ok {
					entry.Status = "unsupported"
					entry.Detail = reason
				} else {
					entry.Status = "fail"
					entry.Detail = fmt.Sprintf("op %s has no mapping and the case is not declared "+
						"unsupported in unsupported.json", unsup.op)
				}
			} else if expectedFail {
				entry.Status = "xfail"
				entry.Detail = err.Error()
			} else {
				entry.Status = "fail"
				entry.Detail = err.Error()
			}
		}
		results = append(results, entry)
	}

	doc := struct {
		SDK         string       `json:"sdk"`
		SpecVersion int          `json:"specVersion"`
		Cases       []caseResult `json:"cases"`
	}{SDK: "go", SpecVersion: sp.SpecVersion, Cases: results}

	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "::error::cannot encode report: %v\n", err)
		return 1
	}
	fmt.Println(string(out))

	bad := 0
	counts := map[string]int{}
	for _, r := range results {
		counts[r.Status]++
		if r.Status == "fail" || r.Status == "xpass" {
			bad++
			fmt.Fprintf(os.Stderr, "::error::%s: %s — %s\n", r.ID, r.Status, r.Detail)
		}
	}
	summary, _ := json.Marshal(counts)
	fmt.Fprintf(os.Stderr, "go: %s\n", summary)
	if bad > 0 {
		return 1
	}
	return 0
}
