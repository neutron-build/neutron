package studio

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// S02 offline unit coverage: the outcome store's deduplication, retention
// and honest-unknown semantics; canonical payload hashing; per-kind
// operation shape validation and batch limits. Live behavior (atomicity,
// replay-without-reexecution, preview/revert round-trips) is pinned by
// commit_v2_e2e_test.go against a real disposable Postgres.

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }

func TestOutcomeStoreDedupAndReplay(t *testing.T) {
	store := newOutcomeStore(8, time.Hour, time.Minute)
	key := outcomeKey("c1", "op-1")

	// Claim, then record a terminal outcome.
	resv := store.reserve(key, "c1", "hash-a")
	if resv.kind != "claimed" {
		t.Fatalf("first reserve = %q, want claimed", resv.kind)
	}
	store.finish(key, func(rec *outcomeRecord) {
		rec.State, rec.Status = outcomeCommitted, 200
		rec.Response = map[string]any{"operationId": "op-1", "rowsAffected": json.Number("1")}
	})

	// Same ID + same payload replays the recorded outcome.
	resv = store.reserve(key, "c1", "hash-a")
	if resv.kind != "replay" || resv.rec.State != outcomeCommitted {
		t.Fatalf("same-payload reserve = %+v, want replay of committed", resv)
	}

	// Same ID + different payload is a conflict, never a re-execution.
	resv = store.reserve(key, "c1", "hash-b")
	if resv.kind != "conflict" {
		t.Fatalf("different-payload reserve = %q, want conflict", resv.kind)
	}

	// Operation IDs are bound to their connection: another connection's
	// key is a different reservation entirely.
	other := store.reserve(outcomeKey("c2", "op-1"), "c2", "hash-a")
	if other.kind != "claimed" {
		t.Fatalf("other-connection reserve = %q, want claimed", other.kind)
	}
}

func TestOutcomeStoreInProgress(t *testing.T) {
	store := newOutcomeStore(8, time.Hour, time.Minute)
	key := outcomeKey("c1", "op-2")

	if resv := store.reserve(key, "c1", "h1"); resv.kind != "claimed" {
		t.Fatalf("first reserve = %q", resv.kind)
	}
	// A concurrent duplicate gets an explicit retryable in-progress state.
	if resv := store.reserve(key, "c1", "h1"); resv.kind != "in_progress" {
		t.Fatalf("concurrent duplicate = %q, want in_progress", resv.kind)
	}

	// An abandoned reservation (handler died) may be taken over once stale.
	store2 := newOutcomeStore(8, time.Hour, 10*time.Millisecond)
	if resv := store2.reserve(key, "c1", "h1"); resv.kind != "claimed" {
		t.Fatalf("store2 first reserve = %q", resv.kind)
	}
	time.Sleep(20 * time.Millisecond)
	if resv := store2.reserve(key, "c1", "h2"); resv.kind != "claimed" {
		t.Fatalf("stale reservation takeover = %q, want claimed", resv.kind)
	}
}

func TestOutcomeStoreEvictionReportsUnknown(t *testing.T) {
	store := newOutcomeStore(2, time.Hour, time.Minute)
	keys := []string{outcomeKey("c1", "a"), outcomeKey("c1", "b"), outcomeKey("c1", "c")}
	for i, k := range keys {
		if resv := store.reserve(k, "c1", "h"); resv.kind != "claimed" {
			t.Fatalf("reserve %d = %q", i, resv.kind)
		}
		store.finish(k, func(rec *outcomeRecord) { rec.State, rec.Status = outcomeCommitted, 200 })
	}
	// The capacity-2 store evicted the oldest record; its tombstone makes
	// the next use of that ID an honest unknown instead of a silent re-run.
	if resv := store.reserve(keys[0], "c1", "h"); resv.kind != "unknown" {
		t.Fatalf("evicted reserve = %q, want unknown", resv.kind)
	}
	if _, state := store.lookup(keys[0]); state != outcomeUnknown {
		t.Fatalf("evicted lookup = %q, want unknown", state)
	}
	if _, state := store.lookup(keys[2]); state != outcomeCommitted {
		t.Fatalf("retained lookup = %q, want committed", state)
	}
}

func TestOutcomeStoreTTLExpiry(t *testing.T) {
	store := newOutcomeStore(8, 30*time.Millisecond, time.Minute)
	key := outcomeKey("c1", "ttl")
	if resv := store.reserve(key, "c1", "h"); resv.kind != "claimed" {
		t.Fatalf("reserve = %q", resv.kind)
	}
	store.finish(key, func(rec *outcomeRecord) { rec.State, rec.Status = outcomeFailed, 400 })
	time.Sleep(50 * time.Millisecond)

	if _, state := store.lookup(key); state != outcomeUnknown {
		t.Fatalf("expired lookup = %q, want unknown", state)
	}
	// A retry of the expired operation is refused as unknown, never
	// silently re-executed and never replayed from a guessed outcome.
	if resv := store.reserve(key, "c1", "h"); resv.kind != "unknown" {
		t.Fatalf("expired reserve = %q, want unknown", resv.kind)
	}
}

func TestOutcomeStoreNeverSeenIsAbsentLookup(t *testing.T) {
	store := newOutcomeStore(4, time.Hour, time.Minute)
	if _, state := store.lookup(outcomeKey("c1", "never")); state != "absent" {
		t.Fatalf("never-seen lookup = %q, want absent", state)
	}
}

func TestCanonicalCommitHashDeterministic(t *testing.T) {
	// The same operations decoded from differently ordered / differently
	// spelled JSON must hash identically: the hash binds decoded content.
	a := `{"connectionId":"c1","operations":[{"op":"update","schema":"public","table":"t","binding":"1:2","key":[{"column":"id","value":1}],"version":"9","column":"note","value":"x"}]}`
	b := `{"operations":[{"value":"x","column":"note","version":"9","key":[{"value":1,"column":"id"}],"binding":"1:2","table":"t","schema":"public","op":"update"}],"connectionId":"c1"}`
	var ra, rb commitRequestV2
	for _, raw := range []struct {
		s string
		d *commitRequestV2
	}{{a, &ra}, {b, &rb}} {
		dec := json.NewDecoder(strings.NewReader(raw.s))
		dec.UseNumber()
		if err := dec.Decode(raw.d); err != nil {
			t.Fatalf("decode: %v", err)
		}
	}
	ha, hb := canonicalCommitHash(ra.ConnectionID, ra.Operations), canonicalCommitHash(rb.ConnectionID, rb.Operations)
	if ha == "" || ha != hb {
		t.Fatalf("hashes differ or empty: %q vs %q", ha, hb)
	}

	// A one-character payload difference must change the hash.
	rb.Operations[0].Value = "y"
	if canonicalCommitHash(rb.ConnectionID, rb.Operations) == ha {
		t.Fatal("different payload produced the same hash")
	}

	// Large integers keep their literal digits (json.Number), so int8
	// keys beyond 2^53 hash by their exact value.
	big := `{"connectionId":"c1","operations":[{"op":"insert","schema":"public","table":"t","binding":"1:2","values":{"id":{"t":"int8","v":"9007199254740993"}}}]}`
	var rb2 commitRequestV2
	dec := json.NewDecoder(strings.NewReader(big))
	dec.UseNumber()
	if err := dec.Decode(&rb2); err != nil {
		t.Fatalf("decode big: %v", err)
	}
	h1 := canonicalCommitHash(rb2.ConnectionID, rb2.Operations)
	rb2.Operations[0].Values["id"] = map[string]any{"t": "int8", "v": "9007199254740992"}
	if canonicalCommitHash(rb2.ConnectionID, rb2.Operations) == h1 {
		t.Fatal("adjacent int8 values hashed identically")
	}
}

func TestValidateCommitOpShape(t *testing.T) {
	validUpdate := commitOp{Op: "update", Schema: "s", Table: "t", Binding: "e:1",
		Key: []keyCell{{Column: "id", Value: json.Number("1")}}, Version: strPtr("7"), Column: strPtr("c"), Value: "v"}
	cases := []struct {
		name string
		op   commitOp
		want string // substring of the refusal; "" = valid
	}{
		{"valid update", validUpdate, ""},
		{"valid update to null", func() commitOp {
			op := validUpdate
			op.IsNull, op.Value = boolPtr(true), nil
			return op
		}(), ""},
		{"valid insert", commitOp{Op: "insert", Schema: "s", Table: "t", Binding: "e:1", Values: map[string]any{}}, ""},
		{"valid delete", commitOp{Op: "delete", Schema: "s", Table: "t", Binding: "e:1",
			Key: []keyCell{{Column: "id", Value: 1}}, Version: strPtr("7")}, ""},
		{"unknown kind", func() commitOp { op := validUpdate; op.Op = "upsert"; return op }(), `op must be "insert", "update" or "delete"`},
		{"insert carrying version", func() commitOp {
			op := validUpdate
			op.Op, op.Values = "insert", map[string]any{}
			return op
		}(), "insert carries update/delete-only fields"},
		{"update carrying values", func() commitOp {
			op := validUpdate
			op.Values = map[string]any{"c": 1}
			return op
		}(), "update carries insert-only fields"},
		{"update without value", func() commitOp {
			op := validUpdate
			op.Value = nil
			return op
		}(), "value is required"},
		{"update isNull plus value", func() commitOp {
			op := validUpdate
			op.IsNull = boolPtr(true)
			return op
		}(), "isNull=true must not carry a value"},
		{"delete carrying column", func() commitOp {
			op := validUpdate
			op.Op = "delete"
			return op
		}(), "delete carries update/insert-only fields"},
		{"missing binding", func() commitOp { op := validUpdate; op.Binding = ""; return op }(), "schema, table and binding are required"},
		{"insert without values", commitOp{Op: "insert", Schema: "s", Table: "t", Binding: "e:1"}, "insert requires values"},
		{"non-numeric version", func() commitOp {
			op := validUpdate
			op.Version = strPtr("one")
			return op
		}(), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCommitOpShape(0, tc.op)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("unexpected refusal: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("refusal = %v, want it to contain %q", err, tc.want)
			}
		})
	}
	// The non-numeric version is caught by the batch validator, which has
	// the limit context.
	if err := validateCommitOps([]commitOp{func() commitOp {
		op := validUpdate
		op.Version = strPtr("one")
		return op
	}()}, 10); err == nil || !strings.Contains(err.Error(), "version must be the row version string") {
		t.Fatalf("version refusal = %v", err)
	}
}

func TestValidateCommitOpsLimits(t *testing.T) {
	op := commitOp{Op: "insert", Schema: "s", Table: "t", Binding: "e:1", Values: map[string]any{}}
	if err := validateCommitOps(nil, 100); err == nil || !strings.Contains(err.Error(), "at least one") {
		t.Fatalf("empty batch refusal = %v", err)
	}
	batch := make([]commitOp, 101)
	for i := range batch {
		batch[i] = op
	}
	if err := validateCommitOps(batch, 100); err == nil || !strings.Contains(err.Error(), "limited to 100 operations") {
		t.Fatalf("oversize batch refusal = %v", err)
	}
	batch = batch[:100]
	if err := validateCommitOps(batch, 100); err != nil {
		t.Fatalf("100-operation batch refused: %v", err)
	}
	// The configured limit can only be lower than the coded bound.
	if err := validateCommitOps(batch, 50); err == nil || !strings.Contains(err.Error(), "limited to 50") {
		t.Fatalf("lowered limit refusal = %v", err)
	}
}

func TestValidateOperationID(t *testing.T) {
	if err := validateOperationID(""); err == nil {
		t.Fatal("empty operation ID accepted")
	}
	if err := validateOperationID(string(make([]byte, 129))); err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("long operation ID refusal = %v", err)
	}
	if err := validateOperationID("op\x00id"); err == nil || !strings.Contains(err.Error(), "control characters") {
		t.Fatalf("control-character refusal = %v", err)
	}
	if err := validateOperationID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"); err != nil {
		t.Fatalf("valid UUID refused: %v", err)
	}
}

func TestClampDownward(t *testing.T) {
	if got := clampDownwardInt(150, 100); got != 100 {
		t.Fatalf("raise attempt = %d, want clamp to 100", got)
	}
	if got := clampDownwardInt(0, 100); got != 100 {
		t.Fatalf("unset = %d, want default", got)
	}
	if got := clampDownwardInt(-5, 100); got != 100 {
		t.Fatalf("negative = %d, want default", got)
	}
	if got := clampDownwardInt(42, 100); got != 42 {
		t.Fatalf("lowered = %d, want 42", got)
	}
	if got := clampDownwardInt64(1<<30, 1<<20); got != 1<<20 {
		t.Fatalf("int64 raise attempt = %d", got)
	}
}

func TestWireValueRoundTrips(t *testing.T) {
	// Captured wire values must be re-acceptable by the strict decoder for
	// their column: tagged columns round-trip through tagged cells; int8's
	// exactness survives; values the protocol cannot express are detected
	// (drives honest irreversible refusals, not corrupt reverts).
	int8Col := tableColumnMeta{TypeOID: oidInt8, TypeName: "int8"}
	if !wireValueRoundTrips(int8Col, map[string]any{"t": "int8", "v": "9007199254740993"}) {
		t.Fatal("tagged int8 value must round-trip")
	}
	// wireValueOf is the producer of inverse values: its output must be
	// decoder-acceptable by construction.
	if enc := wireValueOf(int8Col, int64(9007199254740993)); !wireValueRoundTrips(int8Col, enc) {
		t.Fatalf("wireValueOf int8 output does not round-trip: %#v", enc)
	}
	enc := wireValueOf(tableColumnMeta{TypeOID: oidNumeric, TypeName: "numeric"}, nil)
	if enc != nil {
		t.Fatalf("wireValueOf nil = %#v, want nil (SQL NULL)", enc)
	}
	if wireValueRoundTrips(int8Col, json.Number("9007199254740993")) {
		t.Fatal("bare number for a tagged int8 column must NOT round-trip (strict decoder refuses it)")
	}
	textCol := tableColumnMeta{TypeOID: 25, TypeName: "text"}
	if !wireValueRoundTrips(textCol, "plain") {
		t.Fatal("plain text must round-trip")
	}
	arrayCol := tableColumnMeta{TypeOID: 1009, TypeName: "text[]"}
	if wireValueRoundTrips(arrayCol, []any{"a", "b"}) {
		t.Fatal("array values cannot round-trip the strict mutation decoder; they must be reported irreversible")
	}
	jsonCol := tableColumnMeta{TypeOID: oidJSONB, TypeName: "jsonb"}
	if !wireValueRoundTrips(jsonCol, `{"a": 1}`) {
		t.Fatal("jsonb text must round-trip")
	}
}
