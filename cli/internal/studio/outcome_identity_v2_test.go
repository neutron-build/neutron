package studio

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// These tests exercise the production handlers in process; no listener or
// database is started. Transaction-stage classifications use the same terminal
// recording boundary as the handlers, without claiming live SQL acceptance.
func producerOutcomeRequest(t *testing.T, handler http.HandlerFunc, payload any) (int, map[string]any) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/table/v2/contract", strings.NewReader(string(raw)))
	req.Header.Set(sessionHeader, "producer-contract")
	w := httptest.NewRecorder()
	handler(w, req)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid producer JSON: %s: %v", w.Body.String(), err)
	}
	return w.Code, body
}

func producerAssertFailedLookup(t *testing.T, handler http.HandlerFunc, connID, opID string, status int, direct map[string]any) {
	t.Helper()
	code, lookup := producerOutcomeRequest(t, handler, outcomeRequestV2{connID, opID})
	if code != http.StatusOK || lookup["operationId"] != opID || lookup["state"] != outcomeFailed || lookup["status"] != float64(status) {
		t.Fatalf("invalid failed envelope: status=%d body=%#v", code, lookup)
	}
	nested, ok := lookup["response"].(map[string]any)
	if !ok || nested["operationId"] != opID || direct["operationId"] != opID || !reflect.DeepEqual(nested, direct) {
		t.Fatalf("direct/lookup identity or body differs: direct=%#v lookup=%#v", direct, lookup)
	}
	for _, private := range []string{"PayloadHash", "payloadHash", "Inverse", "inverse", "connectionId"} {
		if _, ok := lookup[private]; ok {
			t.Fatalf("private field leaked: %s", private)
		}
		if _, ok := nested[private]; ok {
			t.Fatalf("private response field leaked: %s", private)
		}
	}
}

func TestProducerOutcomePreparationReplayAndLookup(t *testing.T) {
	op := commitOp{Op: "insert", Schema: "public", Table: "items", Binding: "1:2", Values: map[string]any{"note": "original"}}
	for _, kind := range []string{"commit", "revert", "import"} {
		t.Run(kind, func(t *testing.T) {
			s := &Server{sessionToken: "producer-contract"}
			opID := "submitted-" + kind
			var mutate, lookup http.HandlerFunc
			var payload any
			switch kind {
			case "commit":
				mutate, lookup = s.handleTableCommitV2, s.handleTableOutcomeV2
				payload = commitRequestV2{"offline", opID, []commitOp{op}}
			case "revert":
				mutate, lookup = s.handleTableRevertV2, s.handleTableOutcomeV2
				store := s.outcomeRecords()
				key := outcomeKey("offline", "original-commit")
				store.reserve(key, "offline", "original-hash")
				store.finish(key, func(rec *outcomeRecord) {
					rec.State, rec.Status, rec.Response = outcomeCommitted, http.StatusOK, commitResponseBody("original-commit", nil, true, "")
					rec.Reversible, rec.Inverse = true, []commitOp{op}
				})
				payload = revertRequestV2{"offline", "original-commit", opID}
			case "import":
				mutate, lookup = s.handleTableImportBatchV2, s.handleTableImportOutcomeV2
				payload = importBatchRequestV2{"offline", opID, "public", "items", "1:2", []map[string]any{op.Values}}
			}
			status, direct := producerOutcomeRequest(t, mutate, payload)
			if status != http.StatusBadRequest || direct["error"] == nil {
				t.Fatalf("preparation result: %d %#v", status, direct)
			}
			producerAssertFailedLookup(t, lookup, "offline", opID, status, direct)
			replayStatus, replay := producerOutcomeRequest(t, mutate, payload)
			if replayStatus != status || replay["replayed"] != true || replay["operationId"] != opID {
				t.Fatalf("replay=%d %#v", replayStatus, replay)
			}
			delete(replay, "replayed")
			if !reflect.DeepEqual(direct, replay) {
				t.Fatalf("failure replay changed: %#v %#v", direct, replay)
			}
			// A changed payload under this ID is refused before preparation.
			changed := op
			changed.Values = map[string]any{"note": "changed"}
			switch p := payload.(type) {
			case commitRequestV2:
				p.Operations = []commitOp{changed}
				payload = p
			case importBatchRequestV2:
				p.Rows = []map[string]any{changed.Values}
				payload = p
			case revertRequestV2:
				s.outcomeRecords().finish(outcomeKey("offline", p.OperationID), func(rec *outcomeRecord) { rec.Inverse = []commitOp{changed} })
			}
			conflictStatus, conflict := producerOutcomeRequest(t, mutate, payload)
			if conflictStatus != http.StatusConflict || conflict["state"] != "operation_conflict" {
				t.Fatalf("changed payload admitted: %d %#v", conflictStatus, conflict)
			}
			// A same-ID request on another connection cannot observe this failure.
			_, other := producerOutcomeRequest(t, lookup, outcomeRequestV2{"other", opID})
			if other["state"] != outcomeUnknown || other["response"] != nil {
				t.Fatalf("cross-connection evidence: %#v", other)
			}
		})
	}
}

func TestProducerOutcomeTerminalClassificationIdentity(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		state  string
	}{
		{"domain", mutationDomainError{msg: "invalid column"}, 400, ""},
		{"not-connected", errNotConnected{}, 400, ""},
		{"binding", rowStateError{state: "binding", msg: "relation changed"}, 409, "binding"},
		{"conflict", rowStateError{state: "conflict", msg: "row changed", currentVersion: "99"}, 409, "conflict"},
		{"missing", rowStateError{state: "missing", msg: "row absent"}, 409, "missing"},
		{"input", &pgconn.PgError{Code: "22P02", Message: "invalid input"}, 400, ""},
		{"constraint", &pgconn.PgError{Code: "23505", Message: "duplicate"}, 409, "constraint"},
		{"privilege", &pgconn.PgError{Code: "42501", Message: "denied"}, 403, "privilege"},
		{"introspection", errIntrospection{errors.New("catalog postgres://user:private-password@host/db")}, 502, ""},
		{"execution", errors.New("execution postgres://user:private-password@host/db"), 502, ""},
	}
	for _, tc := range cases {
		for _, stage := range []string{"commit prepare", "commit execution", "revert prepare", "revert execution", "import"} {
			t.Run(tc.name+"/"+stage, func(t *testing.T) {
				s := &Server{sessionToken: "producer-contract"}
				store, lookup := s.outcomeRecords(), http.HandlerFunc(s.handleTableOutcomeV2)
				if stage == "import" {
					store, lookup = s.importOutcomeRecords(), s.handleTableImportOutcomeV2
				}
				opID := "requested-operation"
				key := outcomeKey("c", opID)
				store.reserve(key, "c", "private-payload-hash")
				var status int
				var out map[string]any
				if strings.HasSuffix(stage, "prepare") {
					status, out = finishFailed(store, key, tc.err, stage)
				} else {
					if stage == "import" {
						status, out = importFailureBody(fmtOpError(3, tc.err), 3)
					} else {
						status, out = classifyMutationOutcome(tc.err, stage)
					}
					// A loose body is never the authority for operation ownership.
					out["operationId"] = "unrelated-body-id"
					store.finish(key, func(rec *outcomeRecord) { rec.State, rec.Status, rec.Response = outcomeFailed, status, out })
				}
				// Import preserves its existing wrapped not-connected classification.
				wantStatus := tc.status
				if stage == "import" && tc.name == "not-connected" {
					wantStatus = http.StatusBadGateway
				}
				if status != wantStatus || out["operationId"] != opID || (tc.state != "" && out["state"] != tc.state) {
					t.Fatalf("classification changed: %d %#v", status, out)
				}
				if tc.name == "conflict" && out["currentVersion"] != "99" {
					t.Fatalf("version missing: %#v", out)
				}
				if stage == "import" && (out["applied"] != 0 || out["failedRow"] != 3) {
					t.Fatalf("import atomic failure changed: %#v", out)
				}
				raw, _ := json.Marshal(out)
				if strings.Contains(string(raw), "private-password") {
					t.Fatalf("credential leaked: %s", raw)
				}
				_, direct := producerOutcomeRequest(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, status, out) }, outcomeRequestV2{"c", opID})
				producerAssertFailedLookup(t, lookup, "c", opID, status, direct)
				if store.reserve(key, "c", "private-payload-hash").kind != "replay" || store.reserve(key, "c", "changed").kind != "conflict" {
					t.Fatal("terminal idempotency changed")
				}
			})
		}
	}
}

func TestProducerOutcomeOtherTerminalRecords(t *testing.T) {
	// Begin/FK refusals and ambiguous commits retain their original states and
	// statuses while passing through the shared identity boundary.
	for _, tc := range []struct {
		name, state, bodyState string
		status                 int
	}{
		{"commit begin", outcomeFailed, "unknown", 502},
		{"revert begin", outcomeFailed, "", 502},
		{"revert FK probe", outcomeFailed, "irreversible", 409},
		{"revert FK side effect", outcomeFailed, "irreversible", 409},
		{"commit ambiguous", outcomeUnknown, "unknown", 502},
		{"revert ambiguous", outcomeUnknown, "unknown", 502},
		{"import ambiguous", outcomeUnknown, "unknown", 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Server{sessionToken: "producer-contract"}
			store, lookup := s.outcomeRecords(), http.HandlerFunc(s.handleTableOutcomeV2)
			if strings.HasPrefix(tc.name, "import") {
				store, lookup = s.importOutcomeRecords(), s.handleTableImportOutcomeV2
			}
			key := outcomeKey("c", "expected")
			store.reserve(key, "c", "h")
			out := map[string]any{"error": "unchanged refusal"}
			if tc.bodyState != "" {
				out["state"] = tc.bodyState
			}
			store.finish(key, func(rec *outcomeRecord) { rec.State, rec.Status, rec.Response = tc.state, tc.status, out })
			if out["operationId"] != "expected" {
				t.Fatalf("direct identity missing: %#v", out)
			}
			rec, state := store.lookup(key)
			if state != tc.state || rec.Status != tc.status || !reflect.DeepEqual(copyResponseBody(rec), out) {
				t.Fatalf("terminal record changed: %#v %s", rec, state)
			}
			if tc.state == outcomeFailed {
				_, direct := producerOutcomeRequest(t, func(w http.ResponseWriter, r *http.Request) { writeJSON(w, tc.status, out) }, nil)
				producerAssertFailedLookup(t, lookup, "c", "expected", tc.status, direct)
			} else {
				_, body := producerOutcomeRequest(t, lookup, outcomeRequestV2{"c", "expected"})
				if body["state"] != outcomeUnknown || body["operationId"] != "expected" || body["response"] != nil {
					t.Fatalf("ambiguous lookup fabricated evidence: %#v", body)
				}
			}
		})
	}
}

func TestProducerOutcomeLegacyAndMismatchedOwnership(t *testing.T) {
	for _, kind := range []string{"commit", "import"} {
		for _, corruption := range []string{"missing-body-id", "wrong-body-id", "wrong-record-id", "wrong-record-connection"} {
			t.Run(kind+"/"+corruption, func(t *testing.T) {
				s := &Server{sessionToken: "producer-contract"}
				store, lookup := s.outcomeRecords(), http.HandlerFunc(s.handleTableOutcomeV2)
				if kind == "import" {
					store, lookup = s.importOutcomeRecords(), s.handleTableImportOutcomeV2
				}
				key := outcomeKey("c", "expected")
				rec := store.reserve(key, "c", "h").rec
				// Deliberately seed legacy/corrupt records bypassing finish.
				rec.State, rec.Status, rec.Response = outcomeFailed, 409, map[string]any{"error": "original", "state": "constraint"}
				switch corruption {
				case "wrong-body-id":
					rec.Response["operationId"] = "unrelated-body"
				case "wrong-record-id":
					rec.OperationID = "unrelated-operation"
				case "wrong-record-connection":
					rec.ConnectionID = "unrelated-connection"
				}
				_, body := producerOutcomeRequest(t, lookup, outcomeRequestV2{"c", "expected"})
				if strings.HasPrefix(corruption, "wrong-record") {
					if body["state"] != outcomeUnknown || body["response"] != nil || store.reserve(key, "c", "h").kind != "unknown" {
						t.Fatalf("forged matching evidence from unrelated record: %#v", body)
					}
					changed := false
					store.finish(key, func(*outcomeRecord) { changed = true })
					if changed {
						t.Fatal("unrelated record was mutated")
					}
				} else {
					nested := body["response"].(map[string]any)
					if body["operationId"] != "expected" || nested["operationId"] != "expected" || nested["error"] != "original" {
						t.Fatalf("legacy identity not bound: %#v", body)
					}
					if store.reserve(key, "c", "h").kind != "replay" {
						t.Fatal("owned legacy record no longer replays")
					}
					if corruption == "missing-body-id" && rec.Response["operationId"] != nil {
						t.Fatal("lookup mutated legacy record")
					}
				}
			})
		}
	}
}

func TestProducerOutcomeLookupUnknownAndInProgress(t *testing.T) {
	s := &Server{sessionToken: "producer-contract"}
	store := newOutcomeStore(2, time.Hour, time.Minute)
	s.outcomes = store
	key := outcomeKey("c", "expected")
	for _, state := range []string{"absent", outcomeInProgress, "expired"} {
		if state == outcomeInProgress {
			store.reserve(key, "c", "h")
		}
		if state == "expired" {
			store.finish(key, func(rec *outcomeRecord) { rec.State, rec.Status = outcomeFailed, 400 })
			store.records[key].CreatedAt = time.Now().Add(-2 * time.Hour)
		}
		_, body := producerOutcomeRequest(t, s.handleTableOutcomeV2, outcomeRequestV2{"c", "expected"})
		want := outcomeUnknown
		if state == outcomeInProgress {
			want = outcomeInProgress
		}
		if body["operationId"] != "expected" || body["state"] != want || body["response"] != nil {
			t.Fatalf("lookup %s invented a terminal response: %#v", state, body)
		}
	}
	if store.reserve(key, "c", "h").kind != "unknown" {
		t.Fatal("expired failure became repeatable")
	}
}

func TestProducerOutcomeLookupAdmissionFailure(t *testing.T) {
	s := &Server{sessionToken: "producer-contract"}
	key := outcomeKey("c", "expected")
	s.outcomeRecords().reserve(key, "c", "h")
	for _, handler := range []http.HandlerFunc{s.handleTableOutcomeV2, s.handleTableImportOutcomeV2} {
		req := httptest.NewRequest(http.MethodPost, "/api/table/v2/outcome", strings.NewReader(`{"connectionId":"c","operationId":"expected"}`))
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusForbidden {
			t.Fatalf("unauthenticated lookup=%d", w.Code)
		}
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body["response"] != nil || body["operationId"] != nil {
			t.Fatalf("lookup refusal fabricated terminal evidence: %#v", body)
		}
	}
	if _, state := s.outcomeRecords().lookup(key); state != outcomeInProgress {
		t.Fatalf("lookup failure changed operation: %s", state)
	}
}
