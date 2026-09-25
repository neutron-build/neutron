package studio

import (
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
)

// Batched CSV/JSON import (S06), served under /api/table/v2/import/*.
//
// The browser parses and maps the file (streaming, bounded memory) and sends
// it here as a sequence of batches. Each batch is an explicit atomic unit:
// every row of the batch is inserted inside ONE transaction through the S02
// commit machinery (prepareCommitOps + runCommitOps: fresh catalog
// validation, the read-only/binding gate, the strict per-column wire
// decoder, parameter-bound single-row INSERTs), so a failing row rolls back
// the whole batch and nothing of it is applied. Earlier batches stay
// committed; the import as a whole is NOT atomic, and the client reports
// exactly which batches committed.
//
// Batches carry a client-generated operation ID and are deduplicated like
// commits (same ID + same payload replays the recorded outcome without
// re-inserting; same ID + different payload is operation_conflict; an ID
// whose record expired is unknown and refused). After a dropped response
// the client resolves the batch through /import/outcome before doing
// anything else — this is what makes an interrupted import recoverable
// without duplicating rows. Import outcomes live in their own bounded store
// so a long import never evicts the outcome (and revert) records of the
// editor's own commits.
//
// The endpoint is insert-only by construction (rows are value maps; no key,
// version, update or delete shape exists in the request), bounded by the
// same operation-count and body-size limits as commits, and guarded by the
// S01 session token and exact-origin checks. A failure reports the batch-
// relative row index (failedRow) so the client can name the source row.

type importBatchRequestV2 struct {
	ConnectionID string           `json:"connectionId"`
	OperationID  string           `json:"operationId"`
	Schema       string           `json:"schema"`
	Table        string           `json:"table"`
	Binding      string           `json:"binding"`
	Rows         []map[string]any `json:"rows"`
}

// importOutcomeRecords is the import batches' own outcome store.
func (s *Server) importOutcomeRecords() *outcomeStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.importOutcomes == nil {
		s.importOutcomes = newOutcomeStore(defaultOutcomeCapacity, defaultOutcomeTTL, defaultStaleReservation)
	}
	return s.importOutcomes
}

// importBatchOps turns a batch into insert operations (the commit shape the
// shared validator, preparer and executor take). A nil row is refused —
// {} is the explicit all-DEFAULT row.
func importBatchOps(body importBatchRequestV2) ([]commitOp, error) {
	if body.Schema == "" || body.Table == "" || body.Binding == "" {
		return nil, mutationDomainError{msg: "schema, table and binding are required (the binding from the table metadata read)"}
	}
	ops := make([]commitOp, len(body.Rows))
	for i, row := range body.Rows {
		if row == nil {
			return nil, mutationDomainError{msg: fmt.Sprintf("rows[%d]: a row must be a column map ({} for an all-DEFAULT row)", i)}
		}
		ops[i] = commitOp{Op: "insert", Schema: body.Schema, Table: body.Table, Binding: body.Binding, Values: row}
	}
	return ops, nil
}

var opIndexPrefix = regexp.MustCompile(`^operations\[(\d+)\]`)

// failedOpIndex extracts the batch-relative operation index the preparer
// prefixes its refusals with (fmtOpError), or -1.
func failedOpIndex(err error) int {
	m := opIndexPrefix.FindStringSubmatch(err.Error())
	if m == nil {
		return -1
	}
	i, convErr := strconv.Atoi(m[1])
	if convErr != nil {
		return -1
	}
	return i
}

func importFailureBody(err error, failedRow int) (int, map[string]any) {
	status, out := classifyMutationOutcome(err, "import")
	if out == nil {
		out = map[string]any{}
	}
	if failedRow >= 0 {
		out["failedRow"] = failedRow
	}
	out["applied"] = 0
	return status, out
}

func (s *Server) handleTableImportBatchV2(w http.ResponseWriter, r *http.Request) {
	var body importBatchRequestV2
	if !s.readMutationBody(w, r, &body) {
		return
	}
	if err := validateOperationID(body.OperationID); err != nil {
		writeDomainError(w, err)
		return
	}
	ops, err := importBatchOps(body)
	if err != nil {
		writeDomainError(w, err)
		return
	}
	if err := validateCommitOps(ops, s.commitOpLimit()); err != nil {
		writeDomainError(w, err)
		return
	}

	hash := canonicalCommitHash(body.ConnectionID, ops)
	key := outcomeKey(body.ConnectionID, body.OperationID)
	store := s.importOutcomeRecords()
	switch resv := store.reserve(key, body.ConnectionID, hash); resv.kind {
	case "replay":
		out := copyResponseBody(resv.rec)
		out["replayed"] = true
		writeJSON(w, resv.rec.Status, out)
		return
	case "conflict":
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": "operation_conflict",
			"error": fmt.Sprintf("import batch ID %q was already used with a different payload; batch IDs are single-use — send a new ID for different content", body.OperationID)})
		return
	case "in_progress":
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": "in_progress",
			"error": "this import batch is currently committing (a concurrent duplicate); resolve it through the outcome lookup"})
		return
	case "unknown":
		writeJSON(w, http.StatusConflict, map[string]any{
			"state": "unknown",
			"error": "no retained outcome for this import batch ID (expired, evicted, or the server restarted); it cannot be replayed or safely repeated — check whether its rows exist, then continue with a new batch ID",
		})
		return
	}

	finishFailure := func(status int, out map[string]any) {
		out["operationId"] = body.OperationID
		store.finish(key, func(rec *outcomeRecord) {
			rec.State, rec.Status, rec.Response = outcomeFailed, status, out
		})
		writeJSON(w, status, out)
	}

	prepared, err := s.prepareCommitOps(r.Context(), body.ConnectionID, ops)
	if err != nil {
		finishFailure(importFailureBody(err, failedOpIndex(err)))
		return
	}
	client, _ := s.clientFor(body.ConnectionID)
	tx, err := client.BeginTx(r.Context())
	if err != nil {
		log.Printf("studio: import begin error: %v", err)
		finishFailure(http.StatusBadGateway, map[string]any{"applied": 0,
			"error": "the transaction could not be started; nothing of this batch was applied — " + sanitizeError(err)})
		return
	}
	defer tx.Rollback(r.Context()) //nolint:errcheck

	// One operation at a time inside the single transaction, so an
	// execution-time failure (constraint, type input, privilege) names its
	// exact row; the deferred Rollback discards the whole batch.
	for i := range prepared {
		if _, err := runCommitOps(r.Context(), tx, prepared[i:i+1]); err != nil {
			finishFailure(importFailureBody(fmtOpError(i, err), i))
			return
		}
	}

	if err := tx.Commit(r.Context()); err != nil {
		log.Printf("studio: import commit error: %v", err)
		out := map[string]any{
			"state":       outcomeUnknown,
			"operationId": body.OperationID,
			"error":       "the batch's commit failed after its rows were inserted; whether it persisted cannot be determined — check the table before continuing with a NEW batch ID",
		}
		store.finish(key, func(rec *outcomeRecord) {
			rec.State, rec.Status, rec.Response = outcomeUnknown, http.StatusBadGateway, out
		})
		writeJSON(w, http.StatusBadGateway, out)
		return
	}
	out := map[string]any{
		"operationId": body.OperationID,
		"applied":     len(prepared),
	}
	store.finish(key, func(rec *outcomeRecord) {
		rec.State, rec.Status, rec.Response = outcomeCommitted, http.StatusOK, out
	})
	writeJSON(w, http.StatusOK, out)
}

// handleTableImportOutcomeV2 resolves an import batch ID: committed,
// failed (nothing applied), in_progress, or unknown (never seen, expired,
// evicted, server restarted — never a guess).
func (s *Server) handleTableImportOutcomeV2(w http.ResponseWriter, r *http.Request) {
	var body outcomeRequestV2
	if !s.readMutationBody(w, r, &body) {
		return
	}
	if err := validateOperationID(body.OperationID); err != nil {
		writeDomainError(w, err)
		return
	}
	rec, state := s.importOutcomeRecords().lookup(outcomeKey(body.ConnectionID, body.OperationID))
	switch state {
	case "absent", outcomeUnknown:
		writeJSON(w, http.StatusOK, map[string]any{
			"operationId": body.OperationID,
			"state":       outcomeUnknown,
			"error":       "no outcome is recorded for this import batch on this connection (never seen, expired, evicted, or the server restarted)",
		})
		return
	case outcomeInProgress:
		writeJSON(w, http.StatusOK, map[string]any{
			"operationId": body.OperationID,
			"state":       outcomeInProgress,
			"error":       "this import batch is currently committing",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"operationId": body.OperationID,
		"state":       rec.State,
		"status":      rec.Status,
		"response":    copyResponseBody(rec),
	})
}
