package studio

import (
	"net/http"

	"github.com/neutron-build/neutron/cli/internal/inspect"
)

// X06 cross-model inspection: per-model limits for the connected engine and
// the schema -> migration -> query -> plan -> row -> model -> change-event
// journey for one table. Both endpoints are GET and read-only; navigation
// into the modules that act happens client-side.

// SetMigrationsDir names the application's migrations directory for the
// journey's migrations stage ("" = history only).
func (s *Server) SetMigrationsDir(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.migrationsDir = dir
}

func (s *Server) migrationsDirectory() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.migrationsDir
}

// GET /api/inspect/limits?connectionId=
func (s *Server) handleInspectLimits(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	client, ok := s.clientFor(r.URL.Query().Get("connectionId"))
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	engine, err := inspect.DetectEngine(r.Context(), client)
	if err != nil {
		writeError(w, http.StatusBadGateway, "identify engine: "+sanitizeError(err))
		return
	}
	var live *inspect.LiveSettings
	if engine.Product == "postgres" {
		live = inspect.ReadLiveSettings(r.Context(), client)
	}
	writeJSON(w, http.StatusOK, inspect.BuildReport(engine, live))
}

// statementScope describes what the queries stage can know.
const statementScope = "statements this Studio process executed on this connection (editor and table reads); bound parameters are never recorded; other clients' statements are not visible here"

// GET /api/inspect/journey?connectionId=&schema=&table=
func (s *Server) handleInspectJourney(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	connID, schemaName, tableName := q.Get("connectionId"), q.Get("schema"), q.Get("table")
	if connID == "" || schemaName == "" || tableName == "" {
		writeError(w, http.StatusBadRequest, "connectionId, schema and table are required")
		return
	}
	client, ok := s.clientFor(connID)
	if !ok {
		writeError(w, http.StatusBadRequest, "not connected")
		return
	}
	logged := s.statementLog().snapshot(connID, s.connectionEpoch(connID), 0, 0)
	queries := make([]inspect.QueryRecord, 0, len(logged))
	for _, e := range logged {
		queries = append(queries, inspect.QueryRecord{
			At: e.At, Surface: e.Surface, SQL: e.SQL, DurationMs: e.DurationMs, State: e.State, RowCount: e.RowCount,
		})
	}
	j, err := inspect.BuildJourney(r.Context(), client, inspect.JourneyOptions{
		Schema:        schemaName,
		Table:         tableName,
		MigrationsDir: s.migrationsDirectory(),
		SampleRows:    parseInt(q.Get("sample"), 5),
		EventLimit:    parseInt(q.Get("limit"), 20),
		Queries:       queries,
		QueriesReason: statementScope,
	})
	if err != nil {
		writeError(w, http.StatusBadGateway, sanitizeError(err))
		return
	}
	writeJSON(w, http.StatusOK, j)
}
