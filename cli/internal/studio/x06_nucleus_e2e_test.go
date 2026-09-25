package studio

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/neutron-build/neutron/cli/internal/db"
	"github.com/neutron-build/neutron/cli/internal/inspect"
)

// TestStudioX06JourneyNucleusE2E is the Nucleus leg of V18 for X06: the
// same application journey across models on a REAL engine — SQL rows, a
// graph node bound to one of them, the CDC events their INSERTs emitted —
// and the limits Studio shows for each stage. It runs against a disposable
// engine named by NEUTRON_E2E_NUCLEUS_URL (never a shared one: it writes
// graph nodes, which have no namespace). Skipped when unset;
// NEUTRON_LIVE_REQUIRED=1 fails instead.
func TestStudioX06JourneyNucleusE2E(t *testing.T) {
	nurl := os.Getenv("NEUTRON_E2E_NUCLEUS_URL")
	if nurl == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("NEUTRON_LIVE_REQUIRED=1 but NEUTRON_E2E_NUCLEUS_URL is not set")
		}
		t.Skip("NEUTRON_E2E_NUCLEUS_URL not set; X06 Nucleus journey skipped")
	}
	ctx := context.Background()
	oracle, err := db.Connect(ctx, nurl)
	if err != nil {
		t.Fatal(err)
	}
	defer oracle.Close()
	client, err := db.Connect(ctx, nurl)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	table := fmt.Sprintf("x06_orders_%d", time.Now().UnixNano()%1_000_000_000)
	for _, stmt := range []string{
		fmt.Sprintf(`CREATE TABLE %s (id int PRIMARY KEY, total text)`, table),
		fmt.Sprintf(`INSERT INTO %s VALUES (1, '9.50'), (2, '12.00')`, table),
		fmt.Sprintf(`INSERT INTO %s VALUES (3, '4.25')`, table),
		fmt.Sprintf(`UPDATE %s SET total = '5.00' WHERE id = 3`, table),
	} {
		if err := oracle.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed %q: %v", stmt, err)
		}
	}
	t.Cleanup(func() { _ = oracle.Exec(context.Background(), "DROP TABLE IF EXISTS "+table) })
	var nodeID int64
	props, _ := json.Marshal(map[string]any{"sqlref_table": table, "sqlref_row": 2})
	if err := oracle.QueryRow(ctx, "SELECT GRAPH_ADD_NODE($1, $2)", "Order", string(props)).Scan(&nodeID); err != nil {
		t.Fatalf("bind graph node: %v", err)
	}
	t.Cleanup(func() { _ = oracle.Exec(context.Background(), "SELECT GRAPH_DELETE_NODE($1)", nodeID) })
	// A node stamped for the same table name in ANOTHER schema is not this
	// table's (X02 binding rule) and must not be attributed to it.
	var foreignID int64
	foreign, _ := json.Marshal(map[string]any{"sqlref_table": table, "sqlref_row": 1, "sqlref_schema": "elsewhere"})
	if err := oracle.QueryRow(ctx, "SELECT GRAPH_ADD_NODE($1, $2)", "Order", string(foreign)).Scan(&foreignID); err != nil {
		t.Fatalf("foreign node: %v", err)
	}
	t.Cleanup(func() { _ = oracle.Exec(context.Background(), "SELECT GRAPH_DELETE_NODE($1)", foreignID) })

	store, err := newConnectionStore()
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{sessionToken: "x06-n", store: store, clients: map[string]*db.Client{"n": client},
		saved: &savedQueryStore{path: t.TempDir() + "/saved.json"}}
	mux, err := s.routes()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.corsMiddleware(mux))
	defer ts.Close()
	s.port = ts.Listener.Addr().(*net.TCPAddr).Port

	var nodesBefore int64
	if err := oracle.QueryRow(ctx, "SELECT GRAPH_NODE_COUNT()").Scan(&nodesBefore); err != nil {
		t.Fatal(err)
	}

	// The application's query, run through Studio's editor endpoint.
	appQuery := fmt.Sprintf("SELECT id, total FROM %s WHERE id > 1", table)
	qb, _ := json.Marshal(map[string]any{"sql": appQuery, "connectionId": "n"})
	qreq, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/query", strings.NewReader(string(qb)))
	qreq.Header.Set("Content-Type", "application/json")
	qreq.Header.Set("X-Studio-Session", "x06-n")
	qres, err := http.DefaultClient.Do(qreq)
	if err != nil {
		t.Fatal(err)
	}
	qres.Body.Close()
	if qres.StatusCode != 200 {
		t.Fatalf("app query status %d", qres.StatusCode)
	}

	res, err := http.Get(ts.URL + "/api/inspect/journey?connectionId=n&schema=public&table=" + table)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("journey status %d", res.StatusCode)
	}
	var j struct {
		Engine inspect.Engine `json:"engine"`
		Limits inspect.Report `json:"limits"`
		Stages []struct {
			Stage  string          `json:"stage"`
			Model  string          `json:"model"`
			Status string          `json:"status"`
			Reason string          `json:"reason"`
			Data   json.RawMessage `json:"data"`
		} `json:"stages"`
	}
	if err := json.NewDecoder(res.Body).Decode(&j); err != nil {
		t.Fatal(err)
	}
	if j.Engine.Product != "nucleus" {
		t.Fatalf("engine %+v", j.Engine)
	}
	t.Logf("engine %s %s (measured build current=%v)", j.Engine.Product, j.Engine.Version, j.Limits.Current)
	stages := map[string]int{}
	for i, st := range j.Stages {
		stages[st.Stage] = i
		t.Logf("stage %-14s model %-5s %-11s %s", st.Stage, st.Model, st.Status, st.Reason)
	}

	// Stages this engine cannot back say so; nothing is faked.
	for _, name := range []string{inspect.StageSchema, inspect.StageMigrations, inspect.StagePlan} {
		if st := j.Stages[stages[name]]; st.Status != inspect.StageUnavailable || st.Reason == "" {
			t.Errorf("%s on Nucleus: %+v", name, st)
		}
	}

	var queries inspect.QueriesData
	if err := json.Unmarshal(j.Stages[stages[inspect.StageQueries]].Data, &queries); err != nil {
		t.Fatal(err)
	}
	if len(queries.Entries) != 1 || queries.Entries[0].SQL != appQuery {
		t.Fatalf("queries %+v", queries.Entries)
	}

	var rows inspect.RowsData
	if err := json.Unmarshal(j.Stages[stages[inspect.StageRows]].Data, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows.Rows) != 3 || rows.IDColumn != "id" {
		t.Fatalf("rows %+v", rows)
	}

	var models inspect.ModelsData
	st := j.Stages[stages[inspect.StageModels]]
	if st.Status != inspect.StageAvailable {
		t.Fatalf("models stage %+v", st)
	}
	if err := json.Unmarshal(st.Data, &models); err != nil {
		t.Fatal(err)
	}
	if len(models.GraphNodes) != 1 || models.GraphNodes[0].NodeID != fmt.Sprint(nodeID) || !models.GraphNodes[0].InSample {
		t.Fatalf("bound nodes %+v (node %d)", models.GraphNodes, nodeID)
	}

	var events inspect.ChangeEventsData
	st = j.Stages[stages[inspect.StageChangeEvents]]
	if err := json.Unmarshal(st.Data, &events); err != nil {
		t.Fatal(err)
	}
	// Oracle: CDC_TABLE_READ directly. Two INSERT statements -> two events;
	// the UPDATE emits none on the measured build (X05 cdc.delivery_shape).
	var raw string
	if err := oracle.QueryRow(ctx, "SELECT CDC_TABLE_READ($1, 0, 100000)", table).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var direct []inspect.ChangeEvent
	if err := json.Unmarshal([]byte(raw), &direct); err != nil {
		t.Fatal(err)
	}
	if len(events.Events) != len(direct) || len(direct) == 0 {
		t.Fatalf("journey events %+v, oracle %+v", events.Events, direct)
	}
	for _, e := range events.Events {
		if e.Table != table {
			t.Fatalf("foreign event %+v", e)
		}
	}
	t.Logf("change events for %s: %d (oracle %d); changes %v", table, len(events.Events), len(direct), changeKinds(direct))

	// Limits for every stage's model are the measured ones — and none of
	// them claims atomicity.
	if !j.Limits.Current {
		t.Fatalf("connected build is not the measured one: %s", j.Limits.CurrentNote)
	}
	for _, st := range j.Stages {
		ml, ok := j.Limits.Model(st.Model)
		if !ok {
			t.Fatalf("stage %s model %s missing from limits", st.Stage, st.Model)
		}
		if ml.Transaction == inspect.TxAtomic {
			t.Fatalf("%s claims atomic transactions on Nucleus", ml.Model)
		}
	}
	sqlL, _ := j.Limits.Model("sql")
	cdcL, _ := j.Limits.Model("cdc")
	graphL, _ := j.Limits.Model("graph")
	if sqlL.Transaction != inspect.TxPartial || cdcL.Transaction != inspect.TxNone || graphL.AtomicWithSQL != inspect.Unsupported {
		t.Fatalf("limits sql=%s cdc=%s graph atomicWithSql=%s", sqlL.Transaction, cdcL.Transaction, graphL.AtomicWithSQL)
	}

	// The journey wrote nothing.
	var nodesAfter, rowCount int64
	_ = oracle.QueryRow(ctx, "SELECT GRAPH_NODE_COUNT()").Scan(&nodesAfter)
	_ = oracle.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&rowCount)
	if nodesAfter != nodesBefore || rowCount != 3 {
		t.Fatalf("journey wrote: nodes %d -> %d, rows %d", nodesBefore, nodesAfter, rowCount)
	}
}

func changeKinds(events []inspect.ChangeEvent) string {
	var kinds []string
	for _, e := range events {
		kinds = append(kinds, e.Change)
	}
	return strings.Join(kinds, ",")
}
