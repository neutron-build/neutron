package studio

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStudioLosslessJSONNative(t *testing.T) {
	url := os.Getenv("NEUTRON_E2E_DATABASE_URL")
	if url == "" {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatal("required native PostgreSQL URL missing")
		}
		t.Skip("native PostgreSQL not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal("native connection failed")
	}
	defer pool.Close()
	query := `SELECT NULL::json AS sql_null, 'null'::jsonb AS document_null,
 '{"number":9007199254740993,"decimal":12345678901234567890.123456789,"t":"int8","v":"not-a-cell"}'::jsonb AS exact_document,
 '1e10000'::jsonb AS huge_number, '{ "duplicate": 1, "duplicate": 2 }'::json AS lexical_json, 9223372036854775807::int8 AS bigint`
	for _, binary := range []bool{false, true} {
		var rows pgx.Rows
		if binary {
			rows, err = pool.Query(ctx, query, pgx.QueryResultFormatsByOID{oidJSONB: pgx.BinaryFormatCode})
		} else {
			rows, err = pool.Query(ctx, query, pgx.QueryExecModeSimpleProtocol)
		}
		if err != nil {
			t.Fatal("native fixture query failed")
		}
		result, collectErr := collectTaggedRows(rows)
		rows.Close()
		if collectErr != nil {
			t.Fatal("lossless document collection failed")
		}
		if len(result.data) != 1 {
			t.Fatal("native fixture cardinality differs")
		}
		row := result.data[0]
		if row[0] != nil {
			t.Fatal("SQL NULL became a document")
		}
		nullDoc, ok := row[1].(taggedCell)
		if !ok || nullDoc.T != "jsonb" || nullDoc.V != "null" {
			t.Fatal("JSON null collapsed into SQL NULL")
		}
		exact, ok := row[2].(taggedCell)
		if !ok || !strings.Contains(exact.V, "9007199254740993") || !strings.Contains(exact.V, "12345678901234567890.123456789") {
			t.Fatal("JSON precision changed")
		}
		huge, ok := row[3].(taggedCell)
		if !ok || len(huge.V) != 10001 || huge.V[0] != '1' || strings.Trim(huge.V[1:], "0") != "" {
			t.Fatal("large valid JSON number changed")
		}
		lexical, ok := row[4].(taggedCell)
		if !ok || lexical.T != "json" || lexical.V != `{ "duplicate": 1, "duplicate": 2 }` {
			t.Fatal("JSON lexical document changed")
		}
		big, ok := row[5].(taggedCell)
		if !ok || big.T != "int8" || big.V != "9223372036854775807" {
			t.Fatal("mixed scalar decoding changed")
		}
		encoded, err := json.Marshal(map[string]any{"columns": result.columns, "rows": result.data})
		if err != nil || !json.Valid(encoded) || !strings.Contains(string(encoded), `"t":"jsonb"`) || !strings.Contains(string(encoded), "9007199254740993") {
			t.Fatal("transport encoding failed")
		}
		var native string
		if pool.QueryRow(ctx, `SELECT '{"number":9007199254740993,"decimal":12345678901234567890.123456789,"t":"int8","v":"not-a-cell"}'::jsonb::text`).Scan(&native) != nil || native != exact.V {
			t.Fatal("independent native SQL text differs")
		}
	}
}
