// The standalone consumer imports the actual archived Go module. It observes
// runner-owned fixtures through orm.Select and never imports the native oracle.
package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/orm"
)

type request struct {
	Protocol  string            `json:"protocol"`
	CaseID    string            `json:"case_id"`
	Action    string            `json:"action"`
	Profile   string            `json:"profile"`
	Schema    string            `json:"schema_scope"`
	Ownership string            `json:"ownership_token"`
	Artifacts map[string]string `json:"artifact_hashes"`
}
type fixture struct {
	ID       int32       `db:"id"`
	Big      int64       `db:"big"`
	Precise  orm.Decimal `db:"precise"`
	Moment   time.Time   `db:"moment"`
	SQLNull  *string     `db:"sql_null,nullable"`
	Document *orm.JSON   `db:"document,nullable"`
}

var scopePattern = regexp.MustCompile(`^neutron_polyglot_[0-9a-f]{32}$`)
var hashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func observe() (map[string]any, bool) {
	failure := map[string]any{"status": "fail", "diagnostics": "Go archived-module scalar adapter refused or failed"}
	var r request
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 4*1024*1024))
	if decoder.Decode(&r) != nil {
		return failure, false
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return failure, false
	}
	if r.Protocol != "polyglot-conformance-v1" || r.CaseID != "scalar-extremes" || (r.Action != "observe" && r.Action != "insert") || r.Profile != "postgres-direct" || !scopePattern.MatchString(r.Schema) || !hashPattern.MatchString(r.Ownership) || len(r.Artifacts) == 0 {
		return failure, false
	}
	for name, digest := range r.Artifacts {
		if name == "" || !hashPattern.MatchString(digest) {
			return failure, false
		}
	}
	url := os.Getenv("NEUTRON_TEST_DATABASE_URL")
	if url == "" {
		return failure, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return failure, false
	}
	config.MaxConns = 1
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return failure, false
	}
	defer pool.Close()
	table, err := orm.NewTable[fixture](r.Schema, "values_fixture")
	if err != nil {
		return failure, false
	}
	id, err := orm.NewColumn[fixture, int32](table, "ID")
	if err != nil {
		return failure, false
	}
	if r.Action == "insert" {
		bigColumn, err := orm.NewColumn[fixture, int64](table, "Big")
		if err != nil {
			return failure, false
		}
		preciseColumn, err := orm.NewColumn[fixture, orm.Decimal](table, "Precise")
		if err != nil {
			return failure, false
		}
		momentColumn, err := orm.NewColumn[fixture, time.Time](table, "Moment")
		if err != nil {
			return failure, false
		}
		sqlNullColumn, err := orm.NewColumn[fixture, *string](table, "SQLNull")
		if err != nil {
			return failure, false
		}
		documentColumn, err := orm.NewColumn[fixture, *orm.JSON](table, "Document")
		if err != nil {
			return failure, false
		}
		precise, err := orm.ParseDecimal("-98765432109876543210.000000001")
		if err != nil {
			return failure, false
		}
		document, err := orm.ParseJSON("null")
		if err != nil {
			return failure, false
		}
		_, err = orm.InsertOne(ctx, pool, table,
			orm.Set(id, orm.Some(int32(2))),
			orm.Set(bigColumn, orm.Some(int64(-9223372036854775808))),
			orm.Set(preciseColumn, orm.Some(precise)),
			orm.Set(momentColumn, orm.Some(time.Date(2038, 1, 19, 3, 14, 7, 654321000, time.UTC))),
			orm.Set(sqlNullColumn, orm.Some((*string)(nil))),
			orm.Set(documentColumn, orm.Some(&document)))
		if err != nil {
			return failure, false
		}
	}
	values, err := orm.Select(ctx, pool, table, orm.Query[fixture]{}.OrderBy(id.Asc()))
	if err != nil {
		return failure, false
	}
	rows := make([][]any, 0, len(values))
	for _, value := range values {
		var document any
		if value.Document != nil {
			document = value.Document.String()
		}
		rows = append(rows, []any{strconv.FormatInt(int64(value.ID), 10), strconv.FormatInt(value.Big, 10), value.Precise.String(), value.Moment.UTC().Format("2006-01-02 15:04:05.999999999-07"), value.SQLNull == nil, document})
	}
	return map[string]any{"protocol": r.Protocol, "case_id": r.CaseID, "profile": r.Profile, "schema_scope": r.Schema, "artifact_hashes": r.Artifacts, "status": "pass", "rows": rows}, true
}
func main() {
	response, ok := observe()
	if json.NewEncoder(os.Stdout).Encode(response) != nil {
		os.Exit(1)
	}
	if !ok {
		os.Exit(1)
	}
}
