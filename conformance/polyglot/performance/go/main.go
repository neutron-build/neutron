// Installed archived-module consumer; native tracing counts all SQL dispatches.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"regexp"
	"runtime"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/neutron-build/neutron/go/orm"
)

type request struct {
	Protocol   string            `json:"protocol"`
	Schema     string            `json:"schema_scope"`
	Mode       string            `json:"mode"`
	Workload   string            `json:"workload"`
	Warmup     int               `json:"warmup"`
	Iterations int               `json:"iterations"`
	Artifacts  map[string]string `json:"artifact_hashes"`
}
type row struct {
	ID   int32  `db:"id"`
	Big  int64  `db:"big"`
	Body string `db:"body"`
}
type tracer struct{ count int }

func (t *tracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	t.count++
	return ctx
}
func (t *tracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func run() error {
	var r request
	if err := json.NewDecoder(os.Stdin).Decode(&r); err != nil {
		return err
	}
	if r.Protocol != "polyglot-performance-v1" || !regexp.MustCompile(`^neutron_polyglot_[0-9a-f]{32}$`).MatchString(r.Schema) || (r.Mode != "raw" && r.Mode != "orm") || (r.Workload != "read" && r.Workload != "transaction") || r.Warmup != 64 || r.Iterations != 256 {
		return fmt.Errorf("frozen workload required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Second)
	defer cancel()
	config, err := pgxpool.ParseConfig(os.Getenv("NEUTRON_TEST_DATABASE_URL"))
	if err != nil {
		return err
	}
	trace := &tracer{}
	config.MaxConns = 1
	config.MinConns = 0
	config.ConnConfig.Tracer = trace
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	defer pool.Close()
	table, err := orm.NewTable[row](r.Schema, "perf_fixture")
	if err != nil {
		return err
	}
	id, err := orm.NewColumn[row, int32](table, "ID")
	if err != nil {
		return err
	}
	body, err := orm.NewColumn[row, string](table, "Body")
	if err != nil {
		return err
	}
	readSQL := `SELECT id,big,body FROM "` + r.Schema + `".perf_fixture WHERE id=$1 LIMIT 2`
	updateSQL := `UPDATE "` + r.Schema + `".perf_fixture SET body=$1 WHERE id=$2`
	step := func(i int, phase string) (int64, error) {
		key := int32(i%64 + 1)
		marker := fmt.Sprintf("%s:%d", phase, i)
		var value row
		execute := func(db orm.Executor) error {
			var err error
			if r.Mode == "orm" {
				value, err = orm.SelectOne(ctx, db, table, orm.Query[row]{}.Where(id.Eq(key)))
				if err == nil && r.Workload == "transaction" {
					var n int64
					n, err = orm.Update(ctx, db, table, id.Eq(key), orm.Set(body, orm.Some(marker)))
					if err == nil && n != 1 {
						return fmt.Errorf("update cardinality")
					}
				}
			} else {
				rows, e := db.Query(ctx, readSQL, key)
				err = e
				if err == nil {
					if rows.Next() {
						err = rows.Scan(&value.ID, &value.Big, &value.Body)
					} else {
						err = fmt.Errorf("read cardinality")
					}
					if err == nil && rows.Next() {
						err = fmt.Errorf("read cardinality")
					}
					if err == nil {
						err = rows.Err()
					}
					rows.Close()
				}
				if err == nil && r.Workload == "transaction" {
					tag, e := db.Exec(ctx, updateSQL, marker, key)
					err = e
					if err == nil && tag.RowsAffected() != 1 {
						return fmt.Errorf("update cardinality")
					}
				}
			}
			return err
		}
		if r.Workload == "transaction" {
			if r.Mode == "orm" {
				err = orm.WithTransaction(ctx, pool, orm.TransactionOptions{Isolation: pgx.ReadCommitted}, func(scope *orm.Scope) error { return execute(scope) })
			} else {
				var tx pgx.Tx
				tx, err = pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
				if err == nil {
					err = execute(tx)
					if err == nil {
						err = tx.Commit(ctx)
					} else {
						_ = tx.Rollback(ctx)
					}
				}
			}
		} else {
			err = execute(pool)
		}
		if err != nil {
			return 0, err
		}
		if value.ID != key || value.Big != 9007199254740993+int64(key) {
			return 0, fmt.Errorf("point oracle mismatch")
		}
		return value.Big, nil
	}
	for i := 0; i < r.Warmup; i++ {
		if _, err = step(i, "warmup"); err != nil {
			return err
		}
	}
	trace.count = 0
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var usageBefore, usageAfter syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usageBefore)
	checksum := new(big.Int)
	start := time.Now()
	for i := 0; i < r.Iterations; i++ {
		value, e := step(i, "measure")
		if e != nil {
			return e
		}
		checksum.Add(checksum, big.NewInt(value))
	}
	elapsed := time.Since(start).Nanoseconds()
	runtime.ReadMemStats(&after)
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usageAfter)
	expected := r.Iterations
	if r.Workload == "transaction" {
		expected *= 4
	}
	if trace.count != expected {
		return fmt.Errorf("unexpected native dispatch count")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"protocol": r.Protocol, "schema_scope": r.Schema, "mode": r.Mode, "workload": r.Workload, "iterations": r.Iterations, "warmup": r.Warmup, "artifact_hashes": r.Artifacts, "checksum": checksum.String(), "elapsed_ns": elapsed, "query_count": trace.count, "query_count_scope": "native pgx QueryTracer SQL dispatches", "runtime": runtime.Version(), "memory": map[string]any{"peak_rss_before": usageBefore.Maxrss, "peak_rss_after": usageAfter.Maxrss, "rss_unit": "KiB on Linux; bytes on macOS", "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "heap_before": before.HeapAlloc, "heap_after": after.HeapAlloc}})
}
func main() {
	if run() != nil {
		fmt.Println(`{"status":"fail","diagnostics":"installed Go performance consumer failed"}`)
		os.Exit(1)
	}
}
