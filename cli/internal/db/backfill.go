package db

// This first backfill profile copies one builtin PostgreSQL column to another.
// It owns one transaction per chunk, uses operator-provisioned checkpoints,
// and never executes DDL, callbacks or arbitrary SQL. Idle is not completion.
// Final validation describes one snapshot; compatible writer/cutover policy
// remains an independently verified deployment precondition.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"reflect"
	"strings"
	"time"
)

type BackfillSpec struct {
	Version             int        `json:"version"`
	JobID               string     `json:"jobId"`
	Transformation      string     `json:"transformation"`
	Source              V2Identity `json:"source"`
	Checkpoint          V2Identity `json:"checkpoint"`
	Key                 string     `json:"key"`
	From                string     `json:"from"`
	To                  string     `json:"to"`
	WriterPolicySHA256  string     `json:"writerPolicySha256"`
	BatchRows           int64      `json:"batchRows"`
	TimeoutMilliseconds int64      `json:"timeoutMilliseconds"`
}
type backfillColumn struct {
	Name                         string
	Number                       int16
	TypeOID                      uint32
	Namespace, Kind              string
	Typmod                       int32
	NotNull                      bool
	Identity, Generated, Default string
}
type backfillRelation struct {
	OID, NamespaceOID uint32
	Columns           []backfillColumn
	PrimaryKey        []int16
	DefinitionSHA256  string
}
type BackfillJob struct {
	Spec               BackfillSpec
	Digest             string
	Cluster            string
	DatabaseOID        uint32
	source, checkpoint backfillRelation
}
type BackfillChunk struct {
	Status                                string
	JobDigest                             string
	UpdatedRows, TotalUpdatedRows, Chunks int64
}
type BackfillValidation struct {
	JobDigest         string
	Mismatches        int64
	SnapshotValidated bool
}
type BackfillError struct {
	Operation     string
	Cause         error
	Indeterminate bool
}

func (e *BackfillError) Error() string {
	if e.Indeterminate {
		return "backfill: commit outcome indeterminate; reconcile checkpoint before continuing"
	}
	return "backfill: " + e.Operation + " failed (inspect cause)"
}
func (e *BackfillError) Unwrap() error { return e.Cause }
func backfillError(op string, err error) error {
	if err == nil {
		return nil
	}
	return &BackfillError{Operation: op, Cause: err}
}
func backfillRollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func backfillName(id V2Identity) string { return pgx.Identifier{id.Schema, id.Name}.Sanitize() }
func backfillSpecValid(s BackfillSpec) error {
	if s.Version != 1 || s.Transformation != "copy-column-v1" || !rolloutIdentity(s.JobID) || !rolloutDigest(s.WriterPolicySHA256) || s.BatchRows <= 0 || s.BatchRows > 10000 || s.TimeoutMilliseconds <= 0 || s.TimeoutMilliseconds > 60000 {
		return fmt.Errorf("invalid immutable copy-column-v1 specification")
	}
	for _, v := range []string{s.Source.Schema, s.Source.Name, s.Checkpoint.Schema, s.Checkpoint.Name, s.Key, s.From, s.To} {
		if v == "" || len(v) > 63 || strings.ContainsRune(v, 0) {
			return fmt.Errorf("invalid database identifier")
		}
	}
	if s.From == s.To || s.Key == s.To || s.Source == s.Checkpoint {
		return fmt.Errorf("source/target/checkpoint identities must be distinct")
	}
	return nil
}
func backfillHash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func backfillIdentity(ctx context.Context, q pgQueryer) (string, uint32, error) {
	var version, cluster string
	var database uint32
	if err := q.QueryRow(ctx, `SELECT pg_catalog.version()`).Scan(&version); err != nil {
		return "", 0, err
	}
	if !strings.HasPrefix(version, "PostgreSQL ") || strings.Contains(strings.ToLower(version), "nucleus") {
		return "", 0, fmt.Errorf("native PostgreSQL required")
	}
	// Explicit authority profile: pg_control_system access is required to bind
	// cluster identity, rather than treating a database name as server identity.
	if err := q.QueryRow(ctx, `SELECT system_identifier::pg_catalog.text FROM pg_catalog.pg_control_system()`).Scan(&cluster); err != nil {
		return "", 0, err
	}
	if err := q.QueryRow(ctx, `SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()`).Scan(&database); err != nil {
		return "", 0, err
	}
	return cluster, database, nil
}
func backfillInspectRelation(ctx context.Context, q pgQueryer, id V2Identity) (backfillRelation, error) {
	var r backfillRelation
	var kind, persistence string
	var unsafe bool
	err := q.QueryRow(ctx, `SELECT c.oid,n.oid,c.relkind::pg_catalog.text,c.relpersistence::pg_catalog.text,
 c.relrowsecurity OR c.relforcerowsecurity OR c.relispartition OR
 EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal) OR
 EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite w WHERE w.ev_class=c.oid) OR
 EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid)
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`, id.Schema, id.Name).Scan(&r.OID, &r.NamespaceOID, &kind, &persistence, &unsafe)
	if err != nil {
		return r, err
	}
	if kind != "r" || persistence != "p" || unsafe {
		return r, fmt.Errorf("ordinary permanent non-RLS trigger-free table required")
	}
	rows, err := q.Query(ctx, `SELECT a.attname,a.attnum,a.atttypid,n.nspname,t.typtype::pg_catalog.text,a.atttypmod,a.attnotnull,a.attidentity::pg_catalog.text,a.attgenerated::pg_catalog.text,COALESCE(pg_catalog.pg_get_expr(d.adbin,d.adrelid),'')
 FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, r.OID)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var c backfillColumn
		if err := rows.Scan(&c.Name, &c.Number, &c.TypeOID, &c.Namespace, &c.Kind, &c.Typmod, &c.NotNull, &c.Identity, &c.Generated, &c.Default); err != nil {
			rows.Close()
			return r, err
		}
		r.Columns = append(r.Columns, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	var constraints []string
	rows, err = q.Query(ctx, `SELECT c.contype::pg_catalog.text,c.conkey::pg_catalog.int2[],pg_catalog.pg_get_constraintdef(c.oid),c.convalidated,c.condeferrable FROM pg_catalog.pg_constraint c WHERE c.conrelid=$1 ORDER BY c.conname`, r.OID)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var k, definition string
		var keys []int16
		var valid, deferable bool
		if err := rows.Scan(&k, &keys, &definition, &valid, &deferable); err != nil {
			rows.Close()
			return r, err
		}
		constraints = append(constraints, k+":"+definition)
		if k == "p" {
			if !valid || deferable {
				rows.Close()
				return r, fmt.Errorf("validated immediate primary key required")
			}
			r.PrimaryKey = keys
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	r.DefinitionSHA256 = backfillHash(struct {
		Columns     []backfillColumn
		Constraints []string
	}{r.Columns, constraints})
	return r, nil
}
func backfillColumnByName(r backfillRelation, name string) (backfillColumn, bool) {
	for _, c := range r.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return backfillColumn{}, false
}
func backfillValidateRelations(ctx context.Context, q pgQueryer, s BackfillSpec, source, checkpoint backfillRelation) error {
	var keyIndexValid bool
	if err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_index i ON i.indexrelid=c.conindid WHERE c.conrelid=$1 AND c.contype='p' AND i.indisvalid AND i.indisready AND i.indislive)`, source.OID).Scan(&keyIndexValid); err != nil {
		return err
	}
	if !keyIndexValid {
		return fmt.Errorf("valid ready primary index required")
	}
	key, ok := backfillColumnByName(source, s.Key)
	if !ok || key.TypeOID != 20 || key.Namespace != "pg_catalog" || key.Kind != "b" || !key.NotNull || len(source.PrimaryKey) != 1 || source.PrimaryKey[0] != key.Number {
		return fmt.Errorf("single actual builtin int8 primary key required")
	}
	from, ok := backfillColumnByName(source, s.From)
	to, ok2 := backfillColumnByName(source, s.To)
	supported := map[uint32]bool{16: true, 20: true, 23: true, 25: true, 1700: true}
	if !ok || !ok2 || !supported[from.TypeOID] || from.TypeOID != to.TypeOID || from.Typmod != to.Typmod || from.Namespace != "pg_catalog" || to.Namespace != "pg_catalog" || from.Kind != "b" || to.Kind != "b" || to.Identity != "" || to.Generated != "" || (!from.NotNull && to.NotNull) {
		return fmt.Errorf("same builtin source/target type and nullable-safe writable target required")
	}
	expected := []struct {
		name string
		oid  uint32
	}{{"job_id", 25}, {"job_digest", 25}, {"format", 25}, {"chunks", 20}, {"updated_rows", 20}}
	if len(checkpoint.Columns) != len(expected) || len(checkpoint.PrimaryKey) != 1 {
		return fmt.Errorf("unknown checkpoint schema")
	}
	for _, e := range expected {
		c, ok := backfillColumnByName(checkpoint, e.name)
		if !ok || c.TypeOID != e.oid || c.Namespace != "pg_catalog" || c.Kind != "b" || !c.NotNull || c.Identity != "" || c.Generated != "" || c.Default != "" {
			return fmt.Errorf("unknown checkpoint column contract")
		}
		if e.name == "job_id" && checkpoint.PrimaryKey[0] != c.Number {
			return fmt.Errorf("checkpoint primary key mismatch")
		}
	}
	var constraints int
	if err := q.QueryRow(ctx, `SELECT pg_catalog.count(*) FROM pg_catalog.pg_constraint WHERE conrelid=$1`, checkpoint.OID).Scan(&constraints); err != nil {
		return err
	}
	if constraints != 1 {
		return fmt.Errorf("unknown checkpoint constraints")
	}
	var sourcePrivilege, checkpointPrivilege bool
	err := q.QueryRow(ctx, `SELECT pg_catalog.has_table_privilege($1::pg_catalog.oid,'SELECT') AND pg_catalog.has_column_privilege($1::pg_catalog.oid,$2,'UPDATE'),pg_catalog.has_table_privilege($3::pg_catalog.oid,'SELECT') AND pg_catalog.has_table_privilege($3::pg_catalog.oid,'INSERT') AND pg_catalog.has_table_privilege($3::pg_catalog.oid,'UPDATE')`, source.OID, s.To, checkpoint.OID).Scan(&sourcePrivilege, &checkpointPrivilege)
	if err != nil {
		return err
	}
	if !sourcePrivilege || !checkpointPrivilege {
		return fmt.Errorf("backfill privileges unavailable")
	}
	return nil
}
func InspectBackfillJob(ctx context.Context, c *Client, s BackfillSpec) (BackfillJob, error) {
	var job BackfillJob
	if err := backfillSpecValid(s); err != nil {
		return job, err
	}
	if ctx == nil || c == nil || c.pool == nil {
		return job, fmt.Errorf("context/client required")
	}
	conn, err := c.pool.Acquire(ctx)
	if err != nil {
		return job, backfillError("inspect", err)
	}
	defer conn.Release()
	job.Spec = s
	job.Cluster, job.DatabaseOID, err = backfillIdentity(ctx, conn)
	if err == nil {
		job.source, err = backfillInspectRelation(ctx, conn, s.Source)
	}
	if err == nil {
		job.checkpoint, err = backfillInspectRelation(ctx, conn, s.Checkpoint)
	}
	if err == nil {
		err = backfillValidateRelations(ctx, conn, s, job.source, job.checkpoint)
	}
	if err != nil {
		return BackfillJob{}, backfillError("inspect", err)
	}
	job.Digest = backfillJobDigest(job)
	return job, nil
}
func backfillJobDigest(job BackfillJob) string {
	return backfillHash(struct {
		Spec               BackfillSpec
		Cluster            string
		DatabaseOID        uint32
		Source, Checkpoint backfillRelation
	}{job.Spec, job.Cluster, job.DatabaseOID, job.source, job.checkpoint})
}
func backfillRevalidate(ctx context.Context, tx pgx.Tx, job BackfillJob) error {
	if err := backfillSpecValid(job.Spec); err != nil {
		return err
	}
	if job.Digest == "" || job.Digest != backfillJobDigest(job) {
		return fmt.Errorf("job digest mismatch")
	}
	cluster, database, err := backfillIdentity(ctx, tx)
	if err != nil {
		return err
	}
	if cluster != job.Cluster || database != job.DatabaseOID {
		return fmt.Errorf("database identity changed")
	}
	// Relation locks prevent ALTER/DROP after catalog admission until chunk end.
	for _, id := range []V2Identity{job.Spec.Source, job.Spec.Checkpoint} {
		if _, err := tx.Exec(ctx, "LOCK TABLE "+backfillName(id)+" IN ROW EXCLUSIVE MODE"); err != nil {
			return err
		}
	}
	source, err := backfillInspectRelation(ctx, tx, job.Spec.Source)
	if err != nil {
		return err
	}
	checkpoint, err := backfillInspectRelation(ctx, tx, job.Spec.Checkpoint)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(source, job.source) || !reflect.DeepEqual(checkpoint, job.checkpoint) {
		return fmt.Errorf("relation definition changed")
	}
	return backfillValidateRelations(ctx, tx, job.Spec, source, checkpoint)
}
func RunBackfillChunk(ctx context.Context, c *Client, job BackfillJob) (BackfillChunk, error) {
	result := BackfillChunk{JobDigest: job.Digest}
	if ctx == nil || c == nil || c.pool == nil {
		return result, fmt.Errorf("context/client required")
	}
	if err := backfillSpecValid(job.Spec); err != nil {
		return result, err
	}
	bounded, cancel := context.WithTimeout(ctx, time.Duration(job.Spec.TimeoutMilliseconds)*time.Millisecond)
	defer cancel()
	tx, err := c.pool.BeginTx(bounded, pgx.TxOptions{})
	if err != nil {
		return result, backfillError("begin", err)
	}
	defer backfillRollback(tx)
	if _, err = tx.Exec(bounded, `SELECT pg_catalog.set_config('statement_timeout',$1,true),pg_catalog.set_config('lock_timeout',$1,true)`, fmt.Sprintf("%dms", job.Spec.TimeoutMilliseconds)); err != nil {
		return result, backfillError("timeout", err)
	}
	if err = backfillRevalidate(bounded, tx, job); err != nil {
		return result, backfillError("admission", err)
	}
	cp := backfillName(job.Spec.Checkpoint)
	// Lock first when the row exists, so concurrent workers return busy instead
	// of waiting on an ON CONFLICT insert against the locked job row.
	var digest, format string
	var chunks, total int64
	lock := func() error {
		return tx.QueryRow(bounded, "SELECT job_digest,format,chunks,updated_rows FROM "+cp+" WHERE job_id=$1 FOR UPDATE NOWAIT", job.Spec.JobID).Scan(&digest, &format, &chunks, &total)
	}
	err = lock()
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(bounded, "INSERT INTO "+cp+" (job_id,job_digest,format,chunks,updated_rows) VALUES($1,$2,'copy-column-v1',0,0) ON CONFLICT DO NOTHING", job.Spec.JobID, job.Digest)
		if err == nil {
			err = lock()
		}
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			result.Status = "busy"
			return result, nil
		}
		return result, backfillError("checkpoint", err)
	}
	if digest != job.Digest || format != "copy-column-v1" || chunks < 0 || total < 0 {
		return result, fmt.Errorf("unknown checkpoint row; explicit reconciliation required")
	}
	key := pgx.Identifier{job.Spec.Key}.Sanitize()
	from := pgx.Identifier{job.Spec.From}.Sanitize()
	to := pgx.Identifier{job.Spec.To}.Sanitize()
	source := backfillName(job.Spec.Source)
	rows, err := tx.Query(bounded, "SELECT "+key+" FROM "+source+" WHERE "+to+" IS DISTINCT FROM "+from+" ORDER BY "+key+" LIMIT $1 FOR UPDATE SKIP LOCKED", job.Spec.BatchRows)
	if err != nil {
		return result, backfillError("select candidates", err)
	}
	keys := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return result, backfillError("scan candidates", err)
		}
		keys = append(keys, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, backfillError("scan candidates", err)
	}
	if len(keys) > 0 {
		tag, e := tx.Exec(bounded, "UPDATE "+source+" SET "+to+"="+from+" WHERE "+key+"=ANY($1::pg_catalog.int8[])", keys)
		if e != nil {
			return result, backfillError("copy", e)
		}
		if tag.RowsAffected() != int64(len(keys)) {
			return result, fmt.Errorf("copy cardinality changed")
		}
	}
	result.UpdatedRows = int64(len(keys))
	result.TotalUpdatedRows = total + result.UpdatedRows
	result.Chunks = chunks + 1
	if _, err = tx.Exec(bounded, "UPDATE "+cp+" SET chunks=chunks+1,updated_rows=updated_rows+$1 WHERE job_id=$2", result.UpdatedRows, job.Spec.JobID); err != nil {
		return result, backfillError("checkpoint progress", err)
	}
	if err = tx.Commit(bounded); err != nil {
		return BackfillChunk{Status: "indeterminate", JobDigest: job.Digest}, &BackfillError{Operation: "commit", Cause: err, Indeterminate: true}
	}
	result.Status = "committed"
	if len(keys) == 0 {
		result.Status = "idle"
	}
	return result, nil
}

// ValidateBackfill reports a repeatable-read snapshot mismatch count. It does
// not mark the checkpoint complete or authorize destructive schema changes.
func ValidateBackfill(ctx context.Context, c *Client, job BackfillJob) (BackfillValidation, error) {
	result := BackfillValidation{JobDigest: job.Digest}
	if ctx == nil || c == nil || c.pool == nil {
		return result, fmt.Errorf("context/client required")
	}
	bounded, cancel := context.WithTimeout(ctx, time.Duration(job.Spec.TimeoutMilliseconds)*time.Millisecond)
	defer cancel()
	tx, err := c.pool.BeginTx(bounded, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return result, backfillError("validation begin", err)
	}
	defer backfillRollback(tx)
	if err = backfillRevalidate(bounded, tx, job); err != nil {
		return result, backfillError("validation admission", err)
	}
	from := pgx.Identifier{job.Spec.From}.Sanitize()
	to := pgx.Identifier{job.Spec.To}.Sanitize()
	err = tx.QueryRow(bounded, "SELECT pg_catalog.count(*) FROM "+backfillName(job.Spec.Source)+" WHERE "+to+" IS DISTINCT FROM "+from).Scan(&result.Mismatches)
	if err != nil {
		return result, backfillError("validation", err)
	}
	result.SnapshotValidated = true
	return result, nil
}
