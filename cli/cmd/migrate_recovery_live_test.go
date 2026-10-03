package cmd

// Native recovery qualification owns disposable databases through the existing
// m02 harness. Faults are real PostgreSQL session termination and pg_dump/
// pg_restore operations, never executor hooks or fabricated history records.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/neutron-build/neutron/cli/internal/db"
)

func recoveryScalar(t *testing.T, native *db.Client, ctx context.Context, sql string, args ...any) string {
	t.Helper()
	var value string
	if native.QueryRow(ctx, sql, args...).Scan(&value) != nil {
		t.Fatal("independent recovery oracle failed (connection diagnostics suppressed)")
	}
	return value
}

func recoveryCLI(t *testing.T, ctx context.Context, bin, endpoint string, args ...string) error {
	t.Helper()
	command := exec.CommandContext(ctx, bin, args...)
	command.Dir = t.TempDir()
	command.Env = append(os.Environ(), "DATABASE_URL="+endpoint, "NO_COLOR=1")
	// Native failures may include libpq connection diagnostics. Keep credentials
	// out of test logs; state assertions provide the independent failure oracle.
	command.Stdout, command.Stderr = io.Discard, io.Discard
	return command.Run()
}

func TestMigrationConcurrentIndexSessionLossRecoveryNative(t *testing.T) {
	endpoint, native := newM02CommandDB(t, "indexloss")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	bin := buildCLIBinary(t)
	if native.Exec(ctx, `CREATE TABLE public.source(id bigint PRIMARY KEY,tag text);INSERT INTO public.source SELECT g,'tag '||g FROM generate_series(1,2000) g`) != nil {
		t.Fatal("index-loss fixture setup failed")
	}
	up := "-- neutron:journaled\nCREATE INDEX CONCURRENTLY recovery_tag_idx ON public.source(tag);\n"
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "001_index.up.sql"), up)
	writeFile(t, filepath.Join(dir, "001_index.down.sql"), "DROP INDEX public.recovery_tag_idx;\n")
	blocker, err := pgx.Connect(ctx, endpoint)
	if err != nil {
		t.Fatal("index-loss blocker connection failed")
	}
	defer blocker.Close(context.Background())
	transaction, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatal("index-loss blocker begin failed")
	}
	defer transaction.Rollback(context.Background())
	if _, err := transaction.Exec(ctx, `UPDATE public.source SET tag=tag WHERE id=1`); err != nil {
		t.Fatal("index-loss writer lock failed")
	}
	// CIC first publishes an invalid catalog entry, then waits for this older
	// writer before building. Observe that real window before killing only the
	// explicitly named migration backend in this owned database.
	application := fmt.Sprintf("migration_index_loss_%d", time.Now().UnixNano())
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal("index-loss endpoint invalid")
	}
	options := u.Query()
	options.Set("application_name", application)
	u.RawQuery = options.Encode()
	command := exec.CommandContext(ctx, bin, "migrate", "--dir", dir, "--timeout", "60s")
	command.Dir = t.TempDir()
	command.Env = append(os.Environ(), "DATABASE_URL="+u.String(), "NO_COLOR=1")
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if command.Start() != nil {
		t.Fatal("index-loss migration process failed to start")
	}
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	var pid int32
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if native.QueryRow(ctx, `SELECT a.pid FROM pg_catalog.pg_stat_activity a WHERE a.datname=pg_catalog.current_database() AND a.application_name=$1 AND a.wait_event_type='Lock' AND a.query LIKE '%CREATE INDEX CONCURRENTLY%' AND EXISTS(SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='recovery_tag_idx' AND NOT i.indisvalid)`, application).Scan(&pid) == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("actual concurrent index invalid/writer-wait window not observed")
	}
	var terminated bool
	if native.QueryRow(ctx, `SELECT pg_catalog.pg_terminate_backend(pid) FROM pg_catalog.pg_stat_activity WHERE pid=$1 AND datname=pg_catalog.current_database() AND application_name=$2`, pid, application).Scan(&terminated) != nil || !terminated {
		t.Fatal("owned concurrent index backend was not terminated")
	}
	err = command.Wait()
	waited = true
	if err == nil {
		t.Fatal("session-loss migration falsely succeeded")
	}
	if transaction.Rollback(ctx) != nil {
		t.Fatal("index-loss writer release failed")
	}
	if recoveryScalar(t, native, ctx, `SELECT i.indisvalid::text FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='recovery_tag_idx'`) != "false" || recoveryScalar(t, native, ctx, `SELECT count(*)::text FROM public._neutron_migrations`) != "0" {
		t.Fatal("session-loss left falsely valid index or applied history")
	}
	if recoveryScalar(t, native, ctx, `SELECT count(*)::text FROM pg_catalog.pg_locks WHERE pid=$1 AND locktype='advisory'`, pid) != "0" {
		t.Fatal("terminated migration session retained advisory authority")
	}
	if recoveryCLI(t, ctx, bin, endpoint, "migrate", "resolve", "001", "--retry", "--dir", dir, "--timeout", "45s") != nil {
		t.Fatal("explicit interrupted index reconciliation failed")
	}
	if recoveryScalar(t, native, ctx, `SELECT i.indisvalid::text FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname='recovery_tag_idx'`) != "true" || recoveryScalar(t, native, ctx, `SELECT count(*)::text FROM public.source`) != "2000" {
		t.Fatal("index reconciliation did not preserve native rows/validity")
	}
	digest := sha256.Sum256([]byte(up))
	if recoveryScalar(t, native, ctx, `SELECT checksum||':'||owner||':'||format FROM public._neutron_migrations WHERE version='001'`) != hex.EncodeToString(digest[:])+":neutron-cli:v2" {
		t.Fatal("recovery changed exact protocol-v2 checksum/owner/format")
	}
	if recoveryCLI(t, ctx, bin, endpoint, "migrate", "--dir", dir) != nil || recoveryScalar(t, native, ctx, `SELECT count(*)::text FROM public._neutron_migrations`) != "1" {
		t.Fatal("reconciled migration replay changed history")
	}
}

func recoveryTool(t *testing.T, variable, fallback string) string {
	t.Helper()
	name := os.Getenv(variable)
	if name == "" {
		name = fallback
	}
	path, err := exec.LookPath(name)
	if err != nil {
		if os.Getenv("NEUTRON_LIVE_REQUIRED") == "1" {
			t.Fatalf("required native recovery tool %s unavailable", fallback)
		}
		t.Skipf("native recovery tool %s unavailable", fallback)
	}
	return path
}

func recoveryPGEnvironment(t *testing.T, endpoint string) []string {
	t.Helper()
	config, err := pgx.ParseConfig(endpoint)
	if err != nil {
		t.Fatal("recovery native-tool endpoint invalid")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		t.Fatal("recovery native-tool endpoint invalid")
	}
	// Dedicated fixture connection parameters go through environment variables,
	// never command arguments. Do not inherit another project's libpq settings.
	environment := []string{}
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		if !strings.HasPrefix(name, "PG") {
			environment = append(environment, item)
		}
	}
	sslmode := u.Query().Get("sslmode")
	if sslmode == "" {
		sslmode = "prefer"
	}
	environment = append(environment, "PGHOST="+config.Host, "PGPORT="+strconv.Itoa(int(config.Port)), "PGUSER="+config.User, "PGPASSWORD="+config.Password, "PGDATABASE="+config.Database, "PGSSLMODE="+sslmode)
	return environment
}

func TestMigrationPopulatedDumpRestoreForwardRepairNative(t *testing.T) {
	endpoint, native := newM02CommandDB(t, "restoresource")
	dump := recoveryTool(t, "NEUTRON_PG_DUMP_BINARY", "pg_dump")
	restore := recoveryTool(t, "NEUTRON_PG_RESTORE_BINARY", "pg_restore")
	restoredEndpoint, restored := newM02CommandDB(t, "restoretarget")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bin := buildCLIBinary(t)
	dir := t.TempDir()
	up := `CREATE SCHEMA "Odd Schema";
CREATE TABLE "Odd Schema"."Ledger"(id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,amount numeric(30,4) NOT NULL,note text NOT NULL,payload jsonb);
`
	writeFile(t, filepath.Join(dir, "001_ledger.up.sql"), up)
	writeFile(t, filepath.Join(dir, "001_ledger.down.sql"), "-- IRREVERSIBLE: dropping a populated ledger does not restore its data\n")
	if recoveryCLI(t, ctx, bin, endpoint, "migrate", "--dir", dir) != nil {
		t.Fatal("restore fixture initial migration failed")
	}
	if native.Exec(ctx, `INSERT INTO "Odd Schema"."Ledger"(id,amount,note,payload) VALUES(-1,123456789012345678901234.1234,'first','{"large":9007199254740993}'),(9007199254740993,0.0001,'second','null'),(9223372036854775807,42.0100,'third',NULL);SELECT pg_catalog.setval(pg_catalog.pg_get_serial_sequence('"Odd Schema"."Ledger"','id'),41,true)`) != nil {
		t.Fatal("restore fixture populated values failed")
	}
	rowsSQL := `SELECT pg_catalog.json_agg(pg_catalog.json_build_array(id::text,amount::text,note,payload::text) ORDER BY id)::text FROM "Odd Schema"."Ledger"`
	historySQL := `SELECT pg_catalog.json_agg(pg_catalog.json_build_array(version,name,checksum,owner,format) ORDER BY version)::text FROM public._neutron_migrations`
	wantRows := recoveryScalar(t, native, ctx, rowsSQL)
	wantHistory := recoveryScalar(t, native, ctx, historySQL)
	archive := filepath.Join(t.TempDir(), "populated.dump")
	dumpCommand := exec.CommandContext(ctx, dump, "--format=custom", "--no-owner", "--no-privileges", "--file", archive)
	dumpCommand.Env = recoveryPGEnvironment(t, endpoint)
	dumpCommand.Stdout, dumpCommand.Stderr = io.Discard, io.Discard
	if dumpCommand.Run() != nil {
		t.Fatal("actual populated pg_dump failed (native connection diagnostics suppressed)")
	}
	if native.Exec(ctx, `UPDATE "Odd Schema"."Ledger" SET note='changed after backup' WHERE id=-1`) != nil {
		t.Fatal("post-backup source change failed")
	}
	// pg_restore's --dbname must not contain the secret-bearing endpoint. Use
	// only the database name and pass all other libpq parameters privately.
	config, err := pgx.ParseConfig(restoredEndpoint)
	if err != nil {
		t.Fatal("restore target parameters invalid")
	}
	restoreCommand := exec.CommandContext(ctx, restore, "--exit-on-error", "--no-owner", "--no-privileges", "--dbname", config.Database, archive)
	restoreCommand.Env = recoveryPGEnvironment(t, restoredEndpoint)
	restoreCommand.Stdout, restoreCommand.Stderr = io.Discard, io.Discard
	if restoreCommand.Run() != nil {
		t.Fatal("actual populated pg_restore failed (native connection diagnostics suppressed)")
	}
	if recoveryScalar(t, restored, ctx, rowsSQL) != wantRows || recoveryScalar(t, restored, ctx, historySQL) != wantHistory {
		t.Fatal("restored exact values or protocol-v2 history differ from native snapshot")
	}
	if recoveryCLI(t, ctx, bin, restoredEndpoint, "migrate", "--dir", dir) != nil || recoveryScalar(t, restored, ctx, historySQL) != wantHistory {
		t.Fatal("restored migration history did not resume idempotently")
	}
	if recoveryScalar(t, restored, ctx, `INSERT INTO "Odd Schema"."Ledger"(amount,note) VALUES(1.0000,'postrestore') RETURNING id::text`) != "42" {
		t.Fatal("restore lost native identity sequence state")
	}
	// Forward repair is new checksummed SQL, never editing or fabricating the
	// recorded migration and never pretending a down file restores lost values.
	expand := "ALTER TABLE \"Odd Schema\".\"Ledger\" ADD COLUMN normalized text;\n"
	repair := `-- neutron:journaled
-- neutron:step verify="SELECT count(*) FROM \"Odd Schema\".\"Ledger\" WHERE normalized IS NULL" expect="0"
UPDATE "Odd Schema"."Ledger" SET normalized=pg_catalog.upper(note) WHERE normalized IS NULL;
`
	writeFile(t, filepath.Join(dir, "002_expand.up.sql"), expand)
	writeFile(t, filepath.Join(dir, "002_expand.down.sql"), "-- IRREVERSIBLE: normalized values are not recovered by down\n")
	writeFile(t, filepath.Join(dir, "003_repair.up.sql"), repair)
	writeFile(t, filepath.Join(dir, "003_repair.down.sql"), "-- IRREVERSIBLE: data repair is forward only\n")
	beforeUpgrade := recoveryScalar(t, restored, ctx, rowsSQL)
	if recoveryCLI(t, ctx, bin, restoredEndpoint, "migrate", "--dir", dir, "--timeout", "45s") != nil {
		t.Fatal("restored populated-schema forward upgrade/repair failed")
	}
	if recoveryScalar(t, restored, ctx, `SELECT count(*)::text FROM "Odd Schema"."Ledger" WHERE normalized IS DISTINCT FROM pg_catalog.upper(note)`) != "0" || recoveryScalar(t, restored, ctx, `SELECT count(*)::text FROM public._neutron_migrations`) != "3" || recoveryScalar(t, restored, ctx, rowsSQL) != beforeUpgrade {
		t.Fatal("forward repair missed rows or failed protocol history")
	}
	for version, content := range map[string]string{"001": up, "002": expand, "003": repair} {
		digest := sha256.Sum256([]byte(content))
		if recoveryScalar(t, restored, ctx, `SELECT checksum||':'||owner||':'||format FROM public._neutron_migrations WHERE version=$1`, version) != hex.EncodeToString(digest[:])+":neutron-cli:v2" {
			t.Fatal("forward repair altered exact v2 digest/ownership")
		}
	}
	beforeDown := recoveryScalar(t, restored, ctx, rowsSQL)
	if recoveryCLI(t, ctx, bin, restoredEndpoint, "migrate", "down", "--dir", dir, "--allow-destructive") == nil || recoveryScalar(t, restored, ctx, rowsSQL) != beforeDown || recoveryScalar(t, restored, ctx, `SELECT count(*)::text FROM public._neutron_migrations`) != "3" {
		t.Fatal("irreversible down falsely restored or changed populated data/history")
	}
}
