package orm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type rlsRecord struct {
	Tenant string `db:"tenant"`
	ID     int64  `db:"id"`
	Value  string `db:"value"`
}

// This gate tests PostgreSQL authorization with a real NOSUPERUSER/NOBYPASSRLS
// login, not a manually added ORM tenant predicate. The application is trusted
// to bind authenticated tenant context; arbitrary tenant-spoofing SQL is outside
// that admission boundary. Context settings are transaction local, never SET.
func TestPostgresLeastPrivilegeTenantRLSAndPoolReset(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(records, ".records"), `"`), `"`), `""`, `"`)
	entropy := make([]byte, 32)
	if _, err := rand.Read(entropy); err != nil {
		t.Fatal("role credential generation failed")
	}
	role := "orm_rls_" + hex.EncodeToString(entropy[:8])
	password := hex.EncodeToString(entropy)
	// Generated password is never logged or saved in evidence. Native errors from
	// credential provisioning/connection are deliberately not printed.
	if _, err := admin.Exec(ctx, "CREATE ROLE "+quote(role)+" LOGIN PASSWORD '"+password+"' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"); err != nil {
		t.Fatal("owned least-privilege role creation failed")
	}
	t.Cleanup(func() {
		clean, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if _, err := admin.Exec(clean, "DROP OWNED BY "+quote(role)+"; DROP ROLE "+quote(role)); err != nil {
			t.Error("owned least-privilege role cleanup failed")
		}
	})
	table, err := NewTable[rlsRecord](schema, "tenant_records")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := NewColumn[rlsRecord, int64](table, "ID")
	value, _ := NewColumn[rlsRecord, string](table, "Value")
	ddl := "CREATE TABLE " + table.info.sqlName() + " (tenant text NOT NULL,id bigint NOT NULL,value text NOT NULL,PRIMARY KEY(tenant,id)); INSERT INTO " + table.info.sqlName() + " VALUES ('a',1,'alpha'),('b',1,'beta'); ALTER TABLE " + table.info.sqlName() + " ENABLE ROW LEVEL SECURITY; ALTER TABLE " + table.info.sqlName() + " FORCE ROW LEVEL SECURITY; CREATE POLICY tenant_policy ON " + table.info.sqlName() + " TO " + quote(role) + " USING (tenant = NULLIF(current_setting('app.tenant',true),'')) WITH CHECK (tenant = NULLIF(current_setting('app.tenant',true),'')); GRANT USAGE ON SCHEMA " + quote(schema) + " TO " + quote(role) + "; GRANT SELECT,INSERT,UPDATE,DELETE ON " + table.info.sqlName() + " TO " + quote(role)
	if _, err := admin.Exec(ctx, ddl); err != nil {
		t.Fatal("owned tenant authorization fixture creation failed")
	}
	config, err := pgxpool.ParseConfig(os.Getenv("NEUTRON_ORM_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("native test config parsing failed")
	}
	config.ConnConfig.User = role
	config.ConnConfig.Password = password
	config.MaxConns = 1
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal("least-privilege pool creation failed")
	}
	t.Cleanup(pool.Close)
	oracle, err := pgx.ConnectConfig(ctx, config.ConnConfig.Copy())
	if err != nil {
		t.Fatal("least-privilege oracle connection failed")
	}
	t.Cleanup(func() {
		clean, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = oracle.Close(clean)
	})
	var superuser, bypass bool
	var currentRole string
	if err := pool.QueryRow(ctx, "SELECT current_user,rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user").Scan(&currentRole, &superuser, &bypass); err != nil || superuser || bypass || currentRole != role {
		t.Fatal("authorization role is not the required least-privilege login")
	}
	var pid int
	if err := pool.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatal("pool connection identity query failed")
	}
	assertDefault := func() {
		t.Helper()
		models, err := Select(ctx, pool, table, Query[rlsRecord]{})
		if err != nil || len(models) != 0 {
			t.Fatal("tenant context leaked into unscoped pool read")
		}
		var actual int
		if err := pool.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&actual); err != nil || actual != pid {
			t.Fatal("idle cancellation/query cleanup changed owned pooled connection")
		}
		requirePoolReuse(t, ctx, pool)
	}
	setTenant := func(ctx context.Context, scope *Scope, tenant string) error {
		rows, err := scope.Query(ctx, "SELECT set_config($1,$2,true)", "app.tenant", tenant)
		if err != nil {
			return err
		}
		rows.Close()
		return rows.Err()
	}
	assertTenant := func(scope *Scope, tenant string) {
		t.Helper()
		actual, err := Select(ctx, scope, table, Query[rlsRecord]{}.Where(id.Eq(1)))
		if err != nil || len(actual) != 1 || actual[0].Tenant != tenant {
			t.Fatal("ORM read bypassed tenant authorization")
		}
		// Independently use the same actual role on another native connection.
		tx, err := oracle.Begin(ctx)
		if err != nil {
			t.Fatal("native oracle begin failed")
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, "SELECT set_config('app.tenant',$1,true)", tenant); err != nil {
			t.Fatal("native oracle context binding failed")
		}
		rows, err := tx.Query(ctx, "SELECT tenant,id,value FROM "+table.info.sqlName()+" WHERE id=1")
		if err != nil {
			t.Fatal("native role oracle query failed")
		}
		expected := []rlsRecord{}
		for rows.Next() {
			var model rlsRecord
			if err := rows.Scan(&model.Tenant, &model.ID, &model.Value); err != nil {
				t.Fatal("native role oracle scan failed")
			}
			expected = append(expected, model)
		}
		rows.Close()
		if rows.Err() != nil || !reflect.DeepEqual(actual, expected) {
			t.Fatal("ORM result differs from independent same-role oracle")
		}
	}
	assertDefault()
	for _, tenant := range []string{"a", "b", "a"} {
		err := WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
			if err := setTenant(ctx, scope, tenant); err != nil {
				return err
			}
			assertTenant(scope, tenant)
			return nil
		})
		if err != nil {
			t.Fatal("tenant transaction failed")
		}
		assertDefault()
	}
	// Child rollback restores the parent's local tenant setting. Child success
	// deliberately changes the local setting for the remaining root transaction.
	marker := errors.New("child rollback")
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		if err := setTenant(ctx, scope, "a"); err != nil {
			return err
		}
		err := scope.Savepoint(ctx, func(child *Scope) error {
			if err := setTenant(ctx, child, "b"); err != nil {
				return err
			}
			assertTenant(child, "b")
			return marker
		})
		if !errors.Is(err, marker) {
			t.Fatal("child failure not returned")
		}
		assertTenant(scope, "a")
		if err := scope.Savepoint(ctx, func(child *Scope) error { return setTenant(ctx, child, "b") }); err != nil {
			return err
		}
		assertTenant(scope, "b")
		return nil
	})
	if err != nil {
		t.Fatal("tenant savepoint restoration failed")
	}
	assertDefault()
	// A canceled idle callback uses cleanup context for rollback and leaves the
	// same physical connection with no tenant context for its next borrower.
	canceled, cancel := context.WithCancel(ctx)
	err = WithTransaction(canceled, pool, TransactionOptions{}, func(scope *Scope) error {
		if err := setTenant(ctx, scope, "a"); err != nil {
			return err
		}
		assertTenant(scope, "a")
		cancel()
		return nil
	})
	cancel()
	if !errors.Is(err, context.Canceled) {
		t.Fatal("idle cancellation did not roll back tenant transaction")
	}
	assertDefault()
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		if err := setTenant(ctx, scope, "b"); err != nil {
			return err
		}
		_, err := scope.Exec(ctx, "SELECT 1/0")
		return err
	})
	var native *pgconn.PgError
	if !errors.As(err, &native) || native.Code != "22012" {
		t.Fatal("native query failure not retained")
	}
	assertDefault()
	// RLS rejects moving an authorized row to a different tenant, independently
	// of ORM application predicate scopes. The attempted mutation rolls back.
	tenantColumn, _ := NewColumn[rlsRecord, string](table, "Tenant")
	err = WithTransaction(ctx, pool, TransactionOptions{}, func(scope *Scope) error {
		if err := setTenant(ctx, scope, "a"); err != nil {
			return err
		}
		_, err := Update(ctx, scope, table, id.Eq(1), Set(tenantColumn, Some("b")), Set(value, Some("must rollback")))
		return err
	})
	if !errors.As(err, &native) || native.Code != "42501" {
		t.Fatal("RLS WITH CHECK did not refuse tenant mutation")
	}
	assertDefault()
	var remaining int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM "+table.info.sqlName()+" WHERE (tenant='a' AND value='alpha') OR (tenant='b' AND value='beta')").Scan(&remaining); err != nil || remaining != 2 {
		t.Fatal("unauthorized tenant mutation changed rows")
	}
}
