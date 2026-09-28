package db

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestS07ApplyPathsRunOneStatementEach: every apply path sends each
// statement over the extended protocol, which refuses text holding more
// than one command, so a statement boundary a scanner missed fails closed
// instead of running an unchecked statement (S07 review-1 F2, layer b).
// The allowlisted statement kinds, the concurrent index forms outside a
// transaction included, still run.
func TestS07ApplyPathsRunOneStatementEach(t *testing.T) {
	h := newQ07Harness(t, "s07ext")
	ctx := context.Background()
	h.exec(`CREATE TABLE victim (id int PRIMARY KEY)`)
	sess, err := h.client.LockMigrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Release()
	if err := sess.EnsureMigrationTableV2(ctx); err != nil {
		t.Fatal(err)
	}
	survives := func(t *testing.T, err error) {
		t.Helper()
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Code != "42601" || !strings.Contains(pgErr.Message, "multiple commands") {
			t.Fatalf("two commands in one statement must be refused (42601 multiple commands), got %v", err)
		}
		if got := h.queryOne(`SELECT count(*)::text FROM pg_class WHERE relname = 'victim'`); got != "1" {
			t.Fatal("victim was dropped")
		}
		if got := h.queryOne(`SELECT count(*)::text FROM pg_class WHERE relname IN ('a1', 'a2', 'a3')`); got != "0" {
			t.Fatal("the first command of the refused statement ran")
		}
	}
	const twoCommands = "create table a1 (id int); drop table victim"

	t.Run("planned statements (db push, Studio apply, resolve --abort)", func(t *testing.T) {
		survives(t, sess.ApplyStatementsTx(ctx, []string{twoCommands}, nil))
	})
	t.Run("nontransactional migration", func(t *testing.T) {
		err := sess.ApplyNontransactionalMigration(ctx, MigrationFile{Version: "900", Name: "x"},
			[]string{"create index concurrently a1 on victim (id); drop table victim"}, nil)
		survives(t, errors.Unwrap(err))
	})
	// A statement can change how the server reads the next one, now that
	// they are sent one at a time: with standard_conforming_strings off,
	// 'a\'' ends at its second quote, where the scanner (which assumes the
	// setting on) reads a doubled quote and sees one statement, and the
	// server would run the hidden drop as a statement of its own.
	scsOff := func(t *testing.T, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "turned standard_conforming_strings off") {
			t.Fatalf("turning standard_conforming_strings off must be refused, got %v", err)
		}
		if got := h.queryOne(`SELECT count(*)::text FROM pg_class WHERE relname = 'victim'`); got != "1" {
			t.Fatal("victim was dropped")
		}
		if got := h.queryOne(`SHOW standard_conforming_strings`); got != "on" {
			t.Fatalf("standard_conforming_strings left %s", got)
		}
	}
	for _, turnOff := range []string{
		"SET LOCAL standard_conforming_strings = off",
		"select set_config('standard_' || 'conforming_strings', 'off', true)",
	} {
		sql := turnOff + ";\nselect 'a\\''; drop table victim; select 'b';\n"
		t.Run("transactional migration: "+turnOff, func(t *testing.T) {
			if n := len(SplitSQLStatements(sql)); n != 2 {
				t.Fatalf("fixture: the scanner must see 2 statements, sees %d", n)
			}
			scsOff(t, sess.ApplyMigration(ctx, MigrationFile{Version: "901", Name: "scs", SQL: sql}))
			if got := h.queryOne(`SELECT count(*)::text FROM _neutron_migrations`); got != "0" {
				t.Fatal("a refused migration recorded history")
			}
		})
		t.Run("down migration: "+turnOff, func(t *testing.T) {
			scsOff(t, sess.RevertMigration(ctx, MigrationFile{Version: "902", Name: "d", SQL: sql}))
		})
	}
	t.Run("nontransactional: a session-level change is reset", func(t *testing.T) {
		scsOff(t, sess.ApplyNontransactionalMigration(ctx, MigrationFile{Version: "903", Name: "n"},
			[]string{"select set_config('standard_conforming_strings', 'off', false)", "select 1"}, nil))
	})
	t.Run("journaled steps", func(t *testing.T) {
		_, err := sess.execTransactionalStep(ctx, &JournaledStep{Index: 1, Statement: "create table a2 (id int); drop table victim"})
		survives(t, errors.Unwrap(err))
		_, err = sess.execConcurrent(ctx, nil, &JournaledStep{Index: 2}, "create index concurrently a3 on victim (id); drop table victim", func(JournaledRunEvent) {})
		survives(t, errors.Unwrap(err))
	})

	t.Run("allowlisted kinds run over the extended protocol", func(t *testing.T) {
		h.exec(`CREATE TYPE mood AS ENUM ('ok')`)
		up := `-- Migration: kinds
SET LOCAL lock_timeout = '5s';
CREATE SCHEMA k;
CREATE TABLE k.t (id int PRIMARY KEY, m mood, note text);
CREATE SEQUENCE k.s;
CREATE DOMAIN k.pos AS int CHECK (VALUE > 0);
ALTER TYPE mood ADD VALUE 'bad';
INSERT INTO k.t VALUES (1, 'ok', 'a'), (2, 'ok', 'b');
WITH moved AS (UPDATE k.t SET note = 'c' WHERE id = 2 RETURNING id) SELECT count(*) FROM moved;
MERGE INTO k.t USING (SELECT 3 AS id) s ON k.t.id = s.id WHEN NOT MATCHED THEN INSERT VALUES (3, 'ok', 'm');
DELETE FROM k.t WHERE id = 1;
SELECT id, note FROM k.t ORDER BY id;
CREATE VIEW k.v AS SELECT id FROM k.t WHERE note <> ';';
CREATE MATERIALIZED VIEW k.mv AS SELECT $q$;$q$ AS semi;
CREATE EXTENSION IF NOT EXISTS plpgsql;
ALTER TABLE k.t ADD COLUMN extra int;
TRUNCATE k.t;
DROP VIEW k.v;
`
		if err := sess.ApplyMigration(ctx, MigrationFile{Version: "910", Name: "kinds", SQL: up}); err != nil {
			t.Fatalf("transactional kinds: %v", err)
		}
		if err := sess.ApplyNontransactionalMigration(ctx, MigrationFile{Version: "911", Name: "cic", SQL: "x"}, []string{
			"-- Migration: cic",
			"CREATE INDEX CONCURRENTLY t_note ON k.t (note)",
			"CREATE UNIQUE INDEX CONCURRENTLY t_extra ON k.t (extra)",
			"DROP INDEX CONCURRENTLY k.t_note",
		}, nil); err != nil {
			t.Fatalf("concurrent kinds outside a transaction: %v", err)
		}
		if _, err := sess.execConcurrent(ctx, nil, &JournaledStep{Index: 1}, "CREATE INDEX CONCURRENTLY t_m ON k.t (m)", func(JournaledRunEvent) {}); err != nil {
			t.Fatalf("journaled concurrent step: %v", err)
		}
		if got := h.queryOne(`SELECT string_agg(indexrelid::regclass::text || ':' || indisvalid, ',' ORDER BY indexrelid::regclass::text) FROM pg_index WHERE indrelid = 'k.t'::regclass`); got != "k.t_extra:true,k.t_m:true,k.t_pkey:true" {
			t.Fatalf("indexes: %s", got)
		}
		if got := h.queryOne(`SELECT string_agg(version, ',' ORDER BY version) FROM _neutron_migrations`); got != "910,911" {
			t.Fatalf("history: %s", got)
		}
	})
}
