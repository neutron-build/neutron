package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type migrationQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// migrationNamespace binds bookkeeping to the admitted persistent schema.
// User migration SQL retains its own search_path semantics.
type migrationNamespace struct {
	schema     string
	schemaOID  uint32
	historyOID uint32
}

func (n *migrationNamespace) table() string {
	return pgx.Identifier{n.schema, "_neutron_migrations"}.Sanitize()
}
func captureMigrationNamespace(ctx context.Context, q migrationQuerier) (*migrationNamespace, error) {
	var version string
	if err := q.QueryRow(ctx, "SELECT pg_catalog.version()").Scan(&version); err != nil {
		return nil, fmt.Errorf("migration metadata requires PostgreSQL catalogs: %w", err)
	}
	if !strings.HasPrefix(version, "PostgreSQL ") || strings.Contains(version, "Nucleus") {
		return nil, fmt.Errorf("migration metadata requires PostgreSQL catalogs")
	}
	n := &migrationNamespace{}
	if err := q.QueryRow(ctx, "SELECT n.nspname,n.oid FROM pg_catalog.pg_namespace n WHERE n.nspname=pg_catalog.current_schema()").Scan(&n.schema, &n.schemaOID); err != nil {
		return nil, fmt.Errorf("capture migration application namespace: %w", err)
	}
	if n.schema == "information_schema" || strings.HasPrefix(n.schema, "pg_") {
		return nil, fmt.Errorf("migration metadata requires persistent application namespace, got %q", n.schema)
	}
	if err := n.validate(ctx, q); err != nil {
		return nil, err
	}
	var visible *uint32
	if err := q.QueryRow(ctx, "SELECT pg_catalog.to_regclass('_neutron_migrations')::oid").Scan(&visible); err != nil {
		return nil, err
	}
	if visible != nil && *visible != n.historyOID {
		return nil, fmt.Errorf("migration history is shadowed by a temporary or later search_path relation")
	}
	if n.historyOID != 0 {
		if _, err := inspectMigrationShape(ctx, q, n); err != nil {
			return nil, err
		}
	}
	return n, nil
}
func (n *migrationNamespace) validate(ctx context.Context, q migrationQuerier) error {
	var schemaOID uint32
	if err := q.QueryRow(ctx, "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=$1", n.schema).Scan(&schemaOID); err != nil {
		return fmt.Errorf("migration namespace disappeared: %w", err)
	}
	if schemaOID != n.schemaOID {
		return fmt.Errorf("migration namespace identity changed")
	}
	var oid uint32
	var kind, persistence string
	err := q.QueryRow(ctx, "SELECT c.oid,c.relkind::text,c.relpersistence::text FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname='_neutron_migrations'", n.schema).Scan(&oid, &kind, &persistence)
	if err == pgx.ErrNoRows {
		if n.historyOID != 0 {
			return fmt.Errorf("migration history disappeared")
		}
		return nil
	}
	if err != nil {
		return err
	}
	if kind != "r" || persistence != "p" {
		return fmt.Errorf("migration history must be an ordinary permanent table")
	}
	if n.historyOID != 0 && oid != n.historyOID {
		return fmt.Errorf("migration history identity changed")
	}
	n.historyOID = oid
	return nil
}
func inspectMigrationShape(ctx context.Context, q migrationQuerier, n *migrationNamespace) (HistoryShape, error) {
	if err := n.validate(ctx, q); err != nil {
		return HistoryIncompatible, err
	}
	if n.historyOID == 0 {
		return HistoryAbsent, nil
	}
	rows, err := q.Query(ctx, "SELECT a.attname,pg_catalog.format_type(a.atttypid,NULL),t.oid,t.typtype::text,ns.nspname,t.typname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid JOIN pg_catalog.pg_namespace ns ON ns.oid=t.typnamespace WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped", n.historyOID)
	if err != nil {
		return HistoryIncompatible, err
	}
	defer rows.Close()
	cols := map[string]string{}
	for rows.Next() {
		var name, typ, kind, namespace, typeName string
		var oid uint32
		if err := rows.Scan(&name, &typ, &oid, &kind, &namespace, &typeName); err != nil {
			return HistoryIncompatible, err
		}
		if name == "version" {
			allowed := map[uint32]string{20: "int8", 21: "int2", 23: "int4", 25: "text", 1042: "bpchar", 1043: "varchar"}
			if expected, ok := allowed[oid]; !ok || kind != "b" || namespace != "pg_catalog" || typeName != expected {
				return HistoryIncompatible, fmt.Errorf("_neutron_migrations.version requires an actual builtin integer/text type; got %q.%q (OID %d, kind %q)", namespace, typeName, oid, kind)
			}
			typ = map[uint32]string{20: "bigint", 21: "smallint", 23: "integer", 25: "text", 1042: "character", 1043: "character varying"}[oid]
		}
		cols[name] = typ
	}
	if err := rows.Err(); err != nil {
		return HistoryIncompatible, err
	}
	vt := cols["version"]
	v2 := cols["checksum"] != "" && cols["owner"] != "" && cols["format"] != ""
	integer := vt == "integer" || vt == "smallint" || vt == "bigint"
	text := vt == "text" || strings.HasPrefix(vt, "character varying") || strings.HasPrefix(vt, "character(") || vt == "character"
	switch {
	case integer && v2:
		return HistoryV2Integer, nil
	case integer:
		return HistoryLegacyInteger, nil
	case text && v2:
		return HistoryV2Text, nil
	case text:
		return HistoryLegacyText, nil
	default:
		return HistoryIncompatible, fmt.Errorf("_neutron_migrations.version has unsupported type %q", vt)
	}
}
