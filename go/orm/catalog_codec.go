package orm

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

var ErrCodecUnsupported = errors.New("orm: PostgreSQL codec outside qualified profile")

// CodecError reports catalog identity, never row values or connection secrets.
type CodecError struct {
	Schema, Table, Column, TypeSchema, TypeName string
	OID                                         uint32
}

func (e *CodecError) Error() string {
	return fmt.Sprintf("orm: unsupported codec %s.%s OID %d for %s.%s.%s", quote(e.TypeSchema), quote(e.TypeName), e.OID, quote(e.Schema), quote(e.Table), quote(e.Column))
}
func (e *CodecError) Unwrap() error { return ErrCodecUnsupported }

// Enum preserves an exact PostgreSQL enum label, including an empty label.
// NewPostgresTable admits it only against an actual native enum catalog type.
// The database validates label membership on writes. SQL NULL uses *Enum.
type Enum struct {
	label string
	valid bool
}

func NewEnum(label string) Enum { return Enum{label, true} }
func (e Enum) Label() string    { return e.label }
func (e Enum) Value() (driver.Value, error) {
	if !e.valid {
		return nil, ErrScalarValue
	}
	return e.label, nil
}
func (e *Enum) Scan(source any) error {
	switch value := source.(type) {
	case string:
		*e = NewEnum(value)
		return nil
	case []byte:
		*e = NewEnum(string(value))
		return nil
	}
	return ErrScalarValue
}
func (e Enum) ormScalarValid() bool { return e.valid }

type catalogCodec struct {
	oid, base, element uint32
	schema, name, kind string
	required           bool
}

// NewPostgresTable validates mapped type/nullability against the native qualified
// catalog before returning a table usable for writes. It does no DDL and never
// changes the caller's registry or protocol. Qualification is a point-in-time
// contract for this database; migrations must reconstruct metadata after DDL.
// NewTable remains database-free reflection metadata and is not this certificate.
func NewPostgresTable[M any](ctx context.Context, db Executor, schema, name string, contracts ...CodecContract[M]) (Table[M], error) {
	table, err := NewTable[M](schema, name)
	if err != nil {
		return Table[M]{}, err
	}
	if err := ready(ctx, db); err != nil {
		return Table[M]{}, err
	}
	custom := map[int]CodecContract[M]{}
	for _, contract := range contracts {
		i, ok := table.info.byGoName[contract.field]
		if !ok {
			return Table[M]{}, fmt.Errorf("orm: codec contract field missing")
		}
		typ := table.info.fields[i].typ
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		if _, exists := custom[i]; exists || typ != contract.typ || contract.oid == 0 {
			return Table[M]{}, fmt.Errorf("orm: codec contract type/field conflict")
		}
		custom[i] = contract
	}
	rows, err := db.Query(ctx, `SELECT a.attname,a.atttypid,a.attnotnull,t.typbasetype,t.typelem,n.nspname,t.typname,t.typtype::text,t.typnotnull FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid JOIN pg_catalog.pg_namespace s ON s.oid=c.relnamespace JOIN pg_catalog.pg_type t ON t.oid=a.atttypid JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace WHERE s.nspname=$1 AND c.relname=$2 AND c.relkind IN ('r','p','v','m','f') AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, schema, name)
	if err != nil {
		return Table[M]{}, wrap("qualify codecs", err)
	}
	catalog := map[string]catalogCodec{}
	notNull := map[string]bool{}
	for rows.Next() {
		var column string
		var codec catalogCodec
		var required bool
		if err := rows.Scan(&column, &codec.oid, &required, &codec.base, &codec.element, &codec.schema, &codec.name, &codec.kind, &codec.required); err != nil {
			rows.Close()
			return Table[M]{}, wrap("qualify codecs", err)
		}
		catalog[column] = codec
		notNull[column] = required || codec.required
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Table[M]{}, wrap("qualify codecs", err)
	}
	table.info.catalogOIDs = make([][]uint32, len(table.info.fields))
	for fieldIndex, field := range table.info.fields {
		codec, ok := catalog[field.name]
		if !ok {
			return Table[M]{}, fmt.Errorf("orm: mapped catalog column %s is absent", quote(field.name))
		}
		base := codec
		for depth := 0; base.kind == "d"; depth++ {
			if depth == 16 {
				return Table[M]{}, fmt.Errorf("orm: domain base depth exceeded")
			}
			baseRows, err := db.Query(ctx, `SELECT t.oid,t.typbasetype,t.typelem,n.nspname,t.typname,t.typtype::text,t.typnotnull FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace WHERE t.oid=$1`, base.base)
			if err != nil {
				return Table[M]{}, wrap("qualify domain", err)
			}
			if !baseRows.Next() {
				baseRows.Close()
				return Table[M]{}, fmt.Errorf("orm: domain base catalog type missing")
			}
			err = baseRows.Scan(&base.oid, &base.base, &base.element, &base.schema, &base.name, &base.kind, &base.required)
			baseRows.Close()
			if err != nil {
				return Table[M]{}, wrap("qualify domain", err)
			}
			if err := baseRows.Err(); err != nil {
				return Table[M]{}, wrap("qualify domain", err)
			}
			notNull[field.name] = notNull[field.name] || base.required
		}
		if !field.nullable && !notNull[field.name] {
			return Table[M]{}, fmt.Errorf("orm: mapped nonnullable field %s requires native NOT NULL", quote(field.name))
		}
		typ := field.typ
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		table.info.catalogOIDs[fieldIndex] = []uint32{codec.oid, base.oid}
		if !qualifiedCatalogCodec(typ, base) {
			if contract, ok := custom[fieldIndex]; ok && codec.oid == contract.oid && codec.schema == contract.typeSchema && codec.name == contract.typeName {
				continue
			}
			return Table[M]{}, &CodecError{schema, name, field.name, codec.schema, codec.name, codec.oid}
		}
	}
	return table, nil
}

func qualifiedCatalogCodec(t reflect.Type, c catalogCodec) bool {
	if t == reflect.TypeOf(Enum{}) {
		return c.kind == "e"
	}
	if array, ok := reflect.Zero(t).Interface().(interface{ ormArrayElement() reflect.Type }); ok {
		if c.element == 0 || c.kind != "b" {
			return false
		}
		element := array.ormArrayElement()
		if element.Kind() == reflect.Pointer {
			element = element.Elem()
		}
		return qualifiedCatalogCodec(element, catalogCodec{oid: c.element, kind: "b"})
	}
	if r, ok := reflect.Zero(t).Interface().(interface{ ormRangeElement() reflect.Type }); ok {
		subtype := r.ormRangeElement()
		switch c.oid {
		case pgtype.Int4rangeOID:
			return subtype == reflect.TypeOf(int32(0))
		case pgtype.Int8rangeOID:
			return subtype == reflect.TypeOf(int64(0))
		case pgtype.NumrangeOID:
			return subtype == reflect.TypeOf(Decimal{})
		case pgtype.TsrangeOID, pgtype.TstzrangeOID:
			return subtype == reflect.TypeOf(time.Time{})
		case pgtype.DaterangeOID:
			return subtype == reflect.TypeOf(Date{})
		}
		return false
	}
	if c.kind != "b" {
		return false
	}
	switch {
	case t == reflect.TypeOf(time.Time{}):
		return c.oid == pgtype.TimestampOID || c.oid == pgtype.TimestamptzOID
	case t == reflect.TypeOf(Decimal{}):
		return c.oid == pgtype.NumericOID
	case t == reflect.TypeOf(UUID{}):
		return c.oid == pgtype.UUIDOID
	case t == reflect.TypeOf(JSON{}):
		return c.oid == pgtype.JSONOID || c.oid == pgtype.JSONBOID
	case t == reflect.TypeOf(Bytea{}):
		return c.oid == pgtype.ByteaOID
	case t == reflect.TypeOf(Date{}):
		return c.oid == pgtype.DateOID
	case t == reflect.TypeOf(TimeOfDay{}):
		return c.oid == pgtype.TimeOID
	case t == reflect.TypeOf(Interval{}):
		return c.oid == pgtype.IntervalOID
	}
	switch t.Kind() {
	case reflect.String:
		return c.oid == pgtype.TextOID || c.oid == pgtype.VarcharOID || c.oid == pgtype.BPCharOID
	case reflect.Bool:
		return c.oid == pgtype.BoolOID
	case reflect.Int32:
		return c.oid == pgtype.Int2OID || c.oid == pgtype.Int4OID
	case reflect.Int64:
		return c.oid == pgtype.Int2OID || c.oid == pgtype.Int4OID || c.oid == pgtype.Int8OID
	case reflect.Int:
		return c.oid == pgtype.Int2OID || c.oid == pgtype.Int4OID || (t.Bits() == 64 && c.oid == pgtype.Int8OID)
	case reflect.Float32:
		return c.oid == pgtype.Float4OID
	case reflect.Float64:
		return c.oid == pgtype.Float4OID || c.oid == pgtype.Float8OID
	}
	return false
}
