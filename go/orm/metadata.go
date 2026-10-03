// Package orm provides an opt-in PostgreSQL typed query core. It uses the
// caller's pgx connection, pool, or transaction without changing protocol mode
// or assuming ownership of its lifecycle. This initial core is not GORM parity.
package orm

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

type fieldInfo struct {
	name     string
	index    int
	typ      reflect.Type
	nullable bool
}
type modelInfo struct {
	schema, name string
	fields       []fieldInfo
	byGoName     map[string]int
}

// Table is immutable validated metadata for a struct mapped to a qualified
// PostgreSQL table. Each constructor creates a distinct binding scope.
type Table[M any] struct{ info *modelInfo }

// Go struct types/tags are immutable at runtime. Cache only the validated model
// shape, never a table binding or mutable application state.
var modelShapeCache sync.Map

// NewTable requires explicit db tags on exported, non-embedded mapped fields.
// A nullable field uses a pointer and db:"column,nullable". db:"-" excludes a
// field. Scalars supported by this core are string, bool, int, int32, int64,
// float32, float64, time.Time, Decimal, UUID and JSON (or nullable pointers to
// them). Other codecs
// require separate qualification; numeric should not be mapped to float.
func NewTable[M any](schema, name string) (Table[M], error) {
	var table Table[M]
	if err := identifier(schema); err != nil {
		return table, err
	}
	if err := identifier(name); err != nil {
		return table, err
	}
	t := reflect.TypeOf((*M)(nil)).Elem()
	if t.Kind() != reflect.Struct {
		return table, fmt.Errorf("orm: model must be a struct")
	}
	if cached, ok := modelShapeCache.Load(t); ok {
		shape := cached.(*modelInfo)
		return Table[M]{&modelInfo{schema: schema, name: name, fields: shape.fields, byGoName: shape.byGoName}}, nil
	}
	info := &modelInfo{schema: schema, name: name, byGoName: map[string]int{}}
	seen := map[string]bool{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag, ok := f.Tag.Lookup("db")
		if ok && tag == "-" {
			continue
		}
		if !ok || tag == "" {
			return table, fmt.Errorf("orm: field %s needs an explicit db tag", f.Name)
		}
		if f.PkgPath != "" || f.Anonymous {
			return table, fmt.Errorf("orm: unsupported field %s", f.Name)
		}
		parts := strings.Split(tag, ",")
		if len(parts) > 2 || (len(parts) == 2 && parts[1] != "nullable") {
			return table, fmt.Errorf("orm: invalid db options for %s", f.Name)
		}
		if err := identifier(parts[0]); err != nil {
			return table, err
		}
		if seen[parts[0]] {
			return table, fmt.Errorf("orm: duplicate db tag %q", parts[0])
		}
		seen[parts[0]] = true
		nullable := len(parts) == 2
		base := f.Type
		if nullable != (base.Kind() == reflect.Pointer) {
			return table, fmt.Errorf("orm: pointer/nullability mismatch for %s", f.Name)
		}
		if nullable {
			base = base.Elem()
		}
		if !supportedScalar(base) {
			return table, fmt.Errorf("orm: unsupported type %s for %s", f.Type, f.Name)
		}
		info.byGoName[f.Name] = len(info.fields)
		info.fields = append(info.fields, fieldInfo{parts[0], i, f.Type, nullable})
	}
	if len(info.fields) == 0 {
		return table, fmt.Errorf("orm: model has no mapped fields")
	}
	shape, _ := modelShapeCache.LoadOrStore(t, &modelInfo{fields: info.fields, byGoName: info.byGoName})
	blueprint := shape.(*modelInfo)
	info.fields = blueprint.fields
	info.byGoName = blueprint.byGoName
	table.info = info
	return table, nil
}

func supportedScalar(t reflect.Type) bool {
	if t == reflect.TypeOf(Bytea{}) {
		return true
	}
	if value, ok := reflect.Zero(t).Interface().(interface{ ormScalarType() bool }); ok {
		return value.ormScalarType()
	}
	return supportedBuiltinScalar(t)
}
func supportedBuiltinScalar(t reflect.Type) bool {
	if t == reflect.TypeOf(time.Time{}) || t == reflect.TypeOf(Decimal{}) || t == reflect.TypeOf(UUID{}) || t == reflect.TypeOf(JSON{}) {
		return true
	}
	// Named user-defined scalar codecs are not implicitly certified.
	if t.PkgPath() != "" {
		return false
	}
	switch t.Kind() {
	case reflect.String, reflect.Bool, reflect.Int, reflect.Int32, reflect.Int64, reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func identifier(s string) error {
	if s == "" || strings.IndexByte(s, 0) >= 0 || len([]byte(s)) > 63 {
		return fmt.Errorf("orm: identifier must be nonempty, NUL-free and at most 63 bytes")
	}
	return nil
}
func quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func (t *modelInfo) sqlName() string {
	if t.schema == "" {
		return quote(t.name)
	} // internal CTE binding; NewTable refuses empty schema
	return quote(t.schema) + "." + quote(t.name)
}
func (t *modelInfo) columns() string {
	names := make([]string, len(t.fields))
	for i, f := range t.fields {
		names[i] = quote(f.name)
	}
	return strings.Join(names, ", ")
}

// Column binds a model field's Go value type and table identity. Construct it
// once after NewTable; its generic type must exactly match the struct field.
type Column[M, T any] struct {
	info  *modelInfo
	field fieldInfo
}

func NewColumn[M, T any](table Table[M], goField string) (Column[M, T], error) {
	var col Column[M, T]
	if table.info == nil {
		return col, fmt.Errorf("orm: uninitialized table")
	}
	i, ok := table.info.byGoName[goField]
	if !ok {
		return col, fmt.Errorf("orm: unknown mapped field %q", goField)
	}
	f := table.info.fields[i]
	if f.typ != reflect.TypeOf((*T)(nil)).Elem() {
		return col, fmt.Errorf("orm: column type does not match field %s", goField)
	}
	return Column[M, T]{table.info, f}, nil
}

// snapshot detaches nullable scalar inputs from caller-owned pointers. It
// preserves exact scalar values; no textual or floating-point conversion runs.
func snapshot(value any) any {
	v := reflect.ValueOf(value)
	if v.IsValid() && v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		return v.Elem().Interface()
	}
	return value
}
