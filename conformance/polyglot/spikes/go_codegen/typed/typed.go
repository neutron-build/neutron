// Package typed is an isolated API experiment, not a database ORM.
package typed

import (
	"fmt"
	"reflect"
	"strings"
)

// Optional separates omission from a supplied zero value or nullable pointer.
// Its zero value is omitted. Some((*string)(nil)) is explicit SQL NULL.
type Optional[T any] struct {
	value   T
	present bool
}

func Some[T any](value T) Optional[T] { return Optional[T]{value: value, present: true} }
func (o Optional[T]) Get() (T, bool)  { return o.value, o.present }

type Binding struct {
	Name  string
	Value any
}

// Column retains the model and projected value types without accepting raw SQL.
type Column[M, T any] struct {
	name string
	get  func(M) T
}

func NewColumn[M, T any](name string, get func(M) T) Column[M, T] {
	return Column[M, T]{name: name, get: get}
}
func (c Column[M, T]) Name() string { return c.name }

type Predicate[M any] struct {
	name  string
	value any
}

func (c Column[M, T]) Eq(value T) Predicate[M] { return Predicate[M]{name: c.name, value: value} }
func (p Predicate[M]) Binding() Binding        { return Binding{Name: p.name, Value: p.value} }

// Project is an in-memory typed projection proof, not SELECT execution.
func Project[M, T any](rows []M, column Column[M, T]) []T {
	values := make([]T, len(rows))
	for i, row := range rows {
		values[i] = column.get(row)
	}
	return values
}

type Field struct {
	GoName   string
	DBName   string
	Type     reflect.Type
	Nullable bool
}

// ValidateModel checks generated metadata against the actual Go field types
// and tags. It must run before a future ORM admits the model to execution.
func ValidateModel(model reflect.Type, metadata []Field) error {
	if model == nil || model.Kind() != reflect.Struct {
		return fmt.Errorf("model must be struct")
	}
	seen := make(map[string]bool)
	actual := make(map[string]reflect.StructField)
	for i := 0; i < model.NumField(); i++ {
		f := model.Field(i)
		tag, ok := f.Tag.Lookup("db")
		if !ok {
			return fmt.Errorf("field %s requires explicit db tag", f.Name)
		}
		if tag == "-" {
			continue
		}
		parts := strings.Split(tag, ",")
		if f.PkgPath != "" || f.Anonymous {
			return fmt.Errorf("unsupported field %s", f.Name)
		}
		if parts[0] == "" || seen[parts[0]] {
			return fmt.Errorf("empty or duplicate db tag %q", parts[0])
		}
		seen[parts[0]] = true
		actual[f.Name] = f
	}
	if len(actual) == 0 {
		return fmt.Errorf("model has no mapped fields")
	}
	if len(actual) != len(metadata) {
		return fmt.Errorf("metadata field count mismatch")
	}
	seen = make(map[string]bool)
	for _, m := range metadata {
		f, ok := actual[m.GoName]
		if !ok || seen[m.GoName] {
			return fmt.Errorf("missing or duplicate metadata field %s", m.GoName)
		}
		seen[m.GoName] = true
		parts := strings.Split(f.Tag.Get("db"), ",")
		if len(parts) > 2 || (len(parts) == 2 && parts[1] != "nullable") {
			return fmt.Errorf("unknown db option on %s", m.GoName)
		}
		nullable := len(parts) == 2
		if parts[0] != m.DBName || f.Type != m.Type || nullable != m.Nullable || nullable != (f.Type.Kind() == reflect.Pointer) {
			return fmt.Errorf("metadata/type/nullability mismatch for %s", m.GoName)
		}
	}
	return nil
}
