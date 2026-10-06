package orm

import (
	"database/sql/driver"
	"strings"
)

const MaxCompositeFields = 256
const MaxCompositeTextBytes = 1 << 20

// Composite preserves a bounded native record as nullable exact field text.
// It does not infer a Go struct or narrow numeric fields. Field order follows
// the qualified PostgreSQL composite catalog. SQL NULL uses *Composite;
// NewComposite(nil fields) is invalid, while all-NULL fields form a valid record.
type Composite struct {
	fields []Nullable[string]
	valid  bool
}

func NewComposite(fields []Nullable[string]) (Composite, error) {
	if len(fields) == 0 || len(fields) > MaxCompositeFields {
		return Composite{}, ErrScalarValue
	}
	size := 2
	for _, field := range fields {
		if field.Valid {
			size += 2 + len(field.Value)*2
			if strings.IndexByte(field.Value, 0) >= 0 {
				return Composite{}, ErrScalarValue
			}
		}
		size++
		if size > MaxCompositeTextBytes {
			return Composite{}, ErrScalarValue
		}
	}
	return Composite{append([]Nullable[string](nil), fields...), true}, nil
}
func (v Composite) Fields() []Nullable[string] { return append([]Nullable[string](nil), v.fields...) }
func (v Composite) ormScalarType() bool        { return true }
func (v Composite) ormScalarValid() bool       { return v.valid }
func (v Composite) Value() (driver.Value, error) {
	if !v.valid {
		return nil, ErrScalarValue
	}
	var text strings.Builder
	text.WriteByte('(')
	for i, field := range v.fields {
		if i > 0 {
			text.WriteByte(',')
		}
		if !field.Valid {
			continue
		}
		text.WriteByte('"')
		for _, b := range []byte(field.Value) {
			if b == '"' || b == '\\' {
				text.WriteByte('\\')
			}
			text.WriteByte(b)
		}
		text.WriteByte('"')
	}
	text.WriteByte(')')
	return text.String(), nil
}
func (v *Composite) Scan(source any) error {
	var text string
	switch value := source.(type) {
	case string:
		text = value
	case []byte:
		text = string(value)
	default:
		return ErrScalarValue
	}
	if len(text) < 2 || len(text) > MaxCompositeTextBytes || text[0] != '(' || text[len(text)-1] != ')' {
		return ErrScalarValue
	}
	end := len(text) - 1
	fields := make([]Nullable[string], 0)
	for pos := 1; ; {
		if len(fields) >= MaxCompositeFields {
			return ErrScalarValue
		}
		if pos == end || text[pos] == ',' {
			fields = append(fields, Nullable[string]{})
		} else {
			var field strings.Builder
			quoted := text[pos] == '"'
			if quoted {
				pos++
			}
			closed := !quoted
			for pos < end {
				b := text[pos]
				if b == '\\' {
					pos++
					if pos >= end {
						return ErrScalarValue
					}
					field.WriteByte(text[pos])
					pos++
					continue
				}
				if quoted && b == '"' {
					if pos+1 < end && text[pos+1] == '"' {
						field.WriteByte('"')
						pos += 2
						continue
					}
					pos++
					closed = true
					break
				}
				if !quoted && b == ',' {
					break
				}
				if !quoted && (b == '"' || b == '(' || b == ')') {
					return ErrScalarValue
				}
				field.WriteByte(b)
				pos++
			}
			if !closed {
				return ErrScalarValue
			}
			fields = append(fields, Nullable[string]{Value: field.String(), Valid: true})
		}
		if pos == end {
			break
		}
		if text[pos] != ',' {
			return ErrScalarValue
		}
		pos++
	}
	next, err := NewComposite(fields)
	if err != nil {
		return err
	}
	*v = next
	return nil
}
