package orm

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

var ErrScalarValue = errors.New("orm: invalid or unsupported scalar value")

// Decimal is an immutable, finite exact PostgreSQL numeric value. Its zero
// value is invalid, not numeric zero or SQL NULL. ParseDecimal("0") creates zero.
// NaN and infinities are explicitly unsupported on both write and read paths.
type Decimal struct {
	coefficient string
	exponent    int32
	valid       bool
}

var decimalPattern = regexp.MustCompile(`^([+-]?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([+-]?[0-9]+))?$`)

func ParseDecimal(text string) (Decimal, error) {
	parts := decimalPattern.FindStringSubmatch(text)
	if parts == nil || len(parts[2])+len(parts[3]) == 0 {
		return Decimal{}, ErrScalarValue
	}
	exponent := int64(0)
	if parts[4] != "" {
		var err error
		exponent, err = strconv.ParseInt(parts[4], 10, 32)
		if err != nil {
			return Decimal{}, ErrScalarValue
		}
	}
	exponent -= int64(len(parts[3]))
	if exponent < -16383 || exponent > 131072 {
		return Decimal{}, ErrScalarValue
	}
	coefficient, ok := new(big.Int).SetString(parts[1]+parts[2]+parts[3], 10)
	if !ok {
		return Decimal{}, ErrScalarValue
	}
	return finiteDecimal(coefficient, int32(exponent))
}

func finiteDecimal(coefficient *big.Int, exponent int32) (Decimal, error) {
	if coefficient == nil || exponent < -16383 || exponent > 131072 {
		return Decimal{}, ErrScalarValue
	}
	text := coefficient.String()
	digits := len(strings.TrimPrefix(text, "-"))
	if int64(digits)+int64(exponent) > 131072 {
		return Decimal{}, ErrScalarValue
	}
	return Decimal{text, exponent, true}, nil
}

// String emits exact base-ten text, retaining the represented fractional scale.
// Invalid values return an empty string; NumericValue refuses them explicitly.
func (d Decimal) String() string {
	if !d.valid {
		return ""
	}
	sign, text := "", d.coefficient
	if strings.HasPrefix(text, "-") {
		sign = "-"
		text = text[1:]
	}
	if d.exponent >= 0 {
		return sign + text + strings.Repeat("0", int(d.exponent))
	}
	point := len(text) + int(d.exponent)
	if point <= 0 {
		return sign + "0." + strings.Repeat("0", -point) + text
	}
	return sign + text[:point] + "." + text[point:]
}
func (d Decimal) MarshalJSON() ([]byte, error) {
	if !d.valid {
		return nil, ErrScalarValue
	}
	return json.Marshal(d.String())
}

func (d Decimal) NumericValue() (pgtype.Numeric, error) {
	if !d.valid {
		return pgtype.Numeric{}, ErrScalarValue
	}
	value, ok := new(big.Int).SetString(d.coefficient, 10)
	if !ok {
		return pgtype.Numeric{}, ErrScalarValue
	}
	return pgtype.Numeric{Int: value, Exp: d.exponent, Valid: true}, nil
}
func (d *Decimal) ScanNumeric(value pgtype.Numeric) error {
	if !value.Valid || value.NaN || value.InfinityModifier != pgtype.Finite {
		return ErrScalarValue
	}
	next, err := finiteDecimal(value.Int, value.Exp)
	if err != nil {
		return err
	}
	*d = next
	return nil
}

// UUID is an immutable native UUID value. Its zero bytes are the valid nil UUID;
// SQL NULL is represented by a nil *UUID in a nullable mapped field.
type UUID struct{ bytes [16]byte }

func ParseUUID(text string) (UUID, error) {
	if len(text) != 36 || text[8] != '-' || text[13] != '-' || text[18] != '-' || text[23] != '-' {
		return UUID{}, ErrScalarValue
	}
	raw, err := hex.DecodeString(text[:8] + text[9:13] + text[14:18] + text[19:23] + text[24:])
	if err != nil {
		return UUID{}, ErrScalarValue
	}
	var value UUID
	copy(value.bytes[:], raw)
	return value, nil
}
func (u UUID) String() string                  { return (pgtype.UUID{Bytes: u.bytes, Valid: true}).String() }
func (u UUID) MarshalJSON() ([]byte, error)    { return json.Marshal(u.String()) }
func (u UUID) UUIDValue() (pgtype.UUID, error) { return pgtype.UUID{Bytes: u.bytes, Valid: true}, nil }
func (u *UUID) ScanUUID(value pgtype.UUID) error {
	if !value.Valid {
		return ErrScalarValue
	}
	u.bytes = value.Bytes
	return nil
}

// JSON is an immutable validated JSON document for json/jsonb columns. JSON
// null is ParseJSON("null"); SQL NULL is a nil *JSON on a nullable mapped field.
// The zero value is invalid and never silently becomes {} or SQL NULL.
type JSON struct {
	document string
	valid    bool
}

func ParseJSON(text string) (JSON, error) {
	if !json.Valid([]byte(text)) {
		return JSON{}, ErrScalarValue
	}
	return JSON{text, true}, nil
}
func (j JSON) String() string {
	if !j.valid {
		return ""
	}
	return j.document
}
func (j JSON) IsNull() bool { return j.valid && strings.TrimSpace(j.document) == "null" }
func (j JSON) MarshalJSON() ([]byte, error) {
	if !j.valid {
		return nil, ErrScalarValue
	}
	return []byte(j.document), nil
}
func (j JSON) Value() (driver.Value, error) {
	if !j.valid {
		return nil, ErrScalarValue
	}
	return j.document, nil
}
func (j *JSON) ScanBytes(value []byte) error {
	if value == nil {
		return ErrScalarValue
	}
	next, err := ParseJSON(string(value))
	if err != nil {
		return err
	}
	*j = next
	return nil
}

// JSONCodec's generic **T unmarshal conflates JSON null with SQL NULL. This
// destination uses the native BytesScanner path and decides NULL from the wire.
type nullableJSONDestination struct{ target reflect.Value }

func (d nullableJSONDestination) ScanBytes(value []byte) error {
	if value == nil {
		d.target.Set(reflect.Zero(d.target.Type()))
		return nil
	}
	next, err := ParseJSON(string(value))
	if err != nil {
		return err
	}
	d.target.Set(reflect.ValueOf(&next))
	return nil
}
func scanDestination(value reflect.Value) any {
	if value.Kind() == reflect.Pointer && value.Type().Elem().Implements(reflect.TypeOf((*interface{ ormArrayType() })(nil)).Elem()) {
		return &nullableArrayDestination{target: value}
	}
	if destination, ok := value.Addr().Interface().(interface{ ormDestination() any }); ok {
		return destination.ormDestination()
	}
	if value.Type() == reflect.TypeOf((*JSON)(nil)) {
		return nullableJSONDestination{value}
	}
	return value.Addr().Interface()
}

func validateScalarValue(value any) error {
	if scalar, ok := value.(interface{ ormScalarValid() bool }); ok && !scalar.ormScalarValid() {
		return ErrScalarValue
	}
	switch v := value.(type) {
	case Bytea:
		if !v.valid {
			return ErrScalarValue
		}
	case Decimal:
		if !v.valid {
			return ErrScalarValue
		}
	case JSON:
		if !v.valid {
			return ErrScalarValue
		}
	}
	return nil
}

// These compile-time contracts deliberately use native numeric/uuid/JSON
// scanner paths, without replacing a caller's pgx registry or protocol mode.
var _ pgtype.NumericScanner = (*Decimal)(nil)
var _ pgtype.NumericValuer = Decimal{}
var _ pgtype.UUIDScanner = (*UUID)(nil)
var _ pgtype.UUIDValuer = UUID{}
var _ pgtype.BytesScanner = (*JSON)(nil)
var _ pgtype.BytesScanner = nullableJSONDestination{}
var _ driver.Valuer = JSON{}
