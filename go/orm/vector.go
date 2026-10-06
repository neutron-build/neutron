package orm

import (
	"database/sql/driver"
	"math"
	"strconv"
	"strings"
)

// MaxVectorDimensions matches pgvector's native vector storage limit. Indexes
// may impose smaller limits, which PostgreSQL continues to enforce.
const MaxVectorDimensions = 16000

// Vector owns finite float32 elements, matching native pgvector vector storage.
// SQL NULL is a nil *Vector; zero values and zero-dimensional vectors refuse.
// halfvec, sparsevec and bit embeddings are separate, unqualified families.
type Vector struct {
	elements []float32
	valid    bool
}

func NewVector(elements []float32) (Vector, error) {
	if len(elements) == 0 || len(elements) > MaxVectorDimensions {
		return Vector{}, ErrScalarValue
	}
	for _, value := range elements {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return Vector{}, ErrScalarValue
		}
	}
	return Vector{append([]float32(nil), elements...), true}, nil
}
func (v Vector) Elements() []float32  { return append([]float32(nil), v.elements...) }
func (v Vector) ormScalarType() bool  { return true }
func (v Vector) ormScalarValid() bool { return v.valid }
func (v Vector) Value() (driver.Value, error) {
	if !v.valid {
		return nil, ErrScalarValue
	}
	var text strings.Builder
	text.WriteByte('[')
	for i, value := range v.elements {
		if i > 0 {
			text.WriteByte(',')
		}
		text.WriteString(strconv.FormatFloat(float64(value), 'g', -1, 32))
	}
	text.WriteByte(']')
	return text.String(), nil
}
func (v *Vector) Scan(source any) error {
	var text string
	switch value := source.(type) {
	case string:
		text = value
	case []byte:
		text = string(value)
	default:
		return ErrScalarValue
	}
	// Bound input before allocating a split or float slice. Native vector output
	// uses at most 15 characters per finite float32 plus delimiters.
	if len(text) < 3 || len(text) > MaxVectorDimensions*32+2 || text[0] != '[' || text[len(text)-1] != ']' {
		return ErrScalarValue
	}
	parts := strings.Split(text[1:len(text)-1], ",")
	if len(parts) == 0 || len(parts) > MaxVectorDimensions {
		return ErrScalarValue
	}
	elements := make([]float32, len(parts))
	for i, part := range parts {
		value, err := strconv.ParseFloat(strings.TrimSpace(part), 32)
		if err != nil {
			return ErrScalarValue
		}
		elements[i] = float32(value)
	}
	next, err := NewVector(elements)
	if err != nil {
		return err
	}
	*v = next
	return nil
}
