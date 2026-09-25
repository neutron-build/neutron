package studio

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// Tagged wire values crossing the Studio HTTP boundary (S01, meeting the
// F03/B05 discipline pinned by studio/src/lib/wire.ts).
//
// bigint, decimal, binary and temporal values cross HTTP as
//   {"t": <tag>, "v": <exact string>}
// cells so precision survives JSON.parse: an int8 sent as a JSON number
// would arrive as a rounded double, a bytea as base64, and a timestamp in
// the server process timezone. Plain values pass through unchanged; SQL
// NULL stays JSON null. The same tags are accepted on the way back in for
// keys and mutation values, decoded to exact driver values.
//
// Canonical string forms (matching the neutron-sql codec table):
//   int8        decimal integer, full precision
//   numeric     digits with the column's stored scale (e.g. "123.4560");
//               NaN / Infinity / -Infinity for the specials
//   bytea       lowercase hex, no \x prefix
//   date        YYYY-MM-DD
//   timestamp   YYYY-MM-DDTHH:MM:SS[.ffffff] (T separator, no offset)
//   timestamptz UTC, rendered with a trailing Z
//   temporals   "infinity" / "-infinity" for the specials; BC dates carry
//               PostgreSQL's trailing " BC" era marker
//
// uuid crosses as a plain canonical string (exact without a tag).

const (
	oidInt8        = 20
	oidNumeric     = 1700
	oidBytea       = 17
	oidDate        = 1082
	oidTimestamp   = 1114
	oidTimestamptz = 1184
	oidUUID        = 2950
	oidJSON        = 114
	oidJSONB       = 3802
)

// wireTag returns the wire tag for a PostgreSQL type OID, or "" for types
// that cross as plain JSON.
func wireTag(oid uint32) string {
	switch oid {
	case oidInt8:
		return "int8"
	case oidNumeric:
		return "numeric"
	case oidBytea:
		return "bytea"
	case oidDate:
		return "date"
	case oidTimestamp:
		return "timestamp"
	case oidTimestamptz:
		return "timestamptz"
	default:
		return ""
	}
}

type taggedCell struct {
	T string `json:"t"`
	V string `json:"v"`
}

// formatNumeric renders a pgx numeric exactly as PostgreSQL ::text would:
// the stored digits keep their scale, so numeric(12,4) 9.9 renders "9.9000".
func formatNumeric(n pgtype.Numeric) string {
	switch {
	case !n.Valid:
		return ""
	case n.NaN:
		return "NaN"
	case n.InfinityModifier == pgtype.Infinity:
		return "Infinity"
	case n.InfinityModifier == pgtype.NegativeInfinity:
		return "-Infinity"
	}
	digits := n.Int.String()
	neg := strings.HasPrefix(digits, "-")
	if neg {
		digits = digits[1:]
	}
	switch {
	case n.Exp < 0:
		scale := int(-n.Exp)
		for len(digits) <= scale {
			digits = "0" + digits
		}
		out := digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
		if strings.HasSuffix(out, ".") {
			out += "0"
		}
		if neg {
			out = "-" + out
		}
		return out
	case n.Exp == 0:
		return n.Int.String()
	default:
		out := digits + strings.Repeat("0", int(n.Exp))
		if neg {
			out = "-" + out
		}
		return out
	}
}

// Temporal rendering. Go counts BC years as 0, -1, ...; PostgreSQL counts
// them 1 BC, 2 BC, ... and marks them with a trailing " BC" (after any time
// and zone). Components are formatted by hand because time.Format has no
// BC era and misrenders years outside 0000-9999.
func pgDate(t time.Time) (string, bool) {
	y := t.Year()
	bc := y <= 0
	if bc {
		y = 1 - y
	}
	return fmt.Sprintf("%04d-%02d-%02d", y, int(t.Month()), t.Day()), bc
}

func pgClock(t time.Time) string {
	out := fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
	if us := t.Nanosecond() / 1000; us != 0 {
		out += strings.TrimRight(fmt.Sprintf(".%06d", us), "0")
	}
	return out
}

func formatTemporal(tag string, t time.Time) string {
	if tag == "timestamptz" {
		t = t.UTC()
	}
	date, bc := pgDate(t)
	out := date
	switch tag {
	case "timestamp":
		out += "T" + pgClock(t)
	case "timestamptz":
		out += "T" + pgClock(t) + "Z"
	}
	if bc {
		out += " BC"
	}
	return out
}

// formatInfinity renders pgx's non-finite temporal marker as PostgreSQL's
// own input spelling. (JSON-encoding the marker directly would send the
// integer 1 or -1 — a silently wrong value, and a broken key.)
func formatInfinity(m pgtype.InfinityModifier) (string, bool) {
	switch m {
	case pgtype.Infinity:
		return "infinity", true
	case pgtype.NegativeInfinity:
		return "-infinity", true
	}
	return "", false
}

// formatUUID renders a uuid in its canonical lowercase 8-4-4-4-12 form.
// uuid crosses as a plain JSON string (no tag): the string is exact, and
// pgx's default [16]byte would otherwise marshal as an array of numbers.
func formatUUID(b [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// encodeTaggedCell converts one driver value to its wire form using the
// column's runtime OID. Values of unexpected Go types pass through unchanged
// (they keep whatever JSON encoding they already had) rather than failing
// the whole read.
func encodeTaggedCell(oid uint32, v any) any {
	if v == nil {
		return nil
	}
	if oid == oidUUID {
		if b, ok := v.([16]byte); ok {
			return formatUUID(b)
		}
		return v
	}
	tag := wireTag(oid)
	if tag == "" {
		return v
	}
	switch tag {
	case "int8":
		if i, ok := v.(int64); ok {
			return taggedCell{T: tag, V: fmt.Sprintf("%d", i)}
		}
	case "numeric":
		if n, ok := v.(pgtype.Numeric); ok {
			return taggedCell{T: tag, V: formatNumeric(n)}
		}
		if s, ok := v.(string); ok {
			return taggedCell{T: tag, V: s}
		}
	case "bytea":
		if b, ok := v.([]byte); ok {
			return taggedCell{T: tag, V: fmt.Sprintf("%x", b)}
		}
	case "date", "timestamp", "timestamptz":
		if t, ok := v.(time.Time); ok {
			return taggedCell{T: tag, V: formatTemporal(tag, t)}
		}
		if m, ok := v.(pgtype.InfinityModifier); ok {
			if s, ok := formatInfinity(m); ok {
				return taggedCell{T: tag, V: s}
			}
		}
	}
	return v
}

// decodeWireValue converts one request-supplied cell to the exact driver
// value. Tagged cells validate strictly (a bad payload is an error, never a
// coerced value); plain JSON scalars pass through with json.Number resolved
// exactly (integral numbers become int64, others float64 — never a rounded
// int8). Maps and arrays pass through for json/jsonb targets.
func decodeWireValue(v any) (any, error) {
	switch cell := v.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		if len(cell) == 2 {
			t, tok := cell["t"].(string)
			raw, vok := cell["v"].(string)
			if tok && vok {
				return decodeTagged(taggedCell{T: t, V: raw})
			}
		}
		// A plain JSON object (json/jsonb column value), not a tagged cell.
		return cell, nil
	case json.Number:
		if i, err := cell.Int64(); err == nil {
			return i, nil
		}
		f, err := cell.Float64()
		if err != nil {
			return nil, fmt.Errorf("invalid number %q", cell.String())
		}
		return f, nil
	case string, bool, float64:
		return cell, nil
	case []any:
		return cell, nil
	default:
		return nil, fmt.Errorf("unsupported value type %T", v)
	}
}

var hexDigits = "0123456789abcdefABCDEF"

// hasUTCOffset reports whether a timestamptz payload carries an explicit
// zone: a trailing Z/z, a ±HH[:MM[:SS[.f]]] offset after the time (or after
// a date-only value), or the epoch/infinity specials. PostgreSQL parses
// offset-less input in the SESSION timezone — a silent wrong instant (the
// S03-F2 defect: an 8-hour shift for a Vancouver user) — so the wire layer
// refuses it instead of guessing (S05: chosen over silent UTC
// normalization; documented in studio/README).
func hasUTCOffset(v string) bool {
	s := strings.TrimSpace(v)
	if s == "" {
		return false
	}
	if strings.EqualFold(s, "infinity") || s == "-infinity" || strings.EqualFold(s, "epoch") {
		return true
	}
	// PostgreSQL renders the era marker after the zone: "...+02:00 BC".
	if strings.HasSuffix(s, " BC") {
		s = strings.TrimSpace(strings.TrimSuffix(s, " BC"))
	}
	if !tzDatePrefix.MatchString(s) {
		return false
	}
	rest := s[10:]
	if rest == "" {
		return false // date-only: PostgreSQL would guess midnight session time
	}
	if rest == "Z" || rest == "z" {
		return true
	}
	// A zone attached directly to the date (midnight in that zone).
	if tzOffsetRe.MatchString(rest) {
		return true
	}
	// A time component, then the zone.
	m := tzTimeRe.FindStringSubmatch(rest)
	if m == nil {
		return false
	}
	// PostgreSQL also accepts the zone after one space and the UTC/GMT
	// abbreviations ("2026-09-24 12:34:56 +02", "... UTC").
	zone := strings.TrimPrefix(rest[len(m[0]):], " ")
	return strings.EqualFold(zone, "Z") || strings.EqualFold(zone, "UTC") || strings.EqualFold(zone, "GMT") || tzOffsetRe.MatchString(zone)
}

var (
	tzDatePrefix = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}`)
	// tzTimeRe matches the time component (not the zone): " 12:34:56.78".
	tzTimeRe = regexp.MustCompile(`^[ T]\d{2}(:\d{2})?(:\d{2})?(\.\d+)?`)
	// tzOffsetRe matches a full-string ±HH, ±HH:MM, ±HHMM or ±HH:MM:SS[.f] zone.
	tzOffsetRe = regexp.MustCompile(`^[+-]\d{2}(:?\d{2})?(:\d{2}(\.\d+)?)?$`)
)

func decodeTagged(cell taggedCell) (any, error) {
	switch cell.T {
	case "int8":
		i, err := strconv.ParseInt(cell.V, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("int8 wire cell has invalid payload %q", cell.V)
		}
		return i, nil
	case "numeric", "date", "timestamp":
		if cell.V == "" {
			return nil, fmt.Errorf("%s wire cell has empty payload", cell.T)
		}
		return cell.V, nil
	case "timestamptz":
		if cell.V == "" {
			return nil, fmt.Errorf("timestamptz wire cell has empty payload")
		}
		if !hasUTCOffset(cell.V) {
			return nil, fmt.Errorf(
				"timestamptz wire cell %q has no UTC offset: PostgreSQL would interpret it in the server session timezone, silently storing a different instant; append an explicit offset (e.g. \"+02:00\") or use the canonical UTC form (e.g. 2026-09-24T12:34:56Z)",
				cell.V)
		}
		return cell.V, nil
	case "bytea":
		if len(cell.V)%2 != 0 {
			return nil, fmt.Errorf("bytea wire cell has invalid hex payload %q", cell.V)
		}
		out := make([]byte, len(cell.V)/2)
		for i := 0; i < len(cell.V); i += 2 {
			hi := strings.IndexByte(hexDigits, cell.V[i])
			lo := strings.IndexByte(hexDigits, cell.V[i+1])
			if hi < 0 || lo < 0 {
				return nil, fmt.Errorf("bytea wire cell has invalid hex payload %q", cell.V)
			}
			out[i/2] = byte(hi<<4 | lo)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown wire tag %q", cell.T)
	}
}

// taggedCellOf reports whether a decoded JSON value has the exact tagged-cell
// shape {"t": <string>, "v": <string>}.
func taggedCellOf(v any) (taggedCell, bool) {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 2 {
		return taggedCell{}, false
	}
	t, tok := m["t"].(string)
	raw, vok := m["v"].(string)
	if !tok || !vok {
		return taggedCell{}, false
	}
	return taggedCell{T: t, V: raw}, true
}

// decodeColumnValue is the STRICT per-column decoder used by the v2 row
// protocol for keys, updates and inserts. The column's catalog type, not
// the request, decides the accepted shape:
//
//   - tagged types (int8, numeric, bytea, date, timestamp, timestamptz)
//     accept only a tagged cell with exactly that tag — a bare JSON number
//     for an int8 may already have been rounded by the client, so it is
//     refused rather than trusted;
//   - every other type accepts a JSON string, number or boolean. Numbers are
//     passed on as their literal text, so PostgreSQL parses the exact digits
//     into the column type (never a float64 detour). A tagged cell for an
//     untagged column is a type mismatch and is refused;
//   - json/jsonb accept only a string holding the JSON text. "null" is JSON
//     null; SQL NULL is the request's explicit isNull / JSON null. Objects
//     are refused so that a JSON document can never be mistaken for a
//     tagged cell (or vice versa).
//
// A nil result means SQL NULL; callers decide whether NULL is allowed.
func decodeColumnValue(col tableColumnMeta, raw any) (any, error) {
	if raw == nil {
		return nil, nil
	}
	if tag := wireTag(col.TypeOID); tag != "" {
		cell, ok := taggedCellOf(raw)
		if !ok {
			return nil, fmt.Errorf("%s value must be a {\"t\":%q,\"v\":\"...\"} wire cell", col.TypeName, tag)
		}
		if cell.T != tag {
			return nil, fmt.Errorf("wire tag %q does not match column type %s (expected %q)", cell.T, col.TypeName, tag)
		}
		return decodeTagged(cell)
	}
	if col.TypeOID == oidJSON || col.TypeOID == oidJSONB {
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("%s value must be a string containing JSON text", col.TypeName)
		}
		if !json.Valid([]byte(s)) {
			return nil, fmt.Errorf("%s value is not valid JSON text", col.TypeName)
		}
		return s, nil
	}
	switch v := raw.(type) {
	case string, bool:
		return v, nil
	case json.Number:
		return v.String(), nil
	case map[string]any:
		if cell, ok := taggedCellOf(v); ok {
			return nil, fmt.Errorf("wire tag %q does not match column type %s (it takes a plain value)", cell.T, col.TypeName)
		}
	}
	return nil, fmt.Errorf("unsupported value shape for column type %s", col.TypeName)
}

// maxMutationBody bounds every row-mutation request body (1 MiB).
const maxMutationBody = 1 << 20

// decodeJSONBody decodes a request body keeping numbers exact: json.Number
// preserves the literal digits so large keys are never rounded by float
// parsing before validation.
func decodeJSONBody(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	return dec.Decode(dst)
}

// decodeStrictJSONBody is decodeJSONBody for the v2 protocol: unknown fields
// (e.g. a forged "pkColumn" or "where") and trailing data are errors, so a
// direct client cannot smuggle intent the handler would silently ignore.
func decodeStrictJSONBody(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing data after JSON body")
	}
	return nil
}
