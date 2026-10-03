package studio

import (
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// collectLosslessRow reads JSON documents into their native text bytes instead
// of pgx's default any/float64 JSON decoder. A nil byte slice is SQL NULL;
// the nonnil bytes "null" are a JSON document. Other OIDs retain existing
// driver-native scalar decoders. JSON mutation inputs remain exact text.
func collectLosslessRow(rows pgx.Rows) ([]any, error) {
	fields := rows.FieldDescriptions()
	hasJSON := false
	for _, field := range fields {
		if field.DataTypeOID == oidJSON || field.DataTypeOID == oidJSONB {
			hasJSON = true
		}
	}
	values := make([]any, len(fields))
	if hasJSON {
		documents := make([][]byte, len(fields))
		destinations := make([]any, len(fields))
		for index, field := range fields {
			if field.DataTypeOID == oidJSON || field.DataTypeOID == oidJSONB {
				destinations[index] = &documents[index]
			} else {
				destinations[index] = &values[index]
			}
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}
		for index, field := range fields {
			if field.DataTypeOID == oidJSON || field.DataTypeOID == oidJSONB {
				if documents[index] == nil {
					values[index] = nil
					continue
				}
				if !validNativeJSON(documents[index]) {
					return nil, errors.New("invalid native JSON document")
				}
				tag := "json"
				if field.DataTypeOID == oidJSONB {
					tag = "jsonb"
				}
				values[index] = taggedCell{T: tag, V: string(documents[index])}
			}
		}
	} else {
		var err error
		values, err = rows.Values()
		if err != nil {
			return nil, err
		}
	}
	if len(values) != len(fields) {
		return nil, errors.New("native result shape differs")
	}
	for index, field := range fields {
		if field.DataTypeOID != oidJSON && field.DataTypeOID != oidJSONB {
			values[index] = encodeTaggedCell(field.DataTypeOID, values[index])
		}
	}
	return values, nil
}

// encoding/json accepts invalid UTF-8 and replaces it during marshaling. Refuse
// that input rather than silently changing a document from a non-UTF8 source.
func validNativeJSON(document []byte) bool {
	return utf8.Valid(document) && json.Valid(document)
}
