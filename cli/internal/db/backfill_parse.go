package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// ParseBackfillSpec admits a bounded versioned operator artifact. Exact text
// identities are preserved. Unknown/duplicate fields and trailing documents
// are refused before connection admission or effects.
func ParseBackfillSpec(raw []byte) (BackfillSpec, error) {
	var spec BackfillSpec
	if len(raw) > 1<<20 {
		return spec, fmt.Errorf("backfill artifact exceeds 1 MiB")
	}
	if err := v2RejectDuplicateKeys(raw); err != nil {
		return spec, err
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields == nil {
		return spec, fmt.Errorf("backfill artifact must be an object")
	}
	allowed := map[string]bool{}
	for _, name := range []string{"version", "jobId", "transformation", "source", "checkpoint", "key", "from", "to", "writerPolicySha256", "batchRows", "timeoutMilliseconds"} {
		allowed[name] = true
	}
	for name, value := range fields {
		if !allowed[name] {
			return spec, fmt.Errorf("unknown backfill artifact field")
		}
		if name == "source" || name == "checkpoint" {
			var identity map[string]json.RawMessage
			if json.Unmarshal(value, &identity) != nil || identity == nil {
				return spec, fmt.Errorf("backfill identity must be an object")
			}
			for field := range identity {
				if field != "schema" && field != "name" {
					return spec, fmt.Errorf("unknown backfill identity field")
				}
			}
		}
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		return BackfillSpec{}, fmt.Errorf("invalid backfill artifact")
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return BackfillSpec{}, fmt.Errorf("trailing backfill artifact data")
	}
	if err := backfillSpecValid(spec); err != nil {
		return BackfillSpec{}, err
	}
	return spec, nil
}
