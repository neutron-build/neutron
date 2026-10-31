package db

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseBackfillSpecStrictArtifact(t *testing.T) {
	raw, err := json.Marshal(backfillFixtureSpec())
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseBackfillSpec(raw)
	if err != nil || parsed.JobID != "job" {
		t.Fatal("valid spec refused", err)
	}
	for _, invalid := range []string{
		strings.Replace(string(raw), `"jobId":"job"`, `"jobId":"job","JobID":"other"`, 1), strings.Replace(string(raw), `"schema":"public"`, `"Schema":"public"`, 1), string(raw) + ` {}`, `null`, strings.Replace(string(raw), `"version":1`, `"version":1,"version":1`, 1), strings.Replace(string(raw), `"version":1`, `"version":2`, 1), strings.Replace(string(raw), `"version":1`, `"version":1,"unknown":true`, 1), strings.Replace(string(raw), `"batchRows":2`, `"batchRows":2.5`, 1), strings.Replace(string(raw), `"jobId":"job"`, `"jobId":"001"`, 1) + ` []`, strings.Repeat(" ", 1<<20) + string(raw),
	} {
		if _, err := ParseBackfillSpec([]byte(invalid)); err == nil {
			t.Fatal("invalid spec admitted")
		}
	}
	exact := strings.Replace(string(raw), `"jobId":"job"`, `"jobId":"001"`, 1)
	parsed, err = ParseBackfillSpec([]byte(exact))
	if err != nil || parsed.JobID != "001" {
		t.Fatal("text identity changed", err)
	}
}
