package db

import (
	"errors"
	"strings"
	"testing"
)

func backfillFixtureSpec() BackfillSpec {
	return BackfillSpec{1, "job", "copy-column-v1", V2Identity{"public", "source"}, V2Identity{"public", "progress"}, "id", "src", "dst", strings.Repeat("a", 64), 2, 3000}
}
func TestBackfillSpecificationBoundaries(t *testing.T) {
	if err := backfillSpecValid(backfillFixtureSpec()); err != nil {
		t.Fatal(err)
	}
	tests := []func(*BackfillSpec){func(s *BackfillSpec) { s.Transformation = "arbitrary-sql" }, func(s *BackfillSpec) { s.BatchRows = 10001 }, func(s *BackfillSpec) { s.TimeoutMilliseconds = 60001 }, func(s *BackfillSpec) { s.To = s.Key }, func(s *BackfillSpec) { s.To = s.From }, func(s *BackfillSpec) { s.Checkpoint = s.Source }, func(s *BackfillSpec) { s.WriterPolicySHA256 = "" }}
	for _, bad := range tests {
		s := backfillFixtureSpec()
		bad(&s)
		if err := backfillSpecValid(s); err == nil {
			t.Fatal("unsafe spec accepted")
		}
	}
	if backfillName(V2Identity{"schema", "a\"b"}) != `"schema"."a""b"` {
		t.Fatal("qualified name not quoted")
	}
}
func TestBackfillCommitAmbiguityAndSafeError(t *testing.T) {
	cause := errors.New("secret database credential")
	err := &BackfillError{Operation: "commit", Cause: cause, Indeterminate: true}
	if !errors.Is(err, cause) || strings.Contains(err.Error(), "secret") || !strings.Contains(err.Error(), "indeterminate") {
		t.Fatal("error outcome/cause redaction contract")
	}
}
