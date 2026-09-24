package db

import (
	"strings"
	"testing"
	"time"
)

func TestParseJournaledFileNotJournaled(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE t (id int);",
		"-- ordinary comment\nCREATE TABLE t (id int);",
		"UPDATE t SET a = 1;",
	} {
		jf, err := ParseJournaledFile(sql)
		if err != nil || jf != nil {
			t.Errorf("ParseJournaledFile(%q) = (%v, %v), want (nil, nil)", sql, jf, err)
		}
	}
}

func TestParseJournaledFileStepsAndConfig(t *testing.T) {
	sql := "-- neutron:journaled\n" +
		"SET LOCAL statement_timeout = '5s';\n" +
		"-- neutron:step verify=\"SELECT count(*) FROM t WHERE x IS NULL\" expect=\"0\" lock_timeout=2s\n" +
		"UPDATE t SET x = 0 WHERE x IS NULL;\n" +
		"CREATE INDEX CONCURRENTLY i_t ON t (x);\n"

	jf, err := ParseJournaledFile(sql)
	if err != nil {
		t.Fatalf("ParseJournaledFile: %v", err)
	}
	if jf == nil || len(jf.Steps) != 2 {
		t.Fatalf("steps = %+v, want 2", jf)
	}
	s1 := jf.Steps[0]
	if s1.Kind != "update" || s1.Annotation == nil || s1.HasBuiltIn {
		t.Fatalf("step 1 wrong: %+v", s1)
	}
	if s1.Annotation.Verify != "SELECT count(*) FROM t WHERE x IS NULL" || s1.Annotation.Expect != "0" {
		t.Fatalf("step 1 annotation wrong: %+v", s1.Annotation)
	}
	if s1.Annotation.LockTimeout != "2000ms" {
		t.Fatalf("step 1 lock_timeout = %q, want 2000ms", s1.Annotation.LockTimeout)
	}
	if s1.EffectiveLockTimeout() != "2000ms" || s1.EffectiveStatementTimeout() != pgInterval(JournaledDefaultStatementTimeout) {
		t.Fatalf("step 1 effective knobs wrong: %q %q", s1.EffectiveLockTimeout(), s1.EffectiveStatementTimeout())
	}
	if len(s1.ConfigSQL) != 1 {
		t.Fatalf("step 1 config = %v, want the SET LOCAL", s1.ConfigSQL)
	}
	s2 := jf.Steps[1]
	if s2.Kind != "create index" || !s2.Concurrent() || !s2.HasBuiltIn || s2.Annotation != nil {
		t.Fatalf("step 2 wrong: %+v", s2)
	}
	if s2.BuiltIn.Kind != PostconditionIndexCreate || s2.BuiltIn.Name != "i_t" || s2.BuiltIn.Table != "t" {
		t.Fatalf("step 2 built-in wrong: %+v", s2.BuiltIn)
	}
	if len(s2.ConfigSQL) != 0 {
		t.Fatalf("step 2 config = %v, want none", s2.ConfigSQL)
	}
}

func TestParseJournaledFileDefaults(t *testing.T) {
	sql := "-- neutron:journaled\nCREATE TABLE t (id int);\n"
	jf, err := ParseJournaledFile(sql)
	if err != nil {
		t.Fatalf("ParseJournaledFile: %v", err)
	}
	s := jf.Steps[0]
	if !s.HasBuiltIn {
		t.Fatal("CREATE TABLE must have a built-in postcondition")
	}
	if s.EffectiveLockTimeout() != pgInterval(JournaledDefaultLockTimeout) {
		t.Fatalf("default lock_timeout = %q, want %q", s.EffectiveLockTimeout(), pgInterval(JournaledDefaultLockTimeout))
	}
	if s.EffectiveStatementTimeout() != "0ms" {
		t.Fatalf("default statement_timeout = %q, want 0ms", s.EffectiveStatementTimeout())
	}
}

func TestPgIntervalRendering(t *testing.T) {
	// Go's canonical duration rendering is NOT valid PostgreSQL interval
	// syntax ("1m0s" is rejected with 22023); the runner must send
	// bare-millisecond spellings.
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{60 * time.Second, "60000ms"},
		{300 * time.Millisecond, "300ms"},
		{0, "0ms"},
		{90 * time.Minute, "5400000ms"},
	} {
		if got := pgInterval(tc.d); got != tc.want {
			t.Errorf("pgInterval(%v) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

func TestParseJournaledFileRefusals(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{"dml without verification", "-- neutron:journaled\nUPDATE t SET a = 1;", "no verifiable effect"},
		{"alter without verification", "-- neutron:journaled\nALTER TABLE t ADD COLUMN b int;", "no verifiable effect"},
		{"verify not a select", "-- neutron:journaled\n-- neutron:step verify=\"UPDATE t SET a = 1\" expect=\"1\"\nUPDATE t SET a = 1;", "must be a single SELECT"},
		{"verify without expect", "-- neutron:journaled\n-- neutron:step verify=\"SELECT 1\"\nUPDATE t SET a = 1;", "without expect"},
		{"annotation without marker", "-- neutron:step verify=\"SELECT 1\" expect=\"1\"\nUPDATE t SET a = 1;", "before any `-- neutron:journaled` marker"},
		{"marker after statements", "CREATE TABLE t (id int);\n-- neutron:journaled\nUPDATE t SET a = 1;", "must precede"},
		{"duplicate marker", "-- neutron:journaled\n-- neutron:journaled\nCREATE TABLE t (id int);", "duplicate"},
		{"dangling annotation", "-- neutron:journaled\n-- neutron:step verify=\"SELECT 1\" expect=\"1\"", "dangling"},
		{"set local without step", "-- neutron:journaled\nSET LOCAL lock_timeout = '1s';", "no following step"},
		{"set local before concurrent step", "-- neutron:journaled\nSET LOCAL lock_timeout = '1s';\nCREATE INDEX CONCURRENTLY i ON t (a);", "cannot attach to a concurrent-index step"},
		{"unknown directive", "-- neutron:journald\nCREATE TABLE t (id int);", "unknown journal directive"},
		{"unknown annotation key", "-- neutron:journaled\n-- neutron:step verify=\"SELECT 1\" expect=\"1\" timeout=2s\nUPDATE t SET a = 1;", "unknown annotation key"},
		{"bad duration", "-- neutron:journaled\n-- neutron:step verify=\"SELECT 1\" expect=\"1\" lock_timeout=soon\nUPDATE t SET a = 1;", "not a duration"},
		{"sub-millisecond duration", "-- neutron:journaled\n-- neutron:step verify=\"SELECT 1\" expect=\"1\" lock_timeout=100us\nUPDATE t SET a = 1;", "below 1ms"},
		{"bad on-failure", "-- neutron:journaled\n-- neutron:step verify=\"SELECT 1\" expect=\"1\" on-failure=continue\nUPDATE t SET a = 1;", "on-failure"},
		{"two annotations one statement", "-- neutron:journaled\n-- neutron:step verify=\"SELECT 1\" expect=\"1\"\n-- neutron:step verify=\"SELECT 2\" expect=\"2\"\nUPDATE t SET a = 1;", "one statement"},
		{"annotation on built-in statement needs verify if declared", "-- neutron:journaled\n-- neutron:step expect=\"1\"\nCREATE TABLE t (id int);", "declares no verify"},
		{"unterminated quote", "-- neutron:journaled\n-- neutron:step verify=\"SELECT 1 expect=\"1\"\nUPDATE t SET a = 1;", "unterminated"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseJournaledFile(tc.sql)
			if err == nil {
				t.Fatalf("ParseJournaledFile accepted invalid input: %s", tc.sql)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q missing %q", err.Error(), tc.want)
			}
		})
	}
}

func TestParseJournaledFileCommentsCannotHideStatements(t *testing.T) {
	// The annotation grammar lives in line comments; statements after it
	// still classify through the tokenizer, and out-of-allowlist kinds are
	// refused at journal validation with the batch.
	sql := "-- neutron:journaled\n-- neutron:step verify=\"SELECT 1\" expect=\"1\"\nVACUUM t;"
	if _, err := ParseJournaledFile(sql); err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("VACUUM in journaled file: err = %v, want allowlist refusal", err)
	}
}

func TestRefuseJournaledDown(t *testing.T) {
	if err := RefuseJournaledDown("DROP INDEX IF EXISTS i;"); err != nil {
		t.Fatalf("plain down refused: %v", err)
	}
	err := RefuseJournaledDown("-- neutron:journaled\nDROP INDEX IF EXISTS i;")
	if err == nil || !strings.Contains(err.Error(), "down file") {
		t.Fatalf("journaled down not refused: %v", err)
	}
}

func TestParseJournaledFileVerifyEscapes(t *testing.T) {
	sql := "-- neutron:journaled\n" +
		"-- neutron:step verify=\"SELECT count(*) FROM t WHERE note != 'done\\\"'\" expect=\"0\"\n" +
		"UPDATE t SET note = 'done\"' WHERE note IS NULL;"
	jf, err := ParseJournaledFile(sql)
	if err != nil {
		t.Fatalf("ParseJournaledFile: %v", err)
	}
	if got := jf.Steps[0].Annotation.Verify; got != "SELECT count(*) FROM t WHERE note != 'done\"'" {
		t.Fatalf("escaped verify parsed as %q", got)
	}
}

func TestJournaledVerifyErrorMessages(t *testing.T) {
	e := &JournaledVerifyError{StepIndex: 2, FirstLine: "UPDATE t", Verify: "SELECT 1", Expect: "1", Got: "2"}
	if !strings.Contains(e.Error(), `returned "2", want "1"`) {
		t.Fatalf("mismatch message wrong: %s", e.Error())
	}
}

func TestJournaledIdentityErrorMessage(t *testing.T) {
	e := &JournaledIdentityError{StepIndex: 3, FirstLine: "CREATE INDEX CONCURRENTLY i ON t (id)", Detail: "the unqualified table \"t\" cannot be resolved"}
	msg := e.Error()
	for _, want := range []string{"journaled step 3", "CREATE INDEX CONCURRENTLY i ON t (id)", "cannot be resolved"} {
		if !strings.Contains(msg, want) {
			t.Errorf("identity refusal message missing %q: %s", want, msg)
		}
	}
}
