package main

import (
	"bytes"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestGeneratorNativeCodecProfile(t *testing.T) {
	dir := t.TempDir()
	source := `package model
import sqlorm "github.com/neutron-build/neutron/go/orm"
type Custom int64
type Record struct {
 ID int64 ` + "`db:\"id\"`" + `
 Bytes sqlorm.Bytea ` + "`db:\"bytes\"`" + `
 List *sqlorm.Array[*sqlorm.JSON] ` + "`db:\"list,nullable\"`" + `
 Span sqlorm.Range[sqlorm.Decimal] ` + "`db:\"span\"`" + `
 Day sqlorm.Date ` + "`db:\"day\"`" + `
 Clock sqlorm.TimeOfDay ` + "`db:\"clock\"`" + `
 Duration sqlorm.Interval ` + "`db:\"duration\"`" + `
 State sqlorm.Enum ` + "`db:\"state\"`" + `
 Custom sqlorm.SQLValue[Custom] ` + "`db:\"custom\"`" + `
}`
	if err := os.WriteFile(filepath.Join(dir, "model.go"), []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	generated, err := generate(dir, "Record")
	if err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{"sqlorm.Bytea", "*sqlorm.Array[*sqlorm.JSON]", "sqlorm.Range[sqlorm.Decimal]", "sqlorm.Date", "sqlorm.TimeOfDay", "sqlorm.Interval", "sqlorm.Enum", "sqlorm.SQLValue[Custom]"} {
		if !strings.Contains(string(generated), "sqlorm.Column[Record, "+typ+"]") {
			t.Fatal("qualified codec field absent", typ)
		}
	}
}

func TestGeneratorCLICheckAndSourcePreservation(t *testing.T) {
	if os.Getenv("NEUTRON_ORMGEN_CLI_CHILD") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append([]string{"neutron-ormgen"}, os.Args[i+1:]...)
				break
			}
		}
		flag.CommandLine = flag.NewFlagSet("neutron-ormgen", flag.ExitOnError)
		main()
		return
	}
	dir := t.TempDir()
	model := filepath.Join(dir, "model.go")
	source := "package model\ntype Record struct {ID int64 `db:\"id\"`}\n"
	if err := os.WriteFile(model, []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	args := []string{"-dir", dir, "-type", "Record", "-out", "record.gen.go"}
	run := func(extra ...string) ([]byte, error) {
		argv := append([]string{"-test.run=^TestGeneratorCLICheckAndSourcePreservation$", "--"}, args...)
		argv = append(argv, extra...)
		command := exec.Command(os.Args[0], argv...)
		command.Env = append(os.Environ(), "NEUTRON_ORMGEN_CLI_CHILD=1")
		return command.CombinedOutput()
	}
	if output, err := run(); err != nil {
		t.Fatal("CLI generate", string(output))
	}
	if output, err := run("-check"); err != nil {
		t.Fatal("CLI check", string(output))
	}
	generated := filepath.Join(dir, "record.gen.go")
	before, err := os.ReadFile(generated)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(model, []byte(strings.Replace(source, "db:\"id\"", "db:\"changed\"", 1)), 0644); err != nil {
		t.Fatal(err)
	}
	if output, err := run("-check"); err == nil || !strings.Contains(string(output), "model drift") {
		t.Fatal("CLI drift not refused", string(output))
	}
	after, err := os.ReadFile(generated)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("-check mutated generated artifact", err)
	}
	if err := os.WriteFile(generated, []byte("package model\n// user owned\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if output, err := run(); err == nil || !strings.Contains(string(output), "non-generated source") {
		t.Fatal("user source overwrite not refused", string(output))
	}
}

func TestGeneratorDeterminismTagsAndDrift(t *testing.T) {
	dir := t.TempDir()
	source := "package model\nimport \"time\"\ntype Record struct { ID int64 `db:\"id\"`; Name string `db:\"name\"`; When *time.Time `db:\"when,nullable\"`; Ignored any `db:\"-\"` }\n"
	if err := os.WriteFile(filepath.Join(dir, "model.go"), []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	first, err := generate(dir, "Record")
	if err != nil {
		t.Fatal(err)
	}
	second, err := generate(dir, "Record")
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("nondeterministic generation", err)
	}
	if !strings.Contains(string(first), "orm.Column[Record, int64]") || !strings.Contains(string(first), "orm.Column[Record, *time.Time]") || strings.Contains(string(first), "orm.Column[Record, any]") || !strings.Contains(string(first), "CheckRecordModel") {
		t.Fatal(string(first))
	}
	changed := strings.Replace(source, "db:\"id\"", "db:\"new_id\"", 1)
	if err := os.WriteFile(filepath.Join(dir, "model.go"), []byte(changed), 0644); err != nil {
		t.Fatal(err)
	}
	drifted, err := generate(dir, "Record")
	if err != nil || bytes.Equal(first, drifted) {
		t.Fatal("schema-tag drift not reflected in output", err)
	}
	for _, body := range []string{
		"A int64 `db:\"same\"`; B int64 `db:\"same\"`",
		"A *int64 `db:\"a\"`",
		"A int64 `db:\"a,nullable\"`",
		"A []string `db:\"a\"`",
		"A int64",
		"int64 `db:\"a\"`",
	} {
		if err := os.WriteFile(filepath.Join(dir, "model.go"), []byte("package model\ntype Record struct {"+body+"}\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := generate(dir, "Record"); err == nil {
			t.Fatal("unsupported metadata generated", body)
		}
	}
}

// Compile the generated artifact in a clean consumer module, rather than only
// matching template strings. It links the actual SDK module as its dependency.
func TestGeneratedOutsideModuleConsumerAndRuntimeDrift(t *testing.T) {
	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	sdk := filepath.Clean(filepath.Join(cwd, "..", ".."))
	manifest := "module generated-consumer\n\ngo 1.26.0\n\nrequire " + ormPath[:strings.LastIndex(ormPath, "/")] + " v0.0.0\nreplace " + ormPath[:strings.LastIndex(ormPath, "/")] + " => " + strconvQuotePath(sdk) + "\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(manifest), 0644); err != nil {
		t.Fatal(err)
	}
	model := "package model\ntype Record struct {ID int64 `db:\"id\"`;Name string `db:\"name\"`}\n"
	if err := os.WriteFile(filepath.Join(dir, "model.go"), []byte(model), 0644); err != nil {
		t.Fatal(err)
	}
	generated, err := generate(dir, "Record")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "columns_gen.go"), generated, 0644); err != nil {
		t.Fatal(err)
	}
	consumer := "package model\nimport (\"testing\";\"context\";orm \"" + ormPath + "\")\nfunc typed(ctx context.Context,db orm.Executor)error { table,columns,err:=NewRecordTable(\"tenant\",\"records\");if err!=nil{return err};var names []string;names,err=orm.SelectColumn(ctx,db,columns.Name,orm.Query[Record]{}.Where(columns.ID.Eq(1)));_=table;_=names;return err }\nfunc TestModelShape(t *testing.T){if err:=CheckRecordModel();err!=nil{t.Fatal(err)}}\n"
	path := filepath.Join(dir, "consumer_test.go")
	if err := os.WriteFile(path, []byte(consumer), 0644); err != nil {
		t.Fatal(err)
	}
	run := func() ([]byte, error) {
		cmd := exec.Command("go", "test", "-mod=mod", ".")
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOWORK=off")
		return cmd.CombinedOutput()
	}
	if output, err := run(); err != nil {
		t.Fatalf("generated public consumer failed: %s", output)
	}
	invalid := strings.Replace(consumer, "columns.ID.Eq(1)", "columns.Name.Eq(1)", 1)
	if err := os.WriteFile(path, []byte(invalid), 0644); err != nil {
		t.Fatal(err)
	}
	if output, err := run(); err == nil || !strings.Contains(string(output), "as string value") {
		t.Fatalf("generated value type misuse did not fail compilation: %s", output)
	}
	invalid = strings.Replace(consumer, "columns.ID.Eq(1)", "columns.Missing.Eq(1)", 1)
	if err := os.WriteFile(path, []byte(invalid), 0644); err != nil {
		t.Fatal(err)
	}
	if output, err := run(); err == nil || !strings.Contains(string(output), "Missing undefined") {
		t.Fatalf("generated nonexistent field did not fail compilation: %s", output)
	}
	if err := os.WriteFile(path, []byte(consumer), 0644); err != nil {
		t.Fatal(err)
	}
	drift := strings.Replace(model, "db:\"id\"", "db:\"changed_id\"", 1)
	if err := os.WriteFile(filepath.Join(dir, "model.go"), []byte(drift), 0644); err != nil {
		t.Fatal(err)
	}
	if output, err := run(); err == nil || !strings.Contains(string(output), "mapped field drift") {
		t.Fatalf("stale generated model metadata was accepted: %s", output)
	}
}
func strconvQuotePath(path string) string { return strconv.Quote(path) }
