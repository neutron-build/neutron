package db

// Q10: a planned column rename is compared against the live expressions as
// PostgreSQL itself rewrites them on RENAME COLUMN (RenameTable), never by
// rewriting expression text.

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// q10Renamed: the q09Base table with net renamed to amount.
var q10Renamed = []string{
	`{"name": "net", "type"`, `{"name": "amount", "type"`,
	`"expression": "net * 2"`, `"expression": "amount * 2"`,
}

// q10RenameNormalizer returns tables unchanged, records RenameTable's
// renames and answers it with the given generation expression (or fails).
type q10RenameNormalizer struct {
	renamed string
	fail    bool
	got     *map[string]string
}

func (n q10RenameNormalizer) NormalizeTable(_ context.Context, t V2Table) (V2Table, error) {
	return t, nil
}

func (n q10RenameNormalizer) NormalizeView(_ context.Context, v V2View) (V2View, error) {
	return v, nil
}

func (n q10RenameNormalizer) RenameTable(_ context.Context, t V2Table, renames map[string]string) (V2Table, error) {
	if n.got != nil {
		*n.got = renames
	}
	if n.fail {
		return t, errors.New("twin rename failed")
	}
	out := t
	out.Columns = append([]V2Column(nil), t.Columns...)
	for i := range out.Columns {
		if out.Columns[i].Generated != nil {
			out.Columns[i].Generated = &V2Generated{Expression: n.renamed}
		}
	}
	return out, nil
}

func (q10RenameNormalizer) Close() {}

func TestQ10RenamedLiveExpressionsDecideTheComparison(t *testing.T) {
	base := q09Doc(t)
	desired := q09Doc(t, q10Renamed...)
	opts := func(n V2Normalizer, major int) DiffV2Options {
		return DiffV2Options{Renames: map[string]string{"app.tenants.amount": "net"}, Normalizer: n, ServerMajor: major}
	}

	// The live text after the rename equals the desired text: only the
	// rename is planned, on PostgreSQL 16 too.
	var got map[string]string
	res, err := DiffV2Document(context.Background(), desired, base, opts(q10RenameNormalizer{renamed: "amount * 2", got: &got}, 16))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Up) != 1 || res.Up[0] != `alter table "app"."tenants" rename column "net" to "amount"` || len(res.Warnings) != 0 {
		t.Fatalf("a rename alone plans only the rename: up=%q warnings=%q", res.Up, res.Warnings)
	}
	if len(got) != 1 || got["net"] != "amount" {
		t.Fatalf("RenameTable must receive live name -> new name, got %v", got)
	}

	// A real change still differs after the rename; the down statement
	// restores the renamed live text (it runs before the rename back).
	res, err = DiffV2Document(context.Background(), desired, base, opts(q10RenameNormalizer{renamed: "amount * 3"}, 17))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Up) != 2 || !strings.Contains(res.Up[1], "set expression as (amount * 2)") || !strings.Contains(res.Down[1], "set expression as (amount * 3)") {
		t.Fatalf("a real change plans SET EXPRESSION with a renamed down: up=%q down=%q", res.Up, res.Down)
	}
	if _, err := DiffV2Document(context.Background(), desired, base, opts(q10RenameNormalizer{renamed: "amount * 3"}, 16)); err == nil ||
		!strings.Contains(err.Error(), `generated column "gross" changes its expression`) {
		t.Fatalf("a verified change is refused as a change on PostgreSQL 16: %v", err)
	}

}

// Without the renamed live text (offline planning, or a failed twin) a
// difference cannot be told from the rename, and its down statement would
// name the old column before the rename is reverted: the plan is refused
// with the fix for the mode (Q11), on every server version.
func TestQ10UnrenamedComparisonIsRefused(t *testing.T) {
	base := q09Doc(t)
	desired := q09Doc(t, q10Renamed...)
	renames := map[string]string{"app.tenants.amount": "net"}
	const hint = "predates the rename of net to amount"
	const element = `column gross generation expression: "amount * 2" (desired) vs "net * 2" (live)`

	_, err := DiffV2Document(context.Background(), desired, base, DiffV2Options{Renames: renames})
	if err == nil || !strings.Contains(err.Error(), hint) || !strings.Contains(err.Error(), element) ||
		!strings.Contains(err.Error(), "Plan it as two migrations instead: first generate one that keeps the column under its old name (net) and leaves out the elements listed below, without --rename") || strings.Contains(err.Error(), "live normalizer") {
		t.Fatalf("offline planning must refuse, name the rename and the offline fix: %v", err)
	}

	// A live run whose rename twin failed already had a normalizer, and the
	// database spells the expression with the old name: the advice is the
	// hand rename, never "write it as the database spells it" or "re-run
	// with a live normalizer".
	const byHand = `Rename the column by hand first: alter table "app"."tenants" rename column "net" to "amount" (PostgreSQL rewrites the expressions that reference it), then plan the remaining changes again, leaving out the rename of net to amount (the database already holds the new name; keep any other renames)`
	for _, major := range []int{16, 17} {
		_, err = DiffV2Document(context.Background(), desired, base, DiffV2Options{Renames: renames, Normalizer: q10RenameNormalizer{fail: true}, ServerMajor: major})
		if err == nil {
			t.Fatalf("PostgreSQL %d: a failed rename twin must refuse the plan", major)
		}
		if msg := err.Error(); !strings.Contains(msg, hint) || !strings.Contains(msg, element) || !strings.Contains(msg, byHand) ||
			strings.Contains(msg, "spells it") || strings.Contains(msg, "live normalizer") || strings.Contains(msg, "--rename") {
			t.Fatalf("PostgreSQL %d: a live refusal must name the hand rename, and no CLI flag (Studio has none):\n%s", major, msg)
		}
	}
}

// The live twin: PostgreSQL renames the twin's columns and rewrites every
// expression that references them; literals, function names and other
// columns keep their text, the live table and its column names stay as
// they are.
func TestQ10RenameTableTwinLive(t *testing.T) {
	h := newQ07Harness(t, "q10twin")
	h.exec(`CREATE SCHEMA app`)
	h.exec(`CREATE TABLE app.t (
		id int4 PRIMARY KEY,
		abs numeric,
		"Net" numeric,
		label text,
		gross numeric GENERATED ALWAYS AS (abs(abs) + "Net" + length('abs')) STORED,
		CONSTRAINT t_c CHECK ("Net" > 0 AND label <> 'Net' AND label <> 'abs')
	)`)
	h.exec(`CREATE INDEX t_expr_idx ON app.t (abs(abs), label) INCLUDE ("Net") WHERE "Net" > 1 AND label <> 'Net'`)
	actual, err := h.client.IntrospectV2(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	model, err := ModelFromRoot(actual.Root)
	if err != nil {
		t.Fatal(err)
	}
	live := model.Table(V2Identity{Schema: "app", Name: "t"})
	norm, err := h.client.NewTwinNormalizer(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer norm.Close()

	got, err := norm.RenameTable(context.Background(), *live, map[string]string{"abs": "val", "Net": "amount"})
	if err != nil {
		t.Fatal(err)
	}
	if names := strings.Join(columnNames(got), ","); names != "id,abs,Net,label,gross" {
		t.Fatalf("column names must stay the live ones, got %s", names)
	}
	if g := got.Column("gross").Generated.Expression; g != `((abs(val) + amount) + (length('abs'::text))::numeric)` {
		t.Fatalf("generation expression after the rename: %s", g)
	}
	if c := *got.Constraint("t_c").Expression; c != `((amount > (0)::numeric) AND (label <> 'Net'::text) AND (label <> 'abs'::text))` {
		t.Fatalf("check after the rename: %s", c)
	}
	idx := got.Index("t_expr_idx")
	if k := *idx.Key[0].Expression; k != `abs(val)` || idx.Key[1].Column == nil || *idx.Key[1].Column != "label" {
		t.Fatalf("index key after the rename: %+v", idx.Key)
	}
	if len(idx.Include) != 1 || idx.Include[0] != "amount" {
		t.Fatalf("index INCLUDE after the rename: %q", idx.Include)
	}
	if w := *idx.Where; w != `((amount > (1)::numeric) AND (label <> 'Net'::text))` {
		t.Fatalf("index predicate after the rename: %s", w)
	}
	if live.Column("gross").Generated.Expression == got.Column("gross").Generated.Expression {
		t.Fatal("the live table value must not be modified in place")
	}
	if v := h.queryOne(`SELECT string_agg(attname, ',' ORDER BY attnum) FROM pg_attribute WHERE attrelid = 'app.t'::regclass AND attnum > 0`); v != "id,abs,Net,label,gross" {
		t.Fatalf("the live table must be untouched, columns %s", v)
	}
}
