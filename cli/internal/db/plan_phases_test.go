package db

// Offline Q09 tests: enum-addition phases, the snapshot intermediate
// state, and server-version floors (planner refusal and plan records).

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

const q09Base = `{
	"version": 2, "dialect": "postgresql", "capabilities": [],
	"schemas": [{"name": "app"}],
	"enums": [
		{"identity": {"schema": "app", "name": "mood"}, "managed": true, "values": ["sad", "ok", "glad"]},
		{"identity": {"schema": "app", "name": "size"}, "managed": true, "values": ["s", "m"]}
	],
	"tables": [{
		"identity": {"schema": "app", "name": "tenants"},
		"managed": true,
		"columns": [
			{"name": "id", "type": {"name": "int4", "codec": "number"}, "notNull": true},
			{"name": "tone", "type": {"name": "enum", "codec": "enum", "enum": {"schema": "app", "name": "mood"}}, "notNull": false},
			{"name": "net", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false},
			{"name": "gross", "type": {"name": "numeric", "codec": "decimal-string"}, "notNull": false,
			 "generated": {"expression": "net * 2"}}
		],
		"constraints": [{"name": "tenants_pkey", "type": "primary-key", "columns": ["id"]}],
		"indexes": []
	}],
	"views": [],
	"opaque": []
}`

func q09Doc(t *testing.T, edits ...string) *V2Document {
	t.Helper()
	src := strings.NewReplacer(edits...).Replace(q09Base)
	if len(edits) > 0 && src == q09Base {
		t.Fatal("fixture edit did not apply")
	}
	doc, err := ParseV2Document([]byte(src))
	if err != nil {
		t.Fatalf("fixture must be contract-valid: %v", err)
	}
	return doc
}

var q09AddValues = []string{
	`"values": ["sad", "ok", "glad"]`, `"values": ["sad", "ok", "glad", "elated"]`,
	`"values": ["s", "m"]`, `"values": ["xs", "s", "m"]`,
}

var q09AddView = []string{
	`"views": []`, `"views": [{"identity": {"schema": "app", "name": "elated"}, "managed": true, "definition": "select id from app.tenants where tone = 'elated'"}]`,
}

func TestQ09PlanPhasesSplitsEnumAdditionsFromTheRest(t *testing.T) {
	base := q09Doc(t)
	desired := q09Doc(t, append(append([]string{}, q09AddValues...), q09AddView...)...)
	res, err := DiffV2Document(context.Background(), desired, base, DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.EnumAdditions) != 2 {
		t.Fatalf("the planner must record both enum additions, got %+v", res.EnumAdditions)
	}
	phases, err := PlanPhases(res)
	if err != nil {
		t.Fatal(err)
	}
	if len(phases) != 2 || !phases[0].EnumAdditions || phases[1].EnumAdditions {
		t.Fatalf("expected [enum additions, rest], got %+v", phases)
	}
	if len(phases[0].Up) != 2 || len(phases[0].Down) != 2 {
		t.Fatalf("enum phase must hold exactly the two additions with their downs: %+v", phases[0])
	}
	for _, stmt := range phases[0].Up {
		if !strings.Contains(stmt, " add value ") {
			t.Fatalf("enum phase holds a non-addition: %s", stmt)
		}
	}
	if len(phases[1].Up) != 1 || !strings.HasPrefix(phases[1].Up[0], "create view") {
		t.Fatalf("rest phase must hold the view: %v", phases[1].Up)
	}
	if len(phases[0].Warnings) != 2 {
		t.Fatalf("enum phase carries the additions' warnings: %v", phases[0].Warnings)
	}
	for _, w := range phases[1].Warnings {
		if strings.Contains(w, "will be added") {
			t.Fatalf("an enum-addition warning leaked into the rest phase: %s", w)
		}
	}
	if len(phases[0].Warnings)+len(phases[1].Warnings) != len(res.Warnings) {
		t.Fatalf("warnings must be partitioned, not dropped or duplicated: %d + %d != %d",
			len(phases[0].Warnings), len(phases[1].Warnings), len(res.Warnings))
	}

	// Enum additions alone, or no additions: one phase, the plan as is.
	onlyAdds, err := DiffV2Document(context.Background(), q09Doc(t, q09AddValues...), base, DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	if phases, _ := PlanPhases(onlyAdds); len(phases) != 1 || len(phases[0].Up) != 2 {
		t.Fatalf("an additions-only plan is one transaction, got %+v", phases)
	}
	noAdds, err := DiffV2Document(context.Background(), desired, q09Doc(t, q09AddValues...), DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	if phases, _ := PlanPhases(noAdds); len(phases) != 1 || phases[0].EnumAdditions {
		t.Fatalf("a plan without additions is one transaction, got %+v", phases)
	}
}

func TestQ09EnumAdditionsTargetIsTheIntermediateState(t *testing.T) {
	base := q09Doc(t)
	desired := q09Doc(t, append(append([]string{}, q09AddValues...), q09AddView...)...)
	mid, err := EnumAdditionsTarget(base, desired)
	if err != nil {
		t.Fatal(err)
	}
	if mid.SHA256Hex == base.SHA256Hex || mid.SHA256Hex == desired.SHA256Hex {
		t.Fatal("the intermediate state differs from both ends")
	}
	// Independent oracle: planning base -> intermediate yields exactly the
	// additions; intermediate -> desired yields exactly the rest.
	first, err := DiffV2Document(context.Background(), mid, base, DiffV2Options{SnapshotBase: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Up) != 2 || len(first.EnumAdditions) != 2 {
		t.Fatalf("base -> intermediate must be the two additions, got %v", first.Up)
	}
	second, err := DiffV2Document(context.Background(), desired, mid, DiffV2Options{SnapshotBase: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Up) != 1 || !strings.HasPrefix(second.Up[0], "create view") || len(second.EnumAdditions) != 0 {
		t.Fatalf("intermediate -> desired must be the view only, got %v", second.Up)
	}
	// The value order is the desired order ("xs" before "s").
	m, err := ModelFromRoot(mid.Root)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(m.Enum(V2Identity{Schema: "app", Name: "size"}).Values, ","); got != "xs,s,m" {
		t.Fatalf("intermediate size values = %s", got)
	}
}

func TestQ09SetExpressionServerFloor(t *testing.T) {
	base := q09Doc(t)
	desired := q09Doc(t, `"expression": "net * 2"`, `"expression": "net * 3"`)

	// A known older server: refused at plan time, with the fix named.
	_, err := DiffV2Document(context.Background(), desired, base, DiffV2Options{ServerMajor: 16})
	if err == nil {
		t.Fatal("PostgreSQL 16 cannot run SET EXPRESSION; the planner must refuse")
	}
	for _, want := range []string{"gross", "SET EXPRESSION (PostgreSQL 17+)", "PostgreSQL 16", "--allow-destructive", "add it back"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal must mention %q: %v", want, err)
		}
	}

	// A capable server, or an unknown one (offline): planned and recorded.
	for _, major := range []int{17, 18, 0} {
		res, err := DiffV2Document(context.Background(), desired, base, DiffV2Options{ServerMajor: major})
		if err != nil {
			t.Fatalf("server %d: %v", major, err)
		}
		if len(res.Up) != 1 || !strings.Contains(res.Up[0], "set expression as (net * 3)") {
			t.Fatalf("server %d: expected the SET EXPRESSION statement, got %v", major, res.Up)
		}
		plan, err := BuildPlanArtifact("002", "expr", "001_init", base.SHA256Hex, desired, nil, res)
		if err != nil {
			t.Fatal(err)
		}
		if plan.MinServerMajor != 17 || plan.Operations[0].MinServerMajor != 17 {
			t.Fatalf("plan must record the PostgreSQL 17 floor: plan %d, op %d", plan.MinServerMajor, plan.Operations[0].MinServerMajor)
		}
		raw, err := MarshalPlanJSON(plan)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Count(string(raw), `"minServerMajor": 17`) != 2 {
			t.Fatalf("plan.json must carry the floor on the plan and the operation:\n%s", raw)
		}
	}

	// Plans without a gated statement keep their bytes: no field at all.
	plain, err := DiffV2Document(context.Background(), q09Doc(t, q09AddView...), base, DiffV2Options{})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlanArtifact("002", "view", "001_init", base.SHA256Hex, q09Doc(t, q09AddView...), nil, plain)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := MarshalPlanJSON(plan)
	if strings.Contains(string(raw), "minServerMajor") {
		t.Fatalf("an ungated plan must not mention minServerMajor:\n%s", raw)
	}

	// A plan written by this version round-trips through the strict reader.
	gated, _ := DiffV2Document(context.Background(), desired, base, DiffV2Options{})
	gplan, _ := BuildPlanArtifact("002", "expr", "001_init", base.SHA256Hex, desired, nil, gated)
	graw, _ := MarshalPlanJSON(gplan)
	var back PlanArtifact
	dec := json.NewDecoder(strings.NewReader(string(graw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&back); err != nil || back.MinServerMajor != 17 {
		t.Fatalf("strict decode of a gated plan: %v (floor %d)", err, back.MinServerMajor)
	}
}

func TestQ09StatementMinServerMajor(t *testing.T) {
	for _, stmt := range []string{
		`alter table "app"."t" alter column "g" set expression as ((a * 3))`,
		`ALTER TABLE app.t ALTER g SET EXPRESSION AS (a * 3)`,
		`alter table app.t /* c */ alter column g set /* c */ expression as (a)`,
	} {
		if n, feature := StatementMinServerMajor(stmt); n != 17 || feature == "" {
			t.Fatalf("%s: floor %d %q, want 17", stmt, n, feature)
		}
	}
	for _, stmt := range []string{
		`alter table app.t alter column g drop expression`,
		`alter table app.t add column g numeric generated always as (a * 2) stored`,
		`-- alter table app.t alter column g set expression as (a)`,
		`insert into notes (body) values ('alter table x alter column y set expression as (1)')`,
	} {
		if n, _ := StatementMinServerMajor(stmt); n != 0 {
			t.Fatalf("%s: no floor expected, got %d", stmt, n)
		}
	}
}

func TestQ09ParseServerMajor(t *testing.T) {
	for in, want := range map[string]int{
		"17.11 (Homebrew)":                 17,
		"16.15 (Debian 16.15-1.pgdg120+1)": 16,
		"18.0":                             18,
		"18beta1":                          18,
		"":                                 0,
		"PostgreSQL":                       0,
	} {
		if got := parseServerMajor(in); got != want {
			t.Fatalf("parseServerMajor(%q) = %d, want %d", in, got, want)
		}
	}
}
