package db

// Schema contract v2 golden fixtures and canonicalization tests.
//
// The fixtures live in the tracked contracts/data/golden/ tree and are shared
// with the TypeScript reference consumer (contracts/data/consumer.ts), which
// recomputes the same canonical bytes and hashes. Expected values in
// golden/manifest.json are recorded from this Go implementation via
// `go test ./internal/db -run TestV2Contract -update-golden` and must then be
// reproduced byte-for-byte by the TypeScript side.

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const goldenDir = "../../../contracts/data/golden"

type goldenManifest struct {
	Valid []struct {
		Name       string `json:"name"`
		Document   string `json:"document"`
		Canonical  string `json:"canonical"`
		SHA256     string `json:"sha256"`
	} `json:"valid"`
	Invalid []struct {
		Name     string `json:"name"`
		Document string `json:"document"`
		Code     string `json:"code"`
	} `json:"invalid"`
	V1 []struct {
		Name      string `json:"name"`
		Document  string `json:"document"`
		Canonical string `json:"canonical,omitempty"`
		SHA256    string `json:"sha256,omitempty"`
		Code      string `json:"code,omitempty"`
	} `json:"v1"`
}

var updateGolden = flag.Bool("update-golden", false, "rewrite expected canonical bytes and hashes in contracts/data/golden")

func loadGoldenManifest(t *testing.T) goldenManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(goldenDir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m goldenManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if len(m.Valid) == 0 || len(m.Invalid) == 0 || len(m.V1) == 0 {
		t.Fatalf("manifest must list valid, invalid and v1 fixtures")
	}
	return m
}

func writeGoldenManifest(t *testing.T, m *goldenManifest) {
	t.Helper()
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(filepath.Join(goldenDir, "manifest.json"), raw, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
}

func TestV2ContractGoldenValid(t *testing.T) {
	m := loadGoldenManifest(t)
	shas := map[string]string{}

	for i := range m.Valid {
		fx := &m.Valid[i]
		raw, err := os.ReadFile(filepath.Join(goldenDir, fx.Document))
		if err != nil {
			t.Fatalf("%s: %v", fx.Name, err)
		}
		doc, err := ParseV2Document(raw)
		if err != nil {
			t.Fatalf("%s: expected valid, got %v", fx.Name, err)
		}
		shas[fx.Name] = doc.SHA256Hex

		if *updateGolden {
			fx.SHA256 = doc.SHA256Hex
			if err := os.WriteFile(filepath.Join(goldenDir, fx.Canonical), doc.Canonical, 0o644); err != nil {
				t.Fatalf("%s: write canonical: %v", fx.Name, err)
			}
		}
		if fx.SHA256 == "" {
			t.Fatalf("%s: manifest has no recorded sha256 (run -update-golden once)", fx.Name)
		}
		if fx.SHA256 != doc.SHA256Hex {
			t.Errorf("%s: sha256 mismatch: manifest %s, computed %s", fx.Name, fx.SHA256, doc.SHA256Hex)
		}

		expected, err := os.ReadFile(filepath.Join(goldenDir, fx.Canonical))
		if err != nil {
			t.Fatalf("%s: read canonical: %v (run -update-golden once)", fx.Name, err)
		}
		if !bytes.Equal(expected, doc.Canonical) {
			t.Errorf("%s: canonical bytes differ from golden file\nexpected: %s\ngot:      %s", fx.Name, expected, doc.Canonical)
		}
		if double := sha256.Sum256(doc.Canonical); hex.EncodeToString(double[:]) != doc.SHA256Hex {
			t.Errorf("%s: internal hash inconsistency", fx.Name)
		}
	}
	if *updateGolden {
		writeGoldenManifest(t, &m)
		t.Logf("updated golden canonical bytes/hashes for %d valid fixtures", len(m.Valid))
	}

	// The order-insensitive fixture is a shuffled, numerically respelled
	// rewrite of basic-users: identical canonical bytes and hash. The
	// shuffle covers only genuinely unordered sets (tables, constraints,
	// indexes, object keys) — column arrays are ordered tuples and are
	// kept verbatim.
	if shas["order-insensitive"] != shas["basic-users"] {
		t.Errorf("order-insensitive (%s) must hash identically to basic-users (%s)", shas["order-insensitive"], shas["basic-users"])
	}
	basic := canonicalOf(t, "basic-users")
	shuffled := canonicalOf(t, "order-insensitive")
	if !bytes.Equal(basic, shuffled) {
		t.Errorf("order-insensitive canonical bytes must equal basic-users canonical bytes")
	}

	// Column order is semantic: the column-order fixture is basic-users
	// with rotated column arrays and must hash differently.
	if shas["column-order"] == shas["basic-users"] {
		t.Errorf("column-order must hash differently from basic-users (%s): column order is an ordered tuple", shas["basic-users"])
	}
}

func TestV2UnicodeSortIsBytewise(t *testing.T) {
	// The unicode-sort fixture carries table names with U+FFFF and a
	// non-BMP emoji. Bytewise UTF-8 order puts U+FFFF (EF BF BF) before
	// U+1F600 (F0 9F 98 80); UTF-16 code-unit order (JS default sort)
	// would put the emoji first. Both implementations must sort bytewise.
	canon := canonicalOf(t, "unicode-sort")
	var tree map[string]any
	if err := json.Unmarshal(canon, &tree); err != nil {
		t.Fatal(err)
	}
	tables := tree["tables"].([]any)
	if len(tables) != 2 {
		t.Fatalf("unicode-sort canonical must list 2 tables, got %d", len(tables))
	}
	first := tables[0].(map[string]any)["identity"].(map[string]any)["name"].(string)
	second := tables[1].(map[string]any)["identity"].(map[string]any)["name"].(string)
	if first != "zz\uffff" || second != "zz\U0001F600" {
		t.Errorf("bytewise order: got [%q, %q], want [zz\\uffff, zz\\U0001F600]", first, second)
	}
}

func canonicalOf(t *testing.T, name string) []byte {
	t.Helper()
	m := loadGoldenManifest(t)
	for _, fx := range m.Valid {
		if fx.Name != name {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(goldenDir, fx.Canonical))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return raw
	}
	t.Fatalf("fixture %q not found", name)
	return nil
}

func TestV2ContractEmptyManagedHandPinned(t *testing.T) {
	// The smallest fixture's canonical form is written by hand here, so the
	// serialization rules are pinned independently of both implementations.
	const want = `{"capabilities":[],"dialect":"postgresql","enums":[],"opaque":[],"schemas":[],"tables":[],"version":2,"views":[]}`
	raw, err := os.ReadFile(filepath.Join(goldenDir, "valid/empty-managed.json"))
	if err != nil {
		t.Fatal(err)
	}
	doc, err := ParseV2Document(raw)
	if err != nil {
		t.Fatalf("empty-managed must validate: %v", err)
	}
	if string(doc.Canonical) != want {
		t.Errorf("empty-managed canonical mismatch:\nwant: %s\ngot:  %s", want, doc.Canonical)
	}
}

func TestV2ContractGoldenInvalid(t *testing.T) {
	m := loadGoldenManifest(t)
	for _, fx := range m.Invalid {
		raw, err := os.ReadFile(filepath.Join(goldenDir, fx.Document))
		if err != nil {
			t.Fatalf("%s: %v", fx.Name, err)
		}
		_, err = ParseV2Document(raw)
		if err == nil {
			t.Errorf("%s: expected rejection with [%s], accepted instead", fx.Name, fx.Code)
			continue
		}
		if !strings.Contains(err.Error(), "["+fx.Code+"]") {
			t.Errorf("%s: expected code [%s], got %v", fx.Name, fx.Code, err)
		}
	}
}

func TestV2ContractV1Upgrade(t *testing.T) {
	m := loadGoldenManifest(t)
	for i := range m.V1 {
		fx := &m.V1[i]
		raw, err := os.ReadFile(filepath.Join(goldenDir, fx.Document))
		if err != nil {
			t.Fatalf("%s: %v", fx.Name, err)
		}
		var s Schema
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&s); err != nil {
			t.Fatalf("%s: parse v1: %v", fx.Name, err)
		}
		if err := ValidateSchemaV1ForUpgrade(&s); err != nil {
			t.Fatalf("%s: v1 fixture must pass v1-for-upgrade validation: %v", fx.Name, err)
		}
		tree, err := UpgradeV1Schema(&s)
		if fx.Code != "" {
			if err == nil {
				t.Errorf("%s: expected [%s], upgraded without error", fx.Name, fx.Code)
			} else if !strings.Contains(err.Error(), "["+fx.Code+"]") {
				t.Errorf("%s: expected code [%s], got %v", fx.Name, fx.Code, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%s: upgrade: %v", fx.Name, err)
		}
		treeJSON, err := json.Marshal(tree)
		if err != nil {
			t.Fatalf("%s: marshal upgraded tree: %v", fx.Name, err)
		}
		doc, err := ParseV2Document(treeJSON)
		if err != nil {
			t.Fatalf("%s: upgraded document must validate as v2: %v", fx.Name, err)
		}
		if *updateGolden {
			fx.SHA256 = doc.SHA256Hex
			if err := os.WriteFile(filepath.Join(goldenDir, fx.Canonical), doc.Canonical, 0o644); err != nil {
				t.Fatalf("%s: write canonical: %v", fx.Name, err)
			}
		}
		if fx.SHA256 == "" {
			t.Fatalf("%s: manifest has no recorded sha256 (run -update-golden once)", fx.Name)
		}
		if fx.SHA256 != doc.SHA256Hex {
			t.Errorf("%s: sha256 mismatch: manifest %s, computed %s", fx.Name, fx.SHA256, doc.SHA256Hex)
		}
		expected, err := os.ReadFile(filepath.Join(goldenDir, fx.Canonical))
		if err != nil {
			t.Fatalf("%s: read canonical: %v", fx.Name, err)
		}
		if !bytes.Equal(expected, doc.Canonical) {
			t.Errorf("%s: canonical bytes differ from golden file", fx.Name)
		}
	}
	if *updateGolden {
		writeGoldenManifest(t, &m)
	}
}

func TestV2UpgradeV1AmbiguityNamesTheField(t *testing.T) {
	var s Schema
	raw, err := os.ReadFile(filepath.Join(goldenDir, "v1/ambiguous-default.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSchema(&s); err != nil {
		t.Fatalf("fixture passes v1 validation: %v", err)
	}
	_, err = UpgradeV1Schema(&s)
	if err == nil {
		t.Fatal("expected ambiguity error")
	}
	msg := err.Error()
	for _, want := range []string{"[ambiguous-default]", "public.users", "bio", `"hello world"`} {
		if !strings.Contains(msg, want) {
			t.Errorf("ambiguity error must name %q, got: %s", want, msg)
		}
	}
}

// ---------------------------------------------------------------------------
// Hash sensitivity: any meaningful mutation changes the hash; formatting-only
// changes never do.
// ---------------------------------------------------------------------------

// mutateFixture applies one meaningful mutation to a fixture's canonical tree
// and returns the re-canonicalized, re-validated document hash. Mutations are
// chosen to keep documents valid: the point is that the hash sees the change,
// not that validation rejects it.
func mutateFixture(t *testing.T, fixture string, mutate func(root map[string]any)) string {
	t.Helper()
	tree := parseCanonicalTree(t, fixture)
	mutate(tree)
	treeJSON, err := json.Marshal(tree)
	if err != nil {
		t.Fatalf("%s: marshal mutated tree: %v", fixture, err)
	}
	doc, err := ParseV2Document(treeJSON)
	if err != nil {
		t.Fatalf("%s: mutated document must stay valid (choose non-breaking mutations): %v", fixture, err)
	}
	return doc.SHA256Hex
}

func parseCanonicalTree(t *testing.T, fixture string) map[string]any {
	t.Helper()
	raw := canonicalOf(t, fixture)
	var tree map[string]any
	if err := json.Unmarshal(raw, &tree); err != nil {
		t.Fatalf("%s: parse canonical tree: %v", fixture, err)
	}
	return tree
}

// tree navigation helpers over the canonical tree (tables sorted by
// identity; column arrays preserve declaration order).
func findTable(t *testing.T, root map[string]any, key string) map[string]any {
	for _, tv := range root["tables"].([]any) {
		tm := tv.(map[string]any)
		id := tm["identity"].(map[string]any)
		if id["schema"].(string)+"."+id["name"].(string) == key {
			return tm
		}
	}
	t.Fatalf("table %q not found", key)
	return nil
}

func findColumn(t *testing.T, table map[string]any, name string) map[string]any {
	for _, cv := range table["columns"].([]any) {
		cm := cv.(map[string]any)
		if cm["name"].(string) == name {
			return cm
		}
	}
	t.Fatalf("column %q not found", name)
	return nil
}

func findConstraint(t *testing.T, table map[string]any, name string) map[string]any {
	for _, cv := range table["constraints"].([]any) {
		cm := cv.(map[string]any)
		if cm["name"].(string) == name {
			return cm
		}
	}
	t.Fatalf("constraint %q not found", name)
	return nil
}

func findIndex(t *testing.T, table map[string]any, name string) map[string]any {
	for _, iv := range table["indexes"].([]any) {
		im := iv.(map[string]any)
		if im["identity"].(map[string]any)["name"].(string) == name {
			return im
		}
	}
	t.Fatalf("index %q not found", name)
	return nil
}

func swapTuple(t *testing.T, arr []any) []any {
	out := append([]any(nil), arr...)
	if len(out) < 2 {
		t.Fatal("need >=2 tuple entries to swap")
	}
	out[0], out[1] = out[1], out[0]
	return out
}

func TestV2HashSensitivity(t *testing.T) {
	basic := hashOf(t, "basic-users")
	tenant := hashOf(t, "tenant-composite")
	tagged := hashOf(t, "tagged-defaults")
	catalog := hashOf(t, "catalog-surface")
	empty := hashOf(t, "empty-managed")

	cases := []struct {
		name   string
		fixture string
		base   string
		mutate func(root map[string]any)
	}{
		{"type parameter value", "basic-users", basic, func(r map[string]any) {
			c := findColumn(t, findTable(t, r, "public.users"), "email")
			c["type"].(map[string]any)["params"].(map[string]any)["length"] = float64(256)
		}},
		{"nullability flip", "basic-users", basic, func(r map[string]any) {
			c := findColumn(t, findTable(t, r, "public.users"), "name")
			c["notNull"] = true
		}},
		{"column declaration order", "basic-users", basic, func(r map[string]any) {
			users := findTable(t, r, "public.users")
			users["columns"] = swapTuple(t, users["columns"].([]any))
		}},
		{"literal default value", "basic-users", basic, func(r map[string]any) {
			c := findColumn(t, findTable(t, r, "public.posts"), "views_count")
			c["default"].(map[string]any)["sql"] = "1"
		}},
		{"expression default text", "basic-users", basic, func(r map[string]any) {
			c := findColumn(t, findTable(t, r, "public.users"), "created_at")
			c["default"].(map[string]any)["sql"] = "now() at time zone 'utc'"
		}},
		{"default tag", "basic-users", basic, func(r map[string]any) {
			c := findColumn(t, findTable(t, r, "public.users"), "active")
			c["default"] = map[string]any{"kind": "expression", "sql": "true"}
		}},
		{"composite primary-key column order", "tenant-composite", tenant, func(r map[string]any) {
			con := findConstraint(t, findTable(t, r, "app.memberships"), "memberships_pkey")
			con["columns"] = swapTuple(t, con["columns"].([]any))
		}},
		{"composite unique column order", "tenant-composite", tenant, func(r map[string]any) {
			con := findConstraint(t, findTable(t, r, "app.org_settings"), "org_settings_scope_key")
			con["columns"] = swapTuple(t, con["columns"].([]any))
		}},
		{"index key order (column then expression)", "tenant-composite", tenant, func(r map[string]any) {
			idx := findIndex(t, findTable(t, r, "app.org_settings"), "org_settings_note_idx")
			idx["key"] = swapTuple(t, idx["key"].([]any))
		}},
		{"fk referential action", "tenant-composite", tenant, func(r map[string]any) {
			con := findConstraint(t, findTable(t, r, "public.users"), "users_tenant_fkey")
			con["references"].(map[string]any)["onDelete"] = "restrict"
		}},
		{"opaque inventory reason", "tenant-composite", tenant, func(r map[string]any) {
			r["opaque"].([]any)[0].(map[string]any)["reason"] = "owned by extension pg_partman (stub)"
		}},
		{"table identity rename", "tenant-composite", tenant, func(r map[string]any) {
			findTable(t, r, "app.memberships")["identity"].(map[string]any)["name"] = "member_roles"
		}},
		{"enum value order", "tagged-defaults", tagged, func(r map[string]any) {
			ev := r["enums"].([]any)[0].(map[string]any)
			ev["values"] = swapTuple(t, ev["values"].([]any))
		}},
		{"identity default generated mode", "tagged-defaults", tagged, func(r map[string]any) {
			c := findColumn(t, findTable(t, r, "public.events"), "id")
			c["default"].(map[string]any)["generated"] = "by default"
		}},
		{"sequence default target", "tagged-defaults", tagged, func(r map[string]any) {
			c := findColumn(t, findTable(t, r, "public.events"), "seq_num")
			c["default"].(map[string]any)["sequence"].(map[string]any)["name"] = "events_seq_num_seq2"
		}},
		{"array literal default", "tagged-defaults", tagged, func(r map[string]any) {
			c := findColumn(t, findTable(t, r, "public.events"), "tags")
			c["default"].(map[string]any)["sql"] = "'{draft}'::text[]"
		}},
		{"index unique flag", "basic-users", basic, func(r map[string]any) {
			findIndex(t, findTable(t, r, "public.users"), "users_name_idx")["unique"] = true
		}},
		{"view definition text", "catalog-surface", catalog, func(r map[string]any) {
			for _, vv := range r["views"].([]any) {
				vm := vv.(map[string]any)
				if vm["identity"].(map[string]any)["name"].(string) == "v_active" {
					vm["definition"] = "select name from public.indexes where type = 'inactive'"
				}
			}
		}},
		{"view managed flag", "catalog-surface", catalog, func(r map[string]any) {
			for _, vv := range r["views"].([]any) {
				vm := vv.(map[string]any)
				if vm["identity"].(map[string]any)["name"].(string) == "v_meta_count" {
					vm["managed"] = true
				}
			}
		}},
		{"table with metadata-like name", "catalog-surface", catalog, func(r map[string]any) {
			findTable(t, r, "public.indexes")["identity"].(map[string]any)["name"] = "indexes2"
		}},
		{"empty document gains a capability", "empty-managed", empty, func(r map[string]any) {
			r["capabilities"] = []any{"pgvector"}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mutateFixture(t, tc.fixture, tc.mutate)
			if got == tc.base {
				t.Errorf("mutation %q left the hash unchanged (%s) — the canonical form must see every meaningful field", tc.name, got)
			}
		})
	}
}

func hashOf(t *testing.T, fixture string) string {
	t.Helper()
	tree := parseCanonicalTree(t, fixture)
	doc, err := ParseV2Document(treeJSON(t, tree))
	if err != nil {
		t.Fatalf("%s: %v", fixture, err)
	}
	return doc.SHA256Hex
}

func treeJSON(t *testing.T, tree map[string]any) []byte {
	t.Helper()
	raw, err := json.Marshal(tree)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestV2FormattingOnlyChangesKeepTheHash(t *testing.T) {
	m := loadGoldenManifest(t)
	for _, fx := range m.Valid {
		raw, err := os.ReadFile(filepath.Join(goldenDir, fx.Document))
		if err != nil {
			t.Fatalf("%s: %v", fx.Name, err)
		}
		base, err := ParseV2Document(raw)
		if err != nil {
			t.Fatalf("%s: %v", fx.Name, err)
		}

		// Re-indent with reversed key order at every level: formatting only.
		var tree any
		if err := json.Unmarshal(raw, &tree); err != nil {
			t.Fatal(err)
		}
		shuffled := reverseKeys(tree)
		reformatted, err := json.MarshalIndent(shuffled, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		again, err := ParseV2Document(reformatted)
		if err != nil {
			t.Fatalf("%s: reformatted document must still validate: %v", fx.Name, err)
		}
		if again.SHA256Hex != base.SHA256Hex {
			t.Errorf("%s: key order + indentation changed the hash (%s -> %s)", fx.Name, base.SHA256Hex, again.SHA256Hex)
		}
	}

	// Numeric respelling in the raw text: 2.55e2 for 255.
	raw, err := os.ReadFile(filepath.Join(goldenDir, "valid/basic-users.json"))
	if err != nil {
		t.Fatal(err)
	}
	base, err := ParseV2Document(raw)
	if err != nil {
		t.Fatal(err)
	}
	respelled := strings.Replace(string(raw), `"length": 255`, `"length": 2.55e2`, 1)
	if respelled == string(raw) {
		t.Fatal("respelling did not apply — fixture text changed?")
	}
	again, err := ParseV2Document([]byte(respelled))
	if err != nil {
		t.Fatalf("respelled document must validate: %v", err)
	}
	if again.SHA256Hex != base.SHA256Hex {
		t.Errorf("numeric spelling 255 vs 2.55e2 changed the hash")
	}
}

func reverseKeys(v any) any {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		// reverse insertion order
		for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
			keys[i], keys[j] = keys[j], keys[i]
		}
		out := make(map[string]any, len(x))
		for _, k := range keys {
			out[k] = reverseKeys(x[k])
		}
		return out
	case []any:
		out := make([]any, 0, len(x))
		for _, item := range x {
			out = append(out, reverseKeys(item))
		}
		return out
	default:
		return v
	}
}

// ---------------------------------------------------------------------------
// Version detection and dispatch
// ---------------------------------------------------------------------------

func TestDetectSchemaVersion(t *testing.T) {
	cases := []struct {
		raw      string
		want     int
		wantErr  bool
	}{
		{`{"version":2,"dialect":"postgresql"}`, 2, false},
		{`{"version":1,"tables":[]}`, 1, false},
		{`{"tables":[]}`, 0, false},
		{`{"version":3}`, 3, false},
		{`{"version":2.5}`, 0, true},
		{`not json`, 0, true},
	}
	for _, tc := range cases {
		got, err := DetectSchemaVersion([]byte(tc.raw))
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s: expected error", tc.raw)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.raw, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: got version %d, want %d", tc.raw, got, tc.want)
		}
	}
}

func TestValidateSchemaDocumentDispatch(t *testing.T) {
	// v2 valid: hash present.
	v2raw, err := os.ReadFile(filepath.Join(goldenDir, "valid/basic-users.json"))
	if err != nil {
		t.Fatal(err)
	}
	check, err := ValidateSchemaDocument(v2raw)
	if err != nil {
		t.Fatalf("v2 dispatch: %v", err)
	}
	if check.Version != 2 || check.SHA256 == "" || check.Canonical == nil {
		t.Fatalf("v2 check incomplete: %+v", check)
	}

	// v2 invalid: contract error with code.
	bad := strings.Replace(string(v2raw), `"version": 2`, `"version": 9`, 1)
	if _, err := ValidateSchemaDocument([]byte(bad)); err == nil || !strings.Contains(err.Error(), "[unknown-version]") {
		t.Fatalf("expected unknown-version, got %v", err)
	}

	// v1 valid: existing path, no canonical form.
	v1 := `{"version":1,"tables":[{"name":"t","columns":[{"name":"id","type":"serial","notNull":true,"primaryKey":true}],"indexes":[]}]}`
	check, err = ValidateSchemaDocument([]byte(v1))
	if err != nil {
		t.Fatalf("v1 dispatch: %v", err)
	}
	if check.Version != 1 || check.SHA256 != "" {
		t.Fatalf("v1 check must carry no canonical form: %+v", check)
	}

	// v1 invalid: the v1 validator's message surfaces unchanged.
	badV1 := `{"version":1,"tables":[{"name":"t","columns":[{"name":"id","type":"not_a_real_type"}],"indexes":[]}]}`
	if _, err := ValidateSchemaDocument([]byte(badV1)); err == nil || !strings.Contains(err.Error(), "unsupported type") {
		t.Fatalf("expected v1 unsupported-type error, got %v", err)
	}

	// Unknown version: unknown-version code.
	if _, err := ValidateSchemaDocument([]byte(`{"version":7}`)); err == nil || !strings.Contains(err.Error(), "[unknown-version]") {
		t.Fatalf("expected unknown-version, got %v", err)
	}
}

func TestV2DuplicateJSONKeyRejected(t *testing.T) {
	raw := []byte(`{"version":2,"version":2,"dialect":"postgresql","capabilities":[],"schemas":[],"tables":[],"enums":[],"views":[],"opaque":[]}`)
	_, err := ParseV2Document(raw)
	if err == nil || !strings.Contains(err.Error(), "[duplicate-key]") {
		t.Fatalf("expected duplicate-key rejection, got %v", err)
	}
}

func TestV2DeepNestingRejectedCleanly(t *testing.T) {
	// 15k-deep arrays: Go fails at encoding/json's depth cap, TypeScript at
	// its explicit mirrored cap (or a caught parser RangeError) — both report
	// invalid-json (CANONICAL.md §5).
	deep := strings.Repeat("[", 15000) + strings.Repeat("]", 15000)
	if _, err := ParseV2Document([]byte(deep)); err == nil || !strings.Contains(err.Error(), "[invalid-json]") {
		t.Errorf("expected [invalid-json] for 15k-deep input, got %v", err)
	}
}

func TestV2NonIntegerVersionSpellingParity(t *testing.T) {
	// A non-integer version spelling must report the same code from both
	// Go entry points (CANONICAL.md §5): the dispatch path previously
	// reported invalid-json while ParseV2Document reported invalid-number.
	raw := []byte(`{"version":"2","dialect":"postgresql","capabilities":[],"schemas":[],"tables":[],"enums":[],"views":[],"opaque":[]}`)
	for _, fn := range []struct {
		name string
		call func([]byte) error
	}{
		{"ValidateSchemaDocument", func(raw []byte) error {
			_, err := ValidateSchemaDocument(raw)
			return err
		}},
		{"ParseV2Document", func(raw []byte) error {
			_, err := ParseV2Document(raw)
			return err
		}},
	} {
		err := fn.call(raw)
		if err == nil || !strings.Contains(err.Error(), "[invalid-number]") {
			t.Errorf("%s: expected [invalid-number], got %v", fn.name, err)
		}
	}
}

func TestV2ScalarValueHygiene(t *testing.T) {
	// F1/F4 codepoint coverage in the Go validator: U+FFFD is rejected in
	// any string (over-approximation of unpaired surrogates), U+FFFF is
	// accepted; names reject control characters including NUL and DEL;
	// SQL text rejects NUL per the schema-v2.json sqlText pattern.
	base := func(sql string) []byte {
		return []byte(fmt.Sprintf(`{"version":2,"dialect":"postgresql","capabilities":[],"schemas":[{"name":"public"}],"enums":[],"views":[],"opaque":[],"tables":[{"identity":{"schema":"public","name":"t"},"managed":true,"columns":[{"name":"a","type":{"name":"text","codec":"string"},"notNull":false,"default":{"kind":"literal","sql":%q}}],"constraints":[],"indexes":[]}]}`, sql))
	}
	if _, err := ParseV2Document(base("'a\ufffdb'")); err == nil || !strings.Contains(err.Error(), "[invalid-value]") {
		t.Errorf("U+FFFD in a string must be rejected invalid-value, got %v", err)
	}
	if _, err := ParseV2Document(base("'a\uffffb'")); err != nil {
		t.Errorf("U+FFFF in a string must be accepted, got %v", err)
	}
	delName := []byte(`{"version":2,"dialect":"postgresql","capabilities":[],"schemas":[{"name":"public"}],"enums":[],"views":[],"opaque":[],"tables":[{"identity":{"schema":"public","name":"t"},"managed":true,"columns":[{"name":"a\u007fb","type":{"name":"text","codec":"string"},"notNull":false}],"constraints":[],"indexes":[]}]}`)
	if _, err := ParseV2Document(delName); err == nil || !strings.Contains(err.Error(), "[invalid-identity]") {
		t.Errorf("DEL in a name must be rejected invalid-identity, got %v", err)
	}
	nulSql := []byte(`{"version":2,"dialect":"postgresql","capabilities":[],"schemas":[{"name":"public"}],"enums":[],"views":[],"opaque":[],"tables":[{"identity":{"schema":"public","name":"t"},"managed":true,"columns":[{"name":"a","type":{"name":"text","codec":"string"},"notNull":false,"default":{"kind":"expression","sql":"now()\u0000"}}],"constraints":[],"indexes":[]}]}`)
	if _, err := ParseV2Document(nulSql); err == nil || !strings.Contains(err.Error(), "[invalid-value]") {
		t.Errorf("NUL in SQL text must be rejected invalid-value, got %v", err)
	}
}

func TestV2CanonicalStringEscaping(t *testing.T) {
	// Byte-for-byte parity with ECMascript JSON.stringify escaping is what
	// makes the cross-language hash work; pin the tricky cases.
	if got, want := string(v2AppendString(nil, "a\"b\\b")), `"a\"b\\b"`; got != want {
		t.Errorf("escape: got %s want %s", got, want)
	}
	if got, want := string(v2AppendString(nil, "a\x01b")), "\"a\\u0001b\""; got != want {
		t.Errorf("control escape: got %s want %s", got, want)
	}
	if got, want := string(v2AppendString(nil, "a\nb\tc")), "\"a\\nb\\tc\""; got != want {
		t.Errorf("short-form escapes: got %s want %s", got, want)
	}
	if got, want := string(v2AppendString(nil, "h\u00e9\u2028y")), "\"h\u00e9\u2028y\""; got != want {
		t.Errorf("raw non-ASCII incl. U+2028: got %s want %s", got, want)
	}
	if got, want := string(v2AppendString(nil, "<>&")), "\"<>&\""; got != want {
		t.Errorf("no HTML escaping: got %s want %s", got, want)
	}
}

func TestV2LiteralPattern(t *testing.T) {
	valid := []string{"'hello'", "'it''s'", "'x'::text", "'{}'::text[]", "42", "-1.5", ".5", "1e5", "true", "false", "null", "'draft'"}
	invalid := []string{"now()", "gen_random_uuid()", "1; drop table x", "hello", "'unterminated", "", "'a'::text::text", "nextval('s')"}
	for _, s := range valid {
		if !v2LiteralRegexp.MatchString(s) {
			t.Errorf("literal %q should match", s)
		}
	}
	for _, s := range invalid {
		if v2LiteralRegexp.MatchString(s) {
			t.Errorf("literal %q should NOT match", s)
		}
	}
}

func TestUpgradeV1DerivedContractObjects(t *testing.T) {
	// The upgraded basic fixture must contain the derived objects with the
	// exact names the v1 DDL produces; the canonical golden pins the bytes,
	// this pins the semantics in readable form.
	var s Schema
	raw, err := os.ReadFile(filepath.Join(goldenDir, "v1/basic-upgrade.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSchema(&s); err != nil {
		t.Fatal(err)
	}
	tree, err := UpgradeV1Schema(&s)
	if err != nil {
		t.Fatal(err)
	}
	treeJSON, _ := json.Marshal(tree)
	doc, err := ParseV2Document(treeJSON)
	if err != nil {
		t.Fatalf("upgraded document invalid: %v", err)
	}

	users := findTable(t, doc.Root, "public.users")
	if id := users["identity"].(map[string]any); id["schema"] != "public" || id["name"] != "users" {
		t.Fatalf("identity: %+v", id)
	}
	if users["managed"] != true {
		t.Error("upgraded tables must be managed")
	}
	serial := findColumn(t, users, "id")
	seq := serial["default"].(map[string]any)
	if seq["kind"] != "sequence" {
		t.Fatalf("serial must upgrade to a sequence default, got %+v", seq)
	}
	if name := seq["sequence"].(map[string]any)["name"]; name != "users_id_seq" {
		t.Errorf("derived sequence name: got %v want users_id_seq", name)
	}
	if tn := serial["type"].(map[string]any)["name"]; tn != "int4" {
		t.Errorf("serial must upgrade to int4, got %v", tn)
	}
	email := findColumn(t, users, "email")
	if L := email["type"].(map[string]any)["params"].(map[string]any)["length"]; L != float64(255) {
		t.Errorf("varchar length lost: %v", L)
	}
	unique := findConstraint(t, users, "users_email_key")
	if unique["type"] != "unique" {
		t.Error("v1 unique flag must become a table-level unique constraint")
	}
	posts := findTable(t, doc.Root, "public.posts")
	fk := findConstraint(t, posts, "posts_author_id_fkey")
	ref := fk["references"].(map[string]any)
	if ref["onDelete"] != "cascade" || ref["table"].(map[string]any)["name"] != "users" {
		t.Errorf("fk upgrade: %+v", ref)
	}
	if ref["columns"].([]any)[0] != "id" {
		t.Errorf("renamed target column must be preserved positionally: %+v", ref)
	}
	createdAt := findColumn(t, users, "created_at")
	if d := createdAt["default"].(map[string]any); d["kind"] != "expression" || d["sql"] != "now()" {
		t.Errorf("defaultNow must upgrade to expression now(), got %+v", d)
	}
	idx := findIndex(t, posts, "posts_title_idx")
	if idx["method"] != "btree" {
		t.Error("v1 indexes upgrade as btree")
	}
}

func TestUpgradeV1VectorCapability(t *testing.T) {
	var s Schema
	raw, err := os.ReadFile(filepath.Join(goldenDir, "v1/vector-upgrade.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSchemaV1ForUpgrade(&s); err != nil {
		t.Fatal(err)
	}
	tree, err := UpgradeV1Schema(&s)
	if err != nil {
		t.Fatal(err)
	}
	caps := tree["capabilities"].([]any)
	if len(caps) != 1 || caps[0] != "pgvector" {
		t.Fatalf("vector upgrade must add the pgvector capability (X01 separated PostgreSQL extension detection from Nucleus), got %+v", caps)
	}
	treeJSON, _ := json.Marshal(tree)
	if _, err := ParseV2Document(treeJSON); err != nil {
		t.Fatalf("vector-upgraded document must validate: %v", err)
	}
}

func TestV2SafeIntegerBounds(t *testing.T) {
	// Every integer field also has a semantic domain range, so the two
	// rejection layers are distinct: unsafe magnitudes fail at the numeric
	// layer (invalid-number), safe-but-out-of-domain values fail the field
	// range (invalid-type-params).
	base := `{"version":2,"dialect":"postgresql","capabilities":[],"schemas":[{"name":"public"}],"enums":[],"views":[],"opaque":[],"tables":[{"identity":{"schema":"public","name":"t"},"managed":true,"columns":[{"name":"a","type":{"name":"varchar","codec":"string","params":{"length":%s}},"notNull":false}],"constraints":[],"indexes":[]}]}`
	if _, err := ParseV2Document([]byte(fmt.Sprintf(base, "9007199254740993"))); err == nil || !strings.Contains(err.Error(), "[invalid-number]") {
		t.Fatalf("expected unsafe-integer rejection, got %v", err)
	}
	if _, err := ParseV2Document([]byte(fmt.Sprintf(base, "12.5"))); err == nil || !strings.Contains(err.Error(), "[invalid-number]") {
		t.Fatalf("expected non-integral rejection, got %v", err)
	}
	if _, err := ParseV2Document([]byte(fmt.Sprintf(base, "9007199254740991"))); err == nil || !strings.Contains(err.Error(), "[invalid-type-params]") {
		t.Fatalf("expected domain-range rejection for a safe but out-of-domain integer, got %v", err)
	}
	if _, err := ParseV2Document([]byte(fmt.Sprintf(base, "10485760"))); err != nil {
		t.Fatalf("domain boundary must be accepted, got %v", err)
	}
}
