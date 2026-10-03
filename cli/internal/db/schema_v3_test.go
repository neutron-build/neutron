package db

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

func v3Fixture(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../../conformance/schema-v3/golden/overloads.json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestV3QualifiedOverloadGoldenAndPortableHash(t *testing.T) {
	doc, err := ParseV3Document(v3Fixture(t))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := os.ReadFile("../../../conformance/schema-v3/golden/overloads.canonical.json")
	if err != nil {
		t.Fatal(err)
	}
	hash, err := os.ReadFile("../../../conformance/schema-v3/golden/overloads.sha256")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(doc.Canonical, expected) || doc.SHA256Hex != strings.TrimSpace(string(hash)) {
		t.Fatalf("canonical golden mismatch: %s", doc.Canonical)
	}
	if len(doc.Model.Inventory) != 5 {
		t.Fatal("overloads or domain flattened")
	}
	var root map[string]any
	if err := json.Unmarshal(v3Fixture(t), &root); err != nil {
		t.Fatal(err)
	}
	objects := root["inventory"].([]any)
	for left, right := 0, len(objects)-1; left < right; left, right = left+1, right-1 {
		objects[left], objects[right] = objects[right], objects[left]
	}
	root["inventory"] = objects
	encoded, _ := json.Marshal(root)
	reordered, err := ParseV3Document(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if reordered.SHA256Hex != doc.SHA256Hex {
		t.Fatal("object order changed portable hash")
	}
	// Typed export must retain the explicitly empty routine signature.
	typed, err := json.Marshal(doc.Model)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := ParseV3Document(typed)
	if err != nil {
		t.Fatal(err)
	}
	if imported.SHA256Hex != doc.SHA256Hex {
		t.Fatal("typed export/import changed inventory")
	}
	if doc.RefuseMigration() == nil {
		t.Fatal("inventory accidentally authorized DDL")
	}
}

func TestV3UpgradeRetainsV2BytesAndUnknownCoverage(t *testing.T) {
	doc, err := ParseV3Document(v3Fixture(t))
	if err != nil {
		t.Fatal(err)
	}
	upgraded, err := UpgradeSchemaDocumentV3(doc.Relational.Canonical)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(upgraded.Relational.Canonical, doc.Relational.Canonical) || upgraded.Relational.SHA256Hex != doc.Relational.SHA256Hex {
		t.Fatal("v2 canonical contract changed")
	}
	if len(upgraded.Model.Inventory) != 0 {
		t.Fatal("upgrade invented objects")
	}
	for _, coverage := range upgraded.Model.Coverage {
		if coverage.Family != "relations" && coverage.Status != "not-inspected" {
			t.Fatal("upgrade inferred inventory absence")
		}
	}
	v1 := []byte(`{"version":1,"tables":[]}`)
	if _, err := UpgradeSchemaDocumentV3(v1); err != nil {
		t.Fatal(err)
	}
	ambiguous := []byte(`{"version":1,"tables":[{"name":"t","columns":[{"name":"x","type":"integer","hasDefault":true}]}]}`)
	if _, err := UpgradeSchemaDocumentV3(ambiguous); err == nil {
		t.Fatal("v1 ambiguous-default refusal was bypassed")
	}
}

func TestV3RefusesLossyOrAmbiguousInventory(t *testing.T) {
	tests := []struct {
		name, code string
		change     func(map[string]any)
	}{
		{"duplicate", "duplicate-catalog-object", func(r map[string]any) { a := r["inventory"].([]any); r["inventory"] = append(a, a[0]) }},
		{"managed", "unmanaged-only", func(r map[string]any) { r["inventory"].([]any)[0].(map[string]any)["managed"] = true }},
		{"missing-signature", "missing-signature", func(r map[string]any) {
			delete(r["inventory"].([]any)[0].(map[string]any)["identity"].(map[string]any), "arguments")
		}},
		{"ephemeral-oid", "nonportable-identity", func(r map[string]any) {
			r["inventory"].([]any)[0].(map[string]any)["attributes"] = map[string]any{"oid": "16394"}
		}},
		{"missing-family", "invalid-coverage", func(r map[string]any) { r["coverage"] = r["coverage"].([]any)[1:] }},
		{"invented-complete", "invalid-coverage", func(r map[string]any) { r["coverage"].([]any)[0].(map[string]any)["status"] = "complete" }},
		{"null", "invalid-value", func(r map[string]any) { r["inventory"] = nil }},
		{"no-reason", "missing-reason", func(r map[string]any) { r["inventory"].([]any)[0].(map[string]any)["reason"] = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var root map[string]any
			if err := json.Unmarshal(v3Fixture(t), &root); err != nil {
				t.Fatal(err)
			}
			test.change(root)
			raw, _ := json.Marshal(root)
			_, err := ParseV3Document(raw)
			var ce *ContractError
			if !errors.As(err, &ce) || ce.Code != test.code {
				t.Fatalf("want %s, got %v", test.code, err)
			}
		})
	}
	if _, err := ParseV3Document([]byte(`{"version":3,"version":3}`)); err == nil {
		t.Fatal("duplicate keys accepted")
	}
}
