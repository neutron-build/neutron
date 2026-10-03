package db

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestV3NativeQualifiedRoutinesTypesAndReadOnlyRoundTrip(t *testing.T) {
	h := newQ07Harness(t, "v3inventory")
	for _, statement := range []string{
		`CREATE SCHEMA "Alpha"`, `CREATE SCHEMA beta`,
		`CREATE DOMAIN "Alpha".int4 AS pg_catalog.int4 DEFAULT 7 CHECK (VALUE > 0)`,
		`CREATE TYPE "Alpha".uuid AS ("Case Field" pg_catalog.text, "select" "Alpha".int4)`,
		`CREATE TYPE beta.custom_range AS RANGE (subtype = pg_catalog.int4)`,
		`CREATE TYPE "Alpha".pending_shell`,
		`CREATE FUNCTION "Alpha"."Same Name"(pg_catalog.int4) RETURNS pg_catalog.int4 LANGUAGE SQL IMMUTABLE AS 'SELECT $1'`,
		`CREATE FUNCTION "Alpha"."Same Name"(pg_catalog.text) RETURNS pg_catalog.text LANGUAGE SQL IMMUTABLE AS 'SELECT $1'`,
		`CREATE FUNCTION "Alpha"."Same Name"("Alpha".int4) RETURNS pg_catalog.int4 LANGUAGE SQL IMMUTABLE AS 'SELECT $1::pg_catalog.int4'`,
		`CREATE FUNCTION "Alpha"."Same Name"() RETURNS pg_catalog.bool LANGUAGE SQL AS 'SELECT true'`,
		`CREATE FUNCTION beta."Same Name"(pg_catalog.int4) RETURNS pg_catalog.int4 LANGUAGE SQL IMMUTABLE AS 'SELECT $1 + 1'`,
		`CREATE PROCEDURE beta."procedure name"(INOUT number pg_catalog.int4) LANGUAGE SQL AS 'SELECT number + 1'`,
		`CREATE TABLE "Alpha"."select" (id pg_catalog.int4 PRIMARY KEY, value "Alpha".int4)`,
		`INSERT INTO "Alpha"."select" VALUES (1, 42)`,
		`CREATE TABLE beta."select" (id pg_catalog.int4 PRIMARY KEY, value pg_catalog.int4)`,
		`CREATE POLICY "same policy" ON "Alpha"."select" TO PUBLIC USING (id > 0) WITH CHECK (value > 0)`,
		`CREATE POLICY "same policy" ON beta."select" TO PUBLIC USING (id > 1)`,
		`ALTER TABLE "Alpha"."select" ENABLE ROW LEVEL SECURITY`,
		`CREATE FUNCTION beta.touch() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.value := NEW.value + 1; RETURN NEW; END;$$`,
		`CREATE TRIGGER "same trigger" BEFORE UPDATE ON "Alpha"."select" FOR EACH ROW EXECUTE FUNCTION beta.touch()`,
		`CREATE TRIGGER "same trigger" BEFORE UPDATE ON beta."select" FOR EACH ROW EXECUTE FUNCTION beta.touch()`,
		`ALTER TABLE beta."select" DISABLE TRIGGER "same trigger"`,
		`CREATE EXTENSION vector WITH SCHEMA beta`,
		`CREATE TABLE "Alpha".pg_type (typname pg_catalog.text)`,
		`INSERT INTO "Alpha".pg_type VALUES ('fabricated')`,
		`CREATE FUNCTION "Alpha".pg_get_functiondef(pg_catalog.oid) RETURNS pg_catalog.text LANGUAGE SQL AS 'SELECT ''fabricated'''`,
	} {
		h.exec(statement)
	}
	ctx := context.Background()
	v2before, err := h.client.IntrospectV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	originalDefinition := h.queryOne(`SELECT pg_catalog.pg_get_functiondef('"Alpha"."Same Name"(pg_catalog.int4)'::pg_catalog.regprocedure)`)
	originalPath := h.queryOne(`SHOW search_path`)
	doc, err := h.client.IntrospectV3(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.queryOne(`SHOW search_path`) != originalPath {
		t.Fatal("transaction-local deparser path leaked into pool")
	}
	overloads := 0
	foundDomain, foundComposite, foundProcedure, foundExtension, foundRange, foundShell := false, false, false, false, false, false
	foundExtensionArray := false
	policies, triggers := 0, 0
	foundExtensionMembers := false
	for _, entry := range doc.Model.Inventory {
		if entry.Managed {
			t.Fatal("catalog inventory became managed")
		}
		if entry.Identity.Catalog == "pg_policy" && entry.Identity.Name == "same policy" {
			policies++
			if entry.Identity.Parent == nil || entry.Identity.Parent.Name != "select" || entry.Identity.Parent.Schema != entry.Identity.Schema {
				t.Fatal("policy table scope lost")
			}
			if entry.Identity.Schema == "Alpha" && (entry.Attributes["rowSecurityEnabled"] != "true" || entry.Attributes["check"] == "") {
				t.Fatal("policy RLS/check metadata lost")
			}
		}
		if entry.Identity.Catalog == "pg_trigger" && entry.Identity.Name == "same trigger" {
			triggers++
			if entry.Identity.Parent == nil || entry.Identity.Parent.Name != "select" || entry.References["function"] != (V2Identity{Schema: "beta", Name: "touch"}) || entry.Definition == "" {
				t.Fatal("trigger scope/function/definition lost")
			}
			if entry.Identity.Schema == "beta" && entry.Attributes["enabled"] != "D" {
				t.Fatal("trigger disablement lost")
			}
		}
		if entry.Identity.Catalog == "pg_extension" && entry.Identity.Name == "vector" {
			foundExtensionMembers = entry.Attributes["version"] != "" && len(entry.Parts) > 0
			for _, part := range entry.Parts {
				if part.Kind == "extension-member" && (part.Attributes["objectNames"] == "" || part.Attributes["catalogName"] == "") {
					t.Fatal("extension member address vanished")
				}
			}
		}
		if entry.Identity.Catalog == "pg_proc" && entry.Identity.Schema == "Alpha" && entry.Identity.Name == "Same Name" {
			overloads++
			if len(entry.Identity.Arguments) > 0 {
				arg := entry.Identity.Arguments[0]
				if arg.Name == "int4" && arg.Schema != "pg_catalog" && arg.Schema != "Alpha" {
					t.Fatalf("bad signature type identity: %+v", arg)
				}
			}
		}
		if entry.Identity.Catalog == "pg_type" && entry.Identity.Schema == "Alpha" && entry.Identity.Name == "int4" {
			foundDomain = entry.Kind == "domain" && entry.References["base"] == (V2Identity{Schema: "pg_catalog", Name: "int4"})
			constraint := false
			for _, part := range entry.Parts {
				if part.Kind == "constraint" && part.Definition != "" {
					constraint = true
				}
			}
			if !constraint || entry.Attributes["hasDefault"] != "true" {
				t.Fatal("domain default/constraint vanished")
			}
		}
		if entry.Identity.Catalog == "pg_type" && entry.Identity.Schema == "Alpha" && entry.Identity.Name == "uuid" {
			foundComposite = entry.Kind == "composite" && len(entry.Parts) == 2 && entry.Parts[0].Name == "Case Field" && entry.Parts[1].Name == "select"
			if !foundComposite || entry.Parts[1].Type == nil || *entry.Parts[1].Type != (V2Identity{Schema: "Alpha", Name: "int4"}) {
				t.Fatal("composite physical order/domain identity lost")
			}
		}
		if entry.Identity.Catalog == "pg_proc" && entry.Identity.Schema == "beta" && entry.Identity.Name == "procedure name" {
			foundProcedure = entry.Kind == "procedure" && len(entry.Identity.Arguments) == 1 && entry.Identity.Arguments[0] == (V2Identity{Schema: "pg_catalog", Name: "int4"})
			if len(entry.Parts) != 1 || entry.Parts[0].Attributes["mode"] != "b" {
				t.Fatal("INOUT mode disappeared")
			}
		}
		if entry.Identity.Catalog == "pg_type" && entry.Identity.Schema == "beta" && entry.Identity.Name == "vector" {
			foundExtension = entry.Extension == "vector" && !entry.Managed
		}
		if entry.Identity.Catalog == "pg_type" && entry.Identity.Schema == "beta" && entry.Identity.Name == "_vector" {
			foundExtensionArray = entry.Extension == "vector" && entry.Attributes["extensionOwnership"] == "internal-dependent"
		}
		if entry.Identity.Catalog == "pg_type" && entry.Identity.Schema == "beta" && entry.Identity.Name == "custom_range" {
			foundRange = entry.Kind == "range" && entry.References["rangeSubtype"] == (V2Identity{Schema: "pg_catalog", Name: "int4"}) && entry.References["multirange"].Name != ""
		}
		if entry.Identity.Catalog == "pg_type" && entry.Identity.Schema == "Alpha" && entry.Identity.Name == "pending_shell" {
			foundShell = entry.Kind == "shell-type" && entry.Owner == "" && entry.Attributes["defined"] == "false" && len(entry.References) == 0
		}
	}
	if overloads != 4 || !foundDomain || !foundComposite || !foundProcedure || !foundExtension || !foundExtensionArray || !foundRange || !foundShell {
		t.Fatalf("incomplete inventory: overloads=%d domain=%v composite=%v procedure=%v extension=%v extensionArray=%v range=%v shell=%v policies=%d triggers=%d extensionMembers=%v", overloads, foundDomain, foundComposite, foundProcedure, foundExtension, foundExtensionArray, foundRange, foundShell, policies, triggers, foundExtensionMembers)
	}
	if policies != 2 || triggers != 2 || !foundExtensionMembers {
		t.Fatalf("table-scoped/extension inventory incomplete: policies=%d triggers=%d extension=%v", policies, triggers, foundExtensionMembers)
	}
	exported, err := json.Marshal(doc.Model)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := ParseV3Document(exported)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(imported.Canonical, doc.Canonical) {
		t.Fatal("export/import changed canonical metadata")
	}
	again, err := h.client.IntrospectV3(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.SHA256Hex != doc.SHA256Hex {
		t.Fatal("unchanged database drifted")
	}
	v2after, err := h.client.IntrospectV2(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v2before.Canonical, v2after.Canonical) || h.queryOne(`SELECT value::pg_catalog.text FROM "Alpha"."select" WHERE id=1`) != "42" || h.queryOne(`SELECT pg_catalog.pg_get_functiondef('"Alpha"."Same Name"(pg_catalog.int4)'::pg_catalog.regprocedure)`) != originalDefinition {
		t.Fatal("introspection/import changed native data or definition")
	}
	h.exec(`ALTER DATABASE "` + h.dbName + `" SET search_path TO "Alpha", beta, public, pg_catalog`)
	hostile, err := Connect(ctx, h.client.url)
	if err != nil {
		t.Fatal(err)
	}
	defer hostile.Close()
	hostileDoc, err := hostile.IntrospectV3(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if hostileDoc.SHA256Hex != doc.SHA256Hex {
		t.Fatal("hostile search_path redirected native metadata")
	}
	// Recreate a function: PostgreSQL assigns a new OID but the portable
	// canonical identity/definition must remain exactly unchanged.
	h.exec(`DROP FUNCTION beta."Same Name"(pg_catalog.int4)`)
	h.exec(`CREATE FUNCTION beta."Same Name"(pg_catalog.int4) RETURNS pg_catalog.int4 LANGUAGE SQL IMMUTABLE AS 'SELECT $1 + 1'`)
	recreated, err := h.client.IntrospectV3(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recreated.SHA256Hex != doc.SHA256Hex {
		t.Fatal("ephemeral object OID changed portable hash")
	}
}
