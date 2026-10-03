package db

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

func TestV3NativeExplicitDefaultAndColumnACLs(t *testing.T) {
	h := newQ07Harness(t, "v3acl")
	role := h.dbName + "_reader"
	h.exec(fmt.Sprintf(`CREATE ROLE %q`, role))
	t.Cleanup(func() {
		// Registered after harness cleanup: remove owned-database dependencies
		// before removing this globally scoped, uniquely named role.
		ctx := context.Background()
		if err := h.client.Exec(ctx, fmt.Sprintf(`DROP OWNED BY %q`, role)); err != nil {
			t.Errorf("ACL fixture dependency cleanup: %v", err)
		}
		if err := h.client.Exec(ctx, fmt.Sprintf(`DROP ROLE %q`, role)); err != nil {
			t.Errorf("ACL fixture role cleanup: %v", err)
		}
	})
	for _, sql := range []string{
		`CREATE SCHEMA authority`,
		`CREATE TABLE authority.docs (id int, "Case Column" text)`,
		`CREATE SEQUENCE authority.seq`,
		`CREATE DOMAIN authority.amount AS int`,
		`CREATE FUNCTION authority.same(int) RETURNS int LANGUAGE SQL AS 'SELECT $1'`,
		`CREATE FUNCTION authority.same(text) RETURNS text LANGUAGE SQL AS 'SELECT $1'`,
		`REVOKE ALL ON FUNCTION authority.same(int) FROM PUBLIC`,
		`GRANT EXECUTE ON FUNCTION authority.same(text) TO PUBLIC`,
		fmt.Sprintf(`GRANT USAGE ON SCHEMA authority TO %q`, role),
		fmt.Sprintf(`GRANT SELECT ON authority.docs TO %q WITH GRANT OPTION`, role),
		fmt.Sprintf(`GRANT UPDATE ("Case Column") ON authority.docs TO %q`, role),
		fmt.Sprintf(`GRANT USAGE ON SEQUENCE authority.seq TO %q`, role),
		`REVOKE ALL ON TYPE authority.amount FROM PUBLIC`,
	} {
		h.exec(sql)
	}
	doc, err := h.client.IntrospectV3(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tableGrant, columnGrant, schemaGrant, sequenceGrant := false, false, false, false
	routineIntPublic, routineTextPublic := false, false
	for _, e := range doc.Model.Inventory {
		if e.Identity.Schema != "authority" {
			continue
		}
		for _, p := range v3PartsOfKind(e.Parts, "privilege") {
			a := p.Attributes
			if e.Identity.Catalog == "pg_class" && e.Identity.Name == "docs" && a["grantee"] == role {
				tableGrant = tableGrant || (a["scope"] == "object" && a["privilege"] == "SELECT" && a["grantable"] == "true")
				columnGrant = columnGrant || (a["scope"] == "column" && a["column"] == "Case Column" && a["privilege"] == "UPDATE" && a["grantable"] == "false")
			}
			schemaGrant = schemaGrant || (e.Identity.Catalog == "pg_namespace" && a["grantee"] == role && a["privilege"] == "USAGE")
			sequenceGrant = sequenceGrant || (e.Identity.Catalog == "pg_class" && e.Identity.Name == "seq" && a["grantee"] == role && a["privilege"] == "USAGE")
			if e.Identity.Catalog == "pg_proc" && e.Identity.Name == "same" && a["granteeKind"] == "public" && a["privilege"] == "EXECUTE" {
				if e.Identity.Arguments[0].Name == "int4" {
					routineIntPublic = true
				}
				if e.Identity.Arguments[0].Name == "text" {
					routineTextPublic = true
				}
			}
		}
	}
	if !tableGrant || !columnGrant || !schemaGrant || !sequenceGrant || routineIntPublic || !routineTextPublic {
		t.Fatalf("ACL inventory table=%v column=%v schema=%v sequence=%v intPublic=%v textPublic=%v", tableGrant, columnGrant, schemaGrant, sequenceGrant, routineIntPublic, routineTextPublic)
	}
	if h.queryOne(fmt.Sprintf(`SELECT pg_catalog.has_column_privilege('%s','authority.docs','Case Column','UPDATE')::text`, role)) != "true" {
		t.Fatal("native column authority oracle disagrees")
	}
	if h.queryOne(fmt.Sprintf(`SELECT pg_catalog.has_table_privilege('%s','authority.docs','SELECT WITH GRANT OPTION')::text`, role)) != "true" {
		t.Fatal("native grant-option oracle disagrees")
	}
	raw, err := json.Marshal(doc.Model)
	if err != nil {
		t.Fatal(err)
	}
	imported, err := ParseV3Document(raw)
	if err != nil {
		t.Fatal(err)
	}
	if imported.SHA256Hex != doc.SHA256Hex {
		t.Fatal("ACL export/import hash changed")
	}
	repeated, err := h.client.IntrospectV3(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if repeated.SHA256Hex != doc.SHA256Hex {
		t.Fatal("unchanged ACL hash drift")
	}
}
