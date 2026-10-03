package db

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// Cluster role names/options are authority identities. Passwords and role
// configuration values are deliberately excluded; there is no restore planner.
// pg_roles exposes the public non-secret catalog shape, not pg_authid secrets.
const introspectV3RolesSQL = `
SELECT r.rolname,pg_catalog.jsonb_build_object(
 'superuser',r.rolsuper::pg_catalog.text,'inherit',r.rolinherit::pg_catalog.text,
 'createRole',r.rolcreaterole::pg_catalog.text,'createDatabase',r.rolcreatedb::pg_catalog.text,
 'login',r.rolcanlogin::pg_catalog.text,'replication',r.rolreplication::pg_catalog.text,
 'bypassRLS',r.rolbypassrls::pg_catalog.text,'connectionLimit',r.rolconnlimit::pg_catalog.text,
 'validUntilEpoch',COALESCE(EXTRACT(EPOCH FROM r.rolvaliduntil)::pg_catalog.text,''))::pg_catalog.text,
 COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
  'name',pg_catalog.jsonb_build_array(member.rolname,grantor.rolname)::pg_catalog.text,'kind','role-membership',
  'attributes',pg_catalog.jsonb_build_object('member',member.rolname,'grantor',grantor.rolname,
  'admin',m.admin_option::pg_catalog.text,'inherit',m.inherit_option::pg_catalog.text,'set',m.set_option::pg_catalog.text)))::pg_catalog.text
 FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles member ON member.oid=m.member
 JOIN pg_catalog.pg_roles grantor ON grantor.oid=m.grantor WHERE m.roleid=r.oid),'[]')
FROM pg_catalog.pg_roles r
`

func introspectV3Roles(ctx context.Context, q pgQueryer) ([]V3InventoryEntry, error) {
	rows, err := q.Query(ctx, introspectV3RolesSQL)
	if err != nil {
		return nil, fmt.Errorf("v3 role authority inventory: %w", err)
	}
	defer rows.Close()
	entries := []V3InventoryEntry{}
	for rows.Next() {
		var name, attributes, parts string
		if err := rows.Scan(&name, &attributes, &parts); err != nil {
			return nil, err
		}
		e := v3Entry("pg_roles", "pg_catalog", name, "cluster-role-authority", "", "")
		e.Reason = "cluster role identity/options and direct membership only; passwords/configuration values excluded; effective authority and restore unsupported"
		if err := json.Unmarshal([]byte(attributes), &e.Attributes); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(parts), &e.Parts); err != nil {
			return nil, err
		}
		e.Attributes["namespaceScope"] = "cluster; pg_catalog is catalog-address namespace"
		sort.Slice(e.Parts, func(i, j int) bool { return e.Parts[i].Name < e.Parts[j].Name })
		entries = append(entries, e)
	}
	return entries, rows.Err()
}
