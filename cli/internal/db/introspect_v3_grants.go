package db

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// Resolve all identities through catalogs, never rendered regprocedure strings.
// NULL ACL means PostgreSQL's owner/type defaults, not an empty grant set.
// Column grants have no implicit defaults and remain a separate scope.
const introspectV3GrantsSQL = `
WITH objects AS (
 SELECT pg_catalog.jsonb_build_object('catalog','pg_class','schema',n.nspname,'name',c.relname) AS identity,
        'relation-authority' AS kind,c.relowner AS owner,c.relacl AS acl,
        CASE WHEN c.relkind='S' THEN 's' ELSE 'r' END::pg_catalog."char" AS defaults,
        c.oid AS relation
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE c.relkind IN ('r','p','v','m','f','S') AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
 UNION ALL
 SELECT pg_catalog.jsonb_build_object('catalog','pg_namespace','schema',n.nspname,'name',n.nspname),
        'schema-authority',n.nspowner,n.nspacl,'n'::pg_catalog."char",0::pg_catalog.oid
 FROM pg_catalog.pg_namespace n WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
 UNION ALL
 SELECT pg_catalog.jsonb_build_object('catalog','pg_proc','schema',n.nspname,'name',p.proname,'arguments',
        COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('schema',tn.nspname,'name',t.typname) ORDER BY a.ord)
        FROM pg_catalog.unnest(p.proargtypes::pg_catalog.oid[]) WITH ORDINALITY a(typeoid,ord)
        JOIN pg_catalog.pg_type t ON t.oid=a.typeoid JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace),'[]'::pg_catalog.jsonb)),
        'routine-authority',p.proowner,p.proacl,'f'::pg_catalog."char",0::pg_catalog.oid
 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
 UNION ALL
 SELECT pg_catalog.jsonb_build_object('catalog','pg_type','schema',n.nspname,'name',t.typname),
        'type-authority',t.typowner,t.typacl,'T'::pg_catalog."char",0::pg_catalog.oid
 FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace
 WHERE t.typisdefined AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
)
SELECT o.identity::pg_catalog.text,o.kind,pg_catalog.pg_get_userbyid(o.owner),
       CASE WHEN o.acl IS NULL THEN 'default' ELSE 'explicit' END,
       COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'name',pg_catalog.jsonb_build_array(scope,column_name,grantor_name,grantee_kind,grantee_name,privilege_type,is_grantable)::pg_catalog.text,
          'kind','privilege','attributes',pg_catalog.jsonb_build_object(
            'scope',scope,'column',column_name,'grantor',grantor_name,'granteeKind',grantee_kind,
            'grantee',grantee_name,'privilege',privilege_type,'grantable',is_grantable::pg_catalog.text)))::pg_catalog.text
        FROM (
          SELECT 'object' AS scope,'' AS column_name,pg_catalog.pg_get_userbyid(a.grantor) AS grantor_name,
                 CASE WHEN a.grantee=0 THEN 'public' ELSE 'role' END AS grantee_kind,
                 CASE WHEN a.grantee=0 THEN '' ELSE pg_catalog.pg_get_userbyid(a.grantee) END AS grantee_name,
                 a.privilege_type,a.is_grantable
          FROM pg_catalog.aclexplode(COALESCE(o.acl,pg_catalog.acldefault(o.defaults,o.owner))) a
          UNION ALL
          SELECT 'column',at.attname,pg_catalog.pg_get_userbyid(a.grantor),
                 CASE WHEN a.grantee=0 THEN 'public' ELSE 'role' END,
                 CASE WHEN a.grantee=0 THEN '' ELSE pg_catalog.pg_get_userbyid(a.grantee) END,
                 a.privilege_type,a.is_grantable
          FROM pg_catalog.pg_attribute at CROSS JOIN LATERAL pg_catalog.aclexplode(at.attacl) a
          WHERE at.attrelid=o.relation AND at.attnum>0 AND NOT at.attisdropped
        ) grants),'[]')
FROM objects o
`

func attachV3Grants(ctx context.Context, q pgQueryer, inventory []V3InventoryEntry) ([]V3InventoryEntry, error) {
	rows, err := q.Query(ctx, introspectV3GrantsSQL)
	if err != nil {
		return nil, fmt.Errorf("v3 ACL inventory: %w", err)
	}
	defer rows.Close()
	indexed := map[string]int{}
	for i := range inventory {
		key, _ := json.Marshal(inventory[i].Identity)
		indexed[string(key)] = i
	}
	for rows.Next() {
		var identityJSON, kind, owner, storage, partsJSON string
		if err := rows.Scan(&identityJSON, &kind, &owner, &storage, &partsJSON); err != nil {
			return nil, err
		}
		var identity V3ObjectIdentity
		if err := json.Unmarshal([]byte(identityJSON), &identity); err != nil {
			return nil, err
		}
		var parts []V3InventoryPart
		if err := json.Unmarshal([]byte(partsJSON), &parts); err != nil {
			return nil, err
		}
		// ACL rows are a set. Never hash catalog iteration, OID or server collation order.
		sort.Slice(parts, func(i, j int) bool { return parts[i].Name < parts[j].Name })
		key, _ := json.Marshal(identity)
		i, found := indexed[string(key)]
		if !found {
			entry := v3Entry(identity.Catalog, identity.Schema, identity.Name, kind, owner, "")
			entry.Identity = identity
			entry.Reason = "authority inventory only; complete object definition, inherited/effective authority and DDL reconstruction unsupported"
			i = len(inventory)
			indexed[string(key)] = i
			inventory = append(inventory, entry)
		}
		inventory[i].Attributes["aclStorage"] = storage
		inventory[i].Parts = append(inventory[i].Parts, parts...)
	}
	return inventory, rows.Err()
}
