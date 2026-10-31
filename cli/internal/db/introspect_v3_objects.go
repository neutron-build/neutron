package db

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

const introspectV3PoliciesSQL = `
SELECT n.nspname,c.relname,p.polname,pg_catalog.pg_get_userbyid(c.relowner),
       p.polcmd::pg_catalog.text,p.polpermissive,c.relrowsecurity,c.relforcerowsecurity,
       COALESCE(pg_catalog.pg_get_expr(p.polqual,p.polrelid),''),
       COALESCE(pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid),''),
       COALESCE((SELECT pg_catalog.jsonb_agg(role_name ORDER BY role_name)::pg_catalog.text
        FROM (SELECT CASE WHEN roleid=0 THEN 'PUBLIC' ELSE pg_catalog.pg_get_userbyid(roleid) END AS role_name
         FROM pg_catalog.unnest(p.polroles) AS roles(roleid)) named),'[]')
FROM pg_catalog.pg_policy p JOIN pg_catalog.pg_class c ON c.oid=p.polrelid
JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
ORDER BY n.nspname,c.relname,p.polname
`

const introspectV3TriggersSQL = `
SELECT n.nspname,c.relname,t.tgname,pg_catalog.pg_get_userbyid(c.relowner),
       t.tgenabled::pg_catalog.text,pg_catalog.pg_get_triggerdef(t.oid),
       fn.nspname,f.proname,t.tgdeferrable,t.tginitdeferred,
       (t.tgconstraint<>0)
FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid
JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
JOIN pg_catalog.pg_proc f ON f.oid=t.tgfoid
JOIN pg_catalog.pg_namespace fn ON fn.oid=f.pronamespace
WHERE NOT t.tgisinternal AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
ORDER BY n.nspname,c.relname,t.tgname
`

// Portable addresses preserve even extension member kinds the CLI has no DDL
// implementation for. Unsupported PostgreSQL address extraction fails the read
// instead of treating that extension member as absent. Sort members in Go below
// so server collation/OID order cannot affect the portable hash.
const introspectV3ExtensionsSQL = `
SELECT n.nspname,x.extname,pg_catalog.pg_get_userbyid(x.extowner),x.extversion,x.extrelocatable,
       COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
         'name',pg_catalog.jsonb_build_array(addr.type,addr.object_names,addr.object_args)::pg_catalog.text,
         'kind','extension-member','attributes',pg_catalog.jsonb_build_object(
           'catalogSchema',cn.nspname,'catalogName',cl.relname,
           'objectType',addr.type,'objectNames',COALESCE(pg_catalog.to_json(addr.object_names)::pg_catalog.text,'[]'),
           'objectArguments',COALESCE(pg_catalog.to_json(addr.object_args)::pg_catalog.text,'[]'))))::pg_catalog.text
        FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_class cl ON cl.oid=d.classid
        JOIN pg_catalog.pg_namespace cn ON cn.oid=cl.relnamespace
        CROSS JOIN LATERAL pg_catalog.pg_identify_object_as_address(d.classid,d.objid,d.objsubid) addr
        WHERE d.deptype='e' AND d.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass AND d.refobjid=x.oid),'[]'),
       COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
         'name',pg_catalog.jsonb_build_array(tn.nspname,t.relname)::pg_catalog.text,
         'kind','configuration-table','attributes',pg_catalog.jsonb_build_object(
           'schema',tn.nspname,'table',t.relname,'condition',COALESCE(x.extcondition[config.ord],'')))
         ORDER BY config.ord)::pg_catalog.text
        FROM pg_catalog.unnest(x.extconfig) WITH ORDINALITY config(tableoid,ord)
        JOIN pg_catalog.pg_class t ON t.oid=config.tableoid
        JOIN pg_catalog.pg_namespace tn ON tn.oid=t.relnamespace),'[]'),
       COALESCE(pg_catalog.cardinality(x.extconfig),0)
FROM pg_catalog.pg_extension x JOIN pg_catalog.pg_namespace n ON n.oid=x.extnamespace
ORDER BY x.extname
`

func introspectV3Policies(ctx context.Context, q pgQueryer) ([]V3InventoryEntry, error) {
	rows, err := q.Query(ctx, introspectV3PoliciesSQL)
	if err != nil {
		return nil, fmt.Errorf("v3 policy inventory: %w", err)
	}
	defer rows.Close()
	entries := []V3InventoryEntry{}
	for rows.Next() {
		var schema, table, name, owner, command, using, check, roles string
		var permissive, enabled, forced bool
		if err := rows.Scan(&schema, &table, &name, &owner, &command, &permissive, &enabled, &forced, &using, &check, &roles); err != nil {
			return nil, err
		}
		entry := v3Entry("pg_policy", schema, name, "policy", owner, "")
		entry.Identity.Parent = &V2Identity{Schema: schema, Name: table}
		// Roles form a set. Normalize by portable role name in Go, not server
		// collation or the ephemeral role OID order in polroles.
		var names []string
		if err := json.Unmarshal([]byte(roles), &names); err != nil {
			return nil, err
		}
		sort.Strings(names)
		encoded, _ := json.Marshal(names)
		entry.Attributes = map[string]string{"command": command, "permissive": strconv.FormatBool(permissive), "rowSecurityEnabled": strconv.FormatBool(enabled), "forceRowSecurity": strconv.FormatBool(forced), "using": using, "check": check, "roles": string(encoded)}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func introspectV3Triggers(ctx context.Context, q pgQueryer) ([]V3InventoryEntry, error) {
	rows, err := q.Query(ctx, introspectV3TriggersSQL)
	if err != nil {
		return nil, fmt.Errorf("v3 trigger inventory: %w", err)
	}
	defer rows.Close()
	entries := []V3InventoryEntry{}
	for rows.Next() {
		var schema, table, name, owner, enabled, definition, functionSchema, functionName string
		var deferrable, deferred, constraint bool
		if err := rows.Scan(&schema, &table, &name, &owner, &enabled, &definition, &functionSchema, &functionName, &deferrable, &deferred, &constraint); err != nil {
			return nil, err
		}
		entry := v3Entry("pg_trigger", schema, name, "trigger", owner, "")
		entry.Identity.Parent = &V2Identity{Schema: schema, Name: table}
		entry.Definition = definition
		entry.References["function"] = V2Identity{Schema: functionSchema, Name: functionName}
		entry.Attributes = map[string]string{"enabled": enabled, "deferrable": strconv.FormatBool(deferrable), "initiallyDeferred": strconv.FormatBool(deferred), "constraint": strconv.FormatBool(constraint)}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func introspectV3Extensions(ctx context.Context, q pgQueryer) ([]V3InventoryEntry, error) {
	rows, err := q.Query(ctx, introspectV3ExtensionsSQL)
	if err != nil {
		return nil, fmt.Errorf("v3 extension inventory: %w", err)
	}
	defer rows.Close()
	entries := []V3InventoryEntry{}
	for rows.Next() {
		var schema, name, owner, version, members, config string
		var relocatable bool
		var configCount int
		if err := rows.Scan(&schema, &name, &owner, &version, &relocatable, &members, &config, &configCount); err != nil {
			return nil, err
		}
		entry := v3Entry("pg_extension", schema, name, "extension", owner, name)
		entry.Attributes = map[string]string{"version": version, "relocatable": strconv.FormatBool(relocatable)}
		if err := json.Unmarshal([]byte(members), &entry.Parts); err != nil {
			return nil, err
		}
		sort.Slice(entry.Parts, func(i, j int) bool { return entry.Parts[i].Name < entry.Parts[j].Name })
		var configParts []V3InventoryPart
		if err := json.Unmarshal([]byte(config), &configParts); err != nil {
			return nil, err
		}
		if len(configParts) != configCount {
			return nil, fmt.Errorf("v3 extension %s configuration table address missing; refusing incomplete inventory", name)
		}
		entry.Parts = append(entry.Parts, configParts...)
		entry.Reason = "extension version and portable member/configuration addresses retained; installation/update/member DDL unsupported; unmanaged"
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}
