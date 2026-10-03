package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
)

// No catalog OID is selected into serialized identity. OIDs are transient join
// keys only. proargtypes is PostgreSQL's ordered INPUT/INOUT overload signature.
const introspectV3RoutinesSQL = `
SELECT n.nspname, p.proname, p.prokind::pg_catalog.text,
       pg_catalog.pg_get_userbyid(p.proowner), COALESCE(x.extname,''),
       COALESCE(x.extversion,''),
       COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
           'schema', tn.nspname, 'name', t.typname) ORDER BY a.ord)::pg_catalog.text
         FROM pg_catalog.unnest(p.proargtypes::pg_catalog.oid[]) WITH ORDINALITY a(typeoid,ord)
         JOIN pg_catalog.pg_type t ON t.oid=a.typeoid
         JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace),'[]'),
       rn.nspname, rt.typname,
       CASE WHEN p.prokind='a' THEN '' ELSE pg_catalog.pg_get_functiondef(p.oid) END,
       p.provolatile::pg_catalog.text,p.proparallel::pg_catalog.text,
       p.prosecdef,p.proisstrict,p.proleakproof,p.proretset,l.lanname,
       COALESCE(p.proconfig,'{}'::pg_catalog.text[]),
       COALESCE(p.proacl::pg_catalog.text,''),
       COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
           'name',COALESCE(p.proargnames[a.ord],''),'kind','argument',
           'type',pg_catalog.jsonb_build_object('schema',tn.nspname,'name',t.typname),
           'attributes',pg_catalog.jsonb_build_object('mode',COALESCE(p.proargmodes[a.ord]::pg_catalog.text,'i')))
           ORDER BY a.ord)::pg_catalog.text
         FROM pg_catalog.unnest(COALESCE(p.proallargtypes,p.proargtypes::pg_catalog.oid[])) WITH ORDINALITY a(typeoid,ord)
         JOIN pg_catalog.pg_type t ON t.oid=a.typeoid
         JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace),'[]')
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
JOIN pg_catalog.pg_type rt ON rt.oid=p.prorettype
JOIN pg_catalog.pg_namespace rn ON rn.oid=rt.typnamespace
JOIN pg_catalog.pg_language l ON l.oid=p.prolang
LEFT JOIN pg_catalog.pg_depend d ON d.classid='pg_catalog.pg_proc'::pg_catalog.regclass
 AND d.objid=p.oid AND d.deptype='e' AND d.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass
LEFT JOIN pg_catalog.pg_extension x ON x.oid=d.refobjid
WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
ORDER BY n.nspname,p.proname,p.proargtypes::pg_catalog.text
`

const introspectV3TypesSQL = `
SELECT n.nspname,t.typname,t.typtype::pg_catalog.text,
       pg_catalog.pg_get_userbyid(t.typowner),COALESCE(x.extname,ix.extname,''),COALESCE(x.extversion,ix.extversion,''),
       CASE WHEN x.oid IS NOT NULL THEN 'direct' WHEN ix.oid IS NOT NULL THEN 'internal-dependent' ELSE 'none' END,
       t.typcategory::pg_catalog.text,t.typtypmod::pg_catalog.text,t.typnotnull,t.typisdefined,
       COALESCE(pg_catalog.pg_get_expr(t.typdefaultbin,0),t.typdefault,''),
       (t.typdefaultbin IS NOT NULL OR t.typdefault IS NOT NULL),
       COALESCE((SELECT pg_catalog.jsonb_object_agg(r.label,pg_catalog.jsonb_build_object('schema',r.schema,'name',r.name))::pg_catalog.text
        FROM (
         SELECT 'base' AS label,bn.nspname AS schema,b.typname AS name FROM pg_catalog.pg_type b JOIN pg_catalog.pg_namespace bn ON bn.oid=b.typnamespace WHERE b.oid=t.typbasetype
         UNION ALL SELECT 'element',en.nspname,e.typname FROM pg_catalog.pg_type e JOIN pg_catalog.pg_namespace en ON en.oid=e.typnamespace WHERE e.oid=t.typelem
         UNION ALL SELECT 'array',an.nspname,a.typname FROM pg_catalog.pg_type a JOIN pg_catalog.pg_namespace an ON an.oid=a.typnamespace WHERE a.oid=t.typarray
         UNION ALL SELECT 'relation',cn.nspname,c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace cn ON cn.oid=c.relnamespace WHERE c.oid=t.typrelid
         UNION ALL SELECT 'rangeSubtype',sn.nspname,st.typname FROM pg_catalog.pg_range rg JOIN pg_catalog.pg_type st ON st.oid=rg.rngsubtype JOIN pg_catalog.pg_namespace sn ON sn.oid=st.typnamespace WHERE rg.rngtypid=t.oid OR rg.rngmultitypid=t.oid
         UNION ALL SELECT 'range',rn.nspname,rt.typname FROM pg_catalog.pg_range rg JOIN pg_catalog.pg_type rt ON rt.oid=rg.rngtypid JOIN pg_catalog.pg_namespace rn ON rn.oid=rt.typnamespace WHERE rg.rngtypid=t.oid OR rg.rngmultitypid=t.oid
         UNION ALL SELECT 'multirange',mn.nspname,mt.typname FROM pg_catalog.pg_range rg JOIN pg_catalog.pg_type mt ON mt.oid=rg.rngmultitypid JOIN pg_catalog.pg_namespace mn ON mn.oid=mt.typnamespace WHERE rg.rngtypid=t.oid OR rg.rngmultitypid=t.oid
         UNION ALL SELECT 'rangeOpclass',onsp.nspname,oc.opcname FROM pg_catalog.pg_range rg JOIN pg_catalog.pg_opclass oc ON oc.oid=rg.rngsubopc JOIN pg_catalog.pg_namespace onsp ON onsp.oid=oc.opcnamespace WHERE rg.rngtypid=t.oid OR rg.rngmultitypid=t.oid
         UNION ALL SELECT 'rangeCollation',cn.nspname,col.collname FROM pg_catalog.pg_range rg JOIN pg_catalog.pg_collation col ON col.oid=rg.rngcollation JOIN pg_catalog.pg_namespace cn ON cn.oid=col.collnamespace WHERE rg.rngtypid=t.oid OR rg.rngmultitypid=t.oid
        ) r),'{}'),
       COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'name',a.attname,'kind','attribute','type',pg_catalog.jsonb_build_object('schema',an.nspname,'name',at.typname),
          'attributes',pg_catalog.jsonb_build_object('typeModifier',a.atttypmod::pg_catalog.text,'notNull',a.attnotnull::pg_catalog.text))
          ORDER BY a.attnum)::pg_catalog.text
         FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type at ON at.oid=a.atttypid JOIN pg_catalog.pg_namespace an ON an.oid=at.typnamespace
         WHERE a.attrelid=t.typrelid AND a.attnum>0 AND NOT a.attisdropped),'[]'),
       COALESCE((SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',c.conname,'kind','constraint','definition',pg_catalog.pg_get_constraintdef(c.oid),
          'attributes',pg_catalog.jsonb_build_object('validated',c.convalidated::pg_catalog.text)) ORDER BY c.conname)::pg_catalog.text
         FROM pg_catalog.pg_constraint c WHERE c.contypid=t.oid),'[]'),
       COALESCE((SELECT pg_catalog.jsonb_agg(e.enumlabel ORDER BY e.enumsortorder)::pg_catalog.text FROM pg_catalog.pg_enum e WHERE e.enumtypid=t.oid),'[]'),
       COALESCE(t.typacl::pg_catalog.text,'')
FROM pg_catalog.pg_type t
JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace
LEFT JOIN pg_catalog.pg_depend d ON d.classid='pg_catalog.pg_type'::pg_catalog.regclass
 AND d.objid=t.oid AND d.deptype='e' AND d.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass
LEFT JOIN pg_catalog.pg_extension x ON x.oid=d.refobjid
LEFT JOIN pg_catalog.pg_depend internal ON internal.classid='pg_catalog.pg_type'::pg_catalog.regclass
 AND internal.objid=t.oid AND internal.deptype='i'
 AND internal.refclassid IN ('pg_catalog.pg_type'::pg_catalog.regclass,'pg_catalog.pg_class'::pg_catalog.regclass)
LEFT JOIN pg_catalog.pg_depend internal_owner ON internal_owner.classid=internal.refclassid
 AND internal_owner.objid=internal.refobjid AND internal_owner.deptype='e'
 AND internal_owner.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass
LEFT JOIN pg_catalog.pg_extension ix ON ix.oid=internal_owner.refobjid
WHERE n.nspname <> 'information_schema' AND n.nspname !~ '^pg_'
ORDER BY n.nspname,t.typname
`

func v3Entry(catalog, schema, name, kind, owner, extension string) V3InventoryEntry {
	return V3InventoryEntry{Identity: V3ObjectIdentity{Catalog: catalog, Schema: schema, Name: name}, Kind: kind, Managed: false, Owner: owner, Extension: extension,
		Reason: "preserve-only catalog inventory; DDL planning unsupported", Attributes: map[string]string{}, References: map[string]V2Identity{}, Parts: []V3InventoryPart{}}
}

func introspectV3Routines(ctx context.Context, q pgQueryer) ([]V3InventoryEntry, error) {
	rows, err := q.Query(ctx, introspectV3RoutinesSQL)
	if err != nil {
		return nil, fmt.Errorf("v3 routine inventory: %w", err)
	}
	defer rows.Close()
	entries := []V3InventoryEntry{}
	for rows.Next() {
		var schema, name, kind, owner, extension, extensionVersion, args, returnSchema, returnName, definition, volatility, parallel, language, acl, parts string
		var securityDefiner, strict, leakproof, returnsSet bool
		var settings []string
		if err := rows.Scan(&schema, &name, &kind, &owner, &extension, &extensionVersion, &args, &returnSchema, &returnName, &definition, &volatility, &parallel, &securityDefiner, &strict, &leakproof, &returnsSet, &language, &settings, &acl, &parts); err != nil {
			return nil, err
		}
		namedKind := map[string]string{"f": "function", "p": "procedure", "a": "aggregate", "w": "window-function"}[kind]
		if namedKind == "" {
			namedKind = "unknown-routine"
		}
		entry := v3Entry("pg_proc", schema, name, namedKind, owner, extension)
		entry.Definition = definition
		if err := json.Unmarshal([]byte(args), &entry.Identity.Arguments); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(parts), &entry.Parts); err != nil {
			return nil, err
		}
		entry.References["returnType"] = V2Identity{Schema: returnSchema, Name: returnName}
		encodedSettings, _ := json.Marshal(settings)
		entry.Attributes = map[string]string{"catalogKind": kind, "extensionVersion": extensionVersion, "volatility": volatility, "parallel": parallel, "securityDefiner": strconv.FormatBool(securityDefiner), "strict": strconv.FormatBool(strict), "leakproof": strconv.FormatBool(leakproof), "returnsSet": strconv.FormatBool(returnsSet), "language": language, "settings": string(encodedSettings), "acl": acl}
		if kind == "a" || namedKind == "unknown-routine" {
			entry.Reason = "routine identity retained; aggregate/unknown routine definition is not reconstructed; unmanaged"
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

func introspectV3Types(ctx context.Context, q pgQueryer) ([]V3InventoryEntry, error) {
	rows, err := q.Query(ctx, introspectV3TypesSQL)
	if err != nil {
		return nil, fmt.Errorf("v3 type inventory: %w", err)
	}
	defer rows.Close()
	entries := []V3InventoryEntry{}
	for rows.Next() {
		var schema, name, kind, owner, extension, extensionVersion, extensionOwnership, category, typeModifier, definition, references, attributes, constraints, enumValues, acl string
		var notNull, defined, hasDefault bool
		if err := rows.Scan(&schema, &name, &kind, &owner, &extension, &extensionVersion, &extensionOwnership, &category, &typeModifier, &notNull, &defined, &definition, &hasDefault, &references, &attributes, &constraints, &enumValues, &acl); err != nil {
			return nil, err
		}
		namedKind := map[string]string{"b": "base", "c": "composite", "d": "domain", "e": "enum", "r": "range", "m": "multirange", "p": "pseudo"}[kind]
		if namedKind == "" {
			namedKind = "unknown-type"
		}
		entry := v3Entry("pg_type", schema, name, namedKind, owner, extension)
		if !defined {
			// For shell types PostgreSQL only promises name/namespace/OID.
			// Never serialize guessed owner/default/type metadata.
			entry = v3Entry("pg_type", schema, name, "shell-type", "", extension)
			entry.Reason = "undefined shell type; only qualified identity is reliable; remaining metadata uninspected and unmanaged"
			entry.Attributes["defined"] = "false"
			entries = append(entries, entry)
			continue
		}
		entry.Definition = definition
		if err := json.Unmarshal([]byte(references), &entry.References); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(attributes), &entry.Parts); err != nil {
			return nil, err
		}
		var constraintParts []V3InventoryPart
		if err := json.Unmarshal([]byte(constraints), &constraintParts); err != nil {
			return nil, err
		}
		entry.Parts = append(entry.Parts, constraintParts...)
		entry.Attributes = map[string]string{"catalogKind": kind, "extensionVersion": extensionVersion, "category": category, "typeModifier": typeModifier, "notNull": strconv.FormatBool(notNull), "hasDefault": strconv.FormatBool(hasDefault), "enumValues": enumValues, "acl": acl}
		entry.Attributes["extensionOwnership"] = extensionOwnership
		if kind == "b" || kind == "p" || namedKind == "unknown-type" {
			entry.Reason = "type identity and selected metadata retained; input/output/storage semantics not reconstructed; unmanaged"
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// IntrospectV3 pins all v2/v3 catalog reads to one READ ONLY REPEATABLE READ
// snapshot and a catalog-only deparser path. This changes no database DDL/data;
// local settings disappear before the connection returns to its pool.
func (c *Client) IntrospectV3(ctx context.Context) (*V3Document, error) {
	tx, err := c.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "SET LOCAL search_path TO pg_catalog"); err != nil {
		return nil, err
	}
	relational, err := (&v2CatalogReader{pool: tx}).introspect(ctx)
	if err != nil {
		return nil, err
	}
	routines, err := introspectV3Routines(ctx, tx)
	if err != nil {
		return nil, err
	}
	types, err := introspectV3Types(ctx, tx)
	if err != nil {
		return nil, err
	}
	model := V3DocumentModel{Version: 3, Relational: relational.Canonical, Coverage: []V3Coverage{}, Inventory: append(routines, types...)}
	for _, read := range []func(context.Context, pgQueryer) ([]V3InventoryEntry, error){introspectV3Policies, introspectV3Triggers, introspectV3Extensions} {
		entries, err := read(ctx, tx)
		if err != nil {
			return nil, err
		}
		model.Inventory = append(model.Inventory, entries...)
	}
	model.Inventory, err = attachV3Grants(ctx, tx, model.Inventory)
	if err != nil {
		return nil, err
	}
	for _, family := range v3Families {
		status, detail := "not-inspected", "family not inventoried by this bounded reader; absence is unknown"
		switch family {
		case "relations":
			status, detail = "partial", "existing v2 user-schema subset and opaque refusal inventory; unsupported relation details remain opaque"
		case "routines":
			status, detail = "identity-inventory", "all visible user-schema pg_proc identities and input signatures; aggregate/unknown definitions remain unmanaged and incomplete"
		case "types":
			status, detail = "identity-inventory", "all visible user-schema pg_type identities, domain constraints, composite attributes, enums and range type references; base I/O/storage and complete range semantics remain unmanaged and incomplete"
		case "grants":
			status, detail = "partial", "user-schema relation/schema/routine/type owner defaults and explicit ACL tuples including column grants; global/schema default ACL tuples and current-database ACL included; other database/global authority and inherited effective access remain uninspected; unmanaged"
		case "policies":
			status, detail = "identity-inventory", "all visible user-schema policies with qualified table parents, roles, expressions and RLS flags; authority/DDL planning unsupported"
		case "triggers":
			status, detail = "identity-inventory", "all non-internal visible user-schema triggers with qualified table parents, function references, definitions and enablement; trigger DDL unsupported"
		case "extensions":
			status, detail = "identity-inventory", "all database extension records with version, owner and portable direct member/configuration addresses; install/update/member DDL unsupported"
		}
		model.Coverage = append(model.Coverage, V3Coverage{Family: family, Status: status, Detail: detail})
	}
	raw, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}
	doc, err := ParseV3Document(raw)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return doc, nil
}
