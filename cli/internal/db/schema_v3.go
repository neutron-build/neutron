package db

// Schema-v3 adds preserve-only catalog inventory to an unchanged schema-v2
// relational document. It is a representation contract, not a new DDL planner.
import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const SchemaDocumentVersionV3 = 3

// Catalog plus qualified name identifies a catalog namespace. PostgreSQL
// routine identity additionally includes ordered INPUT/INOUT type identities;
// OUT names/types do not distinguish overloads. Parent scopes table-local
// objects. None of these fields is an ephemeral catalog OID.
type V3ObjectIdentity struct {
	Catalog   string       `json:"catalog"`
	Schema    string       `json:"schema"`
	Name      string       `json:"name"`
	Parent    *V2Identity  `json:"parent,omitempty"`
	Arguments []V2Identity `json:"arguments,omitempty"`
}

func (id V3ObjectIdentity) MarshalJSON() ([]byte, error) {
	type plain V3ObjectIdentity
	if id.Catalog != "pg_proc" {
		return json.Marshal(plain(id))
	}
	// A zero-argument routine still has an explicit signature; omitempty must
	// not turn it into an unspecified overload during export/import.
	arguments := id.Arguments
	if arguments == nil {
		arguments = []V2Identity{}
	}
	return json.Marshal(struct {
		plain
		Arguments []V2Identity `json:"arguments"`
	}{plain: plain(id), Arguments: arguments})
}

type V3InventoryPart struct {
	Name       string            `json:"name"`
	Kind       string            `json:"kind"`
	Type       *V2Identity       `json:"type,omitempty"`
	Definition string            `json:"definition,omitempty"`
	Attributes map[string]string `json:"attributes"`
}

// An inventory entry is always unmanaged. Unknown metadata may be retained
// verbatim in named string attributes/parts; it is never translated into DDL.
// Definition is descriptive SQL, not executable migration authorization.
type V3InventoryEntry struct {
	Identity   V3ObjectIdentity      `json:"identity"`
	Kind       string                `json:"kind"`
	Managed    bool                  `json:"managed"`
	Owner      string                `json:"owner"`
	Extension  string                `json:"extension"`
	Reason     string                `json:"reason"`
	Definition string                `json:"definition"`
	Attributes map[string]string     `json:"attributes"`
	References map[string]V2Identity `json:"references"`
	Parts      []V3InventoryPart     `json:"parts"`
}

type V3Coverage struct {
	Family string `json:"family"`
	Status string `json:"status"` // identity-inventory | partial | not-inspected
	Detail string `json:"detail"`
}

var v3Families = []string{"relations", "routines", "types", "policies", "grants", "triggers", "extensions"}

type V3DocumentModel struct {
	Version    int                `json:"version"`
	Relational json.RawMessage    `json:"relational"` // complete validated v2 document
	Coverage   []V3Coverage       `json:"coverage"`
	Inventory  []V3InventoryEntry `json:"inventory"`
}

type V3Document struct {
	Model      V3DocumentModel
	Relational *V2Document
	Canonical  []byte
	SHA256Hex  string
}

func v3IdentityKey(id V3ObjectIdentity) string {
	// JSON tuple encoding avoids separator collisions for quoted identifiers.
	b, _ := json.Marshal(id)
	return string(b)
}

func v3CheckQualified(id V2Identity, path string) error {
	if err := v2CheckName(id.Schema, path+".schema"); err != nil {
		return err
	}
	return v2CheckName(id.Name, path+".name")
}

func v3CheckMap(values map[string]string, path string) error {
	if values == nil {
		return contractErr("missing-field", path, "map must be present and non-null")
	}
	for key := range values {
		if err := v2CheckName(key, path); err != nil {
			return err
		}
		switch strings.ToLower(key) {
		case "oid", "objid", "classid", "refobjid", "refclassid":
			return contractErr("nonportable-identity", path+"."+key, "ephemeral catalog OIDs must not enter portable inventory")
		}
	}
	return nil
}

// v3Required rejects omitted/defaulted authority fields before typed decoding.
// Null is not part of this contract; optional fields must be absent instead.
func v3Required(root any, path string, required ...string) error {
	obj, ok := root.(map[string]any)
	if !ok {
		return contractErr("not-object", path, "expected object")
	}
	for _, key := range required {
		if _, ok := obj[key]; !ok {
			return contractErr("missing-field", path+"."+key, "required field is absent")
		}
	}
	return nil
}

func v3RejectNull(v any, path string) error {
	switch x := v.(type) {
	case nil:
		return contractErr("invalid-value", path, "null is not representable; omit optional fields")
	case []any:
		for i, item := range x {
			if err := v3RejectNull(item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	case map[string]any:
		for key, item := range x {
			if strings.ContainsRune(key, 0xFFFD) {
				return contractErr("invalid-value", path, "object key contains invalid Unicode")
			}
			if err := v3RejectNull(item, path+"."+key); err != nil {
				return err
			}
		}
	}
	return nil
}

func ParseV3Document(raw []byte) (*V3Document, error) {
	if err := v2RejectDuplicateKeys(raw); err != nil {
		return nil, err
	}
	if _, err := DetectSchemaVersion(raw); err != nil {
		return nil, err
	}
	var tree any
	if err := json.Unmarshal(raw, &tree); err != nil {
		return nil, contractErr("invalid-json", "$", "%v", err)
	}
	if err := v2ScanStrings(tree, "$"); err != nil {
		return nil, err
	}
	if err := v3RejectNull(tree, "$"); err != nil {
		return nil, err
	}
	if err := v3Required(tree, "$", "version", "relational", "coverage", "inventory"); err != nil {
		return nil, err
	}
	root := tree.(map[string]any)
	version, err := v2Int(root["version"], "$.version")
	if err != nil {
		return nil, err
	}
	if version != SchemaDocumentVersionV3 {
		return nil, contractErr("unknown-version", "$.version", "expected version 3")
	}
	var model V3DocumentModel
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&model); err != nil {
		return nil, contractErr("invalid-value", "$", "%v", err)
	}
	relational, err := ParseV2Document(model.Relational)
	if err != nil {
		return nil, fmt.Errorf("v3 relational snapshot: %w", err)
	}
	if model.Coverage == nil || model.Inventory == nil {
		return nil, contractErr("missing-field", "$", "coverage and inventory must be arrays")
	}
	coverage := map[string]bool{}
	for i, item := range model.Coverage {
		path := fmt.Sprintf("$.coverage[%d]", i)
		if err := v3Required(root["coverage"].([]any)[i], path, "family", "status", "detail"); err != nil {
			return nil, err
		}
		known := false
		for _, family := range v3Families {
			if item.Family == family {
				known = true
			}
		}
		if !known || coverage[item.Family] {
			return nil, contractErr("invalid-coverage", path, "unknown or duplicate family %q", item.Family)
		}
		coverage[item.Family] = true
		if item.Status != "identity-inventory" && item.Status != "partial" && item.Status != "not-inspected" {
			return nil, contractErr("invalid-coverage", path, "unsupported coverage status")
		}
		if strings.TrimSpace(item.Detail) == "" {
			return nil, contractErr("invalid-coverage", path, "coverage requires a limitation/detail")
		}
	}
	for _, family := range v3Families {
		if !coverage[family] {
			return nil, contractErr("invalid-coverage", "$.coverage", "missing family %q", family)
		}
	}
	seen := map[string]bool{}
	for i, entry := range model.Inventory {
		path := fmt.Sprintf("$.inventory[%d]", i)
		rawEntry := root["inventory"].([]any)[i]
		if err := v3Required(rawEntry, path, "identity", "kind", "managed", "owner", "extension", "reason", "definition", "attributes", "references", "parts"); err != nil {
			return nil, err
		}
		identity := rawEntry.(map[string]any)["identity"]
		if err := v3Required(identity, path+".identity", "catalog", "schema", "name"); err != nil {
			return nil, err
		}
		if err := v2CheckName(entry.Identity.Catalog, path+".identity.catalog"); err != nil {
			return nil, err
		}
		if err := v3CheckQualified(V2Identity{Schema: entry.Identity.Schema, Name: entry.Identity.Name}, path+".identity"); err != nil {
			return nil, err
		}
		if entry.Identity.Parent != nil {
			if err := v3CheckQualified(*entry.Identity.Parent, path+".identity.parent"); err != nil {
				return nil, err
			}
		}
		if entry.Identity.Catalog == "pg_proc" {
			if _, present := identity.(map[string]any)["arguments"]; !present {
				return nil, contractErr("missing-signature", path+".identity.arguments", "routine identity requires ordered input argument types (including empty array)")
			}
		} else if _, present := identity.(map[string]any)["arguments"]; present {
			return nil, contractErr("invalid-signature", path+".identity.arguments", "arguments only identify pg_proc overloads")
		}
		for j, arg := range entry.Identity.Arguments {
			if err := v3CheckQualified(arg, fmt.Sprintf("%s.identity.arguments[%d]", path, j)); err != nil {
				return nil, err
			}
		}
		key := v3IdentityKey(entry.Identity)
		if seen[key] {
			return nil, contractErr("duplicate-catalog-object", path, "duplicate portable identity")
		}
		seen[key] = true
		if entry.Managed {
			return nil, contractErr("unmanaged-only", path+".managed", "v3 inventory has no authorized DDL planner")
		}
		if err := v2CheckName(entry.Kind, path+".kind"); err != nil {
			return nil, err
		}
		if strings.TrimSpace(entry.Reason) == "" {
			return nil, contractErr("missing-reason", path, "unmanaged entry requires reason")
		}
		if err := v3CheckMap(entry.Attributes, path+".attributes"); err != nil {
			return nil, err
		}
		if entry.References == nil || entry.Parts == nil {
			return nil, contractErr("missing-field", path, "references and parts must be present")
		}
		for name, ref := range entry.References {
			if err := v2CheckName(name, path+".references"); err != nil {
				return nil, err
			}
			if err := v3CheckQualified(ref, path+".references."+name); err != nil {
				return nil, err
			}
		}
		for j, part := range entry.Parts {
			partPath := fmt.Sprintf("%s.parts[%d]", path, j)
			if err := v3Required(rawEntry.(map[string]any)["parts"].([]any)[j], partPath, "name", "kind", "attributes"); err != nil {
				return nil, err
			}
			if err := v2CheckName(part.Kind, partPath+".kind"); err != nil {
				return nil, err
			}
			if part.Type != nil {
				if err := v3CheckQualified(*part.Type, partPath+".type"); err != nil {
					return nil, err
				}
			}
			if err := v3CheckMap(part.Attributes, partPath+".attributes"); err != nil {
				return nil, err
			}
		}
	}
	// Canonicalize the nested v2 using its EXISTING byte/hash rules, not struct tags.
	model.Relational = relational.Canonical
	sort.Slice(model.Coverage, func(i, j int) bool { return model.Coverage[i].Family < model.Coverage[j].Family })
	sort.Slice(model.Inventory, func(i, j int) bool {
		return v3IdentityKey(model.Inventory[i].Identity) < v3IdentityKey(model.Inventory[j].Identity)
	})
	// Ensure zero-argument routines keep their required empty signature array.
	canonicalTree := map[string]any{}
	typed, _ := json.Marshal(model)
	if err := json.Unmarshal(typed, &canonicalTree); err != nil {
		return nil, err
	}
	for i, entry := range model.Inventory {
		if entry.Identity.Catalog == "pg_proc" && len(entry.Identity.Arguments) == 0 {
			canonicalTree["inventory"].([]any)[i].(map[string]any)["identity"].(map[string]any)["arguments"] = []any{}
		}
	}
	canonical := v2AppendValue(nil, canonicalTree)
	sum := sha256.Sum256(canonical)
	return &V3Document{Model: model, Relational: relational, Canonical: canonical, SHA256Hex: hex.EncodeToString(sum[:])}, nil
}

// UpgradeSchemaDocumentV3 preserves v1/v2 semantics. It does not claim newly
// added catalogs are empty: every family not captured by the old representation
// is explicitly not-inspected. V1 ambiguity refusals remain unchanged.
func UpgradeSchemaDocumentV3(raw []byte) (*V3Document, error) {
	version, err := DetectSchemaVersion(raw)
	if err != nil {
		return nil, err
	}
	if version == SchemaDocumentVersionV3 {
		return ParseV3Document(raw)
	}
	if version == SchemaVersion {
		var schema Schema
		if err := json.Unmarshal(raw, &schema); err != nil {
			return nil, err
		}
		if err := ValidateSchema(&schema); err != nil {
			return nil, err
		}
		root, err := UpgradeV1Schema(&schema)
		if err != nil {
			return nil, err
		}
		raw, err = json.Marshal(root)
		if err != nil {
			return nil, err
		}
	} else if version != SchemaDocumentVersionV2 {
		return nil, contractErr("unknown-version", "$.version", "cannot upgrade version %d", version)
	}
	relational, err := ParseV2Document(raw)
	if err != nil {
		return nil, err
	}
	model := V3DocumentModel{Version: 3, Relational: relational.Canonical, Coverage: []V3Coverage{}, Inventory: []V3InventoryEntry{}}
	for _, family := range v3Families {
		status, detail := "not-inspected", "legacy document did not inventory this catalog family; absence is unknown"
		if family == "relations" {
			status, detail = "partial", "validated v2 relational subset retained; unsupported shapes remain v2 opaque inventory"
		}
		model.Coverage = append(model.Coverage, V3Coverage{Family: family, Status: status, Detail: detail})
	}
	encoded, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}
	return ParseV3Document(encoded)
}

// RefuseMigration is deliberate: representation/import is not permission to
// create/alter/drop preserved catalog objects or omit uninspected families.
func (d *V3Document) RefuseMigration() error {
	return contractErr("v3-inventory-only", "$", "schema-v3 is preserve-only catalog inventory; migration planning is not implemented")
}
