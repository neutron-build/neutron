package orm

import (
	"context"
	"fmt"
	"strings"
)

// DeferredForeignKey is a qualified native DEFERRABLE INITIALLY IMMEDIATE FK.
// Its name must be unique inside the schema because SET CONSTRAINTS qualifies
// by schema/name, not by table. Reconstruction is mandatory after relevant DDL.
type DeferredForeignKey struct{ schema, table, name, targetSchema, targetTable string }

func NewDeferredForeignKey[M any](ctx context.Context, db Executor, table Table[M], name string) (DeferredForeignKey, error) {
	if err := ready(ctx, db); err != nil {
		return DeferredForeignKey{}, err
	}
	if table.info == nil {
		return DeferredForeignKey{}, fmt.Errorf("orm: deferred FK requires initialized table")
	}
	if err := identifier(name); err != nil {
		return DeferredForeignKey{}, err
	}
	rows, err := db.Query(ctx, `SELECT t.relname,c.contype::text,c.condeferrable,c.condeferred,rn.nspname,rt.relname FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class t ON t.oid=c.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace LEFT JOIN pg_catalog.pg_class rt ON rt.oid=c.confrelid LEFT JOIN pg_catalog.pg_namespace rn ON rn.oid=rt.relnamespace WHERE n.nspname=$1 AND c.conname=$2`, table.info.schema, name)
	if err != nil {
		return DeferredForeignKey{}, wrap("deferred FK", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return DeferredForeignKey{}, err
		}
		return DeferredForeignKey{}, fmt.Errorf("orm: deferred FK missing")
	}
	var actual, kind string
	var deferred, initially bool
	var targetSchema, targetTable *string
	if err := rows.Scan(&actual, &kind, &deferred, &initially, &targetSchema, &targetTable); err != nil {
		return DeferredForeignKey{}, err
	}
	if actual != table.info.name || kind != "f" || !deferred || initially || targetSchema == nil || targetTable == nil || rows.Next() {
		return DeferredForeignKey{}, fmt.Errorf("orm: exact schema-unique initially-immediate deferrable FK required")
	}
	if err := rows.Err(); err != nil {
		return DeferredForeignKey{}, err
	}
	return DeferredForeignKey{table.info.schema, table.info.name, name, *targetSchema, *targetTable}, nil
}

type graphNodeID struct{ marker byte }

// GraphStep is a sealed, validated INSERT node; mapped models may differ.
type GraphStep interface {
	graphID() *graphNodeID
	graphInfo() *modelInfo
	graphValidate() error
	graphInsert(context.Context, *WriteSession) (any, error)
}
type GraphNode[M any] struct {
	id          *graphNodeID
	repo        HookRepository[M]
	assignments []Assignment[M]
}

func NewGraphNode[M any](repo HookRepository[M], assignments ...Assignment[M]) (GraphNode[M], error) {
	copy := append([]Assignment[M]{}, assignments...)
	if _, _, err := insertSQL(repo.table, copy); err != nil {
		return GraphNode[M]{}, err
	}
	return GraphNode[M]{&graphNodeID{}, repo, copy}, nil
}
func (n GraphNode[M]) graphID() *graphNodeID { return n.id }
func (n GraphNode[M]) graphInfo() *modelInfo { return n.repo.table.info }
func (n GraphNode[M]) graphValidate() error {
	if n.id == nil {
		return fmt.Errorf("orm: zero graph node")
	}
	_, _, err := insertSQL(n.repo.table, n.assignments)
	return err
}
func (n GraphNode[M]) graphInsert(ctx context.Context, session *WriteSession) (any, error) {
	return HookInsert(ctx, session, n.repo, n.assignments...)
}

type DeferredGraphResult struct{ models map[*graphNodeID]any }

func GraphNodeModel[M any](result DeferredGraphResult, node GraphNode[M]) (M, error) {
	var zero M
	value, ok := result.models[node.id]
	if !ok {
		return zero, fmt.Errorf("orm: node outside completed graph result")
	}
	model, ok := value.(M)
	if !ok {
		return zero, fmt.Errorf("orm: graph result model mismatch")
	}
	return *cloneHookEvent(WriteEvent[M]{Model: &model}, node.repo.table.info).Model, nil
}

type DeferredGraphBudget struct{ MaxNodes, MaxCoreStatements int }

// RunDeferredGraph inserts explicit pre-keyed heterogeneous nodes atomically
// under selected native deferred FKs. It forces FK validation before releasing
// the savepoint; failures remove all nodes and their queued after-commit events.
// Selected constraints enter DEFERRED and leave IMMEDIATE on success. The caller
// must use that initial/terminal mode contract. It does not infer relation keys,
// discover arbitrary cycles or allocate cross-node generated-key references.
func RunDeferredGraph(ctx context.Context, session *WriteSession, nodes []GraphStep, constraints []DeferredForeignKey, budget DeferredGraphBudget) (result DeferredGraphResult, err error) {
	if budget.MaxNodes <= 0 || budget.MaxCoreStatements <= 0 || len(nodes) == 0 || len(nodes) > budget.MaxNodes || len(nodes) > budget.MaxCoreStatements-2 || len(constraints) == 0 {
		return result, wrap("deferred graph", ErrGraphBudget)
	}
	steps := append([]GraphStep{}, nodes...)
	keys := append([]DeferredForeignKey{}, constraints...)
	seenNodes := map[*graphNodeID]bool{}
	tables := map[string]bool{}
	for _, node := range steps {
		if node == nil || node.graphInfo() == nil {
			return result, wrap("deferred graph", fmt.Errorf("orm: zero graph step"))
		}
		if err := node.graphValidate(); err != nil {
			return result, wrap("deferred graph", err)
		}
		if seenNodes[node.graphID()] {
			return result, wrap("deferred graph", fmt.Errorf("orm: duplicate graph node"))
		}
		seenNodes[node.graphID()] = true
		tables[node.graphInfo().sqlName()] = true
	}
	quoted := make([]string, len(keys))
	seenConstraints := map[string]bool{}
	for i, key := range keys {
		for _, name := range []string{key.schema, key.table, key.name, key.targetSchema, key.targetTable} {
			if err := identifier(name); err != nil {
				return result, wrap("deferred graph", err)
			}
		}
		quoted[i] = quote(key.schema) + "." + quote(key.name)
		if seenConstraints[quoted[i]] || !tables[quote(key.schema)+"."+quote(key.table)] || !tables[quote(key.targetSchema)+"."+quote(key.targetTable)] {
			return result, wrap("deferred graph", fmt.Errorf("orm: graph FK duplicate or outside explicit node tables"))
		}
		seenConstraints[quoted[i]] = true
	}
	control := "SET CONSTRAINTS " + strings.Join(quoted, ", ")
	err = session.Savepoint(ctx, func(owned *WriteSession) error {
		if _, err := owned.Exec(ctx, control+" DEFERRED"); err != nil {
			return err
		}
		models := map[*graphNodeID]any{}
		for _, node := range steps {
			value, err := node.graphInsert(ctx, owned)
			if err != nil {
				return err
			}
			models[node.graphID()] = value
		}
		if _, err := owned.Exec(ctx, control+" IMMEDIATE"); err != nil {
			return err
		}
		result = DeferredGraphResult{models}
		return nil
	})
	if err != nil {
		result = DeferredGraphResult{}
	}
	return result, wrap("deferred graph", err)
}
