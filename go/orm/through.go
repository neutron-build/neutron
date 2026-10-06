package orm

import (
	"context"
	"fmt"
)

// ThroughRelation follows explicit parent->link and link->target composite keys.
// The intermediate link model is part of the contract; no implicit join table,
// foreign-key ownership, target deletion or cascade is inferred.
type ThroughRelation[P, L, C any] struct {
	links   Relation[P, L]
	targets Relation[L, C]
}

func NewThroughRelation[P, L, C any](links Relation[P, L], targets Relation[L, C]) (ThroughRelation[P, L, C], error) {
	if _, err := NewRelation(links.parent, links.child, links.parts...); err != nil {
		return ThroughRelation[P, L, C]{}, err
	}
	if _, err := NewRelation(targets.parent, targets.child, targets.parts...); err != nil {
		return ThroughRelation[P, L, C]{}, err
	}
	if links.child.info != targets.parent.info {
		return ThroughRelation[P, L, C]{}, fmt.Errorf("orm: through relations require the exact intermediate link binding")
	}
	return ThroughRelation[P, L, C]{links, targets}, nil
}

type ThroughBudget struct{ MaxParents, MaxLinks, MaxRows, BatchSize int }

// LoadThrough preserves parent input positions and link multiplicity/order.
// Every link targets at most one child; ambiguous target keys refuse. Missing
// or filtered targets are omitted like an inner join. Duplicate links/parents
// expand into duplicate output slots and count against all budgets. The link
// query controls per-parent ordering; global link/target paging is refused.
// Multiple batches require caller-owned snapshot isolation for consistency.
func LoadThrough[P, L, C any](ctx context.Context, db Executor, through ThroughRelation[P, L, C], parents []P, linkQuery Query[L], targetQuery Query[C], budget ThroughBudget) ([]Association[P, C], error) {
	if _, err := NewThroughRelation(through.links, through.targets); err != nil {
		return nil, wrap("through load", err)
	}
	if budget.MaxParents <= 0 || budget.MaxLinks <= 0 || budget.MaxRows <= 0 || budget.BatchSize <= 0 {
		return nil, wrap("through load", fmt.Errorf("orm: positive through budgets required"))
	}
	links, err := LoadMany(ctx, db, through.links, parents, linkQuery, LoadBudget{budget.MaxParents, budget.MaxLinks, budget.BatchSize})
	if err != nil {
		return nil, wrap("through load", err)
	}
	flat := make([]L, 0)
	sizes := make([]int, len(links))
	for i, association := range links {
		sizes[i] = len(association.Children)
		flat = append(flat, association.Children...)
	}
	targets, err := LoadOne(ctx, db, through.targets, flat, targetQuery, LoadBudget{budget.MaxLinks, budget.MaxRows, budget.BatchSize})
	if err != nil {
		return nil, wrap("through load", err)
	}
	result := make([]Association[P, C], len(parents))
	offset, count := 0, 0
	for i, parent := range parents {
		result[i] = Association[P, C]{Parent: parent, Children: []C{}}
		for j := 0; j < sizes[i]; j++ {
			children := targets[offset].Children
			offset++
			if count+len(children) > budget.MaxRows {
				return nil, wrap("through load", ErrLoadBudget)
			}
			count += len(children)
			result[i].Children = append(result[i].Children, children...)
		}
	}
	return result, nil
}
