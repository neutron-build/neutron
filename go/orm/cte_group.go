package orm

import (
	"fmt"
	"strings"
)

// CTE is a sealed typed definition. Different mapped models may coexist in the
// same WITH scope; only Derived values from validated constructors implement it.
type CTE interface {
	cteInfo() *modelInfo
	cteDefinition([]any) (string, []any, error)
	cteDependencies() []CTE
	cteRecursive() bool
}

func (d Derived[M]) cteInfo() *modelInfo { return d.table.info }
func (d Derived[M]) cteRecursive() bool  { return d.relation != nil }
func (d Derived[M]) cteDependencies() []CTE {
	return append(append([]CTE{}, d.dependencies...), d.source.requiredCTEs()...)
}
func (p ModelQuery[M]) requiredCTEs() []CTE {
	result := append([]CTE{}, p.ctes...)
	if p.left != nil {
		result = append(result, p.left.requiredCTEs()...)
	}
	if p.right != nil {
		result = append(result, p.right.requiredCTEs()...)
	}
	return result
}

// FromCTE creates a complete-model query over the derived binding, carrying its
// dependency definition into subsequent CTEs/set operations and SelectModels.
func FromCTE[M any](derived Derived[M], query Query[M]) (ModelQuery[M], error) {
	if derived.relation != nil {
		return ModelQuery[M]{}, fmt.Errorf("orm: recursive CTE requires SelectDerived result budget")
	}
	plan, err := NewModelQuery(derived.table, query)
	if err != nil {
		return ModelQuery[M]{}, err
	}
	plan.ctes = []CTE{derived}
	if _, _, err := compileCTEs(plan.ctes, nil); err != nil {
		return ModelQuery[M]{}, err
	}
	return plan, nil
}

// WithCTEs explicitly includes additional definitions. Alias collisions and
// dependency cycles refuse compilation; input slices are copied.
func (d Derived[M]) WithCTEs(definitions ...CTE) (Derived[M], error) {
	d.dependencies = append(append([]CTE{}, d.dependencies...), definitions...)
	if _, _, err := compileCTEs([]CTE{d}, nil); err != nil {
		return Derived[M]{}, err
	}
	return d, nil
}
func compileCTEs(roots []CTE, initial []any) (string, []any, error) {
	if len(roots) == 0 {
		return "", append([]any{}, initial...), nil
	}
	state := map[*modelInfo]uint8{}
	names := map[string]*modelInfo{}
	ordered := []CTE{}
	var visit func(CTE) error
	visit = func(definition CTE) error {
		if definition == nil || definition.cteInfo() == nil {
			return fmt.Errorf("orm: uninitialized CTE dependency")
		}
		info := definition.cteInfo()
		if state[info] == 1 {
			return fmt.Errorf("orm: CTE dependency cycle")
		}
		if previous, ok := names[info.name]; ok && previous != info {
			return fmt.Errorf("orm: duplicate CTE alias")
		}
		if state[info] == 2 {
			return nil
		}
		if len(names) >= 64 {
			return fmt.Errorf("orm: CTE definition budget exceeded")
		}
		names[info.name] = info
		state[info] = 1
		for _, dependency := range definition.cteDependencies() {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[info] = 2
		ordered = append(ordered, definition)
		return nil
	}
	for _, root := range roots {
		if err := visit(root); err != nil {
			return "", nil, err
		}
	}
	args := append([]any{}, initial...)
	parts := make([]string, 0, len(ordered))
	recursive := false
	for _, definition := range ordered {
		part, next, err := definition.cteDefinition(args)
		if err != nil {
			return "", nil, err
		}
		args = next
		parts = append(parts, part)
		recursive = recursive || definition.cteRecursive()
	}
	prefix := "WITH "
	if recursive {
		prefix = "WITH RECURSIVE "
	}
	return prefix + strings.Join(parts, ", ") + " ", args, nil
}
