package wrong_join_null_result

import (
	"context"
	"github.com/neutron-build/neutron/go/orm"
)

type Parent struct{}
type Child struct{}

func invalid(ctx context.Context, db orm.Executor, field orm.JoinedField[Parent, Child, orm.Nullable[int64]], query orm.JoinQuery[Parent, Child]) {
	var result []int64
	result, _ = orm.SelectJoinedColumn(ctx, db, field, query)
	_ = result
}
