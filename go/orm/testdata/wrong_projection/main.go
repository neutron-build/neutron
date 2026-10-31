package wrong_projection

import (
	"context"
	"github.com/neutron-build/neutron/go/orm"
)

type Record struct {
	Name string `db:"name"`
}

var column orm.Column[Record, string]

func invalid(ctx context.Context, db orm.Executor) {
	var result []int
	result, _ = orm.SelectColumn(ctx, db, column, orm.Query[Record]{})
	_ = result
}
