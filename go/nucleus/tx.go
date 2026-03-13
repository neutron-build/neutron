package nucleus

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Tx wraps a pgx transaction, providing access to all data models within
// a single transaction boundary.
type Tx struct {
	tx     pgx.Tx
	client *Client
}

// Begin starts a new transaction.
func (c *Client) Begin(ctx context.Context) (*Tx, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("nucleus: begin tx: %w", err)
	}
	return &Tx{tx: tx, client: c}, nil
}

// SQL returns a transactional SQLModel.
func (t *Tx) SQL() *SQLModel {
	return &SQLModel{pool: &txQuerier{tx: t.tx}}
}

// KV returns a transactional KVModel.
func (t *Tx) KV() *KVModel {
	return &KVModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// Vector returns a transactional VectorModel.
func (t *Tx) Vector() *VectorModel {
	return &VectorModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// TimeSeries returns a transactional TimeSeriesModel.
func (t *Tx) TimeSeries() *TimeSeriesModel {
	return &TimeSeriesModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// Document returns a transactional DocumentModel.
func (t *Tx) Document() *DocumentModel {
	return &DocumentModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// Graph returns a transactional GraphModel.
func (t *Tx) Graph() *GraphModel {
	return &GraphModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// FTS returns a transactional FTSModel.
func (t *Tx) FTS() *FTSModel {
	return &FTSModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// Geo returns a transactional GeoModel.
func (t *Tx) Geo() *GeoModel {
	return &GeoModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// Blob returns a transactional BlobModel.
func (t *Tx) Blob() *BlobModel {
	return &BlobModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// Streams returns a transactional StreamModel.
func (t *Tx) Streams() *StreamModel {
	return &StreamModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// Columnar returns a transactional ColumnarModel.
func (t *Tx) Columnar() *ColumnarModel {
	return &ColumnarModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// Datalog returns a transactional DatalogModel.
func (t *Tx) Datalog() *DatalogModel {
	return &DatalogModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// CDC returns a transactional CDCModel.
func (t *Tx) CDC() *CDCModel {
	return &CDCModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// PubSub returns a transactional PubSubModel.
func (t *Tx) PubSub() *PubSubModel {
	return &PubSubModel{pool: &txQuerier{tx: t.tx}, client: t.client}
}

// Commit commits the transaction.
func (t *Tx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

// Rollback aborts the transaction.
func (t *Tx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

// txQuerier adapts pgx.Tx to the interface expected by SQLModel.
type txQuerier struct {
	tx pgx.Tx
}

func (q *txQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return q.tx.Query(ctx, sql, args...)
}

func (q *txQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return q.tx.QueryRow(ctx, sql, args...)
}

func (q *txQuerier) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return q.tx.Exec(ctx, sql, args...)
}
