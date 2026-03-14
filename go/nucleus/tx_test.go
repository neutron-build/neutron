package nucleus

import (
	"testing"
)

func TestTxQuerierImplementsQuerier(t *testing.T) {
	// Verify txQuerier satisfies the querier interface at compile time
	var _ querier = &txQuerier{}
}

func TestTxModelAccessorsReturnNonNil(t *testing.T) {
	// Create a Tx with a nil pgx.Tx (just testing the accessor pattern)
	client := &Client{features: Features{IsNucleus: true}}
	tx := &Tx{client: client}

	// All model accessors should return non-nil objects
	if tx.SQL() == nil {
		t.Error("SQL() returned nil")
	}
	if tx.KV() == nil {
		t.Error("KV() returned nil")
	}
	if tx.Vector() == nil {
		t.Error("Vector() returned nil")
	}
	if tx.TimeSeries() == nil {
		t.Error("TimeSeries() returned nil")
	}
	if tx.Document() == nil {
		t.Error("Document() returned nil")
	}
	if tx.Graph() == nil {
		t.Error("Graph() returned nil")
	}
	if tx.FTS() == nil {
		t.Error("FTS() returned nil")
	}
	if tx.Geo() == nil {
		t.Error("Geo() returned nil")
	}
	if tx.Blob() == nil {
		t.Error("Blob() returned nil")
	}
	if tx.Streams() == nil {
		t.Error("Streams() returned nil")
	}
	if tx.Columnar() == nil {
		t.Error("Columnar() returned nil")
	}
	if tx.Datalog() == nil {
		t.Error("Datalog() returned nil")
	}
	if tx.CDC() == nil {
		t.Error("CDC() returned nil")
	}
	if tx.PubSub() == nil {
		t.Error("PubSub() returned nil")
	}
}

func TestTxSQLUsesTransactionQuerier(t *testing.T) {
	client := &Client{features: Features{IsNucleus: true}}
	tx := &Tx{client: client}

	sqlModel := tx.SQL()
	if sqlModel == nil {
		t.Fatal("SQL() returned nil")
	}
	// The pool should be a txQuerier, not the original pool
	_, ok := sqlModel.pool.(*txQuerier)
	if !ok {
		t.Error("tx.SQL().pool should be a *txQuerier")
	}
}

func TestTxKVUsesTransactionQuerier(t *testing.T) {
	client := &Client{features: Features{IsNucleus: true}}
	tx := &Tx{client: client}

	kvModel := tx.KV()
	if kvModel == nil {
		t.Fatal("KV() returned nil")
	}
	_, ok := kvModel.pool.(*txQuerier)
	if !ok {
		t.Error("tx.KV().pool should be a *txQuerier")
	}
}

func TestTxModelsShareClient(t *testing.T) {
	client := &Client{features: Features{IsNucleus: true}}
	tx := &Tx{client: client}

	// All model accessors should share the same client reference
	if tx.KV().client != client {
		t.Error("KV client mismatch")
	}
	if tx.Vector().client != client {
		t.Error("Vector client mismatch")
	}
	if tx.CDC().client != client {
		t.Error("CDC client mismatch")
	}
	if tx.Datalog().client != client {
		t.Error("Datalog client mismatch")
	}
	if tx.PubSub().client != client {
		t.Error("PubSub client mismatch")
	}
	if tx.Streams().client != client {
		t.Error("Streams client mismatch")
	}
}

func TestTxStruct(t *testing.T) {
	// Verify the Tx struct has the expected fields
	tx := &Tx{}
	_ = tx.tx
	_ = tx.client
}
