package nucleus

import (
	"context"
)

// DatalogModel provides Datalog reasoning operations over Nucleus SQL functions.
type DatalogModel struct {
	pool   querier
	client *Client
}

// Assert adds a fact to the Datalog knowledge base.
func (d *DatalogModel) Assert(ctx context.Context, fact string) (bool, error) {
	if err := d.client.requireNucleus("Datalog.Assert"); err != nil {
		return false, err
	}
	var ok bool
	err := d.pool.QueryRow(ctx, "SELECT DATALOG_ASSERT($1)", fact).Scan(&ok)
	return ok, wrapErr("datalog assert", err)
}

// Retract removes a fact from the Datalog knowledge base.
func (d *DatalogModel) Retract(ctx context.Context, fact string) (bool, error) {
	if err := d.client.requireNucleus("Datalog.Retract"); err != nil {
		return false, err
	}
	var ok bool
	err := d.pool.QueryRow(ctx, "SELECT DATALOG_RETRACT($1)", fact).Scan(&ok)
	return ok, wrapErr("datalog retract", err)
}

// Rule defines a Datalog rule with a head and body.
func (d *DatalogModel) Rule(ctx context.Context, head, body string) (bool, error) {
	if err := d.client.requireNucleus("Datalog.Rule"); err != nil {
		return false, err
	}
	var ok bool
	err := d.pool.QueryRow(ctx, "SELECT DATALOG_RULE($1, $2)", head, body).Scan(&ok)
	return ok, wrapErr("datalog rule", err)
}

// Query evaluates a Datalog query pattern and returns results as CSV text.
func (d *DatalogModel) Query(ctx context.Context, pattern string) (string, error) {
	if err := d.client.requireNucleus("Datalog.Query"); err != nil {
		return "", err
	}
	var raw string
	err := d.pool.QueryRow(ctx, "SELECT DATALOG_QUERY($1)", pattern).Scan(&raw)
	return raw, wrapErr("datalog query", err)
}

// Clear removes all facts and rules from the Datalog knowledge base.
func (d *DatalogModel) Clear(ctx context.Context) (bool, error) {
	if err := d.client.requireNucleus("Datalog.Clear"); err != nil {
		return false, err
	}
	var ok bool
	err := d.pool.QueryRow(ctx, "SELECT DATALOG_CLEAR()").Scan(&ok)
	return ok, wrapErr("datalog clear", err)
}

// ImportGraph imports the current graph model data into the Datalog knowledge base.
// Returns the number of facts imported.
func (d *DatalogModel) ImportGraph(ctx context.Context) (int64, error) {
	if err := d.client.requireNucleus("Datalog.ImportGraph"); err != nil {
		return 0, err
	}
	var n int64
	err := d.pool.QueryRow(ctx, "SELECT DATALOG_IMPORT_GRAPH()").Scan(&n)
	return n, wrapErr("datalog import_graph", err)
}
