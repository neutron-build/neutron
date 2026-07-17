package nucleus

import (
	"context"
	"fmt"
)

// DatalogModel provides Datalog reasoning operations over Nucleus SQL functions.
type DatalogModel struct {
	pool   querier
	client *Client
}

// Assert adds a fact to the Datalog knowledge base. Returns the engine's status message.
func (d *DatalogModel) Assert(ctx context.Context, fact string) (string, error) {
	if err := d.client.requireNucleus("Datalog.Assert"); err != nil {
		return "", err
	}
	var msg string
	err := d.pool.QueryRow(ctx, "SELECT DATALOG_ASSERT($1)", fact).Scan(&msg)
	return msg, wrapErr("datalog assert", err)
}

// Retract removes a fact from the Datalog knowledge base. Returns the engine's status message.
func (d *DatalogModel) Retract(ctx context.Context, fact string) (string, error) {
	if err := d.client.requireNucleus("Datalog.Retract"); err != nil {
		return "", err
	}
	var msg string
	err := d.pool.QueryRow(ctx, "SELECT DATALOG_RETRACT($1)", fact).Scan(&msg)
	return msg, wrapErr("datalog retract", err)
}

// Rule defines a Datalog rule. The head and body are joined into the engine's
// single-string "head :- body" form. Returns the engine's status message.
func (d *DatalogModel) Rule(ctx context.Context, head, body string) (string, error) {
	if err := d.client.requireNucleus("Datalog.Rule"); err != nil {
		return "", err
	}
	var msg string
	rule := fmt.Sprintf("%s :- %s", head, body)
	err := d.pool.QueryRow(ctx, "SELECT DATALOG_RULE($1)", rule).Scan(&msg)
	return msg, wrapErr("datalog rule", err)
}

// Query evaluates a Datalog query pattern and returns results as a JSON array of arrays.
func (d *DatalogModel) Query(ctx context.Context, pattern string) (string, error) {
	if err := d.client.requireNucleus("Datalog.Query"); err != nil {
		return "", err
	}
	var raw string
	err := d.pool.QueryRow(ctx, "SELECT DATALOG_QUERY($1)", pattern).Scan(&raw)
	return raw, wrapErr("datalog query", err)
}

// Clear removes all facts and rules for a predicate. Returns the engine's status message.
func (d *DatalogModel) Clear(ctx context.Context, predicate string) (string, error) {
	if err := d.client.requireNucleus("Datalog.Clear"); err != nil {
		return "", err
	}
	var msg string
	err := d.pool.QueryRow(ctx, "SELECT DATALOG_CLEAR($1)", predicate).Scan(&msg)
	return msg, wrapErr("datalog clear", err)
}

// ImportGraph imports graph edges as facts: predicate(from_id, edge_type, to_id).
// Returns the engine's status message ("IMPORTED N edges into <predicate>").
func (d *DatalogModel) ImportGraph(ctx context.Context, predicate string) (string, error) {
	if err := d.client.requireNucleus("Datalog.ImportGraph"); err != nil {
		return "", err
	}
	var msg string
	err := d.pool.QueryRow(ctx, "SELECT DATALOG_IMPORT_GRAPH($1)", predicate).Scan(&msg)
	return msg, wrapErr("datalog import_graph", err)
}
