package nucleus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// GraphModel provides graph operations over Nucleus SQL functions.
type GraphModel struct {
	pool   querier
	client *Client
}

// Direction specifies edge traversal direction.
type Direction int

const (
	Outgoing Direction = iota
	Incoming
	Both
)

func (d Direction) String() string {
	switch d {
	case Outgoing:
		return "out"
	case Incoming:
		return "in"
	case Both:
		return "both"
	default:
		return "out"
	}
}

// Node represents a graph node.
type Node struct {
	ID         int64          `json:"id"`
	Labels     []string       `json:"labels,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
}

// Edge represents a graph edge.
type Edge struct {
	ID         int64          `json:"id"`
	Type       string         `json:"type"`
	FromID     int64          `json:"from_id"`
	ToID       int64          `json:"to_id"`
	Properties map[string]any `json:"properties,omitempty"`
}

// GraphResult holds query results. Rows are positional: each row is a
// slice of values aligned with Columns.
type GraphResult struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

// Neighbor is one entry returned by GRAPH_NEIGHBORS.
type Neighbor struct {
	NeighborID int64  `json:"neighbor_id"`
	EdgeID     int64  `json:"edge_id"`
	EdgeType   string `json:"edge_type"`
}

// AddNode creates a new graph node and returns its ID.
// labels is a list of labels to apply to the node.
func (g *GraphModel) AddNode(ctx context.Context, labels []string, props map[string]any) (int64, error) {
	if err := g.client.requireNucleus("Graph.AddNode"); err != nil {
		return 0, err
	}
	label := strings.Join(labels, ":")
	var id int64
	var err error
	if props != nil {
		propsJSON, merr := json.Marshal(props)
		if merr != nil {
			return 0, fmt.Errorf("nucleus: graph marshal props: %w", merr)
		}
		err = g.pool.QueryRow(ctx, "SELECT GRAPH_ADD_NODE($1, $2)", label, string(propsJSON)).Scan(&id)
	} else {
		err = g.pool.QueryRow(ctx, "SELECT GRAPH_ADD_NODE($1)", label).Scan(&id)
	}
	return id, wrapErr("graph add_node", err)
}

// AddEdge creates a new edge between two nodes and returns its ID.
func (g *GraphModel) AddEdge(ctx context.Context, fromID, toID int64, edgeType string, props map[string]any) (int64, error) {
	if err := g.client.requireNucleus("Graph.AddEdge"); err != nil {
		return 0, err
	}
	var id int64
	var err error
	if props != nil {
		propsJSON, merr := json.Marshal(props)
		if merr != nil {
			return 0, fmt.Errorf("nucleus: graph marshal props: %w", merr)
		}
		err = g.pool.QueryRow(ctx, "SELECT GRAPH_ADD_EDGE($1, $2, $3, $4)", fromID, toID, edgeType, string(propsJSON)).Scan(&id)
	} else {
		err = g.pool.QueryRow(ctx, "SELECT GRAPH_ADD_EDGE($1, $2, $3)", fromID, toID, edgeType).Scan(&id)
	}
	return id, wrapErr("graph add_edge", err)
}

// DeleteNode removes a node by ID.
func (g *GraphModel) DeleteNode(ctx context.Context, nodeID int64) (bool, error) {
	if err := g.client.requireNucleus("Graph.DeleteNode"); err != nil {
		return false, err
	}
	var ok bool
	err := g.pool.QueryRow(ctx, "SELECT GRAPH_DELETE_NODE($1)", nodeID).Scan(&ok)
	return ok, wrapErr("graph delete_node", err)
}

// DeleteEdge removes an edge by ID.
func (g *GraphModel) DeleteEdge(ctx context.Context, edgeID int64) (bool, error) {
	if err := g.client.requireNucleus("Graph.DeleteEdge"); err != nil {
		return false, err
	}
	var ok bool
	err := g.pool.QueryRow(ctx, "SELECT GRAPH_DELETE_EDGE($1)", edgeID).Scan(&ok)
	return ok, wrapErr("graph delete_edge", err)
}

// Query executes a Cypher query and returns the result.
// The engine's GRAPH_QUERY takes only the Cypher text; parameters are not
// supported, so a non-empty params map is rejected.
func (g *GraphModel) Query(ctx context.Context, cypher string, params map[string]any) (*GraphResult, error) {
	if err := g.client.requireNucleus("Graph.Query"); err != nil {
		return nil, err
	}
	if len(params) > 0 {
		return nil, fmt.Errorf("nucleus: graph query: parameters are not supported by GRAPH_QUERY; interpolate values into the cypher text")
	}
	var raw string
	err := g.pool.QueryRow(ctx, "SELECT GRAPH_QUERY($1)", cypher).Scan(&raw)
	if err != nil {
		return nil, wrapErr("graph query", err)
	}
	var result GraphResult
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, fmt.Errorf("nucleus: graph query unmarshal: %w", err)
	}
	return &result, nil
}

// Neighbors returns the neighbors of a given node as
// {neighbor_id, edge_id, edge_type} entries.
// edgeType filters by edge type client-side; pass empty string for all edge types.
func (g *GraphModel) Neighbors(ctx context.Context, nodeID int64, edgeType string, direction Direction) ([]Neighbor, error) {
	if err := g.client.requireNucleus("Graph.Neighbors"); err != nil {
		return nil, err
	}
	var raw string
	err := g.pool.QueryRow(ctx, "SELECT GRAPH_NEIGHBORS($1, $2)", nodeID, direction.String()).Scan(&raw)
	if err != nil {
		return nil, wrapErr("graph neighbors", err)
	}
	var neighbors []Neighbor
	if err := json.Unmarshal([]byte(raw), &neighbors); err != nil {
		return nil, fmt.Errorf("nucleus: graph neighbors unmarshal: %w", err)
	}
	// Client-side filtering by edge type if specified
	if edgeType != "" {
		var filtered []Neighbor
		for _, n := range neighbors {
			if n.EdgeType == edgeType {
				filtered = append(filtered, n)
			}
		}
		return filtered, nil
	}
	return neighbors, nil
}

// ShortestPath returns the shortest path between two nodes as a list of
// node IDs. Returns nil if no path exists (the engine returns SQL NULL).
func (g *GraphModel) ShortestPath(ctx context.Context, fromID, toID int64) ([]int64, error) {
	if err := g.client.requireNucleus("Graph.ShortestPath"); err != nil {
		return nil, err
	}
	var raw *string
	err := g.pool.QueryRow(ctx, "SELECT GRAPH_SHORTEST_PATH($1, $2)", fromID, toID).Scan(&raw)
	if err != nil {
		return nil, wrapErr("graph shortest_path", err)
	}
	if raw == nil {
		return nil, nil
	}
	var ids []int64
	if err := json.Unmarshal([]byte(*raw), &ids); err != nil {
		return nil, fmt.Errorf("nucleus: graph shortest_path unmarshal: %w", err)
	}
	return ids, nil
}

// NodeCount returns the total number of nodes in the graph.
func (g *GraphModel) NodeCount(ctx context.Context) (int64, error) {
	if err := g.client.requireNucleus("Graph.NodeCount"); err != nil {
		return 0, err
	}
	var n int64
	err := g.pool.QueryRow(ctx, "SELECT GRAPH_NODE_COUNT()").Scan(&n)
	return n, wrapErr("graph node_count", err)
}

// EdgeCount returns the total number of edges in the graph.
func (g *GraphModel) EdgeCount(ctx context.Context) (int64, error) {
	if err := g.client.requireNucleus("Graph.EdgeCount"); err != nil {
		return 0, err
	}
	var n int64
	err := g.pool.QueryRow(ctx, "SELECT GRAPH_EDGE_COUNT()").Scan(&n)
	return n, wrapErr("graph edge_count", err)
}
