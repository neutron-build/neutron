//! Graph model — GRAPH_ADD_NODE, GRAPH_ADD_EDGE, GRAPH_DELETE_NODE, GRAPH_DELETE_EDGE,
//! GRAPH_QUERY, GRAPH_NEIGHBORS, GRAPH_SHORTEST_PATH, GRAPH_NODE_COUNT, GRAPH_EDGE_COUNT.

use serde::{Deserialize, Serialize};
use serde_json;

use crate::error::NucleusError;
use crate::pool::NucleusPool;
use crate::row_ext::RowExt;

/// Edge traversal direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Out,
    In,
    Both,
}

impl Direction {
    fn as_str(&self) -> &'static str {
        match self {
            Direction::Out => "out",
            Direction::In => "in",
            Direction::Both => "both",
        }
    }
}

/// A neighbor entry returned by GRAPH_NEIGHBORS:
/// `{"neighbor_id":N,"edge_id":N,"edge_type":"..."}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Neighbor {
    pub neighbor_id: i64,
    pub edge_id: i64,
    pub edge_type: String,
}

/// A graph query result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphResult {
    pub columns: Vec<String>,
    pub rows: Vec<serde_json::Value>,
}

/// Handle for graph operations.
pub struct GraphModel {
    pool: NucleusPool,
}

impl GraphModel {
    pub(crate) fn new(pool: NucleusPool) -> Self {
        Self { pool }
    }

    /// Create a new graph node. Returns the node ID.
    pub async fn add_node(
        &self,
        label: &str,
        properties: Option<&serde_json::Value>,
    ) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = if let Some(props) = properties {
            let props_str =
                serde_json::to_string(props).map_err(|e| NucleusError::Serde(e.to_string()))?;
            conn.client()
                .query_one("SELECT GRAPH_ADD_NODE($1, $2)", &[&label, &props_str])
                .await
                .map_err(NucleusError::Query)?
        } else {
            conn.client()
                .query_one("SELECT GRAPH_ADD_NODE($1)", &[&label])
                .await
                .map_err(NucleusError::Query)?
        };
        row.get_ck::<i64>(0)
    }

    /// Create a new edge between two nodes. Returns the edge ID.
    pub async fn add_edge(
        &self,
        from_id: i64,
        to_id: i64,
        edge_type: &str,
        properties: Option<&serde_json::Value>,
    ) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = if let Some(props) = properties {
            let props_str =
                serde_json::to_string(props).map_err(|e| NucleusError::Serde(e.to_string()))?;
            conn.client()
                .query_one(
                    "SELECT GRAPH_ADD_EDGE($1, $2, $3, $4)",
                    &[&from_id, &to_id, &edge_type, &props_str],
                )
                .await
                .map_err(NucleusError::Query)?
        } else {
            conn.client()
                .query_one(
                    "SELECT GRAPH_ADD_EDGE($1, $2, $3)",
                    &[&from_id, &to_id, &edge_type],
                )
                .await
                .map_err(NucleusError::Query)?
        };
        row.get_ck::<i64>(0)
    }

    /// Delete a node by ID.
    pub async fn delete_node(&self, node_id: i64) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_DELETE_NODE($1)", &[&node_id])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<bool>(0)
    }

    /// Delete an edge by ID.
    pub async fn delete_edge(&self, edge_id: i64) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_DELETE_EDGE($1)", &[&edge_id])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<bool>(0)
    }

    /// Execute a Cypher-style graph query.
    pub async fn query(&self, cypher: &str) -> Result<GraphResult, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_QUERY($1)", &[&cypher])
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        let result: GraphResult =
            serde_json::from_str(&raw).map_err(|e| NucleusError::Serde(e.to_string()))?;
        Ok(result)
    }

    /// Return the neighbors of a given node.
    pub async fn neighbors(
        &self,
        node_id: i64,
        direction: Direction,
    ) -> Result<Vec<Neighbor>, NucleusError> {
        let dir = direction.as_str().to_string();
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_NEIGHBORS($1, $2)", &[&node_id, &dir])
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        let neighbors: Vec<Neighbor> =
            serde_json::from_str(&raw).map_err(|e| NucleusError::Serde(e.to_string()))?;
        Ok(neighbors)
    }

    /// Find the shortest path between two nodes. Returns a list of node IDs,
    /// or an empty list when no path exists (the engine returns SQL NULL).
    pub async fn shortest_path(&self, from_id: i64, to_id: i64) -> Result<Vec<i64>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_SHORTEST_PATH($1, $2)", &[&from_id, &to_id])
            .await
            .map_err(NucleusError::Query)?;
        let raw: Option<String> = row.get(0);
        match raw {
            Some(s) => {
                let ids: Vec<i64> =
                    serde_json::from_str(&s).map_err(|e| NucleusError::Serde(e.to_string()))?;
                Ok(ids)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Return the total number of nodes.
    pub async fn node_count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_NODE_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<i64>(0)
    }

    /// Return the total number of edges.
    pub async fn edge_count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_EDGE_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        row.get_ck::<i64>(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Direction enum ---

    #[test]
    fn direction_out_as_str() {
        assert_eq!(Direction::Out.as_str(), "out");
    }

    #[test]
    fn direction_in_as_str() {
        assert_eq!(Direction::In.as_str(), "in");
    }

    #[test]
    fn direction_both_as_str() {
        assert_eq!(Direction::Both.as_str(), "both");
    }

    #[test]
    fn direction_equality() {
        assert_eq!(Direction::Out, Direction::Out);
        assert_ne!(Direction::In, Direction::Out);
        assert_ne!(Direction::Both, Direction::In);
    }

    #[test]
    fn direction_clone() {
        let d = Direction::Both;
        let d2 = d;
        assert_eq!(d, d2);
    }

    #[test]
    fn direction_debug() {
        assert_eq!(format!("{:?}", Direction::Out), "Out");
        assert_eq!(format!("{:?}", Direction::In), "In");
        assert_eq!(format!("{:?}", Direction::Both), "Both");
    }

    // --- Neighbor serde ---

    #[test]
    fn neighbor_serialize_deserialize() {
        let neighbor = Neighbor {
            neighbor_id: 42,
            edge_id: 7,
            edge_type: "KNOWS".to_string(),
        };
        let json = serde_json::to_string(&neighbor).unwrap();
        let deserialized: Neighbor = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.neighbor_id, 42);
        assert_eq!(deserialized.edge_id, 7);
        assert_eq!(deserialized.edge_type, "KNOWS");
    }

    #[test]
    fn neighbor_clone() {
        let neighbor = Neighbor {
            neighbor_id: 1,
            edge_id: 2,
            edge_type: "LINKS".to_string(),
        };
        let cloned = neighbor.clone();
        assert_eq!(cloned.neighbor_id, 1);
        assert_eq!(cloned.edge_id, 2);
        assert_eq!(cloned.edge_type, "LINKS");
    }

    // --- GraphResult serde ---

    #[test]
    fn graph_result_serialize_deserialize() {
        let result = GraphResult {
            columns: vec!["name".into(), "age".into()],
            rows: vec![
                serde_json::json!(["Alice", 30]),
                serde_json::json!(["Bob", 25]),
            ],
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: GraphResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.columns, vec!["name", "age"]);
        assert_eq!(deserialized.rows.len(), 2);
    }

    #[test]
    fn graph_result_empty() {
        let result = GraphResult {
            columns: vec![],
            rows: vec![],
        };
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: GraphResult = serde_json::from_str(&json).unwrap();
        assert!(deserialized.columns.is_empty());
        assert!(deserialized.rows.is_empty());
    }

    #[test]
    fn neighbor_vec_deserialize() {
        // Real engine GRAPH_NEIGHBORS shape.
        let json = r#"[
            {"neighbor_id":1,"edge_id":10,"edge_type":"KNOWS"},
            {"neighbor_id":2,"edge_id":11,"edge_type":"WORKS_WITH"}
        ]"#;
        let neighbors: Vec<Neighbor> = serde_json::from_str(json).unwrap();
        assert_eq!(neighbors.len(), 2);
        assert_eq!(neighbors[0].neighbor_id, 1);
        assert_eq!(neighbors[1].edge_type, "WORKS_WITH");
    }
}
