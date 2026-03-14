//! Graph model — GRAPH_ADD_NODE, GRAPH_ADD_EDGE, GRAPH_DELETE_NODE, GRAPH_DELETE_EDGE,
//! GRAPH_QUERY, GRAPH_NEIGHBORS, GRAPH_SHORTEST_PATH, GRAPH_NODE_COUNT, GRAPH_EDGE_COUNT.

use serde::{Deserialize, Serialize};
use serde_json;

use crate::error::NucleusError;
use crate::pool::NucleusPool;

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

/// A graph node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: i64,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub properties: serde_json::Value,
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
        Ok(row.get::<_, i64>(0))
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
        Ok(row.get::<_, i64>(0))
    }

    /// Delete a node by ID.
    pub async fn delete_node(&self, node_id: i64) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_DELETE_NODE($1)", &[&node_id])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
    }

    /// Delete an edge by ID.
    pub async fn delete_edge(&self, edge_id: i64) -> Result<bool, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_DELETE_EDGE($1)", &[&edge_id])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, bool>(0))
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

    /// Return neighboring nodes of a given node.
    pub async fn neighbors(
        &self,
        node_id: i64,
        direction: Direction,
    ) -> Result<Vec<Node>, NucleusError> {
        let dir = direction.as_str().to_string();
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_NEIGHBORS($1, $2)", &[&node_id, &dir])
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        let nodes: Vec<Node> =
            serde_json::from_str(&raw).map_err(|e| NucleusError::Serde(e.to_string()))?;
        Ok(nodes)
    }

    /// Find the shortest path between two nodes. Returns a list of node IDs.
    pub async fn shortest_path(
        &self,
        from_id: i64,
        to_id: i64,
    ) -> Result<Vec<i64>, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_SHORTEST_PATH($1, $2)", &[&from_id, &to_id])
            .await
            .map_err(NucleusError::Query)?;
        let raw: String = row.get(0);
        let ids: Vec<i64> =
            serde_json::from_str(&raw).map_err(|e| NucleusError::Serde(e.to_string()))?;
        Ok(ids)
    }

    /// Return the total number of nodes.
    pub async fn node_count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_NODE_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
    }

    /// Return the total number of edges.
    pub async fn edge_count(&self) -> Result<i64, NucleusError> {
        let conn = self.pool.get().await?;
        let row = conn
            .client()
            .query_one("SELECT GRAPH_EDGE_COUNT()", &[])
            .await
            .map_err(NucleusError::Query)?;
        Ok(row.get::<_, i64>(0))
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

    // --- Node serde ---

    #[test]
    fn node_serialize_deserialize() {
        let node = Node {
            id: 42,
            labels: vec!["Person".to_string(), "Employee".to_string()],
            properties: serde_json::json!({"name": "Alice", "age": 30}),
        };
        let json = serde_json::to_string(&node).unwrap();
        let deserialized: Node = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, 42);
        assert_eq!(deserialized.labels, vec!["Person", "Employee"]);
        assert_eq!(deserialized.properties["name"], "Alice");
    }

    #[test]
    fn node_default_labels_and_properties() {
        // labels and properties both have #[serde(default)]
        let json = r#"{"id": 1}"#;
        let node: Node = serde_json::from_str(json).unwrap();
        assert_eq!(node.id, 1);
        assert!(node.labels.is_empty());
        assert!(node.properties.is_null());
    }

    #[test]
    fn node_clone() {
        let node = Node {
            id: 1,
            labels: vec!["Test".into()],
            properties: serde_json::json!({}),
        };
        let cloned = node.clone();
        assert_eq!(cloned.id, 1);
        assert_eq!(cloned.labels, vec!["Test"]);
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
    fn node_vec_deserialize() {
        let json = r#"[
            {"id": 1, "labels": ["Person"], "properties": {"name": "Alice"}},
            {"id": 2, "labels": ["Person"], "properties": {"name": "Bob"}}
        ]"#;
        let nodes: Vec<Node> = serde_json::from_str(json).unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id, 1);
        assert_eq!(nodes[1].properties["name"], "Bob");
    }
}
