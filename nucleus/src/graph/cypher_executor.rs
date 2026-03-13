//! Cypher query execution engine (Phase 8b).
//!
//! Takes parsed Cypher AST and executes it against GraphStore.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::cypher::*;
use super::{Direction, EdgeId, GraphStore, Node, NodeId, Properties, PropValue};

#[derive(Debug, Clone)]
enum Binding {
    Node(NodeId),
    Edge(EdgeId),
}

/// The result of executing a Cypher query.
#[derive(Debug, Clone, PartialEq)]
pub struct CypherResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<PropValue>>,
}

/// Execute a parsed Cypher statement against a graph store.
pub fn execute_cypher(
    store: &mut GraphStore,
    stmt: &CypherStatement,
) -> Result<CypherResult, CypherError> {
    match stmt {
        CypherStatement::Match {
            pattern,
            where_clause,
            return_clause,
            optional,
            with_clause,
            with_where,
        } => execute_match(
            store,
            pattern,
            where_clause.as_ref(),
            return_clause,
            *optional,
            with_clause.as_ref(),
            with_where.as_ref(),
        ),
        CypherStatement::Create { items } => execute_create(store, items),
        CypherStatement::Delete { variables } => execute_delete(store, variables),
    }
}

fn execute_match(
    store: &GraphStore,
    pattern: &Pattern,
    where_clause: Option<&WhereClause>,
    return_clause: &ReturnClause,
    optional: bool,
    with_clause: Option<&WithClause>,
    with_where: Option<&WhereClause>,
) -> Result<CypherResult, CypherError> {
    let binding_sets = find_bindings(store, pattern)?;
    let filtered: Vec<HashMap<String, Binding>> = if let Some(wc) = where_clause {
        binding_sets
            .into_iter()
            .filter(|bindings| evaluate_where(store, bindings, wc))
            .collect()
    } else {
        binding_sets
    };

    // OPTIONAL MATCH: if no bindings found, return a single row of NULLs
    if optional && filtered.is_empty() {
        let columns: Vec<String> = return_clause
            .items
            .iter()
            .map(|item| match item {
                ReturnItem::Variable(v) => v.clone(),
                ReturnItem::Property(v, p) => format!("{v}.{p}"),
                ReturnItem::Count => "COUNT(*)".to_string(),
                ReturnItem::All => "*".to_string(),
            })
            .collect();
        let null_row: Vec<PropValue> = columns.iter().map(|_| PropValue::Null).collect();
        return Ok(CypherResult {
            columns,
            rows: vec![null_row],
        });
    }

    // Apply WITH clause: project intermediate bindings, then optionally filter
    let final_bindings = if let Some(wc) = with_clause {
        let projected = apply_with_clause(store, &filtered, wc);
        if let Some(ww) = with_where {
            projected
                .into_iter()
                .filter(|bindings| evaluate_where(store, bindings, ww))
                .collect()
        } else {
            projected
        }
    } else {
        filtered
    };

    project_return(store, &final_bindings, return_clause)
}

/// Apply a WITH clause: project each binding set through the WITH items.
///
/// WITH items can rename variables (via AS alias) or project properties.
/// The result is a new set of bindings with only the projected variables.
fn apply_with_clause(
    _store: &GraphStore,
    binding_sets: &[HashMap<String, Binding>],
    with_clause: &WithClause,
) -> Vec<HashMap<String, Binding>> {
    let mut result = Vec::new();
    for bindings in binding_sets {
        let mut new_bindings = HashMap::new();
        for item in &with_clause.items {
            let name = item
                .alias
                .clone()
                .or_else(|| match &item.expr {
                    ReturnItem::Variable(v) => Some(v.clone()),
                    ReturnItem::Property(v, p) => Some(format!("{v}.{p}")),
                    ReturnItem::Count => Some("COUNT(*)".to_string()),
                    ReturnItem::All => None,
                })
                .unwrap_or_default();

            match &item.expr {
                ReturnItem::Variable(v) => {
                    if let Some(b) = bindings.get(v) {
                        new_bindings.insert(name, b.clone());
                    }
                }
                ReturnItem::Property(v, _p) => {
                    // For property projections, keep the underlying node/edge binding
                    // under the alias so that the RETURN clause can access properties.
                    if let Some(b) = bindings.get(v) {
                        new_bindings.insert(name.clone(), b.clone());
                        // Also keep the original variable for property resolution
                        new_bindings.insert(v.clone(), b.clone());
                    }
                }
                ReturnItem::All => {
                    // Pass through all bindings
                    new_bindings.extend(bindings.clone());
                }
                ReturnItem::Count => {
                    // COUNT(*) in WITH doesn't map to a binding; skip
                }
            }
        }
        result.push(new_bindings);
    }
    result
}

fn find_bindings(
    store: &GraphStore,
    pattern: &Pattern,
) -> Result<Vec<HashMap<String, Binding>>, CypherError> {
    if pattern.nodes.is_empty() {
        return Ok(Vec::new());
    }
    let first_node = &pattern.nodes[0];
    let candidate_ids = candidate_node_ids(store, first_node);
    let mut binding_sets: Vec<HashMap<String, Binding>> = Vec::new();
    for nid in &candidate_ids {
        let node = match store.get_node(*nid) {
            Some(n) => n,
            None => continue,
        };
        if !node_matches_properties(node, &first_node.properties) {
            continue;
        }
        let mut bindings = HashMap::new();
        if let Some(ref var) = first_node.variable {
            bindings.insert(var.clone(), Binding::Node(node.id));
        }
        binding_sets.push(bindings);
    }
    for edge_pat in &pattern.edges {
        let target_node_pat = &pattern.nodes[edge_pat.to_idx];
        let mut new_binding_sets = Vec::new();

        let is_variable_length = edge_pat.min_hops.is_some() || edge_pat.max_hops.is_some();

        for bindings in &binding_sets {
            let source_node_pat = &pattern.nodes[edge_pat.from_idx];
            let source_id = match resolve_node_id(bindings, source_node_pat) {
                Some(id) => id,
                None => continue,
            };

            if is_variable_length {
                // Variable-length path expansion: DFS from source within hop bounds
                let min_hops = edge_pat.min_hops.unwrap_or(1);
                let max_hops = edge_pat.max_hops.unwrap_or(10);
                let terminal_nodes = variable_length_expand(
                    store, source_id, edge_pat.direction,
                    edge_pat.edge_type.as_deref(), min_hops, max_hops,
                );
                for terminal_id in terminal_nodes {
                    let target_node = match store.get_node(terminal_id) {
                        Some(n) => n,
                        None => continue,
                    };
                    if !node_matches_labels(target_node, &target_node_pat.labels) {
                        continue;
                    }
                    if !node_matches_properties(target_node, &target_node_pat.properties) {
                        continue;
                    }
                    let mut nb = bindings.clone();
                    if let Some(ref var) = target_node_pat.variable {
                        nb.insert(var.clone(), Binding::Node(target_node.id));
                    }
                    new_binding_sets.push(nb);
                }
            } else {
                // Single-hop traversal (original logic)
                let neighbors = store.neighbors(
                    source_id, edge_pat.direction, edge_pat.edge_type.as_deref(),
                );
                for (neighbor_id, edge) in &neighbors {
                    let target_node = match store.get_node(*neighbor_id) {
                        Some(n) => n,
                        None => continue,
                    };
                    if !node_matches_labels(target_node, &target_node_pat.labels) {
                        continue;
                    }
                    if !node_matches_properties(target_node, &target_node_pat.properties) {
                        continue;
                    }
                    let mut nb = bindings.clone();
                    if let Some(ref var) = edge_pat.variable {
                        nb.insert(var.clone(), Binding::Edge(edge.id));
                    }
                    if let Some(ref var) = target_node_pat.variable {
                        nb.insert(var.clone(), Binding::Node(target_node.id));
                    }
                    new_binding_sets.push(nb);
                }
            }
        }
        binding_sets = new_binding_sets;
    }
    Ok(binding_sets)
}

/// Expand variable-length paths from a source node via DFS.
/// Returns deduplicated terminal node IDs reachable within `min_hops..=max_hops`.
fn variable_length_expand(
    store: &GraphStore,
    source_id: NodeId,
    direction: Direction,
    edge_type: Option<&str>,
    min_hops: usize,
    max_hops: usize,
) -> Vec<NodeId> {
    let mut results = Vec::new();
    let mut seen_terminals = HashSet::new();
    // Stack: (current_node, depth, visited_set)
    let mut stack: Vec<(NodeId, usize, HashSet<NodeId>)> = Vec::new();
    let mut initial_visited = HashSet::new();
    initial_visited.insert(source_id);
    stack.push((source_id, 0, initial_visited));

    while let Some((current, depth, visited)) = stack.pop() {
        if depth >= min_hops && depth <= max_hops && current != source_id
            && seen_terminals.insert(current) {
                results.push(current);
            }
        if depth >= max_hops {
            continue;
        }
        for (neighbor, _) in store.neighbors(current, direction, edge_type) {
            if !visited.contains(&neighbor) {
                let mut new_visited = visited.clone();
                new_visited.insert(neighbor);
                stack.push((neighbor, depth + 1, new_visited));
            }
        }
    }
    results
}

fn candidate_node_ids(store: &GraphStore, np: &NodePattern) -> Vec<NodeId> {
    if let Some(label) = np.labels.first() {
        store.nodes_by_label(label).iter().map(|n| n.id).collect()
    } else {
        store.all_nodes().iter().map(|n| n.id).collect()
    }
}

fn node_matches_labels(node: &Node, required: &[String]) -> bool {
    required.iter().all(|label| node.labels.contains(label))
}

fn node_matches_properties(node: &Node, required: &BTreeMap<String, PropValue>) -> bool {
    for (key, value) in required {
        match node.properties.get(key) {
            Some(v) if v == value => {}
            _ => return false,
        }
    }
    true
}

fn resolve_node_id(bindings: &HashMap<String, Binding>, np: &NodePattern) -> Option<NodeId> {
    if let Some(ref var) = np.variable
        && let Some(Binding::Node(id)) = bindings.get(var) {
            return Some(*id);
        }
    None
}

fn evaluate_where(
    store: &GraphStore,
    bindings: &HashMap<String, Binding>,
    wc: &WhereClause,
) -> bool {
    wc.conditions.iter().all(|c| evaluate_condition(store, bindings, c))
}

fn evaluate_condition(
    store: &GraphStore,
    bindings: &HashMap<String, Binding>,
    condition: &Condition,
) -> bool {
    match condition {
        Condition::PropertyEquals { variable, property, value } => {
            match bindings.get(variable) {
                Some(Binding::Node(id)) => store.get_node(*id)
                    .is_some_and(|n| n.properties.get(property) == Some(value)),
                Some(Binding::Edge(id)) => store.get_edge(*id)
                    .is_some_and(|e| e.properties.get(property) == Some(value)),
                None => false,
            }
        }
        Condition::And(left, right) => {
            evaluate_condition(store, bindings, left)
                && evaluate_condition(store, bindings, right)
        }
    }
}


fn project_return(
    store: &GraphStore,
    binding_sets: &[HashMap<String, Binding>],
    return_clause: &ReturnClause,
) -> Result<CypherResult, CypherError> {
    if return_clause.items.len() == 1 && return_clause.items[0] == ReturnItem::Count {
        return Ok(CypherResult {
            columns: vec!["COUNT(*)".to_string()],
            rows: vec![vec![PropValue::Int(binding_sets.len() as i64)]],
        });
    }
    let columns: Vec<String> = return_clause
        .items
        .iter()
        .map(|item| match item {
            ReturnItem::Variable(v) => v.clone(),
            ReturnItem::Property(v, p) => format!("{v}.{p}"),
            ReturnItem::Count => "COUNT(*)".to_string(),
            ReturnItem::All => "*".to_string(),
        })
        .collect();
    let mut rows = Vec::new();
    for bindings in binding_sets {
        let mut row = Vec::new();
        for item in &return_clause.items {
            row.push(project_item(store, bindings, item));
        }
        rows.push(row);
    }
    Ok(CypherResult { columns, rows })
}

fn project_item(
    store: &GraphStore,
    bindings: &HashMap<String, Binding>,
    item: &ReturnItem,
) -> PropValue {
    match item {
        ReturnItem::Variable(var) => match bindings.get(var) {
            Some(Binding::Node(id)) => PropValue::Int(*id as i64),
            Some(Binding::Edge(id)) => PropValue::Int(*id as i64),
            None => PropValue::Null,
        },
        ReturnItem::Property(var, prop) => match bindings.get(var) {
            Some(Binding::Node(id)) => store
                .get_node(*id)
                .and_then(|n| n.properties.get(prop))
                .cloned()
                .unwrap_or(PropValue::Null),
            Some(Binding::Edge(id)) => store
                .get_edge(*id)
                .and_then(|e| e.properties.get(prop))
                .cloned()
                .unwrap_or(PropValue::Null),
            None => PropValue::Null,
        },
        ReturnItem::Count => PropValue::Null,
        ReturnItem::All => {
            let mut parts: Vec<String> = Vec::new();
            for (var, binding) in bindings {
                match binding {
                    Binding::Node(id) => {
                        if let Some(node) = store.get_node(*id) {
                            parts.push(format!("{var}=Node({})", node.id));
                        }
                    }
                    Binding::Edge(id) => {
                        if let Some(edge) = store.get_edge(*id) {
                            parts.push(format!("{var}=Edge({})", edge.id));
                        }
                    }
                }
            }
            parts.sort();
            PropValue::Text(parts.join(", "))
        }
    }
}

fn execute_create(
    store: &mut GraphStore,
    items: &[CreateItem],
) -> Result<CypherResult, CypherError> {
    let mut var_map: HashMap<String, NodeId> = HashMap::new();
    let mut created_node_ids: Vec<NodeId> = Vec::new();
    let mut created_edge_ids: Vec<u64> = Vec::new();
    for item in items {
        match item {
            CreateItem::Node { variable, labels, properties } => {
                // If this variable already exists and has no new labels/properties,
                // treat it as a reference to an existing node (not a new creation).
                if let Some(var) = variable
                    && var_map.contains_key(var) && labels.is_empty() && properties.is_empty() {
                        continue;
                    }
                let props: Properties = properties.clone();
                let node_id = store.create_node(labels.clone(), props);
                created_node_ids.push(node_id);
                if let Some(var) = variable {
                    var_map.insert(var.clone(), node_id);
                }
            }
            CreateItem::Edge { from_var, to_var, edge_type, properties } => {
                let from_id = var_map.get(from_var).ok_or_else(|| {
                    CypherError::InvalidSyntax(
                        format!("undefined variable in CREATE edge: {from_var}"))
                })?;
                let to_id = var_map.get(to_var).ok_or_else(|| {
                    CypherError::InvalidSyntax(
                        format!("undefined variable in CREATE edge: {to_var}"))
                })?;
                let props: Properties = properties.clone();
                let edge_id = store
                    .create_edge(*from_id, *to_id, edge_type.clone(), props)
                    .ok_or_else(|| {
                        CypherError::InvalidSyntax(
                            "failed to create edge: node not found".to_string())
                    })?;
                created_edge_ids.push(edge_id);
            }
        }
    }
    let mut columns = Vec::new();
    let mut row = Vec::new();
    for (i, nid) in created_node_ids.iter().enumerate() {
        columns.push(format!("node_{i}"));
        row.push(PropValue::Int(*nid as i64));
    }
    for (i, eid) in created_edge_ids.iter().enumerate() {
        columns.push(format!("edge_{i}"));
        row.push(PropValue::Int(*eid as i64));
    }
    Ok(CypherResult {
        columns,
        rows: if row.is_empty() { Vec::new() } else { vec![row] },
    })
}

fn execute_delete(
    store: &mut GraphStore,
    variables: &[String],
) -> Result<CypherResult, CypherError> {
    let mut deleted = 0i64;
    for var in variables {
        match var.parse::<u64>() {
            Ok(id) => {
                if store.delete_node(id) {
                    deleted += 1;
                }
            }
            Err(_) => {
                return Err(CypherError::InvalidSyntax(
                    format!("DELETE requires node IDs, got variable '{var}'")
                ));
            }
        }
    }
    Ok(CypherResult {
        columns: vec!["deleted".to_string()],
        rows: vec![vec![PropValue::Int(deleted)]],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::props;

    fn social_graph() -> GraphStore {
        let mut g = GraphStore::new();
        g.create_node(vec!["Person".into()],
            props(vec![("name", PropValue::Text("Alice".into())), ("age", PropValue::Int(30))]));
        g.create_node(vec!["Person".into()],
            props(vec![("name", PropValue::Text("Bob".into())), ("age", PropValue::Int(25))]));
        g.create_node(vec!["Person".into()],
            props(vec![("name", PropValue::Text("Charlie".into())), ("age", PropValue::Int(35))]));
        g.create_node(vec!["Company".into()],
            props(vec![("name", PropValue::Text("Acme Corp".into()))]));
        g.create_edge(1, 2, "FRIENDS".into(), Properties::new());
        g.create_edge(2, 3, "FRIENDS".into(), Properties::new());
        g.create_edge(1, 4, "WORKS_AT".into(), props(vec![("since", PropValue::Int(2020))]));
        g.create_edge(2, 4, "WORKS_AT".into(), props(vec![("since", PropValue::Int(2022))]));
        g
    }

    #[test]
    fn match_by_label() {
        let mut s = social_graph();
        let r = execute_cypher(&mut s, &parse_cypher("MATCH (n:Person) RETURN n").unwrap()).unwrap();
        assert_eq!(r.columns, vec!["n"]);
        assert_eq!(r.rows.len(), 3);
    }
    #[test]
    fn match_with_where_int() {
        let mut s = social_graph();
        let r = execute_cypher(&mut s, &parse_cypher("MATCH (n:Person) WHERE n.age = 25 RETURN n.name").unwrap()).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], PropValue::Text("Bob".into()));
    }
    #[test]
    fn match_count_aggregation() {
        let mut s = social_graph();
        let r = execute_cypher(&mut s, &parse_cypher("MATCH (n:Person) RETURN COUNT(*)").unwrap()).unwrap();
        assert_eq!(r.columns, vec!["COUNT(*)"]);
        assert_eq!(r.rows[0][0], PropValue::Int(3));
    }
    #[test]
    fn match_return_property() {
        let mut s = social_graph();
        let r = execute_cypher(&mut s, &parse_cypher("MATCH (n:Person) RETURN n.name, n.age").unwrap()).unwrap();
        assert_eq!(r.columns, vec!["n.name", "n.age"]);
        assert_eq!(r.rows.len(), 3);
    }
    #[test]
    fn match_company_nodes() {
        let mut s = social_graph();
        let r = execute_cypher(&mut s, &parse_cypher("MATCH (c:Company) RETURN c.name").unwrap()).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], PropValue::Text("Acme Corp".into()));
    }
    #[test]
    fn match_edge_traversal() {
        let mut s = social_graph();
        let r = execute_cypher(&mut s,
            &parse_cypher("MATCH (a:Person)-[r:FRIENDS]->(b:Person) RETURN a.name, b.name").unwrap()).unwrap();
        assert_eq!(r.rows.len(), 2);
    }
    #[test]
    fn match_edge_type_filter() {
        let mut s = social_graph();
        let r = execute_cypher(&mut s,
            &parse_cypher("MATCH (p:Person)-[r:WORKS_AT]->(c:Company) RETURN p.name, c.name").unwrap()).unwrap();
        assert_eq!(r.rows.len(), 2);
    }
    #[test]
    fn create_node_no_properties() {
        let mut s = GraphStore::new();
        let r = execute_cypher(&mut s, &parse_cypher("CREATE (n:Marker)").unwrap()).unwrap();
        assert_eq!(s.node_count(), 1);
        assert_eq!(r.rows.len(), 1);
    }
    #[test]
    fn match_with_where_string() {
        let mut s = social_graph();
        let cypher = r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#;
        let r = execute_cypher(&mut s, &parse_cypher(cypher).unwrap()).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], PropValue::Text("Alice".into()));
    }
    #[test]
    fn create_node_with_props() {
        let mut s = GraphStore::new();
        let cypher = r#"CREATE (n:Person {name: "Eve", age: 22})"#;
        let r = execute_cypher(&mut s, &parse_cypher(cypher).unwrap()).unwrap();
        assert_eq!(r.columns, vec!["node_0"]);
        assert_eq!(s.node_count(), 1);
        let p = s.nodes_by_label("Person");
        assert_eq!(p[0].properties.get("name"), Some(&PropValue::Text("Eve".into())));
        assert_eq!(p[0].properties.get("age"), Some(&PropValue::Int(22)));
    }
    #[test]
    fn create_node_and_edge() {
        let mut s = GraphStore::new();
        let cypher = r#"CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"}), (a)-[:KNOWS]->(b)"#;
        let r = execute_cypher(&mut s, &parse_cypher(cypher).unwrap()).unwrap();
        assert_eq!(s.node_count(), 2);
        assert_eq!(s.edge_count(), 1);
        assert!(r.columns.contains(&"edge_0".to_string()));
    }
    #[test]
    fn roundtrip_create_match() {
        let mut s = GraphStore::new();
        let c = r#"CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"}), (a)-[:FRIENDS]->(b)"#;
        execute_cypher(&mut s, &parse_cypher(c).unwrap()).unwrap();
        let q = "MATCH (a:Person)-[r:FRIENDS]->(b:Person) RETURN a.name, b.name";
        let r = execute_cypher(&mut s, &parse_cypher(q).unwrap()).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], PropValue::Text("Alice".into()));
        assert_eq!(r.rows[0][1], PropValue::Text("Bob".into()));
    }
    #[test]
    fn roundtrip_create_count() {
        let mut s = GraphStore::new();
        execute_cypher(&mut s, &parse_cypher(r#"CREATE (n:City {name: "NYC"})"#).unwrap()).unwrap();
        execute_cypher(&mut s, &parse_cypher(r#"CREATE (n:City {name: "LA"})"#).unwrap()).unwrap();
        execute_cypher(&mut s, &parse_cypher(r#"CREATE (n:City {name: "CHI"})"#).unwrap()).unwrap();
        let r = execute_cypher(&mut s, &parse_cypher("MATCH (c:City) RETURN COUNT(*)").unwrap()).unwrap();
        assert_eq!(r.rows[0][0], PropValue::Int(3));
    }
    #[test]
    fn roundtrip_create_where() {
        let mut s = GraphStore::new();
        execute_cypher(&mut s, &parse_cypher(r#"CREATE (a:Person {name: "Alice", age: 30})"#).unwrap()).unwrap();
        execute_cypher(&mut s, &parse_cypher(r#"CREATE (b:Person {name: "Bob", age: 25})"#).unwrap()).unwrap();
        let r = execute_cypher(&mut s, &parse_cypher("MATCH (n:Person) WHERE n.age = 30 RETURN n.name").unwrap()).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], PropValue::Text("Alice".into()));
    }
    #[test]
    fn match_no_results() {
        let mut s = social_graph();
        let cypher = r#"MATCH (n:Person) WHERE n.name = "Nobody" RETURN n.name"#;
        let r = execute_cypher(&mut s, &parse_cypher(cypher).unwrap()).unwrap();
        assert_eq!(r.rows.len(), 0);
    }

    // ====================================================================
    // OPTIONAL MATCH tests
    // ====================================================================

    #[test]
    fn optional_match_no_results_returns_nulls() {
        let mut s = social_graph();
        let cypher = r#"OPTIONAL MATCH (n:NonExistent) RETURN n.name"#;
        let r = execute_cypher(&mut s, &parse_cypher(cypher).unwrap()).unwrap();
        // OPTIONAL MATCH returns a single row of NULLs when no matches
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], PropValue::Null);
    }

    #[test]
    fn optional_match_with_results() {
        let mut s = social_graph();
        // This should behave like normal MATCH when results exist
        let cypher = r#"OPTIONAL MATCH (n:Person) RETURN n.name"#;
        let r = execute_cypher(&mut s, &parse_cypher(cypher).unwrap()).unwrap();
        // Should return actual results (same as MATCH)
        assert!(!r.rows.is_empty());
        assert!(r.rows.iter().any(|row| row[0] != PropValue::Null));
    }

    #[test]
    fn optional_match_where_no_match() {
        let mut s = social_graph();
        let cypher = r#"OPTIONAL MATCH (n:Person) WHERE n.name = "Nobody" RETURN n.name"#;
        let r = execute_cypher(&mut s, &parse_cypher(cypher).unwrap()).unwrap();
        // WHERE filters all — OPTIONAL gives us NULL row
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], PropValue::Null);
    }

    // ---- WITH clause tests ----

    #[test]
    fn with_passthrough() {
        // WITH n simply passes bindings through to RETURN
        let mut s = social_graph();
        let cypher = r#"MATCH (n:Person) WITH n RETURN n.name"#;
        let r = execute_cypher(&mut s, &parse_cypher(cypher).unwrap()).unwrap();
        assert!(!r.rows.is_empty());
        // Should return the same names as without WITH
        let names: Vec<&PropValue> = r.rows.iter().map(|row| &row[0]).collect();
        assert!(names.contains(&&PropValue::Text("Alice".to_string())));
    }

    #[test]
    fn with_alias() {
        // WITH n.name AS name renames the binding
        let mut s = social_graph();
        let cypher = r#"MATCH (n:Person) WITH n AS person RETURN person.name"#;
        let r = execute_cypher(&mut s, &parse_cypher(cypher).unwrap()).unwrap();
        assert!(!r.rows.is_empty());
        let names: Vec<&PropValue> = r.rows.iter().map(|row| &row[0]).collect();
        assert!(names.contains(&&PropValue::Text("Alice".to_string())));
    }

    #[test]
    fn with_where_filter() {
        // WITH + WHERE filters intermediate results
        let mut s = social_graph();
        let cypher =
            r#"MATCH (n:Person) WITH n WHERE n.name = "Alice" RETURN n.name"#;
        let r = execute_cypher(&mut s, &parse_cypher(cypher).unwrap()).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], PropValue::Text("Alice".to_string()));
    }
}
