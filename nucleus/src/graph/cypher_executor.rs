//! Cypher query execution engine (Phase 8b).
//!
//! Takes parsed Cypher AST and executes it against GraphStore.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::cypher::*;
use super::{Direction, EdgeId, GraphStore, Node, NodeId, PropValue, Properties};

#[derive(Debug, Clone)]
enum Binding {
    Node(NodeId),
    Edge(EdgeId),
    /// A scalar projected by `WITH n.prop AS alias` (GRP-9). Rendering a
    /// node binding under such an alias used to print the node ID.
    Scalar(PropValue),
}

/// Bind `variable` (and the positional internal `__node_{idx}` slot) to `id`.
///
/// A repeated variable in a pattern is an EQUALITY constraint, not a
/// rebinding (GRP-2): a candidate that would bind it to a different node
/// cannot match, so the caller must skip it — returning false here. The
/// positional slot lets an ANONYMOUS pattern node anchor the next edge in
/// the chain (GRP-3); user identifiers starting with `__` are rejected at
/// parse time so the two spaces cannot collide.
fn bind_node_if_consistent(
    nb: &mut HashMap<String, Binding>,
    variable: Option<&str>,
    positional_idx: usize,
    id: NodeId,
) -> bool {
    let positional = format!("__node_{positional_idx}");
    let consistent = |b: &Binding| !matches!(b, Binding::Node(existing) if *existing != id);
    if let Some(v) = variable
        && let Some(b) = nb.get(v)
        && !consistent(b)
    {
        return false;
    }
    if let Some(b) = nb.get(&positional)
        && !consistent(b)
    {
        return false;
    }
    if let Some(v) = variable {
        nb.insert(v.to_string(), Binding::Node(id));
    }
    nb.insert(positional, Binding::Node(id));
    true
}

/// Edge-binding twin of [`bind_node_if_consistent`].
fn bind_edge_if_consistent(
    nb: &mut HashMap<String, Binding>,
    variable: Option<&str>,
    id: EdgeId,
) -> bool {
    if let Some(v) = variable
        && let Some(Binding::Edge(existing)) = nb.get(v)
        && *existing != id
    {
        return false;
    }
    if let Some(v) = variable {
        nb.insert(v.to_string(), Binding::Edge(id));
    }
    true
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
        CypherStatement::CreateNodeIndex {
            label,
            property,
            if_not_exists,
        } => {
            let created = store.create_node_index(label, property);
            if !created && !*if_not_exists {
                return Err(CypherError::InvalidSyntax(format!(
                    "an index on :{label}({property}) already exists"
                )));
            }
            Ok(CypherResult {
                columns: vec!["created".to_string()],
                rows: vec![vec![PropValue::Bool(created)]],
            })
        }
        CypherStatement::DropNodeIndex {
            label,
            property,
            if_exists,
        } => {
            let dropped = store.drop_node_index(label, property);
            if !dropped && !*if_exists {
                return Err(CypherError::InvalidSyntax(format!(
                    "no index on :{label}({property})"
                )));
            }
            Ok(CypherResult {
                columns: vec!["dropped".to_string()],
                rows: vec![vec![PropValue::Bool(dropped)]],
            })
        }
        CypherStatement::ShowIndexes => Ok(CypherResult {
            columns: vec![
                "label".to_string(),
                "property".to_string(),
                "entries".to_string(),
            ],
            rows: store
                .node_index_defs()
                .into_iter()
                .map(|(l, p, n)| {
                    vec![
                        PropValue::Text(l),
                        PropValue::Text(p),
                        PropValue::Int(n as i64),
                    ]
                })
                .collect(),
        }),
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
    store: &GraphStore,
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
                ReturnItem::Property(v, p) => {
                    // GRP-9: resolve the property NOW and bind the SCALAR
                    // under the alias — the old code stored the node binding
                    // there, and `RETURN alias` rendered the node ID.
                    // Cold-aware (GRP-8): the source may be evicted.
                    let scalar = match bindings.get(v) {
                        Some(Binding::Node(id)) => store
                            .get_node_full(*id)
                            .and_then(|n| n.properties.get(p).cloned()),
                        Some(Binding::Edge(id)) => store
                            .get_edge_full(*id)
                            .and_then(|e| e.properties.get(p).cloned()),
                        _ => None,
                    }
                    .unwrap_or(PropValue::Null);
                    new_bindings.insert(name, Binding::Scalar(scalar));
                    // Also keep the original variable for property resolution
                    if let Some(b) = bindings.get(v) {
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
        // Cold-aware fetch (GRP-8): an evicted node's hot properties are
        // empty, so a hot-only read would drop it from every predicate.
        let node = match store.get_node_full(*nid) {
            Some(n) => n,
            None => continue,
        };
        // GRP-7: the anchor's FULL label set must be enforced.
        // `candidate_node_ids` narrows by the first label only, and when the
        // first label is absent from the pattern the candidate set is every
        // node — making this check the only label enforcement.
        if !node_matches_labels(&node, &first_node.labels) {
            continue;
        }
        if !node_matches_properties(&node, &first_node.properties) {
            continue;
        }
        let mut bindings = HashMap::new();
        bind_node_if_consistent(&mut bindings, first_node.variable.as_deref(), 0, node.id);
        binding_sets.push(bindings);
    }
    for edge_pat in &pattern.edges {
        let target_node_pat = &pattern.nodes[edge_pat.to_idx];
        let mut new_binding_sets = Vec::new();

        for bindings in &binding_sets {
            let source_node_pat = &pattern.nodes[edge_pat.from_idx];
            let source_id = match resolve_node_id(bindings, source_node_pat, edge_pat.from_idx) {
                Some(id) => id,
                None => continue,
            };

            if edge_pat.star {
                // Variable-length path expansion (GRP-5/GRP-11): layered BFS
                // over deduped terminals.
                let min_hops = edge_pat.min_hops.unwrap_or(1);
                let terminal_nodes = variable_length_expand(
                    store,
                    source_id,
                    edge_pat.direction,
                    edge_pat.edge_type.as_deref(),
                    &edge_pat.properties,
                    min_hops,
                    edge_pat.max_hops,
                );
                for terminal_id in terminal_nodes {
                    let target_node = match store.get_node_full(terminal_id) {
                        Some(n) => n,
                        None => continue,
                    };
                    if !node_matches_labels(&target_node, &target_node_pat.labels) {
                        continue;
                    }
                    if !node_matches_properties(&target_node, &target_node_pat.properties) {
                        continue;
                    }
                    let mut nb = bindings.clone();
                    if !bind_node_if_consistent(
                        &mut nb,
                        target_node_pat.variable.as_deref(),
                        edge_pat.to_idx,
                        target_node.id,
                    ) {
                        continue;
                    }
                    new_binding_sets.push(nb);
                }
            } else {
                // Single-hop traversal (original logic)
                let neighbors =
                    store.neighbors(source_id, edge_pat.direction, edge_pat.edge_type.as_deref());
                for (neighbor_id, edge) in &neighbors {
                    if !edge_props_match(store, edge, &edge_pat.properties) {
                        continue;
                    }
                    let target_node = match store.get_node_full(*neighbor_id) {
                        Some(n) => n,
                        None => continue,
                    };
                    if !node_matches_labels(&target_node, &target_node_pat.labels) {
                        continue;
                    }
                    if !node_matches_properties(&target_node, &target_node_pat.properties) {
                        continue;
                    }
                    let mut nb = bindings.clone();
                    if !bind_edge_if_consistent(&mut nb, edge_pat.variable.as_deref(), edge.id) {
                        continue;
                    }
                    if !bind_node_if_consistent(
                        &mut nb,
                        target_node_pat.variable.as_deref(),
                        edge_pat.to_idx,
                        target_node.id,
                    ) {
                        continue;
                    }
                    new_binding_sets.push(nb);
                }
            }
        }
        binding_sets = new_binding_sets;
    }
    Ok(binding_sets)
}

/// Whether an edge satisfies an inline relationship property map (GRP-6).
/// An evicted edge's hot properties are empty; when a filter is required
/// and a cold tier exists, resolve the full edge before deciding (GRP-8).
fn edge_props_match(
    store: &GraphStore,
    edge: &super::Edge,
    required: &BTreeMap<String, PropValue>,
) -> bool {
    if required.is_empty() {
        return true;
    }
    let props = if edge.properties.is_empty() && store.has_cold_tier() {
        match store.get_edge_full(edge.id) {
            Some(e) => e.properties,
            None => return false,
        }
    } else {
        edge.properties.clone()
    };
    required.iter().all(|(k, v)| props.get(k) == Some(v))
}

/// Variable-length expansion: terminals are `{v : min <= mindist(source,v) <=
/// max}` (deduped by minimal BFS distance), plus the source itself when
/// `min == 0` or a qualifying self-loop puts it back in band at some depth.
///
/// Layered BFS with one `seen` set — O(levels·(V+E)) time, O(V) memory. The
/// previous DFS enumerated every simple path with a cloned visited-set per
/// push (K25 `*..8` ≈ 1.7e10 paths — hours of CPU from one query) only to
/// deduplicate the identical terminal set at the end (GRP-11). Longer cycles
/// back to the source are NOT terminals: that is the standing node-unique vs
/// relationship-unique deviation, documented and oracle-compensated.
fn variable_length_expand(
    store: &GraphStore,
    source_id: NodeId,
    direction: Direction,
    edge_type: Option<&str>,
    edge_props: &BTreeMap<String, PropValue>,
    min_hops: usize,
    max_hops: Option<usize>,
) -> Vec<NodeId> {
    let mut terminals = Vec::new();
    let mut seen = HashSet::new();
    seen.insert(source_id);
    if min_hops == 0 {
        terminals.push(source_id); // the zero-hop path
    }
    let mut frontier = vec![source_id];
    let mut depth = 0usize;
    while !frontier.is_empty() && max_hops.is_none_or(|m| depth < m) {
        depth += 1;
        let mut next = Vec::new();
        for cur in &frontier {
            for (nbr, edge) in store.neighbors(*cur, direction, edge_type) {
                if !edge_props_match(store, edge, edge_props) {
                    continue;
                }
                if nbr == source_id
                    && *cur == source_id
                    && depth >= min_hops
                    && !terminals.contains(&source_id)
                {
                    // A TRUE self-loop (an edge source→source) puts the
                    // source back in band at one hop. Longer cycles back to
                    // the source are not terminals — the documented
                    // node-unique vs relationship-unique deviation.
                    terminals.push(source_id);
                }
                if seen.insert(nbr) {
                    // First-seen == minimal distance.
                    if depth >= min_hops {
                        terminals.push(nbr);
                    }
                    next.push(nbr);
                }
            }
        }
        frontier = next;
    }
    terminals
}

/// Candidate anchor nodes for the first node pattern of a MATCH.
///
/// When the pattern carries a label and an inline property equality, and an
/// index covers that `(label, property)`, the index answers directly instead of
/// scanning every node with the label. This is a *narrowing* only: the caller
/// still runs `node_matches_properties` over the full property map, so an index
/// on one of several properties is safe, and a wrong index could only ever make
/// the query slow, never wrong.
fn candidate_node_ids(store: &GraphStore, np: &NodePattern) -> Vec<NodeId> {
    if let Some(label) = np.labels.first() {
        // `np.properties` is a BTreeMap, so "the first usable index" is a
        // deterministic choice rather than a hash-order accident.
        for (key, value) in &np.properties {
            if let Some(ids) = store.node_index_lookup(label, key, value) {
                return ids;
            }
        }
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

/// Resolve a pattern node to a bound id: by variable first, then by the
/// positional internal `__node_{idx}` slot — the fallback that lets an
/// ANONYMOUS node anchor the next edge in a chain (GRP-3).
fn resolve_node_id(
    bindings: &HashMap<String, Binding>,
    np: &NodePattern,
    idx: usize,
) -> Option<NodeId> {
    if let Some(ref var) = np.variable
        && let Some(Binding::Node(id)) = bindings.get(var)
    {
        return Some(*id);
    }
    if let Some(Binding::Node(id)) = bindings.get(&format!("__node_{idx}")) {
        return Some(*id);
    }
    None
}

fn evaluate_where(
    store: &GraphStore,
    bindings: &HashMap<String, Binding>,
    wc: &WhereClause,
) -> bool {
    wc.conditions
        .iter()
        .all(|c| evaluate_condition(store, bindings, c))
}

fn evaluate_condition(
    store: &GraphStore,
    bindings: &HashMap<String, Binding>,
    condition: &Condition,
) -> bool {
    match condition {
        Condition::PropertyEquals {
            variable,
            property,
            value,
        } => match bindings.get(variable) {
            Some(Binding::Node(id)) => store
                .get_node_full(*id)
                .is_some_and(|n| n.properties.get(property) == Some(value)),
            Some(Binding::Edge(id)) => store
                .get_edge_full(*id)
                .is_some_and(|e| e.properties.get(property) == Some(value)),
            Some(Binding::Scalar(v)) => v == value,
            None => false,
        },
        Condition::VariableEquals { variable, value } => match bindings.get(variable) {
            Some(Binding::Scalar(v)) => v == value,
            // A node/edge binding has no scalar identity to compare against
            // a literal; only WITH-projected scalars reach this arm.
            _ => false,
        },
        Condition::And(left, right) => {
            evaluate_condition(store, bindings, left) && evaluate_condition(store, bindings, right)
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
            Some(Binding::Scalar(v)) => v.clone(),
            None => PropValue::Null,
        },
        ReturnItem::Property(var, prop) => match bindings.get(var) {
            Some(Binding::Node(id)) => store
                .get_node_full(*id)
                .and_then(|n| n.properties.get(prop).cloned())
                .unwrap_or(PropValue::Null),
            Some(Binding::Edge(id)) => store
                .get_edge_full(*id)
                .and_then(|e| e.properties.get(prop).cloned())
                .unwrap_or(PropValue::Null),
            Some(Binding::Scalar(_)) => PropValue::Null,
            None => PropValue::Null,
        },
        ReturnItem::Count => PropValue::Null,
        ReturnItem::All => {
            let mut parts: Vec<String> = Vec::new();
            for (var, binding) in bindings {
                match binding {
                    Binding::Node(id) => {
                        if let Some(node) = store.get_node_full(*id) {
                            parts.push(format!("{var}=Node({})", node.id));
                        }
                    }
                    Binding::Edge(id) => {
                        if let Some(edge) = store.get_edge_full(*id) {
                            parts.push(format!("{var}=Edge({})", edge.id));
                        }
                    }
                    Binding::Scalar(v) => parts.push(format!("{var}={v:?}")),
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
            CreateItem::Node {
                variable,
                labels,
                properties,
            } => {
                // If this variable already exists and has no new labels/properties,
                // treat it as a reference to an existing node (not a new creation).
                if let Some(var) = variable
                    && var_map.contains_key(var)
                    && labels.is_empty()
                    && properties.is_empty()
                {
                    continue;
                }
                let props: Properties = properties.clone();
                let node_id = store.create_node(labels.clone(), props);
                created_node_ids.push(node_id);
                if let Some(var) = variable {
                    var_map.insert(var.clone(), node_id);
                }
            }
            CreateItem::Edge {
                from_var,
                to_var,
                edge_type,
                properties,
            } => {
                let from_id = var_map.get(from_var).ok_or_else(|| {
                    CypherError::InvalidSyntax(format!(
                        "undefined variable in CREATE edge: {from_var}"
                    ))
                })?;
                let to_id = var_map.get(to_var).ok_or_else(|| {
                    CypherError::InvalidSyntax(format!(
                        "undefined variable in CREATE edge: {to_var}"
                    ))
                })?;
                let props: Properties = properties.clone();
                let edge_id = store
                    .create_edge(*from_id, *to_id, edge_type.clone(), props)
                    .ok_or_else(|| {
                        CypherError::InvalidSyntax(
                            "failed to create edge: node not found".to_string(),
                        )
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
        rows: if row.is_empty() {
            Vec::new()
        } else {
            vec![row]
        },
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
                return Err(CypherError::InvalidSyntax(format!(
                    "DELETE requires node IDs, got variable '{var}'"
                )));
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
    use super::super::props;
    use super::*;

    fn social_graph() -> GraphStore {
        let mut g = GraphStore::new();
        g.create_node(
            vec!["Person".into()],
            props(vec![
                ("name", PropValue::Text("Alice".into())),
                ("age", PropValue::Int(30)),
            ]),
        );
        g.create_node(
            vec!["Person".into()],
            props(vec![
                ("name", PropValue::Text("Bob".into())),
                ("age", PropValue::Int(25)),
            ]),
        );
        g.create_node(
            vec!["Person".into()],
            props(vec![
                ("name", PropValue::Text("Charlie".into())),
                ("age", PropValue::Int(35)),
            ]),
        );
        g.create_node(
            vec!["Company".into()],
            props(vec![("name", PropValue::Text("Acme Corp".into()))]),
        );
        g.create_edge(1, 2, "FRIENDS".into(), Properties::new());
        g.create_edge(2, 3, "FRIENDS".into(), Properties::new());
        g.create_edge(
            1,
            4,
            "WORKS_AT".into(),
            props(vec![("since", PropValue::Int(2020))]),
        );
        g.create_edge(
            2,
            4,
            "WORKS_AT".into(),
            props(vec![("since", PropValue::Int(2022))]),
        );
        g
    }

    #[test]
    fn match_by_label() {
        let mut s = social_graph();
        let r =
            execute_cypher(&mut s, &parse_cypher("MATCH (n:Person) RETURN n").unwrap()).unwrap();
        assert_eq!(r.columns, vec!["n"]);
        assert_eq!(r.rows.len(), 3);
    }
    #[test]
    fn match_with_where_int() {
        let mut s = social_graph();
        let r = execute_cypher(
            &mut s,
            &parse_cypher("MATCH (n:Person) WHERE n.age = 25 RETURN n.name").unwrap(),
        )
        .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], PropValue::Text("Bob".into()));
    }
    #[test]
    fn match_count_aggregation() {
        let mut s = social_graph();
        let r = execute_cypher(
            &mut s,
            &parse_cypher("MATCH (n:Person) RETURN COUNT(*)").unwrap(),
        )
        .unwrap();
        assert_eq!(r.columns, vec!["COUNT(*)"]);
        assert_eq!(r.rows[0][0], PropValue::Int(3));
    }
    #[test]
    fn match_return_property() {
        let mut s = social_graph();
        let r = execute_cypher(
            &mut s,
            &parse_cypher("MATCH (n:Person) RETURN n.name, n.age").unwrap(),
        )
        .unwrap();
        assert_eq!(r.columns, vec!["n.name", "n.age"]);
        assert_eq!(r.rows.len(), 3);
    }
    #[test]
    fn match_company_nodes() {
        let mut s = social_graph();
        let r = execute_cypher(
            &mut s,
            &parse_cypher("MATCH (c:Company) RETURN c.name").unwrap(),
        )
        .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], PropValue::Text("Acme Corp".into()));
    }
    #[test]
    fn match_edge_traversal() {
        let mut s = social_graph();
        let r = execute_cypher(
            &mut s,
            &parse_cypher("MATCH (a:Person)-[r:FRIENDS]->(b:Person) RETURN a.name, b.name")
                .unwrap(),
        )
        .unwrap();
        assert_eq!(r.rows.len(), 2);
    }
    #[test]
    fn match_edge_type_filter() {
        let mut s = social_graph();
        let r = execute_cypher(
            &mut s,
            &parse_cypher("MATCH (p:Person)-[r:WORKS_AT]->(c:Company) RETURN p.name, c.name")
                .unwrap(),
        )
        .unwrap();
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
        assert_eq!(
            p[0].properties.get("name"),
            Some(&PropValue::Text("Eve".into()))
        );
        assert_eq!(p[0].properties.get("age"), Some(&PropValue::Int(22)));
    }
    #[test]
    fn create_node_and_edge() {
        let mut s = GraphStore::new();
        let cypher =
            r#"CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"}), (a)-[:KNOWS]->(b)"#;
        let r = execute_cypher(&mut s, &parse_cypher(cypher).unwrap()).unwrap();
        assert_eq!(s.node_count(), 2);
        assert_eq!(s.edge_count(), 1);
        assert!(r.columns.contains(&"edge_0".to_string()));
    }
    #[test]
    fn roundtrip_create_match() {
        let mut s = GraphStore::new();
        let c =
            r#"CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"}), (a)-[:FRIENDS]->(b)"#;
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
        execute_cypher(
            &mut s,
            &parse_cypher(r#"CREATE (n:City {name: "NYC"})"#).unwrap(),
        )
        .unwrap();
        execute_cypher(
            &mut s,
            &parse_cypher(r#"CREATE (n:City {name: "LA"})"#).unwrap(),
        )
        .unwrap();
        execute_cypher(
            &mut s,
            &parse_cypher(r#"CREATE (n:City {name: "CHI"})"#).unwrap(),
        )
        .unwrap();
        let r = execute_cypher(
            &mut s,
            &parse_cypher("MATCH (c:City) RETURN COUNT(*)").unwrap(),
        )
        .unwrap();
        assert_eq!(r.rows[0][0], PropValue::Int(3));
    }
    #[test]
    fn roundtrip_create_where() {
        let mut s = GraphStore::new();
        execute_cypher(
            &mut s,
            &parse_cypher(r#"CREATE (a:Person {name: "Alice", age: 30})"#).unwrap(),
        )
        .unwrap();
        execute_cypher(
            &mut s,
            &parse_cypher(r#"CREATE (b:Person {name: "Bob", age: 25})"#).unwrap(),
        )
        .unwrap();
        let r = execute_cypher(
            &mut s,
            &parse_cypher("MATCH (n:Person) WHERE n.age = 30 RETURN n.name").unwrap(),
        )
        .unwrap();
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
        let cypher = r#"MATCH (n:Person) WITH n WHERE n.name = "Alice" RETURN n.name"#;
        let r = execute_cypher(&mut s, &parse_cypher(cypher).unwrap()).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], PropValue::Text("Alice".to_string()));
    }

    // ====================================================================
    // GRP cluster — binding, traversal and tiering regressions
    // ====================================================================

    fn count_of(s: &mut GraphStore, q: &str) -> i64 {
        let r = execute_cypher(s, &parse_cypher(q).unwrap()).unwrap();
        match r.rows.first().and_then(|row| row.first()) {
            Some(PropValue::Int(n)) => *n,
            other => panic!("expected a COUNT(*) integer, got {other:?}"),
        }
    }

    /// GRP-2: a repeated variable in a pattern is an equality constraint,
    /// not a rebinding. On the 2-cycle 1⇄2 (+ extra 2→3), `(a)->(b)->(a)`
    /// has exactly two legal assignments — (1,2) and (2,1); the rebinding
    /// bug also accepted (a=3,b=2), where no 3→2 edge exists at all.
    #[test]
    fn repeated_variables_constrain_instead_of_rebinding() {
        let mut s = GraphStore::new();
        for _ in 0..3 {
            s.create_node(vec!["N".into()], Properties::new());
        }
        // 2-cycle 1→2→1 plus an extra edge 2→3.
        s.create_edge(1, 2, "K".into(), Properties::new());
        s.create_edge(2, 1, "K".into(), Properties::new());
        s.create_edge(2, 3, "K".into(), Properties::new());
        let r = execute_cypher(
            &mut s,
            &parse_cypher("MATCH (a)-[:K]->(b)-[:K]->(a) RETURN a, b").unwrap(),
        )
        .unwrap();
        let mut pairs: Vec<(i64, i64)> = r
            .rows
            .iter()
            .filter_map(|row| match (&row[0], &row[1]) {
                (PropValue::Int(a), PropValue::Int(b)) => Some((*a, *b)),
                _ => None,
            })
            .collect();
        pairs.sort_unstable();
        assert_eq!(
            pairs,
            vec![(1, 2), (2, 1)],
            "the closing edge must constrain, not rebind: an assignment with no \
             backing edge is the rebinding defect"
        );

        let mut s2 = GraphStore::new();
        s2.create_node(
            vec!["P".into()],
            props(vec![("name", PropValue::Text("x".into()))]),
        );
        s2.create_node(
            vec!["P".into()],
            props(vec![("name", PropValue::Text("y".into()))]),
        );
        s2.create_node(
            vec!["P".into()],
            props(vec![("name", PropValue::Text("z".into()))]),
        );
        // Self-loop plus two other out-edges.
        s2.create_edge(1, 1, "K".into(), Properties::new());
        s2.create_edge(1, 2, "K".into(), Properties::new());
        s2.create_edge(1, 3, "K".into(), Properties::new());
        assert_eq!(
            count_of(
                &mut s2,
                r#"MATCH (a:P {name: "x"})-[:K]->(a) RETURN COUNT(*)"#
            ),
            1,
            "a self-loop pattern must count the loop, not the out-degree"
        );
    }

    /// GRP-3: an anonymous node pattern as an edge source used to leave no
    /// binding, so the next edge in the chain resolved nothing and every row
    /// was dropped.
    #[test]
    fn anonymous_source_yields_real_rows() {
        let mut s = GraphStore::new();
        s.create_node(
            vec!["P".into()],
            props(vec![("name", PropValue::Text("x".into()))]),
        );
        s.create_node(
            vec!["P".into()],
            props(vec![("name", PropValue::Text("y".into()))]),
        );
        s.create_node(
            vec!["P".into()],
            props(vec![("name", PropValue::Text("z".into()))]),
        );
        s.create_edge(1, 2, "K".into(), Properties::new());
        s.create_edge(1, 3, "K".into(), Properties::new());
        assert_eq!(
            count_of(
                &mut s,
                r#"MATCH (:P {name: "x"})-[:K]->(b) RETURN COUNT(*)"#
            ),
            2,
            "an anonymous source must still anchor the edge chain"
        );
    }

    /// GRP-5, concretely: same 12-node chain, but each node carries its
    /// position so terminals are identifiable. Asserts the hop table from
    /// node 1.
    #[test]
    fn variable_length_hop_table_from_node_one() {
        let mut s = GraphStore::new();
        for pos in 1..=12i64 {
            s.create_node(vec!["N".into()], props(vec![("pos", PropValue::Int(pos))]));
        }
        for from in 1..=11u64 {
            s.create_edge(from, from + 1, "T".into(), Properties::new());
        }
        let mut terminals = |q: &str| -> Vec<i64> {
            let r = execute_cypher(&mut s, &parse_cypher(q).unwrap()).unwrap();
            let mut v: Vec<i64> = r
                .rows
                .iter()
                .filter_map(|row| match row.first() {
                    Some(PropValue::Int(n)) => Some(*n),
                    _ => None,
                })
                .collect();
            v.sort_unstable();
            v
        };

        // Fixed *2: exactly the distance-2 node (pos 3).
        assert_eq!(
            terminals("MATCH (a:N {pos: 1})-[:T*2]->(b) RETURN b.pos"),
            vec![3],
            "*2 must mean exactly two hops"
        );
        // Open *1.. : all 11 others.
        assert_eq!(
            terminals("MATCH (a:N {pos: 1})-[:T*1..]->(b) RETURN b.pos").len(),
            11,
            "*1.. must be unbounded, not capped at 10"
        );
        // Bare * means the same 1..inf.
        assert_eq!(
            terminals("MATCH (a:N {pos: 1})-[:T*]->(b) RETURN b.pos").len(),
            11,
            "bare * must mean 1..inf, not a single hop"
        );
        // *.. unbounded with no min spelled: same as bare *.
        assert_eq!(
            terminals("MATCH (a:N {pos: 1})-[:T*..]->(b) RETURN b.pos").len(),
            11,
            "*.. must mean 1..inf"
        );

        // Zero-hop *0..1: the source itself plus distance-1.
        assert_eq!(
            terminals("MATCH (a:N {pos: 1})-[:T*0..1]->(b) RETURN b.pos"),
            vec![1, 2],
            "*0.. must include the zero-hop terminal (the source)"
        );
        // Self-loop: *1..1 on a node with a self-loop returns the node.
        let mut sl = GraphStore::new();
        sl.create_node(vec!["S".into()], Properties::new());
        sl.create_node(vec!["S".into()], Properties::new());
        sl.create_edge(1, 1, "L".into(), Properties::new());
        sl.create_edge(1, 2, "L".into(), Properties::new());
        assert_eq!(
            count_of(&mut sl, "MATCH (a:S)-[:L*1..1]->(a) RETURN COUNT(*)"),
            1,
            "a self-loop terminal at 1 hop in band must return the source"
        );
    }

    /// GRP-11: path enumeration is exponential; the BFS rewrite answers the
    /// same deduped terminal set in polynomial time. K25, `*..8` from one
    /// anchor would need ~1.7e10 simple paths under the old DFS.
    #[test]
    fn variable_length_expansion_is_polynomial_on_a_complete_graph() {
        let n = 25u64;
        let mut s = GraphStore::new();
        for pos in 1..=n {
            s.create_node(
                vec!["K".into()],
                props(vec![("pos", PropValue::Int(pos as i64))]),
            );
        }
        for from in 1..=n {
            for to in 1..=n {
                if from != to {
                    s.create_edge(from, to, "T".into(), Properties::new());
                }
            }
        }
        let start = std::time::Instant::now();
        let c = count_of(&mut s, "MATCH (a:K {pos: 1})-[:T*..8]->(b) RETURN COUNT(*)");
        let elapsed = start.elapsed();
        assert_eq!(
            c, 24,
            "from one anchor, every other node is reachable in 1 hop"
        );
        assert!(
            elapsed.as_secs() < 5,
            "K25 *..8 took {elapsed:?} — path enumeration is still exponential"
        );
    }

    /// GRP-6: an inline property map on a relationship pattern was parsed and
    /// silently discarded, over-counting matches.
    #[test]
    fn inline_edge_property_maps_filter() {
        let mut s = GraphStore::new();
        s.create_node(vec!["P".into()], Properties::new());
        s.create_node(vec!["P".into()], Properties::new());
        s.create_edge(1, 2, "KNOWS".into(), props(vec![("w", PropValue::Int(1))]));
        s.create_edge(1, 2, "KNOWS".into(), props(vec![("w", PropValue::Int(2))]));
        assert_eq!(
            count_of(&mut s, "MATCH (a)-[r:KNOWS {w: 1}]->(b) RETURN COUNT(*)"),
            1,
            "the inline property map must filter, not vanish"
        );
    }

    /// GRP-7: only the first label of a multi-label anchor was enforced.
    #[test]
    fn all_anchor_labels_are_enforced() {
        let mut s = GraphStore::new();
        s.create_node(vec!["P".into()], Properties::new());
        s.create_node(vec!["P".into(), "Admin".into()], Properties::new());
        assert_eq!(
            count_of(&mut s, "MATCH (n:P:Admin) RETURN COUNT(*)"),
            1,
            "the anchor must enforce every label, not just the first"
        );
    }

    /// GRP-9: `WITH n.prop AS alias RETURN alias` returned the node ID — the
    /// Property arm stored the node binding under the alias and the Variable
    /// arm rendered nodes as their integer id.
    #[test]
    fn with_property_projection_yields_the_value() {
        let mut s = social_graph();
        let r = execute_cypher(
            &mut s,
            &parse_cypher("MATCH (n:Person) WITH n.age AS a RETURN a").unwrap(),
        )
        .unwrap();
        assert_eq!(r.rows.len(), 3);
        let mut ages: Vec<i64> = r
            .rows
            .iter()
            .filter_map(|row| match row.first() {
                Some(PropValue::Int(n)) => Some(*n),
                _ => None,
            })
            .collect();
        ages.sort_unstable();
        assert_eq!(ages, vec![25, 30, 35], "got rows: {:?}", r.rows);

        let r = execute_cypher(
            &mut s,
            &parse_cypher(r#"MATCH (n:Person) WITH n.name AS nm WHERE nm = "Alice" RETURN nm"#)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            r.rows,
            vec![vec![PropValue::Text("Alice".into())]],
            "a WITH-projected scalar must be usable in WHERE"
        );
    }

    /// GRP-8: once eviction starts (>max_hot_nodes), Cypher reads were
    /// hot-only — evicted nodes lost their properties and the property index's
    /// proposals were rejected by the hot-only post-filter.
    #[test]
    fn cypher_reads_survive_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = GraphStore::open(dir.path()).unwrap();
        s.max_hot_nodes = 4;
        let n = 20i64;
        for i in 1..=n {
            s.create_node(vec!["N".into()], props(vec![("k", PropValue::Int(i))]));
        }
        assert!(
            s.node_count_hot() <= 4,
            "fixture must actually evict (hot={})",
            s.node_count_hot()
        );

        // (1) RETURN n.k must surface every node's value, not NULLs.
        let r = execute_cypher(&mut s, &parse_cypher("MATCH (n:N) RETURN n.k").unwrap()).unwrap();
        let mut vals: Vec<i64> = r
            .rows
            .iter()
            .filter_map(|row| match row.first() {
                Some(PropValue::Int(v)) => Some(*v),
                _ => None,
            })
            .collect();
        vals.sort_unstable();
        assert_eq!(
            vals,
            (1..=n).collect::<Vec<_>>(),
            "evicted nodes lost their properties in Cypher reads"
        );

        // (2) indexed MATCH must still find an evicted node.
        execute_cypher(&mut s, &parse_cypher("CREATE INDEX ON :N(k)").unwrap()).unwrap();
        let r = execute_cypher(
            &mut s,
            &parse_cypher("MATCH (n:N {k: 17}) RETURN n.k").unwrap(),
        )
        .unwrap();
        assert_eq!(
            r.rows,
            vec![vec![PropValue::Int(17)]],
            "an indexed lookup of an evicted node returned nothing"
        );

        // (3) Dijkstra over an evicted weighted edge must read the stored
        // weight, not default to 1.0.
        let mut g = GraphStore::open(dir.path().join("dijkstra").as_path()).unwrap();
        g.max_hot_nodes = 1;
        let a = g.create_node(vec!["V".into()], Properties::new());
        let b = g.create_node(vec!["V".into()], Properties::new());
        let c = g.create_node(vec!["V".into()], Properties::new());
        g.create_edge(
            a,
            b,
            "E".into(),
            props(vec![("dist", PropValue::Float(0.5))]),
        );
        g.create_edge(
            b,
            c,
            "E".into(),
            props(vec![("dist", PropValue::Float(0.5))]),
        );
        // Creating c evicted a and b's properties (and their edges').
        let (cost, _) = g
            .dijkstra(a, c, super::Direction::Outgoing, "dist")
            .unwrap();
        assert!(
            (cost - 1.0).abs() < 1e-9,
            "dijkstra read weight 1.0 for an evicted edge, got {cost}"
        );
    }
}
