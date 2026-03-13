"""Graph model — wraps Nucleus GRAPH_* SQL functions."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features


class Node(BaseModel):
    id: str
    labels: list[str] = []
    properties: dict[str, Any] = {}


class Edge(BaseModel):
    id: str
    type: str
    from_id: str
    to_id: str
    properties: dict[str, Any] = {}


class GraphResult(BaseModel):
    nodes: list[Node] = []
    edges: list[Edge] = []


class GraphModel:
    """Graph database operations over Nucleus (Cypher + programmatic API).

    Usage::

        node_id = await db.graph.add_node(["Person"], {"name": "Alice"})
        await db.graph.add_edge("KNOWS", node_id, other_id)
        result = await db.graph.query("MATCH (n:Person) RETURN n")
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "Graph")

    async def add_node(
        self, labels: list[str], properties: dict[str, Any] | None = None
    ) -> str:
        """Add a node with labels and properties. Returns the node ID."""
        self._require()
        label = labels[0] if labels else "Node"
        props_json = json.dumps(properties) if properties else None
        if props_json:
            node_id = await self._exec.fetchval(
                "SELECT GRAPH_ADD_NODE($1, $2)", label, props_json
            )
        else:
            node_id = await self._exec.fetchval(
                "SELECT GRAPH_ADD_NODE($1)", label
            )
        return str(node_id)

    async def add_edge(
        self,
        edge_type: str,
        from_id: str,
        to_id: str,
        properties: dict[str, Any] | None = None,
    ) -> str:
        """Add an edge between two nodes. Returns the edge ID."""
        self._require()
        props_json = json.dumps(properties) if properties else None
        if props_json:
            edge_id = await self._exec.fetchval(
                "SELECT GRAPH_ADD_EDGE($1, $2, $3, $4)",
                int(from_id),
                int(to_id),
                edge_type,
                props_json,
            )
        else:
            edge_id = await self._exec.fetchval(
                "SELECT GRAPH_ADD_EDGE($1, $2, $3)",
                int(from_id),
                int(to_id),
                edge_type,
            )
        return str(edge_id)

    async def delete_node(self, node_id: str) -> bool:
        """Delete a node by ID."""
        self._require()
        return await self._exec.fetchval(
            "SELECT GRAPH_DELETE_NODE($1)", int(node_id)
        )

    async def delete_edge(self, edge_id: str) -> bool:
        """Delete an edge by ID."""
        self._require()
        return await self._exec.fetchval(
            "SELECT GRAPH_DELETE_EDGE($1)", int(edge_id)
        )

    async def query(
        self, cypher: str, params: dict[str, Any] | None = None
    ) -> GraphResult:
        """Execute a Cypher query."""
        self._require()
        raw = await self._exec.fetchval("SELECT GRAPH_QUERY($1)", cypher)
        if not raw:
            return GraphResult()
        data = json.loads(raw)
        return GraphResult(
            nodes=data.get("nodes", []),
            edges=data.get("edges", []),
        )

    async def neighbors(
        self,
        node_id: str,
        edge_type: str | None = None,
        direction: str = "both",
    ) -> list[Node]:
        """Get neighboring nodes, optionally filtered by edge type."""
        self._require()
        if edge_type:
            # Use Cypher to filter by edge type
            nid = int(node_id)
            if direction == "out":
                cypher = f"MATCH (n)-[r:{edge_type}]->(m) WHERE id(n) = {nid} RETURN m"
            elif direction == "in":
                cypher = f"MATCH (n)<-[r:{edge_type}]-(m) WHERE id(n) = {nid} RETURN m"
            else:
                cypher = f"MATCH (n)-[r:{edge_type}]-(m) WHERE id(n) = {nid} RETURN m"
            raw = await self._exec.fetchval("SELECT GRAPH_QUERY($1)", cypher)
            if not raw:
                return []
            data = json.loads(raw)
            rows = data.get("rows", [])
            nodes: list[Node] = []
            for row in rows:
                if row:
                    item = row[0] if isinstance(row, list) else row
                    if isinstance(item, dict):
                        nodes.append(
                            Node(
                                id=str(item.get("id", "")),
                                properties={k: v for k, v in item.items() if k != "id"},
                            )
                        )
            return nodes
        # No edge_type filter — use GRAPH_NEIGHBORS for efficiency
        raw = await self._exec.fetchval(
            "SELECT GRAPH_NEIGHBORS($1, $2)", int(node_id), direction
        )
        if not raw:
            return []
        data = json.loads(raw)
        return [Node(id=str(n.get("id", "")), properties=n) for n in data]

    async def shortest_path(
        self, from_id: str, to_id: str, max_depth: int = 10
    ) -> list[Node]:
        """Find shortest path between two nodes."""
        self._require()
        raw = await self._exec.fetchval(
            "SELECT GRAPH_SHORTEST_PATH($1, $2)", int(from_id), int(to_id)
        )
        if not raw:
            return []
        ids = json.loads(raw)
        return [Node(id=str(nid)) for nid in ids]

    async def node_count(self) -> int:
        """Count all nodes."""
        self._require()
        return await self._exec.fetchval("SELECT GRAPH_NODE_COUNT()")

    async def edge_count(self) -> int:
        """Count all edges."""
        self._require()
        return await self._exec.fetchval("SELECT GRAPH_EDGE_COUNT()")
