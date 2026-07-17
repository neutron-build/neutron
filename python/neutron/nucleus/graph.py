"""Graph model — wraps Nucleus GRAPH_* SQL functions."""

from __future__ import annotations

import json
from typing import Any, cast

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
    columns: list[str] = []
    rows: list[list[Any]] = []


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
        return cast(
            "bool",
            await self._exec.fetchval(
                "SELECT GRAPH_DELETE_NODE($1)", int(node_id)
            )
        )

    async def delete_edge(self, edge_id: str) -> bool:
        """Delete an edge by ID."""
        self._require()
        return cast(
            "bool",
            await self._exec.fetchval(
                "SELECT GRAPH_DELETE_EDGE($1)", int(edge_id)
            )
        )

    async def query(
        self, cypher: str, params: dict[str, Any] | None = None
    ) -> GraphResult:
        """Execute a Cypher query.

        The engine returns ``{"columns": [...], "rows": [[...], ...]}`` with
        positional row values.
        """
        self._require()
        raw = await self._exec.fetchval("SELECT GRAPH_QUERY($1)", cypher)
        if not raw:
            return GraphResult()
        data = json.loads(raw)
        return GraphResult(
            columns=data.get("columns", []),
            rows=data.get("rows", []),
        )

    async def neighbors(
        self,
        node_id: str,
        edge_type: str | None = None,
        direction: str = "both",
    ) -> list[Node]:
        """Get neighboring nodes, optionally filtered by edge type.

        The engine returns ``[{"neighbor_id": N, "edge_id": E, "edge_type": "T"}]``;
        the ``edge_type`` filter is applied client-side.
        """
        self._require()
        if direction not in ("in", "out", "both"):
            raise ValueError(
                f"Invalid direction: {direction!r}. Must be 'in', 'out', or 'both'."
            )
        raw = await self._exec.fetchval(
            "SELECT GRAPH_NEIGHBORS($1, $2)", int(node_id), direction
        )
        if not raw:
            return []
        data = json.loads(raw)
        nodes: list[Node] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            if edge_type is not None and item.get("edge_type") != edge_type:
                continue
            nodes.append(Node(id=str(item.get("neighbor_id", ""))))
        return nodes

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
        return cast("int", await self._exec.fetchval("SELECT GRAPH_NODE_COUNT()"))

    async def edge_count(self) -> int:
        """Count all edges."""
        self._require()
        return cast("int", await self._exec.fetchval("SELECT GRAPH_EDGE_COUNT()"))
