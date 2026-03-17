"""Vector model — wraps Nucleus VECTOR_* SQL functions."""

from __future__ import annotations

import json
import re
from typing import Any

from pydantic import BaseModel

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features

VALID_METRICS = ("cosine", "l2", "inner")

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class VectorResult(BaseModel):
    id: str
    score: float
    metadata: dict[str, Any] = {}


class VectorModel:
    """Vector similarity search over Nucleus.

    Usage::

        await db.vector.create_collection("articles", dimension=1536)
        await db.vector.insert("articles", "doc-1", embedding, {"title": "..."})
        results = await db.vector.search("articles", query_vec, k=10)
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "Vector")

    async def create_collection(
        self,
        name: str,
        dimension: int,
        metric: str = "cosine",
    ) -> None:
        """Create a vector collection (table + index)."""
        self._require()
        if metric not in VALID_METRICS:
            raise ValueError(
                f"Invalid distance metric: {metric}. "
                f"Must be one of: {', '.join(VALID_METRICS)}"
            )
        safe_name = _safe(name)
        await self._exec.execute(
            f"CREATE TABLE IF NOT EXISTS {safe_name} ("
            f"  id TEXT PRIMARY KEY,"
            f"  embedding VECTOR,"
            f"  metadata JSONB DEFAULT '{{}}'"
            f")"
        )
        await self._exec.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{safe_name}_vec "
            f"ON {safe_name} USING VECTOR (embedding) "
            f"WITH (metric = '{metric}')"
        )

    async def insert(
        self,
        collection: str,
        id: str,
        vector: list[float],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Insert or upsert a vector."""
        self._require()
        vec_json = json.dumps(vector)
        meta_json = json.dumps(metadata or {})
        await self._exec.execute(
            f"INSERT INTO {_safe(collection)} (id, embedding, metadata) "
            f"VALUES ($1, VECTOR($2), $3::jsonb) "
            f"ON CONFLICT (id) DO UPDATE SET embedding = VECTOR($2), metadata = $3::jsonb",
            id,
            vec_json,
            meta_json,
        )

    async def search(
        self,
        collection: str,
        query: list[float],
        *,
        k: int = 10,
        metric: str = "cosine",
        filter: dict[str, Any] | None = None,
    ) -> list[VectorResult]:
        """Search for nearest neighbors."""
        self._require()
        if metric not in VALID_METRICS:
            raise ValueError(
                f"Invalid distance metric: {metric}. "
                f"Must be one of: {', '.join(VALID_METRICS)}"
            )
        vec_json = json.dumps(query)

        # Build parameterized filter clauses to prevent SQL injection
        args: list[Any] = [vec_json, metric]
        where = ""
        if filter:
            clauses = []
            for fk, fv in filter.items():
                if not _IDENTIFIER_RE.match(fk):
                    raise ValueError(
                        f"Invalid metadata filter key: {fk!r}. "
                        f"Keys must be valid identifiers (letters, digits, underscores)."
                    )
                args.append(str(fv))
                clauses.append(f"metadata->>'{fk}' = ${len(args)}")
            where = "WHERE " + " AND ".join(clauses)

        args.append(k)
        rows = await self._exec.fetch(
            f"SELECT id, "
            f"VECTOR_DISTANCE(embedding, VECTOR($1), $2) AS score, "
            f"metadata "
            f"FROM {_safe(collection)} {where} "
            f"ORDER BY score LIMIT ${len(args)}",
            *args,
        )
        results = []
        for row in rows:
            meta = row["metadata"]
            if isinstance(meta, str):
                meta = json.loads(meta)
            results.append(
                VectorResult(id=row["id"], score=row["score"], metadata=meta or {})
            )
        return results

    async def delete(self, collection: str, id: str) -> None:
        """Delete a vector by ID."""
        self._require()
        await self._exec.execute(
            f"DELETE FROM {_safe(collection)} WHERE id = $1", id
        )

    async def count(self, collection: str) -> int:
        """Count vectors in a collection."""
        self._require()
        return await self._exec.fetchval(
            f"SELECT COUNT(*) FROM {_safe(collection)}"
        )


def _safe(name: str) -> str:
    """Basic identifier sanitization."""
    return "".join(c for c in name if c.isalnum() or c == "_")
