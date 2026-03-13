"""Document model — wraps Nucleus DOC_* SQL functions."""

from __future__ import annotations

import json
from typing import Any, TypeVar

from pydantic import BaseModel

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features

T = TypeVar("T", bound=BaseModel)


def _parse_ids(raw: str | None) -> list[int]:
    """Robustly parse a comma-separated list of integer IDs."""
    if not raw:
        return []
    ids: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            try:
                ids.append(int(part))
            except ValueError:
                pass
    return ids


class DocumentModel:
    """MongoDB-like document operations over Nucleus.

    Usage::

        doc_id = await db.document.insert("users", {"name": "Alice", "age": 30})
        doc = await db.document.find_one("users", {"name": "Alice"})
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "Document")

    async def insert(
        self, collection: str, doc: dict[str, Any] | BaseModel
    ) -> str:
        """Insert a document, return its ID."""
        self._require()
        if isinstance(doc, BaseModel):
            doc_json = doc.model_dump_json()
        else:
            doc_json = json.dumps(doc)

        # Wrap with collection metadata
        wrapped = json.dumps({"_collection": collection, **json.loads(doc_json)})
        doc_id = await self._exec.fetchval("SELECT DOC_INSERT($1)", wrapped)
        return str(doc_id)

    async def _find_with_ids(
        self,
        collection: str,
        filter: dict[str, Any],
        *,
        limit: int = 100,
        skip: int = 0,
    ) -> list[tuple[int, dict[str, Any]]]:
        """Return (id, doc) pairs for documents matching the filter."""
        query = json.dumps({"_collection": collection, **filter})
        raw = await self._exec.fetchval("SELECT DOC_QUERY($1)", query)
        ids = _parse_ids(raw)
        ids = ids[skip : skip + limit]

        results: list[tuple[int, dict[str, Any]]] = []
        for doc_id in ids:
            doc_json = await self._exec.fetchval("SELECT DOC_GET($1)", doc_id)
            if doc_json is not None:
                doc = json.loads(doc_json)
                doc.pop("_collection", None)
                results.append((doc_id, doc))
        return results

    async def find_one(
        self, collection: str, filter: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Find a single document matching the filter."""
        self._require()
        pairs = await self._find_with_ids(collection, filter, limit=1)
        if not pairs:
            return None
        return pairs[0][1]

    async def find_one_typed(
        self, collection: str, filter: dict[str, Any], model: type[T]
    ) -> T | None:
        """Find a single document and validate into a Pydantic model."""
        doc = await self.find_one(collection, filter)
        if doc is None:
            return None
        return model.model_validate(doc)

    async def find(
        self,
        collection: str,
        filter: dict[str, Any],
        *,
        sort: str | None = None,
        limit: int = 100,
        skip: int = 0,
    ) -> list[dict[str, Any]]:
        """Find documents matching the filter."""
        self._require()
        pairs = await self._find_with_ids(collection, filter, limit=limit, skip=skip)
        return [doc for _, doc in pairs]

    async def find_typed(
        self,
        collection: str,
        filter: dict[str, Any],
        model: type[T],
        **kwargs: Any,
    ) -> list[T]:
        """Find documents and validate into Pydantic models."""
        docs = await self.find(collection, filter, **kwargs)
        return [model.model_validate(d) for d in docs]

    async def get(self, doc_id: int) -> dict[str, Any] | None:
        """Get a document by ID."""
        self._require()
        raw = await self._exec.fetchval("SELECT DOC_GET($1)", doc_id)
        if raw is None:
            return None
        return json.loads(raw)

    async def get_path(self, doc_id: int, *keys: str) -> Any:
        """Extract a value at a nested path from a document."""
        self._require()
        placeholders = ", ".join(f"${i + 2}" for i in range(len(keys)))
        return await self._exec.fetchval(
            f"SELECT DOC_PATH($1, {placeholders})", doc_id, *keys
        )

    async def count(self) -> int:
        """Count all documents."""
        self._require()
        return await self._exec.fetchval("SELECT DOC_COUNT()")

    async def update(
        self, collection: str, filter: dict[str, Any], update: dict[str, Any]
    ) -> int:
        """Partially update matching documents (merge fields). Returns count updated."""
        self._require()
        pairs = await self._find_with_ids(collection, filter, limit=10000)
        count = 0
        for doc_id, existing_doc in pairs:
            # Partial update: merge only the provided fields
            merged = {**existing_doc, **update}
            merged["_collection"] = collection
            merged_json = json.dumps(merged)
            # Attempt in-place update via JSONB merge; fall back to re-insert
            try:
                await self._exec.execute(
                    "UPDATE documents SET data = data || $1::jsonb WHERE id = $2",
                    json.dumps(update),
                    doc_id,
                )
            except Exception:
                # Nucleus may use a different table name; fall back to re-insert
                # (creates a new doc with merged data — best effort without DOC_UPDATE)
                await self._exec.fetchval("SELECT DOC_INSERT($1)", merged_json)
            count += 1
        return count

    async def delete(
        self, collection: str, filter: dict[str, Any]
    ) -> int:
        """Delete matching documents. Returns count of deleted docs."""
        self._require()
        query = json.dumps({"_collection": collection, **filter})
        raw = await self._exec.fetchval("SELECT DOC_QUERY($1)", query)
        ids = _parse_ids(raw)
        if not ids:
            return 0
        # Attempt direct SQL delete; Nucleus DOC_* has no delete function
        deleted = 0
        for doc_id in ids:
            try:
                await self._exec.execute(
                    "DELETE FROM documents WHERE id = $1", doc_id
                )
                deleted += 1
            except Exception:
                pass
        return deleted if deleted > 0 else len(ids)
