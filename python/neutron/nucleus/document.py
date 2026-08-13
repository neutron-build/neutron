"""Document model — wraps Nucleus DOC_* SQL functions."""

from __future__ import annotations

import json
from typing import Any, TypeVar, cast

from pydantic import BaseModel

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features

T = TypeVar("T", bound=BaseModel)


def _doc_id(doc_id: int) -> str:
    """Render a document id the way the engine expects it over pgwire.

    Nucleus reports a parameter whose type it cannot infer as TEXT, and a
    driver then refuses to bind an integer to it. The engine parses a
    text-encoded integer id for exactly this reason, so sending the digits is
    the supported encoding, not a workaround.
    """
    return str(doc_id)


def _decode_path_value(raw: object) -> Any:
    """Return the VALUE at a document path, not its JSON encoding.

    DOC_PATH hands back raw JSON, so a stored string arrived as `'"ada"'` and
    every caller had to json.loads it — while `get`/`get_in` on the same client
    return a decoded dict. Two shapes for the same idea in one API is drift, and
    the live conformance spec asserts the decoded form for all seven SDKs.

    A value that is not valid JSON is returned untouched rather than raising:
    the engine is the only producer here, but a client that turns a readable
    value into an exception is worse than one that passes it through.
    """
    if raw is None or not isinstance(raw, str):
        return raw
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return raw


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

    Collections are enforced by the ENGINE. This client used to implement them
    itself, by writing a reserved ``_collection`` key into the document and
    filtering on it — which collided with any user field of that name, leaked
    the key back out of ``get``, and left ``get``/``count`` unscoped so they
    reported across every collection. It also disagreed with the other SDKs
    about what a collection is: a document written here was invisible to a Go
    or TypeScript caller naming the same collection.

    **Migration:** documents written by an older version of this client carry
    ``_collection`` in their body and live in the engine's default collection.
    They are not visible to ``find("<name>", ...)`` after this change. Re-insert
    them under the collection they belong to, or read them back with the
    default collection and the key still present.
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "Document")

    async def insert(
        self, collection: str, doc: dict[str, Any] | BaseModel
    ) -> str:
        """Insert a document into ``collection``, return its ID."""
        self._require()
        if isinstance(doc, BaseModel):
            doc_json = doc.model_dump_json()
        else:
            doc_json = json.dumps(doc)

        # The one-argument form when no collection is named, so this still
        # works against a server that predates collections.
        if collection:
            doc_id = await self._exec.fetchval(
                "SELECT DOC_INSERT($1, $2)", collection, doc_json
            )
        else:
            doc_id = await self._exec.fetchval("SELECT DOC_INSERT($1)", doc_json)
        return str(doc_id)

    async def _raw_doc(self, collection: str, doc_id: int) -> str | None:
        """Fetch a document's JSON text, scoped to a collection."""
        if collection:
            return cast(
                "str | None",
                await self._exec.fetchval(
                    "SELECT DOC_GET($1, $2)", collection, _doc_id(doc_id)
                ),
            )
        return cast(
            "str | None",
            await self._exec.fetchval("SELECT DOC_GET($1)", _doc_id(doc_id)),
        )

    async def _find_with_ids(
        self,
        collection: str,
        filter: dict[str, Any],
        *,
        limit: int = 100,
        skip: int = 0,
    ) -> list[tuple[int, dict[str, Any]]]:
        """Return (id, doc) pairs for documents matching the filter."""
        query = json.dumps(filter)
        if collection:
            raw = await self._exec.fetchval(
                "SELECT DOC_QUERY($1, $2)", collection, query
            )
        else:
            raw = await self._exec.fetchval("SELECT DOC_QUERY($1)", query)
        ids = _parse_ids(raw)
        ids = ids[skip : skip + limit]

        results: list[tuple[int, dict[str, Any]]] = []
        for doc_id in ids:
            doc_json = await self._raw_doc(collection, doc_id)
            if doc_json is not None:
                results.append((doc_id, json.loads(doc_json)))
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
        """Get a document by ID from the default collection."""
        return await self.get_in("", doc_id)

    async def get_in(self, collection: str, doc_id: int) -> dict[str, Any] | None:
        """Get a document by ID from ``collection``.

        A document in another collection reads as absent — holding an id is not
        enough to read across a collection boundary.
        """
        self._require()
        raw = await self._raw_doc(collection, doc_id)
        if raw is None:
            return None
        return cast("dict[str, Any] | None", json.loads(raw))

    async def get_path(self, doc_id: int, *keys: str) -> Any:
        """Extract a value at a nested path from a default-collection document."""
        return await self.get_path_in("", doc_id, *keys)

    async def get_path_in(self, collection: str, doc_id: int, *keys: str) -> Any:
        """Extract a value at a nested path from a document in ``collection``.

        The scoped form is a distinct FUNCTION rather than an extra argument:
        the key tail is variadic, so a leading collection could not be told
        apart from a leading id.
        """
        self._require()
        if not keys:
            # Sending this built `DOC_PATH($1, )` — a malformed statement whose
            # error named nothing useful.
            raise ValueError("document path requires at least one key")
        base = 3 if collection else 2
        placeholders = ", ".join(f"${i + base}" for i in range(len(keys)))
        if collection:
            raw = await self._exec.fetchval(
                f"SELECT DOC_PATH_IN($1, $2, {placeholders})",
                collection,
                _doc_id(doc_id),
                *keys,
            )
        else:
            raw = await self._exec.fetchval(
                f"SELECT DOC_PATH($1, {placeholders})", _doc_id(doc_id), *keys
            )
        return _decode_path_value(raw)

    async def count(self) -> int:
        """Count documents in the default collection."""
        return await self.count_in("")

    async def count_in(self, collection: str) -> int:
        """Count documents in ``collection``."""
        self._require()
        if collection:
            return cast(
                "int", await self._exec.fetchval("SELECT DOC_COUNT($1)", collection)
            )
        return cast("int", await self._exec.fetchval("SELECT DOC_COUNT()"))

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
            body = json.dumps(merged)
            if collection:
                ok = await self._exec.fetchval(
                    "SELECT DOC_UPDATE($1, $2, $3)",
                    collection,
                    _doc_id(doc_id),
                    body,
                )
            else:
                ok = await self._exec.fetchval(
                    "SELECT DOC_UPDATE($1, $2)", _doc_id(doc_id), body
                )
            if ok:
                count += 1
        return count

    async def delete(
        self, collection: str, filter: dict[str, Any]
    ) -> int:
        """Delete matching documents. Returns count of deleted docs."""
        self._require()
        query = json.dumps(filter)
        if collection:
            raw = await self._exec.fetchval(
                "SELECT DOC_QUERY($1, $2)", collection, query
            )
        else:
            raw = await self._exec.fetchval("SELECT DOC_QUERY($1)", query)
        ids = _parse_ids(raw)
        deleted = 0
        for doc_id in ids:
            if collection:
                ok = await self._exec.fetchval(
                    "SELECT DOC_DELETE($1, $2)", collection, _doc_id(doc_id)
                )
            else:
                ok = await self._exec.fetchval(
                    "SELECT DOC_DELETE($1)", _doc_id(doc_id)
                )
            if ok:
                deleted += 1
        return deleted
