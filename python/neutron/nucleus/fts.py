"""Full-Text Search model — wraps Nucleus FTS_* SQL functions."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features


class FTSResult(BaseModel):
    id: str
    score: float
    highlights: dict[str, str] = {}


class FTSModel:
    """Full-text search operations over Nucleus.

    Usage::

        await db.fts.index_doc("articles", "doc-1", {"title": "Hello", "body": "..."})
        results = await db.fts.search("articles", "hello world", limit=10)
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "FTS")

    async def create_index(
        self,
        index: str,
        *,
        language: str = "english",
    ) -> None:
        """Create (or ensure) a full-text search index.

        Creates the backing SQL index used by FTS_INDEX/FTS_SEARCH on the
        ``_fts_docs`` table.  Safe to call multiple times (IF NOT EXISTS).
        """
        self._require()
        safe_index = "".join(c for c in index if c.isalnum() or c == "_")
        await self._exec.execute(
            f"CREATE INDEX IF NOT EXISTS fts_{safe_index}_idx "
            f"ON _fts_docs USING gin(to_tsvector($1, body))",
            language,
        )

    async def index_doc(
        self, index: str, id: str, fields: dict[str, str]
    ) -> None:
        """Index a document's text fields."""
        self._require()
        # Combine all fields into one text blob for FTS_INDEX
        text = " ".join(fields.values())
        doc_id = int(id) if id.isdigit() else hash(id) & 0x7FFFFFFF
        await self._exec.fetchval("SELECT FTS_INDEX($1, $2)", doc_id, text)

    async def search(
        self,
        index: str,
        query: str,
        *,
        limit: int = 10,
        highlight: bool = False,
        fuzzy: int = 0,
    ) -> list[FTSResult]:
        """Search the full-text index."""
        self._require()
        if fuzzy > 0:
            raw = await self._exec.fetchval(
                "SELECT FTS_FUZZY_SEARCH($1, $2, $3)", query, fuzzy, limit
            )
        else:
            raw = await self._exec.fetchval(
                "SELECT FTS_SEARCH($1, $2)", query, limit
            )
        if not raw:
            return []
        data = json.loads(raw)
        return [
            FTSResult(
                id=str(item.get("doc_id", "")),
                score=float(item.get("score", 0.0)),
            )
            for item in data
        ]

    async def delete_doc(self, index: str, id: str) -> None:
        """Remove a document from the index."""
        self._require()
        doc_id = int(id) if id.isdigit() else hash(id) & 0x7FFFFFFF
        await self._exec.fetchval("SELECT FTS_REMOVE($1)", doc_id)

    async def doc_count(self) -> int:
        """Count indexed documents."""
        self._require()
        return await self._exec.fetchval("SELECT FTS_DOC_COUNT()")

    async def term_count(self) -> int:
        """Count unique indexed terms."""
        self._require()
        return await self._exec.fetchval("SELECT FTS_TERM_COUNT()")
