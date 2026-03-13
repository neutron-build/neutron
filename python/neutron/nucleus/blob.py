"""Blob storage model — wraps Nucleus BLOB_* SQL functions."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from pydantic import BaseModel

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features


class BlobMeta(BaseModel):
    key: str
    size: int = 0
    content_type: str | None = None
    created_at: datetime | None = None
    metadata: dict[str, str] = {}


class BlobModel:
    """Binary object storage over Nucleus.

    Usage::

        await db.blob.put("uploads", "photo.jpg", data, content_type="image/jpeg")
        data = await db.blob.get("uploads", "photo.jpg")
        await db.blob.delete("uploads", "photo.jpg")
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "Blob")

    async def put(
        self,
        bucket: str,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> None:
        """Store a blob."""
        self._require()
        full_key = f"{bucket}/{key}"
        data_hex = data.hex()
        if content_type:
            await self._exec.fetchval(
                "SELECT BLOB_STORE($1, $2, $3)", full_key, data_hex, content_type
            )
        else:
            await self._exec.fetchval(
                "SELECT BLOB_STORE($1, $2)", full_key, data_hex
            )

        # Apply metadata tags
        if metadata:
            for tag_key, tag_value in metadata.items():
                await self._exec.fetchval(
                    "SELECT BLOB_TAG($1, $2, $3)", full_key, tag_key, tag_value
                )

    async def get(self, bucket: str, key: str) -> bytes | None:
        """Retrieve a blob as bytes."""
        self._require()
        full_key = f"{bucket}/{key}"
        raw = await self._exec.fetchval("SELECT BLOB_GET($1)", full_key)
        if raw is None:
            return None
        return bytes.fromhex(raw)

    async def get_meta(self, bucket: str, key: str) -> BlobMeta | None:
        """Get blob metadata without downloading data."""
        self._require()
        full_key = f"{bucket}/{key}"
        raw = await self._exec.fetchval("SELECT BLOB_META($1)", full_key)
        if raw is None:
            return None
        meta = json.loads(raw)
        return BlobMeta(
            key=key,
            size=meta.get("size", 0),
            content_type=meta.get("content_type"),
            metadata=meta.get("tags", {}),
        )

    async def delete(self, bucket: str, key: str) -> bool:
        """Delete a blob."""
        self._require()
        full_key = f"{bucket}/{key}"
        return await self._exec.fetchval("SELECT BLOB_DELETE($1)", full_key)

    async def exists(self, bucket: str, key: str) -> bool:
        """Check if a blob exists."""
        self._require()
        full_key = f"{bucket}/{key}"
        meta = await self._exec.fetchval("SELECT BLOB_META($1)", full_key)
        return meta is not None

    async def list(
        self, bucket: str, prefix: str = ""
    ) -> list[BlobMeta]:
        """List blobs in a bucket."""
        self._require()
        full_prefix = f"{bucket}/{prefix}" if prefix else bucket
        raw = await self._exec.fetchval("SELECT BLOB_LIST($1)", full_prefix)
        if not raw:
            return []
        items = json.loads(raw)
        results: list[BlobMeta] = []
        for item in items:
            if isinstance(item, str):
                results.append(BlobMeta(key=item))
            elif isinstance(item, dict):
                results.append(
                    BlobMeta(
                        key=item.get("key", ""),
                        size=item.get("size", 0),
                        content_type=item.get("content_type"),
                    )
                )
        return results

    async def count(self) -> int:
        """Count all blobs."""
        self._require()
        return await self._exec.fetchval("SELECT BLOB_COUNT()")

    async def dedup_ratio(self) -> float:
        """Get deduplication ratio."""
        self._require()
        return await self._exec.fetchval("SELECT BLOB_DEDUP_RATIO()")

    async def copy(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
    ) -> None:
        """Copy a blob from one location to another."""
        self._require()
        data = await self.get(src_bucket, src_key)
        if data is None:
            raise KeyError(f"Source blob not found: {src_bucket}/{src_key}")
        meta = await self.get_meta(src_bucket, src_key)
        content_type = meta.content_type if meta else None
        await self.put(dst_bucket, dst_key, data, content_type=content_type)
