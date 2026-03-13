"""NucleusClient — asyncpg pool with auto-detection of Nucleus vs plain PostgreSQL."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import asyncpg

from neutron.nucleus.sql import SQLModel


@dataclass
class Features:
    """Detected capabilities of the connected database."""

    is_nucleus: bool = False
    version: str | None = None
    has_kv: bool = False
    has_vector: bool = False
    has_ts: bool = False
    has_document: bool = False
    has_graph: bool = False
    has_fts: bool = False
    has_geo: bool = False
    has_blob: bool = False


class NucleusClient:
    """Multi-model database client backed by asyncpg.

    Usage::

        db = await NucleusClient.connect("postgres://localhost/mydb")
        users = await db.sql.query(User, "SELECT * FROM users")
        await db.kv.set("key", "value")
        await db.close()
    """

    def __init__(self, pool: asyncpg.Pool, features: Features) -> None:
        from neutron.nucleus._exec import Executor
        from neutron.nucleus.blob import BlobModel
        from neutron.nucleus.cdc import CDCModel
        from neutron.nucleus.columnar import ColumnarModel
        from neutron.nucleus.datalog import DatalogModel
        from neutron.nucleus.document import DocumentModel
        from neutron.nucleus.fts import FTSModel
        from neutron.nucleus.geo import GeoModel
        from neutron.nucleus.graph import GraphModel
        from neutron.nucleus.kv import KVModel
        from neutron.nucleus.pubsub import PubSubModel
        from neutron.nucleus.streams import StreamsModel
        from neutron.nucleus.timeseries import TimeSeriesModel
        from neutron.nucleus.vector import VectorModel

        self._pool = pool
        self.features = features
        self._exec = Executor(pool)
        self._sql = SQLModel(pool)
        self._kv = KVModel(self._exec, features)
        self._vector = VectorModel(self._exec, features)
        self._timeseries = TimeSeriesModel(self._exec, features)
        self._document = DocumentModel(self._exec, features)
        self._graph = GraphModel(self._exec, features)
        self._fts = FTSModel(self._exec, features)
        self._geo = GeoModel(self._exec, features)
        self._blob = BlobModel(self._exec, features)
        self._pubsub = PubSubModel(pool, self._exec, features)
        self._streams = StreamsModel(self._exec, features)
        self._columnar = ColumnarModel(self._exec, features)
        self._datalog = DatalogModel(self._exec, features)
        self._cdc = CDCModel(self._exec, features)

    @classmethod
    async def connect(
        cls,
        url: str,
        *,
        min_size: int = 5,
        max_size: int = 25,
    ) -> NucleusClient:
        """Connect and auto-detect Nucleus vs plain PostgreSQL."""
        pool = await asyncpg.create_pool(url, min_size=min_size, max_size=max_size)
        features = await _detect_features(pool)
        return cls(pool, features)

    @property
    def pool(self) -> asyncpg.Pool:
        return self._pool

    @property
    def sql(self) -> SQLModel:
        return self._sql

    @property
    def kv(self) -> KVModel:
        return self._kv

    @property
    def vector(self) -> VectorModel:
        return self._vector

    @property
    def timeseries(self) -> TimeSeriesModel:
        return self._timeseries

    @property
    def document(self) -> DocumentModel:
        return self._document

    @property
    def graph(self) -> GraphModel:
        return self._graph

    @property
    def fts(self) -> FTSModel:
        return self._fts

    @property
    def geo(self) -> GeoModel:
        return self._geo

    @property
    def blob(self) -> BlobModel:
        return self._blob

    @property
    def pubsub(self) -> PubSubModel:
        return self._pubsub

    @property
    def streams(self) -> StreamsModel:
        return self._streams

    @property
    def columnar(self) -> ColumnarModel:
        return self._columnar

    @property
    def datalog(self) -> DatalogModel:
        return self._datalog

    @property
    def cdc(self) -> CDCModel:
        return self._cdc

    def transaction(self) -> Transaction:
        """Cross-model transaction context manager."""
        from neutron.nucleus.tx import Transaction

        return Transaction(self._pool, self.features)

    async def close(self) -> None:
        """Close the connection pool."""
        await self._pool.close()

    async def migrate(self, migrations_dir: str) -> list[str]:
        """Run pending migrations from a directory."""
        from neutron.nucleus.migrate import Migrator

        migrator = Migrator(self._pool)
        return await migrator.migrate(migrations_dir)

    async def migrate_sql(self, migrations: list) -> list[str]:
        """Run programmatic migrations."""
        from neutron.nucleus.migrate import Migrator

        migrator = Migrator(self._pool)
        return await migrator.run_migrations(migrations)

    async def migration_status(self) -> set[int]:
        """Get applied migration versions."""
        from neutron.nucleus.migrate import Migrator

        migrator = Migrator(self._pool)
        return await migrator.get_applied()


async def _detect_features(pool: asyncpg.Pool) -> Features:
    """Detect Nucleus vs plain PostgreSQL by parsing SELECT VERSION()."""
    async with pool.acquire() as conn:
        version_string: str = await conn.fetchval("SELECT VERSION()")

    is_nucleus = "Nucleus" in version_string
    version = None
    if is_nucleus:
        m = re.search(r"Nucleus (\S+)", version_string)
        if m:
            version = m.group(1)

    return Features(
        is_nucleus=is_nucleus,
        version=version,
        has_kv=is_nucleus,
        has_vector=is_nucleus,
        has_ts=is_nucleus,
        has_document=is_nucleus,
        has_graph=is_nucleus,
        has_fts=is_nucleus,
        has_geo=is_nucleus,
        has_blob=is_nucleus,
    )
