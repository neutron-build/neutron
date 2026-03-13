"""Nucleus database client — multi-model access over asyncpg."""

from neutron.nucleus.blob import BlobMeta, BlobModel
from neutron.nucleus.cdc import CDCEvent, CDCModel
from neutron.nucleus.client import Features, NucleusClient
from neutron.nucleus.columnar import ColumnarModel
from neutron.nucleus.datalog import DatalogModel
from neutron.nucleus.document import DocumentModel
from neutron.nucleus.fts import FTSModel, FTSResult
from neutron.nucleus.geo import GeoFeature, GeoModel
from neutron.nucleus.graph import Edge, GraphModel, GraphResult, Node
from neutron.nucleus.kv import KVModel
from neutron.nucleus.pubsub import PubSubModel
from neutron.nucleus.sql import SQLModel
from neutron.nucleus.streams import StreamEntry, StreamsModel
from neutron.nucleus.timeseries import TimeSeriesModel, TimeSeriesPoint
from neutron.nucleus.vector import VectorModel, VectorResult

__all__ = [
    "NucleusClient",
    "Features",
    "SQLModel",
    "KVModel",
    "VectorModel",
    "VectorResult",
    "TimeSeriesModel",
    "TimeSeriesPoint",
    "DocumentModel",
    "GraphModel",
    "GraphResult",
    "Node",
    "Edge",
    "FTSModel",
    "FTSResult",
    "GeoModel",
    "GeoFeature",
    "BlobModel",
    "BlobMeta",
    "PubSubModel",
    "StreamsModel",
    "StreamEntry",
    "ColumnarModel",
    "DatalogModel",
    "CDCModel",
    "CDCEvent",
]
