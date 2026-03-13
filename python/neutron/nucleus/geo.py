"""Geospatial model — wraps Nucleus GEO_*/ST_* SQL functions."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel

from neutron.nucleus._exec import Executor, require_nucleus
from neutron.nucleus.client import Features


class GeoFeature(BaseModel):
    id: str
    lat: float
    lon: float
    properties: dict[str, Any] = {}


class GeoModel:
    """Geospatial operations over Nucleus.

    Usage::

        await db.geo.insert("shops", GeoFeature(id="s1", lat=40.7, lon=-74.0))
        nearby = await db.geo.nearest("shops", 40.7, -74.0, 1000)
    """

    def __init__(self, executor: Executor, features: Features) -> None:
        self._exec = executor
        self._features = features

    def _require(self) -> None:
        require_nucleus(self._features, "Geo")

    async def distance(
        self, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> float:
        """Calculate distance in meters between two points (haversine)."""
        self._require()
        return await self._exec.fetchval(
            "SELECT GEO_DISTANCE($1, $2, $3, $4)", lat1, lon1, lat2, lon2
        )

    async def distance_euclidean(
        self, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> float:
        """Calculate Euclidean distance between two points."""
        self._require()
        return await self._exec.fetchval(
            "SELECT GEO_DISTANCE_EUCLIDEAN($1, $2, $3, $4)",
            lat1, lon1, lat2, lon2,
        )

    async def within(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
        radius_m: float,
    ) -> bool:
        """Check if two points are within a radius."""
        self._require()
        return await self._exec.fetchval(
            "SELECT GEO_WITHIN($1, $2, $3, $4, $5)",
            lat1,
            lon1,
            lat2,
            lon2,
            radius_m,
        )

    async def area(self, geometry: str) -> float:
        """Calculate the area of a geometry in square meters."""
        self._require()
        return await self._exec.fetchval("SELECT GEO_AREA($1)", geometry)

    async def make_point(self, lon: float, lat: float) -> str:
        """Create a point geometry from lon/lat coordinates."""
        self._require()
        return await self._exec.fetchval(
            "SELECT ST_MAKEPOINT($1, $2)", lon, lat
        )

    async def point_x(self, point: str) -> float:
        """Extract the X (longitude) coordinate from a point."""
        self._require()
        return await self._exec.fetchval("SELECT ST_X($1)", point)

    async def point_y(self, point: str) -> float:
        """Extract the Y (latitude) coordinate from a point."""
        self._require()
        return await self._exec.fetchval("SELECT ST_Y($1)", point)

    async def insert(self, layer: str, feature: GeoFeature) -> None:
        """Insert a geo feature into a layer."""
        self._require()
        table = _safe(layer)
        await self._exec.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ("
            f"  id TEXT PRIMARY KEY,"
            f"  location POINT,"
            f"  properties JSONB DEFAULT '{{}}'"
            f")"
        )
        await self._exec.execute(
            f"INSERT INTO {table} (id, location, properties) "
            f"VALUES ($1, ST_MAKEPOINT($2, $3), $4::jsonb) "
            f"ON CONFLICT (id) DO UPDATE SET "
            f"location = ST_MAKEPOINT($2, $3), properties = $4::jsonb",
            feature.id,
            feature.lon,
            feature.lat,
            json.dumps(feature.properties),
        )

    async def nearest(
        self,
        layer: str,
        lat: float,
        lon: float,
        radius_m: float,
        *,
        limit: int = 10,
    ) -> list[GeoFeature]:
        """Find features nearest to a point within a radius."""
        self._require()
        table = _safe(layer)
        rows = await self._exec.fetch(
            f"SELECT id, ST_Y(location) AS lat, ST_X(location) AS lon, properties "
            f"FROM {table} "
            f"WHERE GEO_WITHIN(ST_Y(location), ST_X(location), $1, $2, $3) "
            f"ORDER BY GEO_DISTANCE(ST_Y(location), ST_X(location), $1, $2) "
            f"LIMIT $4",
            lat,
            lon,
            radius_m,
            limit,
        )
        return [_row_to_feature(row) for row in rows]

    async def within_bbox(
        self,
        layer: str,
        sw: tuple[float, float],
        ne: tuple[float, float],
    ) -> list[GeoFeature]:
        """Find features within a bounding box (sw_lat, sw_lon, ne_lat, ne_lon)."""
        self._require()
        table = _safe(layer)
        rows = await self._exec.fetch(
            f"SELECT id, ST_Y(location) AS lat, ST_X(location) AS lon, properties "
            f"FROM {table} "
            f"WHERE ST_Y(location) BETWEEN $1 AND $3 "
            f"AND ST_X(location) BETWEEN $2 AND $4",
            sw[0],
            sw[1],
            ne[0],
            ne[1],
        )
        return [_row_to_feature(row) for row in rows]

    async def within_polygon(
        self,
        layer: str,
        polygon: list[tuple[float, float]],
    ) -> list[GeoFeature]:
        """Find features within a polygon (list of (lat, lon) tuples).

        Uses a bounding-box pre-filter in SQL then applies an exact
        ray-casting point-in-polygon test in Python.
        """
        self._require()
        table = _safe(layer)
        lats = [p[0] for p in polygon]
        lons = [p[1] for p in polygon]
        # Bounding-box pre-filter (cheap SQL pass)
        rows = await self._exec.fetch(
            f"SELECT id, ST_Y(location) AS lat, ST_X(location) AS lon, properties "
            f"FROM {table} "
            f"WHERE ST_Y(location) BETWEEN $1 AND $2 "
            f"AND ST_X(location) BETWEEN $3 AND $4",
            min(lats),
            max(lats),
            min(lons),
            max(lons),
        )
        # Exact point-in-polygon test using ray-casting algorithm
        candidates = [_row_to_feature(row) for row in rows]
        return [f for f in candidates if _point_in_polygon(f.lat, f.lon, polygon)]


def _safe(name: str) -> str:
    return "".join(c for c in name if c.isalnum() or c == "_")


def _point_in_polygon(
    lat: float, lon: float, polygon: list[tuple[float, float]]
) -> bool:
    """Ray-casting algorithm for point-in-polygon containment.

    ``polygon`` is a list of (lat, lon) tuples forming a closed polygon.
    """
    n = len(polygon)
    inside = False
    j = n - 1
    for i in range(n):
        lat_i, lon_i = polygon[i]
        lat_j, lon_j = polygon[j]
        if (lon_i > lon) != (lon_j > lon):
            intersect_lat = (lat_j - lat_i) * (lon - lon_i) / (lon_j - lon_i) + lat_i
            if lat < intersect_lat:
                inside = not inside
        j = i
    return inside


def _row_to_feature(row: Any) -> GeoFeature:
    props = row.get("properties", {})
    if isinstance(props, str):
        props = json.loads(props)
    return GeoFeature(
        id=row["id"],
        lat=row["lat"],
        lon=row["lon"],
        properties=props or {},
    )
