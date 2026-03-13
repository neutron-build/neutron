"""Geo/Spatial model — GEO_*/ST_* SQL functions."""

struct GeoModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

struct GeoPoint
    lat::Float64
    lon::Float64
end

"""GEO_DISTANCE(lat1,lon1,lat2,lon2) → Float64 meters (haversine)"""
function distance(m::GeoModel, a::GeoPoint, b::GeoPoint)::Float64
    require_nucleus(m.features, "Geo")
    result = LibPQ.execute(m.conn,
        "SELECT GEO_DISTANCE(\$1, \$2, \$3, \$4)",
        [a.lat, a.lon, b.lat, b.lon])
    return Float64(first(result)[1])
end

"""GEO_DISTANCE_EUCLIDEAN(x1,y1,x2,y2) → Float64"""
function distance_euclidean(m::GeoModel, a::GeoPoint, b::GeoPoint)::Float64
    require_nucleus(m.features, "Geo")
    result = LibPQ.execute(m.conn,
        "SELECT GEO_DISTANCE_EUCLIDEAN(\$1, \$2, \$3, \$4)",
        [a.lon, a.lat, b.lon, b.lat])
    return Float64(first(result)[1])
end

"""GEO_WITHIN(lat1,lon1,lat2,lon2,radius_m) → Bool"""
function within(m::GeoModel, a::GeoPoint, b::GeoPoint, radius_m::Float64)::Bool
    require_nucleus(m.features, "Geo")
    result = LibPQ.execute(m.conn,
        "SELECT GEO_WITHIN(\$1, \$2, \$3, \$4, \$5)",
        [a.lat, a.lon, b.lat, b.lon, radius_m])
    return _bool(result)
end

"""GEO_AREA(lon1,lat1,lon2,lat2,...) → Float64"""
function area(m::GeoModel, points::Vector{GeoPoint})::Float64
    require_nucleus(m.features, "Geo")
    coords = vcat([[p.lon, p.lat] for p in points]...)
    placeholders = join(["\$$(i)" for i in 1:length(coords)], ", ")
    sql_str = "SELECT GEO_AREA($placeholders)"
    result = LibPQ.execute(m.conn, sql_str, coords)
    return Float64(first(result)[1])
end

"""ST_MAKEPOINT(lon, lat) → point"""
function makepoint(m::GeoModel, lon::Float64, lat::Float64)
    require_nucleus(m.features, "Geo")
    result = LibPQ.execute(m.conn, "SELECT ST_MAKEPOINT(\$1, \$2)", [lon, lat])
    return first(result)[1]
end

"""ST_X(point) → Float64 longitude"""
function st_x(m::GeoModel, point)::Float64
    require_nucleus(m.features, "Geo")
    result = LibPQ.execute(m.conn, "SELECT ST_X(\$1)", [point])
    return Float64(first(result)[1])
end

"""ST_Y(point) → Float64 latitude"""
function st_y(m::GeoModel, point)::Float64
    require_nucleus(m.features, "Geo")
    result = LibPQ.execute(m.conn, "SELECT ST_Y(\$1)", [point])
    return Float64(first(result)[1])
end
