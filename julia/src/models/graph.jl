"""Graph model — property graph via GRAPH_* SQL functions + Graphs.jl extension."""

struct GraphModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

@enum GraphDirection GraphOut GraphIn GraphBoth

struct GraphNode
    id::Int64
    label::String
    properties::Dict{String, Any}
end

struct GraphEdge
    id::Int64
    from_id::Int64
    to_id::Int64
    edge_type::String
    properties::Dict{String, Any}
end

"""GRAPH_ADD_NODE(label [, properties_json]) → Int64 node ID"""
function add_node!(m::GraphModel, label::String;
                   properties::Dict=Dict())::Int64
    require_nucleus(m.features, "Graph")
    if isempty(properties)
        result = LibPQ.execute(m.conn, "SELECT GRAPH_ADD_NODE(\$1)", [label])
    else
        result = LibPQ.execute(m.conn, "SELECT GRAPH_ADD_NODE(\$1, \$2)",
                               [label, JSON3.write(properties)])
    end
    return _int(result)
end

"""GRAPH_ADD_EDGE(from_id, to_id, type [, props_json]) → Int64 edge ID"""
function add_edge!(m::GraphModel, from_id::Int64, to_id::Int64, edge_type::String;
                   properties::Dict=Dict())::Int64
    require_nucleus(m.features, "Graph")
    if isempty(properties)
        result = LibPQ.execute(m.conn,
            "SELECT GRAPH_ADD_EDGE(\$1, \$2, \$3)",
            [from_id, to_id, edge_type])
    else
        result = LibPQ.execute(m.conn,
            "SELECT GRAPH_ADD_EDGE(\$1, \$2, \$3, \$4)",
            [from_id, to_id, edge_type, JSON3.write(properties)])
    end
    return _int(result)
end

"""GRAPH_DELETE_NODE(node_id) → Bool"""
function delete_node!(m::GraphModel, node_id::Int64)::Bool
    require_nucleus(m.features, "Graph")
    result = LibPQ.execute(m.conn, "SELECT GRAPH_DELETE_NODE(\$1)", [node_id])
    return _bool(result)
end

"""GRAPH_DELETE_EDGE(edge_id) → Bool"""
function delete_edge!(m::GraphModel, edge_id::Int64)::Bool
    require_nucleus(m.features, "Graph")
    result = LibPQ.execute(m.conn, "SELECT GRAPH_DELETE_EDGE(\$1)", [edge_id])
    return _bool(result)
end

"""GRAPH_QUERY(cypher) → Dict{String,Any} with columns and rows"""
function graph_query(m::GraphModel, cypher::String)::Dict{String, Any}
    require_nucleus(m.features, "Graph")
    result = LibPQ.execute(m.conn, "SELECT GRAPH_QUERY(\$1)", [cypher])
    val = first(result)[1]
    return JSON3.read(val, Dict{String, Any})
end

"""GRAPH_NEIGHBORS(node_id [, direction]) → Vector{Dict{String,Any}}"""
function neighbors(m::GraphModel, node_id::Int64;
                   direction::GraphDirection=GraphOut)::Vector{Dict{String, Any}}
    require_nucleus(m.features, "Graph")
    dir_str = direction == GraphOut ? "out" : direction == GraphIn ? "in" : "both"
    result = LibPQ.execute(m.conn, "SELECT GRAPH_NEIGHBORS(\$1, \$2)",
                           [node_id, dir_str])
    val = first(result)[1]
    ismissing(val) && return Dict{String,Any}[]
    return JSON3.read(val, Vector{Dict{String, Any}})
end

"""GRAPH_SHORTEST_PATH(from_id, to_id) → Vector{Int64} node IDs on path"""
function shortest_path(m::GraphModel, from_id::Int64, to_id::Int64)::Vector{Int64}
    require_nucleus(m.features, "Graph")
    result = LibPQ.execute(m.conn, "SELECT GRAPH_SHORTEST_PATH(\$1, \$2)",
                           [from_id, to_id])
    val = first(result)[1]
    ismissing(val) && return Int64[]
    return JSON3.read(val, Vector{Int64})
end

"""GRAPH_NODE_COUNT() → Int64"""
function node_count(m::GraphModel)::Int64
    require_nucleus(m.features, "Graph")
    result = LibPQ.execute(m.conn, "SELECT GRAPH_NODE_COUNT()")
    return _int(result)
end

"""GRAPH_EDGE_COUNT() → Int64"""
function edge_count(m::GraphModel)::Int64
    require_nucleus(m.features, "Graph")
    result = LibPQ.execute(m.conn, "SELECT GRAPH_EDGE_COUNT()")
    return _int(result)
end
