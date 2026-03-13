"""PubSub model — publish/subscribe via PUBSUB_* SQL functions + LISTEN/NOTIFY."""

struct PubSubModel
    conn::LibPQ.Connection
    features::NucleusFeatures
end

"""PUBSUB_PUBLISH(channel, message) → Int64 subscribers reached"""
function publish!(m::PubSubModel, channel::String, message::String)::Int64
    require_nucleus(m.features, "PubSub")
    result = LibPQ.execute(m.conn, "SELECT PUBSUB_PUBLISH(\$1, \$2)", [channel, message])
    return _int(result)
end

"""PUBSUB_CHANNELS([pattern]) → Vector{String}"""
function channels(m::PubSubModel; pattern::Union{String, Nothing}=nothing)::Vector{String}
    require_nucleus(m.features, "PubSub")
    if pattern === nothing
        result = LibPQ.execute(m.conn, "SELECT PUBSUB_CHANNELS()")
    else
        result = LibPQ.execute(m.conn, "SELECT PUBSUB_CHANNELS(\$1)", [pattern])
    end
    return _split_csv(first(result)[1])
end

"""PUBSUB_SUBSCRIBERS(channel) → Int64"""
function subscriber_count(m::PubSubModel, channel::String)::Int64
    require_nucleus(m.features, "PubSub")
    result = LibPQ.execute(m.conn, "SELECT PUBSUB_SUBSCRIBERS(\$1)", [channel])
    return _int(result)
end

"""
    subscribe(m, channel, callback) → Channel{String}

LISTEN on `channel`. Messages are delivered asynchronously to `callback(payload)`
and also pushed to the returned Channel{String}. Close the channel to unsubscribe.
"""
function subscribe(m::PubSubModel, channel::String, callback::Function)::Channel{String}
    require_nucleus(m.features, "PubSub")
    LibPQ.execute(m.conn, "LISTEN $channel")
    ch = Channel{String}(32)
    @async begin
        while isopen(ch)
            try
                notifs = LibPQ.notifications(m.conn)
                for n in notifs
                    callback(n.payload)
                    put!(ch, n.payload)
                end
            catch
                break
            end
            sleep(0.05)
        end
    end
    return ch
end
