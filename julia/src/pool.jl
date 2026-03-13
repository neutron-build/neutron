"""
    ConnectionPool

A thread-safe pool of LibPQ connections to Nucleus/PostgreSQL.
Connections are pre-established on construction and reused across requests.

# Example
```julia
pool = ConnectionPool("postgres://localhost:5432/mydb", size=4)

# Acquire a client, use it, automatically released
with_pool(pool) do client
    rows = query(sql(client), "SELECT * FROM users")
    set!(kv(client), "cache:key", "value")
end

close(pool)
```
"""
struct ConnectionPool
    url::String
    channel::Channel{NucleusClient}
    size::Int
end

"""
    ConnectionPool(url; size=4) → ConnectionPool

Create a pool of `size` persistent connections. All connections are opened
immediately. Throws if any connection fails.
"""
function ConnectionPool(url::String; size::Int=4)
    size > 0 || throw(ArgumentError("Pool size must be > 0"))
    ch = Channel{NucleusClient}(size)
    for _ in 1:size
        client = connect(url)
        put!(ch, client)
    end
    return ConnectionPool(url, ch, size)
end

"""
    acquire(pool) → NucleusClient

Borrow a client from the pool. Blocks until one is available.
Always pair with `release(pool, client)` or use `with_pool`.
"""
function acquire(pool::ConnectionPool)::NucleusClient
    return take!(pool.channel)
end

"""
    release(pool, client)

Return a client to the pool. If the connection is no longer open, a fresh
one is created to keep the pool at full capacity.
"""
function release(pool::ConnectionPool, client::NucleusClient)
    if isopen(client)
        put!(pool.channel, client)
    else
        # Reconnect and return a healthy connection to the pool
        try
            fresh = connect(pool.url)
            put!(pool.channel, fresh)
        catch
            # If reconnect fails, pool shrinks; log for visibility
            @warn "ConnectionPool: failed to reconnect, pool capacity reduced" pool.url
        end
    end
    return nothing
end

"""
    with_pool(f, pool) → result

Acquire a client, call `f(client)`, then release. Guarantees release even
if `f` throws. This is the recommended way to use a pool.

# Example
```julia
result = with_pool(pool) do client
    query(sql(client), "SELECT COUNT(*) FROM events")
end
```
"""
function with_pool(f::Function, pool::ConnectionPool)
    client = acquire(pool)
    try
        return f(client)
    finally
        release(pool, client)
    end
end

"""Close all pooled connections and drain the channel."""
function close(pool::ConnectionPool)
    # Drain all idle connections
    while isready(pool.channel)
        client = take!(pool.channel)
        try
            close(client)
        catch
        end
    end
    close(pool.channel)
    return nothing
end

isopen(pool::ConnectionPool) = isopen(pool.channel)
Base.length(pool::ConnectionPool) = pool.size

"""Number of connections currently idle (available) in the pool."""
idle_count(pool::ConnectionPool) = Base.n_avail(pool.channel)
