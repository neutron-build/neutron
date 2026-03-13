"""
Sensor Data Pipeline — real-time ingestion, caching, and aggregation.

Demonstrates:
- KV: caching latest sensor readings
- TimeSeries: persisting measurements with millisecond timestamps
- PubSub: notifying subscribers on threshold breach
- ConnectionPool: concurrent ingestion from multiple sensors

Run:
    julia --project=. examples/sensor_pipeline.jl postgres://localhost:5432/mydb
"""

using NeutronJulia

const URL = get(ARGS, 1, get(ENV, "NUCLEUS_TEST_URL", ""))
isempty(URL) && error("Usage: julia examples/sensor_pipeline.jl <postgres-url>")

# Simulated sensors
const SENSORS = ["sensor:temp:room1", "sensor:temp:room2", "sensor:humidity:room1",
                  "sensor:pressure:outdoor", "sensor:co2:room1"]

# Alert thresholds
const THRESHOLDS = Dict(
    "sensor:temp" => (low=15.0, high=30.0),
    "sensor:humidity" => (low=30.0, high=70.0),
    "sensor:co2" => (low=0.0, high=1000.0),
)

function check_threshold(series::String, value::Float64)
    for (prefix, bounds) in THRESHOLDS
        if startswith(series, prefix)
            return value < bounds.low || value > bounds.high
        end
    end
    return false
end

function simulate_reading(series::String)::Float64
    if occursin("temp", series)
        return 20.0 + 10.0 * rand() - 2.0
    elseif occursin("humidity", series)
        return 45.0 + 20.0 * rand() - 5.0
    elseif occursin("pressure", series)
        return 1013.25 + 10.0 * rand() - 3.0
    else  # co2
        return 400.0 + 200.0 * rand()
    end
end

println("Connecting to Nucleus (pool size=3)...")
pool = ConnectionPool(URL; size=3)

println("Starting sensor pipeline — $(length(SENSORS)) sensors, 20 rounds...\n")

for round in 1:20
    now_ms = Int64(round(time() * 1000))

    for series in SENSORS
        value = simulate_reading(series)

        with_pool(pool) do client
            # 1. Store in TimeSeries
            Base.insert!(timeseries(client), series, now_ms, value)

            # 2. Cache latest reading in KV (with 60s TTL)
            kv_set!(kv(client), "latest:$series", string(value); ttl=60)

            # 3. Publish alert if threshold breached
            if check_threshold(series, value)
                publish!(pubsub(client), "alerts",
                         """{"series":"$series","value":$value,"ts":$now_ms}""")
            end
        end
    end

    # Print stats every 5 rounds
    if round % 5 == 0
        println("Round $round:")
        with_pool(pool) do client
            k = kv(client)
            for series in SENSORS[1:2]
                cached = kv_get(k, "latest:$series")
                println("  $series → $cached")
            end

            ts = timeseries(client)
            count1 = ts_count(ts, SENSORS[1])
            println("  $(SENSORS[1]) has $count1 stored points")
        end
        println()
    end
end

# Final aggregation
println("Final aggregation:")
with_pool(pool) do client
    ts = timeseries(client)
    start_ms = Int64(round(time() * 1000)) - 300_000  # last 5 min
    end_ms   = Int64(round(time() * 1000))
    for series in SENSORS
        avg = range_avg(ts, series, start_ms, end_ms)
        cnt = range_count(ts, series, start_ms, end_ms)
        println("  $series: $cnt readings, avg=$(round(avg === nothing ? 0.0 : avg; digits=2))")
    end
end

println("\nSensor pipeline complete.")
close(pool)
