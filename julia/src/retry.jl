#
# Serialization-failure classification and a managed retry helper.
#
# SERIALIZABLE is real on the shipping engine, which makes SQLSTATE 40001
# something applications actually receive. A serializable transaction that is
# never retried is a transaction that randomly fails under concurrency, and no
# PostgreSQL driver retries for you — drivers surface the code, the application
# decides. This is the SDK's answer to that obligation, per
# FRAMEWORK_CONTRACT.md §3.14.
#

"""
SQLSTATE 40001 — the transaction lost a conflict and MUST be retried from the
beginning. Raised by two mechanisms: strict 2PL wait-die on the disk engine
(the younger transaction is killed to break a potential deadlock) and SSI on
the MVCC engine (a dangerous structure detected at commit).
"""
const SQLSTATE_SERIALIZATION_FAILURE = "40001"

"""
SQLSTATE 55P03 — `lock_timeout` elapsed waiting for a table lock. Deliberately
NOT retryable: the holder is still there, so retrying spins against a lock that
is not moving. Raise `lock_timeout` or find the transaction holding it.
"""
const SQLSTATE_LOCK_NOT_AVAILABLE = "55P03"

"""
SQLSTATE 25P02 — a statement was issued after the transaction had already been
aborted. Only ROLLBACK is accepted, so the whole transaction must re-run, which
is what `with_retry` does.
"""
const SQLSTATE_IN_FAILED_TRANSACTION = "25P02"

"""
    sqlstate(e) -> Union{String, Nothing}

The SQLSTATE of an exception, or `nothing` if it carries no code.

Classification is by SQLSTATE, never by message text. LibPQ surfaces the code
on its own error type; anything else is matched on the five-character code as
it appears in the server's message, which is where a wrapped error still has it.
"""
function sqlstate(e)
    # LibPQ.Errors carries the raw fields; fall back to scraping the code out
    # of the message, which is what survives wrapping.
    if hasproperty(e, :sqlstate) && getproperty(e, :sqlstate) !== nothing
        return String(getproperty(e, :sqlstate))
    end
    msg = try
        sprint(showerror, e)
    catch
        return nothing
    end
    for code in (SQLSTATE_SERIALIZATION_FAILURE,
                 SQLSTATE_LOCK_NOT_AVAILABLE,
                 SQLSTATE_IN_FAILED_TRANSACTION)
        occursin(code, msg) && return code
    end
    return nothing
end

"""
    is_serialization_failure(e) -> Bool

Whether `e` is a conflict the caller should retry (40001, or 25P02 from a
transaction already killed by one).
"""
is_serialization_failure(e) =
    sqlstate(e) in (SQLSTATE_SERIALIZATION_FAILURE, SQLSTATE_IN_FAILED_TRANSACTION)

"""
    is_lock_not_available(e) -> Bool

Whether `e` is a `lock_timeout` expiry (55P03).

Kept distinct from [`is_serialization_failure`](@ref) on purpose: the two call
for opposite responses. A serialization failure means "someone else won, try
again"; a lock timeout means "the lock is still held, retrying will not help".
"""
is_lock_not_available(e) = sqlstate(e) == SQLSTATE_LOCK_NOT_AVAILABLE

"""
    with_retry(f, client; max_attempts=5, base_delay=0.002, max_delay=0.25, isolation=nothing)

Run `f(tx)` inside a transaction, retrying it on serialization failure.

`f` MUST be idempotent with respect to anything outside the database: it can run
more than once. Everything it does through `tx` is rolled back between
attempts; anything it does elsewhere (sending mail, charging a card, mutating a
global) is not.

On success the transaction commits. On a serialization failure it is rolled back
and retried with jittered exponential backoff. On any other error it is rolled
back and the exception rethrown unchanged — in particular a `lock_timeout`
(55P03) is NOT retried, because the lock is still held.

# Example
```julia
with_retry(client; isolation="SERIALIZABLE") do tx
    execute!(sql(tx), "UPDATE accounts SET balance = balance - 10 WHERE id = \$1", id)
end
```
"""
function with_retry(f::Function, client::NucleusClient;
                    max_attempts::Int = 5,
                    base_delay::Float64 = 0.002,
                    max_delay::Float64 = 0.25,
                    isolation::Union{String,Nothing} = nothing)
    attempts = max(max_attempts, 1)
    delay = base_delay > 0 ? base_delay : 0.002
    last_error = nothing

    for attempt in 1:attempts
        try
            return transaction(client) do tx
                if isolation !== nothing
                    # An engine that cannot honour the level refuses rather
                    # than silently downgrading, so this surfaces a mismatch.
                    execute!(sql(tx), "SET TRANSACTION ISOLATION LEVEL $(isolation)")
                end
                f(tx)
            end
        catch e
            if !is_serialization_failure(e)
                rethrow(e)
            end
            last_error = e
            if attempt == attempts
                break
            end
            # Full jitter. Without it two conflicting transactions retry in
            # lockstep and collide again on the same schedule — and under
            # wait-die the younger one loses every round, so a fixed backoff
            # can starve it indefinitely.
            sleep(rand() * delay)
            delay = min(delay * 2, max_delay)
        end
    end

    throw(last_error)
end
