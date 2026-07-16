#!/usr/bin/env bash
# Run randomized simulation (invariant checking) on every runnable Quint spec.
#
# A spec is "runnable" when it defines an init + step state machine and safety
# invariants. The common/ building-block modules (types, crash, network) have no
# init/step and are type-check-only, so they are exercised by check.sh instead.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPECS_DIR="$(dirname "$SCRIPT_DIR")/specs"

MAX_SAMPLES="${MAX_SAMPLES:-500}"
MAX_STEPS="${MAX_STEPS:-40}"

echo "=== Quint Random Simulation (${MAX_SAMPLES} traces, ${MAX_STEPS} steps) ==="

if ! command -v quint &>/dev/null; then
    echo "Quint not installed."
    exit 1
fi

# Each entry: "<relative-spec-path>|<conjoined safety invariants>"
RUNNABLE=(
    "nucleus/distributed_tx.qnt|commit_validity and atomicity and no_committed_abort"
    "nucleus/membership.qnt|non_empty and no_duplicate_add and config_monotonic"
    "nucleus/multi_raft.qnt|election_safety and log_matching"
    "nucleus/replication.qnt|replicas_behind and sync_durability"
    "nucleus/resharding.qnt|no_data_loss and no_double_ownership and key_conservation"
    "nucleus/snapshot_transfer.qnt|source_unchanged"
    "framework/circuit_breaker.qnt|valid_state and non_negative_counts and reject_bound and open_has_bounded_ticks and half_open_bounded and closed_under_threshold"
    "framework/csrf_lifecycle.qnt|valid_token_states and no_replay and session_isolation and expired_not_active and ttl_non_negative"
    "framework/rate_limiter.qnt|offset_bounded and count_bounded and rate_enforced and fair_capacity and no_undercount and reject_accounting"
    "framework/session_lifecycle.qnt|valid_phases and terminal_permanent and renewal_bounded and active_has_ttl and expired_reason and owner_immutable and revoked_is_final"
    "realtime/hot_reload.qnt|version_monotonic and connected_bounded_lag and delta_ordering and no_version_gaps and sync_reaches_server and server_monotonic and disconnected_no_pending and valid_client_states"
    "realtime/websocket_hub.qnt|members_connected and no_self_delivery and delivered_was_pending and no_duplicate_delivery and empty_rooms_clean and broadcast_scoped"
)

fail=0
for entry in "${RUNNABLE[@]}"; do
    spec="${entry%%|*}"
    inv="${entry#*|}"
    echo ""
    echo "--- Simulating $spec ---"
    if quint run --max-samples="$MAX_SAMPLES" --max-steps="$MAX_STEPS" \
        --invariant="$inv" "$SPECS_DIR/$spec" 2>&1; then
        echo "  Simulation passed"
    else
        echo "  Simulation FAILED"
        fail=1
    fi
done

echo ""
if [ "$fail" -ne 0 ]; then
    echo "=== Simulation FAILED ==="
    exit 1
fi
echo "=== Simulation complete ==="
