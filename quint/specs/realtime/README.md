# Real-Time Protocol Specifications

Quint specifications for real-time communication protocols in the Neutron ecosystem.

## Specs

| Spec | Source | Invariants | Tests |
|------|--------|-----------|-------|
| `websocket_hub.qnt` | go/neutronrealtime/ | members_connected, no_self_delivery, no_duplicate_delivery, broadcast_scoped | websocket_hub_test.qnt |
| `hot_reload.qnt` | mobile-preview/ | version_monotonic, delta_ordering, no_version_gaps, disconnected_no_pending | hot_reload_test.qnt |

## Verified Properties

- Message delivery guarantees under concurrent join/leave
- No message loss during network reconnection
- Room cleanup when last connection leaves
- Delta bundle ordering during hot reload
- Client version never exceeds server version
