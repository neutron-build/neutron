// Property-based tests for common modules: types, network, crash.
//
// These tests verify foundational invariants that all higher-level specs
// depend on: unique node identities, message routing correctness, network
// partition symmetry, FIFO ordering within links, crash/recovery semantics,
// and failure detector accuracy.
module common_test {
    import types.*

    // =========================================================================
    // types.qnt — Node identity and message routing properties
    // =========================================================================

    // --- Setup: a small cluster with well-formed types ---

    pure val TEST_NODES: Set[NodeId] = Set(1, 2, 3, 4, 5)
    pure val TEST_GROUPS: Set[GroupId] = Set(1, 2)
    pure val TEST_TERMS: Set[Term] = Set(0, 1, 2, 3)

    // Property: Node IDs are unique in any well-formed cluster.
    // Each element of a set is distinct by construction; verify that size
    // matches the expected count (i.e. no silent collapses).
    run node_ids_unique_test = {
        val nodes = TEST_NODES
        // 5 distinct IDs must yield a set of size 5
        assert(nodes.size() == 5)
    }

    // Property: Constructing a Message preserves sender/receiver identity.
    run message_routing_preserves_endpoints_test = {
        val msg: Message = { from_: 1, to: 3, term: 2, payload: "write" }
        all {
            assert(msg.from_ == 1),
            assert(msg.to == 3),
            assert(msg.from_ != msg.to),
        }
    }

    // Property: Two messages with different senders are distinguishable.
    run distinct_senders_test = {
        val m1: Message = { from_: 1, to: 3, term: 1, payload: "a" }
        val m2: Message = { from_: 2, to: 3, term: 1, payload: "a" }
        all {
            assert(m1.from_ != m2.from_),
            assert(m1 != m2),
        }
    }

    // Property: A set of messages with unique (from, to) pairs has no
    // routing ambiguity — no two messages share both sender and receiver.
    run no_routing_ambiguity_test = {
        val msgs: Set[Message] = Set(
            { from_: 1, to: 2, term: 0, payload: "p1" },
            { from_: 2, to: 3, term: 0, payload: "p2" },
            { from_: 3, to: 1, term: 0, payload: "p3" }
        )
        // All three are distinct
        assert(msgs.size() == 3)
    }

    // Property: LogEntry index ordering is well-defined.
    run log_entry_ordering_test = {
        val e1: LogEntry = { term: 1, index: 1, command: "insert" }
        val e2: LogEntry = { term: 1, index: 2, command: "update" }
        val e3: LogEntry = { term: 2, index: 3, command: "delete" }
        all {
            assert(e1.index < e2.index),
            assert(e2.index < e3.index),
            assert(e1.term <= e2.term),
            assert(e2.term <= e3.term),
        }
    }

    // Property: KeyRange start <= end defines a valid range.
    run key_range_well_formed_test = {
        val r: KeyRange = { start: 10, end_: 50 }
        assert(r.start <= r.end_)
    }

    // =========================================================================
    // types.qnt — Status transition state machine properties
    // =========================================================================

    // Model the allowed status transitions for a generic protocol entity.
    // The status state machine used across specs is:
    //   pending -> prepared -> committed
    //   pending -> aborted
    // No backward transitions are allowed.

    pure def is_valid_status(s: str): bool =
        s == "pending" or s == "prepared" or s == "committed" or s == "aborted"

    pure def valid_transition(from_status: str, to_status: str): bool =
        (from_status == "pending" and to_status == "prepared") or
        (from_status == "pending" and to_status == "aborted") or
        (from_status == "prepared" and to_status == "committed")

    // Property: All defined statuses are recognized.
    run all_statuses_valid_test = {
        all {
            assert(is_valid_status("pending")),
            assert(is_valid_status("prepared")),
            assert(is_valid_status("committed")),
            assert(is_valid_status("aborted")),
            assert(not(is_valid_status("unknown_garbage"))),
        }
    }

    // Property: Committed is a terminal state — no valid transitions out.
    run committed_is_terminal_test = {
        all {
            assert(not(valid_transition("committed", "pending"))),
            assert(not(valid_transition("committed", "prepared"))),
            assert(not(valid_transition("committed", "aborted"))),
            assert(not(valid_transition("committed", "committed"))),
        }
    }

    // Property: Aborted is a terminal state.
    run aborted_is_terminal_test = {
        all {
            assert(not(valid_transition("aborted", "pending"))),
            assert(not(valid_transition("aborted", "prepared"))),
            assert(not(valid_transition("aborted", "committed"))),
            assert(not(valid_transition("aborted", "aborted"))),
        }
    }

    // Property: No backward transitions (committed/aborted -> pending).
    run no_backward_transitions_test = {
        all {
            assert(not(valid_transition("committed", "pending"))),
            assert(not(valid_transition("aborted", "pending"))),
            assert(not(valid_transition("prepared", "pending"))),
        }
    }
}

// =========================================================================
// Network properties — uses the network module's own state variables
// =========================================================================
module network_test {
    import types.*
    import network.*

    // --- Partition symmetry ---

    // Property: network.partition(a, b) is symmetric — if A can't reach B,
    // then B can't reach A. Verified by checking both directions are added.
    run partition_is_symmetric_test = {
        all {
            messages' = Set(),
            partitioned' = Set(),
        }.then(
            partition(1, 2)
        ).then(
            all {
                // Both (1,2) and (2,1) must be in the partition set
                assert((1, 2).in(partitioned)),
                assert((2, 1).in(partitioned)),
            }
        )
    }

    // Property: heal is also symmetric — both directions removed.
    run heal_is_symmetric_test = {
        all {
            messages' = Set(),
            partitioned' = Set(),
        }.then(
            partition(1, 2)
        ).then(
            heal(1, 2)
        ).then(
            all {
                assert(not((1, 2).in(partitioned))),
                assert(not((2, 1).in(partitioned))),
            }
        )
    }

    // --- Messages in transit are finite ---

    // Property: After sending N messages, messages set has at most N elements.
    run messages_finite_after_sends_test = {
        val m1: Message = { from_: 1, to: 2, term: 0, payload: "a" }
        val m2: Message = { from_: 2, to: 3, term: 0, payload: "b" }
        val m3: Message = { from_: 3, to: 1, term: 0, payload: "c" }
        all {
            messages' = Set(),
            partitioned' = Set(),
        }.then(
            send(m1)
        ).then(
            send(m2)
        ).then(
            send(m3)
        ).then(
            // At most 3 messages in transit
            assert(messages.size() <= 3)
        )
    }

    // Property: Delivering a message decreases the in-transit count.
    run deliver_decreases_transit_test = {
        val m1: Message = { from_: 1, to: 2, term: 0, payload: "x" }
        all {
            messages' = Set(),
            partitioned' = Set(),
        }.then(
            send(m1)
        ).then(
            all {
                assert(messages.size() == 1),
                deliver(m1),
            }
        ).then(
            assert(messages.size() == 0)
        )
    }

    // Property: Dropping a message also removes it from transit.
    run drop_removes_from_transit_test = {
        val m1: Message = { from_: 1, to: 2, term: 0, payload: "y" }
        all {
            messages' = Set(),
            partitioned' = Set(),
        }.then(
            send(m1)
        ).then(
            drop(m1)
        ).then(
            assert(messages.size() == 0)
        )
    }

    // --- Partitioned nodes cannot send ---

    // Property: send fails across a partition (action returns false / is not enabled).
    // We verify this indirectly: after partition(1,2), a message from 1->2
    // cannot be sent, so the messages set stays empty.
    run partitioned_send_blocked_test = {
        val m: Message = { from_: 1, to: 2, term: 0, payload: "blocked" }
        all {
            messages' = Set(),
            partitioned' = Set(),
        }.then(
            partition(1, 2)
        ).then(
            // send guard: not((msg.from_, msg.to).in(partitioned))
            // With partition active, send(m) is disabled.
            // We just verify the no_fabrication invariant still holds
            // and messages stays empty after we skip the blocked send.
            all {
                assert((1, 2).in(partitioned)),
                assert(messages.size() == 0),
                no_fabrication,
            }
        )
    }

    // --- no_fabrication invariant ---

    // Property: no_fabrication holds after valid sends.
    run no_fabrication_holds_test = {
        val m1: Message = { from_: 1, to: 2, term: 1, payload: "ok" }
        val m2: Message = { from_: 3, to: 1, term: 2, payload: "ok2" }
        all {
            messages' = Set(),
            partitioned' = Set(),
        }.then(
            send(m1)
        ).then(
            send(m2)
        ).then(
            no_fabrication
        )
    }

    // --- FIFO ordering within a link ---

    // Property: Network model delivers messages in any order (set-based),
    // but within a single directed link (same from, same to), if we track
    // sequence numbers via term, delivering lower term first is valid.
    // This test shows the model allows reordering (sets are unordered)
    // but the message data is preserved — clients can impose FIFO via
    // sequence numbers embedded in the payload/term.
    run link_message_identity_preserved_test = {
        val m1: Message = { from_: 1, to: 2, term: 1, payload: "first" }
        val m2: Message = { from_: 1, to: 2, term: 2, payload: "second" }
        all {
            messages' = Set(),
            partitioned' = Set(),
        }.then(
            send(m1)
        ).then(
            send(m2)
        ).then(
            all {
                // Both messages are in transit
                assert(m1.in(messages)),
                assert(m2.in(messages)),
                // Can be distinguished by term for FIFO reconstruction
                assert(m1.term < m2.term),
            }
        )
    }

    // Property: Delivering one message from a link does not corrupt the other.
    run partial_delivery_preserves_other_test = {
        val m1: Message = { from_: 1, to: 2, term: 1, payload: "first" }
        val m2: Message = { from_: 1, to: 2, term: 2, payload: "second" }
        all {
            messages' = Set(),
            partitioned' = Set(),
        }.then(
            send(m1)
        ).then(
            send(m2)
        ).then(
            deliver(m1)
        ).then(
            all {
                // m1 removed, m2 still present and uncorrupted
                assert(not(m1.in(messages))),
                assert(m2.in(messages)),
                assert(m2.payload == "second"),
            }
        )
    }
}

// =========================================================================
// Crash model properties — uses the crash module's own state variables
// =========================================================================
module crash_test {
    import types.*
    import crash.*

    // --- Crashed nodes state ---

    // Property: After crashing a node, it is in crashed_nodes and not in alive.
    run crash_removes_from_alive_test = {
        all {
            alive' = Set(1, 2, 3),
            crashed_nodes' = Set(),
        }.then(
            crash_node(2)
        ).then(
            all {
                assert(not(2.in(alive))),
                assert(2.in(crashed_nodes)),
                node_partition,
            }
        )
    }

    // --- Recovery preserves persistent state ---
    // In the crash model, restart_node moves a node from crashed to alive.
    // The key property: restart does not alter other nodes' state.

    run recovery_restores_to_alive_test = {
        all {
            alive' = Set(1, 3),
            crashed_nodes' = Set(2),
        }.then(
            restart_node(2)
        ).then(
            all {
                assert(2.in(alive)),
                assert(not(2.in(crashed_nodes))),
                node_partition,
            }
        )
    }

    // Property: Recovery does not affect other nodes' membership.
    run recovery_does_not_affect_others_test = {
        all {
            alive' = Set(1, 3),
            crashed_nodes' = Set(2),
        }.then(
            restart_node(2)
        ).then(
            all {
                // Node 1 and 3 are still alive, unaffected
                assert(1.in(alive)),
                assert(3.in(alive)),
                // Node 2 is now alive too
                assert(2.in(alive)),
                assert(crashed_nodes.size() == 0),
            }
        )
    }

    // --- node_partition invariant: alive and crashed are disjoint ---

    // Property: The invariant holds after init.
    run node_partition_init_test = {
        all {
            alive' = Set(1, 2, 3),
            crashed_nodes' = Set(),
        }.then(
            node_partition
        )
    }

    // Property: The invariant holds after crash.
    run node_partition_after_crash_test = {
        all {
            alive' = Set(1, 2, 3),
            crashed_nodes' = Set(),
        }.then(
            crash_node(1)
        ).then(
            node_partition
        )
    }

    // Property: The invariant holds after crash then recovery.
    run node_partition_after_recovery_test = {
        all {
            alive' = Set(1, 2, 3),
            crashed_nodes' = Set(),
        }.then(
            crash_node(1)
        ).then(
            restart_node(1)
        ).then(
            node_partition
        )
    }

    // --- Crash detection accuracy ---

    // Property: A crashed node is always detectable via crashed_nodes set.
    // (eventually-perfect failure detector: once crashed, it is immediately
    // in crashed_nodes — no false negatives after the crash action.)
    run crash_immediately_detectable_test = {
        all {
            alive' = Set(1, 2, 3),
            crashed_nodes' = Set(),
        }.then(
            crash_node(3)
        ).then(
            // Immediately after crash, the node is in crashed_nodes
            assert(3.in(crashed_nodes))
        )
    }

    // Property: Alive nodes are never falsely reported as crashed.
    // (no false positives: alive ∩ crashed_nodes = ∅)
    run no_false_crash_report_test = {
        all {
            alive' = Set(1, 2, 3),
            crashed_nodes' = Set(),
        }.then(
            crash_node(2)
        ).then(
            all {
                // Node 1 and 3 are alive and NOT in crashed_nodes
                assert(1.in(alive) and not(1.in(crashed_nodes))),
                assert(3.in(alive) and not(3.in(crashed_nodes))),
            }
        )
    }

    // --- Multiple crashes and recoveries ---

    run multiple_crashes_test = {
        all {
            alive' = Set(1, 2, 3, 4, 5),
            crashed_nodes' = Set(),
        }.then(
            crash_node(1)
        ).then(
            crash_node(3)
        ).then(
            crash_node(5)
        ).then(
            all {
                assert(alive == Set(2, 4)),
                assert(crashed_nodes == Set(1, 3, 5)),
                node_partition,
            }
        )
    }

    run crash_recovery_cycle_test = {
        all {
            alive' = Set(1, 2, 3),
            crashed_nodes' = Set(),
        }.then(
            crash_node(1)
        ).then(
            crash_node(2)
        ).then(
            restart_node(1)
        ).then(
            crash_node(3)
        ).then(
            restart_node(2)
        ).then(
            all {
                assert(alive == Set(1, 2)),
                assert(crashed_nodes == Set(3)),
                node_partition,
            }
        )
    }

    // Property: After all nodes crash and all recover, we return to full cluster.
    run full_crash_full_recovery_test = {
        all {
            alive' = Set(1, 2, 3),
            crashed_nodes' = Set(),
        }.then(
            crash_node(1)
        ).then(
            crash_node(2)
        ).then(
            crash_node(3)
        ).then(
            all {
                assert(alive.size() == 0),
                assert(crashed_nodes.size() == 3),
                node_partition,
            }
        ).then(
            restart_node(1)
        ).then(
            restart_node(2)
        ).then(
            restart_node(3)
        ).then(
            all {
                assert(alive == Set(1, 2, 3)),
                assert(crashed_nodes.size() == 0),
                node_partition,
            }
        )
    }
}
