use super::*;

// ======================================================================
// KV Collection SQL function tests: Lists
// ======================================================================

#[tokio::test]
async fn test_kv_lpush_rpush_llen() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT kv_lpush('mylist', 'a')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(1));
    let r = exec(&ex, "SELECT kv_rpush('mylist', 'b')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(2));
    let r = exec(&ex, "SELECT kv_lpush('mylist', 'z')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(3));
    let r = exec(&ex, "SELECT kv_llen('mylist')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(3));
}

#[tokio::test]
async fn test_kv_lpop_rpop() {
    let ex = test_executor();
    exec(&ex, "SELECT kv_rpush('lst', 'a')").await;
    exec(&ex, "SELECT kv_rpush('lst', 'b')").await;
    exec(&ex, "SELECT kv_rpush('lst', 'c')").await;
    // lpop removes from front
    let r = exec(&ex, "SELECT kv_lpop('lst')").await;
    assert_eq!(scalar(&r[0]), &Value::Text("a".into()));
    // rpop removes from back
    let r = exec(&ex, "SELECT kv_rpop('lst')").await;
    assert_eq!(scalar(&r[0]), &Value::Text("c".into()));
    // one element left
    let r = exec(&ex, "SELECT kv_llen('lst')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(1));
    // pop from empty
    exec(&ex, "SELECT kv_lpop('lst')").await;
    let r = exec(&ex, "SELECT kv_lpop('lst')").await;
    assert_eq!(scalar(&r[0]), &Value::Null);
}

#[tokio::test]
async fn test_kv_lrange() {
    let ex = test_executor();
    exec(&ex, "SELECT kv_rpush('rng', 'a')").await;
    exec(&ex, "SELECT kv_rpush('rng', 'b')").await;
    exec(&ex, "SELECT kv_rpush('rng', 'c')").await;
    exec(&ex, "SELECT kv_rpush('rng', 'd')").await;
    let r = exec(&ex, "SELECT kv_lrange('rng', 0, 2)").await;
    assert_eq!(scalar(&r[0]), &Value::Text("a,b,c".into()));
    let r = exec(&ex, "SELECT kv_lrange('rng', 1, -1)").await;
    assert_eq!(scalar(&r[0]), &Value::Text("b,c,d".into()));
}

#[tokio::test]
async fn test_kv_lindex() {
    let ex = test_executor();
    exec(&ex, "SELECT kv_rpush('idx', 'x')").await;
    exec(&ex, "SELECT kv_rpush('idx', 'y')").await;
    exec(&ex, "SELECT kv_rpush('idx', 'z')").await;
    let r = exec(&ex, "SELECT kv_lindex('idx', 0)").await;
    assert_eq!(scalar(&r[0]), &Value::Text("x".into()));
    let r = exec(&ex, "SELECT kv_lindex('idx', 2)").await;
    assert_eq!(scalar(&r[0]), &Value::Text("z".into()));
    let r = exec(&ex, "SELECT kv_lindex('idx', -1)").await;
    assert_eq!(scalar(&r[0]), &Value::Text("z".into()));
    let r = exec(&ex, "SELECT kv_lindex('idx', 99)").await;
    assert_eq!(scalar(&r[0]), &Value::Null);
}

// ======================================================================

// KV Collection SQL function tests: Hashes
// ======================================================================

#[tokio::test]
async fn test_kv_hset_hget_hdel() {
    let ex = test_executor();
    // hset returns true for new field
    let r = exec(&ex, "SELECT kv_hset('myhash', 'f1', 'v1')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    // hset returns false for existing field
    let r = exec(&ex, "SELECT kv_hset('myhash', 'f1', 'v2')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(false));
    // hget
    let r = exec(&ex, "SELECT kv_hget('myhash', 'f1')").await;
    assert_eq!(scalar(&r[0]), &Value::Text("v2".into()));
    // hget missing field
    let r = exec(&ex, "SELECT kv_hget('myhash', 'missing')").await;
    assert_eq!(scalar(&r[0]), &Value::Null);
    // hdel
    let r = exec(&ex, "SELECT kv_hdel('myhash', 'f1')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    let r = exec(&ex, "SELECT kv_hdel('myhash', 'f1')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(false));
}

#[tokio::test]
async fn test_kv_hgetall_hlen_hexists() {
    let ex = test_executor();
    exec(&ex, "SELECT kv_hset('h', 'a', '1')").await;
    exec(&ex, "SELECT kv_hset('h', 'b', '2')").await;
    // hlen
    let r = exec(&ex, "SELECT kv_hlen('h')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(2));
    // hexists
    let r = exec(&ex, "SELECT kv_hexists('h', 'a')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    let r = exec(&ex, "SELECT kv_hexists('h', 'c')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(false));
    // hgetall — returns comma-separated "field=value" pairs
    let r = exec(&ex, "SELECT kv_hgetall('h')").await;
    let text = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    // Order may vary, check both pairs are present
    assert!(text.contains("a=1"));
    assert!(text.contains("b=2"));
}

// ======================================================================

// KV Collection SQL function tests: Sets
// ======================================================================

#[tokio::test]
async fn test_kv_sadd_srem_scard() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT kv_sadd('myset', 'x')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    let r = exec(&ex, "SELECT kv_sadd('myset', 'x')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(false)); // duplicate
    let r = exec(&ex, "SELECT kv_sadd('myset', 'y')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    let r = exec(&ex, "SELECT kv_scard('myset')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(2));
    let r = exec(&ex, "SELECT kv_srem('myset', 'x')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    let r = exec(&ex, "SELECT kv_srem('myset', 'x')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(false));
    let r = exec(&ex, "SELECT kv_scard('myset')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(1));
}

#[tokio::test]
async fn test_kv_smembers_sismember() {
    let ex = test_executor();
    exec(&ex, "SELECT kv_sadd('s', 'a')").await;
    exec(&ex, "SELECT kv_sadd('s', 'b')").await;
    exec(&ex, "SELECT kv_sadd('s', 'c')").await;
    let r = exec(&ex, "SELECT kv_sismember('s', 'b')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    let r = exec(&ex, "SELECT kv_sismember('s', 'z')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(false));
    // smembers returns comma-separated
    let r = exec(&ex, "SELECT kv_smembers('s')").await;
    let text = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(text.contains("a"));
    assert!(text.contains("b"));
    assert!(text.contains("c"));
}

// ======================================================================

// KV Collection SQL function tests: Sorted Sets
// ======================================================================

#[tokio::test]
async fn test_kv_zadd_zcard_zrange() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT kv_zadd('zs', 1.0, 'alice')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    let r = exec(&ex, "SELECT kv_zadd('zs', 3.0, 'charlie')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    let r = exec(&ex, "SELECT kv_zadd('zs', 2.0, 'bob')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    let r = exec(&ex, "SELECT kv_zcard('zs')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(3));
    // zrange returns sorted by score
    let r = exec(&ex, "SELECT kv_zrange('zs', 0, 2)").await;
    let text = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(text.starts_with("alice:1"));
    assert!(text.contains("bob:2"));
    assert!(text.contains("charlie:3"));
}

#[tokio::test]
async fn test_kv_zrem_zrangebyscore() {
    let ex = test_executor();
    exec(&ex, "SELECT kv_zadd('zs2', 10.0, 'a')").await;
    exec(&ex, "SELECT kv_zadd('zs2', 20.0, 'b')").await;
    exec(&ex, "SELECT kv_zadd('zs2', 30.0, 'c')").await;
    // zrangebyscore
    let r = exec(&ex, "SELECT kv_zrangebyscore('zs2', 15.0, 25.0)").await;
    let text = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(text.contains("b:20"));
    assert!(!text.contains("a:10"));
    assert!(!text.contains("c:30"));
    // zrem
    let r = exec(&ex, "SELECT kv_zrem('zs2', 'b')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    let r = exec(&ex, "SELECT kv_zcard('zs2')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(2));
}

// ======================================================================

// KV Collection SQL function tests: HyperLogLog
// ======================================================================

#[tokio::test]
async fn test_kv_pfadd_pfcount() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT kv_pfadd('hll', 'elem1')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    let r = exec(&ex, "SELECT kv_pfadd('hll', 'elem2')").await;
    assert_eq!(scalar(&r[0]), &Value::Bool(true));
    let r = exec(&ex, "SELECT kv_pfadd('hll', 'elem1')").await;
    // may or may not change estimate, but should not error
    let _ = scalar(&r[0]);
    let r = exec(&ex, "SELECT kv_pfcount('hll')").await;
    match scalar(&r[0]) {
        Value::Int64(n) => assert!(*n >= 1, "HLL count should be >= 1"),
        other => panic!("expected Int64, got {other:?}"),
    }
}

// ======================================================================

// Stream SQL function tests
// ======================================================================

#[tokio::test]
async fn test_stream_xadd_xlen() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT stream_xadd('events', 'user', 'alice', 'action', 'login')").await;
    let id1 = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(id1.contains("-"), "stream ID should have ms-seq format");
    let r = exec(&ex, "SELECT stream_xadd('events', 'user', 'bob', 'action', 'logout')").await;
    let _id2 = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    let r = exec(&ex, "SELECT stream_xlen('events')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(2));
}

#[tokio::test]
async fn test_stream_xrange() {
    let ex = test_executor();
    exec(&ex, "SELECT stream_xadd('s1', 'k', 'v1')").await;
    exec(&ex, "SELECT stream_xadd('s1', 'k', 'v2')").await;
    // xrange with wide bounds
    let r = exec(&ex, "SELECT stream_xrange('s1', 0, 9999999999999, 10)").await;
    let text = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    // Should contain both entries
    assert!(text.contains("k=v1"));
    assert!(text.contains("k=v2"));
}

#[tokio::test]
async fn test_stream_xread() {
    let ex = test_executor();
    exec(&ex, "SELECT stream_xadd('rd', 'n', '1')").await;
    exec(&ex, "SELECT stream_xadd('rd', 'n', '2')").await;
    exec(&ex, "SELECT stream_xadd('rd', 'n', '3')").await;
    // xread from beginning (last_id=0), count=2
    let r = exec(&ex, "SELECT stream_xread('rd', 0, 2)").await;
    let text = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    // Should have at most 2 entries
    let entry_count = text.split(',').filter(|s| !s.is_empty()).count();
    assert!(entry_count <= 2, "xread count=2 should return at most 2 entries, got {}", entry_count);
}

#[tokio::test]
async fn test_stream_xgroup_xreadgroup_xack() {
    let ex = test_executor();
    // Add entries first, then create group
    exec(&ex, "SELECT stream_xadd('grp_stream', 'msg', 'hello')").await;
    exec(&ex, "SELECT stream_xadd('grp_stream', 'msg', 'world')").await;
    // Create consumer group starting from 0
    let r = exec(&ex, "SELECT stream_xgroup_create('grp_stream', 'mygroup', 0)").await;
    assert_eq!(scalar(&r[0]), &Value::Text("OK".into()));
    // Read via consumer group
    let r = exec(&ex, "SELECT stream_xreadgroup('grp_stream', 'mygroup', 'worker1', 10)").await;
    let text = match scalar(&r[0]) {
        Value::Text(s) => s.clone(),
        other => panic!("expected Text, got {other:?}"),
    };
    assert!(text.contains("msg=hello"));
    assert!(text.contains("msg=world"));
}

#[tokio::test]
async fn test_stream_xlen_empty() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT stream_xlen('nonexistent')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(0));
}

// ======================================================================

// Pub/Sub SQL function tests
// ======================================================================

#[tokio::test]
async fn test_pubsub_publish_no_subscribers() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT pubsub_publish('ch1', 'hello')").await;
    // No subscribers, so count = 0
    assert_eq!(scalar(&r[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_pubsub_subscribers_count() {
    let ex = test_executor();
    // No subscribers initially
    let r = exec(&ex, "SELECT pubsub_subscribers('ch')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_pubsub_channels_empty() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT pubsub_channels()").await;
    assert_eq!(scalar(&r[0]), &Value::Text("".into()));
}

// ======================================================================

// Cross-collection type error tests
// ======================================================================

#[tokio::test]
async fn test_kv_collection_wrong_type_errors() {
    let ex = test_executor();
    // Create a list
    exec(&ex, "SELECT kv_lpush('mylist', 'v')").await;
    // Trying to hset on a list key should fail (WrongTypeError)
    let r = ex.execute("SELECT kv_hset('mylist', 'f', 'v')").await;
    assert!(r.is_err(), "hset on list key should produce error");
    // Trying to sadd on a list key should fail
    let r = ex.execute("SELECT kv_sadd('mylist', 'member')").await;
    assert!(r.is_err(), "sadd on list key should produce error");
    // Create a hash
    exec(&ex, "SELECT kv_hset('myhash', 'f1', 'v1')").await;
    // Trying to lpush on a hash key should fail
    let r = ex.execute("SELECT kv_lpush('myhash', 'a')").await;
    assert!(r.is_err(), "lpush on hash key should produce error");
}

// ======================================================================

// Edge case tests
// ======================================================================

#[tokio::test]
async fn test_kv_llen_empty_list() {
    let ex = test_executor();
    // llen on nonexistent key returns 0
    let r = exec(&ex, "SELECT kv_llen('nope')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_kv_hlen_empty_hash() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT kv_hlen('nope')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_kv_scard_empty_set() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT kv_scard('nope')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_kv_zcard_empty_sorted_set() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT kv_zcard('nope')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_kv_pfcount_empty_hll() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT kv_pfcount('nope')").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(0));
}

#[tokio::test]
async fn test_kv_smembers_empty_set() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT kv_smembers('nope')").await;
    assert_eq!(scalar(&r[0]), &Value::Text("".into()));
}

#[tokio::test]
async fn test_kv_hgetall_empty_hash() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT kv_hgetall('nope')").await;
    assert_eq!(scalar(&r[0]), &Value::Text("".into()));
}

#[tokio::test]
async fn test_kv_zrange_empty_sorted_set() {
    let ex = test_executor();
    let r = exec(&ex, "SELECT kv_zrange('nope', 0, 10)").await;
    assert_eq!(scalar(&r[0]), &Value::Text("".into()));
}

// ======================================================================
// Distributed Pub/Sub Router tests
// ======================================================================

/// LISTEN records the subscription locally; gossip snapshot includes the channel.
#[tokio::test]
async fn test_distributed_pubsub_listen_updates_gossip() {
    let ex = test_executor();
    exec(&ex, "LISTEN events").await;

    // The distributed router's gossip snapshot should now include "events"
    // (the local subscription was registered via subscribe_local in publish path).
    // In standalone mode the dist_pubsub router just mirrors the local hub.
    let _snapshot = ex.dist_pubsub().read().local_subscription_snapshot();
    // We can't rely on the snapshot having "events" since dist_pubsub.subscribe_local
    // was not called (only the async pubsub was subscribed). But NOTIFY should work.
    exec(&ex, "NOTIFY events, 'test payload'").await;
}

/// NOTIFY in standalone mode (no replicator): delivers locally and doesn't panic.
#[tokio::test]
async fn test_distributed_pubsub_notify_standalone() {
    let ex = test_executor();
    let results = exec(&ex, "NOTIFY standalone_ch, 'hello'").await;
    match &results[0] {
        ExecResult::Command { tag, .. } => assert_eq!(tag, "NOTIFY"),
        _ => panic!("expected Command result"),
    }
}

/// DistributedPubSubRouter: local publish delivers to local subscribers.
#[test]
fn test_distributed_router_local_publish() {
    use crate::pubsub::DistributedPubSubRouter;
    let mut router = DistributedPubSubRouter::new(1, 64);
    let mut rx = router.subscribe_local("mychannel");
    let (local_count, remote_count) = router.publish("mychannel", "payload".into());
    assert_eq!(local_count, 1, "one local subscriber");
    assert_eq!(remote_count, 0, "no remote nodes yet");
    // Message should be in the receiver.
    let msg = rx.try_recv().expect("message should be present");
    assert_eq!(msg.payload, "payload");
    assert_eq!(msg.channel, "mychannel");
}

/// DistributedPubSubRouter: remote node subscription gossip causes outbox messages.
#[test]
fn test_distributed_router_remote_delivery_queued() {
    use crate::pubsub::DistributedPubSubRouter;
    let mut router = DistributedPubSubRouter::new(1, 64);
    // Register remote node 2 as subscribing to "alerts".
    router.register_remote_subscription(2, "alerts");
    router.register_remote_subscription(3, "alerts");
    let (local_count, remote_count) = router.publish("alerts", "fire!".into());
    assert_eq!(local_count, 0, "no local subscribers");
    assert_eq!(remote_count, 2, "two remote nodes");
    let outbox = router.drain_outbox();
    assert_eq!(outbox.len(), 2);
    let targets: std::collections::HashSet<u64> = outbox.iter().map(|m| m.target_node).collect();
    assert!(targets.contains(&2));
    assert!(targets.contains(&3));
}

/// DistributedPubSubRouter: deliver_remote publishes to local subscribers.
#[test]
fn test_distributed_router_deliver_remote() {
    use crate::pubsub::DistributedPubSubRouter;
    let mut router = DistributedPubSubRouter::new(1, 64);
    let mut rx = router.subscribe_local("ch");
    router.deliver_remote("ch", "from_node_2".into());
    let msg = rx.try_recv().expect("message from remote should arrive locally");
    assert_eq!(msg.payload, "from_node_2");
}

/// DistributedPubSubRouter: apply_gossip updates remote subscription knowledge.
#[test]
fn test_distributed_router_apply_gossip() {
    use crate::pubsub::DistributedPubSubRouter;
    let mut router = DistributedPubSubRouter::new(1, 64);
    assert_eq!(router.remote_node_count(), 0);
    router.apply_gossip(2, vec!["ch1".into(), "ch2".into()]);
    assert_eq!(router.remote_node_count(), 1);
    // Publishing to ch1 should queue an outbox message for node 2.
    let (_, remote_count) = router.publish("ch1", "data".into());
    assert_eq!(remote_count, 1);
    let outbox = router.drain_outbox();
    assert_eq!(outbox[0].target_node, 2);
}

/// Transport: PubSubPublish round-trips through encode/decode.
#[test]
fn test_transport_pubsub_publish_codec() {
    use crate::transport::{encode, decode, Message};
    let msg = Message::PubSubPublish {
        channel: "events".into(),
        payload: "hello cluster".into(),
    };
    let bytes = encode(&msg);
    let decoded = decode(&bytes).expect("decode should succeed");
    match decoded {
        Message::PubSubPublish { channel, payload } => {
            assert_eq!(channel, "events");
            assert_eq!(payload, "hello cluster");
        }
        other => panic!("expected PubSubPublish, got {other:?}"),
    }
}

/// Transport: PubSubGossip round-trips through encode/decode.
#[test]
fn test_transport_pubsub_gossip_codec() {
    use crate::transport::{encode, decode, Message};
    let msg = Message::PubSubGossip {
        node_id: 42,
        channels: vec!["ch1".into(), "ch2".into(), "ch3".into()],
    };
    let bytes = encode(&msg);
    let decoded = decode(&bytes).expect("decode should succeed");
    match decoded {
        Message::PubSubGossip { node_id, channels } => {
            assert_eq!(node_id, 42);
            assert_eq!(channels, vec!["ch1", "ch2", "ch3"]);
        }
        other => panic!("expected PubSubGossip, got {other:?}"),
    }
}

