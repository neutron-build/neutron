use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use neutron::extract::State;
use neutron_jobs::{
    JobContext, JobResult, JobStore, JobWorker, MemoryJobStore, PersistentJobQueue, StoreError,
    StoredJob,
};

async fn wait_count(counter: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while counter.load(Ordering::SeqCst) != expected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("jobs did not execute");
}

async fn fencing(store: Arc<dyn JobStore>, queue: &str) {
    let id = store
        .push(StoredJob::new("job", queue, vec![], 3))
        .await
        .unwrap();
    let first = store.claim_due(queue, 1).await.unwrap().pop().unwrap();
    assert!(first.claim_token > 0);
    assert!(store.claim_due(queue, 1).await.unwrap().is_empty());
    tokio::time::sleep(Duration::from_millis(3)).await;
    store.recover_stale(0).await.unwrap();
    let second = store.claim_due(queue, 1).await.unwrap().pop().unwrap();
    assert_eq!(second.id, id);
    assert!(second.claim_token > first.claim_token);
    assert!(matches!(
        store.mark_completed(id, first.claim_token).await,
        Err(StoreError::StaleClaim(_))
    ));
    assert!(matches!(
        store.mark_failed(id, first.claim_token, "late").await,
        Err(StoreError::StaleClaim(_))
    ));
    assert!(matches!(
        store.schedule_retry(id, first.claim_token, 2, 0).await,
        Err(StoreError::StaleClaim(_))
    ));
    store
        .schedule_retry(id, second.claim_token, 3, 0)
        .await
        .unwrap();
    assert!(matches!(
        store.mark_completed(id, second.claim_token).await,
        Err(StoreError::StaleClaim(_))
    ));
    let third = store.claim_due(queue, 1).await.unwrap().pop().unwrap();
    assert_eq!(third.attempt, 3);
    store.mark_completed(id, third.claim_token).await.unwrap();
    assert!(store.claim_due(queue, 1).await.unwrap().is_empty());
    assert!(matches!(
        store.schedule_retry(id, third.claim_token, 4, 0).await,
        Err(StoreError::StaleClaim(_))
    ));
}

#[tokio::test]
async fn memory_fences_stale_owners() {
    fencing(Arc::new(MemoryJobStore::new()), "fence").await;
}

async fn delivery(store: Arc<dyn JobStore>, queue_name: &str) {
    let future = StoredJob::new("job", queue_name, vec![], 3).with_delay_ms(25);
    store.push(future).await.unwrap();
    let queue = PersistentJobQueue::with_stale_timeout(store.clone(), 300)
        .await
        .unwrap()
        .with_queues([queue_name]);
    let counter = Arc::new(AtomicUsize::new(0));
    let worker = JobWorker::new(queue.queue())
        .with_store(store.clone())
        .state(counter.clone())
        .job(
            "job",
            |State(counter): State<Arc<AtomicUsize>>, ctx: JobContext| async move {
                counter.fetch_add(1, Ordering::SeqCst);
                if ctx.attempt == 1 {
                    JobResult::retry_after(Duration::from_millis(15))
                } else {
                    JobResult::Ok
                }
            },
        );
    let task = tokio::spawn(worker.run());
    wait_count(&counter, 2).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn existing_future_named_jobs_and_persisted_retries_are_admitted() {
    delivery(Arc::new(MemoryJobStore::new()), "named").await;
}

#[tokio::test]
async fn two_workers_drain_more_than_startup_limit_without_double_execution() {
    let store = Arc::new(MemoryJobStore::new());
    let a = PersistentJobQueue::new(store.clone()).await.unwrap();
    let b = PersistentJobQueue::new(store.clone()).await.unwrap();
    let executions = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for queue in [a.queue(), b.queue()] {
        let seen = executions.clone();
        let counter = counter.clone();
        handles.push(tokio::spawn(
            JobWorker::new(queue)
                .with_store(store.clone())
                .job("job", move |ctx: JobContext| {
                    let seen = seen.clone();
                    let counter = counter.clone();
                    async move {
                        assert!(seen.lock().unwrap().insert(ctx.job_id));
                        counter.fetch_add(1, Ordering::SeqCst);
                        JobResult::Ok
                    }
                })
                .run(),
        ));
    }
    for _ in 0..10_001 {
        a.enqueue("job", vec![]).await.unwrap();
    }
    wait_count(&counter, 10_001).await;
    assert_eq!(executions.lock().unwrap().len(), 10_001);
    for handle in handles {
        handle.abort();
        let _ = handle.await;
    }
}

#[tokio::test]
async fn recovery_exhaustion_does_not_redeliver_forever() {
    let store = Arc::new(MemoryJobStore::new());
    store
        .push(StoredJob::new("job", "default", vec![], 1))
        .await
        .unwrap();
    store.claim_due("default", 1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(3)).await;
    store.recover_stale(0).await.unwrap();
    assert!(store.claim_due("default", 1).await.unwrap().is_empty());
}

#[cfg(feature = "redis")]
#[tokio::test]
#[ignore = "requires disposable Redis via NEUTRON_JOBS_TEST_REDIS"]
async fn live_redis_atomic_fencing_and_delivery() {
    let url = std::env::var("NEUTRON_JOBS_TEST_REDIS").expect("disposable Redis URL required");
    let store = Arc::new(neutron_jobs::RedisJobStore::new(&url).await.unwrap());
    fencing(store.clone(), "audit-fencing").await;
    concurrent_claims(store.clone(), "audit-concurrent").await;
    delivery(store.clone(), "audit-delivery").await;
    redis_failure_preserves_state(store, &url).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "requires disposable PostgreSQL via NEUTRON_JOBS_TEST_POSTGRES"]
async fn live_postgres_atomic_fencing_and_delivery() {
    let url =
        std::env::var("NEUTRON_JOBS_TEST_POSTGRES").expect("disposable PostgreSQL URL required");
    let store = Arc::new(neutron_jobs::PostgresJobStore::new(&url, 4).await.unwrap());
    fencing(store.clone(), "audit-fencing").await;
    concurrent_claims(store.clone(), "audit-concurrent").await;
    delivery(store, "audit-delivery").await;
}

#[tokio::test]
async fn delayed_enqueue_executes_and_claims_only_available_capacity() {
    let store = Arc::new(MemoryJobStore::new());
    let queue = PersistentJobQueue::new(store.clone()).await.unwrap();
    let started = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let gate_handler = gate.clone();
    let counter = started.clone();
    let handle = tokio::spawn(
        JobWorker::new(queue.queue())
            .with_store(store.clone())
            .concurrency(1)
            .job("job", move || {
                let gate = gate_handler.clone();
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    let permit = gate.acquire().await.unwrap();
                    permit.forget();
                    JobResult::Ok
                }
            })
            .run(),
    );
    queue
        .enqueue_delayed("job", vec![], Duration::from_millis(25))
        .await
        .unwrap();
    wait_count(&started, 1).await;
    queue.enqueue("job", vec![]).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    // While the sole handler slot is occupied, the second job remains unclaimed.
    let pending = store.claim_due("default", 1).await.unwrap();
    assert_eq!(pending.len(), 1);
    store
        .mark_completed(pending[0].id, pending[0].claim_token)
        .await
        .unwrap();
    gate.add_permits(1);
    handle.abort();
    let _ = handle.await;
}

async fn concurrent_claims(store: Arc<dyn JobStore>, queue: &str) {
    for _ in 0..20 {
        store
            .push(StoredJob::new("job", queue, vec![], 3))
            .await
            .unwrap();
    }
    let (left, right) = tokio::join!(store.claim_due(queue, 20), store.claim_due(queue, 20));
    let mut ids = std::collections::HashSet::new();
    for job in left.unwrap().into_iter().chain(right.unwrap()) {
        assert!(ids.insert(job.id), "duplicate current claim");
        store.mark_completed(job.id, job.claim_token).await.unwrap();
    }
    assert_eq!(ids.len(), 20);
}

#[cfg(feature = "redis")]
async fn redis_failure_preserves_state(store: Arc<neutron_jobs::RedisJobStore>, url: &str) {
    use redis::AsyncCommands;
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let id = store
        .push(StoredJob::new("job", "audit-wrongtype", vec![], 3))
        .await
        .unwrap();
    let job = store
        .claim_due("audit-wrongtype", 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let _: () = conn
        .set("jobs:pending:audit-wrongtype", "corrupt-index")
        .await
        .unwrap();
    assert!(store
        .schedule_retry(id, job.claim_token, 2, 0)
        .await
        .is_err());
    let status: String = conn
        .hget(format!("jobs:data:{id}"), "status")
        .await
        .unwrap();
    assert_eq!(status, "running");
    let running: Option<f64> = conn.zscore("jobs:running", id).await.unwrap();
    assert!(running.is_some());
    tokio::time::sleep(Duration::from_millis(3)).await;
    assert!(store.recover_stale(0).await.is_err());
    let status: String = conn
        .hget(format!("jobs:data:{id}"), "status")
        .await
        .unwrap();
    assert_eq!(status, "running");
    let before: Vec<String> = conn.keys("jobs:data:*").await.unwrap();
    assert!(store
        .push(StoredJob::new("job", "audit-wrongtype", vec![], 3))
        .await
        .is_err());
    let after: Vec<String> = conn.keys("jobs:data:*").await.unwrap();
    assert_eq!(
        before.len(),
        after.len(),
        "failed push must not leave an orphan hash"
    );
    let _: () = conn.del("jobs:pending:audit-wrongtype").await.unwrap();
    store.recover_stale(0).await.unwrap();
    let recovered = store
        .claim_due("audit-wrongtype", 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    store
        .mark_completed(id, recovered.claim_token)
        .await
        .unwrap();
}

#[tokio::test]
async fn cancelling_worker_aborts_owned_handlers_and_recovers_claim() {
    let store = Arc::new(MemoryJobStore::new());
    let queue = PersistentJobQueue::new(store.clone()).await.unwrap();
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let started_handler = started.clone();
    let completed_handler = completed.clone();
    let gate_handler = gate.clone();
    let worker = JobWorker::new(queue.queue())
        .with_store(store.clone())
        .job("job", move || {
            let started = started_handler.clone();
            let completed = completed_handler.clone();
            let gate = gate_handler.clone();
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                let _permit = gate.acquire().await.unwrap();
                completed.fetch_add(1, Ordering::SeqCst);
                JobResult::Ok
            }
        });
    let active = worker.active();
    let handle = tokio::spawn(worker.run());
    let id = queue.enqueue("job", vec![]).await.unwrap();
    wait_count(&started, 1).await;
    handle.abort();
    let _ = handle.await;
    wait_count(&active, 0).await;
    gate.add_permits(1);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(completed.load(Ordering::SeqCst), 0);
    store.recover_stale(0).await.unwrap();
    let recovered = store.claim_due("default", 1).await.unwrap().pop().unwrap();
    assert_eq!(recovered.id, id);
    assert_eq!(recovered.attempt, 2);
}

#[cfg(feature = "redis")]
#[tokio::test]
#[ignore = "requires isolated disposable standalone Redis; run durable tests serially"]
async fn redis_integer_preflight_rejects_corrupt_second_record_before_any_mutation() {
    use redis::AsyncCommands;
    let url = std::env::var("NEUTRON_JOBS_TEST_REDIS").expect("isolated disposable Redis required");
    let store = neutron_jobs::RedisJobStore::new(&url).await.unwrap();
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    for bad in [
        "1.5",
        "9223372036854775807",
        "9223372036854775808",
        "01",
        "+1",
    ] {
        let queue = format!("review-token-{bad}");
        let first = store
            .push(StoredJob::new("job", &queue, vec![], 3))
            .await
            .unwrap();
        let second = store
            .push(StoredJob::new("job", &queue, vec![], 3))
            .await
            .unwrap();
        let _: () = conn
            .hset(format!("jobs:data:{second}"), "claim_token", bad)
            .await
            .unwrap();
        assert!(store.claim_due(&queue, 2).await.is_err());
        for id in [first, second] {
            let status: String = conn
                .hget(format!("jobs:data:{id}"), "status")
                .await
                .unwrap();
            assert_eq!(status, "pending");
            let pending: Option<f64> = conn
                .zscore(format!("jobs:pending:{queue}"), id)
                .await
                .unwrap();
            assert!(pending.is_some());
            let running: Option<f64> = conn.zscore("jobs:running", id).await.unwrap();
            assert!(running.is_none());
        }
        let _: () = conn
            .hset(format!("jobs:data:{second}"), "claim_token", "0")
            .await
            .unwrap();
        for job in store.claim_due(&queue, 2).await.unwrap() {
            store.mark_completed(job.id, job.claim_token).await.unwrap();
        }
    }
    for bad in ["1.5", "4294967295", "9223372036854775807", "-1"] {
        let queue = format!("review-attempt-{bad}");
        let first = store
            .push(StoredJob::new("job", &queue, vec![], 3))
            .await
            .unwrap();
        let second = store
            .push(StoredJob::new("job", &queue, vec![], 3))
            .await
            .unwrap();
        let claimed = store.claim_due(&queue, 2).await.unwrap();
        let _: () = conn
            .hset(format!("jobs:data:{second}"), "attempt", bad)
            .await
            .unwrap();
        assert!(store.recover_stale(0).await.is_err());
        for id in [first, second] {
            let status: String = conn
                .hget(format!("jobs:data:{id}"), "status")
                .await
                .unwrap();
            assert_eq!(status, "running");
            let running: Option<f64> = conn.zscore("jobs:running", id).await.unwrap();
            assert!(running.is_some());
        }
        let first_attempt: String = conn
            .hget(format!("jobs:data:{first}"), "attempt")
            .await
            .unwrap();
        assert_eq!(first_attempt, "1");
        let _: () = conn
            .hset(format!("jobs:data:{second}"), "attempt", "1")
            .await
            .unwrap();
        for job in claimed {
            store.mark_completed(job.id, job.claim_token).await.unwrap();
        }
    }
}

#[cfg(feature = "redis")]
#[tokio::test]
#[ignore = "requires isolated disposable standalone Redis; run durable tests serially"]
async fn redis_utf8_preflight_and_binary_extras_preserve_batch_ownership() {
    use redis::AsyncCommands;
    async fn snapshot(
        conn: &mut redis::aio::MultiplexedConnection,
        ids: [u64; 2],
        queue: &str,
    ) -> Vec<(
        std::collections::HashMap<Vec<u8>, Vec<u8>>,
        Option<f64>,
        Option<f64>,
    )> {
        let mut result = Vec::new();
        for id in ids {
            result.push((
                conn.hgetall(format!("jobs:data:{id}")).await.unwrap(),
                conn.zscore(format!("jobs:pending:{queue}"), id)
                    .await
                    .unwrap(),
                conn.zscore("jobs:running", id).await.unwrap(),
            ));
        }
        result
    }
    let url = std::env::var("NEUTRON_JOBS_TEST_REDIS").expect("isolated disposable Redis required");
    let store = neutron_jobs::RedisJobStore::new(&url).await.unwrap();
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let queue = "review-utf8";
    let first = store
        .push(StoredJob::new("valid", queue, vec![], 3))
        .await
        .unwrap();
    let second = store
        .push(StoredJob::new("valid", queue, vec![], 3))
        .await
        .unwrap();
    let key = format!("jobs:data:{second}");
    // Unknown binary field names AND values must never be decoded as job text.
    let _: () = conn.hset(&key, vec![0xffu8], vec![0xfeu8]).await.unwrap();
    for bad in [
        vec![0xff],
        vec![0xc0, 0x80],
        vec![0xed, 0xa0, 0x80],
        vec![0xf4, 0x90, 0x80, 0x80],
        vec![0xe2, 0x82],
    ] {
        let _: () = conn.hset(&key, "job_type", bad).await.unwrap();
        let before = snapshot(&mut conn, [first, second], queue).await;
        assert!(store.claim_due(queue, 2).await.is_err());
        assert_eq!(snapshot(&mut conn, [first, second], queue).await, before);
    }
    let _: () = conn.hset(&key, "job_type", "valid-🐾").await.unwrap();
    let claimed = store.claim_due(queue, 2).await.unwrap();
    assert_eq!(claimed.len(), 2);
    assert!(claimed
        .iter()
        .any(|j| j.id == second && j.job_type == "valid-🐾"));
    for field in ["job_type", "queue"] {
        let original: Vec<u8> = conn.hget(&key, field).await.unwrap();
        let _: () = conn.hset(&key, field, vec![0xffu8]).await.unwrap();
        let before = snapshot(&mut conn, [first, second], queue).await;
        assert!(store.recover_stale(0).await.is_err());
        assert_eq!(snapshot(&mut conn, [first, second], queue).await, before);
        let _: () = conn.hset(&key, field, original).await.unwrap();
    }
    let recovered = store.recover_stale(0).await.unwrap();
    assert_eq!(recovered.len(), 2);
    for job in &claimed {
        let recovered_job = recovered.iter().find(|j| j.id == job.id).unwrap();
        assert_eq!(recovered_job.claim_token, job.claim_token);
        assert_eq!(recovered_job.attempt, job.attempt + 1);
        let pending: Option<f64> = conn
            .zscore(format!("jobs:pending:{queue}"), job.id)
            .await
            .unwrap();
        let running: Option<f64> = conn.zscore("jobs:running", job.id).await.unwrap();
        assert!(pending.is_some());
        assert!(running.is_none());
    }
    for job in store.claim_due(queue, 2).await.unwrap() {
        store.mark_completed(job.id, job.claim_token).await.unwrap();
    }
}
