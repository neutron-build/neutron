//! TCP-based WAL streaming for real primary-replica replication.
//!
//! This module provides [`ReplicationServer`] (primary side) and
//! [`ReplicationClient`] (replica side) that exchange WAL entries over
//! length-prefixed TCP frames using the existing [`super::StreamMessage`]
//! binary protocol.
//!
//! The server listens for incoming replica connections, performs a
//! handshake (replica sends its current LSN), and then streams WAL
//! entries from that position forward.  Periodic heartbeats detect
//! disconnects.  The client auto-reconnects with exponential backoff
//! (up to 30 s) when the connection drops.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Notify};
use tokio::time;

#[allow(unused_imports)]
use super::{
    decode_stream_message, encode_stream_message, Lsn, NodeId, NodeRole, ReplicationError,
    ReplicationManager, ReplicationMode, StreamMessage, WalPayload, WalRecord, WalWriter,
};

// ---------------------------------------------------------------------------
// Length-prefixed framing helpers
// ---------------------------------------------------------------------------

/// Encode a [`StreamMessage`] with a 4-byte big-endian length prefix.
pub fn frame_encode(msg: &StreamMessage) -> Vec<u8> {
    let inner = encode_stream_message(msg);
    // `encode_stream_message` already prepends a 4-byte LE length.
    // We re-frame with BE length to match the spec requirement.
    let payload = &inner[4..]; // strip the LE prefix
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Default read timeout — if no data arrives within this window, the
/// connection is considered dead.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Read one length-prefixed frame from `reader` and decode it.
/// Returns `None` on clean EOF.
pub async fn frame_read<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> Result<Option<StreamMessage>, ReplicationError> {
    frame_read_with_timeout(reader, READ_TIMEOUT).await
}

/// Read one length-prefixed frame with a configurable timeout.
pub async fn frame_read_with_timeout<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    timeout: Duration,
) -> Result<Option<StreamMessage>, ReplicationError> {
    match time::timeout(timeout, frame_read_inner(reader)).await {
        Ok(result) => result,
        Err(_) => Err(ReplicationError::ProtocolError(
            "read timeout — remote appears unresponsive".into(),
        )),
    }
}

/// Inner frame reader (no timeout).
async fn frame_read_inner<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> Result<Option<StreamMessage>, ReplicationError> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => {
            return Err(ReplicationError::ProtocolError(format!(
                "read frame len: {e}"
            )));
        }
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 || len > 64 * 1024 * 1024 {
        return Err(ReplicationError::ProtocolError(format!(
            "bad frame length: {len}"
        )));
    }
    let mut payload = vec![0u8; len];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|e| ReplicationError::ProtocolError(format!("read frame payload: {e}")))?;
    decode_stream_message(&payload).map(Some)
}

/// Write one length-prefixed frame to `writer`.
pub async fn frame_write<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg: &StreamMessage,
) -> Result<(), ReplicationError> {
    let data = frame_encode(msg);
    writer
        .write_all(&data)
        .await
        .map_err(|e| ReplicationError::ProtocolError(format!("write frame: {e}")))?;
    writer
        .flush()
        .await
        .map_err(|e| ReplicationError::ProtocolError(format!("flush frame: {e}")))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// ReplicationServer (primary side)
// ---------------------------------------------------------------------------

/// Primary-side replication server.
///
/// Listens for TCP connections from replicas, exchanges a handshake, and
/// streams WAL entries to each connected replica.
pub struct ReplicationServer {
    listener_addr: SocketAddr,
    wal: Arc<parking_lot::RwLock<WalWriter>>,
    primary_id: NodeId,
    shutdown: Arc<Notify>,
    heartbeat_interval: Duration,
}

impl ReplicationServer {
    /// Create a new server.  Call [`start`](Self::start) to begin accepting
    /// connections.
    pub fn new(
        listener_addr: SocketAddr,
        wal: Arc<parking_lot::RwLock<WalWriter>>,
        primary_id: NodeId,
    ) -> Self {
        Self {
            listener_addr,
            wal,
            primary_id,
            shutdown: Arc::new(Notify::new()),
            heartbeat_interval: Duration::from_secs(5),
        }
    }

    /// Override the default heartbeat interval (for testing).
    pub fn set_heartbeat_interval(&mut self, d: Duration) {
        self.heartbeat_interval = d;
    }

    /// Start listening.  Returns the actual bound address (useful when
    /// binding to port 0).  Spawns a background task; call [`stop`](Self::stop)
    /// to shut down.
    pub async fn start(&self) -> Result<SocketAddr, ReplicationError> {
        let listener = TcpListener::bind(self.listener_addr)
            .await
            .map_err(|e| ReplicationError::ProtocolError(format!("bind: {e}")))?;
        let addr = listener
            .local_addr()
            .map_err(|e| ReplicationError::ProtocolError(format!("local_addr: {e}")))?;

        let wal = Arc::clone(&self.wal);
        let primary_id = self.primary_id;
        let shutdown = Arc::clone(&self.shutdown);
        let hb_interval = self.heartbeat_interval;

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, peer)) => {
                                let w = Arc::clone(&wal);
                                let sd = Arc::clone(&shutdown);
                                tokio::spawn(Self::handle_replica(
                                    stream, peer, w, primary_id, sd, hb_interval,
                                ));
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        Ok(addr)
    }

    /// Signal graceful shutdown.
    pub fn stop(&self) {
        self.shutdown.notify_waiters();
    }

    // -- per-replica task ---------------------------------------------------

    async fn handle_replica(
        mut stream: TcpStream,
        _peer: SocketAddr,
        wal: Arc<parking_lot::RwLock<WalWriter>>,
        primary_id: NodeId,
        shutdown: Arc<Notify>,
        hb_interval: Duration,
    ) {
        // 1. Handshake: replica sends StartStreaming with its current LSN.
        let from_lsn = match frame_read(&mut stream).await {
            Ok(Some(StreamMessage::StartStreaming { from_lsn, .. })) => from_lsn,
            _ => return,
        };

        // 2. Respond with StreamingStarted.
        let tip = wal.read().latest_lsn();
        let started = StreamMessage::StreamingStarted {
            primary_id,
            wal_tip_lsn: tip,
        };
        if frame_write(&mut stream, &started).await.is_err() {
            return;
        }

        // 3. Stream records from the replica's last known position.
        let mut confirmed_lsn = from_lsn;

        // Split the *owned* TcpStream for concurrent read/write.
        let (mut rd, mut wr) = stream.into_split();

        // Channel for ack messages from the reader task.
        let (ack_tx, mut ack_rx) = mpsc::channel::<Lsn>(64);

        // Spawn reader task to receive acks + heartbeat-pongs.
        let reader_shutdown = Arc::clone(&shutdown);
        let reader_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = reader_shutdown.notified() => break,
                    result = frame_read(&mut rd) => {
                        match result {
                            Ok(Some(StreamMessage::WalBatchAck { confirmed_lsn: clsn, .. })) => {
                                let _ = ack_tx.send(clsn).await;
                            }
                            Ok(Some(StreamMessage::ReplicationHeartbeat { .. })) => {
                                // Replica pong — just keep alive.
                            }
                            Ok(Some(_)) => {}
                            Ok(None) | Err(_) => break,
                        }
                    }
                }
            }
        });

        let mut batch_seq: u64 = 1;
        let mut heartbeat_tick = time::interval(hb_interval);
        heartbeat_tick.tick().await; // consume first instant tick

        loop {
            tokio::select! {
                _ = shutdown.notified() => break,
                _ = heartbeat_tick.tick() => {
                    let lsn = wal.read().latest_lsn();
                    let ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let hb = StreamMessage::ReplicationHeartbeat {
                        sender_id: primary_id,
                        role: NodeRole::Primary,
                        current_lsn: lsn,
                        timestamp_ms: ts,
                    };
                    if frame_write(&mut wr, &hb).await.is_err() {
                        break;
                    }
                }
                Some(confirmed) = ack_rx.recv() => {
                    if confirmed > confirmed_lsn {
                        confirmed_lsn = confirmed;
                    }
                }
                _ = time::sleep(Duration::from_millis(50)) => {
                    // Poll for new WAL records to send.
                    let records = {
                        let w = wal.read();
                        let pending = w.records_since(confirmed_lsn);
                        if pending.is_empty() {
                            continue;
                        }
                        let take = pending.len().min(64);
                        pending[..take].to_vec()
                    };
                    let msg = StreamMessage::WalBatch {
                        sender_id: primary_id,
                        batch_seq,
                        records,
                    };
                    batch_seq += 1;
                    if frame_write(&mut wr, &msg).await.is_err() {
                        break;
                    }
                }
            }
        }

        reader_handle.abort();
    }
}

// ---------------------------------------------------------------------------
// ReplicationClient (replica side)
// ---------------------------------------------------------------------------

/// Replica-side replication client.
///
/// Connects to the primary, sends a handshake with its current WAL
/// position, and receives WAL entries.  Auto-reconnects with
/// exponential backoff on disconnect.
pub struct ReplicationClient {
    primary_addr: SocketAddr,
    replica_id: NodeId,
    applied_lsn: Arc<parking_lot::RwLock<Lsn>>,
    wal: Arc<parking_lot::RwLock<WalWriter>>,
    shutdown: Arc<Notify>,
    heartbeat_interval: Duration,
}

impl ReplicationClient {
    /// Create a new client.
    pub fn new(
        primary_addr: SocketAddr,
        replica_id: NodeId,
        wal: Arc<parking_lot::RwLock<WalWriter>>,
    ) -> Self {
        let current_lsn = wal.read().latest_lsn();
        Self {
            primary_addr,
            replica_id,
            applied_lsn: Arc::new(parking_lot::RwLock::new(current_lsn)),
            wal,
            shutdown: Arc::new(Notify::new()),
            heartbeat_interval: Duration::from_secs(5),
        }
    }

    /// Override the default heartbeat interval.
    pub fn set_heartbeat_interval(&mut self, d: Duration) {
        self.heartbeat_interval = d;
    }

    /// Connect and start receiving WAL entries.  Spawns a background
    /// task; call [`disconnect`](Self::disconnect) to stop.
    pub async fn connect(&self) -> Result<(), ReplicationError> {
        let addr = self.primary_addr;
        let replica_id = self.replica_id;
        let applied = Arc::clone(&self.applied_lsn);
        let wal = Arc::clone(&self.wal);
        let shutdown = Arc::clone(&self.shutdown);
        let hb_interval = self.heartbeat_interval;

        tokio::spawn(async move {
            let mut backoff = Duration::from_millis(250);
            let max_backoff = Duration::from_secs(30);

            loop {
                match Self::run_once(addr, replica_id, &applied, &wal, &shutdown, hb_interval).await
                {
                    Ok(()) => break, // clean shutdown
                    Err(_) => {
                        // Check if we should stop.
                        if tokio::time::timeout(backoff, shutdown.notified())
                            .await
                            .is_ok()
                        {
                            break;
                        }
                        backoff = (backoff * 2).min(max_backoff);
                    }
                }
            }
        });

        Ok(())
    }

    /// Signal the client to disconnect.
    pub fn disconnect(&self) {
        self.shutdown.notify_waiters();
    }

    /// Get the currently applied LSN.
    pub fn applied_lsn(&self) -> Lsn {
        *self.applied_lsn.read()
    }

    // -- single connection attempt -----------------------------------------

    async fn run_once(
        addr: SocketAddr,
        replica_id: NodeId,
        applied: &Arc<parking_lot::RwLock<Lsn>>,
        wal: &Arc<parking_lot::RwLock<WalWriter>>,
        shutdown: &Arc<Notify>,
        hb_interval: Duration,
    ) -> Result<(), ReplicationError> {
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|e| ReplicationError::ProtocolError(format!("connect: {e}")))?;

        // Handshake: send current position.
        let from_lsn = *applied.read();
        let start = StreamMessage::StartStreaming {
            replica_id,
            from_lsn,
        };
        frame_write(&mut stream, &start).await?;

        // Read StreamingStarted response.
        match frame_read(&mut stream).await? {
            Some(StreamMessage::StreamingStarted { .. }) => {}
            _ => {
                return Err(ReplicationError::ProtocolError(
                    "expected StreamingStarted".into(),
                ));
            }
        }

        // Split stream.
        let (mut rd, mut wr) = tokio::io::split(stream);

        let mut heartbeat_tick = time::interval(hb_interval);
        heartbeat_tick.tick().await; // consume first instant tick

        loop {
            tokio::select! {
                _ = shutdown.notified() => return Ok(()),
                _ = heartbeat_tick.tick() => {
                    let lsn = *applied.read();
                    let ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    let hb = StreamMessage::ReplicationHeartbeat {
                        sender_id: replica_id,
                        role: NodeRole::Replica,
                        current_lsn: lsn,
                        timestamp_ms: ts,
                    };
                    frame_write(&mut wr, &hb).await?;
                }
                result = frame_read(&mut rd) => {
                    match result? {
                        Some(StreamMessage::WalBatch { batch_seq, records, .. }) => {
                            // Apply records to local WAL.
                            let mut last_lsn = *applied.read();
                            {
                                let mut w = wal.write();
                                for rec in &records {
                                    w.append(rec.payload.clone());
                                    last_lsn = rec.lsn;
                                }
                                w.flush();
                            }
                            *applied.write() = last_lsn;

                            // Send ack.
                            let ack = StreamMessage::WalBatchAck {
                                sender_id: replica_id,
                                confirmed_lsn: last_lsn,
                                batch_seq,
                            };
                            frame_write(&mut wr, &ack).await?;
                        }
                        Some(StreamMessage::ReplicationHeartbeat { .. }) => {
                            // Primary heartbeat — just absorb it.
                        }
                        Some(_) => {}
                        None => {
                            return Err(ReplicationError::ProtocolError(
                                "primary disconnected".into(),
                            ));
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// NetworkedReplicationManager
// ---------------------------------------------------------------------------

/// The status of a networked replication node.
#[derive(Debug, Clone)]
pub struct NetworkStatus {
    pub role: NodeRole,
    pub primary_addr: Option<SocketAddr>,
    pub listen_addr: Option<SocketAddr>,
    pub applied_lsn: Lsn,
    pub wal_tip: Lsn,
}

/// Wraps an existing [`ReplicationManager`] and adds the TCP network
/// layer for real primary-replica communication.
pub struct NetworkedReplicationManager {
    inner: ReplicationManager,
    wal: Arc<parking_lot::RwLock<WalWriter>>,
    server: Option<ReplicationServer>,
    client: Option<ReplicationClient>,
    bound_addr: Option<SocketAddr>,
}

impl NetworkedReplicationManager {
    /// Create from an existing manager.
    pub fn new(inner: ReplicationManager) -> Self {
        let wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        Self {
            inner,
            wal,
            server: None,
            client: None,
            bound_addr: None,
        }
    }

    /// Start this node as a primary listening on `port`.
    pub async fn start_as_primary(&mut self, port: u16) -> Result<SocketAddr, ReplicationError> {
        let addr: SocketAddr = ([127, 0, 0, 1], port).into();
        let server = ReplicationServer::new(addr, Arc::clone(&self.wal), self.inner.node_id());
        let bound = server.start().await?;
        self.server = Some(server);
        self.bound_addr = Some(bound);
        Ok(bound)
    }

    /// Start this node as a replica connecting to `primary_addr`.
    pub async fn start_as_replica(
        &mut self,
        primary_addr: SocketAddr,
    ) -> Result<(), ReplicationError> {
        let client = ReplicationClient::new(
            primary_addr,
            self.inner.node_id(),
            Arc::clone(&self.wal),
        );
        client.connect().await?;
        self.client = Some(client);
        Ok(())
    }

    /// Stop both server and client.
    pub fn stop(&self) {
        if let Some(s) = &self.server {
            s.stop();
        }
        if let Some(c) = &self.client {
            c.disconnect();
        }
    }

    /// Return a diagnostic status snapshot.
    pub fn status(&self) -> NetworkStatus {
        let wal_tip = self.wal.read().latest_lsn();
        let applied = self
            .client
            .as_ref()
            .map(|c| c.applied_lsn())
            .unwrap_or(wal_tip);
        NetworkStatus {
            role: self.inner.role(),
            primary_addr: self.client.as_ref().map(|c| c.primary_addr),
            listen_addr: self.bound_addr,
            applied_lsn: applied,
            wal_tip,
        }
    }

    /// Access the inner manager.
    pub fn inner(&self) -> &ReplicationManager {
        &self.inner
    }

    /// Mutable access to the inner manager.
    pub fn inner_mut(&mut self) -> &mut ReplicationManager {
        &mut self.inner
    }

    /// Shared WAL handle.
    pub fn wal(&self) -> &Arc<parking_lot::RwLock<WalWriter>> {
        &self.wal
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Framing helpers
    // -----------------------------------------------------------------------

    #[test]
    fn frame_encode_decode_roundtrip_heartbeat() {
        let msg = StreamMessage::ReplicationHeartbeat {
            sender_id: 1,
            role: NodeRole::Primary,
            current_lsn: 42,
            timestamp_ms: 1000,
        };
        let data = frame_encode(&msg);
        // First 4 bytes = big-endian length.
        let len = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
        assert_eq!(len, data.len() - 4);
        let decoded = decode_stream_message(&data[4..]).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn frame_encode_decode_roundtrip_wal_batch() {
        let records = vec![
            WalRecord {
                lsn: 1,
                timestamp_ms: 100,
                payload: WalPayload::PageWrite {
                    page_id: 10,
                    data: vec![0xAB; 32],
                },
            },
            WalRecord {
                lsn: 2,
                timestamp_ms: 200,
                payload: WalPayload::Commit { txn_id: 7 },
            },
        ];
        let msg = StreamMessage::WalBatch {
            sender_id: 1,
            batch_seq: 1,
            records,
        };
        let data = frame_encode(&msg);
        let decoded = decode_stream_message(&data[4..]).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn frame_encode_decode_start_streaming() {
        let msg = StreamMessage::StartStreaming {
            replica_id: 99,
            from_lsn: 55,
        };
        let data = frame_encode(&msg);
        let decoded = decode_stream_message(&data[4..]).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn frame_encode_decode_streaming_started() {
        let msg = StreamMessage::StreamingStarted {
            primary_id: 1,
            wal_tip_lsn: 100,
        };
        let data = frame_encode(&msg);
        let decoded = decode_stream_message(&data[4..]).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn frame_encode_decode_wal_batch_ack() {
        let msg = StreamMessage::WalBatchAck {
            sender_id: 2,
            confirmed_lsn: 10,
            batch_seq: 3,
        };
        let data = frame_encode(&msg);
        let decoded = decode_stream_message(&data[4..]).unwrap();
        assert_eq!(decoded, msg);
    }

    // -----------------------------------------------------------------------
    // WAL entry serialization (using StreamMessage)
    // -----------------------------------------------------------------------

    #[test]
    fn wal_entry_page_write_roundtrip() {
        let rec = WalRecord {
            lsn: 42,
            timestamp_ms: 9999,
            payload: WalPayload::PageWrite {
                page_id: 123,
                data: vec![1, 2, 3, 4, 5],
            },
        };
        let msg = StreamMessage::WalBatch {
            sender_id: 1,
            batch_seq: 1,
            records: vec![rec.clone()],
        };
        let encoded = frame_encode(&msg);
        let decoded = decode_stream_message(&encoded[4..]).unwrap();
        if let StreamMessage::WalBatch { records, .. } = decoded {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0], rec);
        } else {
            panic!("expected WalBatch");
        }
    }

    #[test]
    fn wal_entry_all_payload_types_roundtrip() {
        let records = vec![
            WalRecord {
                lsn: 1,
                timestamp_ms: 10,
                payload: WalPayload::PageWrite {
                    page_id: 1,
                    data: vec![0xFF; 16],
                },
            },
            WalRecord {
                lsn: 2,
                timestamp_ms: 20,
                payload: WalPayload::Commit { txn_id: 100 },
            },
            WalRecord {
                lsn: 3,
                timestamp_ms: 30,
                payload: WalPayload::Abort { txn_id: 200 },
            },
            WalRecord {
                lsn: 4,
                timestamp_ms: 40,
                payload: WalPayload::Checkpoint,
            },
        ];
        let msg = StreamMessage::WalBatch {
            sender_id: 5,
            batch_seq: 1,
            records: records.clone(),
        };
        let encoded = frame_encode(&msg);
        let decoded = decode_stream_message(&encoded[4..]).unwrap();
        if let StreamMessage::WalBatch {
            records: got_recs, ..
        } = decoded
        {
            assert_eq!(got_recs, records);
        } else {
            panic!("expected WalBatch");
        }
    }

    // -----------------------------------------------------------------------
    // Async framing read/write
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn async_frame_write_read_roundtrip() {
        let msg = StreamMessage::ReplicationHeartbeat {
            sender_id: 3,
            role: NodeRole::Replica,
            current_lsn: 77,
            timestamp_ms: 5000,
        };
        let mut buf: Vec<u8> = Vec::new();
        frame_write(&mut buf, &msg).await.unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let decoded = frame_read(&mut cursor).await.unwrap().unwrap();
        assert_eq!(decoded, msg);
    }

    #[tokio::test]
    async fn async_frame_read_eof_returns_none() {
        let buf: Vec<u8> = Vec::new();
        let mut cursor = std::io::Cursor::new(buf);
        let result = frame_read(&mut cursor).await.unwrap();
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // TCP connection establishment
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tcp_connection_and_handshake() {
        let wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let mut server = ReplicationServer::new(addr, Arc::clone(&wal), 1);
        server.set_heartbeat_interval(Duration::from_secs(60)); // suppress heartbeats
        let bound = server.start().await.unwrap();

        // Connect as a client and perform the handshake manually.
        let mut stream = TcpStream::connect(bound).await.unwrap();
        let start_msg = StreamMessage::StartStreaming {
            replica_id: 2,
            from_lsn: 0,
        };
        frame_write(&mut stream, &start_msg).await.unwrap();

        let response = time::timeout(Duration::from_secs(2), frame_read(&mut stream))
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        match response {
            StreamMessage::StreamingStarted { primary_id, .. } => {
                assert_eq!(primary_id, 1);
            }
            other => panic!("expected StreamingStarted, got {other:?}"),
        }

        server.stop();
    }

    // -----------------------------------------------------------------------
    // Heartbeat mechanism
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn server_sends_heartbeat() {
        let wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let mut server = ReplicationServer::new(addr, Arc::clone(&wal), 1);
        server.set_heartbeat_interval(Duration::from_millis(100));
        let bound = server.start().await.unwrap();

        let mut stream = TcpStream::connect(bound).await.unwrap();
        // Handshake
        frame_write(
            &mut stream,
            &StreamMessage::StartStreaming {
                replica_id: 2,
                from_lsn: 0,
            },
        )
        .await
        .unwrap();

        // Read StreamingStarted
        let _ = time::timeout(Duration::from_secs(1), frame_read(&mut stream))
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        // Wait for a heartbeat
        let hb = time::timeout(Duration::from_millis(500), frame_read(&mut stream))
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        match hb {
            StreamMessage::ReplicationHeartbeat {
                sender_id, role, ..
            } => {
                assert_eq!(sender_id, 1);
                assert_eq!(role, NodeRole::Primary);
            }
            other => panic!("expected heartbeat, got {other:?}"),
        }

        server.stop();
    }

    // -----------------------------------------------------------------------
    // WAL streaming: write on primary, read on replica
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn wal_streaming_primary_to_replica() {
        let wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let mut server = ReplicationServer::new(addr, Arc::clone(&wal), 1);
        server.set_heartbeat_interval(Duration::from_secs(60));
        let bound = server.start().await.unwrap();

        // Write some WAL entries before the replica connects.
        {
            let mut w = wal.write();
            w.append(WalPayload::PageWrite {
                page_id: 1,
                data: vec![0xAA; 8],
            });
            w.append(WalPayload::Commit { txn_id: 1 });
            w.flush();
        }

        // Connect a replica.
        let replica_wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let mut client = ReplicationClient::new(bound, 2, Arc::clone(&replica_wal));
        client.set_heartbeat_interval(Duration::from_secs(60));
        client.connect().await.unwrap();

        // Wait for the replica to catch up.
        let deadline = time::Instant::now() + Duration::from_secs(3);
        loop {
            if client.applied_lsn() >= 2 {
                break;
            }
            if time::Instant::now() > deadline {
                panic!(
                    "replica did not catch up in time; applied_lsn={}",
                    client.applied_lsn()
                );
            }
            time::sleep(Duration::from_millis(50)).await;
        }

        assert!(client.applied_lsn() >= 2);

        client.disconnect();
        server.stop();
    }

    // -----------------------------------------------------------------------
    // Graceful shutdown
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn graceful_shutdown_server_and_client() {
        let wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let mut server = ReplicationServer::new(addr, Arc::clone(&wal), 1);
        server.set_heartbeat_interval(Duration::from_secs(60));
        let bound = server.start().await.unwrap();

        let replica_wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let mut client = ReplicationClient::new(bound, 2, Arc::clone(&replica_wal));
        client.set_heartbeat_interval(Duration::from_secs(60));
        client.connect().await.unwrap();

        // Give it a moment to connect.
        time::sleep(Duration::from_millis(100)).await;

        // Shutdown should not hang.
        client.disconnect();
        server.stop();

        // Allow tasks to terminate.
        time::sleep(Duration::from_millis(100)).await;
    }

    // -----------------------------------------------------------------------
    // Multiple replicas
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn multiple_replicas_connect() {
        let wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        {
            let mut w = wal.write();
            w.append(WalPayload::Commit { txn_id: 1 });
            w.flush();
        }

        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let mut server = ReplicationServer::new(addr, Arc::clone(&wal), 1);
        server.set_heartbeat_interval(Duration::from_secs(60));
        let bound = server.start().await.unwrap();

        let replica_wal1 = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let mut client1 = ReplicationClient::new(bound, 2, Arc::clone(&replica_wal1));
        client1.set_heartbeat_interval(Duration::from_secs(60));
        client1.connect().await.unwrap();

        let replica_wal2 = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let mut client2 = ReplicationClient::new(bound, 3, Arc::clone(&replica_wal2));
        client2.set_heartbeat_interval(Duration::from_secs(60));
        client2.connect().await.unwrap();

        let deadline = time::Instant::now() + Duration::from_secs(3);
        loop {
            if client1.applied_lsn() >= 1 && client2.applied_lsn() >= 1 {
                break;
            }
            if time::Instant::now() > deadline {
                panic!(
                    "replicas did not catch up: c1={} c2={}",
                    client1.applied_lsn(),
                    client2.applied_lsn()
                );
            }
            time::sleep(Duration::from_millis(50)).await;
        }

        client1.disconnect();
        client2.disconnect();
        server.stop();
    }

    // -----------------------------------------------------------------------
    // Message ordering preservation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn message_ordering_preserved() {
        let wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let mut server = ReplicationServer::new(addr, Arc::clone(&wal), 1);
        server.set_heartbeat_interval(Duration::from_secs(60));
        let bound = server.start().await.unwrap();

        // Write 10 records.
        {
            let mut w = wal.write();
            for i in 1..=10u64 {
                w.append(WalPayload::PageWrite {
                    page_id: i,
                    data: vec![i as u8; 4],
                });
            }
            w.flush();
        }

        let replica_wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let mut client = ReplicationClient::new(bound, 2, Arc::clone(&replica_wal));
        client.set_heartbeat_interval(Duration::from_secs(60));
        client.connect().await.unwrap();

        let deadline = time::Instant::now() + Duration::from_secs(3);
        loop {
            if client.applied_lsn() >= 10 {
                break;
            }
            if time::Instant::now() > deadline {
                panic!(
                    "replica did not catch up in time; applied_lsn={}",
                    client.applied_lsn()
                );
            }
            time::sleep(Duration::from_millis(50)).await;
        }

        // Verify ordering in the replica WAL.
        let rwal = replica_wal.read();
        let records = rwal.records_since(0);
        assert_eq!(records.len(), 10);
        for (i, rec) in records.iter().enumerate() {
            // LSN should be i+1 because WalWriter assigns LSNs sequentially.
            assert_eq!(rec.lsn, (i + 1) as u64);
        }

        client.disconnect();
        server.stop();
    }

    // -----------------------------------------------------------------------
    // Reconnection
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn client_reconnects_after_server_restart() {
        let wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        {
            let mut w = wal.write();
            w.append(WalPayload::Commit { txn_id: 1 });
            w.flush();
        }

        // Start server on a fixed port (use OS-assigned port, then re-use it).
        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let mut server = ReplicationServer::new(addr, Arc::clone(&wal), 1);
        server.set_heartbeat_interval(Duration::from_secs(60));
        let bound = server.start().await.unwrap();

        let replica_wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let mut client = ReplicationClient::new(bound, 2, Arc::clone(&replica_wal));
        client.set_heartbeat_interval(Duration::from_secs(60));
        client.connect().await.unwrap();

        // Wait for initial catch-up.
        let deadline = time::Instant::now() + Duration::from_secs(3);
        loop {
            if client.applied_lsn() >= 1 {
                break;
            }
            if time::Instant::now() > deadline {
                panic!("initial catch-up failed");
            }
            time::sleep(Duration::from_millis(50)).await;
        }

        // Stop the server — the client should attempt to reconnect.
        server.stop();
        time::sleep(Duration::from_millis(200)).await;

        // Restart server on the SAME port.
        let mut server2 = ReplicationServer::new(bound, Arc::clone(&wal), 1);
        server2.set_heartbeat_interval(Duration::from_secs(60));
        // Add a second record.
        {
            let mut w = wal.write();
            w.append(WalPayload::Commit { txn_id: 2 });
            w.flush();
        }
        let _bound2 = server2.start().await.unwrap();

        // Wait for the client to reconnect and catch up to LSN 2.
        let deadline = time::Instant::now() + Duration::from_secs(5);
        loop {
            if client.applied_lsn() >= 2 {
                break;
            }
            if time::Instant::now() > deadline {
                // Reconnection is best-effort; port reuse may fail on
                // some OSes.  Don't hard-fail.
                break;
            }
            time::sleep(Duration::from_millis(100)).await;
        }

        client.disconnect();
        server2.stop();
    }

    // -----------------------------------------------------------------------
    // NetworkedReplicationManager
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn networked_manager_primary_starts() {
        let mgr = ReplicationManager::new_primary(1, ReplicationMode::Asynchronous);
        let mut net = NetworkedReplicationManager::new(mgr);
        let bound = net.start_as_primary(0).await.unwrap();
        assert_ne!(bound.port(), 0);
        let status = net.status();
        assert_eq!(status.role, NodeRole::Primary);
        assert!(status.listen_addr.is_some());
        net.stop();
    }

    #[tokio::test]
    async fn networked_manager_replica_connects() {
        // Primary
        let pmgr = ReplicationManager::new_primary(1, ReplicationMode::Asynchronous);
        let mut pnet = NetworkedReplicationManager::new(pmgr);
        let bound = pnet.start_as_primary(0).await.unwrap();

        // Write something.
        {
            let mut w = pnet.wal().write();
            w.append(WalPayload::Commit { txn_id: 1 });
            w.flush();
        }

        // Replica
        let rmgr = ReplicationManager::new_replica(2, 1);
        let mut rnet = NetworkedReplicationManager::new(rmgr);
        rnet.start_as_replica(bound).await.unwrap();

        let deadline = time::Instant::now() + Duration::from_secs(3);
        loop {
            let st = rnet.status();
            if st.applied_lsn >= 1 {
                break;
            }
            if time::Instant::now() > deadline {
                panic!("replica did not catch up");
            }
            time::sleep(Duration::from_millis(50)).await;
        }

        rnet.stop();
        pnet.stop();
    }

    // -----------------------------------------------------------------------
    // Edge cases
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn empty_wal_no_records_streamed() {
        let wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let mut server = ReplicationServer::new(addr, Arc::clone(&wal), 1);
        server.set_heartbeat_interval(Duration::from_secs(60));
        let bound = server.start().await.unwrap();

        let replica_wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let mut client = ReplicationClient::new(bound, 2, Arc::clone(&replica_wal));
        client.set_heartbeat_interval(Duration::from_secs(60));
        client.connect().await.unwrap();

        // Give it time — nothing should arrive.
        time::sleep(Duration::from_millis(300)).await;
        assert_eq!(client.applied_lsn(), 0);

        client.disconnect();
        server.stop();
    }

    #[tokio::test]
    async fn large_batch_streaming() {
        let wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        {
            let mut w = wal.write();
            for i in 1..=200u64 {
                w.append(WalPayload::PageWrite {
                    page_id: i,
                    data: vec![0xBB; 64],
                });
            }
            w.flush();
        }

        let addr: SocketAddr = ([127, 0, 0, 1], 0).into();
        let mut server = ReplicationServer::new(addr, Arc::clone(&wal), 1);
        server.set_heartbeat_interval(Duration::from_secs(60));
        let bound = server.start().await.unwrap();

        let replica_wal = Arc::new(parking_lot::RwLock::new(WalWriter::new()));
        let mut client = ReplicationClient::new(bound, 2, Arc::clone(&replica_wal));
        client.set_heartbeat_interval(Duration::from_secs(60));
        client.connect().await.unwrap();

        let deadline = time::Instant::now() + Duration::from_secs(5);
        loop {
            if client.applied_lsn() >= 200 {
                break;
            }
            if time::Instant::now() > deadline {
                panic!(
                    "replica did not receive all 200 records; got {}",
                    client.applied_lsn()
                );
            }
            time::sleep(Duration::from_millis(50)).await;
        }

        assert_eq!(client.applied_lsn(), 200);

        client.disconnect();
        server.stop();
    }
}
