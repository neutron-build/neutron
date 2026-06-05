//! Regression tests for finding #16: nucleus must honor SIGTERM/SIGINT
//! and exit within a bounded budget, even under load.
//!
//! These tests spawn the release `nucleus` binary as a subprocess on a
//! random port, send a signal, and assert the process is gone before the
//! deadline. They are Unix-only (signals + /dev/tcp readiness probe).
//!
//! Run with:
//!     cargo test --release --features server --test signal_shutdown -- --nocapture

#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Maximum time we tolerate between sending a signal and the process exiting.
/// Must match (or be slightly larger than) `SHUTDOWN_DEADLINE_SECS` in main.rs.
const EXIT_DEADLINE: Duration = Duration::from_secs(5);

/// Hard cap on how long we wait for the server to bind before declaring the
/// test infrastructure (not the fix) broken.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);

fn nucleus_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_nucleus"))
}

/// Pick a port that is currently free. The server then re-binds the same
/// port after we close the probe socket; on the rare race we re-roll.
fn pick_free_port() -> u16 {
    for _ in 0..32 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        // Skip well-known ports the server defaults trip over.
        if !matches!(port, 5432..=5434 | 6379) {
            return port;
        }
    }
    panic!("could not allocate a free port");
}

fn wait_for_port(port: u16) -> bool {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn send_signal(pid: u32, sig: i32) {
    // SAFETY: kill is async-signal-safe and pid is a valid child pid we
    // just spawned and have not yet reaped.
    let rc = unsafe { libc::kill(pid as libc::pid_t, sig) };
    assert_eq!(
        rc,
        0,
        "kill({pid}, {sig}) failed: {}",
        std::io::Error::last_os_error()
    );
}

struct ServerHandle {
    child: std::process::Child,
    data_dir: tempfile::TempDir,
    port: u16,
}

impl ServerHandle {
    fn start() -> Self {
        let port = pick_free_port();
        // Pick three more ports for cluster/repl so we don't collide with
        // other agents running their own nucleus on the defaults.
        let cluster_port = pick_free_port();
        let repl_port = pick_free_port();
        let data_dir = tempfile::tempdir().expect("tempdir");

        let child = Command::new(nucleus_binary())
            .args([
                "start",
                "--port",
                &port.to_string(),
                "--cluster-port",
                &cluster_port.to_string(),
                "--replication-port",
                &repl_port.to_string(),
                "--resp-port",
                "0",
                "--no-tls",
                "--data",
                data_dir.path().to_str().unwrap(),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn nucleus binary");

        if !wait_for_port(port) {
            // Surface server output to make CI failures debuggable.
            let mut handle = ServerHandle {
                child,
                data_dir,
                port,
            };
            let logs = handle.drain_output();
            handle.kill();
            panic!(
                "nucleus did not bind port {port} within {STARTUP_TIMEOUT:?}\n--- logs ---\n{logs}"
            );
        }

        ServerHandle {
            child,
            data_dir,
            port,
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Best-effort SIGKILL + reap. Used on test failure paths.
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    fn drain_output(&mut self) -> String {
        let mut buf = String::new();
        if let Some(stdout) = self.child.stdout.take() {
            for line in BufReader::new(stdout).lines().take(200).flatten() {
                buf.push_str(&line);
                buf.push('\n');
            }
        }
        if let Some(stderr) = self.child.stderr.take() {
            for line in BufReader::new(stderr).lines().take(200).flatten() {
                buf.push_str(&line);
                buf.push('\n');
            }
        }
        buf
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        // If the test panicked before killing, make sure we don't leak the
        // process and tie up the port.
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        // Keep tempdir alive until child is reaped to avoid spurious file
        // errors during nucleus shutdown.
        let _ = &self.data_dir;
    }
}

/// Poll `Child::try_wait` until exit or deadline. Returns the elapsed time
/// on success, or None on timeout.
fn wait_for_exit(handle: &mut ServerHandle, deadline: Duration) -> Option<Duration> {
    let start = Instant::now();
    while start.elapsed() < deadline {
        match handle.child.try_wait() {
            Ok(Some(_status)) => return Some(start.elapsed()),
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
            Err(e) => panic!("try_wait failed: {e}"),
        }
    }
    None
}

#[test]
fn sigterm_exits_within_deadline() {
    let mut server = ServerHandle::start();
    let pid = server.pid();

    send_signal(pid, libc::SIGTERM);

    match wait_for_exit(&mut server, EXIT_DEADLINE) {
        Some(elapsed) => {
            println!("nucleus exited {elapsed:?} after SIGTERM");
            assert!(
                elapsed < EXIT_DEADLINE,
                "exit took {elapsed:?}, exceeds deadline {EXIT_DEADLINE:?}"
            );
        }
        None => {
            server.kill();
            panic!("nucleus did not exit within {EXIT_DEADLINE:?} of SIGTERM");
        }
    }
}

#[test]
fn sigint_exits_within_deadline() {
    let mut server = ServerHandle::start();
    let pid = server.pid();

    send_signal(pid, libc::SIGINT);

    match wait_for_exit(&mut server, EXIT_DEADLINE) {
        Some(elapsed) => {
            println!("nucleus exited {elapsed:?} after SIGINT");
            assert!(elapsed < EXIT_DEADLINE);
        }
        None => {
            server.kill();
            panic!("nucleus did not exit within {EXIT_DEADLINE:?} of SIGINT");
        }
    }
}

#[test]
fn sigterm_with_active_connections_exits_within_deadline() {
    let mut server = ServerHandle::start();
    let port = server.port;
    let pid = server.pid();

    // Hold open several raw TCP connections so the drain path has work to do.
    // We don't speak the postgres protocol, but the server still tracks them
    // as active until the read times out — which is exactly the case the bug
    // report calls out (Observe holds long-lived idle connections).
    let mut conns: Vec<TcpStream> = (0..8)
        .map(|_| TcpStream::connect(("127.0.0.1", port)).expect("open conn"))
        .collect();

    // Give the server a beat to register the connections.
    std::thread::sleep(Duration::from_millis(200));

    send_signal(pid, libc::SIGTERM);

    let elapsed = wait_for_exit(&mut server, EXIT_DEADLINE).unwrap_or_else(|| {
        for c in &conns {
            let _ = c.shutdown(std::net::Shutdown::Both);
        }
        server.kill();
        panic!(
            "nucleus did not exit within {EXIT_DEADLINE:?} of SIGTERM with {} active conns",
            conns.len()
        );
    });

    println!(
        "nucleus exited {elapsed:?} with {} active connections",
        conns.len()
    );
    assert!(elapsed < EXIT_DEADLINE);

    // Drop after the assertion so the connections are alive during shutdown.
    conns.clear();
}
