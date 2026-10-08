//! Review regressions. These require native execution before transport acceptance.
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "ws")]
#[tokio::test(flavor = "current_thread")]
async fn upgraded_callback_is_joined_before_hooks_for_normal_and_workers() {
    use neutron::{app::Neutron, router::Router, ws::WebSocketUpgrade};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    for workers in [0, 2] {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = reservation.local_addr().unwrap();
        drop(reservation);
        let entered = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let ec = entered.clone();
        let dc = dropped.clone();
        let router = Router::new().get("/ws", move |upgrade: WebSocketUpgrade| {
            let ec = ec.clone();
            let dc = dc.clone();
            async move {
                upgrade.on_upgrade(move |_socket| async move {
                    let _guard = Dropped(dc);
                    ec.notify_one();
                    std::future::pending::<()>().await;
                })
            }
        });
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let hook = dropped.clone();
        let server = tokio::spawn(async move {
            Neutron::new()
                .router(router)
                .workers(workers)
                .shutdown_timeout(Duration::from_millis(40))
                .shutdown_signal(async move {
                    let _ = stopped.await;
                })
                .on_shutdown(move || {
                    let hook = hook.clone();
                    async move {
                        assert!(
                            hook.load(Ordering::SeqCst),
                            "hook ran before upgraded callback terminated"
                        );
                    }
                })
                .listen(addr)
                .await
                .unwrap();
        });
        let mut peer = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match tokio::net::TcpStream::connect(addr).await {
                    Ok(peer) => break peer,
                    Err(_) => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .unwrap();
        peer.write_all(b"GET /ws HTTP/1.1\r\nHost: localhost\r\nConnection: upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
        let mut headers = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !headers.ends_with(b"\r\n\r\n") {
                headers.push(peer.read_u8().await.unwrap());
            }
        })
        .await
        .unwrap();
        assert!(headers.starts_with(b"HTTP/1.1 101"));
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        assert!(dropped.load(Ordering::SeqCst));
    }
}

#[cfg(feature = "http3")]
mod h3_regressions {
    use super::*;
    use bytes::Bytes;
    use neutron::{app::Neutron, router::Router, tls::TlsConfig};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    async fn connected(
        certificate: rustls::pki_types::CertificateDer<'static>,
        addr: std::net::SocketAddr,
    ) -> (quinn::Endpoint, quinn::Connection) {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate).unwrap();
        let mut crypto = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        crypto.alpn_protocols = vec![b"h3".to_vec()];
        let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(crypto).unwrap();
        let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
        let conn = tokio::time::timeout(
            Duration::from_secs(2),
            endpoint.connect(addr, "localhost").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        (endpoint, conn)
    }
    fn fixture() -> (
        std::net::SocketAddr,
        TlsConfig,
        rustls::pki_types::CertificateDer<'static>,
    ) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let tls = TlsConfig::from_pem_bytes(
            cert.cert.pem().as_bytes(),
            cert.key_pair.serialize_pem().as_bytes(),
        )
        .unwrap();
        let reservation = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = reservation.local_addr().unwrap();
        drop(reservation);
        (addr, tls, cert.cert.der().clone())
    }
    #[tokio::test]
    async fn active_h3_request_drains_and_goaway_refuses_new_work() {
        let (addr, tls, cert) = fixture();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let ec = entered.clone();
        let rc = release.clone();
        let router = Router::new().get("/held", move || {
            let ec = ec.clone();
            let rc = rc.clone();
            async move {
                ec.notify_one();
                rc.notified().await;
                "drained"
            }
        });
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let hook = Arc::new(AtomicBool::new(false));
        let hc = hook.clone();
        let server = tokio::spawn(async move {
            Neutron::new()
                .router(router)
                .shutdown_timeout(Duration::from_secs(1))
                .shutdown_signal(async move {
                    let _ = stopped.await;
                })
                .on_shutdown(move || {
                    let hc = hc.clone();
                    async move {
                        hc.store(true, Ordering::SeqCst);
                    }
                })
                .listen_h3(addr, tls)
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let (endpoint, conn) = connected(cert, addr).await;
        let (mut driver, mut sender) = h3::client::new(h3_quinn::Connection::new(conn))
            .await
            .unwrap();
        let driver = tokio::spawn(async move { driver.wait_idle().await });
        let mut request = sender
            .send_request(
                http::Request::builder()
                    .uri(format!("https://localhost:{}/held", addr.port()))
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        request.finish().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        stop.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!hook.load(Ordering::SeqCst));
        // The client driver must observe GOAWAY before a newer request is admitted.
        let newer = sender
            .send_request(
                http::Request::builder()
                    .uri(format!("https://localhost:{}/held", addr.port()))
                    .body(())
                    .unwrap(),
            )
            .await;
        assert!(newer.is_err(), "post-GOAWAY request admitted");
        release.notify_one();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), request.recv_response())
                .await
                .unwrap()
                .unwrap()
                .status(),
            200
        );
        let mut body = Vec::new();
        while let Some(mut data) = request.recv_data().await.unwrap() {
            use bytes::Buf;
            body.extend_from_slice(&data.copy_to_bytes(data.remaining()));
        }
        assert_eq!(body, b"drained");
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        assert!(hook.load(Ordering::SeqCst));
        endpoint.close(0u32.into(), b"done");
        driver.abort();
        let _ = driver.await;
    }
    #[tokio::test]
    async fn h3_body_ceiling_and_forced_drain_cover_active_streams() {
        struct DropMark(Arc<AtomicBool>);
        impl Drop for DropMark {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let (addr, tls, cert) = fixture();
        let entered = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(AtomicUsize::new(0));
        let ec = entered.clone();
        let dc = dropped.clone();
        let cc = calls.clone();
        let router = Router::new()
            .get("/held", move || {
                let ec = ec.clone();
                let dc = dc.clone();
                async move {
                    let _guard = DropMark(dc);
                    ec.notify_one();
                    std::future::pending::<()>().await;
                    "unreachable"
                }
            })
            .post("/upload", move || {
                let cc = cc.clone();
                async move {
                    cc.fetch_add(1, Ordering::SeqCst);
                    "bad"
                }
            });
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let hc = dropped.clone();
        let server = tokio::spawn(async move {
            Neutron::new()
                .router(router)
                .max_body_size(4)
                .shutdown_timeout(Duration::from_millis(50))
                .shutdown_signal(async move {
                    let _ = stopped.await;
                })
                .on_shutdown(move || {
                    let hc = hc.clone();
                    async move {
                        assert!(hc.load(Ordering::SeqCst));
                    }
                })
                .listen_h3(addr, tls)
                .await
                .unwrap();
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let (endpoint, conn) = connected(cert, addr).await;
        let (mut driver, mut sender) = h3::client::new(h3_quinn::Connection::new(conn))
            .await
            .unwrap();
        let driver = tokio::spawn(async move { driver.wait_idle().await });
        let mut oversized = sender
            .send_request(
                http::Request::builder()
                    .method("POST")
                    .uri(format!("https://localhost:{}/upload", addr.port()))
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        oversized
            .send_data(Bytes::from_static(b"123456789"))
            .await
            .unwrap();
        oversized.finish().await.unwrap();
        assert_eq!(oversized.recv_response().await.unwrap().status(), 413);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let mut held = sender
            .send_request(
                http::Request::builder()
                    .uri(format!("https://localhost:{}/held", addr.port()))
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        held.finish().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let mut incomplete = sender
            .send_request(
                http::Request::builder()
                    .method("POST")
                    .uri(format!("https://localhost:{}/upload", addr.port()))
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        incomplete
            .send_data(Bytes::from_static(b"1"))
            .await
            .unwrap(); // deliberately leave stream open
        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        endpoint.close(0u32.into(), b"done");
        driver.abort();
        let _ = driver.await;
    }
}
