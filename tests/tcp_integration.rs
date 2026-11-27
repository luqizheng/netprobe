use std::time::Duration;

use netprobe::client::{run_tcp, ClientConfig};
use netprobe::server;
use tokio::net::TcpListener;

fn test_cfg(max_rounds: u64) -> ClientConfig {
    ClientConfig {
        interval: Duration::from_millis(10),
        timeout: Duration::from_secs(2),
        retries: 3,
        max_rounds: Some(max_rounds),
    }
}

#[tokio::test]
async fn tcp_completes_three_rounds() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn(server::run_tcp(listener));

    let rounds = run_tcp(addr, &test_cfg(3)).await.unwrap();
    assert_eq!(rounds.len(), 3);
    for (i, r) in rounds.iter().enumerate() {
        assert!(r.success, "round {} failed", i + 1);
        assert_eq!(r.round, (i + 1) as u64);
    }

    server_handle.abort();
}

#[tokio::test]
async fn tcp_client_reuses_single_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let (stream, _peer) = listener.accept().await.unwrap();
        server::handle_tcp_connection(stream).await;
        // After the (only) connection ends, verify the client never dialed again.
        tokio::time::timeout(Duration::from_millis(300), listener.accept())
            .await
            .map(|r| r.map(|(_, p)| p))
    });

    let rounds = run_tcp(addr, &test_cfg(3)).await.unwrap();
    assert_eq!(rounds.len(), 3);
    assert!(rounds.iter().all(|r| r.success));

    let second_accept = handle.await.unwrap();
    assert!(
        second_accept.is_err(),
        "client opened a second connection instead of reusing the first"
    );
}

#[tokio::test]
async fn tcp_server_survives_client_disconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn(server::run_tcp(listener));

    let first = run_tcp(addr, &test_cfg(1)).await.unwrap();
    assert!(first[0].success);

    // A second client must be served normally after the first one is gone.
    let second = run_tcp(addr, &test_cfg(2)).await.unwrap();
    assert_eq!(second.len(), 2);
    assert!(second.iter().all(|r| r.success));

    server_handle.abort();
}
