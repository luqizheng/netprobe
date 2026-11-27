use std::sync::{Arc, Mutex};
use std::time::Duration;

use netprobe::client::{run_udp, ClientConfig};
use netprobe::protocol::Message;
use netprobe::server;
use tokio::net::UdpSocket;

fn test_cfg(max_rounds: u64) -> ClientConfig {
    ClientConfig {
        interval: Duration::from_millis(10),
        timeout: Duration::from_millis(200),
        retries: 3,
        max_rounds: Some(max_rounds),
    }
}

#[tokio::test]
async fn udp_completes_three_rounds() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let server_handle = tokio::spawn(server::run_udp(socket));

    let rounds = run_udp(addr, &test_cfg(3)).await.unwrap();
    assert_eq!(rounds.len(), 3);
    assert!(rounds.iter().all(|r| r.success));
    assert_eq!(rounds[0].round, 1);
    assert_eq!(rounds[2].round, 3);

    server_handle.abort();
}

#[tokio::test]
async fn udp_retransmits_after_packet_loss() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = socket.local_addr().unwrap();
    let helo_count = Arc::new(Mutex::new(0u32));
    let counter = Arc::clone(&helo_count);

    // Mock server: drops the very first helo it receives, then responds normally.
    let mock = tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        let mut dropped_first = false;
        loop {
            let (len, from) = socket.recv_from(&mut buf).await.unwrap();
            let msg = Message::parse(std::str::from_utf8(&buf[..len]).unwrap()).unwrap();
            match msg {
                Message::Helo(n) => {
                    *counter.lock().unwrap() += 1;
                    if !dropped_first {
                        dropped_first = true;
                        continue;
                    }
                    socket
                        .send_to(format!("ok({n})").as_bytes(), from)
                        .await
                        .unwrap();
                }
                Message::Ack(n) => {
                    socket
                        .send_to(format!("end({n})").as_bytes(), from)
                        .await
                        .unwrap();
                    break;
                }
                _ => {}
            }
        }
    });

    let rounds = run_udp(target, &test_cfg(1)).await.unwrap();
    assert_eq!(rounds.len(), 1);
    assert!(rounds[0].success, "round should succeed via retransmission");
    assert_eq!(
        *helo_count.lock().unwrap(),
        2,
        "expected exactly one helo retransmission after the simulated loss"
    );

    mock.await.unwrap();
}

#[tokio::test]
async fn udp_server_replies_idempotently() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let server_handle = tokio::spawn(server::run_udp(socket));

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut buf = [0u8; 1024];

    async fn recv_reply(client: &UdpSocket, buf: &mut [u8]) -> String {
        let (len, _) = tokio::time::timeout(Duration::from_secs(2), client.recv_from(buf))
            .await
            .expect("reply within 2s")
            .expect("recv ok");
        std::str::from_utf8(&buf[..len]).unwrap().to_string()
    }

    // Duplicate helo(1) must be answered with ok(1) every time.
    for _ in 0..2 {
        client.send_to(b"helo(1)", addr).await.unwrap();
        assert_eq!(recv_reply(&client, &mut buf).await, "ok(1)");
    }

    // ack(1) -> end(1), duplicate ack(1) -> end(1) again (idempotent).
    client.send_to(b"ack(1)", addr).await.unwrap();
    assert_eq!(recv_reply(&client, &mut buf).await, "end(1)");
    client.send_to(b"ack(1)", addr).await.unwrap();
    assert_eq!(recv_reply(&client, &mut buf).await, "end(1)");

    server_handle.abort();
}

#[tokio::test]
async fn udp_multiple_clients_independent() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let server_handle = tokio::spawn(server::run_udp(socket));

    let cfg_a = test_cfg(2);
    let cfg_b = test_cfg(2);
    let (ra, rb) = tokio::join!(run_udp(addr, &cfg_a), run_udp(addr, &cfg_b));
    assert!(ra.unwrap().iter().all(|r| r.success));
    assert!(rb.unwrap().iter().all(|r| r.success));

    server_handle.abort();
}
