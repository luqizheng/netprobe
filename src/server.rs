use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::protocol::{AckDecision, HeloDecision, Message, RoundTracker, MAX_MESSAGE_LEN};

const IDLE_SESSION_TIMEOUT: Duration = Duration::from_secs(120);
const IDLE_CLEANUP_INTERVAL: Duration = Duration::from_secs(30);
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

type UdpSessions = Arc<Mutex<HashMap<SocketAddr, RoundTracker>>>;
type TcpWriter = tokio::io::WriteHalf<TcpStream>;

// ---------------------------------------------------------------- TCP server

pub async fn run_tcp(listener: TcpListener) -> anyhow::Result<()> {
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                tokio::spawn(async move {
                    handle_tcp_connection(stream).await;
                });
            }
            Err(e) => {
                warn!("[WARN] TCP accept error: {e}");
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
            }
        }
    }
}

pub async fn handle_tcp_connection(stream: TcpStream) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    info!("[INFO] Client connected: {peer}");
    info!("[INFO] Waiting for messages from {peer}");

    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut tracker = RoundTracker::new();
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                if let Some(round) = tracker.unfinished_round() {
                    warn!("[WARN] Client: {peer}");
                    warn!("[WARN] Round: {round}");
                    warn!("[WARN] Status: FAILED");
                    warn!("[WARN] Reason: connection closed before round completed");
                }
                info!("[INFO] Client disconnected: {peer}");
                return;
            }
            Ok(_) => {
                if line.len() > MAX_MESSAGE_LEN + 1 {
                    warn!(
                        "[WARN] {peer} sent oversized line ({} bytes), ignoring",
                        line.len()
                    );
                    continue;
                }
                let text = line.trim_end();
                match Message::parse(text) {
                    Ok(msg) => {
                        if handle_tcp_message(&mut tracker, &peer, msg, &mut writer)
                            .await
                            .is_err()
                        {
                            if let Some(round) = tracker.unfinished_round() {
                                warn!("[WARN] Client: {peer}");
                                warn!("[WARN] Round: {round}");
                                warn!("[WARN] Status: FAILED");
                                warn!("[WARN] Reason: failed to send response");
                            }
                            warn!("[WARN] Send error to {peer}, closing connection");
                            return;
                        }
                    }
                    Err(e) => {
                        warn!("[WARN] {peer} invalid message {text:?}: {e}");
                    }
                }
            }
            Err(e) => {
                if let Some(round) = tracker.unfinished_round() {
                    warn!("[WARN] Client: {peer}");
                    warn!("[WARN] Round: {round}");
                    warn!("[WARN] Status: FAILED");
                    warn!("[WARN] Reason: read error ({e})");
                }
                warn!("[WARN] Read error from {peer}: {e}, closing connection");
                return;
            }
        }
    }
}

async fn handle_tcp_message(
    tracker: &mut RoundTracker,
    peer: &str,
    msg: Message,
    writer: &mut TcpWriter,
) -> std::io::Result<()> {
    match msg {
        Message::Helo(n) => {
            info!("[RECV] {peer} helo({n})");
            if let HeloDecision::Superseded { abandoned } = tracker.on_helo(n) {
                warn!("[WARN] Client: {peer}");
                warn!("[WARN] Round: {abandoned}");
                warn!("[WARN] Status: FAILED");
                warn!("[WARN] Reason: superseded by round {n}");
            }
            send_tcp(writer, peer, Message::ok(n)).await?;
        }
        Message::Ack(n) => {
            info!("[RECV] {peer} ack({n})");
            match tracker.on_ack(n) {
                AckDecision::Complete { round, elapsed_ms } => {
                    send_tcp(writer, peer, Message::end(n)).await?;
                    log_round_summary(peer, round, elapsed_ms);
                }
                AckDecision::Duplicate { round, .. } => {
                    warn!("[WARN] {peer} duplicate ack({round}), resending end({round})");
                    send_tcp(writer, peer, Message::end(n)).await?;
                }
                AckDecision::Mismatch { received, current } => {
                    warn!("[WARN] {peer} unexpected ack({received}), current round {current:?}");
                }
            }
        }
        other => {
            warn!("[WARN] {peer} unexpected message {other}");
        }
    }
    Ok(())
}

async fn send_tcp(writer: &mut TcpWriter, peer: &str, msg: Message) -> std::io::Result<()> {
    writer.write_all(format!("{msg}\n").as_bytes()).await?;
    writer.flush().await?;
    info!("[SEND] {peer} {msg}");
    Ok(())
}

fn log_round_summary(peer: &str, round: u64, elapsed_ms: f64) {
    info!("[INFO] Client: {peer}");
    info!("[INFO] Round: {round}");
    info!("[INFO] Status: SUCCESS");
    info!("[INFO] Elapsed: {elapsed_ms:.3} ms");
}

// ---------------------------------------------------------------- UDP server

pub async fn run_udp(socket: UdpSocket) -> anyhow::Result<()> {
    let sessions: UdpSessions = Arc::new(Mutex::new(HashMap::new()));
    tokio::select! {
        res = udp_recv_loop(socket, &sessions) => res,
        _ = udp_cleanup_loop(&sessions) => Ok(()),
    }
}

async fn udp_cleanup_loop(sessions: &UdpSessions) {
    loop {
        tokio::time::sleep(IDLE_CLEANUP_INTERVAL).await;
        let mut map = sessions.lock().await;
        let expired: Vec<SocketAddr> = map
            .iter()
            .filter(|(_, tracker)| tracker.is_idle(IDLE_SESSION_TIMEOUT))
            .map(|(addr, _)| *addr)
            .collect();
        for addr in expired {
            if let Some(tracker) = map.remove(&addr) {
                if let Some(round) = tracker.unfinished_round() {
                    warn!("[WARN] UDP client {addr} idle session expired, Round: {round}, Status: FAILED");
                } else {
                    info!("[INFO] UDP client {addr} idle session removed");
                }
            }
        }
    }
}

async fn udp_recv_loop(socket: UdpSocket, sessions: &UdpSessions) -> anyhow::Result<()> {
    let mut buf = [0u8; MAX_MESSAGE_LEN];
    loop {
        let (len, from) = match socket.recv_from(&mut buf).await {
            Ok(x) => x,
            Err(e) => {
                warn!("[WARN] UDP recv error: {e}");
                continue;
            }
        };
        let text = match std::str::from_utf8(&buf[..len]) {
            Ok(t) => t,
            Err(_) => {
                warn!("[WARN] {from} sent non-utf8 datagram ({len} bytes), ignoring");
                continue;
            }
        };
        let msg = match Message::parse(text) {
            Ok(m) => m,
            Err(e) => {
                warn!("[WARN] {from} invalid datagram {text:?}: {e}");
                continue;
            }
        };
        let mut map = sessions.lock().await;
        if !map.contains_key(&from) {
            info!("[INFO] New client detected: {from}");
        }
        let tracker = map.entry(from).or_insert_with(RoundTracker::new);
        handle_udp_message(&socket, from, tracker, msg).await;
    }
}

async fn handle_udp_message(
    socket: &UdpSocket,
    peer: SocketAddr,
    tracker: &mut RoundTracker,
    msg: Message,
) {
    match msg {
        Message::Helo(n) => {
            info!("[RECV] {peer} helo({n})");
            match tracker.on_helo(n) {
                HeloDecision::Superseded { abandoned } => {
                    warn!("[WARN] Client: {peer}");
                    warn!("[WARN] Round: {abandoned}");
                    warn!("[WARN] Status: FAILED");
                    warn!("[WARN] Reason: superseded by round {n}");
                }
                HeloDecision::Expired { current } => {
                    warn!("[WARN] {peer} expired helo({n}), current round {current}");
                }
                HeloDecision::Duplicate => {
                    warn!("[WARN] {peer} duplicate helo({n}), resending ok({n})");
                }
                HeloDecision::NewRound => {}
            }
            send_udp(socket, peer, Message::ok(n)).await;
        }
        Message::Ack(n) => {
            info!("[RECV] {peer} ack({n})");
            match tracker.on_ack(n) {
                AckDecision::Complete { round, elapsed_ms } => {
                    send_udp(socket, peer, Message::end(n)).await;
                    log_round_summary(&peer.to_string(), round, elapsed_ms);
                }
                AckDecision::Duplicate { round, .. } => {
                    warn!("[WARN] {peer} duplicate ack({round}), resending end({round})");
                    send_udp(socket, peer, Message::end(n)).await;
                }
                AckDecision::Mismatch { received, current } => {
                    warn!("[WARN] {peer} unexpected ack({received}), current round {current:?}");
                }
            }
        }
        other => {
            warn!("[WARN] {peer} unexpected message {other}");
        }
    }
}

async fn send_udp(socket: &UdpSocket, peer: SocketAddr, msg: Message) {
    let payload = msg.to_string();
    match socket.send_to(payload.as_bytes(), peer).await {
        Ok(_) => info!("[SEND] {peer} {msg}"),
        Err(e) => warn!("[WARN] Failed to send {msg} to {peer}: {e}"),
    }
}
