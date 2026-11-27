use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::net::{TcpStream, UdpSocket};
use tracing::{info, warn};

use crate::protocol::{Message, MessageKind, MAX_MESSAGE_LEN};

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub interval: Duration,
    pub timeout: Duration,
    pub retries: u32,
    /// Test-only upper bound on rounds; `None` means run forever.
    pub max_rounds: Option<u64>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(3),
            retries: 3,
            max_rounds: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RoundResult {
    pub round: u64,
    pub success: bool,
    pub elapsed_ms: f64,
}

type TcpReader = BufReader<ReadHalf<TcpStream>>;
type TcpWriter = WriteHalf<TcpStream>;

// ---------------------------------------------------------------- TCP client

pub async fn run_tcp(target: SocketAddr, cfg: &ClientConfig) -> anyhow::Result<Vec<RoundResult>> {
    log_banner("TCP", target, cfg);
    let mut seq: u64 = 1;
    let mut results: Vec<RoundResult> = Vec::new();
    'outer: loop {
        if rounds_complete(cfg, results.len()) {
            break;
        }
        let Some((mut reader, mut writer)) = connect_with_log(target, cfg).await else {
            log_waiting(cfg);
            tokio::time::sleep(cfg.interval).await;
            continue;
        };
        loop {
            if rounds_complete(cfg, results.len()) {
                break 'outer;
            }
            let start = Instant::now();
            let outcome = tcp_round(&mut writer, &mut reader, seq, cfg).await;
            let elapsed_ms = elapsed_ms_since(start);
            match outcome {
                Ok(()) => {
                    info!("[INFO] Round {seq} SUCCESS (elapsed {elapsed_ms:.3} ms)");
                    results.push(RoundResult {
                        round: seq,
                        success: true,
                        elapsed_ms,
                    });
                }
                Err(reason) => {
                    warn!("[WARN] Round {seq} FAILED: {reason}");
                    results.push(RoundResult {
                        round: seq,
                        success: false,
                        elapsed_ms,
                    });
                    warn!("[WARN] Closing connection to {target} and reconnecting to clear half-round state");
                    seq += 1;
                    log_waiting(cfg);
                    tokio::time::sleep(cfg.interval).await;
                    break;
                }
            }
            seq += 1;
            log_waiting(cfg);
            tokio::time::sleep(cfg.interval).await;
        }
    }
    Ok(results)
}

async fn connect_with_log(
    target: SocketAddr,
    cfg: &ClientConfig,
) -> Option<(TcpReader, TcpWriter)> {
    match tokio::time::timeout(cfg.timeout, TcpStream::connect(target)).await {
        Ok(Ok(stream)) => {
            info!("[INFO] Connected to {target}");
            let (reader, writer) = tokio::io::split(stream);
            Some((BufReader::new(reader), writer))
        }
        Ok(Err(e)) => {
            warn!("[WARN] Connect to {target} failed: {e}");
            None
        }
        Err(_) => {
            warn!(
                "[WARN] Connect to {target} timed out after {} seconds",
                cfg.timeout.as_secs_f64()
            );
            None
        }
    }
}

async fn tcp_round(
    writer: &mut TcpWriter,
    reader: &mut TcpReader,
    seq: u64,
    cfg: &ClientConfig,
) -> Result<(), String> {
    send_tcp_line(writer, Message::helo(seq)).await?;
    wait_tcp_msg(reader, MessageKind::Ok, seq, cfg.timeout).await?;
    send_tcp_line(writer, Message::ack(seq)).await?;
    wait_tcp_msg(reader, MessageKind::End, seq, cfg.timeout).await
}

async fn send_tcp_line(writer: &mut TcpWriter, msg: Message) -> Result<(), String> {
    let line = format!("{msg}\n");
    writer
        .write_all(line.as_bytes())
        .await
        .map_err(|e| format!("write error: {e}"))?;
    writer
        .flush()
        .await
        .map_err(|e| format!("flush error: {e}"))?;
    info!("[SEND] {msg}");
    Ok(())
}

async fn wait_tcp_msg(
    reader: &mut TcpReader,
    expect: MessageKind,
    seq: u64,
    timeout_dur: Duration,
) -> Result<(), String> {
    let deadline = Instant::now() + timeout_dur;
    let mut line = String::new();
    loop {
        let remain = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remain, reader.read_line(&mut line)).await {
            Err(_) => return Err(format!("timeout waiting for {expect}({seq})")),
            Ok(Err(e)) => return Err(format!("read error: {e}")),
            Ok(Ok(0)) => return Err("connection closed by peer".to_string()),
            Ok(Ok(_)) => {
                if line.len() > MAX_MESSAGE_LEN + 1 {
                    warn!(
                        "[WARN] oversized line from server ({} bytes), ignoring",
                        line.len()
                    );
                    line.clear();
                    continue;
                }
                let text = line.trim();
                match Message::parse(text) {
                    Ok(m) if m.kind() == expect && m.seq() == seq => {
                        info!("[RECV] {m}");
                        return Ok(());
                    }
                    Ok(m) => {
                        warn!("[WARN] ignoring unexpected message {m}, expecting {expect}({seq})");
                        line.clear();
                    }
                    Err(e) => {
                        warn!("[WARN] invalid message {text:?} from server: {e}");
                        line.clear();
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------- UDP client

pub async fn run_udp(target: SocketAddr, cfg: &ClientConfig) -> anyhow::Result<Vec<RoundResult>> {
    log_banner("UDP", target, cfg);
    let bind_addr = if target.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind_addr).await?;
    let mut seq: u64 = 1;
    let mut results: Vec<RoundResult> = Vec::new();
    loop {
        if rounds_complete(cfg, results.len()) {
            break;
        }
        let start = Instant::now();
        let outcome = udp_round(&socket, target, seq, cfg).await;
        let elapsed_ms = elapsed_ms_since(start);
        match outcome {
            Ok(()) => {
                info!("[INFO] Round {seq} SUCCESS (elapsed {elapsed_ms:.3} ms)");
                results.push(RoundResult {
                    round: seq,
                    success: true,
                    elapsed_ms,
                });
            }
            Err(reason) => {
                warn!("[WARN] Round {seq} FAILED: {reason}");
                results.push(RoundResult {
                    round: seq,
                    success: false,
                    elapsed_ms,
                });
            }
        }
        seq += 1;
        log_waiting(cfg);
        tokio::time::sleep(cfg.interval).await;
    }
    Ok(results)
}

async fn udp_round(
    socket: &UdpSocket,
    target: SocketAddr,
    seq: u64,
    cfg: &ClientConfig,
) -> Result<(), String> {
    udp_step(
        socket,
        target,
        &Message::helo(seq).to_string(),
        MessageKind::Ok,
        seq,
        cfg,
    )
    .await?;
    udp_step(
        socket,
        target,
        &Message::ack(seq).to_string(),
        MessageKind::End,
        seq,
        cfg,
    )
    .await
}

async fn udp_step(
    socket: &UdpSocket,
    target: SocketAddr,
    payload: &str,
    expect: MessageKind,
    seq: u64,
    cfg: &ClientConfig,
) -> Result<(), String> {
    send_udp_datagram(socket, target, payload).await?;
    let mut deadline = Instant::now() + cfg.timeout;
    let mut buf = [0u8; MAX_MESSAGE_LEN];
    let mut attempts: u32 = 0;
    loop {
        let remain = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remain, socket.recv_from(&mut buf)).await {
            Err(_) => {
                attempts += 1;
                if attempts > cfg.retries {
                    return Err(format!(
                        "timeout waiting for {expect}({seq}) after {} retries",
                        cfg.retries
                    ));
                }
                warn!(
                    "[WARN] timeout waiting for {expect}({seq}), retry {attempts}/{}",
                    cfg.retries
                );
                send_udp_datagram(socket, target, payload).await?;
                deadline = Instant::now() + cfg.timeout;
            }
            Ok(Err(e)) => return Err(format!("recv error: {e}")),
            Ok(Ok((len, from))) => {
                if from != target {
                    warn!("[WARN] ignoring datagram from unexpected source {from}");
                    continue;
                }
                let text = std::str::from_utf8(&buf[..len])
                    .map_err(|_| format!("non-utf8 datagram from {target}"))?;
                match Message::parse(text) {
                    Ok(m) if m.kind() == expect && m.seq() == seq => {
                        info!("[RECV] {m}");
                        return Ok(());
                    }
                    Ok(m) => {
                        warn!("[WARN] ignoring unexpected message {m}, expecting {expect}({seq})");
                    }
                    Err(e) => {
                        warn!("[WARN] invalid datagram {text:?}: {e}");
                    }
                }
            }
        }
    }
}

async fn send_udp_datagram(
    socket: &UdpSocket,
    target: SocketAddr,
    payload: &str,
) -> Result<(), String> {
    socket
        .send_to(payload.as_bytes(), target)
        .await
        .map_err(|e| format!("send error: {e}"))?;
    info!("[SEND] {payload}");
    Ok(())
}

// ---------------------------------------------------------------- shared helpers

fn rounds_complete(cfg: &ClientConfig, done: usize) -> bool {
    matches!(cfg.max_rounds, Some(max) if (done as u64) >= max)
}

fn elapsed_ms_since(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn log_banner(protocol: &str, target: SocketAddr, cfg: &ClientConfig) {
    info!("[INFO] NetProbe Client started");
    info!("[INFO] Protocol: {protocol}");
    info!("[INFO] Target: {target}");
    info!("[INFO] Interval: {:.0} seconds", cfg.interval.as_secs_f64());
    info!("[INFO] Timeout: {:.0} seconds", cfg.timeout.as_secs_f64());
    info!("[INFO] Retries: {}", cfg.retries);
    match cfg.max_rounds {
        Some(max) => info!("[INFO] Max rounds: {max}"),
        None => info!("[INFO] Running continuously (press Ctrl+C to stop)"),
    }
}

fn log_waiting(cfg: &ClientConfig) {
    info!(
        "[INFO] Waiting {:.0} seconds...",
        cfg.interval.as_secs_f64()
    );
}
