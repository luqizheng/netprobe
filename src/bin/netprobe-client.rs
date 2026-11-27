use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::time::Duration;

use anyhow::{anyhow, Result};
use clap::{Parser, ValueEnum};
use tracing::{error, info};

use netprobe::client::{run_tcp, run_udp, ClientConfig};

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Protocol {
    Tcp,
    Udp,
}

#[derive(Parser)]
#[command(
    name = "netprobe-client",
    about = "TCP/UDP bidirectional connectivity probe client"
)]
struct Args {
    /// Transport protocol to use
    #[arg(long, value_enum, default_value_t = Protocol::Tcp)]
    protocol: Protocol,

    /// Target server IP or hostname (required)
    #[arg(long)]
    host: String,

    /// Target server port
    #[arg(long, default_value_t = 9000)]
    port: u16,

    /// Seconds to wait between rounds
    #[arg(long, default_value_t = 5)]
    interval_secs: u64,

    /// Seconds to wait for a single response before timeout/retransmit
    #[arg(long, default_value_t = 3)]
    timeout_secs: u64,

    /// Max retransmission attempts per UDP step (excludes first send)
    #[arg(long, default_value_t = 3)]
    retries: u32,
}

fn resolve_target(host: &str, port: u16) -> Result<SocketAddr> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    let addrs: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();
    addrs
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("could not resolve host: {host}"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    netprobe::init_tracing();
    let target = resolve_target(&args.host, args.port)?;
    let cfg = ClientConfig {
        interval: Duration::from_secs(args.interval_secs),
        timeout: Duration::from_secs(args.timeout_secs),
        retries: args.retries,
        max_rounds: None,
    };

    tokio::select! {
        res = async {
            match args.protocol {
                Protocol::Tcp => run_tcp(target, &cfg).await,
                Protocol::Udp => run_udp(target, &cfg).await,
            }
        } => {
            match res {
                Ok(rounds) => info!("[INFO] Client finished after {} rounds", rounds.len()),
                Err(e) => {
                    error!("[ERROR] Client terminated: {e}");
                    std::process::exit(1);
                }
            }
        }
        _ = tokio::signal::ctrl_c() => info!("[INFO] Ctrl+C received, shutting down"),
    }
    Ok(())
}
