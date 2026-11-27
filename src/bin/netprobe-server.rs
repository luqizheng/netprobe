use std::net::SocketAddr;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use tokio::net::{TcpListener, UdpSocket};
use tracing::info;

use netprobe::server;

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Protocol {
    Tcp,
    Udp,
}

#[derive(Parser)]
#[command(
    name = "netprobe-server",
    about = "TCP/UDP bidirectional connectivity probe server"
)]
struct Args {
    /// Transport protocol to listen on
    #[arg(long, value_enum, default_value_t = Protocol::Tcp)]
    protocol: Protocol,

    /// IP address to bind (IPv4 or IPv6)
    #[arg(long, default_value = "0.0.0.0")]
    bind: std::net::IpAddr,

    /// Port to listen on
    #[arg(long, default_value_t = 9000)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    netprobe::init_tracing();
    let bind_addr = SocketAddr::new(args.bind, args.port);

    match args.protocol {
        Protocol::Tcp => {
            let listener = TcpListener::bind(bind_addr).await?;
            info!("[INFO] TCP Server started");
            info!("[INFO] TCP Server listening on {bind_addr}");
            info!("[INFO] Waiting for connections...");
            tokio::select! {
                res = server::run_tcp(listener) => res?,
                _ = tokio::signal::ctrl_c() => info!("[INFO] Ctrl+C received, shutting down"),
            }
        }
        Protocol::Udp => {
            let socket = UdpSocket::bind(bind_addr).await?;
            info!("[INFO] UDP Server started");
            info!("[INFO] UDP Server listening on {bind_addr}");
            info!("[INFO] Waiting for datagrams...");
            tokio::select! {
                res = server::run_udp(socket) => res?,
                _ = tokio::signal::ctrl_c() => info!("[INFO] Ctrl+C received, shutting down"),
            }
        }
    }
    Ok(())
}
