pub mod client;
pub mod protocol;
pub mod server;

pub fn init_tracing() {
    tracing_subscriber::fmt().with_target(false).init();
}
