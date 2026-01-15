use anyhow::Result;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use tracing::{info, warn};

pub const INGEST_COUNT: &str = "log_daemon_ingest_count";
pub const BYTES_PROCESSED: &str = "log_daemon_bytes_processed";
pub const DROPPED_MESSAGES: &str = "log_daemon_dropped_messages";
pub const WRITE_LATENCY: &str = "log_daemon_write_latency_ms";
pub const ACTIVE_CONNECTIONS: &str = "log_daemon_active_connections";

/// Initialize metrics exporter and signal handler
pub async fn init_metrics(port: u16) -> Result<()> {
    // Setup Prometheus exporter
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let builder = PrometheusBuilder::new();

    builder
        .with_http_listener(addr)
        .install()
        .map_err(|e| anyhow::anyhow!("Failed to install Prometheus exporter: {}", e))?;

    info!(
        "Metrics endpoint listening on http://0.0.0.0:{}/metrics",
        port
    );

    // Spawn signal handler for SIGUSR1 to dump stats to log
    tokio::spawn(async move {
        if let Err(e) = handle_signals().await {
            warn!("Signal handler error: {}", e);
        }
    });

    Ok(())
}

#[cfg(unix)]
async fn handle_signals() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigusr1 = signal(SignalKind::user_defined1())?;

    loop {
        sigusr1.recv().await;
        dump_stats();
    }
}

#[cfg(not(unix))]
async fn handle_signals() -> Result<()> {
    // No-op on non-unix
    std::future::pending::<()>().await;
    Ok(())
}

fn dump_stats() {
    // getting metrics values is a bit complex with the generic facade,
    // so we'll just log that we received the signal for now
    // In a real app we might maintain a separate atomic counter for easy dumping
    info!("Received SIGUSR1: Metrics are available at /metrics endpoint");
}
