use anyhow::{Context, Result};
use bytes::{Buf, BytesMut};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::Duration;
use tokio_uring::net::{UnixListener, UnixStream};
use tracing::{debug, error, info, warn};

use crate::schema::{LogEntry, SchemaValidator};
use crate::storage::StorageEngine;

/// Unix socket server using io_uring for zero-copy ingestion
pub struct LogServer {
    socket_path: std::path::PathBuf,
    validator: Arc<SchemaValidator>,
    max_connections: usize,
    flush_interval: Duration,
}

impl LogServer {
    pub fn new(
        socket_path: std::path::PathBuf,
        validator: SchemaValidator,
        max_connections: usize,
        flush_interval_secs: u64,
    ) -> Self {
        Self {
            socket_path,
            validator: Arc::new(validator),
            max_connections,
            flush_interval: Duration::from_secs(flush_interval_secs),
        }
    }

    /// Start the server and listen for connections using io_uring
    pub fn run(self, storage: StorageEngine) -> Result<()> {
        tokio_uring::start(async move { self.run_async(storage).await })
    }

    async fn run_async(self, mut storage: StorageEngine) -> Result<()> {
        // Remove existing socket file if it exists
        if self.socket_path.exists() {
            std::fs::remove_file(&self.socket_path).with_context(|| {
                format!("Failed to remove existing socket: {:?}", self.socket_path)
            })?;
        }

        // Bind to Unix socket using tokio-uring
        let listener = UnixListener::bind(&self.socket_path)
            .with_context(|| format!("Failed to bind to socket: {:?}", self.socket_path))?;

        info!(
            "Log daemon listening on {:?} (io_uring enabled)",
            self.socket_path
        );

        // Create bounded channel for backpressure (10k items)
        let (tx, mut rx) = mpsc::channel::<LogEntry>(10000);

        // Semaphore for connection limiting
        let semaphore = Arc::new(Semaphore::new(self.max_connections));

        // Spawn storage task that consumes the channel
        let flush_interval = self.flush_interval;
        tokio_uring::spawn(async move {
            loop {
                match tokio::time::timeout(flush_interval, rx.recv()).await {
                    Ok(Some(log)) => {
                        if let Err(e) = storage.add_log(log) {
                            error!("Storage error: {}", e);
                        }
                    }
                    Ok(None) => break, // Channel closed
                    Err(_) => {
                        // Timeout, flush
                        if let Err(e) = storage.flush() {
                            error!("Flush error: {}", e);
                        }
                    }
                }
            }
            // Final flush
            let _ = storage.flush();
        });

        // Connection counter
        let active_connections = std::sync::atomic::AtomicUsize::new(0);
        let active_connections = Arc::new(active_connections);

        // Accept connections
        loop {
            let permit = semaphore.clone().acquire_owned().await?;

            match listener.accept().await {
                Ok(stream) => {
                    // tokio-uring accept returns Ok(stream)
                    let tx = tx.clone();
                    let validator = self.validator.clone();
                    let connections = active_connections.clone();

                    tokio_uring::spawn(async move {
                        // Increment gauge
                        let count =
                            connections.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                        metrics::gauge!(crate::metrics::ACTIVE_CONNECTIONS, count as f64);

                        if let Err(e) = handle_connection(stream, tx, validator).await {
                            debug!("Connection closed: {}", e);
                        }

                        // Decrement gauge
                        let count =
                            connections.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
                        metrics::gauge!(crate::metrics::ACTIVE_CONNECTIONS, count as f64);

                        drop(permit);
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}

/// Handle a single client connection
async fn handle_connection(
    stream: UnixStream,
    tx: mpsc::Sender<LogEntry>,
    validator: Arc<SchemaValidator>,
) -> Result<()> {
    // 8KB read buffer
    let mut buf = vec![0u8; 8192];
    // Accumulation buffer for framing
    let mut accumulator = BytesMut::with_capacity(16384);

    let stream = stream;

    loop {
        // Read into buffer using io_uring
        let (res, b) = stream.read(buf).await;
        buf = b;
        let n = res?;

        if n == 0 {
            break;
        }

        // Append read data to accumulator
        accumulator.extend_from_slice(&buf[..n]);

        // Process framed messages
        loop {
            // Need at least 4 bytes for length
            if accumulator.len() < 4 {
                break;
            }

            // Peek length
            let length = u32::from_be_bytes([
                accumulator[0],
                accumulator[1],
                accumulator[2],
                accumulator[3],
            ]) as usize;

            // Check if we have the full message
            if accumulator.len() < 4 + length {
                break;
            }

            // Consume length + message
            accumulator.advance(4);
            // We need a mutable slice for SIMD parsing.
            // accumulator.split_to(length) gives us a BytesMut which is mostly contiguous,
            // but simd-json needs `&mut [u8]`.
            // We can make it contiguous.
            let mut msg_bytes = accumulator.split_to(length);

            // Fast Parse (SIMD)
            // Note: simd_json modifies the input slice (in-place string filtering)
            match validator.parse_fast(&mut msg_bytes) {
                Ok(log) => {
                    // Backpressure check: try_send
                    match tx.try_send(log) {
                        Ok(_) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            metrics::counter!(crate::metrics::DROPPED_MESSAGES, 1);
                            // In a real implementation we would send error back to client
                            // But for io_uring proof-of-concept avoiding complex Write logic for now
                            warn!("Backend overloaded, dropping log");

                            // To send error:
                            // let (res, b) = stream.write(vec_from("ERROR: Overloaded")).await;
                            // buf = b; ... difficult with moved stream.
                        }
                        Err(_) => break, // Channel closed
                    }
                }
                Err(e) => {
                    warn!("Invalid log: {}", e);
                }
            }
        }
    }

    Ok(())
}
