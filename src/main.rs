mod ai_api;
mod config;
mod metrics;
mod otel;
mod query;
mod schema;
mod server;
mod storage;
mod trace_storage;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::info;

use query::QueryEngine;
use schema::SchemaValidator;
use server::LogServer;
use storage::{parse_compression, StorageEngine};

#[derive(Parser)]
#[command(name = "daemon_rs")]
#[command(about = "High-Performance Structured Logging Daemon", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the log daemon server
    Serve {
        /// Path to Unix socket
        #[arg(short, long, default_value = "/tmp/logdaemon.sock")]
        socket: PathBuf,

        /// Storage directory for Parquet files
        #[arg(short = 'd', long, default_value = "./logs")]
        storage: PathBuf,

        /// Path to JSON Schema file (optional, uses default if not provided)
        #[arg(long)]
        schema: Option<PathBuf>,

        /// Batch size for Parquet writes
        #[arg(short, long, default_value = "1000")]
        batch_size: usize,

        /// Compression codec (snappy, zstd, gzip, none)
        #[arg(short, long, default_value = "snappy")]
        compression: String,

        /// Maximum concurrent connections
        #[arg(short, long, default_value = "1000")]
        max_connections: usize,

        /// File rotation size in MB
        #[arg(short, long, default_value = "100")]
        rotation_mb: u64,

        /// Flush interval in seconds
        #[arg(short, long, default_value = "5")]
        flush_interval: u64,

        /// Enable OpenTelemetry tracing
        #[arg(long, default_value = "true")]
        otel_enabled: bool,

        /// OTLP endpoint for trace export (optional)
        #[arg(long)]
        otel_endpoint: Option<String>,

        /// Trace sampling rate (0.0 to 1.0)
        #[arg(long, default_value = "1.0")]
        otel_sampling_rate: f64,

        /// AI API server port
        #[arg(long, default_value = "9101")]
        ai_api_port: u16,

        /// Trace storage directory
        #[arg(long, default_value = "./traces")]
        trace_storage: PathBuf,
    },

    /// Query stored logs
    Query {
        /// Storage directory
        #[arg(short = 'd', long, default_value = "./logs")]
        storage: PathBuf,

        /// Show total count only
        #[arg(short, long)]
        count: bool,
    },

    /// Validate a JSON Schema file
    ValidateSchema {
        /// Path to schema file
        schema: PathBuf,
    },

    /// Ingest logs from stdin (for testing)
    Ingest {
        /// Path to Unix socket
        #[arg(short, long, default_value = "/tmp/logdaemon.sock")]
        socket: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Serve {
            socket,
            storage,
            schema,
            batch_size,
            compression,
            max_connections,
            rotation_mb,
            flush_interval,
            otel_enabled,
            otel_endpoint,
            otel_sampling_rate,
            ai_api_port,
            trace_storage,
        } => {
            info!("Starting log daemon server...");

            // Initialize OpenTelemetry if enabled
            if otel_enabled {
                info!("Initializing OpenTelemetry tracing...");
                let subscriber = otel::init_tracing_and_subscriber(
                    "daemon_rs",
                    otel_endpoint.clone(),
                    otel_sampling_rate,
                )?;
                tracing::subscriber::set_global_default(subscriber)
                    .expect("Failed to set tracing subscriber");
            } else {
                // Standard tracing without OTEL
                tracing_subscriber::fmt()
                    .with_env_filter(
                        tracing_subscriber::EnvFilter::from_default_env()
                            .add_directive(tracing::Level::INFO.into()),
                    )
                    .init();
            }

            // Initialize metrics on port 9100
            crate::metrics::init_metrics(9100).await?;

            // Start AI API server if OTEL is enabled
            if otel_enabled {
                let trace_dir = trace_storage.clone();
                let api_port = ai_api_port;
                tokio::spawn(async move {
                    if let Err(e) = ai_api::start_api_server(api_port, trace_dir).await {
                        eprintln!("AI API server error: {}", e);
                    }
                });
                info!("AI Agent API started on port {}", ai_api_port);
            }

            info!("Socket: {:?}", socket);
            info!("Storage: {:?}", storage);
            info!("Batch size: {}", batch_size);
            info!("Compression: {}", compression);

            // Load or create schema validator
            let validator = if let Some(schema_path) = schema {
                info!("Loading schema from {:?}", schema_path);
                SchemaValidator::from_file(&schema_path)?
            } else {
                info!("Using default schema");
                SchemaValidator::default_schema()?
            };

            // Create storage engine
            let storage_engine = StorageEngine::new(
                storage,
                parse_compression(&compression),
                batch_size,
                rotation_mb * 1024 * 1024,
            )?;

            // Create and run server (runs with tokio-uring)
            // Note: LogServer::run now blocks the current thread with tokio-uring runtime
            let server = LogServer::new(socket, validator, max_connections, flush_interval);

            // We need to run this outside of the current tokio runtime if we are inside one?
            // #[tokio::main] creates a runtime. tokio-uring creates its own.
            // Nesting tokio-uring inside tokio runtime is tricky.
            // Ideally we shouldn't use #[tokio::main] if using tokio-uring for the main thread.
            // But we need tokio for metrics/CLIs.

            // Solution: Spawn the server on a dedicated thread that sets up tokio-uring
            std::thread::spawn(move || {
                if let Err(e) = server.run(storage_engine) {
                    eprintln!("Server error: {}", e);
                    std::process::exit(1);
                }
            })
            .join()
            .expect("Server thread panicked");
        }

        Commands::Query { storage, count } => {
            let query_engine = QueryEngine::new(storage);

            if count {
                let total = query_engine.count_logs()?;
                println!("Total logs: {}", total);
            } else {
                let batches = query_engine.read_all()?;
                query_engine.print_logs(&batches)?;
            }
        }

        Commands::ValidateSchema { schema } => {
            info!("Validating schema: {:?}", schema);
            let _validator = SchemaValidator::from_file(&schema)?;
            println!("✓ Schema is valid");
        }

        Commands::Ingest { socket } => {
            use tokio::io::{AsyncBufReadExt, BufReader};
            use tokio::net::UnixStream;

            info!("Connecting to {:?}", socket);
            let stream = UnixStream::connect(&socket).await?;
            let (_reader, mut writer) = stream.into_split();

            let stdin = tokio::io::stdin();
            let mut stdin_reader = BufReader::new(stdin);
            let mut line = String::new();

            println!("Enter JSON logs (one per line, Ctrl+D to exit):");

            while stdin_reader.read_line(&mut line).await? > 0 {
                // Parse to validate JSON
                match serde_json::from_str::<serde_json::Value>(&line) {
                    Ok(json) => {
                        let json_str = json.to_string();
                        let length = json_str.len() as u32;

                        // Send length-prefixed message
                        use tokio::io::AsyncWriteExt;
                        writer.write_all(&length.to_be_bytes()).await?;
                        writer.write_all(json_str.as_bytes()).await?;
                        writer.flush().await?;

                        println!("✓ Sent");
                    }
                    Err(e) => {
                        eprintln!("✗ Invalid JSON: {}", e);
                    }
                }

                line.clear();
            }
        }
    }

    Ok(())
}
