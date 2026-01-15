mod config;
mod metrics;
mod query;
mod schema;
mod server;
mod storage;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber;

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
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

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
        } => {
            info!("Starting log daemon server...");

            // Initialize metrics on port 9100
            crate::metrics::init_metrics(9100).await?;

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
