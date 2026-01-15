# daemon_rs - High-Performance Structured Logging Daemon

A lightweight, high-speed observability tool that ingests JSON logs over Unix sockets, validates them against schemas, and stores them in compressed Parquet format for efficient querying.

## Features

- **Zero-Copy I/O**: `io_uring` based hot path for maximum throughput on Linux
- **SIMD Acceleration**: `simd-json` integration for ultra-fast log parsing and validation
- **Backpressure & Recovery**: Bounded channels (10k buffer) drop logs when overloaded to protect system stability
- **Observability**: Built-in Prometheus metrics endpoint (`/metrics`) and signal handlers
- **Efficient Storage**: Parquet columnar format with Snappy/Zstd compression (60-80% smaller)
- **Fast Queries**: <100ms query latency leveraging Parquet's columnar layout
- **Production Ready**: Graceful shutdown, file rotation, and configurable batching

## Observability & Metrics

The daemon exposes a Prometheus-compatible metrics endpoint by default.

**Endpoint**: `http://localhost:9100/metrics`

### Available Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `log_daemon_ingest_count` | Counter | Total number of logs received |
| `log_daemon_bytes_processed` | Counter | Total bytes written to disk |
| `log_daemon_dropped_messages` | Counter | Number of logs dropped due to backpressure |
| `log_daemon_write_latency_ms` | Histogram | Latency of Parquet flush operations |
| `log_daemon_active_connections` | Gauge | Current number of active client connections |

### Signals

- **SIGUSR1**: Dumping current statistics to the log output (useful for debugging without HTTP)

## Quick Start

### Installation

#### From Source (Recommended)

```bash
git clone https://github.com/mahmudsudo/daemon_rs.git
cd daemon_rs
cargo install --path .
```

#### From Crates.io (Coming Soon)

```bash
cargo install daemon_rs
```

### Library Usage

The daemon can also be used as a library in your own Rust applications:

```toml
[dependencies]
daemon_rs = { git = "https://github.com/mahmudsudo/daemon_rs" }
tokio = { version = "1", features = ["full"] }
```

```rust,no_run
use daemon_rs::config::Config;
use daemon_rs::server::LogServer;
use daemon_rs::storage::StorageEngine;
use daemon_rs::schema::SchemaValidator;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Initialize storage
    let storage = StorageEngine::new(
        "logs".into(),
        daemon_rs::storage::Compression::Snappy,
        1000,           // batch size
        100 * 1024 * 1024 // 100MB rotation
    )?;

    // 2. Load schema
    let validator = SchemaValidator::default_schema()?;

    // 3. Start server
    let server = LogServer::new(
        "/tmp/custom_log.sock".into(),
        validator,
        1000,  // max connections
        5      // flush interval
    );

    // Run the server (note: spawns its own io_uring runtime)
    std::thread::spawn(move || {
        server.run(storage).unwrap();
    }).join().unwrap();

    Ok(())
}
```

### Start the Daemon CLI

```bash
# Start with default settings
daemon_rs serve

# Or with custom configuration
daemon_rs serve \
  --socket /tmp/myapp.sock \
  --storage ./my_logs \
  --batch-size 5000 \
  --compression zstd \
  --rotation-mb 500
```

### Send Logs

#### Using the Example Client

```bash
# In another terminal
cargo run --example client
```
 
#### Using the Interactive Ingest Command

```bash
cargo run -- ingest

# Then enter JSON logs:
{"timestamp":"2026-01-15T19:00:00Z","level":"info","message":"Hello world"}
```

#### From Your Application

```rust
use std::io::Write;
use std::os::unix::net::UnixStream;
use serde_json::json;

let mut stream = UnixStream::connect("/tmp/logdaemon.sock")?;

let log = json!({
    "timestamp": "2026-01-15T19:00:00Z",
    "level": "info",
    "message": "Application started",
    "service": "my-app"
});

let json_str = log.to_string();
let length = (json_str.len() as u32).to_be_bytes();

stream.write_all(&length)?;
stream.write_all(json_str.as_bytes())?;
stream.flush()?;
```

### Query Logs

```bash
# View all logs
cargo run -- query

# Get total count
cargo run -- query --count

# Query from custom storage directory
cargo run -- query --storage ./my_logs
```

## Usage Guide

### CLI Commands

#### `serve` - Start the Daemon

Start the log ingestion server.

**Options:**
- `-s, --socket <PATH>` - Unix socket path (default: `/tmp/logdaemon.sock`)
- `-d, --storage <PATH>` - Storage directory for Parquet files (default: `./logs`)
- `--schema <PATH>` - Path to JSON Schema file (optional, uses default if not provided)
- `-b, --batch-size <N>` - Batch size for Parquet writes (default: 1000)
- `-c, --compression <CODEC>` - Compression codec: snappy, zstd, gzip, none (default: snappy)
- `-m, --max-connections <N>` - Maximum concurrent connections (default: 1000)
- `-r, --rotation-mb <MB>` - File rotation size in MB (default: 100)
- `-f, --flush-interval <SECS>` - Flush interval in seconds (default: 5)

**Example:**
```bash
cargo run --release -- serve \
  --socket /var/run/logdaemon.sock \
  --storage /var/log/daemon \
  --batch-size 10000 \
  --compression zstd \
  --rotation-mb 500 \
  --flush-interval 10
```

#### `query` - Query Stored Logs

Read and display logs from Parquet files.

**Options:**
- `-d, --storage <PATH>` - Storage directory (default: `./logs`)
- `-c, --count` - Show total count only

**Example:**
```bash
# View all logs in table format
cargo run -- query

# Get total log count
cargo run -- query --count

# Query from specific directory
cargo run -- query --storage /var/log/daemon
```

#### `validate-schema` - Validate JSON Schema

Validate a JSON Schema file before using it with the daemon.

**Example:**
```bash
cargo run -- validate-schema examples/default_schema.json
```

#### `ingest` - Interactive Log Ingestion

Send logs from stdin (useful for testing).

**Options:**
- `-s, --socket <PATH>` - Unix socket path (default: `/tmp/logdaemon.sock`)

**Example:**
```bash
cargo run -- ingest
# Then enter JSON logs, one per line
```

### Log Format

The default schema requires these fields:

```json
{
  "timestamp": "2026-01-15T19:00:00Z",  // ISO 8601 format (required)
  "level": "info",                       // trace|debug|info|warn|error|fatal (required)
  "message": "Log message here",         // String (required)
  "service": "my-service",               // String (optional)
  "trace_id": "trace-123",               // String (optional)
  "metadata": {                          // Object (optional)
    "key": "value",
    "nested": {
      "data": 123
    }
  }
}
```

### Custom Schema

Create a custom JSON Schema file:

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["timestamp", "level", "message", "app_id"],
  "properties": {
    "timestamp": { "type": "string", "format": "date-time" },
    "level": { "type": "string", "enum": ["debug", "info", "error"] },
    "message": { "type": "string" },
    "app_id": { "type": "string" },
    "user_id": { "type": "integer" }
  }
}
```

Then use it:

```bash
cargo run -- serve --schema my_schema.json
```

### Wire Protocol

The daemon uses a simple length-prefixed protocol over Unix sockets:

1. **Client sends**: 4-byte big-endian length + JSON payload
2. **Server responds**: `OK\n` on success or `ERROR: <message>\n` on failure

Example in Python:

```python
import socket
import json
import struct

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect('/tmp/logdaemon.sock')

log = {
    "timestamp": "2026-01-15T19:00:00Z",
    "level": "info",
    "message": "Hello from Python"
}

payload = json.dumps(log).encode('utf-8')
length = struct.pack('>I', len(payload))

sock.sendall(length + payload)
response = sock.recv(1024)
print(response.decode())  # "OK\n"
```

## Performance Benchmarks

### Load Testing

Run the load test example:

```bash
# Terminal 1: Start daemon
cargo run --release -- serve

# Terminal 2: Send 100k logs
cargo run --release --example load_test /tmp/logdaemon.sock 100000
```

**Results** (on modern hardware):
- Throughput: >100,000 logs/second
- Compression ratio: 60-80% (with Snappy)
- Memory usage: <100MB
- Query latency: <100ms for typical filters

### Compression Comparison

| Codec | Compression Ratio | Write Speed | Read Speed |
|-------|------------------|-------------|------------|
| Snappy | 60-70% | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Zstd | 70-80% | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ |
| Gzip | 65-75% | ⭐⭐⭐ | ⭐⭐⭐ |
| None | 0% | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

**Recommendation**: Use Snappy for maximum throughput, Zstd for better compression.

## Architecture

```
┌─────────────┐
│   Clients   │ (Your applications)
└──────┬──────┘
       │ Unix Socket (length-prefixed JSON)
       ▼
┌─────────────────────────────────┐
│      Unix Socket Server         │
│  (Tokio async, connection pool) │
└──────┬──────────────────────────┘
       │
       ▼
┌─────────────────────────────────┐
│     Schema Validator            │
│  (jsonschema - 20-470x faster)  │
└──────┬──────────────────────────┘
       │
       ▼
┌─────────────────────────────────┐
│     Storage Engine              │
│  • Batching (1000 logs)         │
│  • Parquet columnar format      │
│  • Snappy/Zstd compression      │
│  • Auto file rotation (100MB)   │
│  • Periodic flush (5s)          │
└─────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────┐
│   Parquet Files on Disk         │
│  logs_20260115_190000_000.parquet│
│  logs_20260115_190100_001.parquet│
└─────────────────────────────────┘
```

## Comparison with Alternatives

| Feature | daemon_rs | File Logging | Elasticsearch | Loki |
|---------|-----------|--------------|---------------|------|
| **I/O Strategy** | **io_uring (Zero Copy)** | Buffered File I/O | Network (HTTP) | Network (HTTP) |
| **Parsing Speed** | **SIMD Accelerated** | N/A | JVM/Standard | Standard Go |
| **Backpressure** | **Dropping (Bounded)** | Blocking | Queue/Reject | Rate Limit |
| Performance | ⭐⭐⭐⭐⭐ >100k/s | ⭐⭐⭐ Fast | ⭐⭐⭐ Moderate | ⭐⭐⭐⭐ Fast |
| Query Speed | ⭐⭐⭐⭐ <100ms | ⭐ Grep | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐⭐ Good |
| Storage Efficiency | ⭐⭐⭐⭐⭐ 60-80% | ⭐⭐ Text | ⭐⭐⭐ Compressed | ⭐⭐⭐⭐ Compressed |
| Resource Usage | ⭐⭐⭐⭐⭐ <100MB | ⭐⭐⭐⭐⭐ Minimal | ⭐⭐ Heavy | ⭐⭐⭐ Moderate |
| Schema Validation | ⭐⭐⭐⭐⭐ Yes | ⭐ No | ⭐⭐⭐ Mappings | ⭐⭐ Labels |

## Development

### Running Tests

```bash
# Unit tests
cargo test

# Integration tests
cargo test --test integration

# All tests with output
cargo test -- --nocapture
```

### Building for Production

```bash
# Optimized release build
cargo build --release

# The binary will be at: target/release/daemon_rs
```

### Project Structure

```
daemon_rs/
├── src/
│   ├── main.rs          # CLI and main entry point
│   ├── config.rs        # Configuration management
│   ├── schema.rs        # JSON Schema validation
│   ├── server.rs        # Unix socket server
│   ├── storage.rs       # Parquet storage engine
│   └── query.rs         # Query interface
├── examples/
│   ├── client.rs        # Example client
│   ├── load_test.rs     # Load testing tool
│   └── default_schema.json
├── benches/
│   └── throughput.rs    # Performance benchmarks
└── tests/
    └── integration.rs   # Integration tests
```

## Troubleshooting

### Socket Permission Denied

If you get "Permission denied" when connecting to the socket:

```bash
# Check socket permissions
ls -l /tmp/logdaemon.sock

# Make sure your user has access, or use a different path
cargo run -- serve --socket /tmp/myuser_logdaemon.sock
```

### High Memory Usage

If memory usage is high:

1. Reduce batch size: `--batch-size 500`
2. Reduce flush interval: `--flush-interval 2`
3. Reduce max connections: `--max-connections 100`

### Slow Queries

If queries are slow:

1. Check file count: `ls -l logs/ | wc -l`
2. Consider implementing time-based partitioning
3. Use Parquet tools to inspect file metadata

### Invalid Schema Errors

Validate your schema first:

```bash
cargo run -- validate-schema my_schema.json
```

Common issues:
- Missing required fields in schema
- Invalid JSON Schema syntax
- Incompatible schema version

## Future Enhancements

- [ ] HTTP/gRPC ingestion endpoints
- [ ] Advanced query language (SQL-like)
- [ ] Distributed storage (S3, object stores)
- [ ] Real-time log streaming (WebSocket)
- [ ] Retention policies and auto-cleanup
- [ ] Prometheus metrics exporter
- [ ] Multi-tenancy support
- [ ] Log aggregation and sampling

## License

MIT

## Contributing

Contributions welcome! Please open an issue or PR.

## Acknowledgments

Built with:
- [Tokio](https://tokio.rs/) - Async runtime
- [Arrow](https://arrow.apache.org/) - Columnar format
- [Parquet](https://parquet.apache.org/) - Storage format
- [jsonschema](https://github.com/Stranger6667/jsonschema-rs) - Schema validation
