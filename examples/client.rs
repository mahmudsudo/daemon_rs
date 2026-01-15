use anyhow::Result;
use serde_json::json;
use std::io::Write;
use std::os::unix::net::UnixStream;

fn main() -> Result<()> {
    let socket_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/logdaemon.sock".to_string());

    println!("Connecting to {}...", socket_path);
    let mut stream = match UnixStream::connect(&socket_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to socket: {}", e);
            eprintln!("Make sure the daemon is running: cargo run --release -- serve");
            return Err(e.into());
        }
    };

    // Send some example logs
    let logs = vec![
        json!({
            "timestamp": "2026-01-15T19:00:00Z",
            "level": "info",
            "message": "Application started",
            "service": "example-client"
        }),
        json!({
            "timestamp": "2026-01-15T19:00:01Z",
            "level": "debug",
            "message": "Processing request",
            "service": "example-client",
            "metadata": {
                "request_id": "req-123",
                "user_id": 42
            }
        }),
        json!({
            "timestamp": "2026-01-15T19:00:02Z",
            "level": "warn",
            "message": "High memory usage detected",
            "service": "example-client",
            "metadata": {
                "memory_mb": 1024
            }
        }),
        json!({
            "timestamp": "2026-01-15T19:00:03Z",
            "level": "error",
            "message": "Database connection failed",
            "service": "example-client",
            "trace_id": "trace-abc-123",
            "metadata": {
                "error": "Connection timeout",
                "retry_count": 3
            }
        }),
    ];

    for log in &logs {
        let json_str = log.to_string();
        let length = (json_str.len() as u32).to_be_bytes();

        // Send length-prefixed message
        stream.write_all(&length)?;
        stream.write_all(json_str.as_bytes())?;
        stream.flush()?;

        println!("✓ Sent: {}", log["message"]);

        // Small delay between messages
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    println!("\nSent {} logs successfully!", logs.len());

    Ok(())
}
