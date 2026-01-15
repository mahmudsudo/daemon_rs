use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::time::Instant;

fn main() -> Result<()> {
    let socket_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/logdaemon.sock".to_string());

    let count: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10000);

    println!("Connecting to {}...", socket_path);
    let mut stream = UnixStream::connect(&socket_path)?;

    println!("Sending {} logs...", count);
    let start = Instant::now();

    for i in 0..count {
        let log = json!({
            "timestamp": Utc::now().to_rfc3339(),
            "level": if i % 10 == 0 { "error" } else { "info" },
            "message": format!("Load test message {}", i),
            "service": "load-test",
            "metadata": {
                "iteration": i,
                "batch": i / 1000
            }
        });

        let json_str = log.to_string();
        let length = (json_str.len() as u32).to_be_bytes();

        stream.write_all(&length)?;
        stream.write_all(json_str.as_bytes())?;

        if i % 1000 == 0 {
            stream.flush()?;
            print!("\rSent: {}/{}", i, count);
            std::io::stdout().flush()?;
        }
    }

    stream.flush()?;
    let elapsed = start.elapsed();

    println!("\n\n✓ Sent {} logs in {:.2}s", count, elapsed.as_secs_f64());
    println!(
        "Throughput: {:.0} logs/second",
        count as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}
