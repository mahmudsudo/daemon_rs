use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use serde_json::json;
use std::time::Duration;

fn benchmark_json_validation(c: &mut Criterion) {
    let validator = daemon_rs::schema::SchemaValidator::default_schema().unwrap();

    let log = json!({
        "timestamp": "2026-01-15T19:00:00Z",
        "level": "info",
        "message": "Benchmark log message",
        "service": "benchmark",
        "metadata": {
            "key": "value",
            "count": 123
        }
    });

    c.bench_function("validate_log", |b| {
        b.iter(|| {
            validator.validate(black_box(&log)).unwrap();
        })
    });
}

fn benchmark_parquet_write(c: &mut Criterion) {
    use daemon_rs::storage::{parse_compression, StorageEngine};
    use tempfile::TempDir;

    let mut group = c.benchmark_group("parquet_write");
    group.throughput(Throughput::Elements(1000));
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("batch_1000", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let mut engine = StorageEngine::new(
                temp_dir.path().to_path_buf(),
                parse_compression("snappy"),
                1000,
                1024 * 1024 * 100,
            )
            .unwrap();

            for i in 0..1000 {
                let log: daemon_rs::schema::LogEntry = serde_json::from_value(json!({
                    "timestamp": "2026-01-15T19:00:00Z",
                    "level": "info",
                    "message": format!("Benchmark message {}", i),
                }))
                .unwrap();
                engine.add_log(log).unwrap();
            }

            engine.flush().unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, benchmark_json_validation, benchmark_parquet_write);
criterion_main!(benches);
