use anyhow::{Context, Result};
use arrow::array::{ArrayRef, RecordBatch, StringBuilder, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info};

use crate::schema::LogEntry;

/// Storage engine for writing logs to Parquet files
pub struct StorageEngine {
    storage_dir: PathBuf,
    compression: Compression,
    batch_size: usize,
    // rotation_size: u64, // Deprecated: we rotate on every flush now

    // Current batch
    current_batch: Vec<LogEntry>,
    current_file_path: Option<PathBuf>,
    current_file_size: u64,
    file_counter: u64,
}

impl StorageEngine {
    pub fn new(
        storage_dir: PathBuf,
        compression: Compression,
        batch_size: usize,
        _rotation_size: u64,
    ) -> Result<Self> {
        // Create storage directory if it doesn't exist
        std::fs::create_dir_all(&storage_dir)
            .with_context(|| format!("Failed to create storage directory: {:?}", storage_dir))?;

        Ok(Self {
            storage_dir,
            compression,
            batch_size,
            // rotation_size,
            current_batch: Vec::with_capacity(batch_size),
            current_file_path: None,
            current_file_size: 0,
            file_counter: 0,
        })
    }

    /// Add a log entry to the current batch
    #[tracing::instrument(skip(self, log), fields(batch_size = self.current_batch.len()))]
    pub fn add_log(&mut self, log: LogEntry) -> Result<()> {
        self.current_batch.push(log);
        metrics::counter!(crate::metrics::INGEST_COUNT, 1);

        // Flush if batch is full
        if self.current_batch.len() >= self.batch_size {
            self.flush()?;
        }

        Ok(())
    }

    /// Flush the current batch to disk
    #[tracing::instrument(skip(self), fields(batch_size = self.current_batch.len()))]
    pub fn flush(&mut self) -> Result<()> {
        if self.current_batch.is_empty() {
            return Ok(());
        }

        debug!("Flushing {} logs to Parquet", self.current_batch.len());
        let start = std::time::Instant::now();

        // Always generate a new file for each batch to ensure valid Parquet
        // (Appending to Parquet requires keeping writer open or complex merging)
        let file_path = self.generate_file_path();

        // Convert logs to RecordBatch
        let batch = self.logs_to_record_batch(&self.current_batch)?;
        let _num_rows = batch.num_rows();

        // Write to Parquet
        self.write_record_batch(&file_path, batch)?;

        let elapsed = start.elapsed().as_millis() as u64;
        metrics::histogram!(crate::metrics::WRITE_LATENCY, elapsed as f64);
        metrics::counter!(crate::metrics::BYTES_PROCESSED, self.current_file_size); // Approximate increment

        // Clear the current batch
        self.current_batch.clear();

        // Reset file path tracking (we don't keep files open across batches currently)
        self.current_file_path = None;
        self.current_file_size = 0;

        Ok(())
    }

    /*
    /// Check if the current file should be rotated
    fn should_rotate(&self) -> bool {
        self.current_file_size >= self.rotation_size
    }

    /// Rotate to a new file
    fn rotate_file(&mut self) -> Result<()> {
        info!("Rotating log file (size: {} bytes)", self.current_file_size);
        self.current_file_path = None;
        self.current_file_size = 0;
        Ok(())
    }
    */

    /// Generate a new file path with timestamp
    fn generate_file_path(&mut self) -> PathBuf {
        let now = Utc::now();
        let filename = format!(
            "logs_{}_{}.parquet",
            now.format("%Y%m%d_%H%M%S_%3f"),
            self.file_counter
        );
        self.file_counter += 1;
        self.storage_dir.join(filename)
    }

    /// Convert JSON logs to Arrow RecordBatch
    fn logs_to_record_batch(&self, logs: &[LogEntry]) -> Result<RecordBatch> {
        let schema = self.create_schema();
        let mut timestamp_builder = Vec::with_capacity(logs.len());
        let mut level_builder = StringBuilder::new();
        let mut message_builder = StringBuilder::new();
        let mut service_builder = StringBuilder::new();
        let mut trace_id_builder = StringBuilder::new();
        let mut metadata_builder = StringBuilder::new();

        for log in logs {
            // Timestamp
            let timestamp = DateTime::parse_from_rfc3339(&log.timestamp)
                .ok()
                .map(|dt| dt.timestamp_millis())
                .unwrap_or(0);
            timestamp_builder.push(timestamp);

            // Level
            level_builder.append_value(&log.level);

            // Message
            message_builder.append_value(&log.message);

            // Optional fields
            if let Some(s) = &log.service {
                service_builder.append_value(s);
            } else {
                service_builder.append_null();
            }

            if let Some(t) = &log.trace_id {
                trace_id_builder.append_value(t);
            } else {
                trace_id_builder.append_null();
            }

            // Metadata
            if let Some(m) = &log.metadata {
                metadata_builder.append_value(m.to_string());
            } else {
                metadata_builder.append_null();
            }
        }

        // Build arrays
        let timestamp_array =
            Arc::new(TimestampMillisecondArray::from(timestamp_builder)) as ArrayRef;
        let level_array = Arc::new(level_builder.finish()) as ArrayRef;
        let message_array = Arc::new(message_builder.finish()) as ArrayRef;
        let service_array = Arc::new(service_builder.finish()) as ArrayRef;
        let trace_id_array = Arc::new(trace_id_builder.finish()) as ArrayRef;
        let metadata_array = Arc::new(metadata_builder.finish()) as ArrayRef;

        RecordBatch::try_new(
            schema,
            vec![
                timestamp_array,
                level_array,
                message_array,
                service_array,
                trace_id_array,
                metadata_array,
            ],
        )
        .context("Failed to create RecordBatch")
    }

    /// Create Arrow schema for log entries
    fn create_schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("level", DataType::Utf8, false),
            Field::new("message", DataType::Utf8, false),
            Field::new("service", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("metadata", DataType::Utf8, true),
        ]))
    }

    /// Write RecordBatch to Parquet file
    fn write_record_batch(&mut self, path: &Path, batch: RecordBatch) -> Result<()> {
        let file = File::create(path)?;

        let props = WriterProperties::builder()
            .set_compression(self.compression)
            .build();

        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Update file size
        let metadata = std::fs::metadata(path)?;
        self.current_file_size = metadata.len();

        info!("Wrote {} rows to {:?}", batch.num_rows(), path);

        Ok(())
    }

    /// Get list of all Parquet files in storage directory
    #[allow(dead_code)]
    pub fn list_files(&self) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        for entry in std::fs::read_dir(&self.storage_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                files.push(path);
            }
        }

        files.sort();
        Ok(files)
    }
}

impl Drop for StorageEngine {
    fn drop(&mut self) {
        // Flush any remaining logs on drop
        if let Err(e) = self.flush() {
            eprintln!("Error flushing logs on drop: {}", e);
        }
    }
}

/// Parse compression string to Parquet Compression enum
pub fn parse_compression(s: &str) -> Compression {
    match s.to_lowercase().as_str() {
        "snappy" => Compression::SNAPPY,
        "zstd" => Compression::ZSTD(Default::default()),
        "gzip" => Compression::GZIP(Default::default()),
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        _ => Compression::SNAPPY, // default
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn test_storage_engine_basic() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = StorageEngine::new(
            temp_dir.path().to_path_buf(),
            Compression::SNAPPY,
            10,
            1024 * 1024,
        )
        .unwrap();

        let log: LogEntry = serde_json::from_value(json!({
            "timestamp": "2026-01-15T19:00:00Z",
            "level": "info",
            "message": "Test log"
        }))
        .unwrap();

        engine.add_log(log).unwrap();
        engine.flush().unwrap();

        let files = engine.list_files().unwrap();
        assert_eq!(files.len(), 1);
    }
}
