use anyhow::{Context, Result};
use arrow::array::{ArrayRef, RecordBatch, StringBuilder, TimestampMicrosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info};

/// Represents a single span in a distributed trace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_us: u64,
    pub attributes: HashMap<String, String>,
    pub events: Vec<SpanEvent>,
    pub status: SpanStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEvent {
    pub name: String,
    pub timestamp: DateTime<Utc>,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpanStatus {
    Ok,
    Error { message: String },
}

/// Storage engine for trace data using Parquet
pub struct TraceStorage {
    storage_dir: PathBuf,
    compression: Compression,
    batch_size: usize,
    current_batch: Vec<TraceSpan>,
    file_counter: usize,
}

impl TraceStorage {
    pub fn new(storage_dir: PathBuf, compression: Compression, batch_size: usize) -> Result<Self> {
        std::fs::create_dir_all(&storage_dir)
            .with_context(|| format!("Failed to create storage directory: {:?}", storage_dir))?;

        Ok(Self {
            storage_dir,
            compression,
            batch_size,
            current_batch: Vec::with_capacity(batch_size),
            file_counter: 0,
        })
    }

    /// Add a span to the current batch
    pub fn add_span(&mut self, span: TraceSpan) -> Result<()> {
        self.current_batch.push(span);

        if self.current_batch.len() >= self.batch_size {
            self.flush()?;
        }

        Ok(())
    }

    /// Flush the current batch to disk
    pub fn flush(&mut self) -> Result<()> {
        if self.current_batch.is_empty() {
            return Ok(());
        }

        let batch = self.spans_to_record_batch(&self.current_batch)?;
        let file_path = self.generate_file_path();

        self.write_record_batch(&file_path, batch)?;

        info!(
            "Flushed {} spans to {:?}",
            self.current_batch.len(),
            file_path
        );

        self.current_batch.clear();
        Ok(())
    }

    /// Generate a new file path with timestamp
    fn generate_file_path(&mut self) -> PathBuf {
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
        let filename = format!("traces_{}_{:03}.parquet", timestamp, self.file_counter);
        self.file_counter += 1;
        self.storage_dir.join(filename)
    }

    /// Convert spans to Arrow RecordBatch
    fn spans_to_record_batch(&self, spans: &[TraceSpan]) -> Result<RecordBatch> {
        let mut trace_ids = StringBuilder::new();
        let mut span_ids = StringBuilder::new();
        let mut parent_span_ids = StringBuilder::new();
        let mut names = StringBuilder::new();
        let mut start_times = Vec::new();
        let mut end_times = Vec::new();
        let mut durations = Vec::new();
        let mut attributes_json = StringBuilder::new();
        let mut events_json = StringBuilder::new();
        let mut statuses = StringBuilder::new();

        for span in spans {
            trace_ids.append_value(&span.trace_id);
            span_ids.append_value(&span.span_id);
            parent_span_ids.append_option(span.parent_span_id.as_deref());
            names.append_value(&span.name);
            start_times.push(span.start_time.timestamp_micros());
            end_times.push(span.end_time.timestamp_micros());
            durations.push(span.duration_us);

            // Serialize attributes and events as JSON
            let attrs_json = serde_json::to_string(&span.attributes)?;
            attributes_json.append_value(&attrs_json);

            let events_str = serde_json::to_string(&span.events)?;
            events_json.append_value(&events_str);

            let status_str = match &span.status {
                SpanStatus::Ok => "OK".to_string(),
                SpanStatus::Error { message } => format!("ERROR: {}", message),
            };
            statuses.append_value(&status_str);
        }

        let schema = self.create_schema();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(trace_ids.finish()) as ArrayRef,
                Arc::new(span_ids.finish()),
                Arc::new(parent_span_ids.finish()),
                Arc::new(names.finish()),
                Arc::new(TimestampMicrosecondArray::from(start_times)),
                Arc::new(TimestampMicrosecondArray::from(end_times)),
                Arc::new(UInt64Array::from(durations)),
                Arc::new(attributes_json.finish()),
                Arc::new(events_json.finish()),
                Arc::new(statuses.finish()),
            ],
        )?;

        Ok(batch)
    }

    /// Create Arrow schema for trace spans
    fn create_schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("span_id", DataType::Utf8, false),
            Field::new("parent_span_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "start_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "end_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("duration_us", DataType::UInt64, false),
            Field::new("attributes", DataType::Utf8, false),
            Field::new("events", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, false),
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

        debug!("Wrote trace batch to {:?}", path);
        Ok(())
    }

    /// List all trace files in storage directory
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

impl Drop for TraceStorage {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
