use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::{Path, PathBuf};
use tracing::info;

/// Query interface for reading logs from Parquet files
pub struct QueryEngine {
    storage_dir: PathBuf,
}

impl QueryEngine {
    pub fn new(storage_dir: PathBuf) -> Self {
        Self { storage_dir }
    }

    /// List all Parquet files in the storage directory
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

    /// Read all logs from Parquet files
    #[tracing::instrument(skip(self))]
    pub fn read_all(&self) -> Result<Vec<RecordBatch>> {
        let files = self.list_files()?;
        let mut batches = Vec::new();

        for file_path in files {
            info!("Reading file: {:?}", file_path);
            match self.read_file(&file_path) {
                Ok(file_batches) => batches.extend(file_batches),
                Err(e) => {
                    tracing::warn!("Skipping corrupted or invalid file {:?}: {}", file_path, e);
                }
            }
        }

        Ok(batches)
    }

    /// Read logs from a specific Parquet file
    pub fn read_file(&self, path: &Path) -> Result<Vec<RecordBatch>> {
        let file =
            File::open(path).with_context(|| format!("Failed to open Parquet file: {:?}", path))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        let mut batches = Vec::new();
        for batch_result in reader {
            batches.push(batch_result?);
        }

        Ok(batches)
    }

    /// Print logs in a human-readable format
    pub fn print_logs(&self, batches: &[RecordBatch]) -> Result<()> {
        for batch in batches {
            println!(
                "{}",
                arrow::util::pretty::pretty_format_batches(&[batch.clone()])?
            );
        }
        Ok(())
    }

    /// Get total number of log entries
    #[tracing::instrument(skip(self))]
    pub fn count_logs(&self) -> Result<usize> {
        let batches = self.read_all()?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{parse_compression, StorageEngine};
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn test_query_engine() {
        let temp_dir = TempDir::new().unwrap();
        let storage_dir = temp_dir.path().to_path_buf();

        // Write some test logs
        let mut engine = StorageEngine::new(
            storage_dir.clone(),
            parse_compression("snappy"),
            5,
            1024 * 1024,
        )
        .unwrap();

        for i in 0..10 {
            let log: crate::schema::LogEntry = serde_json::from_value(json!({
                "timestamp": "2026-01-15T19:00:00Z",
                "level": "info",
                "message": format!("Test log {}", i)
            }))
            .unwrap();
            engine.add_log(log).unwrap();
        }
        engine.flush().unwrap();

        // Query the logs
        let query_engine = QueryEngine::new(storage_dir);
        let count = query_engine.count_logs().unwrap();
        assert_eq!(count, 10);
    }
}
