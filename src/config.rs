use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct Config {
    /// Unix socket path for log ingestion
    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,

    /// Directory for storing Parquet files
    #[serde(default = "default_storage_dir")]
    pub storage_dir: PathBuf,

    /// Path to JSON Schema file for validation
    pub schema_path: Option<PathBuf>,

    /// Batch size for Parquet writes
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Compression codec (snappy, zstd, gzip, none)
    #[serde(default = "default_compression")]
    pub compression: String,

    /// Maximum concurrent connections
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// File rotation size in bytes (default: 100MB)
    #[serde(default = "default_rotation_size")]
    pub rotation_size: u64,

    /// Flush interval in seconds (default: 5s)
    #[serde(default = "default_flush_interval")]
    pub flush_interval_secs: u64,
}

impl Default for Config {
    #[allow(dead_code)]
    fn default() -> Self {
        Self {
            socket_path: default_socket_path(),
            storage_dir: default_storage_dir(),
            schema_path: None,
            batch_size: default_batch_size(),
            compression: default_compression(),
            max_connections: default_max_connections(),
            rotation_size: default_rotation_size(),
            flush_interval_secs: default_flush_interval(),
        }
    }
}

#[allow(dead_code)]
fn default_socket_path() -> PathBuf {
    PathBuf::from("/tmp/logdaemon.sock")
}

#[allow(dead_code)]
fn default_storage_dir() -> PathBuf {
    PathBuf::from("./logs")
}

#[allow(dead_code)]
fn default_batch_size() -> usize {
    1000
}

#[allow(dead_code)]
fn default_compression() -> String {
    "snappy".to_string()
}

#[allow(dead_code)]
fn default_max_connections() -> usize {
    1000
}

#[allow(dead_code)]
fn default_rotation_size() -> u64 {
    100 * 1024 * 1024 // 100MB
}

#[allow(dead_code)]
fn default_flush_interval() -> u64 {
    5
}

#[allow(dead_code)]
impl Config {
    /// Load configuration from a TOML file
    pub fn from_file(path: &PathBuf) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    /// Validate configuration
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.batch_size == 0 {
            anyhow::bail!("batch_size must be greater than 0");
        }

        if self.max_connections == 0 {
            anyhow::bail!("max_connections must be greater than 0");
        }

        if !["snappy", "zstd", "gzip", "none"].contains(&self.compression.as_str()) {
            anyhow::bail!(
                "Invalid compression codec: {}. Must be one of: snappy, zstd, gzip, none",
                self.compression
            );
        }

        Ok(())
    }
}
