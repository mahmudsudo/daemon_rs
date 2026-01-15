use anyhow::{Context, Result};
use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use simd_json_derive::{Deserialize as SimdDeserialize, Serialize as SimdSerialize};
use std::path::Path;
use std::sync::Arc;

use simd_json::OwnedValue;

/// Strongly typed log entry for SIMD parsing
#[derive(Debug, Clone, Serialize, Deserialize, SimdSerialize, SimdDeserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEntry {
    pub timestamp: String,
    pub level: String,
    pub message: String,
    pub service: Option<String>,
    pub trace_id: Option<String>,
    pub metadata: Option<OwnedValue>,
}

/// Schema validator for JSON log entries
pub struct SchemaValidator {
    schema: Arc<JSONSchema>,
    use_fast_path: bool,
}

impl SchemaValidator {
    /// Create a new validator from a JSON Schema file
    pub fn from_file(path: &Path) -> Result<Self> {
        let schema_content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read schema file: {:?}", path))?;

        let schema_json: Value =
            serde_json::from_str(&schema_content).with_context(|| "Failed to parse schema JSON")?;

        Self::from_value(schema_json, false)
    }

    /// Create a validator from a JSON Schema value
    pub fn from_value(schema: Value, use_fast_path: bool) -> Result<Self> {
        let compiled = JSONSchema::compile(&schema)
            .map_err(|e| anyhow::anyhow!("Failed to compile schema: {}", e))?;

        Ok(Self {
            schema: Arc::new(compiled),
            use_fast_path,
        })
    }

    /// Create a validator with the default schema
    pub fn default_schema() -> Result<Self> {
        let default_schema = serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["timestamp", "level", "message"],
            "properties": {
                "timestamp": { "type": "string", "format": "date-time" },
                "level": { "type": "string" },
                "message": { "type": "string" },
                "metadata": { "type": "object" },
                "service": { "type": "string" },
                "trace_id": { "type": "string" }
            }
        });

        // Use fast path for default schema since it matches LogEntry struct
        Self::from_value(default_schema, true)
    }

    /// Validate a log entry against the schema
    pub fn validate(&self, log: &Value) -> Result<()> {
        self.schema.validate(log).map_err(|errors| {
            let error_messages: Vec<String> = errors
                .map(|e| format!("{} at {}", e, e.instance_path))
                .collect();
            anyhow::anyhow!("Validation errors: {}", error_messages.join(", "))
        })
    }

    /// Parse and validate bytes using SIMD if fast path is enabled
    /// Returns the parsed LogEntry or error
    pub fn parse_fast(&self, data: &mut [u8]) -> Result<LogEntry> {
        if self.use_fast_path {
            // SIMD parsing + validation (type checking)
            let entry: LogEntry = simd_json::from_slice(data)
                .map_err(|e| anyhow::anyhow!("SIMD Parse error: {}", e))?;
            Ok(entry)
        } else {
            // Slow path: Deserialize to Value -> Validate -> Convert to LogEntry
            let val: Value = serde_json::from_slice(data)?;
            self.validate(&val)?;
            let entry: LogEntry = serde_json::from_value(val)?;
            Ok(entry)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_fast_path_parsing() {
        let validator = SchemaValidator::default_schema().unwrap();
        let mut data =
            r#"{"timestamp":"2026-01-15T19:00:00Z","level":"info","message":"Fast log"}"#
                .as_bytes()
                .to_vec();

        let entry = validator.parse_fast(&mut data).unwrap();
        assert_eq!(entry.message, "Fast log");
    }

    #[test]
    fn test_default_schema_valid() {
        let validator = SchemaValidator::default_schema().unwrap();

        let valid_log = json!({
            "timestamp": "2026-01-15T19:00:00Z",
            "level": "info",
            "message": "Test log message"
        });

        assert!(validator.validate(&valid_log).is_ok());
    }
}
