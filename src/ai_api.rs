use anyhow::Result;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::trace_storage::{SpanStatus, TraceSpan};

/// AI Agent API server state
#[derive(Clone)]
pub struct ApiState {
    pub trace_storage_dir: std::path::PathBuf,
}

/// Query parameters for trace listing
#[derive(Debug, Deserialize)]
pub struct TraceQueryParams {
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub end_time: Option<String>,
    #[serde(default)]
    pub min_duration_ms: Option<u64>,
    #[serde(default)]
    pub has_error: Option<bool>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    100
}

/// Response for trace listing
#[derive(Debug, Serialize)]
pub struct TraceListResponse {
    pub traces: Vec<TraceSummary>,
    pub total_count: usize,
}

/// Summary of a trace for listing
#[derive(Debug, Serialize)]
pub struct TraceSummary {
    pub trace_id: String,
    pub root_span_name: String,
    pub start_time: String,
    pub total_duration_ms: f64,
    pub span_count: usize,
    pub error_count: usize,
}

/// Complete trace tree response
#[derive(Debug, Serialize)]
pub struct TraceDetailResponse {
    pub trace_id: String,
    pub root_span: SpanNode,
    pub summary: TraceAnalysis,
}

/// Hierarchical span node for trace tree
#[derive(Debug, Serialize)]
pub struct SpanNode {
    pub span_id: String,
    pub name: String,
    pub start_time: String,
    pub duration_ms: f64,
    pub attributes: HashMap<String, String>,
    pub events: Vec<SpanEventInfo>,
    pub status: String,
    pub children: Vec<SpanNode>,
}

#[derive(Debug, Serialize)]
pub struct SpanEventInfo {
    pub name: String,
    pub timestamp: String,
    pub attributes: HashMap<String, String>,
}

/// AI-friendly trace analysis
#[derive(Debug, Serialize)]
pub struct TraceAnalysis {
    pub total_spans: usize,
    pub total_duration_ms: f64,
    pub error_count: usize,
    pub critical_path_ms: f64,
    pub span_breakdown: HashMap<String, usize>,
    pub slowest_operations: Vec<SlowOperation>,
}

#[derive(Debug, Serialize)]
pub struct SlowOperation {
    pub name: String,
    pub duration_ms: f64,
    pub span_id: String,
}

/// Start the AI Agent API server
pub async fn start_api_server(port: u16, trace_storage_dir: std::path::PathBuf) -> Result<()> {
    let state = ApiState { trace_storage_dir };

    let app = Router::new()
        .route("/api/traces", get(list_traces))
        .route("/api/traces/:trace_id", get(get_trace_detail))
        .route("/api/traces/search", get(search_traces))
        .route("/api/health", get(health_check))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    info!("AI Agent API listening on http://{}", addr);

    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Health check endpoint
async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "service": "daemon_rs_ai_api"
    }))
}

/// List traces with filtering
async fn list_traces(
    State(state): State<ApiState>,
    Query(params): Query<TraceQueryParams>,
) -> Result<Json<TraceListResponse>, (StatusCode, String)> {
    let spans = load_all_spans(&state.trace_storage_dir)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Group spans by trace_id
    let mut traces_map: HashMap<String, Vec<TraceSpan>> = HashMap::new();
    for span in spans {
        traces_map
            .entry(span.trace_id.clone())
            .or_default()
            .push(span);
    }

    // Build trace summaries
    let mut summaries: Vec<TraceSummary> = traces_map
        .into_iter()
        .map(|(trace_id, spans)| build_trace_summary(trace_id, spans))
        .collect();

    // Apply filters
    if let Some(min_duration) = params.min_duration_ms {
        summaries.retain(|s| s.total_duration_ms >= min_duration as f64);
    }

    if let Some(has_error) = params.has_error {
        if has_error {
            summaries.retain(|s| s.error_count > 0);
        }
    }

    // Sort by start time (most recent first)
    summaries.sort_by(|a, b| b.start_time.cmp(&a.start_time));

    let total_count = summaries.len();
    summaries.truncate(params.limit);

    Ok(Json(TraceListResponse {
        traces: summaries,
        total_count,
    }))
}

/// Get detailed trace tree
async fn get_trace_detail(
    State(state): State<ApiState>,
    Path(trace_id): Path<String>,
) -> Result<Json<TraceDetailResponse>, (StatusCode, String)> {
    let spans = load_all_spans(&state.trace_storage_dir)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let trace_spans: Vec<TraceSpan> = spans
        .into_iter()
        .filter(|s| s.trace_id == trace_id)
        .collect();

    if trace_spans.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            format!("Trace {} not found", trace_id),
        ));
    }

    let root_span = build_trace_tree(&trace_spans);
    let summary = analyze_trace(&trace_spans);

    Ok(Json(TraceDetailResponse {
        trace_id,
        root_span,
        summary,
    }))
}

/// Search traces (alias for list_traces with different endpoint)
async fn search_traces(
    state: State<ApiState>,
    params: Query<TraceQueryParams>,
) -> Result<Json<TraceListResponse>, (StatusCode, String)> {
    list_traces(state, params).await
}

/// Load all spans from Parquet files
fn load_all_spans(storage_dir: &std::path::Path) -> Result<Vec<TraceSpan>> {
    let mut all_spans = Vec::new();

    // Return empty vec if directory doesn't exist yet
    if !storage_dir.exists() {
        return Ok(all_spans);
    }

    for entry in std::fs::read_dir(storage_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            let file = std::fs::File::open(&path)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

            for batch_result in reader {
                let batch = batch_result?;
                let spans = parse_spans_from_batch(&batch)?;
                all_spans.extend(spans);
            }
        }
    }

    Ok(all_spans)
}

/// Parse spans from Arrow RecordBatch
fn parse_spans_from_batch(batch: &arrow::array::RecordBatch) -> Result<Vec<TraceSpan>> {
    use arrow::array::{Array, StringArray, TimestampMicrosecondArray, UInt64Array};

    let trace_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let span_ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let parent_span_ids = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let names = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let start_times = batch
        .column(4)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let end_times = batch
        .column(5)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let durations = batch
        .column(6)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let attributes = batch
        .column(7)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let events = batch
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let statuses = batch
        .column(9)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let mut spans = Vec::new();

    for i in 0..batch.num_rows() {
        let trace_id = trace_ids.value(i).to_string();
        let span_id = span_ids.value(i).to_string();
        let parent_span_id = if parent_span_ids.is_null(i) {
            None
        } else {
            Some(parent_span_ids.value(i).to_string())
        };
        let name = names.value(i).to_string();
        let start_time =
            chrono::DateTime::from_timestamp_micros(start_times.value(i)).unwrap_or_default();
        let end_time =
            chrono::DateTime::from_timestamp_micros(end_times.value(i)).unwrap_or_default();
        let duration_us = durations.value(i);

        let attrs: HashMap<String, String> = serde_json::from_str(attributes.value(i))?;
        let evts: Vec<crate::trace_storage::SpanEvent> = serde_json::from_str(events.value(i))?;

        let status_str = statuses.value(i);
        let status = if status_str.starts_with("ERROR") {
            SpanStatus::Error {
                message: status_str.strip_prefix("ERROR: ").unwrap_or("").to_string(),
            }
        } else {
            SpanStatus::Ok
        };

        spans.push(TraceSpan {
            trace_id,
            span_id,
            parent_span_id,
            name,
            start_time,
            end_time,
            duration_us,
            attributes: attrs,
            events: evts,
            status,
        });
    }

    Ok(spans)
}

/// Build trace summary from spans
fn build_trace_summary(trace_id: String, spans: Vec<TraceSpan>) -> TraceSummary {
    let span_count = spans.len();
    let error_count = spans
        .iter()
        .filter(|s| matches!(s.status, SpanStatus::Error { .. }))
        .count();

    let root_span = spans
        .iter()
        .find(|s| s.parent_span_id.is_none())
        .or_else(|| spans.first())
        .unwrap();

    let total_duration_ms = root_span.duration_us as f64 / 1000.0;

    TraceSummary {
        trace_id,
        root_span_name: root_span.name.clone(),
        start_time: root_span.start_time.to_rfc3339(),
        total_duration_ms,
        span_count,
        error_count,
    }
}

/// Build hierarchical trace tree
fn build_trace_tree(spans: &[TraceSpan]) -> SpanNode {
    let mut span_map: HashMap<String, &TraceSpan> = HashMap::new();
    for span in spans {
        span_map.insert(span.span_id.clone(), span);
    }

    let root = spans
        .iter()
        .find(|s| s.parent_span_id.is_none())
        .or_else(|| spans.first())
        .unwrap();

    build_span_node(root, spans)
}

fn build_span_node(span: &TraceSpan, all_spans: &[TraceSpan]) -> SpanNode {
    let children: Vec<SpanNode> = all_spans
        .iter()
        .filter(|s| s.parent_span_id.as_ref() == Some(&span.span_id))
        .map(|child| build_span_node(child, all_spans))
        .collect();

    let events = span
        .events
        .iter()
        .map(|e| SpanEventInfo {
            name: e.name.clone(),
            timestamp: e.timestamp.to_rfc3339(),
            attributes: e.attributes.clone(),
        })
        .collect();

    let status = match &span.status {
        SpanStatus::Ok => "OK".to_string(),
        SpanStatus::Error { message } => format!("ERROR: {}", message),
    };

    SpanNode {
        span_id: span.span_id.clone(),
        name: span.name.clone(),
        start_time: span.start_time.to_rfc3339(),
        duration_ms: span.duration_us as f64 / 1000.0,
        attributes: span.attributes.clone(),
        events,
        status,
        children,
    }
}

/// Analyze trace for AI consumption
fn analyze_trace(spans: &[TraceSpan]) -> TraceAnalysis {
    let total_spans = spans.len();
    let error_count = spans
        .iter()
        .filter(|s| matches!(s.status, SpanStatus::Error { .. }))
        .count();

    let root = spans
        .iter()
        .find(|s| s.parent_span_id.is_none())
        .or_else(|| spans.first())
        .unwrap();
    let total_duration_ms = root.duration_us as f64 / 1000.0;

    let mut span_breakdown: HashMap<String, usize> = HashMap::new();
    for span in spans {
        *span_breakdown.entry(span.name.clone()).or_insert(0) += 1;
    }

    let mut slowest: Vec<SlowOperation> = spans
        .iter()
        .map(|s| SlowOperation {
            name: s.name.clone(),
            duration_ms: s.duration_us as f64 / 1000.0,
            span_id: s.span_id.clone(),
        })
        .collect();
    slowest.sort_by(|a, b| b.duration_ms.partial_cmp(&a.duration_ms).unwrap());
    slowest.truncate(10);

    TraceAnalysis {
        total_spans,
        total_duration_ms,
        error_count,
        critical_path_ms: total_duration_ms,
        span_breakdown,
        slowest_operations: slowest,
    }
}
