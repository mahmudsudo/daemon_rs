//! Example AI agent that consumes traces from the daemon AI API
//!
//! This demonstrates how an AI agent can query and analyze
//! distributed traces from the logging daemon.

use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct TraceListResponse {
    traces: Vec<TraceSummary>,
    total_count: usize,
}

#[derive(Debug, Deserialize)]
struct TraceSummary {
    trace_id: String,
    root_span_name: String,
    start_time: String,
    total_duration_ms: f64,
    span_count: usize,
    error_count: usize,
}

#[derive(Debug, Deserialize)]
struct TraceDetailResponse {
    trace_id: String,
    root_span: SpanNode,
    summary: TraceAnalysis,
}

#[derive(Debug, Deserialize)]
struct SpanNode {
    span_id: String,
    name: String,
    start_time: String,
    duration_ms: f64,
    attributes: HashMap<String, String>,
    status: String,
    children: Vec<SpanNode>,
}

#[derive(Debug, Deserialize)]
struct TraceAnalysis {
    total_spans: usize,
    total_duration_ms: f64,
    error_count: usize,
    slowest_operations: Vec<SlowOperation>,
}

#[derive(Debug, Deserialize)]
struct SlowOperation {
    name: String,
    duration: f64,
    span_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let api_base = "http://localhost:9101";

    println!("🤖 AI Agent - Trace Analyzer\\n");

    // 1. List all traces
    println!("📋 Fetching traces...");
    let list_url = format!("{}/api/traces?limit=10", api_base);
    let response = reqwest::get(&list_url).await?;
    let trace_list: TraceListResponse = response.json().await?;

    println!("Found {} total traces\\n", trace_list.total_count);

    // 2. Analyze each trace
    for trace_summary in &trace_list.traces {
        println!("🔍 Analyzing trace: {}", trace_summary.trace_id);
        println!("   Operation: {}", trace_summary.root_span_name);
        println!("   Duration: {:.2}ms", trace_summary.total_duration_ms);
        println!("   Spans: {}", trace_summary.span_count);

        if trace_summary.error_count > 0 {
            println!("   ⚠️  Errors: {}", trace_summary.error_count);
        }

        // 3. Get detailed trace information
        let detail_url = format!("{}/api/traces/{}", api_base, trace_summary.trace_id);
        let detail_response = reqwest::get(&detail_url).await?;
        let detail: TraceDetailResponse = detail_response.json().await?;

        // 4. AI-based analysis
        analyze_trace(&detail);

        println!();
    }

    // 5. Search for slow traces
    println!("\\n🐌 Searching for slow operations (>100ms)...");
    let slow_url = format!("{}/api/traces?min_duration_ms=100", api_base);
    let slow_response = reqwest::get(&slow_url).await?;
    let slow_traces: TraceListResponse = slow_response.json().await?;

    println!("Found {} slow traces", slow_traces.traces.len());
    for trace in &slow_traces.traces {
        println!(
            "  - {} ({:.2}ms)",
            trace.root_span_name, trace.total_duration_ms
        );
    }

    // 6. Search for error traces
    println!("\\n❌ Searching for traces with errors...");
    let error_url = format!("{}/api/traces?has_error=true", api_base);
    let error_response = reqwest::get(&error_url).await?;
    let error_traces: TraceListResponse = error_response.json().await?;

    println!("Found {} traces with errors", error_traces.traces.len());
    for trace in &error_traces.traces {
        println!(
            "  - {} (errors: {})",
            trace.root_span_name, trace.error_count
        );
    }

    Ok(())
}

/// AI-based trace analysis
fn analyze_trace(detail: &TraceDetailResponse) {
    println!("\\n  📊 AI Analysis:");

    // Identify bottlenecks
    if !detail.summary.slowest_operations.is_empty() {
        println!("     Bottlenecks:");
        for (i, op) in detail.summary.slowest_operations.iter().take(3).enumerate() {
            println!("       {}. {} ({:.2}ms)", i + 1, op.name, op.duration);
        }
    }

    // Calculate span breakdown
    let avg_span_duration = detail.summary.total_duration_ms / detail.summary.total_spans as f64;
    println!("     Average span duration: {:.2}ms", avg_span_duration);

    // Detect anomalies
    if detail.summary.error_count > 0 {
        println!(
            "     ⚠️  Anomaly detected: {} errors in trace",
            detail.summary.error_count
        );
    }

    // Recursive depth analysis
    let max_depth = calculate_depth(&detail.root_span, 0);
    println!("     Trace depth: {} levels", max_depth);

    if max_depth > 5 {
        println!("     💡 Recommendation: Consider flattening deep call hierarchies");
    }
}

fn calculate_depth(span: &SpanNode, current_depth: usize) -> usize {
    if span.children.is_empty() {
        current_depth + 1
    } else {
        span.children
            .iter()
            .map(|child| calculate_depth(child, current_depth + 1))
            .max()
            .unwrap_or(current_depth + 1)
    }
}
