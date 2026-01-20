use anyhow::{Context, Result};
use opentelemetry::trace::{Tracer, TracerProvider as _};
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::{Sampler, TracerProvider};
use opentelemetry_sdk::Resource;
use tracing::Subscriber;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

/// Initialize OpenTelemetry tracing and return a subscriber
/// This combines init and subscriber creation to work around type limitations
pub fn init_tracing_and_subscriber(
    service_name: &str,
    otlp_endpoint: Option<String>,
    sampling_rate: f64,
) -> Result<impl Subscriber> {
    // Create resource with service name
    let resource = Resource::new(vec![KeyValue::new(
        "service.name",
        service_name.to_string(),
    )]);

    // Configure sampler based on sampling rate
    let sampler = if sampling_rate >= 1.0 {
        Sampler::AlwaysOn
    } else if sampling_rate <= 0.0 {
        Sampler::AlwaysOff
    } else {
        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(sampling_rate)))
    };

    // Build tracer provider
    let mut provider_builder = TracerProvider::builder().with_config(
        opentelemetry_sdk::trace::Config::default()
            .with_resource(resource)
            .with_sampler(sampler),
    );

    // Add OTLP exporter if endpoint is provided
    if let Some(endpoint) = otlp_endpoint {
        let exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint(endpoint)
            .build_span_exporter()
            .context("Failed to create OTLP exporter")?;

        let batch_processor = opentelemetry_sdk::trace::BatchSpanProcessor::builder(
            exporter,
            opentelemetry_sdk::runtime::Tokio,
        )
        .build();

        provider_builder = provider_builder.with_span_processor(batch_processor);
    }

    let provider = provider_builder.build();

    // Get SDK tracer before setting global provider
    let tracer = provider.tracer("daemon_rs");

    // Set global provider
    global::set_tracer_provider(provider);

    // Create telemetry layer with concrete SDK tracer
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    // Create and return subscriber
    let subscriber = Registry::default()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry);

    Ok(subscriber)
}

/// Shutdown OpenTelemetry gracefully
pub fn shutdown_tracing() {
    global::shutdown_tracer_provider();
}

/// Helper to create a span with common attributes
#[macro_export]
macro_rules! trace_span {
    ($name:expr, $($key:expr => $value:expr),*) => {
        tracing::info_span!(
            $name,
            $($key = $value,)*
            otel.kind = "internal",
            otel.status_code = tracing::field::Empty,
        )
    };
}

/// Helper to record span errors
pub fn record_error(span: &tracing::Span, error: &anyhow::Error) {
    span.record("otel.status_code", "ERROR");
    span.record("error.message", error.to_string().as_str());
}
