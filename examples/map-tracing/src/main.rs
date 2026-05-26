//! Tracing-aware map UDF example.
//!
//! The Numaflow data plane injects the platform's per-stage span context into
//! `sys_metadata["tracing_udf"]` (W3C traceparent + optional tracestate)
//! before invoking the UDF. This example shows how to:
//!
//!   1. Initialise an OTLP gRPC tracer in the UDF process so it can export spans.
//!   2. Extract the platform parent context from the message's system metadata.
//!   3. Create a child span (`user.work`) under the platform's
//!      `numaflow.{topology}.map` span so user-defined work shows up nested in
//!      the same trace.
//!
//! Required environment variables:
//!
//!   OTEL_EXPORTER_OTLP_TRACES_ENDPOINT  (or the generic OTEL_EXPORTER_OTLP_ENDPOINT)
//!   OTEL_SERVICE_NAME                   (optional; defaults to "numaflow-udf")
//!
//! When neither endpoint variable is set the tracer init is a no-op and the
//! example continues to function as a plain pass-through map.

use std::collections::HashMap;

use numaflow::map::{self, MapRequest, Message, SystemMetadata};
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{Context, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// sys_metadata group the Numaflow data plane uses to inject the platform
/// parent context for the current stage.
const TRACING_UDF_GROUP: &str = "tracing_udf";

/// Initialise the OTLP gRPC tracer provider and the W3C propagator, then wire
/// `tracing-opentelemetry` so `tracing::info_span!` macros export through OTel.
///
/// Returns the `SdkTracerProvider` so `main` can call `shutdown()` before exit
/// to flush in-flight spans. Returns `None` (and falls back to a plain fmt
/// subscriber) when no OTLP endpoint is configured.
fn init_tracer() -> Option<SdkTracerProvider> {
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .ok()
        .filter(|e| !e.trim().is_empty());

    let Some(endpoint) = endpoint else {
        // No OTLP endpoint -> logs only, no tracer.
        tracing_subscriber::fmt::init();
        eprintln!("[tracing] OTLP endpoint not set; UDF spans will be no-ops");
        return None;
    };

    let service_name =
        std::env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "numaflow-udf".to_string());

    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        // The Rust OTel gRPC exporter expects a full URI (with scheme); unlike
        // the Go client which wants host:port. Pass the value through as-is.
        .with_endpoint(&endpoint)
        .build()
    {
        Ok(e) => e,
        Err(err) => {
            tracing_subscriber::fmt::init();
            eprintln!("[tracing] Failed to build OTLP exporter: {err}");
            return None;
        }
    };

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        // Honour the upstream platform's sampling decision so we never produce
        // orphaned UDF spans when the platform sampled the trace out.
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
        .with_resource(
            Resource::builder()
                .with_service_name(service_name.clone())
                .build(),
        )
        .build();

    global::set_tracer_provider(provider.clone());
    global::set_text_map_propagator(TraceContextPropagator::new());

    let tracer = provider.tracer("numaflow-rs-example/map-tracing");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(otel_layer)
        .with(tracing_subscriber::fmt::layer())
        .init();

    eprintln!("[tracing] OTLP exporter configured: endpoint={endpoint} service={service_name}");
    Some(provider)
}

/// Adapter so the W3C propagator can extract from a `HashMap<String, String>`.
struct MapExtractor<'a>(&'a HashMap<String, String>);

impl Extractor for MapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

/// Pull the W3C traceparent/tracestate the platform wrote into
/// `sys_metadata["tracing_udf"]` and return a context whose current span is
/// the platform-side `numaflow.{topology}.map` span. Spans started with this
/// context become children of the platform span in the trace tree.
///
/// Safe to call when tracing is disabled: empty bytes -> empty Context.
fn extract_trace_context(sys: &SystemMetadata) -> Context {
    let tp = sys.value(TRACING_UDF_GROUP, "traceparent");
    if tp.is_empty() {
        return Context::new();
    }
    let mut carrier: HashMap<String, String> = HashMap::new();
    if let Ok(s) = std::str::from_utf8(&tp) {
        carrier.insert("traceparent".to_string(), s.to_string());
    }
    let ts = sys.value(TRACING_UDF_GROUP, "tracestate");
    if let Ok(s) = std::str::from_utf8(&ts) {
        if !s.is_empty() {
            carrier.insert("tracestate".to_string(), s.to_string());
        }
    }
    TraceContextPropagator::new().extract(&MapExtractor(&carrier))
}

/// Pass-through map UDF that emits a `user.work` span under the platform's
/// per-message `numaflow.{topology}.map` span. Replace the body of `map` with
/// your real work; any further `tracing::info_span!` / `.instrument` calls
/// nest under `user.work`.
struct TracedMapper;

#[tonic::async_trait]
impl map::Mapper for TracedMapper {
    async fn map(&self, input: MapRequest) -> Vec<Message> {
        let parent_cx = extract_trace_context(&input.system_metadata);
        let span = tracing::info_span!("user.work");
        // `set_parent` returns Err only when no OTel layer is registered on
        // the subscriber. That's the disabled-tracing path; safe to ignore.
        let _ = span.set_parent(parent_cx);

        async move {
            // Real UDF work would go here. We just pass the value through so
            // the example stays focused on the tracing wiring.
            vec![Message::new(input.value).with_keys(input.keys)]
        }
        .instrument(span)
        .await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = init_tracer();

    let result = map::Server::new(TracedMapper).start().await;

    if let Some(p) = provider {
        // Flush in-flight spans on shutdown so the platform's last spans land
        // in the collector before the process exits.
        if let Err(e) = p.shutdown() {
            eprintln!("[tracing] tracer shutdown error: {e}");
        }
    }

    result
}
