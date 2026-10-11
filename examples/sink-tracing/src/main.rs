//! Tracing-aware sink UDF example.
//!
//! The Numaflow data plane injects the platform's per-stage span context into
//! `sys_metadata["tracing_udf"]` (W3C traceparent + optional tracestate)
//! before invoking the sink UDF. This example shows how to:
//!
//!   1. Initialise an OTLP gRPC tracer in the sink process so it can export
//!      spans.
//!   2. Extract the platform parent context per incoming message.
//!   3. Create a child span (`user.persist`) under the platform's
//!      `numaflow.{topology}.sink.write` span — typical place to span an
//!      external DB write, HTTP POST, or other persistence call.
//!
//! Required environment variables:
//!
//!   OTEL_EXPORTER_OTLP_TRACES_ENDPOINT  (or the generic OTEL_EXPORTER_OTLP_ENDPOINT)
//!   OTEL_SERVICE_NAME                   (optional; defaults to "numaflow-udf")
//!
//! When neither endpoint variable is set the tracer init is a no-op and the
//! example continues to function as a plain log sink.

use std::collections::HashMap;

use numaflow::sink::{self, Response, SinkRequest, SystemMetadata};
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{Context, global};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
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
        tracing_subscriber::fmt::init();
        eprintln!("[tracing] OTLP endpoint not set; sink spans will be no-ops");
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
        // orphaned sink spans when the platform sampled the trace out.
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::AlwaysOn)))
        .with_resource(
            Resource::builder()
                .with_service_name(service_name.clone())
                .build(),
        )
        .build();

    global::set_tracer_provider(provider.clone());
    global::set_text_map_propagator(TraceContextPropagator::new());

    let tracer = provider.tracer("numaflow-rs-example/sink-tracing");
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
/// the platform-side `numaflow.{topology}.sink.write` span. Spans started
/// with this context become children of the platform sink span in the trace
/// tree.
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

/// Log sink that emits a `user.persist` span per message under the platform's
/// per-message sink.write span. Replace the body of the loop with your real
/// persistence work (DB write, HTTP POST, etc.) and add nested spans there
/// for finer attribution.
struct TracedSink;

#[tonic::async_trait]
impl sink::Sinker for TracedSink {
    async fn sink(&self, mut input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
        let mut responses: Vec<Response> = Vec::new();

        while let Some(req) = input.recv().await {
            let parent_cx = extract_trace_context(&req.system_metadata);
            let span = tracing::info_span!("user.persist");
            // `set_parent` returns Err only when no OTel layer is registered
            // on the subscriber. That's the disabled-tracing path; safe to
            // ignore.
            let _ = span.set_parent(parent_cx);

            // Hold the span for the duration of the persistence work. Spans
            // created synchronously while `_guard` is alive nest under
            // `user.persist`.
            let _guard = span.enter();
            let response = match std::str::from_utf8(&req.value) {
                Ok(v) => {
                    println!("Traced sink: {v}");
                    Response::ok(req.id)
                }
                Err(e) => Response::failure(req.id, format!("Invalid UTF-8 sequence: {e}")),
            };
            drop(_guard);

            responses.push(response);
        }

        responses
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = init_tracer();

    let result = sink::Server::new(TracedSink).start().await;

    if let Some(p) = provider {
        if let Err(e) = p.shutdown() {
            eprintln!("[tracing] tracer shutdown error: {e}");
        }
    }

    result
}
