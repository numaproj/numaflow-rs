//! An HTTP source example for Numaflow.
//!
//! This source starts an HTTP server on a configurable port (default 8443) and forwards
//! incoming POST requests as messages into the Numaflow pipeline.
//!
//! # Endpoints
//! - `GET /health` - Health check (returns 200 OK)
//! - `POST /vertices/{vertex_name}` - Data ingestion endpoint
//!
//! # Headers
//! - `X-Numaflow-Id` - Optional message ID (auto-generated UUIDv7 if absent)
//! - `X-Numaflow-Event-Time` - Optional event time as epoch milliseconds (defaults to now)
//! - `X-Numaflow-Keys` - Optional comma-separated keys
//!
//! # Configuration
//! - `HTTP_SOURCE_PORT` env var sets the listen port (default: 8443)

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

const NUMAFLOW_ID_HEADER: &str = "x-numaflow-id";
const NUMAFLOW_ID_HEADER_KEY: &str = "X-Numaflow-Id";
const NUMAFLOW_EVENT_TIME_HEADER: &str = "x-numaflow-event-time";
const NUMAFLOW_KEYS_HEADER: &str = "x-numaflow-keys";

const DEFAULT_PORT: u16 = 8443;
const DEFAULT_BUFFER_SIZE: usize = 500;
const DEFAULT_READ_TIMEOUT_MS: u64 = 5;
const VERTEX_NAME: &str = "in";

/// An HTTP message received from the server, pending ack/nack.
struct HttpMessage {
    body: Bytes,
    headers: HashMap<String, String>,
    event_time: DateTime<Utc>,
    id: String,
    keys: Vec<String>,
}

/// Map of inflight request IDs to their response channels.
type InflightRequests = Arc<Mutex<HashMap<String, oneshot::Sender<StatusCode>>>>;

/// Shared state for the axum handler.
#[derive(Clone)]
struct HttpState {
    tx: mpsc::Sender<HttpMessage>,
    inflight_requests: InflightRequests,
}

/// The HTTP source that implements the Numaflow [`Sourcer`] trait.
///
/// It spins up an HTTP server and bridges incoming requests into the
/// source read/ack/nack lifecycle.
pub struct HttpSource {
    rx: Mutex<mpsc::Receiver<HttpMessage>>,
    inflight_requests: InflightRequests,
    read_timeout: Duration,
    _cancel_token: CancellationToken,
}

impl HttpSource {
    /// Create a new `HttpSource` and start the HTTP server in the background.
    pub async fn new(port: u16, cancel_token: CancellationToken) -> Self {
        let (tx, rx) = mpsc::channel(DEFAULT_BUFFER_SIZE);
        let inflight_requests: InflightRequests = Arc::new(Mutex::new(HashMap::new()));

        let state = HttpState {
            tx,
            inflight_requests: Arc::clone(&inflight_requests),
        };

        let router = Router::new()
            .route("/health", get(health_handler))
            .route(&format!("/vertices/{VERTEX_NAME}"), post(data_handler))
            .with_state(state);

        let addr: SocketAddr = format!("0.0.0.0:{port}").parse().expect("valid address");
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("failed to bind HTTP listener");
        info!(%addr, "HTTP source server listening");

        let token = cancel_token.clone();
        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move { token.cancelled().await })
                .await
                .expect("HTTP server error");
        });

        Self {
            rx: Mutex::new(rx),
            inflight_requests,
            read_timeout: Duration::from_millis(DEFAULT_READ_TIMEOUT_MS),
            _cancel_token: cancel_token,
        }
    }
}

#[tonic::async_trait]
impl numaflow::source::Sourcer for HttpSource {
    async fn read(
        &self,
        request: numaflow::source::SourceReadRequest,
        transmitter: tokio::sync::mpsc::Sender<numaflow::source::Message>,
    ) {
        let mut rx = self.rx.lock().await;
        let mut count = 0usize;

        let timeout = tokio::time::timeout(self.read_timeout, std::future::pending::<()>());
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                biased;
                _ = &mut timeout => break,
                msg = rx.recv() => {
                    match msg {
                        Some(http_msg) => {
                            let source_msg = numaflow::source::Message {
                                value: http_msg.body.to_vec(),
                                event_time: http_msg.event_time,
                                offset: numaflow::source::Offset {
                                    offset: http_msg.id.clone().into_bytes(),
                                    partition_id: 0,
                                },
                                keys: http_msg.keys,
                                headers: http_msg.headers,
                                user_metadata: None,
                            };
                            if transmitter.send(source_msg).await.is_err() {
                                break;
                            }
                            count += 1;
                            if count >= request.count {
                                break;
                            }
                        }
                        None => break, // channel closed
                    }
                }
            }
        }
    }

    async fn ack(&self, offsets: Vec<numaflow::source::Offset>) {
        let mut inflight = self.inflight_requests.lock().await;
        for offset in offsets {
            let id = String::from_utf8(offset.offset).unwrap();
            if let Some(response_tx) = inflight.remove(&id) {
                let _ = response_tx.send(StatusCode::OK);
            }
        }
    }

    async fn nack(&self, offsets: Vec<numaflow::source::Offset>) {
        let mut inflight = self.inflight_requests.lock().await;
        for offset in offsets {
            let id = String::from_utf8(offset.offset).unwrap();
            if let Some(response_tx) = inflight.remove(&id) {
                let _ = response_tx.send(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    async fn pending(&self) -> Option<usize> {
        // HTTP sources don't support pending count — returning None
        // avoids misleading the autoscaler.
        None
    }

    async fn partitions(&self) -> Option<Vec<i32>> {
        Some(vec![0])
    }
}

// ── Axum handlers ──────────────────────────────────────────────────────

async fn health_handler() -> impl IntoResponse {
    StatusCode::OK
}

async fn data_handler(
    State(state): State<HttpState>,
    mut headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Extract or generate message ID.
    let id = match headers.get(NUMAFLOW_ID_HEADER) {
        Some(val) => match val.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("'{NUMAFLOW_ID_HEADER}' header contains non-ASCII characters"),
                )
                    .into_response();
            }
        },
        None => Uuid::now_v7().to_string(),
    };

    // Extract or default event time.
    let event_time = match headers.get(NUMAFLOW_EVENT_TIME_HEADER) {
        Some(val) => match parse_event_time(val) {
            Ok(t) => t,
            Err(resp) => return resp,
        },
        None => Utc::now(),
    };

    // Extract keys (comma-separated).
    let keys: Vec<String> = match headers.get(NUMAFLOW_KEYS_HEADER) {
        Some(val) => match val.to_str() {
            Ok(s) => s.split(',').map(|k| k.trim().to_string()).collect(),
            Err(_) => vec![],
        },
        None => vec![],
    };

    // Remove headers we don't want to propagate.
    headers.remove(NUMAFLOW_EVENT_TIME_HEADER);
    headers.remove(axum::http::header::AUTHORIZATION);

    // Build the header map to forward.
    let mut header_map = HashMap::new();
    header_map.insert(NUMAFLOW_ID_HEADER_KEY.to_string(), id.clone());
    for (key, value) in headers.iter() {
        if let Ok(v) = value.to_str() {
            header_map.insert(key.to_string(), v.to_string());
        }
    }

    // Create oneshot for the ack/nack response.
    let (response_tx, response_rx) = oneshot::channel();

    // Guard against duplicate in-flight IDs.
    {
        let mut inflight = state.inflight_requests.lock().await;
        if inflight.contains_key(&id) {
            return (
                StatusCode::CONFLICT,
                axum::Json(serde_json::json!({ "error": "Duplicate request ID", "id": id })),
            )
                .into_response();
        }
        inflight.insert(id.clone(), response_tx);
    }

    // Try to enqueue the message.
    let message = HttpMessage {
        body,
        headers: header_map,
        event_time,
        id: id.clone(),
        keys,
    };

    match state.tx.try_send(message) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            state.inflight_requests.lock().await.remove(&id);
            return (StatusCode::TOO_MANY_REQUESTS, "Buffer full").into_response();
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            state.inflight_requests.lock().await.remove(&id);
            return (StatusCode::INTERNAL_SERVER_ERROR, "Channel closed").into_response();
        }
    }

    // Block until the pipeline acks or nacks.
    match response_rx.await {
        Ok(StatusCode::OK) => (
            StatusCode::OK,
            axum::Json(serde_json::json!({ "message": "Data received successfully", "id": id })),
        )
            .into_response(),
        Ok(status) => (
            status,
            axum::Json(serde_json::json!({ "error": "Request processing failed", "id": id })),
        )
            .into_response(),
        Err(_) => {
            warn!(%id, "Response channel dropped — likely shutting down");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(
                    serde_json::json!({ "error": "Request processing interrupted", "id": id }),
                ),
            )
                .into_response()
        }
    }
}

#[allow(clippy::result_large_err)]
fn parse_event_time(val: &HeaderValue) -> Result<DateTime<Utc>, axum::response::Response> {
    let s = val.to_str().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            format!("'{NUMAFLOW_EVENT_TIME_HEADER}' is not a valid ASCII string"),
        )
            .into_response()
    })?;

    let epoch_millis: i64 = s.parse().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            format!("'{NUMAFLOW_EVENT_TIME_HEADER}' is not a valid integer: {s}"),
        )
            .into_response()
    })?;

    Utc.timestamp_millis_opt(epoch_millis)
        .single()
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                format!("'{NUMAFLOW_EVENT_TIME_HEADER}' epoch millis out of range: {s}"),
            )
                .into_response()
        })
}

// ── main ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let port: u16 = env::var("HTTP_SOURCE_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let cancel_token = CancellationToken::new();
    let source = HttpSource::new(port, cancel_token).await;

    numaflow::source::Server::new(source).start().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use numaflow::source::{Offset, SourceReadRequest, Sourcer};
    use tokio::sync::mpsc;

    /// Helper: spin up the HTTP source on an ephemeral port.
    async fn setup() -> (HttpSource, u16) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener); // free the port for HttpSource to bind

        let cancel = CancellationToken::new();
        let source = HttpSource::new(port, cancel).await;
        (source, port)
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let (_source, port) = setup().await;
        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{port}/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn test_post_and_ack() {
        let (source, port) = setup().await;

        // Post a message in the background — it will block until acked.
        let client = reqwest::Client::new();
        let url = format!("http://127.0.0.1:{port}/vertices/in");
        let post_handle = tokio::spawn({
            let client = client.clone();
            let url = url.clone();
            async move {
                client
                    .post(&url)
                    .header("X-Numaflow-Id", "test-id-1")
                    .body("hello")
                    .send()
                    .await
                    .unwrap()
            }
        });

        // Give the server a moment to accept the request.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Read the message via the Sourcer trait.
        let (tx, mut rx) = mpsc::channel(10);
        let request = SourceReadRequest {
            count: 10,
            timeout: Duration::from_secs(1),
        };
        source.read(request, tx).await;

        let mut messages = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            messages.push(msg);
        }
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, b"hello");
        assert_eq!(
            String::from_utf8(messages[0].offset.offset.clone()).unwrap(),
            "test-id-1"
        );

        // Ack the message — this should unblock the HTTP response.
        source.ack(vec![messages.remove(0).offset]).await;

        let resp = post_handle.await.unwrap();
        assert_eq!(resp.status(), 200);
    }

    #[tokio::test]
    async fn test_post_and_nack() {
        let (source, port) = setup().await;

        let url = format!("http://127.0.0.1:{port}/vertices/in");
        let client = reqwest::Client::new();
        let post_handle = tokio::spawn({
            let client = client.clone();
            let url = url.clone();
            async move {
                client
                    .post(&url)
                    .header("X-Numaflow-Id", "nack-id")
                    .body("bad")
                    .send()
                    .await
                    .unwrap()
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let (tx, mut rx) = mpsc::channel(10);
        source
            .read(
                SourceReadRequest {
                    count: 10,
                    timeout: Duration::from_secs(1),
                },
                tx,
            )
            .await;

        let mut messages = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            messages.push(msg);
        }
        assert_eq!(messages.len(), 1);

        source.nack(vec![messages.remove(0).offset]).await;

        let resp = post_handle.await.unwrap();
        assert_eq!(resp.status(), 500);
    }

    #[tokio::test]
    async fn test_pending_returns_none() {
        let (source, _port) = setup().await;
        assert_eq!(source.pending().await, None);
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn test_partitions() {
        let (source, _port) = setup().await;
        assert_eq!(source.partitions().await, Some(vec![0]));
    }

    #[tokio::test]
    async fn test_custom_event_time_and_keys() {
        let (source, port) = setup().await;

        let url = format!("http://127.0.0.1:{port}/vertices/in");
        let client = reqwest::Client::new();
        let _post_handle = tokio::spawn({
            let client = client.clone();
            let url = url.clone();
            async move {
                client
                    .post(&url)
                    .header("X-Numaflow-Event-Time", "1700000300000")
                    .header("X-Numaflow-Keys", "keyA, keyB")
                    .body("payload")
                    .send()
                    .await
                    .unwrap()
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let (tx, mut rx) = mpsc::channel(10);
        source
            .read(
                SourceReadRequest {
                    count: 10,
                    timeout: Duration::from_secs(1),
                },
                tx,
            )
            .await;

        let mut messages = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            messages.push(msg);
        }
        assert_eq!(messages.len(), 1);

        let msg = &messages[0];
        assert_eq!(msg.value, b"payload");
        assert_eq!(msg.keys, vec!["keyA", "keyB"]);
        assert_eq!(
            msg.event_time,
            Utc.timestamp_millis_opt(1700000300000).unwrap()
        );

        // Event-time header should NOT be forwarded.
        assert!(!msg.headers.contains_key(NUMAFLOW_EVENT_TIME_HEADER));

        // Ack to clean up.
        source
            .ack(vec![Offset {
                offset: msg.offset.offset.clone(),
                partition_id: 0,
            }])
            .await;
    }

    #[tokio::test]
    async fn test_duplicate_id_returns_conflict() {
        let (source, port) = setup().await;

        let url = format!("http://127.0.0.1:{port}/vertices/in");
        let client = reqwest::Client::new();

        // First request — will block waiting for ack.
        let _first = tokio::spawn({
            let client = client.clone();
            let url = url.clone();
            async move {
                client
                    .post(&url)
                    .header("X-Numaflow-Id", "dup-id")
                    .body("first")
                    .send()
                    .await
                    .unwrap()
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Second request with the same ID should get 409.
        let resp = client
            .post(&url)
            .header("X-Numaflow-Id", "dup-id")
            .body("second")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 409);

        // Clean up: read and ack the first message.
        let (tx, mut rx) = mpsc::channel(10);
        source
            .read(
                SourceReadRequest {
                    count: 10,
                    timeout: Duration::from_secs(1),
                },
                tx,
            )
            .await;
        let mut messages = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            messages.push(msg);
        }
        for msg in messages {
            source
                .ack(vec![Offset {
                    offset: msg.offset.offset,
                    partition_id: 0,
                }])
                .await;
        }
    }
}
