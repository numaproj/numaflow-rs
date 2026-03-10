use numaflow::sink::{self, Response, SinkRequest, Sinker};
use redis::AsyncCommands;
use std::env;
use tokio::sync::Mutex;

/// The mode of operation for the Redis sink.
#[derive(Debug, Clone, PartialEq)]
enum Mode {
    /// Default mode: store messages in Redis hashes using HINCR.
    Hash,
    /// Ordered mode: store messages in Redis lists using RPUSH,
    /// preserving insertion order per key.
    Ordered,
}

/// RedisTestSink is a sink that writes messages to Redis.
/// Created for numaflow e2e tests.
///
/// Supports two modes controlled by the `MODE` environment variable:
/// - Default (hash): writes to Redis hashes via HINCR
/// - "ordered": writes to Redis lists via RPUSH, keyed by `{SINK_KEY}:{joined_message_keys}`
struct RedisTestSink {
    /// Redis key prefix. Set by `SINK_KEY` or `SINK_HASH_KEY` environment variable
    /// (interchangeable).
    sink_key: String,
    /// Used to determine how many subsequent number of messages at a time to check the order of.
    /// This is set by an environment variable `MESSAGE_COUNT`
    message_count: usize,
    /// Used to collect `message_count` number of messages, check whether they all arrived in order
    /// and increment the count for the order result in Redis
    inflight_messages: Mutex<Vec<SinkRequest>>,
    client: redis::Client,
    /// If true, checks the order of messages based on event time.
    /// This is set by an environment variable `CHECK_ORDER`
    check_order: bool,
    /// The mode of operation.
    mode: Mode,
}

impl RedisTestSink {
    /// Creates a new instance of RedisTestSink with a Redis client.
    fn new() -> Self {
        let client =
            redis::Client::open("redis://redis:6379").expect("Failed to create Redis client");

        // SINK_KEY and SINK_HASH_KEY are interchangeable
        let sink_key = env::var("SINK_KEY")
            .or_else(|_| env::var("SINK_HASH_KEY"))
            .expect("SINK_KEY or SINK_HASH_KEY environment variable is not set");

        let message_count: usize = env::var("MESSAGE_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let check_order: bool = env::var("CHECK_ORDER")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(false);

        let mode = match env::var("MODE").ok().as_deref() {
            Some("ordered") => Mode::Ordered,
            _ => Mode::Hash,
        };

        RedisTestSink {
            client,
            sink_key,
            message_count,
            inflight_messages: Mutex::new(Vec::with_capacity(message_count)),
            check_order,
            mode,
        }
    }

    /// Hash mode: store messages in Redis hashes using HINCR.
    async fn sink_hash(
        &self,
        mut input: tokio::sync::mpsc::Receiver<SinkRequest>,
    ) -> Vec<Response> {
        let mut results: Vec<Response> = Vec::new();

        let mut con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .expect("Failed to get Redis connection");

        while let Some(datum) = input.recv().await {
            let id = datum.id.clone();
            let value = datum.value.clone();

            if self.check_order {
                let mut inflight = self.inflight_messages.lock().await;
                inflight.push(datum);

                if inflight.len() == self.message_count {
                    // Check if messages are ordered by event time
                    let ordered = inflight
                        .windows(2)
                        .all(|w| w[0].event_time <= w[1].event_time);

                    let result_message = if ordered { "ordered" } else { "not ordered" };

                    // Increment the count for the order result in Redis
                    let result: Result<(), redis::RedisError> =
                        con.hincr(&self.sink_key, result_message, 1).await;

                    match result {
                        Ok(_) => {
                            println!(
                                "Incremented by 1 the no. of occurrences of {} under hash key {}",
                                result_message, self.sink_key
                            );
                        }
                        Err(e) => {
                            eprintln!("Set Error - {:?}", e);
                        }
                    }

                    // Reset the inflight messages
                    inflight.clear();
                }
            }

            // We use redis hashes to store messages.
            // Each field of a hash is the content of a message and
            // value of the field is the no. of occurrences of the message.
            let value_str = String::from_utf8(value).unwrap_or_else(|_| "".to_string());

            let result: Result<(), redis::RedisError> =
                con.hincr(&self.sink_key, &value_str, 1).await;

            match result {
                Ok(_) => {
                    println!(
                        "Incremented by 1 the no. of occurrences of {} under hash key {}",
                        value_str, self.sink_key
                    );
                }
                Err(e) => {
                    eprintln!("Set Error - {:?}", e);
                }
            }

            results.push(Response::ok(id));
        }

        results
    }

    /// Ordered mode: store messages in Redis lists using RPUSH.
    /// The list key is `{sink_key}_{keys joined by ":"}`.
    async fn sink_ordered(
        &self,
        mut input: tokio::sync::mpsc::Receiver<SinkRequest>,
    ) -> Vec<Response> {
        let mut results: Vec<Response> = Vec::new();

        let mut con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .expect("Failed to get Redis connection");

        while let Some(datum) = input.recv().await {
            let id = datum.id.clone();
            let value_str =
                String::from_utf8(datum.value.clone()).unwrap_or_else(|_| "".to_string());

            let list_key = format!("{}_{}", self.sink_key, datum.keys.join(":"));

            let result: Result<(), redis::RedisError> = con.rpush(&list_key, &value_str).await;

            match result {
                Ok(_) => {
                    println!("RPUSH {} to list {}", value_str, list_key);
                }
                Err(e) => {
                    eprintln!("RPUSH Error - {:?}", e);
                }
            }

            results.push(Response::ok(id));
        }

        results
    }
}

#[tonic::async_trait]
impl Sinker for RedisTestSink {
    /// This redis UDSink is created for numaflow e2e tests. This handle function assumes that
    /// a redis instance listening on address redis:6379 has already been up and running.
    async fn sink(&self, input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
        match self.mode {
            Mode::Hash => self.sink_hash(input).await,
            Mode::Ordered => self.sink_ordered(input).await,
        }
    }
}

#[tokio::main]
async fn main() {
    let sink = RedisTestSink::new();

    println!("Starting redis-sink in {:?} mode", sink.mode);

    let server = sink::Server::new(sink);

    if let Err(e) = server.start().await {
        panic!("Failed to start sink function server: {:?}", e);
    }
}
