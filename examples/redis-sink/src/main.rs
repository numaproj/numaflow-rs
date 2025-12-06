use numaflow::sink::{self, Response, SinkRequest, Sinker};
use redis::AsyncCommands;
use std::env;
use std::sync::Arc;
use tokio::sync::Mutex;

/// RedisTestSink is a sink that writes messages to Redis hashes.
/// Created for numaflow e2e tests.
struct RedisTestSink {
    hash_key: String,
    message_count: usize,
    inflight_messages: Arc<Mutex<Vec<SinkRequest>>>,
    client: redis::Client,
    check_order: bool,
}

impl RedisTestSink {
    /// Creates a new instance of RedisTestSink with a Redis client.
    fn new() -> Self {
        let client = redis::Client::open("redis://redis:6379")
            .expect("Failed to create Redis client");

        let hash_key = env::var("SINK_HASH_KEY")
            .expect("SINK_HASH_KEY environment variable is not set");

        let message_count: usize = env::var("MESSAGE_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let check_order: bool = env::var("CHECK_ORDER")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(false);

        RedisTestSink {
            client,
            hash_key,
            message_count,
            inflight_messages: Arc::new(Mutex::new(Vec::with_capacity(message_count))),
            check_order,
        }
    }
}

#[tonic::async_trait]
impl Sinker for RedisTestSink {
    /// This redis UDSink is created for numaflow e2e tests. This handle function assumes that
    /// a redis instance listening on address redis:6379 has already been up and running.
    async fn sink(&self, mut input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
        let mut results: Vec<Response> = Vec::new();

        // Get async connection to Redis
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
                    let result: Result<(), redis::RedisError> = con
                        .hincr(&self.hash_key, result_message, 1i64)
                        .await;

                    match result {
                        Ok(_) => {
                            println!(
                                "Incremented by 1 the no. of occurrences of {} under hash key {}",
                                result_message, self.hash_key
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

            // Watermark and event time of the message can be accessed
            let _ = datum.event_time;
            let _ = datum.watermark;

            // We use redis hashes to store messages.
            // Each field of a hash is the content of a message and
            // value of the field is the no. of occurrences of the message.
            let value_str = String::from_utf8(value).unwrap_or_else(|_| "".to_string());

            let result: Result<(), redis::RedisError> = con
                .hincr(&self.hash_key, &value_str, 1i64)
                .await;

            match result {
                Ok(_) => {
                    println!(
                        "Incremented by 1 the no. of occurrences of {} under hash key {}",
                        value_str, self.hash_key
                    );
                }
                Err(e) => {
                    eprintln!("Set Error - {:?}", e);
                }
            }

            results.push(Response {
                id,
                response_type: sink::ResponseType::Success,
                err: None,
                serve_response: None,
                on_success_msg: None,
            });
        }

        results
    }
}

#[tokio::main]
async fn main() {
    let sink = RedisTestSink::new();

    let server = sink::Server::new(sink);

    if let Err(e) = server.start().await {
        panic!("Failed to start sink function server: {:?}", e);
    }
}