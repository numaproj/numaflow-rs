use numaflow::sink::KeyValueGroup;
use numaflow::sink::{self, Message, Response, SinkRequest};
use rand::Rng;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    sink::Server::new(SinkHandler).start().await
}

struct SinkHandler;

#[tonic::async_trait]
impl sink::Sinker for SinkHandler {
    async fn sink(&self, mut input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
        let mut responses: Vec<Response> = Vec::new();

        while let Some(datum) = input.recv().await {
            let response = match primary_sink_write_status() {
                // Writing to primary sink was successful, we want to send a message to on_success sink
                // The message sent to on_success sink can be different from the original message
                true => {
                    // build user metadata for on_success sink message payload
                    let user_metadata = HashMap::from([(
                        String::from("key1"),
                        KeyValueGroup::from(HashMap::from([(
                            datum.id.clone(),
                            datum.value.clone(),
                        )])),
                    )]);
                    Response::on_success(
                        datum.id,
                        // optional message, send original message to on_success sink in case `None` is provided
                        Message::new(datum.value) // required value
                            .with_keys(vec!["key1".to_string()]) // optional keys
                            .with_user_metadata(user_metadata) // optional metadata
                            .build(),
                    )
                }
                // Writing to primary sink was not successful, we want to send a message to fallback sink
                // The message sent to the fallback sink will be the original message
                false => Response::fallback(datum.id),
            };

            // return the responses
            responses.push(response);
        }

        responses
    }
}

/// Get status of writing to primary sink
/// true: write successful
/// false: write failed
fn primary_sink_write_status() -> bool {
    // dummy logic to simulate write status
    rand::rng().random_range(0..=1) == 1
}
