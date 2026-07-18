use std::collections::HashMap;
use numaflow::shared::NackOptions;
use numaflow::sink::{self, Response, SinkRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    sink::Server::new(Logger).start().await
}

struct Logger;

#[tonic::async_trait]
impl sink::Sinker for Logger {
    async fn sink(&self, mut input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
        let mut responses: Vec<Response> = Vec::new();

        while let Some(datum) = input.recv().await {
            // do something better, but for now let's just log it.
            // please note that `from_utf8` is working because the input in this
            // example uses utf-8 data.
            let response = match std::str::from_utf8(&datum.value) {
                Ok(v) => {
                    println!("{}", v);
                    // record the response
                    Response::ok(datum.id)
                }
                Err(e) => {
                    let mut nack_map = HashMap::new();
                    nack_map.insert("property".to_string(), "value".to_string());
                    let nack_options = NackOptions{
                        reason: Some(format!("Nacked due to failure: {}", e)),
                        nack_map,
                        ..Default::default()
                    };
                    Response::nack(datum.id, Some(nack_options))
                },
            };

            // return the responses
            responses.push(response);
        }

        responses
    }
}

