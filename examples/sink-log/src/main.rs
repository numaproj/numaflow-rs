use numaflow::sink::{self, Response, SinkRequest};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    sink::Server::new(Logger {}).start().await
}

struct Logger {}

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
                    Response {
                        id: datum.id,
                        success: true,
                        err: "".to_string(),
                    }
                }
                Err(e) => Response {
                    id: datum.id,
                    success: true, // there is no point setting success to false as retrying is not going to help
                    err: format!("Invalid UTF-8 sequence: {}", e),
                },
            };

            // return the responses
            responses.push(response);
        }

        responses
    }
}
