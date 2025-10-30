use numaflow::sink::{self, OnSuccessMessage, Response, SinkRequest};
use rand::Rng;

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
            // generate random response
            let response = RandomResponse::from(rand::rng().random_range(1..=5))
                .to_response(datum.id, datum.value);

            // return the responses
            responses.push(response);
        }

        responses
    }
}

enum RandomResponse {
    Success,
    Failure,
    Fallback,
    OnSuccessOriginal,
    OnSuccessChanged,
}

impl From<usize> for RandomResponse {
    fn from(value: usize) -> Self {
        match value {
            1 => RandomResponse::Failure,
            2 => RandomResponse::Fallback,
            3 => RandomResponse::OnSuccessOriginal,
            4 => RandomResponse::OnSuccessChanged,
            _ => RandomResponse::Success,
        }
    }
}

impl RandomResponse {
    fn to_response(&self, id: String, value: Vec<u8>) -> Response {
        match self {
            RandomResponse::Success => Response::ok(id),
            RandomResponse::Failure => Response::failure(id, "bad luck".to_string()),
            RandomResponse::Fallback => Response::fallback(id),
            RandomResponse::OnSuccessOriginal => Response::on_success(id, None),
            RandomResponse::OnSuccessChanged => {
                let changed_val =
                    String::from("Changed value: ") + std::str::from_utf8(&value).unwrap();
                Response::on_success(
                    id,
                    Some(OnSuccessMessage {
                        user_metadata: None,
                        value: changed_val.into(),
                        keys: None,
                    }),
                )
            }
        }
    }
}
