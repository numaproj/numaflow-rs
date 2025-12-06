use numaflow::sink::{self, Response, SinkRequest};

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
            let response = Response::on_success(
                datum.id, None
            );

            // return the responses
            responses.push(response);
        }

        responses
    }
}