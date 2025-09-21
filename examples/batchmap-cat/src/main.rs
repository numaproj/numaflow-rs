use numaflow::batchmap;
use numaflow::batchmap::{BatchResponse, Datum, Message};

#[tokio::main]
async fn main() -> Result<(), numaflow::error::Error> {
    batchmap::Server::new(Cat).start().await
}

struct Cat;

#[tonic::async_trait]
impl batchmap::BatchMapper for Cat {
    async fn batchmap(&self, mut input: tokio::sync::mpsc::Receiver<Datum>) -> Vec<BatchResponse> {
        let mut responses: Vec<BatchResponse> = Vec::new();
        while let Some(datum) = input.recv().await {
            let mut response = BatchResponse::from_id(datum.id);
            response.append(Message::new(datum.value).with_keys(datum.keys.clone()));
            responses.push(response);
        }
        responses
    }
}
