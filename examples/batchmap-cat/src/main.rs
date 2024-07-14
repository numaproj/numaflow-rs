use numaflow::batchmap;
use numaflow::batchmap::{BatchResponse, Datum, Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    batchmap::Server::new(Cat).start().await
}

struct Cat;

#[tonic::async_trait]
impl batchmap::BatchMapper for Cat {
    async fn batchmap(&self, mut input: tokio::sync::mpsc::Receiver<Datum>) -> Vec<BatchResponse> {
        let mut responses: Vec<BatchResponse> = Vec::new();
             while let Some(datum) = input.recv().await {
                let mut response = BatchResponse::from_id(datum.id);
                response.append(Message {
                        keys: Option::from(datum.keys),
                        value: datum.value,
                        tags: None,
                });
                responses.push(response);
            }
        responses
    }
}
