use numaflow::batchmap;
use numaflow::batchmap::{BatchResponse, Datum, Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    batchmap::Server::new(Flatmap).start().await
}

struct Flatmap;

#[tonic::async_trait]
impl batchmap::BatchMapper for Flatmap {
    async fn batchmap(&self, mut input: tokio::sync::mpsc::Receiver<Datum>) -> Vec<BatchResponse> {
        let mut responses: Vec<BatchResponse> = Vec::new();
        while let Some(datum) = input.recv().await {
            let mut response = BatchResponse::from_id(datum.id);

            // Convert Vec<u8> to String, using from_utf8_lossy to ignore errors
            let s = String::from_utf8_lossy(&datum.value);

            // Split the string by ","
            let parts: Vec<&str> = s.split(',').collect();

            // Print the resulting parts
            for part in parts {
                response.append(Message {
                    keys: Some(datum.keys.clone()),
                    value: Vec::from(part),
                    tags: None,
                });
            }
            responses.push(response);
        }
        responses
    }
}
