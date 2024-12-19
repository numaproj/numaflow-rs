use numaflow::mapstream;
use numaflow::mapstream::Message;
use tokio::sync::mpsc::Sender;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    mapstream::Server::new(Cat).start().await
}

struct Cat;

#[tonic::async_trait]
impl mapstream::MapStreamer for Cat {
    async fn map_stream(&self, input: mapstream::MapStreamRequest, tx: Sender<Message>) {
        let payload_str = String::from_utf8(input.value).unwrap_or_default();
        let splits: Vec<&str> = payload_str.split(',').collect();

        for split in splits {
            let message = Message::new(split.as_bytes().to_vec())
                .keys(input.keys.clone())
                .tags(vec![]);
            if tx.send(message).await.is_err() {
                break;
            }
        }
    }
}
