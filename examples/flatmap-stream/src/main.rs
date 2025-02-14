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

        for i in 0..2 {
            let message = Message::new(input.value.clone())
                .with_keys(vec![format!("key-{}", i)])
                .with_tags(vec![]);
            if tx.send(message).await.is_err() {
                break;
            }
        }
    }
}
