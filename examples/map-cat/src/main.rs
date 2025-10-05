use numaflow::map;
use numaflow::shared::grpc_server::ServerExtras;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    map::Server::new(Cat)
        .with_max_message_size(10240)
        .start()
        .await
}

struct Cat;

#[tonic::async_trait]
impl map::Mapper for Cat {
    async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
        vec![map::Message::new(input.value).with_keys(input.keys.clone())]
    }
}
