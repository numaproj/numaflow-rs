use numaflow::map;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    map::Server::new(Cat).start().await
}

struct Cat;

#[tonic::async_trait]
impl map::Mapper for Cat {
    async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
        let message = map::MessageBuilder::new()
            .keys(input.keys)
            .values(input.value)
            .tags(vec![])
            .build();
        vec![message]
    }
}
