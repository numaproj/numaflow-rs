use numaflow::map;
use numaflow::shared::NackOptions;
use numaflow::shared::grpc_server::ServerExtras;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    map::Server::new(NackCat)
        .with_max_message_size(10240)
        .start()
        .await
}

struct NackCat;

#[tonic::async_trait]
impl map::Mapper for NackCat {
    async fn map(&self, _input: map::MapRequest) -> Vec<map::Message> {
        let mut nack_map = HashMap::new();
        nack_map.insert("key".to_string(), "value".to_string());
        let nack_options = NackOptions {
            reason: Some("nacked in udf".to_string()),
            nack_map,
            ..Default::default()
        };
        vec![map::Message::message_to_nack(Some(nack_options))]
    }
}
