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

#[cfg(test)]
mod tests {
    use super::*;
    use numaflow::map::{MapRequest, Mapper, SystemMetadata, UserMetadata};
    use std::collections::HashMap;

    fn create_request(keys: Vec<String>, value: Vec<u8>) -> MapRequest {
        MapRequest {
            keys,
            value,
            watermark: chrono::Utc::now(),
            eventtime: chrono::Utc::now(),
            headers: HashMap::new(),
            user_metadata: UserMetadata::new(),
            system_metadata: SystemMetadata::new(),
        }
    }

    #[tokio::test]
    async fn test_map_cat_passes_through_value() {
        let cat = Cat;
        let request = create_request(vec!["key1".to_string()], b"Hello, World!".to_vec());

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, b"Hello, World!");
    }

    #[tokio::test]
    async fn test_map_cat_preserves_keys() {
        let cat = Cat;
        let request = create_request(
            vec!["key1".to_string(), "key2".to_string()],
            b"test".to_vec(),
        );

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].keys,
            Some(vec!["key1".to_string(), "key2".to_string()])
        );
    }

    #[tokio::test]
    async fn test_map_cat_empty_value() {
        let cat = Cat;
        let request = create_request(vec!["key1".to_string()], vec![]);

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, Vec::<u8>::new());
    }

    #[tokio::test]
    async fn test_map_cat_empty_keys() {
        let cat = Cat;
        let request = create_request(vec![], b"value".to_vec());

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].keys, Some(vec![]));
        assert_eq!(messages[0].value, b"value");
    }

    #[tokio::test]
    async fn test_map_cat_binary_data() {
        let cat = Cat;
        let binary_data = vec![0x00, 0x01, 0xff, 0xfe, 0x10];
        let request = create_request(vec!["key1".to_string()], binary_data.clone());

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, binary_data);
    }
}
