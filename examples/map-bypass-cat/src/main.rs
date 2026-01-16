//! This example is added to allow creation of a reproducible image to be used for e2e testing
//! of MonoVertex's bypass feature.
//!
//! Based on the message content, tags will be added to messages which will allow the bypass router
//! to route them to the specific sink:
//! * Add "fallback" tag to all the messages which have the word "fallback" in their value.
//! * Add "onSuccess" tag to all the messages which have the word "onSuccess" in their value.
//! * Add "sink" tag to all the messages which have the word "primary" in their value.
//!
//! This example will be used along with the following bypass spec:
//! ```yaml
//! bypass:
//!   sink:
//!     tags:
//!       operator: or
//!       values:
//!         - sink
//!   fallback:
//!     tags:
//!       operator: or
//!       values:
//!         - fallback
//!   onSuccess:
//!     tags:
//!       operator: or
//!       values:
//!         - onSuccess
//! ```

use numaflow::map;
use numaflow::shared::grpc_server::ServerExtras;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    map::Server::new(BypassCat)
        .with_max_message_size(10240)
        .start()
        .await
}

struct BypassCat;

#[tonic::async_trait]
impl map::Mapper for BypassCat {
    async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
        let input_str = String::from_utf8(input.value.clone()).expect("Invalid UTF-8");
        let mut tags = vec![];
        if input_str.contains("fallback") || input_str.contains("Fallback") {
            tags.push("fallback".to_string());
        }
        if input_str.contains("onSuccess")
            || input_str.contains("OnSuccess")
            || input_str.contains("on_success")
            || input_str.contains("on-success")
        {
            tags.push("onSuccess".to_string());
        }
        if input_str.contains("primary") {
            tags.push("sink".to_string());
        }
        vec![
            map::Message::new(input.value)
                .with_keys(input.keys.clone())
                .with_tags(tags),
        ]
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
    async fn test_map_cat_no_tags() {
        let cat = BypassCat;
        let request = create_request(vec!["key1".to_string()], b"regular message".to_vec());

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert!(messages[0].tags.as_ref().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_map_cat_fallback_lowercase() {
        let cat = BypassCat;
        let request = create_request(vec!["key1".to_string()], b"this is fallback data".to_vec());

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert!(
            messages[0]
                .tags
                .as_ref()
                .unwrap()
                .contains(&"fallback".to_string())
        );
    }

    #[tokio::test]
    async fn test_map_cat_fallback_capitalized() {
        let cat = BypassCat;
        let request = create_request(vec!["key1".to_string()], b"this is Fallback data".to_vec());

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert!(
            messages[0]
                .tags
                .as_ref()
                .unwrap()
                .contains(&"fallback".to_string())
        );
    }

    #[tokio::test]
    async fn test_map_cat_on_success_camel_case() {
        let cat = BypassCat;
        let request = create_request(vec!["key1".to_string()], b"onSuccess event".to_vec());

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert!(
            messages[0]
                .tags
                .as_ref()
                .unwrap()
                .contains(&"onSuccess".to_string())
        );
    }

    #[tokio::test]
    async fn test_map_cat_on_success_pascal_case() {
        let cat = BypassCat;
        let request = create_request(vec!["key1".to_string()], b"OnSuccess event".to_vec());

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert!(
            messages[0]
                .tags
                .as_ref()
                .unwrap()
                .contains(&"onSuccess".to_string())
        );
    }

    #[tokio::test]
    async fn test_map_cat_on_success_snake_case() {
        let cat = BypassCat;
        let request = create_request(vec!["key1".to_string()], b"on_success event".to_vec());

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert!(
            messages[0]
                .tags
                .as_ref()
                .unwrap()
                .contains(&"onSuccess".to_string())
        );
    }

    #[tokio::test]
    async fn test_map_cat_on_success_kebab_case() {
        let cat = BypassCat;
        let request = create_request(vec!["key1".to_string()], b"on-success event".to_vec());

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert!(
            messages[0]
                .tags
                .as_ref()
                .unwrap()
                .contains(&"onSuccess".to_string())
        );
    }

    #[tokio::test]
    async fn test_map_cat_primary_adds_sink_tag() {
        let cat = BypassCat;
        let request = create_request(vec!["key1".to_string()], b"primary destination".to_vec());

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert!(
            messages[0]
                .tags
                .as_ref()
                .unwrap()
                .contains(&"sink".to_string())
        );
    }

    #[tokio::test]
    async fn test_map_cat_multiple_tags() {
        let cat = BypassCat;
        let request = create_request(
            vec!["key1".to_string()],
            b"fallback and onSuccess and primary".to_vec(),
        );

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        let tags = messages[0].tags.as_ref().unwrap();
        assert_eq!(tags.len(), 3);
        assert!(tags.contains(&"fallback".to_string()));
        assert!(tags.contains(&"onSuccess".to_string()));
        assert!(tags.contains(&"sink".to_string()));
    }

    #[tokio::test]
    async fn test_map_cat_fallback_and_on_success() {
        let cat = BypassCat;
        let request = create_request(
            vec!["key1".to_string()],
            b"Fallback with on_success".to_vec(),
        );

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        let tags = messages[0].tags.as_ref().unwrap();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&"fallback".to_string()));
        assert!(tags.contains(&"onSuccess".to_string()));
    }

    #[tokio::test]
    async fn test_map_cat_preserves_value_with_tags() {
        let cat = BypassCat;
        let value = b"fallback message content".to_vec();
        let request = create_request(vec!["key1".to_string()], value.clone());

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, value);
        assert!(
            messages[0]
                .tags
                .as_ref()
                .unwrap()
                .contains(&"fallback".to_string())
        );
    }

    #[tokio::test]
    async fn test_map_cat_preserves_keys_with_tags() {
        let cat = BypassCat;
        let request = create_request(
            vec!["key1".to_string(), "key2".to_string()],
            b"primary data".to_vec(),
        );

        let messages = cat.map(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].keys,
            Some(vec!["key1".to_string(), "key2".to_string()])
        );
        assert!(
            messages[0]
                .tags
                .as_ref()
                .unwrap()
                .contains(&"sink".to_string())
        );
    }
}
