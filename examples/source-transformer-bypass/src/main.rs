use numaflow::{map, sourcetransform};

/// A simple source transformer which assigns event time to the current time in utc.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    sourcetransform::Server::new(BypassCat).start().await
}

struct BypassCat;

#[tonic::async_trait]
impl sourcetransform::SourceTransformer for BypassCat {
    async fn transform(
        &self,
        input: sourcetransform::SourceTransformRequest,
    ) -> Vec<sourcetransform::Message> {
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
            sourcetransform::Message::new(input.value, chrono::offset::Utc::now())
                .with_keys(input.keys.clone())
                .with_tags(tags),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use numaflow::sourcetransform::{
        SourceTransformRequest, SourceTransformer, SystemMetadata, UserMetadata,
    };

    fn create_request(value: Vec<u8>, keys: Vec<String>) -> SourceTransformRequest {
        SourceTransformRequest {
            keys,
            value,
            watermark: Utc::now(),
            eventtime: Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap(),
            headers: Default::default(),
            user_metadata: UserMetadata::new(),
            system_metadata: SystemMetadata::new(),
        }
    }

    #[tokio::test]
    async fn test_transform_single_message() {
        let now_cat = BypassCat;
        let before = Utc::now();

        let request = create_request(b"hello numaflow".to_vec(), vec!["key1".to_string()]);
        let messages = now_cat.transform(request).await;

        let after = Utc::now();

        assert_eq!(messages.len(), 1, "Should return one message");
        assert_eq!(
            messages[0].value, b"hello numaflow",
            "Value should be preserved"
        );
        assert_eq!(
            messages[0].keys,
            Some(vec!["key1".to_string()]),
            "Keys should be preserved"
        );

        // Event time should be set to current time (between before and after)
        let event_time = messages[0].event_time;
        assert!(
            event_time >= before && event_time <= after,
            "Event time should be set to current UTC time"
        );
    }

    #[tokio::test]
    async fn test_transform_preserves_multiple_keys() {
        let now_cat = BypassCat;

        let request = create_request(
            b"data".to_vec(),
            vec!["key1".to_string(), "key2".to_string(), "key3".to_string()],
        );
        let messages = now_cat.transform(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].keys,
            Some(vec![
                "key1".to_string(),
                "key2".to_string(),
                "key3".to_string()
            ]),
            "All keys should be preserved"
        );
    }

    #[tokio::test]
    async fn test_transform_empty_value() {
        let now_cat = BypassCat;

        let request = create_request(vec![], vec!["key1".to_string()]);
        let messages = now_cat.transform(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].value,
            Vec::<u8>::new(),
            "Empty value should be preserved"
        );
    }

    #[tokio::test]
    async fn test_transform_updates_old_event_time() {
        let now_cat = BypassCat;

        // Create a request with an old event time
        let old_event_time = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let request = SourceTransformRequest {
            keys: vec!["key1".to_string()],
            value: b"old event".to_vec(),
            watermark: Utc::now(),
            eventtime: old_event_time,
            headers: Default::default(),
            user_metadata: UserMetadata::new(),
            system_metadata: SystemMetadata::new(),
        };

        let before = Utc::now();
        let messages = now_cat.transform(request).await;
        let after = Utc::now();

        assert_eq!(messages.len(), 1);
        let new_event_time = messages[0].event_time;
        assert!(
            new_event_time >= before && new_event_time <= after,
            "Old event time should be replaced with current time"
        );
        assert!(
            new_event_time > old_event_time,
            "New event time should be more recent than the old one"
        );
    }

    #[tokio::test]
    async fn test_transform_empty_keys() {
        let now_cat = BypassCat;

        let request = create_request(b"data".to_vec(), vec![]);
        let messages = now_cat.transform(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].keys,
            Some(vec![]),
            "Empty keys should be preserved"
        );
    }

    #[tokio::test]
    async fn test_transform_no_tags() {
        let cat = BypassCat;
        let request = create_request(b"regular message".to_vec(), vec!["key1".to_string()]);

        let messages = cat.transform(request).await;

        assert_eq!(messages.len(), 1);
        assert!(messages[0].tags.as_ref().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_transform_fallback_lowercase() {
        let cat = BypassCat;
        let request = create_request(b"this is fallback data".to_vec(), vec!["key1".to_string()]);

        let messages = cat.transform(request).await;

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
    async fn test_transform_fallback_capitalized() {
        let cat = BypassCat;
        let request = create_request(b"this is Fallback data".to_vec(), vec!["key1".to_string()]);

        let messages = cat.transform(request).await;

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
    async fn test_transform_on_success_camel_case() {
        let cat = BypassCat;
        let request = create_request(b"onSuccess event".to_vec(), vec!["key1".to_string()]);

        let messages = cat.transform(request).await;

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
    async fn test_transform_on_success_pascal_case() {
        let cat = BypassCat;
        let request = create_request(b"OnSuccess event".to_vec(), vec!["key1".to_string()]);

        let messages = cat.transform(request).await;

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
    async fn test_transform_on_success_snake_case() {
        let cat = BypassCat;
        let request = create_request(b"on_success event".to_vec(), vec!["key1".to_string()]);

        let messages = cat.transform(request).await;

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
    async fn test_transform_on_success_kebab_case() {
        let cat = BypassCat;
        let request = create_request(b"on-success event".to_vec(), vec!["key1".to_string()]);

        let messages = cat.transform(request).await;

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
    async fn test_transform_primary_adds_sink_tag() {
        let cat = BypassCat;
        let request = create_request(b"primary destination".to_vec(), vec!["key1".to_string()]);

        let messages = cat.transform(request).await;

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
    async fn test_transform_multiple_tags() {
        let cat = BypassCat;
        let request = create_request(
            b"fallback and onSuccess and primary".to_vec(),
            vec!["key1".to_string()],
        );

        let messages = cat.transform(request).await;

        assert_eq!(messages.len(), 1);
        let tags = messages[0].tags.as_ref().unwrap();
        assert_eq!(tags.len(), 3);
        assert!(tags.contains(&"fallback".to_string()));
        assert!(tags.contains(&"onSuccess".to_string()));
        assert!(tags.contains(&"sink".to_string()));
    }

    #[tokio::test]
    async fn test_transform_fallback_and_on_success() {
        let cat = BypassCat;
        let request = create_request(
            b"Fallback with on_success".to_vec(),
            vec!["key1".to_string()],
        );

        let messages = cat.transform(request).await;

        assert_eq!(messages.len(), 1);
        let tags = messages[0].tags.as_ref().unwrap();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&"fallback".to_string()));
        assert!(tags.contains(&"onSuccess".to_string()));
    }

    #[tokio::test]
    async fn test_transform_preserves_value_with_tags() {
        let cat = BypassCat;
        let value = b"fallback message content".to_vec();
        let request = create_request(value.clone(), vec!["key1".to_string()]);

        let messages = cat.transform(request).await;

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
    async fn test_transform_preserves_keys_with_tags() {
        let cat = BypassCat;
        let request = create_request(
            b"primary data".to_vec(),
            vec!["key1".to_string(), "key2".to_string()],
        );

        let messages = cat.transform(request).await;

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

    #[tokio::test]
    async fn test_transform_event_time_with_tags() {
        let cat = BypassCat;
        let before = Utc::now();
        let request = create_request(b"fallback event".to_vec(), vec!["key1".to_string()]);

        let messages = cat.transform(request).await;
        let after = Utc::now();

        assert_eq!(messages.len(), 1);
        assert!(
            messages[0]
                .tags
                .as_ref()
                .unwrap()
                .contains(&"fallback".to_string())
        );
        let event_time = messages[0].event_time;
        assert!(
            event_time >= before && event_time <= after,
            "Event time should be set to current UTC time even with tags"
        );
    }
}
