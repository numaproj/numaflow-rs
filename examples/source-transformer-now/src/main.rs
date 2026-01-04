use numaflow::sourcetransform;

/// A simple source transformer which assigns event time to the current time in utc.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    sourcetransform::Server::new(NowCat).start().await
}

struct NowCat;

#[tonic::async_trait]
impl sourcetransform::SourceTransformer for NowCat {
    async fn transform(
        &self,
        input: sourcetransform::SourceTransformRequest,
    ) -> Vec<sourcetransform::Message> {
        vec![
            sourcetransform::Message::new(input.value, chrono::offset::Utc::now())
                .with_keys(input.keys.clone()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use numaflow::sourcetransform::{SourceTransformRequest, SourceTransformer};

    fn create_request(value: Vec<u8>, keys: Vec<String>) -> SourceTransformRequest {
        SourceTransformRequest {
            keys,
            value,
            watermark: Utc::now(),
            eventtime: Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap(),
            headers: Default::default(),
        }
    }

    #[tokio::test]
    async fn test_transform_single_message() {
        let now_cat = NowCat;
        let before = Utc::now();

        let request = create_request(b"hello numaflow".to_vec(), vec!["key1".to_string()]);
        let messages = now_cat.transform(request).await;

        let after = Utc::now();

        assert_eq!(messages.len(), 1, "Should return one message");
        assert_eq!(messages[0].value, b"hello numaflow", "Value should be preserved");
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
        let now_cat = NowCat;

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
        let now_cat = NowCat;

        let request = create_request(vec![], vec!["key1".to_string()]);
        let messages = now_cat.transform(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, Vec::<u8>::new(), "Empty value should be preserved");
    }

    #[tokio::test]
    async fn test_transform_updates_old_event_time() {
        let now_cat = NowCat;

        // Create a request with an old event time
        let old_event_time = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let request = SourceTransformRequest {
            keys: vec!["key1".to_string()],
            value: b"old event".to_vec(),
            watermark: Utc::now(),
            eventtime: old_event_time,
            headers: Default::default(),
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
        let now_cat = NowCat;

        let request = create_request(b"data".to_vec(), vec![]);
        let messages = now_cat.transform(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].keys, Some(vec![]), "Empty keys should be preserved");
    }
}
