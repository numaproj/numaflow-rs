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
                .with_keys(input.keys.clone())
                .with_tags(vec![]);
            if tx.send(message).await.is_err() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use numaflow::mapstream::{MapStreamRequest, MapStreamer};
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    fn create_request(keys: Vec<String>, value: Vec<u8>) -> MapStreamRequest {
        MapStreamRequest {
            keys,
            value,
            watermark: chrono::Utc::now(),
            eventtime: chrono::Utc::now(),
            headers: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_map_stream_splits_comma_separated() {
        let cat = Cat;
        let (tx, mut rx) = mpsc::channel(10);

        let request = create_request(vec!["key1".to_string()], b"a,b,c".to_vec());
        cat.map_stream(request, tx).await;

        let mut messages = Vec::new();
        while let Some(msg) = rx.recv().await {
            messages.push(msg);
        }

        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].value, b"a");
        assert_eq!(messages[1].value, b"b");
        assert_eq!(messages[2].value, b"c");
    }

    #[tokio::test]
    async fn test_map_stream_single_value() {
        let cat = Cat;
        let (tx, mut rx) = mpsc::channel(10);

        let request = create_request(vec!["key1".to_string()], b"single".to_vec());
        cat.map_stream(request, tx).await;

        let mut messages = Vec::new();
        while let Some(msg) = rx.recv().await {
            messages.push(msg);
        }

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, b"single");
    }

    #[tokio::test]
    async fn test_map_stream_preserves_keys() {
        let cat = Cat;
        let (tx, mut rx) = mpsc::channel(10);

        let request = create_request(
            vec!["key1".to_string(), "key2".to_string()],
            b"x,y".to_vec(),
        );
        cat.map_stream(request, tx).await;

        let mut messages = Vec::new();
        while let Some(msg) = rx.recv().await {
            messages.push(msg);
        }

        assert_eq!(messages.len(), 2);
        for msg in &messages {
            assert_eq!(msg.keys, Some(vec!["key1".to_string(), "key2".to_string()]));
        }
    }

    #[tokio::test]
    async fn test_map_stream_empty_value() {
        let cat = Cat;
        let (tx, mut rx) = mpsc::channel(10);

        let request = create_request(vec!["key1".to_string()], vec![]);
        cat.map_stream(request, tx).await;

        let mut messages = Vec::new();
        while let Some(msg) = rx.recv().await {
            messages.push(msg);
        }

        // Empty string split gives one empty part
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, b"");
    }

    #[tokio::test]
    async fn test_map_stream_sets_empty_tags() {
        let cat = Cat;
        let (tx, mut rx) = mpsc::channel(10);

        let request = create_request(vec!["key1".to_string()], b"value".to_vec());
        cat.map_stream(request, tx).await;

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.tags, Some(vec![]));
    }

    #[tokio::test]
    async fn test_map_stream_invalid_utf8_returns_empty() {
        let cat = Cat;
        let (tx, mut rx) = mpsc::channel(10);

        // Invalid UTF-8 - from_utf8 will fail and return default empty string
        let invalid_utf8 = vec![0xff, 0xfe];
        let request = create_request(vec!["key1".to_string()], invalid_utf8);
        cat.map_stream(request, tx).await;

        let mut messages = Vec::new();
        while let Some(msg) = rx.recv().await {
            messages.push(msg);
        }

        // Invalid UTF-8 becomes empty string, split gives one empty part
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, b"");
    }
}
