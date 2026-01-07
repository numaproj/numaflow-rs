use numaflow::batchmap;
use numaflow::batchmap::{BatchResponse, Datum, Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    batchmap::Server::new(Flatmap).start().await
}

struct Flatmap;

#[tonic::async_trait]
impl batchmap::BatchMapper for Flatmap {
    async fn batchmap(&self, mut input: tokio::sync::mpsc::Receiver<Datum>) -> Vec<BatchResponse> {
        let mut responses: Vec<BatchResponse> = Vec::new();
        while let Some(datum) = input.recv().await {
            let mut response = BatchResponse::from_id(datum.id);

            // Convert Vec<u8> to String, using from_utf8_lossy to ignore errors
            let s = String::from_utf8_lossy(&datum.value);

            // Split the string by ","
            let parts: Vec<&str> = s.split(',').collect();

            // return the resulting parts as a Vec<Message>
            for part in parts {
                response.append(Message::new(Vec::from(part)).with_keys(datum.keys.clone()));
            }
            responses.push(response);
        }
        responses
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use numaflow::batchmap::BatchMapper;
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    fn create_datum(id: &str, keys: Vec<String>, value: Vec<u8>) -> Datum {
        Datum {
            id: id.to_string(),
            keys,
            value,
            watermark: chrono::Utc::now(),
            event_time: chrono::Utc::now(),
            headers: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_flatmap_splits_comma_separated_values() {
        let flatmap = Flatmap;
        let (tx, rx) = mpsc::channel(10);

        let datum = create_datum("msg-1", vec!["key1".to_string()], b"a,b,c".to_vec());
        tx.send(datum).await.unwrap();
        drop(tx);

        let responses = flatmap.batchmap(rx).await;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].id, "msg-1");
        assert_eq!(responses[0].message.len(), 3);
        assert_eq!(responses[0].message[0].value, b"a");
        assert_eq!(responses[0].message[1].value, b"b");
        assert_eq!(responses[0].message[2].value, b"c");
    }

    #[tokio::test]
    async fn test_flatmap_single_value_no_comma() {
        let flatmap = Flatmap;
        let (tx, rx) = mpsc::channel(10);

        let datum = create_datum("msg-1", vec!["key1".to_string()], b"single_value".to_vec());
        tx.send(datum).await.unwrap();
        drop(tx);

        let responses = flatmap.batchmap(rx).await;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].message.len(), 1);
        assert_eq!(responses[0].message[0].value, b"single_value");
    }

    #[tokio::test]
    async fn test_flatmap_preserves_keys_for_all_parts() {
        let flatmap = Flatmap;
        let (tx, rx) = mpsc::channel(10);

        let datum = create_datum(
            "msg-1",
            vec!["key1".to_string(), "key2".to_string()],
            b"x,y".to_vec(),
        );
        tx.send(datum).await.unwrap();
        drop(tx);

        let responses = flatmap.batchmap(rx).await;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].message.len(), 2);
        for msg in &responses[0].message {
            assert_eq!(msg.keys, Some(vec!["key1".to_string(), "key2".to_string()]));
        }
    }

    #[tokio::test]
    async fn test_flatmap_multiple_messages() {
        let flatmap = Flatmap;
        let (tx, rx) = mpsc::channel(10);

        tx.send(create_datum(
            "msg-1",
            vec!["k1".to_string()],
            b"a,b".to_vec(),
        ))
        .await
        .unwrap();
        tx.send(create_datum(
            "msg-2",
            vec!["k2".to_string()],
            b"x,y,z".to_vec(),
        ))
        .await
        .unwrap();
        drop(tx);

        let responses = flatmap.batchmap(rx).await;

        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0].id, "msg-1");
        assert_eq!(responses[0].message.len(), 2);
        assert_eq!(responses[1].id, "msg-2");
        assert_eq!(responses[1].message.len(), 3);
    }

    #[tokio::test]
    async fn test_flatmap_empty_input() {
        let flatmap = Flatmap;
        let (tx, rx) = mpsc::channel(10);
        drop(tx);

        let responses = flatmap.batchmap(rx).await;

        assert_eq!(responses.len(), 0);
    }

    #[tokio::test]
    async fn test_flatmap_empty_value() {
        let flatmap = Flatmap;
        let (tx, rx) = mpsc::channel(10);

        let datum = create_datum("msg-1", vec!["key1".to_string()], vec![]);
        tx.send(datum).await.unwrap();
        drop(tx);

        let responses = flatmap.batchmap(rx).await;

        assert_eq!(responses.len(), 1);
        // Empty string split by comma gives one empty part
        assert_eq!(responses[0].message.len(), 1);
        assert_eq!(responses[0].message[0].value, b"");
    }

    #[tokio::test]
    async fn test_flatmap_handles_invalid_utf8() {
        let flatmap = Flatmap;
        let (tx, rx) = mpsc::channel(10);

        // Invalid UTF-8 bytes - from_utf8_lossy should handle gracefully
        let invalid_utf8 = vec![0xff, 0xfe, b',', b'a'];
        let datum = create_datum("msg-1", vec!["key1".to_string()], invalid_utf8);
        tx.send(datum).await.unwrap();
        drop(tx);

        let responses = flatmap.batchmap(rx).await;

        // Should still process and split by comma
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].message.len(), 2);
    }
}
