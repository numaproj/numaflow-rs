use numaflow::batchmap;
use numaflow::batchmap::{BatchResponse, Datum, Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    batchmap::Server::new(Cat).start().await
}

struct Cat;

#[tonic::async_trait]
impl batchmap::BatchMapper for Cat {
    async fn batchmap(&self, mut input: tokio::sync::mpsc::Receiver<Datum>) -> Vec<BatchResponse> {
        let mut responses: Vec<BatchResponse> = Vec::new();
        while let Some(datum) = input.recv().await {
            let mut response = BatchResponse::from_id(datum.id);
            response.append(Message::new(datum.value).with_keys(datum.keys.clone()));
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
    async fn test_batchmap_single_message() {
        let cat = Cat;
        let (tx, rx) = mpsc::channel(10);

        let datum = create_datum(
            "msg-1",
            vec!["key1".to_string()],
            b"Hello, Numaflow!".to_vec(),
        );
        tx.send(datum).await.unwrap();
        drop(tx);

        let responses = cat.batchmap(rx).await;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].id, "msg-1");
        assert_eq!(responses[0].message.len(), 1);
        assert_eq!(responses[0].message[0].value, b"Hello, Numaflow!");
        assert_eq!(responses[0].message[0].keys, Some(vec!["key1".to_string()]));
    }

    #[tokio::test]
    async fn test_batchmap_multiple_messages() {
        let cat = Cat;
        let (tx, rx) = mpsc::channel(10);

        for i in 0..5 {
            let datum = create_datum(
                &format!("msg-{}", i),
                vec![format!("key-{}", i)],
                format!("Message {}", i).into_bytes(),
            );
            tx.send(datum).await.unwrap();
        }
        drop(tx);

        let responses = cat.batchmap(rx).await;

        assert_eq!(responses.len(), 5);
        for (i, response) in responses.iter().enumerate() {
            assert_eq!(response.id, format!("msg-{}", i));
            assert_eq!(response.message.len(), 1);
            assert_eq!(response.message[0].value, format!("Message {}", i).into_bytes());
        }
    }

    #[tokio::test]
    async fn test_batchmap_empty_input() {
        let cat = Cat;
        let (tx, rx) = mpsc::channel(10);
        drop(tx);

        let responses = cat.batchmap(rx).await;

        assert_eq!(responses.len(), 0, "No responses for empty input");
    }

    #[tokio::test]
    async fn test_batchmap_preserves_keys() {
        let cat = Cat;
        let (tx, rx) = mpsc::channel(10);

        let datum = create_datum(
            "msg-1",
            vec!["key1".to_string(), "key2".to_string()],
            b"test value".to_vec(),
        );
        tx.send(datum).await.unwrap();
        drop(tx);

        let responses = cat.batchmap(rx).await;

        assert_eq!(responses.len(), 1);
        assert_eq!(
            responses[0].message[0].keys,
            Some(vec!["key1".to_string(), "key2".to_string()])
        );
    }

    #[tokio::test]
    async fn test_batchmap_empty_value() {
        let cat = Cat;
        let (tx, rx) = mpsc::channel(10);

        let datum = create_datum("msg-empty", vec!["key1".to_string()], vec![]);
        tx.send(datum).await.unwrap();
        drop(tx);

        let responses = cat.batchmap(rx).await;

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].id, "msg-empty");
        assert_eq!(responses[0].message[0].value, Vec::<u8>::new());
    }
}
