use numaflow::sink::{self, Response, SinkRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    sink::Server::new(Logger).start().await
}

struct Logger;

#[tonic::async_trait]
impl sink::Sinker for Logger {
    async fn sink(&self, mut input: tokio::sync::mpsc::Receiver<SinkRequest>) -> Vec<Response> {
        let mut responses: Vec<Response> = Vec::new();

        while let Some(datum) = input.recv().await {
            // do something better, but for now let's just log it.
            // please note that `from_utf8` is working because the input in this
            // example uses utf-8 data.
            let response = match std::str::from_utf8(&datum.value) {
                Ok(v) => {
                    println!("{}", v);
                    // record the response
                    Response::ok(datum.id)
                }
                Err(e) => Response::failure(datum.id, format!("Invalid UTF-8 sequence: {}", e)),
            };

            // return the responses
            responses.push(response);
        }

        responses
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use numaflow::sink::Sinker;
    use tokio::sync::mpsc;

    fn create_sink_request(id: &str, value: Vec<u8>, keys: Vec<String>) -> SinkRequest {
        SinkRequest {
            id: id.to_string(),
            keys,
            value,
            watermark: std::time::SystemTime::now().into(),
            event_time: std::time::SystemTime::now().into(),
            headers: Default::default(),
        }
    }

    #[tokio::test]
    async fn test_sink_single_valid_utf8_message() {
        let logger = Logger;
        let (tx, rx) = mpsc::channel(10);

        let request = create_sink_request(
            "msg-1",
            b"Hello, Numaflow!".to_vec(),
            vec!["key1".to_string()],
        );
        tx.send(request).await.unwrap();
        drop(tx);

        let responses = logger.sink(rx).await;

        assert_eq!(responses.len(), 1);
        assert!(responses[0].success, "Valid UTF-8 should succeed");
        assert_eq!(responses[0].id, "msg-1");
        assert!(responses[0].err.is_empty(), "No error expected");
    }

    #[tokio::test]
    async fn test_sink_multiple_messages() {
        let logger = Logger;
        let (tx, rx) = mpsc::channel(10);

        for i in 0..5 {
            let request = create_sink_request(
                &format!("msg-{}", i),
                format!("Message {}", i).into_bytes(),
                vec![format!("key-{}", i)],
            );
            tx.send(request).await.unwrap();
        }
        drop(tx);

        let responses = logger.sink(rx).await;

        assert_eq!(responses.len(), 5);
        for (i, response) in responses.iter().enumerate() {
            assert!(response.success, "All valid UTF-8 messages should succeed");
            assert_eq!(response.id, format!("msg-{}", i));
        }
    }

    #[tokio::test]
    async fn test_sink_invalid_utf8_message() {
        let logger = Logger;
        let (tx, rx) = mpsc::channel(10);

        // Invalid UTF-8 sequence
        let invalid_utf8 = vec![0xff, 0xfe, 0x00, 0x01];
        let request = create_sink_request("msg-invalid", invalid_utf8, vec!["key1".to_string()]);
        tx.send(request).await.unwrap();
        drop(tx);

        let responses = logger.sink(rx).await;

        assert_eq!(responses.len(), 1);
        assert!(!responses[0].success, "Invalid UTF-8 should fail");
        assert_eq!(responses[0].id, "msg-invalid");
        assert!(
            responses[0].err.contains("Invalid UTF-8"),
            "Error message should mention UTF-8"
        );
    }

    #[tokio::test]
    async fn test_sink_empty_input() {
        let logger = Logger;
        let (tx, rx) = mpsc::channel(10);
        drop(tx);

        let responses = logger.sink(rx).await;

        assert_eq!(responses.len(), 0, "No responses for empty input");
    }

    #[tokio::test]
    async fn test_sink_empty_value() {
        let logger = Logger;
        let (tx, rx) = mpsc::channel(10);

        let request = create_sink_request("msg-empty", vec![], vec!["key1".to_string()]);
        tx.send(request).await.unwrap();
        drop(tx);

        let responses = logger.sink(rx).await;

        assert_eq!(responses.len(), 1);
        assert!(responses[0].success, "Empty value is valid UTF-8");
        assert_eq!(responses[0].id, "msg-empty");
    }
}
