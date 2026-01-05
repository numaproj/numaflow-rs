use numaflow::reducestream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let handler_creator = streaming_counter::StreamingCounterCreator {};
    reducestream::Server::new(handler_creator).start().await?;
    Ok(())
}

mod streaming_counter {
    use numaflow::reducestream::{Message, ReduceStreamRequest};
    use numaflow::reducestream::{Metadata, ReduceStreamer};
    use tokio::sync::mpsc::{Receiver, Sender};
    use tonic::async_trait;

    pub(crate) struct StreamingCounter {}

    pub(crate) struct StreamingCounterCreator {}

    impl numaflow::reducestream::ReduceStreamerCreator for StreamingCounterCreator {
        type R = StreamingCounter;

        fn create(&self) -> Self::R {
            StreamingCounter::new()
        }
    }

    impl StreamingCounter {
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    #[async_trait]
    impl ReduceStreamer for StreamingCounter {
        async fn reducestream(
            &self,
            keys: Vec<String>,
            mut input: Receiver<ReduceStreamRequest>,
            output: Sender<Message>,
            _md: &Metadata,
        ) {
            let mut counter = 0;
            // Stream intermediate results every 10 messages
            while input.recv().await.is_some() {
                counter += 1;

                // Emit intermediate count every 10 messages
                if counter % 10 == 0 {
                    let message = Message::new(format!("intermediate: {}", counter).into_bytes())
                        .with_keys(keys.clone());

                    if output.send(message).await.is_err() {
                        // Client disconnected, stop processing
                        return;
                    }
                }
            }

            // Send final count when the window closes
            let final_message =
                Message::new(format!("final: {}", counter).into_bytes()).with_keys(keys.clone());

            let _ = output.send(final_message).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use numaflow::reducestream::{IntervalWindow, Metadata, ReduceStreamRequest, ReduceStreamer};
    use std::collections::HashMap;
    use tokio::sync::mpsc;

    use super::streaming_counter::StreamingCounter;

    fn create_request(keys: Vec<String>, value: Vec<u8>) -> ReduceStreamRequest {
        ReduceStreamRequest {
            keys,
            value,
            watermark: chrono::Utc::now(),
            eventtime: chrono::Utc::now(),
            headers: HashMap::new(),
        }
    }

    fn create_metadata() -> Metadata {
        Metadata::new(IntervalWindow::default())
    }

    #[tokio::test]
    async fn test_reducestream_final_count() {
        let counter = StreamingCounter::new();
        let (input_tx, input_rx) = mpsc::channel(10);
        let (output_tx, mut output_rx) = mpsc::channel(10);

        let keys = vec!["key1".to_string()];
        let md = create_metadata();

        // Send 5 messages (less than 10, so no intermediate)
        for i in 0..5 {
            let request = create_request(keys.clone(), format!("msg-{}", i).into_bytes());
            input_tx.send(request).await.unwrap();
        }
        drop(input_tx);

        counter.reducestream(keys.clone(), input_rx, output_tx, &md).await;

        let mut messages = Vec::new();
        while let Some(msg) = output_rx.recv().await {
            messages.push(msg);
        }

        // Should only have the final message
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, b"final: 5");
        assert_eq!(messages[0].keys, Some(vec!["key1".to_string()]));
    }

    #[tokio::test]
    async fn test_reducestream_intermediate_and_final() {
        let counter = StreamingCounter::new();
        let (input_tx, input_rx) = mpsc::channel(30);
        let (output_tx, mut output_rx) = mpsc::channel(30);

        let keys = vec!["key1".to_string()];
        let md = create_metadata();

        // Send 25 messages (should trigger 2 intermediate at 10 and 20)
        for i in 0..25 {
            let request = create_request(keys.clone(), format!("msg-{}", i).into_bytes());
            input_tx.send(request).await.unwrap();
        }
        drop(input_tx);

        counter.reducestream(keys.clone(), input_rx, output_tx, &md).await;

        let mut messages = Vec::new();
        while let Some(msg) = output_rx.recv().await {
            messages.push(msg);
        }

        // Should have 2 intermediate + 1 final = 3 messages
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].value, b"intermediate: 10");
        assert_eq!(messages[1].value, b"intermediate: 20");
        assert_eq!(messages[2].value, b"final: 25");
    }

    #[tokio::test]
    async fn test_reducestream_empty_input() {
        let counter = StreamingCounter::new();
        let (input_tx, input_rx) = mpsc::channel(10);
        let (output_tx, mut output_rx) = mpsc::channel(10);

        let keys = vec!["key1".to_string()];
        let md = create_metadata();

        drop(input_tx); // No messages sent

        counter.reducestream(keys.clone(), input_rx, output_tx, &md).await;

        let mut messages = Vec::new();
        while let Some(msg) = output_rx.recv().await {
            messages.push(msg);
        }

        // Should have final message with count 0
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, b"final: 0");
    }

    #[tokio::test]
    async fn test_reducestream_exactly_ten_messages() {
        let counter = StreamingCounter::new();
        let (input_tx, input_rx) = mpsc::channel(20);
        let (output_tx, mut output_rx) = mpsc::channel(10);

        let keys = vec!["key1".to_string()];
        let md = create_metadata();

        // Send exactly 10 messages
        for i in 0..10 {
            let request = create_request(keys.clone(), format!("msg-{}", i).into_bytes());
            input_tx.send(request).await.unwrap();
        }
        drop(input_tx);

        counter.reducestream(keys.clone(), input_rx, output_tx, &md).await;

        let mut messages = Vec::new();
        while let Some(msg) = output_rx.recv().await {
            messages.push(msg);
        }

        // Should have 1 intermediate + 1 final = 2 messages
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].value, b"intermediate: 10");
        assert_eq!(messages[1].value, b"final: 10");
    }

    #[tokio::test]
    async fn test_reducestream_preserves_keys() {
        let counter = StreamingCounter::new();
        let (input_tx, input_rx) = mpsc::channel(10);
        let (output_tx, mut output_rx) = mpsc::channel(10);

        let keys = vec!["key1".to_string(), "key2".to_string()];
        let md = create_metadata();

        let request = create_request(keys.clone(), b"msg".to_vec());
        input_tx.send(request).await.unwrap();
        drop(input_tx);

        counter.reducestream(keys.clone(), input_rx, output_tx, &md).await;

        let msg = output_rx.recv().await.unwrap();
        assert_eq!(msg.keys, Some(vec!["key1".to_string(), "key2".to_string()]));
    }
}
