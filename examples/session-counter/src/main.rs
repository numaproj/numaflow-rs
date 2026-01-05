use numaflow::session_reduce;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let handler_creator = counter::CounterCreator {};
    session_reduce::Server::new(handler_creator).start().await?;
    Ok(())
}

mod counter {
    use numaflow::session_reduce::{Message, SessionReduceRequest, SessionReducer};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::sync::mpsc;
    use tonic::async_trait;

    pub(crate) struct Counter {
        count: Arc<AtomicU32>,
    }

    pub(crate) struct CounterCreator {}

    impl numaflow::session_reduce::SessionReducerCreator for CounterCreator {
        type R = Counter;

        fn create(&self) -> Self::R {
            Counter::new()
        }
    }

    impl Counter {
        pub(crate) fn new() -> Self {
            Self {
                count: Arc::new(AtomicU32::new(0)),
            }
        }
    }

    #[async_trait]
    impl SessionReducer for Counter {
        async fn session_reduce(
            &self,
            keys: Vec<String>,
            mut input: mpsc::Receiver<SessionReduceRequest>,
            output: mpsc::Sender<Message>,
        ) {
            // Count all incoming messages in this session
            while input.recv().await.is_some() {
                self.count.fetch_add(1, Ordering::Relaxed);
            }

            // Send the current count as the result
            let count_value = self.count.load(Ordering::Relaxed);
            let message = Message::new(count_value.to_string().into_bytes()).with_keys(keys);

            if let Err(e) = output.send(message).await {
                eprintln!("Failed to send message: {}", e);
            }
        }

        async fn accumulator(&self) -> Vec<u8> {
            // Return the current count as bytes for accumulator
            let count = self.count.load(Ordering::Relaxed);
            count.to_string().into_bytes()
        }

        async fn merge_accumulator(&self, accumulator: Vec<u8>) {
            // Parse the accumulator value and add it to our count
            if let Ok(accumulator_str) = String::from_utf8(accumulator) {
                if let Ok(accumulator_count) = accumulator_str.parse::<u32>() {
                    self.count.fetch_add(accumulator_count, Ordering::Relaxed);
                } else {
                    eprintln!("Failed to parse accumulator value: {}", accumulator_str);
                }
            } else {
                eprintln!("Failed to convert accumulator bytes to string");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::counter::Counter;
    use numaflow::session_reduce::{SessionReduceRequest, SessionReducer};
    use tokio::sync::mpsc;
    fn create_request(value: Vec<u8>, keys: Vec<String>) -> SessionReduceRequest {
        SessionReduceRequest {
            keys,
            value,
            watermark: std::time::SystemTime::now().into(),
            event_time: std::time::SystemTime::now().into(),
            headers: Default::default(),
        }
    }
    #[tokio::test]
    async fn test_counter() {
        let counter = Counter::new();
        let (input_tx, input_rx) = mpsc::channel(10);
        let (output_tx, mut output_rx) = mpsc::channel(10);
        let keys = vec!["user123".to_string()];
        let mut reducer = counter;
        let reducer_task = tokio::spawn(async move {
            reducer.session_reduce(keys, input_rx, output_tx).await;
        });
        let messages = vec![
            create_request(b"a".to_vec(), vec!["user123".to_string()]),
            create_request(b"b".to_vec(), vec!["user123".to_string()]),
            create_request(b"c".to_vec(), vec!["user123".to_string()]),
            create_request(b"d".to_vec(), vec!["user123".to_string()]),
        ];
        for msg in messages {
            input_tx.send(msg).await.unwrap();
        }
        drop(input_tx);
        reducer_task.await.unwrap();
        let mut results = Vec::new();
        while let Some(msg) = output_rx.recv().await {
            results.push(msg);
        }
        assert_eq!(results.len(), 1);
        let output = String::from_utf8(results[0].value.clone()).unwrap();
        assert_eq!(output, "4");
    }
}
