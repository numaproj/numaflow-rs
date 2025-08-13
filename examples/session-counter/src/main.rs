use numaflow::session_reduce;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let handler_creator = counter::CounterCreator {};
    session_reduce::Server::new(handler_creator).start().await?;
    Ok(())
}

mod counter {
    use numaflow::session_reduce::{Message, SessionReduceRequest, SessionReducer};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
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
