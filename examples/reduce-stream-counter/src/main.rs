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
