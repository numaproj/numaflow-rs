use numaflow::accumulator::{Accumulator, AccumulatorCreator, AccumulatorRequest, Message};
use tokio::sync::mpsc;
use tonic::async_trait;
use tracing::{info, warn};

/// Blackhole is an accumulator that intentionally discards every datum it
/// receives without forwarding any data downstream.
///
/// A naive implementation may simply read the input stream and emit nothing.
/// However, an accumulator that never emits anything for the datums it consumes
/// leaves the framework unable to release the per-datum tracking state, leading
/// to unbounded memory growth (see numaflow-python#356).
///
/// Instead, this example emits a *drop* message for every datum using
/// [`Message::message_to_drop`]. A drop message is not forwarded to the next
/// vertex, but it still allows the framework to advance the watermark and
/// release the tracked state for that datum — giving us "blackhole" semantics
/// without leaking memory.
struct Blackhole;

impl Blackhole {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Accumulator for Blackhole {
    /// Accumulate drains the input stream and drops every datum, emitting a drop
    /// message so the framework can release the datum's tracked state.
    async fn accumulate(
        &self,
        mut input: mpsc::Receiver<AccumulatorRequest>,
        output: mpsc::Sender<Message>,
    ) {
        while let Some(request) = input.recv().await {
            info!(
                "Dropping datum with event time: {}, watermark: {}",
                request.event_time.timestamp_millis(),
                request.watermark.timestamp_millis()
            );

            // Emit a drop message: nothing is forwarded downstream, but the
            // framework still advances the watermark and releases the tracked
            // state for this datum.
            let message = Message::message_to_drop(request);
            if let Err(e) = output.send(message).await {
                warn!("Failed to send drop message: {}", e);
                break;
            }
        }

        info!("Input channel closed, exiting accumulator");
    }
}

/// BlackholeCreator creates a Blackhole accumulator for every key.
struct BlackholeCreator;

impl AccumulatorCreator for BlackholeCreator {
    type A = Blackhole;

    /// Create creates an Accumulator for every key. It will be closed only when the timeout has expired.
    fn create(&self) -> Self::A {
        Blackhole::new()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting accumulator-blackhole server");

    let creator = BlackholeCreator;
    numaflow::accumulator::Server::new(creator).start().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::DateTime;
    use numaflow::shared::DROP;
    use std::collections::HashMap;

    fn make_request(id: &str, event_time: i64) -> AccumulatorRequest {
        AccumulatorRequest {
            keys: vec!["key1".to_string()],
            value: b"data".to_vec(),
            watermark: DateTime::from_timestamp(event_time, 0).unwrap(),
            event_time: DateTime::from_timestamp(event_time, 0).unwrap(),
            headers: HashMap::new(),
            id: id.to_string(),
        }
    }

    #[tokio::test]
    async fn test_blackhole_drops_every_datum() {
        let (input_tx, input_rx) = mpsc::channel(10);
        let (output_tx, mut output_rx) = mpsc::channel(10);

        // Run the accumulator in the background.
        let handle = tokio::spawn(async move {
            Blackhole::new().accumulate(input_rx, output_tx).await;
        });

        // Feed a few datums, then close the input stream.
        let requests = vec![
            make_request("1", 100),
            make_request("2", 50),
            make_request("3", 150),
        ];
        for request in &requests {
            input_tx.send(request.clone()).await.unwrap();
        }
        drop(input_tx);

        // Collect everything the accumulator emitted.
        let mut emitted = Vec::new();
        while let Some(message) = output_rx.recv().await {
            emitted.push(message);
        }
        handle.await.unwrap();

        // Every input datum must produce exactly one drop message so the
        // framework can release its tracked state.
        assert_eq!(emitted.len(), requests.len());
        for (message, request) in emitted.iter().zip(requests.iter()) {
            // Tagged for drop so it is not forwarded downstream.
            assert_eq!(message.tags(), &Some(vec![DROP.to_string()]));
            // No value is forwarded, but identifying/watermark fields are preserved.
            assert!(message.value().is_empty());
            assert_eq!(message.id(), request.id);
            assert_eq!(message.event_time(), request.event_time);
            assert_eq!(message.watermark(), request.watermark);
        }
    }
}
