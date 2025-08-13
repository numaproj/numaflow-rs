use chrono::{DateTime, Utc};
use numaflow::accumulator::{Accumulator, AccumulatorCreator, AccumulatorRequest, Message};
use tokio::sync::mpsc;
use tonic::async_trait;
use tracing::{info, warn};

/// StreamSorter buffers the incoming unordered (unordered by event-time but honors watermark) stream(s) and stores
/// in the sorted buffer.
struct StreamSorter;

impl StreamSorter {
    fn new() -> Self {
        Self
    }
}

/// insert_sorted will do a binary-search and inserts the AccumulatorRequest into the sorted buffer.
fn insert_sorted(sorted_buffer: &mut Vec<AccumulatorRequest>, request: AccumulatorRequest) {
    let event_time = request.event_time;

    // Find the insertion point using binary search
    let index = sorted_buffer
        .binary_search_by(|probe| probe.event_time.cmp(&event_time))
        .unwrap_or_else(|e| e);

    sorted_buffer.insert(index, request);
}

/// flush_buffer flushes the sorted results from the buffer into the outbound stream. While flushing it will
/// truncate the buffer up till <= (current watermark - 1).
async fn flush_buffer(
    sorted_buffer: &mut Vec<AccumulatorRequest>,
    latest_wm: DateTime<Utc>,
    output: &mpsc::Sender<Message>,
) {
    info!(
        "Watermark updated, flushing sorted_buffer: {}",
        latest_wm.timestamp_millis()
    );

    let mut flush_count = 0;

    for (i, request) in sorted_buffer.iter().enumerate() {
        if request.event_time > latest_wm {
            break;
        }

        let message = Message::from_accumulator_request(request.clone());
        if let Err(e) = output.send(message).await {
            warn!("Failed to send message: {}", e);
            break;
        }

        info!(
            "Sent datum with event time: {}",
            request.event_time.timestamp_millis()
        );
        flush_count = i + 1;
    }

    // Truncate the buffer to remove flushed elements
    sorted_buffer.drain(0..flush_count);
}

#[async_trait]
impl Accumulator for StreamSorter {
    /// Accumulate accumulates the read data in the sorted buffer and will emit the sorted results whenever the watermark
    /// progresses.
    async fn accumulate(
        &self,
        mut input: mpsc::Receiver<AccumulatorRequest>,
        output: mpsc::Sender<Message>,
    ) {
        // Local state for this accumulation session
        let mut latest_wm = DateTime::from_timestamp(-1, 0).unwrap_or_else(Utc::now);
        let mut sorted_buffer = Vec::new();

        while let Some(request) = input.recv().await {
            info!(
                "Received datum with event time: {}",
                request.event_time.timestamp_millis()
            );

            // Check if watermark has moved, let's flush
            if request.watermark > latest_wm {
                latest_wm = request.watermark;
                flush_buffer(&mut sorted_buffer, latest_wm, &output).await;
            }

            // Store the data into the internal buffer
            insert_sorted(&mut sorted_buffer, request);
        }

        info!("Input channel closed, exiting accumulator");
    }
}

/// StreamSorterCreator creates a StreamSorter for every key.
struct StreamSorterCreator;

impl AccumulatorCreator for StreamSorterCreator {
    type A = StreamSorter;

    /// Create creates an Accumulator for every key. It will be closed only when the timeout has expired.
    fn create(&self) -> Self::A {
        StreamSorter::new()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting stream-sorter accumulator server");

    let creator = StreamSorterCreator;
    numaflow::accumulator::Server::new(creator).start().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_insert_sorted() {
        let mut sorted_buffer = Vec::new();

        // Create test requests with different event times
        let req1 = AccumulatorRequest {
            keys: vec!["key1".to_string()],
            value: b"data1".to_vec(),
            watermark: DateTime::from_timestamp(1000, 0).unwrap(),
            event_time: DateTime::from_timestamp(100, 0).unwrap(),
            headers: HashMap::new(),
            id: "1".to_string(),
        };

        let req2 = AccumulatorRequest {
            keys: vec!["key1".to_string()],
            value: b"data2".to_vec(),
            watermark: DateTime::from_timestamp(1000, 0).unwrap(),
            event_time: DateTime::from_timestamp(50, 0).unwrap(),
            headers: HashMap::new(),
            id: "2".to_string(),
        };

        let req3 = AccumulatorRequest {
            keys: vec!["key1".to_string()],
            value: b"data3".to_vec(),
            watermark: DateTime::from_timestamp(1000, 0).unwrap(),
            event_time: DateTime::from_timestamp(150, 0).unwrap(),
            headers: HashMap::new(),
            id: "3".to_string(),
        };

        // Insert in non-sorted order
        insert_sorted(&mut sorted_buffer, req1);
        insert_sorted(&mut sorted_buffer, req2);
        insert_sorted(&mut sorted_buffer, req3);

        // Verify they are sorted by event time
        assert_eq!(sorted_buffer.len(), 3);
        assert_eq!(sorted_buffer[0].event_time.timestamp(), 50);
        assert_eq!(sorted_buffer[1].event_time.timestamp(), 100);
        assert_eq!(sorted_buffer[2].event_time.timestamp(), 150);

        // Verify the data is correct
        assert_eq!(sorted_buffer[0].value, b"data2");
        assert_eq!(sorted_buffer[1].value, b"data1");
        assert_eq!(sorted_buffer[2].value, b"data3");
    }
}
