use chrono::{DateTime, Utc};
use numaflow::accumulator::{Accumulator, AccumulatorCreator, AccumulatorRequest, Message};
use tokio::sync::mpsc;
use tonic::async_trait;
use tracing::{info, warn};

/// Stream Sorter Accumulator Example
///
/// This example demonstrates the new Message API that follows the Go implementation pattern:
///
/// 1. Messages are created from AccumulatorRequest using `Message::from_datum()`
/// 2. Headers, event_time, watermark, and id are read-only (preserved from input datum)
/// 3. Keys, value, and tags can be modified using builder methods
/// 4. When flushing sorted data, all original metadata is preserved automatically

/// StreamSorter buffers the incoming unordered (unordered by event-time but honors watermark) stream(s) and stores
/// in the sorted buffer.
struct StreamSorter {
    latest_wm: DateTime<Utc>,
    /// sorted_buffer stores the sorted data it has seen so far. The data stored will be completely sorted for
    /// (current watermark - 1) because it honors watermark.
    sorted_buffer: Vec<AccumulatorRequest>,
}

impl StreamSorter {
    fn new() -> Self {
        Self {
            latest_wm: DateTime::from_timestamp(-1, 0).unwrap_or_else(Utc::now),
            sorted_buffer: Vec::new(),
        }
    }

    /// insert_sorted will do a binary-search and inserts the AccumulatorRequest into the internal sorted buffer.
    fn insert_sorted(&mut self, request: AccumulatorRequest) {
        let event_time = request.event_time;

        // Find the insertion point using binary search
        let index = self
            .sorted_buffer
            .binary_search_by(|probe| probe.event_time.cmp(&event_time))
            .unwrap_or_else(|e| e);

        self.sorted_buffer.insert(index, request);
    }

    /// flush_buffer flushes the sorted results from the buffer into the outbound stream. While flushing it will
    /// truncate the buffer up till <= (current watermark - 1).
    async fn flush_buffer(&mut self, output: &mpsc::Sender<Message>) {
        info!(
            "Watermark updated, flushing sorted_buffer: {}",
            self.latest_wm.timestamp_millis()
        );

        let mut flush_count = 0;

        for (i, request) in self.sorted_buffer.iter().enumerate() {
            if request.event_time > self.latest_wm {
                break;
            }

            // Create message from the request datum - this preserves all the original metadata
            // like headers, event_time, watermark, and id from the input.
            // This follows the Go implementation pattern where Message::from_datum() is the only
            // way to create a Message, ensuring metadata consistency.
            let message = Message::from_datum(request.clone());

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
        self.sorted_buffer.drain(0..flush_count);
    }
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
        // Since we need to mutate self, we need to work around the &self constraint
        // by creating our own mutable state
        let mut sorter = StreamSorter::new();

        while let Some(request) = input.recv().await {
            info!(
                "Received datum with event time: {}",
                request.event_time.timestamp_millis()
            );

            // Check if watermark has moved, let's flush
            if request.watermark > sorter.latest_wm {
                sorter.latest_wm = request.watermark;
                sorter.flush_buffer(&output).await;
            }

            // Store the data into the internal buffer
            sorter.insert_sorted(request);
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
        let mut sorter = StreamSorter::new();

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
        sorter.insert_sorted(req1);
        sorter.insert_sorted(req2);
        sorter.insert_sorted(req3);

        // Verify they are sorted by event time
        assert_eq!(sorter.sorted_buffer.len(), 3);
        assert_eq!(sorter.sorted_buffer[0].event_time.timestamp(), 50);
        assert_eq!(sorter.sorted_buffer[1].event_time.timestamp(), 100);
        assert_eq!(sorter.sorted_buffer[2].event_time.timestamp(), 150);

        // Verify the data is correct
        assert_eq!(sorter.sorted_buffer[0].value, b"data2");
        assert_eq!(sorter.sorted_buffer[1].value, b"data1");
        assert_eq!(sorter.sorted_buffer[2].value, b"data3");
    }
}
