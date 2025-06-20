# Stream Sorter Accumulator Example

This example demonstrates a stream sorter accumulator that buffers incoming unordered streams (unordered by event-time but honors watermark) and emits sorted results when the watermark progresses.

## Overview

The stream sorter accumulator:

1. **Buffers incoming data**: Stores incoming `AccumulatorRequest` messages in a sorted buffer ordered by event time
2. **Tracks watermark progression**: Monitors watermark advancement to determine when to flush sorted data
3. **Emits sorted results**: When the watermark progresses, flushes all data with event time ≤ current watermark in sorted order
4. **Uses binary search**: Efficiently inserts new data into the sorted buffer using binary search

## Key Components

### StreamSorter

The main accumulator implementation that:
- Maintains a `sorted_buffer` of `AccumulatorRequest` messages sorted by event time
- Tracks the `latest_wm` (latest watermark) seen
- Implements the `Accumulator` trait with the `accumulate` method

### StreamSorterCreator

Implements the `AccumulatorCreator` trait to create new `StreamSorter` instances for each key.

## Algorithm

1. **Receive data**: For each incoming `AccumulatorRequest`:
   - Check if the watermark has advanced
   - If watermark advanced, flush all data ≤ new watermark in sorted order
   - Insert the new data into the sorted buffer using binary search

2. **Binary search insertion**: Uses Rust's `binary_search_by` to find the correct insertion point and maintains sorted order

3. **Watermark-based flushing**: Only emits data when watermark progresses, ensuring proper ordering guarantees

## Building and Running

### Build the example:
```bash
cargo build --release --bin stream-sorter
```

### Build Docker image:
```bash
make image TAG=latest
```

### Run with Numaflow:
```bash
kubectl apply -f manifests/stream-sorter-pipeline.yaml
```

## Configuration

The example pipeline uses:
- **Generator source**: Produces test data with some jitter for realistic unordered streams
- **Session window**: Allows accumulation over time with a 30s timeout
- **Keyed grouping**: Sorts data per key independently
- **Log sink**: Outputs the sorted results

## Message API

This example demonstrates the new Message API that follows the Go implementation pattern:

1. **Message creation**: Messages are created from `AccumulatorRequest` using `Message::from_datum()`
2. **Read-only metadata**: Headers, event_time, watermark, and id are preserved from the input datum
3. **Mutable fields**: Keys, value, and tags can be modified using builder methods
4. **Metadata preservation**: When flushing sorted data, all original metadata is preserved

## Comparison with Go Implementation

This Rust implementation mirrors the Go version with these key differences:

1. **Memory management**: Rust's ownership system eliminates the need for manual memory management
2. **Type safety**: Compile-time guarantees prevent common runtime errors
3. **Async/await**: Uses Rust's async ecosystem with tokio for concurrent processing
4. **Error handling**: Uses Result types for explicit error handling
5. **Message API**: Follows the same pattern as Go with immutable metadata fields and mutable data fields

## Testing

The implementation includes comprehensive logging to observe:
- Incoming data with event times
- Watermark progression
- Data flushing and sorting behavior
- Buffer management

Enable debug logging by setting `NUMAFLOW_DEBUG=true` in the container environment.
