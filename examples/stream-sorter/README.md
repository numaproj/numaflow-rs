# Stream Sorter Accumulator Example

This example demonstrates a stream sorter accumulator that buffers incoming unordered streams (unordered by event-time
but honors watermark) and emits sorted results when the watermark progresses.