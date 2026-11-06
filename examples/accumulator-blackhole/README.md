# Blackhole Accumulator Example

This example demonstrates a "blackhole" accumulator that intentionally discards
every datum it receives without forwarding any data to the next vertex.

## Why emit drop messages instead of nothing?

An accumulator that simply reads its input and emits nothing leaves the
framework unable to release the per-datum tracking state, which leads to
unbounded memory growth (see [numaflow-python#356](https://github.com/numaproj/numaflow-python/issues/356)).

To get "blackhole" semantics without leaking memory, this example emits a *drop*
message for every datum using `Message::message_to_drop`. A drop message is not
forwarded downstream, but it still lets the framework advance the watermark and
release the tracked state for that datum.

This pattern is useful for multiplexer-, cross-join-, or filter-style
accumulators that legitimately need to omit some (or all) of their inputs.
