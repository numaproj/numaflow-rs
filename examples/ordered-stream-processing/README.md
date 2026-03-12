# Ordered Stream Processing

## Temporal Ordering vs Spatial Ordering

### Temporal Ordering (Stream Sorter / Accumulator)

Events often arrive out of order with respect to their event-time. For example, an event with event-time `t=100` may 
arrive after an event with event-time `t=200` due to network delays, multi-source ingestion, or upstream processing latencies.

The **stream-sorter accumulator** solves this by buffering incoming events and emitting them sorted by event-time as the 
watermark advances. It answers: *"Given events that arrived in arbitrary order, can I re-emit them in event-time order?"*

### Spatial Ordering (Ordered Processing)

Even after events are temporally sorted, they can become spatially disordered when a downstream vertex has multiple partitions. 
By default, Numaflow distributes messages across partitions for throughput, and each partition processes independently — 
so two messages emitted in order by the sorter may be processed out of order if they land on different partitions that run at different speeds.

**Ordered processing** (`spec.ordered.enabled: true`) solves this by enforcing partitioned FIFO semantics: messages are 
routed to partitions by key hash, and within each partition, the N-th message is processed only after the (N-1)-th completes. 
It answers: *"Given events that are already in order, can I guarantee they stay in order throughout the downstream processing?"*

### Why Both Are Needed

| Without stream-sorter | Without ordered processing |
|---|---|
| Events arrive out of event-time order | Events may be reordered across partitions |
| Downstream sees `t=200` before `t=100` | Partition 1 may process `t=200` before partition 2 finishes `t=100` |

Combining both gives an end-to-end guarantee: events are first sorted by event-time (temporal), then processed in that order 
through all downstream vertices (spatial).

## Pipeline Architecture

```
input-one (HTTP) ──┐
                    ├──► sorter (accumulator) ──► order-checker (map, 3 partitions) ──► out (log sink, 3 partitions)
input-two (HTTP) ──┘
```

- **input-one, input-two**: HTTP sources. Send events with custom event-times using the `x-numaflow-event-time` header 
(value in epoch milliseconds). Idle watermark is configured so the pipeline progresses even when sources stop receiving data.
- **sorter**: Re-uses the `stream-sorter` accumulator to buffer and emit events sorted by event-time.
- **order-checker**: A map vertex that tracks the last-seen event-time per key and logs whether each arriving event maintains 
non-decreasing order. This is where you can observe the effect of ordered processing.
- **out**: A log sink.

## Sending Test Data

Once the pipeline is running, send events with explicit event-times to either HTTP source:

```bash
# Send events out of temporal order to input-one
curl -kq -X POST -H "x-numaflow-event-time: 1700000300000" -H "x-numaflow-keys: A" -d '{"seq":3}' https://<input-one-url>
curl -kq -X POST -H "x-numaflow-event-time: 1700000100000" -H "x-numaflow-keys: A" -d '{"seq":1}' https://<input-one-url>
curl -kq -X POST -H "x-numaflow-event-time: 1700000200000" -H "x-numaflow-keys: A" -d '{"seq":2}' https://<input-one-url>
```

The stream-sorter will buffer these and emit them as `seq:1, seq:2, seq:3` once the watermark advances. 
The order-checker will then confirm they arrive in non-decreasing event-time order, logging `"Order maintained"` for each. 
If ordering were broken, you would see `"Order violation detected"` warnings in the order-checker logs.
