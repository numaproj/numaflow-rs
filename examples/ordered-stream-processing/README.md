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

## Key-Partition Tracking (Optional)

The order-checker can optionally track which replica processes each message key using Redis. This proves that Numaflow's key-hash routing is consistent — every message with a given key always lands on the same partition/replica.

### How It Works

When enabled, the order-checker uses a Redis HASH (`numaflow:key_partition_map`) where each field is a serialized message key and the value is the replica ID that first claimed it. On the first encounter of each key, it uses `HSETNX` (atomic set-if-not-exists):

- If the key is new, it is claimed by the current replica
- If the key already exists and belongs to a different replica, a `KEY ROUTING VIOLATION` error is logged

A background task periodically logs a summary of key distribution across replicas.

### Environment Variables

| Variable                           | Default                      | Description                                                       |
|------------------------------------|------------------------------|-------------------------------------------------------------------|
| `ENABLE_KEY_TRACKING`              | (disabled)                   | Set to `"true"` to enable Redis-based key tracking                |
| `REDIS_URL`                        | `redis://redis:6379`         | Redis connection URL                                              |
| `KEY_TRACKING_HASH`                | `numaflow:key_partition_map` | Redis HASH key name                                               |
| `KEY_TRACKING_CHECK_INTERVAL_SECS` | `30`                         | Interval (seconds) between periodic key distribution summary logs |
| `NUMAFLOW_REPLICA`                 | `"unknown"`                  | Replica identifier (set automatically by Numaflow)                |

### What to Look For in Logs

- `"Key-partition tracking enabled"` — confirms tracking is active
- `"Key partition tracking summary"` — periodic summary showing total keys and replica count
- `"Replica key distribution"` — per-replica key count breakdown
- Absence of `"KEY ROUTING VIOLATION"` — proves consistent key-to-partition routing
