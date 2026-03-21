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
| `NUMAFLOW_REPLICA`                 | `"unknown"`                  | Replica identifier (set automatically by Numaflow)                |

### What to Look For in Logs

- `"Key-partition tracking enabled"` — confirms tracking is active
- `"Key partition tracking summary"` — periodic summary showing total keys and replica count
- `"Replica key distribution"` — per-replica key count breakdown
- Absence of `"KEY ROUTING VIOLATION"` — proves consistent key-to-partition routing

## Example Run Walkthrough

> Based on [this PR comment](https://github.com/numaproj/numaflow-rs/pull/168#issuecomment-4069231805).

Below is a concrete example showing how events sent **out of order** across two HTTP sources are **sorted by event-time** and then **delivered in order per key** to separate sink partitions.

### 1. Events Sent

25 events are sent in 7 batches across `input-one` (port 8444) and `input-two` (port 8445). Each event carries a key (`A` or `Z`) and an event-time. Events are intentionally sent **out of temporal order** — for example, events at `16:53:08` are sent before events at `16:52:58`.

| Batch | Source    | Key | Event Time (UTC) | Epoch (ms)    |
|-------|-----------|-----|------------------|---------------|
| 1     | input-one | A   | 16:53:08.000     | 1773679988000 |
| 1     | input-one | Z   | 16:53:08.003     | 1773679988003 |
| 1     | input-two | A   | 16:53:08.001     | 1773679988001 |
| 1     | input-two | Z   | 16:53:08.002     | 1773679988002 |
| 2     | input-one | A   | 16:52:58.000     | 1773679978000 |
| 2     | input-one | Z   | 16:52:58.003     | 1773679978003 |
| 2     | input-two | A   | 16:52:58.002     | 1773679978002 |
| 2     | input-two | Z   | 16:52:58.001     | 1773679978001 |
| 3     | input-one | A   | 16:53:18.003     | 1773679998003 |
| 3     | input-one | Z   | 16:53:18.000     | 1773679998000 |
| 3     | input-two | A   | 16:53:18.002     | 1773679998002 |
| 3     | input-two | Z   | 16:53:18.001     | 1773679998001 |
| 4     | input-one | Z   | 16:53:19.000     | 1773679999000 |
| 4     | input-one | A   | 16:53:19.003     | 1773679999003 |
| 4     | input-two | Z   | 16:53:19.002     | 1773679999002 |
| 4     | input-two | A   | 16:53:19.001     | 1773679999001 |
| 5     | input-one | Z   | 16:54:10.003     | 1773680050003 |
| 5     | input-one | A   | 16:54:10.002     | 1773680050002 |
| 5     | input-two | Z   | 16:54:10.001     | 1773680050001 |
| 5     | input-two | A   | 16:54:10.000     | 1773680050000 |
| 6     | input-one | Z   | 17:04:10.000     | 1773680650000 |
| 6     | input-one | A   | 17:04:10.001     | 1773680650001 |
| 6     | input-two | Z   | 17:04:10.002     | 1773680650002 |
| 6     | input-two | A   | 17:04:10.003     | 1773680650003 |
| 7     | input-two | Z   | 19:50:50.003     | 1773690650003 |

Notice that Batch 2 has **earlier** event-times than Batch 1 — this simulates real-world out-of-order arrival.

### 2. Stream Sorter Behavior

The stream-sorter accumulator buffers incoming events and flushes them **in event-time order** as the watermark advances. Here is a simplified view of how it processes the events:

**Receiving phase** — events arrive in send order (not event-time order):
```
Received: 16:53:08.000 (A)  ← Batch 1 arrives first
Received: 16:53:08.003 (Z)
Received: 16:53:08.001 (A)
Received: 16:53:08.002 (Z)
Received: 16:52:58.000 (A)  ← Batch 2 has earlier times, buffered
Received: 16:52:58.003 (Z)
Received: 16:52:58.002 (A)
Received: 16:52:58.001 (Z)
...
```

**Flushing phase** — as the watermark advances past buffered events, they are emitted **sorted**:
```
Sent: 16:52:58.000 (A)  ← earliest first
Sent: 16:52:58.002 (A)
Sent: 16:53:08.000 (A)
Sent: 16:53:08.001 (A)
...later flush...
Sent: 16:52:58.001 (Z)
Sent: 16:52:58.003 (Z)
Sent: 16:53:08.002 (Z)
Sent: 16:53:08.003 (Z)
```

Events that arrived out of order (Batch 2 before Batch 1) are now emitted in correct event-time order.

### 3. Sink Output — Ordered Per Key Per Partition

With ordered processing enabled, each key is consistently routed to the same partition, and events within a partition are processed in FIFO order. The sink logs confirm this:

**Partition 0 — Key `Z` (all events in event-time order):**

| Order | Event Time   | Key |
|-------|--------------|-----|
| 1     | 16:52:58.001 | Z   |
| 2     | 16:52:58.003 | Z   |
| 3     | 16:53:08.002 | Z   |
| 4     | 16:53:08.003 | Z   |
| 5     | 16:53:18.000 | Z   |
| 6     | 16:53:18.001 | Z   |
| 7     | 16:53:19.000 | Z   |
| 8     | 16:53:19.002 | Z   |
| 9     | 16:54:10.001 | Z   |
| 10    | 16:54:10.003 | Z   |

**Partition 2 — Key `A` (all events in event-time order):**

| Order | Event Time   | Key |
|-------|--------------|-----|
| 1     | 16:52:58.000 | A   |
| 2     | 16:52:58.002 | A   |
| 3     | 16:53:08.000 | A   |
| 4     | 16:53:08.001 | A   |
| 5     | 16:53:18.002 | A   |
| 6     | 16:53:18.003 | A   |
| 7     | 16:53:19.001 | A   |
| 8     | 16:53:19.003 | A   |
| 9     | 16:54:10.000 | A   |
| 10    | 16:54:10.002 | A   |

### 4. Key Takeaways

- **Consistent key routing**: Post reduce vertex, all `Z` events land on partition 0; all `A` events land on partition 2. 
    The key-hash routing is deterministic as well as ordered. This is the spatial order preserving behavior with ordered processing enabled.
- **Event-time ordering preserved**: Within each partition, event-times are strictly non-decreasing — 
    the combination of stream-sorter + ordered processing works end-to-end.
- **Cross-source merging**: Events from both `input-one` and `input-two` are correctly interleaved by event-time, not by arrival order.
- **Watermark-driven flushing**: The stream-sorter holds events until the watermark advances far enough, 
    which is why the last few events in a batch may not appear in the sink until a later event (with a sufficiently advanced event-time) 
    triggers a flush. In this run, the final event at `19:50:50.003` was sent specifically to flush the remaining buffered events.
