# jitter-source

A Numaflow user-defined source, modeled on [`simple-source`](../simple-source),
purpose-built to exercise the [stream-sorter accumulator](../stream-sorter).

It generates events across several keys and:

- assigns each event an `event_time` with random **jitter**, so the merged
  stream is out-of-order by event-time (giving the accumulator something to
  sort); and
- **pauses keys in round-robin** — exactly one key at a time. A key is paused
  for `PAUSE_TIMEOUT_SECS`, then it resumes and the next key in order pauses,
  cycling continuously. While a key is paused it emits nothing; the other keys
  keep advancing the watermark, so the paused key's accumulator window flushes
  and — once the pause outlasts the accumulator's idle `timeout` — closes,
  before that key resumes.

## Configuration (environment variables)

| Var | Default | Meaning |
|-----|---------|---------|
| `NUM_KEYS` | `3` | Number of distinct keys (`key-0` … `key-(N-1)`). |
| `EVENT_TIME_JITTER_MS` | `5000` | Event time = `now()` shifted by a random offset within ± this many ms. |
| `PAUSE_TIMEOUT_SECS` | `45` | How long each key stays paused on its round-robin turn before the rotation advances. |
| `EMIT_INTERVAL_MS` | `200` | Pacing between read batches (also the pause-roll cadence). |
| `MAX_TPS` | `0` (unlimited) | Caps the source's **total** events/sec across all keys (token bucket). `0` or negative disables limiting. |

**Rate limiting:** `MAX_TPS` bounds the source's *total* output rate across all
keys via a token bucket (burst = 1 second of tokens). It is the upper bound —
backpressure or pauses can make the actual rate lower. The example manifest sets
`MAX_TPS=20` for a controlled, observable demo; set it to `0` to emit as fast as
backpressure allows.

**Round-robin pauses:** exactly one key is paused at a time; with `N` keys each
key is therefore paused ~`1/N` of the time, in turn. Keep `PAUSE_TIMEOUT_SECS`
greater than the accumulator window `timeout` (30s in the manifest) so a paused
key's window closes during its turn. With a single key, rotation is disabled
(there is nothing to round-robin) and that key always emits.

## Running

```bash
make image    # build and (PUSH=true) push the container image

# deploy the pipeline: jitter-source -> stream-sorter accumulator -> log sink
kubectl apply -f manifests/jitter-stream-sorter-pipeline.yaml
```

Watch the **`sorter`** vertex logs to see the accumulator buffer events, emit
them in event-time order as the watermark advances, and close a key's window
once it has been idle past the timeout. The source logs
`key <k> entering pause …` and `key <k> resumed emitting`, so you can correlate
pauses with the sorter's flush/close behavior. The `out` (log) sink shows the
final, event-time-ordered stream.
