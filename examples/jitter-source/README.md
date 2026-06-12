# jitter-source

A Numaflow user-defined source, modeled on [`simple-source`](../simple-source),
purpose-built to exercise the [stream-sorter accumulator](../stream-sorter).

It generates events across several keys and:

- assigns each event an `event_time` with random **jitter**, so the merged
  stream is out-of-order by event-time (giving the accumulator something to
  sort); and
- **every now and then pauses some keys** for a configurable timeout. Each
  active key independently has a small per-cycle chance of pausing; while a key
  is paused it emits nothing, the other keys keep advancing the watermark, and
  the paused key's accumulator window eventually flushes — and, once the pause
  outlasts the accumulator's idle `timeout`, closes.

## Configuration (environment variables)

| Var | Default | Meaning |
|-----|---------|---------|
| `NUM_KEYS` | `3` | Number of distinct keys (`key-0` … `key-(N-1)`). |
| `EVENT_TIME_JITTER_MS` | `5000` | Event time = `now()` shifted by a random offset within ± this many ms. |
| `PAUSE_TIMEOUT_SECS` | `45` | How long a key stays paused once it pauses. |
| `PAUSE_PROBABILITY` | `0.002` | Per active key, per read cycle, chance of entering a pause. |
| `EMIT_INTERVAL_MS` | `200` | Pacing between read batches (also the pause-roll cadence). |

**Tuning note:** a key's steady-state paused fraction is roughly
`PAUSE_TIMEOUT / (PAUSE_TIMEOUT + EMIT_INTERVAL / PAUSE_PROBABILITY)`. Because a
pause spans many cycles, a small `PAUSE_PROBABILITY` adds up fast — e.g. `0.05`
with a 45s pause keeps a key idle ~90% of the time and the whole stream idle
most of the run. Keep `PAUSE_PROBABILITY` low (the default `0.002` keeps a key
idle ~30% of the time, so usually only some keys are paused), and keep
`PAUSE_TIMEOUT_SECS` **greater than** the accumulator window `timeout` (30s in
the manifest) so a paused key's window actually closes.

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
