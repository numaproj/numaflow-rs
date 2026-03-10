# Redis E2E Test Sink

A User Defined Sink that writes messages to Redis. Used by Numaflow E2E testing.

It supports two modes of operation controlled by the `MODE` environment variable.

## Modes

### Hash mode (default)

Stores messages in a Redis hash using `HINCR`. Each field in the hash is a message payload and the value is its occurrence count.

Optionally checks that messages arrive in event-time order when `CHECK_ORDER=true`. In that case, `MESSAGE_COUNT` consecutive messages are collected and compared; the result (`ordered` or `not ordered`) is also recorded in the hash.

### Ordered mode (`MODE=ordered`)

Appends each message's payload to a Redis list using `RPUSH`, preserving insertion order. The list key is `{SINK_KEY}_{key1}:{key2}:...` where the keys come from the message's keys field.

This mode is useful for verifying ordered processing in pipelines (see [numaflow#1677](https://github.com/numaproj/numaflow/issues/1677)).

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `SINK_KEY` / `SINK_HASH_KEY` | Yes | Redis key (or key prefix in ordered mode). Either name works; `SINK_KEY` takes precedence. |
| `MODE` | No | Set to `ordered` for list mode. Defaults to hash mode. |
| `CHECK_ORDER` | No | Hash mode only. Set to `true` to check event-time ordering. |
| `MESSAGE_COUNT` | No | Hash mode only. Number of consecutive messages to compare for order checking. |