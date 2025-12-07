# Redis E2E Test Sink
A User Defined Sink using redis hashes to store messages.
The hash key is set by an environment variable `SINK_HASH_KEY` under the sink container spec.

For each message received, the sink will store the message in the hash with the key being the payload of the message
and the value being the no. of occurrences of that payload so far.

The environment variable `CHECK_ORDER` is used to determine whether to check the order of the messages based one event time.
The environment variable `MESSAGE_COUNT` is used to determine how many subsequent number of messages at a time to check the order of.

This sink is used by Numaflow E2E testing.