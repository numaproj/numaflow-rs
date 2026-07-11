//! A User Defined Source whose active key set evolves over time. At any instant
//! it emits across a window of exactly `NUM_KEYS` consecutive keys
//! (`key-0 .. key-(NUM_KEYS-1)` initially). Every `FLUSH_INTERVAL_SECS` of
//! wall-clock time the window slides forward by one: the lowest-valued key
//! retires and a new key, one higher than the current maximum, joins.
//!
//! It exercises downstream per-key behavior — reduce/session-window GC, per-key
//! autoscaling, and watermark/idle handling as keys are born and retired. It is
//! a sibling to `examples/jitter-source` and shares its structure.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    let source = sliding_keys_source::SlidingKeysSource::new();
    numaflow::source::Server::new(source).start().await
}

pub(crate) mod sliding_keys_source {
    use chrono::{DateTime, Utc};
    use numaflow::source::{Message, Offset, SourceReadRequest, Sourcer};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, RwLock};
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc::Sender;
    use tracing::{info, warn};

    /// Runtime configuration, read once from the environment with validated,
    /// safe defaults.
    #[derive(Debug, Clone)]
    pub(crate) struct Config {
        /// Number of simultaneously-active keys in the sliding window.
        pub(crate) num_keys: usize,
        /// Wall-clock cadence at which the key window slides forward by one.
        pub(crate) flush_interval: Duration,
        /// Inter-batch pacing sleep; avoids busy-spin between batches.
        pub(crate) emit_interval: Duration,
        /// Max total events/sec across all keys. `0.0` means unlimited.
        pub(crate) max_tps: f64,
        /// Token-bucket burst capacity: `max(max_tps, 1.0)` tokens — one second
        /// of capacity when `max_tps >= 1.0`, floored to 1 so at least one token
        /// can accrue when `max_tps` is very small.
        pub(crate) tps_burst: f64,
    }

    impl Config {
        /// Build a sanitized config from raw values.
        pub(crate) fn new(
            num_keys: usize,
            flush_interval_secs: u64,
            emit_interval_ms: u64,
            max_tps: f64,
        ) -> Self {
            // Only a finite, positive value enables rate limiting; anything else
            // (0, negative, NaN, infinite) means unlimited.
            let max_tps = if max_tps.is_finite() && max_tps > 0.0 {
                max_tps
            } else {
                0.0
            };
            Self {
                num_keys: num_keys.max(1),
                // Floor to 1s so the window always has a positive period
                // (also guarantees `as_nanos() > 0`, no divide-by-zero).
                flush_interval: Duration::from_secs(flush_interval_secs.max(1)),
                // Floor to 1ms so a 0 value still paces reads (busy-spin guard).
                emit_interval: Duration::from_millis(emit_interval_ms.max(1)),
                max_tps,
                tps_burst: max_tps.max(1.0),
            }
        }

        /// Read configuration from the environment, falling back to defaults.
        pub(crate) fn from_env() -> Self {
            Self::new(
                parse_env("NUM_KEYS", 5),
                parse_env("FLUSH_INTERVAL_SECS", 10),
                parse_env("EMIT_INTERVAL_MS", 100),
                parse_env("MAX_TPS", 0.0),
            )
        }
    }

    /// Parse an environment variable, returning `default` when unset and
    /// warning (then defaulting) when set but unparseable.
    fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
        match std::env::var(key) {
            Ok(raw) => match raw.parse::<T>() {
                Ok(parsed) => parsed,
                Err(_) => {
                    warn!("invalid value {raw:?} for {key}; using default");
                    default
                }
            },
            Err(std::env::VarError::NotPresent) => default,
            Err(std::env::VarError::NotUnicode(raw)) => {
                warn!("non-UTF-8 value for {key} ({raw:?}); using default");
                default
            }
        }
    }

    /// Number of full flush intervals elapsed = how far the window has slid.
    /// `flush_interval` is guaranteed >= 1s by [`Config::new`], so the divisor
    /// is never zero.
    pub(crate) fn window_base(elapsed: Duration, flush_interval: Duration) -> u64 {
        (elapsed.as_nanos() / flush_interval.as_nanos()) as u64
    }

    /// The key index emitted for each of `count` messages in a batch, given the
    /// current window `base` and the rotating `start_pos`. Positions wrap modulo
    /// `num_keys`, so `count > num_keys` repeats keys to fill the batch and
    /// `count <= num_keys` walks distinct positions that rotate across reads.
    pub(crate) fn batch_key_indices(
        base: u64,
        start_pos: usize,
        count: usize,
        num_keys: usize,
    ) -> Vec<u64> {
        (0..count)
            .map(|i| base + ((start_pos + i) % num_keys) as u64)
            .collect()
    }

    /// Token-bucket state for rate limiting. Shared across `read` calls.
    #[derive(Debug)]
    pub(crate) struct TokenBucket {
        tokens: f64,
        last: Instant,
    }

    impl TokenBucket {
        fn new(initial: f64, now: Instant) -> Self {
            Self {
                tokens: initial,
                last: now,
            }
        }
    }

    /// Refill `bucket` for the time elapsed since its last call (capped at
    /// `burst`) and take up to `want` tokens, returning how many were granted.
    /// A non-positive `tps` means unlimited and always grants `want`.
    pub(crate) fn take_tokens(
        bucket: &mut TokenBucket,
        now: Instant,
        want: usize,
        tps: f64,
        burst: f64,
    ) -> usize {
        if tps <= 0.0 {
            return want;
        }
        let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
        bucket.last = now;
        bucket.tokens = (bucket.tokens + elapsed * tps).min(burst);
        let granted = (bucket.tokens.floor() as usize).min(want);
        bucket.tokens -= granted as f64;
        granted
    }

    /// A pending (read-but-unacked) message, retained so a nack can re-emit it
    /// with the exact same key, value, and event time.
    #[derive(Debug, Clone)]
    struct Pending {
        key: String,
        event_time: DateTime<Utc>,
        value: Vec<u8>,
    }

    /// Emits events across a key window that slides forward on a wall-clock
    /// cadence. Shared state uses interior mutability because [`Sourcer`] hands
    /// out `&self`.
    pub(crate) struct SlidingKeysSource {
        config: Config,
        /// Captured at construction; basis for [`window_base`].
        start: Instant,
        /// Persistent per-read rotation cursor; positions taken `% num_keys`.
        cursor: AtomicUsize,
        /// Global monotonic sequence for unique offsets/values.
        counter: AtomicUsize,
        /// offset -> pending message (drives backpressure and `pending`).
        yet_to_ack: RwLock<HashMap<String, Pending>>,
        /// offset -> pending message to re-emit on the next read.
        nacked: RwLock<HashMap<String, Pending>>,
        /// Token bucket for total-TPS rate limiting.
        bucket: Mutex<TokenBucket>,
    }

    impl SlidingKeysSource {
        pub(crate) fn new() -> Self {
            Self::with_config(Config::from_env())
        }

        pub(crate) fn with_config(config: Config) -> Self {
            let bucket = Mutex::new(TokenBucket::new(config.tps_burst, Instant::now()));
            Self {
                config,
                start: Instant::now(),
                cursor: AtomicUsize::new(0),
                counter: AtomicUsize::new(0),
                yet_to_ack: RwLock::new(HashMap::new()),
                nacked: RwLock::new(HashMap::new()),
                bucket,
            }
        }

        fn build_message(&self, offset: &str, pending: &Pending) -> Message {
            Message {
                value: pending.value.clone(),
                event_time: pending.event_time,
                offset: Offset {
                    offset: offset.as_bytes().to_vec(),
                    partition_id: 0,
                },
                keys: vec![pending.key.clone()],
                headers: Default::default(),
                user_metadata: None,
            }
        }
    }

    #[tonic::async_trait]
    impl Sourcer for SlidingKeysSource {
        async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>) {
            // Backpressure: don't read more until the current batch is acked.
            if !self.yet_to_ack.read().unwrap().is_empty() {
                return;
            }

            // Re-emit nacked messages first, then return. (Populated by `nack`,
            // added in Task 5; drains empty until then.)
            let nacked: Vec<(String, Pending)> = self.nacked.write().unwrap().drain().collect();
            if !nacked.is_empty() {
                let mut sent = 0;
                for (offset, pending) in &nacked {
                    if let Err(e) = transmitter.send(self.build_message(offset, pending)).await {
                        warn!("failed to send nacked message: {e}");
                        break;
                    }
                    sent += 1;
                }
                // Only re-track what was actually delivered; undelivered items
                // must not linger in `yet_to_ack` (they'd wedge backpressure).
                let mut yet = self.yet_to_ack.write().unwrap();
                for (offset, pending) in nacked.into_iter().take(sent) {
                    yet.insert(offset, pending);
                }
                return;
            }

            // Pace generation (prevents busy-spin between batches).
            tokio::time::sleep(self.config.emit_interval).await;

            // Rate-limit total emission to MAX_TPS (unlimited when 0). The bucket
            // guard is `!Send`, so keep it in this synchronous block (no `.await`)
            // to keep the `read` future `Send`.
            let allowed = {
                let mut bucket = self.bucket.lock().unwrap();
                take_tokens(
                    &mut bucket,
                    Instant::now(),
                    request.count,
                    self.config.max_tps,
                    self.config.tps_burst,
                )
            };
            if allowed == 0 {
                return;
            }

            // Snapshot the window position and reserve a contiguous run of
            // rotation positions for this batch. `fetch_add` returns the prior
            // cursor (our start_pos); positions are taken `% num_keys` on use.
            // Advance by `allowed` (not `request.count`) so a token-limited batch
            // doesn't skip rotation positions.
            let base = window_base(self.start.elapsed(), self.config.flush_interval);
            let start_pos = self.cursor.fetch_add(allowed, Ordering::Relaxed);
            let now_utc = Utc::now();
            let ts_nanos = now_utc.timestamp_nanos_opt().unwrap_or(0);

            let planned: Vec<(String, Pending)> =
                batch_key_indices(base, start_pos, allowed, self.config.num_keys)
                    .into_iter()
                    .map(|idx| {
                        let key = format!("key-{idx}");
                        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
                        let offset = format!("{ts_nanos}-{seq}");
                        let value = format!("{seq}:{key}").into_bytes();
                        (
                            offset,
                            Pending {
                                key,
                                event_time: now_utc,
                                value,
                            },
                        )
                    })
                    .collect();

            if planned.is_empty() {
                return;
            }

            let mut emitted = 0;
            for (offset, pending) in &planned {
                if let Err(e) = transmitter.send(self.build_message(offset, pending)).await {
                    warn!("failed to send message: {e}");
                    break;
                }
                emitted += 1;
            }

            // Only track what was actually delivered.
            let mut yet = self.yet_to_ack.write().unwrap();
            for (offset, pending) in planned.into_iter().take(emitted) {
                yet.insert(offset, pending);
            }
            let hi = base + self.config.num_keys as u64 - 1;
            info!("emitted {emitted} messages across key window [key-{base}, key-{hi}]");
        }

        async fn ack(&self, offset: Vec<Offset>) {
            let mut yet = self.yet_to_ack.write().unwrap();
            for o in offset {
                if let Ok(key) = String::from_utf8(o.offset) {
                    yet.remove(&key);
                }
            }
        }

        async fn nack(&self, offset: Vec<Offset>) {
            // Remove from pending first (release that lock), then stage for retry.
            let mut removed = Vec::new();
            {
                let mut yet = self.yet_to_ack.write().unwrap();
                for o in offset {
                    if let Ok(key) = String::from_utf8(o.offset) {
                        if let Some(pending) = yet.remove(&key) {
                            removed.push((key, pending));
                        }
                    }
                }
            }
            let mut nacked = self.nacked.write().unwrap();
            for (offset, pending) in removed {
                nacked.insert(offset, pending);
            }
        }

        async fn pending(&self) -> Option<usize> {
            Some(self.yet_to_ack.read().unwrap().len())
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![0])
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::time::Duration;
        use tokio::sync::mpsc;

        #[test]
        fn config_new_sanitizes_input() {
            // num_keys floored to 1
            assert_eq!(Config::new(0, 10, 100, 0.0).num_keys, 1);
            // flush_interval floored to 1s (no divide-by-zero in window_base)
            assert_eq!(
                Config::new(5, 0, 100, 0.0).flush_interval,
                Duration::from_secs(1)
            );
            // emit_interval floored to 1ms so 0 still paces reads
            assert_eq!(
                Config::new(5, 10, 0, 0.0).emit_interval,
                Duration::from_millis(1)
            );
            // MAX_TPS: only finite positive enables limiting; 0/negative/NaN/inf
            // mean unlimited (0.0). tps_burst is floored to 1.0.
            assert_eq!(Config::new(5, 10, 100, 20.0).max_tps, 20.0);
            assert_eq!(Config::new(5, 10, 100, -5.0).max_tps, 0.0);
            assert_eq!(Config::new(5, 10, 100, f64::NAN).max_tps, 0.0);
            assert_eq!(Config::new(5, 10, 100, f64::INFINITY).max_tps, 0.0);
            assert_eq!(Config::new(5, 10, 100, 20.0).tps_burst, 20.0);
            assert_eq!(Config::new(5, 10, 100, 0.0).tps_burst, 1.0);
            // in-range values pass through, mapped to the right units
            let c = Config::new(8, 30, 250, 0.0);
            assert_eq!(c.num_keys, 8);
            assert_eq!(c.flush_interval, Duration::from_secs(30));
            assert_eq!(c.emit_interval, Duration::from_millis(250));
        }

        #[test]
        fn take_tokens_unlimited_grants_all() {
            // tps <= 0 => unlimited, always grants the full `want`.
            let mut b = TokenBucket::new(0.0, Instant::now());
            assert_eq!(take_tokens(&mut b, Instant::now(), 100, 0.0, 1.0), 100);
        }

        #[test]
        fn take_tokens_caps_by_available_then_empties() {
            let base = Instant::now();
            let mut b = TokenBucket::new(0.0, base);
            // After 1s at 10 tps, ~10 tokens accrue (capped at burst 10).
            let g1 = take_tokens(&mut b, base + Duration::from_secs(1), 100, 10.0, 10.0);
            assert_eq!(g1, 10, "1s of refill at 10 tps grants 10");
            // Immediately asking again (no elapsed time) grants nothing.
            let g2 = take_tokens(&mut b, base + Duration::from_secs(1), 100, 10.0, 10.0);
            assert_eq!(g2, 0, "bucket emptied, no refill yet");
        }

        #[test]
        fn take_tokens_refill_capped_at_burst() {
            let base = Instant::now();
            let mut b = TokenBucket::new(0.0, base);
            // 100s of refill at 10 tps would be 1000, but burst caps it at 10.
            let g = take_tokens(&mut b, base + Duration::from_secs(100), 1000, 10.0, 10.0);
            assert_eq!(g, 10, "refill is capped at burst");
        }

        #[test]
        fn take_tokens_grants_only_up_to_want() {
            let base = Instant::now();
            let mut b = TokenBucket::new(0.0, base);
            // Plenty of tokens available, but `want` bounds the grant.
            let g = take_tokens(&mut b, base + Duration::from_secs(10), 5, 100.0, 1000.0);
            assert_eq!(g, 5, "never grants more than requested");
        }

        #[test]
        fn window_base_counts_elapsed_intervals() {
            let interval = Duration::from_secs(10);
            assert_eq!(window_base(Duration::from_secs(0), interval), 0);
            assert_eq!(window_base(Duration::from_secs(9), interval), 0);
            assert_eq!(window_base(Duration::from_secs(10), interval), 1);
            assert_eq!(window_base(Duration::from_secs(35), interval), 3);
        }

        #[test]
        fn batch_key_indices_rotates_across_reads() {
            // num_keys=3, base=0: consecutive reads of 2 walk distinct positions
            // as the cursor (start_pos) advances by `count` each read.
            assert_eq!(batch_key_indices(0, 0, 2, 3), vec![0, 1]);
            assert_eq!(batch_key_indices(0, 2, 2, 3), vec![2, 0]);
            assert_eq!(batch_key_indices(0, 4, 2, 3), vec![1, 2]);
        }

        #[test]
        fn batch_key_indices_wraps_when_count_exceeds_window() {
            // num_keys=3, count=5: positions wrap, repeating keys to fill batch.
            assert_eq!(batch_key_indices(0, 0, 5, 3), vec![0, 1, 2, 0, 1]);
        }

        #[test]
        fn batch_key_indices_slides_with_base() {
            // base=2 shifts every index up by 2: key-0,key-1 retired; window is
            // [key-2, key-4]; key-4 (previous max + 1) is now present.
            assert_eq!(batch_key_indices(2, 0, 3, 3), vec![2, 3, 4]);
        }

        #[test]
        fn batch_key_indices_full_coverage_over_reads() {
            // num_keys=4, count=2: two reads cover all 4 active positions.
            let mut seen: std::collections::BTreeSet<u64> = Default::default();
            seen.extend(batch_key_indices(0, 0, 2, 4));
            seen.extend(batch_key_indices(0, 2, 2, 4));
            assert_eq!(seen, [0, 1, 2, 3].into_iter().collect());
        }

        fn test_config(num_keys: usize) -> Config {
            // 1ms emit pacing (0 floors to 1) keeps tests fast; flush 1h away so
            // `base` stays 0 during a test → active window is key-0..key-(n-1).
            // max_tps=0.0 => unlimited, so token limiting never interferes.
            Config::new(num_keys, 3600, 0, 0.0)
        }

        async fn drain(rx: &mut mpsc::Receiver<Message>) -> Vec<Message> {
            let mut out = Vec::new();
            while let Ok(msg) = rx.try_recv() {
                out.push(msg);
            }
            out
        }

        #[tokio::test]
        async fn read_emits_count_within_active_window() {
            let source = SlidingKeysSource::with_config(test_config(3));
            let (tx, mut rx) = mpsc::channel(64);
            source
                .read(
                    SourceReadRequest {
                        count: 6,
                        timeout: Duration::from_secs(1),
                    },
                    tx,
                )
                .await;

            let msgs = drain(&mut rx).await;
            assert_eq!(msgs.len(), 6, "should emit request.count messages");

            for m in &msgs {
                assert_eq!(m.keys.len(), 1, "each message has exactly one key");
                assert_eq!(m.offset.partition_id, 0);
            }
            // base == 0 (flush 1h away): active window is key-0..key-2.
            let active: std::collections::HashSet<String> = ["key-0", "key-1", "key-2"]
                .iter()
                .map(|s| s.to_string())
                .collect();
            for m in &msgs {
                assert!(
                    active.contains(&m.keys[0]),
                    "key {:?} outside window",
                    m.keys[0]
                );
            }
            let offsets: std::collections::HashSet<_> =
                msgs.iter().map(|m| m.offset.offset.clone()).collect();
            assert_eq!(offsets.len(), msgs.len(), "offsets must be unique");

            assert_eq!(source.pending().await, Some(6));
        }

        #[tokio::test]
        async fn read_respects_tps_cap() {
            // max_tps=3 => tps_burst=3 and the bucket starts full (3 tokens).
            // Asking for 100 should yield exactly the 3 available tokens, even
            // though the active window (num_keys=5) could supply more.
            let source = SlidingKeysSource::with_config(Config::new(5, 3600, 0, 3.0));
            let (tx, mut rx) = mpsc::channel(64);
            source
                .read(
                    SourceReadRequest {
                        count: 100,
                        timeout: Duration::from_secs(1),
                    },
                    tx,
                )
                .await;
            assert_eq!(
                drain(&mut rx).await.len(),
                3,
                "bucket had 3 tokens; should emit exactly 3"
            );
        }

        #[tokio::test]
        async fn ack_clears_pending() {
            let source = SlidingKeysSource::with_config(test_config(3));
            let (tx, mut rx) = mpsc::channel(64);
            source
                .read(
                    SourceReadRequest {
                        count: 4,
                        timeout: Duration::from_secs(1),
                    },
                    tx,
                )
                .await;
            let offsets: Vec<Offset> = drain(&mut rx).await.into_iter().map(|m| m.offset).collect();
            assert_eq!(source.pending().await, Some(4));
            source.ack(offsets).await;
            assert_eq!(source.pending().await, Some(0));
        }

        #[tokio::test]
        async fn read_applies_backpressure_until_acked() {
            let source = SlidingKeysSource::with_config(test_config(3));
            let (tx, mut rx) = mpsc::channel(64);
            source
                .read(
                    SourceReadRequest {
                        count: 3,
                        timeout: Duration::from_secs(1),
                    },
                    tx,
                )
                .await;
            assert_eq!(drain(&mut rx).await.len(), 3);

            let (tx2, mut rx2) = mpsc::channel(64);
            source
                .read(
                    SourceReadRequest {
                        count: 3,
                        timeout: Duration::from_secs(1),
                    },
                    tx2,
                )
                .await;
            assert_eq!(drain(&mut rx2).await.len(), 0, "backpressure: no new reads");
        }

        #[tokio::test]
        #[allow(deprecated)]
        async fn partitions_is_zero() {
            let source = SlidingKeysSource::with_config(test_config(3));
            assert_eq!(source.partitions().await, Some(vec![0]));
        }

        #[tokio::test]
        async fn nack_then_reread_reemits_identically() {
            let source = SlidingKeysSource::with_config(test_config(3));
            let (tx, mut rx) = mpsc::channel(64);
            source
                .read(
                    SourceReadRequest {
                        count: 4,
                        timeout: Duration::from_secs(1),
                    },
                    tx,
                )
                .await;
            let first = drain(&mut rx).await;
            let offsets: Vec<Offset> = first
                .iter()
                .map(|m| Offset {
                    offset: m.offset.offset.clone(),
                    partition_id: m.offset.partition_id,
                })
                .collect();

            source.nack(offsets).await;
            assert_eq!(source.pending().await, Some(0), "nack moves out of pending");

            // Next read must re-emit the nacked messages before any new data.
            let (tx2, mut rx2) = mpsc::channel(64);
            source
                .read(
                    SourceReadRequest {
                        count: 10,
                        timeout: Duration::from_secs(1),
                    },
                    tx2,
                )
                .await;
            let reread = drain(&mut rx2).await;
            assert_eq!(reread.len(), first.len(), "re-emits exactly the nacked set");

            let key = |m: &Message| String::from_utf8(m.offset.offset.clone()).unwrap();
            let mut a: Vec<_> = first
                .iter()
                .map(|m| (key(m), m.keys.clone(), m.value.clone(), m.event_time))
                .collect();
            let mut b: Vec<_> = reread
                .iter()
                .map(|m| (key(m), m.keys.clone(), m.value.clone(), m.event_time))
                .collect();
            a.sort_by(|x, y| x.0.cmp(&y.0));
            b.sort_by(|x, y| x.0.cmp(&y.0));
            assert_eq!(a, b, "re-emitted messages must match the originals");

            assert_eq!(
                source.pending().await,
                Some(4),
                "re-read moves back to pending"
            );
        }
    }
}
