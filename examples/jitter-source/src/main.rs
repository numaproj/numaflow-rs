//! A User Defined Source that generates multi-key events with jittered event
//! times and periodically pauses *some* keys for a configurable timeout. It is
//! purpose-built to exercise the stream-sorter accumulator
//! (`examples/stream-sorter`): the jitter produces an out-of-order-by-event-time
//! stream for the sorter to order, and the per-key pauses drive watermark
//! progression and the accumulator's per-key idle-timeout window close.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    let source = jitter_source::JitterSource::new();
    numaflow::source::Server::new(source).start().await
}

pub(crate) mod jitter_source {
    use chrono::{DateTime, Utc};
    use numaflow::source::{Message, Offset, SourceReadRequest, Sourcer};
    use rand::Rng;
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
        /// Number of distinct keys: `key-0` .. `key-(num_keys-1)`.
        pub(crate) num_keys: usize,
        /// Maximum absolute jitter in milliseconds applied to event time.
        pub(crate) jitter_ms: i64,
        /// How long each key stays paused on its round-robin turn.
        pub(crate) pause_timeout: Duration,
        /// Inter-batch pacing sleep; also avoids busy-spin between batches.
        pub(crate) emit_interval: Duration,
        /// Max total events/sec across all keys. `0.0` means unlimited.
        pub(crate) max_tps: f64,
        /// Token-bucket burst capacity: `max(max_tps, 1.0)` tokens — one second
        /// of capacity when `max_tps >= 1.0`, floored to 1 so at least one token
        /// can accrue when `max_tps` is very small.
        pub(crate) tps_burst: f64,
    }

    impl Config {
        /// Build a sanitized config from raw values (clamps out-of-range input).
        pub(crate) fn new(
            num_keys: usize,
            jitter_ms: i64,
            pause_timeout_secs: u64,
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
                jitter_ms: jitter_ms.max(0),
                pause_timeout: Duration::from_secs(pause_timeout_secs),
                // Floor to 1ms so a 0 value still paces reads (busy-spin guard).
                emit_interval: Duration::from_millis(emit_interval_ms.max(1)),
                max_tps,
                tps_burst: max_tps.max(1.0),
            }
        }

        /// Read configuration from the environment, falling back to defaults.
        pub(crate) fn from_env() -> Self {
            Self::new(
                parse_env("NUM_KEYS", 3),
                parse_env("EVENT_TIME_JITTER_MS", 5000),
                parse_env("PAUSE_TIMEOUT_SECS", 45),
                parse_env("EMIT_INTERVAL_MS", 200),
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
                    tracing::warn!("invalid value {raw:?} for {key}; using default");
                    default
                }
            },
            // Var unset: silently use the default.
            Err(std::env::VarError::NotPresent) => default,
            // Var set but not valid UTF-8: operator mistake, so warn.
            Err(std::env::VarError::NotUnicode(raw)) => {
                tracing::warn!("non-UTF-8 value for {key} ({raw:?}); using default");
                default
            }
        }
    }

    /// Return `now` shifted by a uniformly random offset in
    /// `[-jitter_ms, +jitter_ms]` milliseconds. A non-positive `jitter_ms`
    /// returns `now` unchanged.
    pub(crate) fn jittered_event_time<R: Rng>(
        now: DateTime<Utc>,
        jitter_ms: i64,
        rng: &mut R,
    ) -> DateTime<Utc> {
        if jitter_ms <= 0 {
            return now;
        }
        let offset = rng.random_range(-jitter_ms..=jitter_ms);
        now + chrono::Duration::milliseconds(offset)
    }

    /// Round-robin pause cursor: the index of the key currently paused and when
    /// that pause expires. Exactly one key is paused at a time.
    #[derive(Debug)]
    pub(crate) struct Rotation {
        paused_idx: usize,
        expiry: Instant,
    }

    /// Advance the round-robin pause when the current pause has expired, moving
    /// the pause to the next key (wrapping) for `timeout`. Returns the index of
    /// the currently-paused key — or `None` when there are fewer than 2 keys
    /// (nothing to round-robin) — and whether this call advanced the rotation
    /// (so the caller can log the transition).
    pub(crate) fn rotate_pause(
        rotation: &mut Rotation,
        now: Instant,
        num_keys: usize,
        timeout: Duration,
    ) -> (Option<usize>, bool) {
        if num_keys < 2 {
            return (None, false);
        }
        if now >= rotation.expiry {
            // Advances one step only; if several timeouts elapsed between calls
            // (e.g. under backpressure) the missed rotations are skipped — fine
            // here, the next call simply continues the rotation.
            rotation.paused_idx = (rotation.paused_idx + 1) % num_keys;
            rotation.expiry = now + timeout;
            (Some(rotation.paused_idx), true)
        } else {
            (Some(rotation.paused_idx), false)
        }
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

    /// Generates multi-key, jittered, occasionally-paused events. Shared state
    /// uses interior mutability because [`Sourcer`] hands out `&self`.
    pub(crate) struct JitterSource {
        config: Config,
        keys: Vec<String>,
        /// offset -> pending message (drives backpressure and `pending`).
        yet_to_ack: RwLock<HashMap<String, Pending>>,
        /// offset -> pending message to re-emit on the next read.
        nacked: RwLock<HashMap<String, Pending>>,
        counter: AtomicUsize,
        /// Round-robin pause cursor (which key is paused, and until when).
        rotation: Mutex<Rotation>,
        /// Token bucket for total-TPS rate limiting.
        bucket: Mutex<TokenBucket>,
    }

    impl JitterSource {
        pub(crate) fn new() -> Self {
            Self::with_config(Config::from_env())
        }

        pub(crate) fn with_config(config: Config) -> Self {
            let keys = (0..config.num_keys).map(|i| format!("key-{i}")).collect();
            let bucket = Mutex::new(TokenBucket::new(config.tps_burst, Instant::now()));
            // key-0 is paused first, for a full timeout, then the rotation advances.
            let rotation = Mutex::new(Rotation {
                paused_idx: 0,
                expiry: Instant::now() + config.pause_timeout,
            });
            Self {
                config,
                keys,
                yet_to_ack: RwLock::new(HashMap::new()),
                nacked: RwLock::new(HashMap::new()),
                counter: AtomicUsize::new(0),
                rotation,
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
    impl Sourcer for JitterSource {
        async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>) {
            // Backpressure: don't read more until the current batch is acked.
            if !self.yet_to_ack.read().unwrap().is_empty() {
                return;
            }

            // Re-emit nacked messages first, then return.
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

            // Pace generation (also prevents busy-spin when all keys are paused).
            tokio::time::sleep(self.config.emit_interval).await;

            // Plan the batch synchronously: the RNG (`ThreadRng`) and the
            // `Mutex` guard are both `!Send`, so they MUST be dropped before any
            // `.await` below, or the `read` future stops being `Send`.
            let planned: Vec<(String, Pending)> = {
                let now_instant = Instant::now();
                let now_utc = Utc::now();
                let mut rng = rand::rng();

                let active: Vec<&str> = {
                    let (paused_idx, rotated) = {
                        let mut rotation = self.rotation.lock().unwrap();
                        rotate_pause(
                            &mut rotation,
                            now_instant,
                            self.keys.len(),
                            self.config.pause_timeout,
                        )
                    };
                    if rotated {
                        if let Some(p) = paused_idx {
                            let resumed = (p + self.keys.len() - 1) % self.keys.len();
                            info!("round-robin pause: key-{resumed} resumed, key-{p} now paused");
                        }
                    }
                    self.keys
                        .iter()
                        .enumerate()
                        .filter(|&(i, _)| Some(i) != paused_idx)
                        .map(|(_, k)| k.as_str())
                        .collect()
                };

                // Round-robin keeps at least one key active (with 1 key none is
                // paused; with >=2, exactly one is), so `active` is never empty.
                debug_assert!(!active.is_empty());
                // Rate-limit total emission to MAX_TPS (unlimited when 0).
                let allowed = {
                    let mut bucket = self.bucket.lock().unwrap();
                    take_tokens(
                        &mut bucket,
                        now_instant,
                        request.count,
                        self.config.max_tps,
                        self.config.tps_burst,
                    )
                };
                let ts_nanos = now_utc.timestamp_nanos_opt().unwrap_or(0);
                let mut plan = Vec::with_capacity(allowed);
                for i in 0..allowed {
                    let key = active[i % active.len()].to_string();
                    let event_time = jittered_event_time(now_utc, self.config.jitter_ms, &mut rng);
                    let seq = self.counter.fetch_add(1, Ordering::Relaxed);
                    let offset = format!("{ts_nanos}-{seq}");
                    let value =
                        format!("{seq}:{key}:{}", event_time.timestamp_millis()).into_bytes();
                    plan.push((
                        offset,
                        Pending {
                            key,
                            event_time,
                            value,
                        },
                    ));
                }
                plan
            };

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

            // Distinct keys actually emitted this batch (owned, so we can still
            // move `planned` below).
            let keys_emitted: std::collections::BTreeSet<String> = planned
                .iter()
                .take(emitted)
                .map(|(_, p)| p.key.clone())
                .collect();

            // Only track what was actually delivered; undelivered items are
            // dropped so they cannot get stuck in `yet_to_ack` forever.
            let mut yet = self.yet_to_ack.write().unwrap();
            for (offset, pending) in planned.into_iter().take(emitted) {
                yet.insert(offset, pending);
            }
            info!("emitted {emitted} messages across keys {keys_emitted:?}");
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
        use rand::SeedableRng;
        use rand::rngs::StdRng;
        use tokio::sync::mpsc;

        fn test_config(num_keys: usize) -> Config {
            // No rate limit (0.0); 1ms pacing (0 floors to 1) keeps tests fast.
            // Round-robin starts with key-0 paused, so tests see key-0 idle.
            Config::new(num_keys, 5000, 45, 0, 0.0)
        }

        async fn drain(rx: &mut mpsc::Receiver<Message>) -> Vec<Message> {
            let mut out = Vec::new();
            while let Ok(msg) = rx.try_recv() {
                out.push(msg);
            }
            out
        }

        #[test]
        fn config_new_sanitizes_input() {
            // num_keys floored to 1
            assert_eq!(Config::new(0, 100, 10, 50, 0.0).num_keys, 1);
            // negative jitter floored to 0
            assert_eq!(Config::new(3, -100, 10, 50, 0.0).jitter_ms, 0);
            // emit_interval floored to 1ms so 0 still paces reads
            assert_eq!(
                Config::new(3, 100, 10, 0, 0.0).emit_interval,
                Duration::from_millis(1)
            );
            // durations mapped from the right units
            assert_eq!(
                Config::new(3, 100, 7, 250, 0.0).pause_timeout,
                Duration::from_secs(7)
            );
            assert_eq!(
                Config::new(3, 100, 7, 250, 0.0).emit_interval,
                Duration::from_millis(250)
            );
            // MAX_TPS: only finite positive enables limiting; 0/negative/NaN/inf
            // mean unlimited (stored as 0.0). Burst is 1s of tokens (>= 1).
            assert_eq!(Config::new(3, 100, 10, 50, 20.0).max_tps, 20.0);
            assert_eq!(Config::new(3, 100, 10, 50, -5.0).max_tps, 0.0);
            assert_eq!(Config::new(3, 100, 10, 50, f64::NAN).max_tps, 0.0);
            assert_eq!(Config::new(3, 100, 10, 50, f64::INFINITY).max_tps, 0.0);
            assert_eq!(Config::new(3, 100, 10, 50, 20.0).tps_burst, 20.0);
            assert_eq!(Config::new(3, 100, 10, 50, 0.0).tps_burst, 1.0);
            // in-range values pass through unchanged
            let c = Config::new(5, 300, 30, 100, 0.0);
            assert_eq!(c.num_keys, 5);
            assert_eq!(c.jitter_ms, 300);
        }

        #[test]
        fn jittered_event_time_stays_within_bounds() {
            let now = Utc::now();
            let mut rng = StdRng::seed_from_u64(7);
            for _ in 0..1000 {
                let et = jittered_event_time(now, 5000, &mut rng);
                let diff = (et - now).num_milliseconds();
                assert!((-5000..=5000).contains(&diff), "diff {diff} out of bounds");
            }
        }

        #[test]
        fn jittered_event_time_zero_jitter_is_now() {
            let now = Utc::now();
            let mut rng = StdRng::seed_from_u64(7);
            assert_eq!(jittered_event_time(now, 0, &mut rng), now);
        }

        #[test]
        fn rotate_pause_disabled_for_single_key() {
            let now = Instant::now();
            let mut r = Rotation {
                paused_idx: 0,
                expiry: now + Duration::from_secs(45),
            };
            assert_eq!(
                rotate_pause(&mut r, now, 1, Duration::from_secs(45)),
                (None, false)
            );
        }

        #[test]
        fn rotate_pause_holds_until_expiry() {
            let now = Instant::now();
            let mut r = Rotation {
                paused_idx: 0,
                expiry: now + Duration::from_secs(45),
            };
            // before expiry: key-0 stays paused, no rotation.
            assert_eq!(
                rotate_pause(&mut r, now, 3, Duration::from_secs(45)),
                (Some(0), false)
            );
        }

        #[test]
        fn rotate_pause_advances_and_wraps() {
            let base = Instant::now();
            let timeout = Duration::from_secs(45);
            let mut r = Rotation {
                paused_idx: 0,
                expiry: base, // already expired -> first call rotates
            };
            let t1 = base + Duration::from_secs(1);
            assert_eq!(rotate_pause(&mut r, t1, 3, timeout), (Some(1), true)); // 0 -> 1
            assert_eq!(rotate_pause(&mut r, t1, 3, timeout), (Some(1), false)); // holds
            let t2 = t1 + Duration::from_secs(46);
            assert_eq!(rotate_pause(&mut r, t2, 3, timeout), (Some(2), true)); // 1 -> 2
            let t3 = t2 + Duration::from_secs(46);
            assert_eq!(rotate_pause(&mut r, t3, 3, timeout), (Some(0), true)); // 2 -> 0 wraps
        }

        #[test]
        fn rotate_pause_alternates_for_two_keys() {
            let base = Instant::now();
            let timeout = Duration::from_secs(45);
            let mut r = Rotation {
                paused_idx: 0,
                expiry: base, // already expired -> first call rotates
            };
            let t1 = base + Duration::from_secs(1);
            assert_eq!(rotate_pause(&mut r, t1, 2, timeout), (Some(1), true)); // 0 -> 1
            let t2 = t1 + Duration::from_secs(46);
            assert_eq!(rotate_pause(&mut r, t2, 2, timeout), (Some(0), true)); // 1 -> 0
            let t3 = t2 + Duration::from_secs(46);
            assert_eq!(rotate_pause(&mut r, t3, 2, timeout), (Some(1), true)); // 0 -> 1
        }

        #[test]
        fn take_tokens_unlimited_grants_all() {
            let mut b = TokenBucket::new(0.0, Instant::now());
            assert_eq!(take_tokens(&mut b, Instant::now(), 100, 0.0, 1.0), 100);
        }

        #[test]
        fn take_tokens_caps_by_available_then_empties() {
            let base = Instant::now();
            let mut b = TokenBucket::new(0.0, base);
            // 10 TPS, 1s elapsed -> 10 tokens (burst 10); want 100 -> grant 10.
            let g1 = take_tokens(&mut b, base + Duration::from_secs(1), 100, 10.0, 10.0);
            assert_eq!(g1, 10);
            // No time passes -> no tokens -> grant 0.
            let g2 = take_tokens(&mut b, base + Duration::from_secs(1), 100, 10.0, 10.0);
            assert_eq!(g2, 0);
        }

        #[test]
        fn take_tokens_refill_capped_at_burst() {
            let base = Instant::now();
            let mut b = TokenBucket::new(0.0, base);
            // 100s elapsed at 10 TPS would be 1000 tokens, but burst caps at 10.
            let g = take_tokens(&mut b, base + Duration::from_secs(100), 1000, 10.0, 10.0);
            assert_eq!(g, 10);
        }

        #[test]
        fn take_tokens_grants_only_up_to_want() {
            let base = Instant::now();
            let mut b = TokenBucket::new(0.0, base);
            // Plenty of tokens, but only want 5.
            let g = take_tokens(&mut b, base + Duration::from_secs(10), 5, 100.0, 1000.0);
            assert_eq!(g, 5);
        }

        #[tokio::test]
        async fn read_emits_count_skipping_the_paused_key() {
            // 3 keys; round-robin pauses key-0 first, so key-1 and key-2 emit.
            let source = JitterSource::with_config(test_config(3));
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
            // key-0 is paused; the other two keys are active.
            let distinct: std::collections::HashSet<_> =
                msgs.iter().map(|m| m.keys[0].clone()).collect();
            assert!(!distinct.contains("key-0"), "paused key-0 must not emit");
            assert_eq!(
                distinct,
                ["key-1".to_string(), "key-2".to_string()]
                    .into_iter()
                    .collect()
            );

            // Offsets must be globally unique.
            let offsets: std::collections::HashSet<_> =
                msgs.iter().map(|m| m.offset.offset.clone()).collect();
            assert_eq!(offsets.len(), msgs.len(), "offsets must be unique");

            assert_eq!(source.pending().await, Some(6));
        }

        #[tokio::test]
        async fn read_respects_tps_cap() {
            // max_tps=3 => tps_burst=3 and the bucket starts full (3 tokens).
            // Asking for 100 should yield exactly the 3 available tokens.
            let source = JitterSource::with_config(Config::new(3, 0, 45, 0, 3.0));
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
            let source = JitterSource::with_config(test_config(2));
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
        async fn nack_then_reread_reemits_identically() {
            let source = JitterSource::with_config(test_config(2));
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

            // Re-emitted messages are identical in offset, key, value, event_time.
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

        #[tokio::test]
        async fn read_applies_backpressure_until_acked() {
            let source = JitterSource::with_config(test_config(2));
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

            // Second read while messages are unacked must emit nothing.
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
            let source = JitterSource::with_config(test_config(3));
            assert_eq!(source.partitions().await, Some(vec![0]));
        }
    }
}
