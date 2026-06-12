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
        /// How long a key stays paused once it enters a pause.
        pub(crate) pause_timeout: Duration,
        /// Per active key, per read cycle, probability of entering a pause.
        pub(crate) pause_probability: f64,
        /// Inter-batch pacing sleep; also avoids busy-spin when all keys paused.
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
            pause_probability: f64,
            emit_interval_ms: u64,
            max_tps: f64,
        ) -> Self {
            let pause_probability = if pause_probability.is_finite() {
                pause_probability.clamp(0.0, 1.0)
            } else {
                // Non-finite (NaN/inf) falls back to the same default `from_env` uses.
                0.002
            };
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
                pause_probability,
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
                parse_env("PAUSE_PROBABILITY", 0.002),
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

    /// Outcome of evaluating a key for the current read cycle.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum KeyDecision {
        /// Key is active and should emit this cycle.
        Active,
        /// Key's pause just expired, so it is active again this cycle.
        Resumed,
        /// Key is in an ongoing pause and is skipped.
        Paused,
        /// Key just entered a pause this cycle and is skipped.
        JustPaused,
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

    /// Decide whether `key` is active this cycle, mutating `paused_until`:
    /// - if currently paused (`now < expiry`) -> `Paused`;
    /// - otherwise an expired entry is cleared (the key had been paused);
    /// - then with probability `prob` the key enters a new pause
    ///   (`now + timeout`) -> `JustPaused`;
    /// - else -> `Resumed` if it had just been un-paused, otherwise `Active`.
    ///
    /// `prob` must be in `[0.0, 1.0]` (a value above `1.0` makes the underlying
    /// `rand` call panic). Callers clamp it via [`Config::new`].
    pub(crate) fn decide_key<R: Rng>(
        now: Instant,
        paused_until: &mut HashMap<String, Instant>,
        key: &str,
        prob: f64,
        timeout: Duration,
        rng: &mut R,
    ) -> KeyDecision {
        let was_paused = match paused_until.get(key) {
            Some(&expiry) if now < expiry => return KeyDecision::Paused,
            Some(_) => {
                paused_until.remove(key);
                true
            }
            None => false,
        };
        if prob > 0.0 && rng.random_bool(prob) {
            paused_until.insert(key.to_string(), now + timeout);
            KeyDecision::JustPaused
        } else if was_paused {
            KeyDecision::Resumed
        } else {
            KeyDecision::Active
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
        /// key -> instant at which its current pause expires.
        paused_until: Mutex<HashMap<String, Instant>>,
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
            Self {
                config,
                keys,
                yet_to_ack: RwLock::new(HashMap::new()),
                nacked: RwLock::new(HashMap::new()),
                counter: AtomicUsize::new(0),
                paused_until: Mutex::new(HashMap::new()),
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
                    let mut paused = self.paused_until.lock().unwrap();
                    let mut active = Vec::new();
                    for key in &self.keys {
                        match decide_key(
                            now_instant,
                            &mut paused,
                            key,
                            self.config.pause_probability,
                            self.config.pause_timeout,
                            &mut rng,
                        ) {
                            KeyDecision::Active => active.push(key.as_str()),
                            KeyDecision::Resumed => {
                                info!("key {key} resumed emitting");
                                active.push(key.as_str());
                            }
                            KeyDecision::Paused => {}
                            KeyDecision::JustPaused => info!(
                                "key {key} entering pause for {:?}",
                                self.config.pause_timeout
                            ),
                        }
                    }
                    active
                };

                if active.is_empty() {
                    Vec::new()
                } else {
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
                        let event_time =
                            jittered_event_time(now_utc, self.config.jitter_ms, &mut rng);
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
                }
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

        async fn pending(&self) -> Option<usize> {
            Some(self.yet_to_ack.read().unwrap().len())
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![0])
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
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use rand::SeedableRng;
        use rand::rngs::StdRng;
        use tokio::sync::mpsc;

        fn test_config(num_keys: usize) -> Config {
            // probability 0.0 => deterministic (no pauses); 1ms pacing (0 floors to 1).
            Config::new(num_keys, 5000, 45, 0.0, 0, 0.0)
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
            assert_eq!(Config::new(0, 100, 10, 0.1, 50, 0.0).num_keys, 1);
            // negative jitter floored to 0
            assert_eq!(Config::new(3, -100, 10, 0.1, 50, 0.0).jitter_ms, 0);
            // probability clamped into [0.0, 1.0]
            assert_eq!(Config::new(3, 100, 10, 5.0, 50, 0.0).pause_probability, 1.0);
            assert_eq!(
                Config::new(3, 100, 10, -1.0, 50, 0.0).pause_probability,
                0.0
            );
            // non-finite probability falls back to the default (0.002)
            assert_eq!(
                Config::new(3, 100, 10, f64::NAN, 50, 0.0).pause_probability,
                0.002
            );
            assert_eq!(
                Config::new(3, 100, 10, f64::INFINITY, 50, 0.0).pause_probability,
                0.002
            );
            // emit_interval floored to 1ms so 0 still paces reads
            assert_eq!(
                Config::new(3, 100, 10, 0.1, 0, 0.0).emit_interval,
                Duration::from_millis(1)
            );
            // durations mapped from the right units
            assert_eq!(
                Config::new(3, 100, 7, 0.1, 250, 0.0).pause_timeout,
                Duration::from_secs(7)
            );
            assert_eq!(
                Config::new(3, 100, 7, 0.1, 250, 0.0).emit_interval,
                Duration::from_millis(250)
            );
            // in-range values pass through unchanged
            let c = Config::new(5, 300, 30, 0.3, 100, 0.0);
            assert_eq!(c.num_keys, 5);
            assert_eq!(c.jitter_ms, 300);
            assert_eq!(c.pause_probability, 0.3);
            // MAX_TPS: only finite positive enables limiting; 0/negative/NaN/inf
            // mean unlimited (stored as 0.0). Burst is 1s of tokens (>= 1).
            assert_eq!(Config::new(3, 100, 10, 0.1, 50, 20.0).max_tps, 20.0);
            assert_eq!(Config::new(3, 100, 10, 0.1, 50, -5.0).max_tps, 0.0);
            assert_eq!(Config::new(3, 100, 10, 0.1, 50, f64::NAN).max_tps, 0.0);
            assert_eq!(Config::new(3, 100, 10, 0.1, 50, f64::INFINITY).max_tps, 0.0);
            assert_eq!(Config::new(3, 100, 10, 0.1, 50, 20.0).tps_burst, 20.0);
            assert_eq!(Config::new(3, 100, 10, 0.1, 50, 0.0).tps_burst, 1.0);
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
        fn decide_key_already_paused_stays_paused() {
            let now = Instant::now();
            let mut paused = HashMap::new();
            paused.insert("k".to_string(), now + Duration::from_secs(60));
            let mut rng = StdRng::seed_from_u64(1);
            // probability 1.0 is ignored because the key is already paused
            let d = decide_key(
                now,
                &mut paused,
                "k",
                1.0,
                Duration::from_secs(45),
                &mut rng,
            );
            assert!(matches!(d, KeyDecision::Paused));
        }

        #[test]
        fn decide_key_zero_probability_is_active() {
            let now = Instant::now();
            let mut paused = HashMap::new();
            let mut rng = StdRng::seed_from_u64(1);
            let d = decide_key(
                now,
                &mut paused,
                "k",
                0.0,
                Duration::from_secs(45),
                &mut rng,
            );
            assert!(matches!(d, KeyDecision::Active));
            assert!(!paused.contains_key("k"));
        }

        #[test]
        fn decide_key_full_probability_enters_pause() {
            let now = Instant::now();
            let mut paused = HashMap::new();
            let mut rng = StdRng::seed_from_u64(1);
            let d = decide_key(
                now,
                &mut paused,
                "k",
                1.0,
                Duration::from_secs(45),
                &mut rng,
            );
            assert!(matches!(d, KeyDecision::JustPaused));
            assert_eq!(*paused.get("k").unwrap(), now + Duration::from_secs(45));
        }

        #[test]
        fn decide_key_expired_pause_resumes() {
            let base = Instant::now();
            let mut paused = HashMap::new();
            paused.insert("k".to_string(), base); // expiry == base
            let later = base + Duration::from_millis(10);
            let mut rng = StdRng::seed_from_u64(1);
            let d = decide_key(
                later,
                &mut paused,
                "k",
                0.0,
                Duration::from_secs(45),
                &mut rng,
            );
            assert!(matches!(d, KeyDecision::Resumed));
            assert!(!paused.contains_key("k"), "expired pause should be removed");
        }

        #[test]
        fn decide_key_expired_pause_can_repause() {
            let base = Instant::now();
            let mut paused = HashMap::new();
            paused.insert("k".to_string(), base); // expired (expiry == base)
            let later = base + Duration::from_millis(10);
            let mut rng = StdRng::seed_from_u64(1);
            // prob 1.0: the stale entry is cleared, then a fresh pause is entered.
            let d = decide_key(
                later,
                &mut paused,
                "k",
                1.0,
                Duration::from_secs(45),
                &mut rng,
            );
            assert!(matches!(d, KeyDecision::JustPaused));
            assert_eq!(*paused.get("k").unwrap(), later + Duration::from_secs(45));
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
        async fn read_emits_count_messages_across_active_keys() {
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

            // Every message carries exactly one key from the configured set.
            let valid: Vec<String> = (0..3).map(|i| format!("key-{i}")).collect();
            for m in &msgs {
                assert_eq!(m.keys.len(), 1, "each message has exactly one key");
                assert!(valid.contains(&m.keys[0]), "unexpected key {:?}", m.keys);
                assert_eq!(m.offset.partition_id, 0);
            }
            // Round-robin over 3 active keys => all 3 keys are represented.
            let distinct: std::collections::HashSet<_> =
                msgs.iter().map(|m| m.keys[0].clone()).collect();
            assert_eq!(distinct.len(), 3, "all active keys should be used");

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
            let source = JitterSource::with_config(Config::new(3, 0, 45, 0.0, 0, 3.0));
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
        async fn read_emits_nothing_when_all_keys_paused() {
            // probability 1.0 => every key enters a pause on the first cycle.
            let source = JitterSource::with_config(Config::new(3, 5000, 45, 1.0, 0, 0.0));
            let (tx, mut rx) = mpsc::channel(64);
            source
                .read(
                    SourceReadRequest {
                        count: 5,
                        timeout: Duration::from_secs(1),
                    },
                    tx,
                )
                .await;
            assert_eq!(
                drain(&mut rx).await.len(),
                0,
                "all keys paused => no messages"
            );
            assert_eq!(source.pending().await, Some(0));
        }

        #[tokio::test]
        #[allow(deprecated)]
        async fn partitions_is_zero() {
            let source = JitterSource::with_config(test_config(3));
            assert_eq!(source.partitions().await, Some(vec![0]));
        }
    }
}
