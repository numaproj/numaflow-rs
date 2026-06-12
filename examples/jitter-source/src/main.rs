//! A User Defined Source that generates multi-key events with jittered event
//! times and periodically pauses *some* keys for a configurable timeout. It is
//! purpose-built to exercise the stream-sorter accumulator
//! (`examples/stream-sorter`): the jitter produces an out-of-order-by-event-time
//! stream for the sorter to order, and the per-key pauses drive watermark
//! progression and the accumulator's per-key idle-timeout window close.

// NOTE: `main` is a stub at this stage. It is replaced with the real server
// wiring in a later task, once `JitterSource` exists.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    Ok(())
}

pub(crate) mod jitter_source {
    use chrono::{DateTime, Utc};
    use rand::Rng;
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

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
    }

    impl Config {
        /// Build a sanitized config from raw values (clamps out-of-range input).
        pub(crate) fn new(
            num_keys: usize,
            jitter_ms: i64,
            pause_timeout_secs: u64,
            pause_probability: f64,
            emit_interval_ms: u64,
        ) -> Self {
            let pause_probability = if pause_probability.is_finite() {
                pause_probability.clamp(0.0, 1.0)
            } else {
                0.05
            };
            Self {
                num_keys: num_keys.max(1),
                jitter_ms: jitter_ms.max(0),
                pause_timeout: Duration::from_secs(pause_timeout_secs),
                pause_probability,
                // Floor to 1ms so a 0 value still paces reads (busy-spin guard).
                emit_interval: Duration::from_millis(emit_interval_ms.max(1)),
            }
        }

        /// Read configuration from the environment, falling back to defaults.
        pub(crate) fn from_env() -> Self {
            Self::new(
                parse_env("NUM_KEYS", 3),
                parse_env("EVENT_TIME_JITTER_MS", 5000),
                parse_env("PAUSE_TIMEOUT_SECS", 45),
                parse_env("PAUSE_PROBABILITY", 0.05),
                parse_env("EMIT_INTERVAL_MS", 200),
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
    /// - otherwise an expired entry is cleared, then with probability `prob`
    ///   the key enters a pause until `now + timeout` -> `JustPaused`;
    /// - else -> `Active`.
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
        if let Some(&expiry) = paused_until.get(key) {
            if now < expiry {
                return KeyDecision::Paused;
            }
            paused_until.remove(key);
        }
        if prob > 0.0 && rng.random_bool(prob) {
            paused_until.insert(key.to_string(), now + timeout);
            KeyDecision::JustPaused
        } else {
            KeyDecision::Active
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use rand::SeedableRng;
        use rand::rngs::StdRng;

        #[test]
        fn config_new_sanitizes_input() {
            // num_keys floored to 1
            assert_eq!(Config::new(0, 100, 10, 0.1, 50).num_keys, 1);
            // negative jitter floored to 0
            assert_eq!(Config::new(3, -100, 10, 0.1, 50).jitter_ms, 0);
            // probability clamped into [0.0, 1.0]
            assert_eq!(Config::new(3, 100, 10, 5.0, 50).pause_probability, 1.0);
            assert_eq!(Config::new(3, 100, 10, -1.0, 50).pause_probability, 0.0);
            // non-finite probability falls back to default
            assert_eq!(
                Config::new(3, 100, 10, f64::NAN, 50).pause_probability,
                0.05
            );
            assert_eq!(
                Config::new(3, 100, 10, f64::INFINITY, 50).pause_probability,
                0.05
            );
            // emit_interval floored to 1ms so 0 still paces reads
            assert_eq!(
                Config::new(3, 100, 10, 0.1, 0).emit_interval,
                Duration::from_millis(1)
            );
            // durations mapped from the right units
            assert_eq!(
                Config::new(3, 100, 7, 0.1, 250).pause_timeout,
                Duration::from_secs(7)
            );
            assert_eq!(
                Config::new(3, 100, 7, 0.1, 250).emit_interval,
                Duration::from_millis(250)
            );
            // in-range values pass through unchanged
            let c = Config::new(5, 300, 30, 0.3, 100);
            assert_eq!(c.num_keys, 5);
            assert_eq!(c.jitter_ms, 300);
            assert_eq!(c.pause_probability, 0.3);
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
        fn decide_key_expired_pause_becomes_active() {
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
            assert!(matches!(d, KeyDecision::Active));
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
    }
}
