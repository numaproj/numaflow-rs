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
    use std::time::Duration;

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

    #[cfg(test)]
    mod tests {
        use super::*;

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
    }
}
