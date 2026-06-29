fn main() {}

pub(crate) mod sliding_keys_source {
    use std::time::Duration;

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
    }

    impl Config {
        /// Build a sanitized config from raw values.
        pub(crate) fn new(num_keys: usize, flush_interval_secs: u64, emit_interval_ms: u64) -> Self {
            Self {
                num_keys: num_keys.max(1),
                // Floor to 1s so the window always has a positive period
                // (also guarantees `as_nanos() > 0`, no divide-by-zero).
                flush_interval: Duration::from_secs(flush_interval_secs.max(1)),
                // Floor to 1ms so a 0 value still paces reads (busy-spin guard).
                emit_interval: Duration::from_millis(emit_interval_ms.max(1)),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::time::Duration;

        #[test]
        fn config_new_sanitizes_input() {
            // num_keys floored to 1
            assert_eq!(Config::new(0, 10, 100).num_keys, 1);
            // flush_interval floored to 1s (no divide-by-zero in window_base)
            assert_eq!(Config::new(5, 0, 100).flush_interval, Duration::from_secs(1));
            // emit_interval floored to 1ms so 0 still paces reads
            assert_eq!(Config::new(5, 10, 0).emit_interval, Duration::from_millis(1));
            // in-range values pass through, mapped to the right units
            let c = Config::new(8, 30, 250);
            assert_eq!(c.num_keys, 8);
            assert_eq!(c.flush_interval, Duration::from_secs(30));
            assert_eq!(c.emit_interval, Duration::from_millis(250));
        }
    }
}
