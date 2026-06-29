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
    }
}
