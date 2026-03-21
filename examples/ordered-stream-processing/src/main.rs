use chrono::{DateTime, Utc};
use numaflow::map;
use std::collections::{HashMap, HashSet};
use std::env;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use redis::{AsyncCommands, RedisResult};
use tonic::async_trait;
use tracing::{error, info, warn};

/// Tracks which replica processes each key by writing `key → replica_id` mappings to a Redis HASH.
/// If a key is already claimed by a different replica, it indicates a routing violation.
struct KeyPartitionTracker {
    connection: redis::aio::MultiplexedConnection,
    replica_id: String,
    hash_key: String,
}

impl KeyPartitionTracker {
    async fn new(
        redis_url: &str,
        replica_id: String,
        hash_key: String,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = redis::Client::open(redis_url)?;
        let connection = client.get_multiplexed_async_connection().await?;
        Ok(Self {
            connection,
            replica_id,
            hash_key,
        })
    }

    /// Register a key for this replica. Returns `true` if the key belongs to this replica,
    /// `false` if another replica already claimed it (routing violation).
    async fn register_key(&self, key: &str) -> Result<bool, redis::RedisError> {
        let existing: Option<String> = self.connection.clone().hget(&self.hash_key, key).await?;

        println!("existing: {:?}", existing);

        match existing {
            Some(replica_id) => Ok(replica_id == self.replica_id),
            None => {
                let result: RedisResult<bool> = self.connection.clone().hset(&self.hash_key, key, self.replica_id.clone()).await;
                println!("result: {:?}", result);
                Ok(true)
            }
        }
    }
}

/// OrderChecker is a map vertex that validates whether events arrive in non-decreasing
/// event-time order per key. It tracks the last-seen event-time for each key and logs
/// whether each incoming event maintains the expected ordering.
///
/// This is useful for verifying that upstream sorting (e.g., stream-sorter accumulator)
/// combined with ordered processing actually delivers events in order.
///
/// Optionally tracks key-to-partition mapping in Redis to prove that each key is
/// consistently routed to the same replica. Enable with `ENABLE_KEY_TRACKING=true`.
struct OrderChecker {
    /// Tracks the last-seen event-time per key.
    last_seen: Arc<Mutex<HashMap<Vec<String>, DateTime<Utc>>>>,
    /// Optional Redis-based key-partition tracker.
    key_tracker: Option<Arc<KeyPartitionTracker>>,
}

impl OrderChecker {
    async fn new() -> Self {
        let key_tracker = if env::var("ENABLE_KEY_TRACKING").ok().as_deref() == Some("true") {
            let redis_url =
                env::var("REDIS_URL").unwrap_or_else(|_| "redis://redis:6379".to_string());
            let replica_id = env::var("NUMAFLOW_REPLICA").unwrap_or_else(|_| "unknown".to_string());
            let hash_key = env::var("KEY_TRACKING_HASH")
                .unwrap_or_else(|_| "numaflow:key_partition_map".to_string());

            match KeyPartitionTracker::new(&redis_url, replica_id.clone(), hash_key).await {
                Ok(tracker) => {
                    info!(
                        replica = %replica_id,
                        redis = %redis_url,
                        "Key-partition tracking enabled"
                    );
                    Some(Arc::new(tracker))
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "Failed to initialize key-partition tracking, continuing without it"
                    );
                    None
                }
            }
        } else {
            None
        };

        Self {
            last_seen: Arc::new(Mutex::new(HashMap::new())),
            key_tracker,
        }
    }
}

#[async_trait]
impl map::Mapper for OrderChecker {
    async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
        let current_event_time = input.eventtime;
        let keys = input.keys.clone();

        {
            let mut state = self.last_seen.lock().unwrap();

            match state.get(&keys) {
                Some(&last_event_time) if current_event_time < last_event_time => {
                    warn!(
                        keys = ?keys,
                        current_event_time = current_event_time.timestamp_millis(),
                        last_event_time = last_event_time.timestamp_millis(),
                        "Order violation detected: current event-time is before last seen event-time"
                    );
                }
                Some(&last_event_time) => {
                    info!(
                        keys = ?keys,
                        current_event_time = current_event_time.timestamp_millis(),
                        last_event_time = last_event_time.timestamp_millis(),
                        "Order maintained"
                    );
                }
                None => {
                    info!(
                        keys = ?keys,
                        current_event_time = current_event_time.timestamp_millis(),
                        "First event for key"
                    );
                }
            }

            state.insert(keys.clone(), current_event_time);

            // Log the current state of all tracked keys
            info!(
                state = ?state.iter().map(|(k, v)| (k.clone(), v.timestamp_millis())).collect::<Vec<_>>(),
                "Current order-checker state"
            );
        }

        // Key-partition tracking (optional)
        if let Some(tracker) = &self.key_tracker {
            let key_str = keys.join(":");
            match tracker.register_key(&key_str).await {
                Ok(true) => {} // correctly assigned to this replica
                Ok(false) => {
                    error!(
                        key = %key_str,
                        replica = %tracker.replica_id,
                        "KEY ROUTING VIOLATION: key assigned to different replica"
                    );
                }
                Err(e) => {
                    warn!(key = %key_str, error = %e, "Failed to register key in Redis");
                }
            }
        }

        vec![map::Message::new(input.value).with_keys(input.keys)]
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    info!("Starting order-checker map server");

    let checker = OrderChecker::new().await;

    // Spawn periodic key distribution check if tracking is enabled
    if let Some(tracker) = checker.key_tracker.clone() {
        let interval_secs: u64 = env::var("KEY_TRACKING_CHECK_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
    }

    map::Server::new(checker).start().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use numaflow::map::{MapRequest, Mapper, SystemMetadata, UserMetadata};

    fn create_request(keys: Vec<String>, value: Vec<u8>, eventtime: DateTime<Utc>) -> MapRequest {
        MapRequest {
            keys,
            value,
            watermark: chrono::Utc::now(),
            eventtime,
            headers: HashMap::new(),
            user_metadata: UserMetadata::new(),
            system_metadata: SystemMetadata::new(),
        }
    }

    #[tokio::test]
    async fn test_in_order_events() {
        let checker = OrderChecker::new().await;

        let t1 = DateTime::from_timestamp(100, 0).unwrap();
        let t2 = DateTime::from_timestamp(200, 0).unwrap();
        let t3 = DateTime::from_timestamp(300, 0).unwrap();

        let keys = vec!["key1".to_string()];

        let msgs = checker
            .map(create_request(keys.clone(), b"a".to_vec(), t1))
            .await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].value, b"a");

        let msgs = checker
            .map(create_request(keys.clone(), b"b".to_vec(), t2))
            .await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].value, b"b");

        let msgs = checker
            .map(create_request(keys.clone(), b"c".to_vec(), t3))
            .await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].value, b"c");

        // Verify state tracks the latest event-time
        let state = checker.last_seen.lock().unwrap();
        assert_eq!(state.get(&keys), Some(&t3));
    }

    #[tokio::test]
    async fn test_out_of_order_detected() {
        let checker = OrderChecker::new().await;

        let t1 = DateTime::from_timestamp(200, 0).unwrap();
        let t2 = DateTime::from_timestamp(100, 0).unwrap(); // Earlier than t1

        let keys = vec!["key1".to_string()];

        checker
            .map(create_request(keys.clone(), b"a".to_vec(), t1))
            .await;

        // Out-of-order event still gets forwarded
        let msgs = checker
            .map(create_request(keys.clone(), b"b".to_vec(), t2))
            .await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].value, b"b");

        // State is updated to the latest received event-time (even if out of order)
        let state = checker.last_seen.lock().unwrap();
        assert_eq!(state.get(&keys), Some(&t2));
    }

    #[tokio::test]
    async fn test_independent_key_tracking() {
        let checker = OrderChecker::new().await;

        let t1 = DateTime::from_timestamp(100, 0).unwrap();
        let t2 = DateTime::from_timestamp(200, 0).unwrap();

        let keys_a = vec!["a".to_string()];
        let keys_b = vec!["b".to_string()];

        checker
            .map(create_request(keys_a.clone(), b"a1".to_vec(), t2))
            .await;
        checker
            .map(create_request(keys_b.clone(), b"b1".to_vec(), t1))
            .await;

        let state = checker.last_seen.lock().unwrap();
        assert_eq!(state.get(&keys_a), Some(&t2));
        assert_eq!(state.get(&keys_b), Some(&t1));
    }

    #[tokio::test]
    async fn test_equal_event_times_are_valid() {
        let checker = OrderChecker::new().await;

        let t = DateTime::from_timestamp(100, 0).unwrap();
        let keys = vec!["key1".to_string()];

        checker
            .map(create_request(keys.clone(), b"a".to_vec(), t))
            .await;

        // Same event-time should be treated as in-order
        let msgs = checker
            .map(create_request(keys.clone(), b"b".to_vec(), t))
            .await;
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].value, b"b");
    }

    #[tokio::test]
    async fn test_passes_through_keys_and_value() {
        let checker = OrderChecker::new().await;
        let t = DateTime::from_timestamp(100, 0).unwrap();

        let msgs = checker
            .map(create_request(
                vec!["k1".to_string(), "k2".to_string()],
                b"payload".to_vec(),
                t,
            ))
            .await;

        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].keys, Some(vec!["k1".to_string(), "k2".to_string()]));
        assert_eq!(msgs[0].value, b"payload");
    }
}
