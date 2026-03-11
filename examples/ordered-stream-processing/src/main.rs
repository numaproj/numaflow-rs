use chrono::{DateTime, Utc};
use numaflow::map;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::async_trait;
use tracing::{info, warn};

/// OrderChecker is a map vertex that validates whether events arrive in non-decreasing
/// event-time order per key. It tracks the last-seen event-time for each key and logs
/// whether each incoming event maintains the expected ordering.
///
/// This is useful for verifying that upstream sorting (e.g., stream-sorter accumulator)
/// combined with ordered processing actually delivers events in order.
struct OrderChecker {
    /// Tracks the last-seen event-time per key.
    last_seen: Arc<Mutex<HashMap<Vec<String>, DateTime<Utc>>>>,
}

impl OrderChecker {
    fn new() -> Self {
        Self {
            last_seen: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl map::Mapper for OrderChecker {
    async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
        let current_event_time = input.eventtime;
        let keys = input.keys.clone();

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

        state.insert(keys, current_event_time);

        // Log the current state of all tracked keys
        info!(
            state = ?state.iter().map(|(k, v)| (k.clone(), v.timestamp_millis())).collect::<Vec<_>>(),
            "Current order-checker state"
        );

        vec![map::Message::new(input.value).with_keys(input.keys)]
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    info!("Starting order-checker map server");

    map::Server::new(OrderChecker::new()).start().await
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
        let checker = OrderChecker::new();

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
        let checker = OrderChecker::new();

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
        let checker = OrderChecker::new();

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
        let checker = OrderChecker::new();

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
        let checker = OrderChecker::new();
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
