use std::collections::HashMap;
use std::sync::Mutex;

use numaflow::sourcetransform;

/// Payload that triggers the "always fail" behavior. A message whose body equals
/// this string is failed on every delivery — it never recovers.
const ALWAYS_FAIL_PAYLOAD: &str = "fail";

/// Number of times a recoverable (non-"fail") message is failed before it is
/// passed through unchanged. Overridable via the `FAIL_COUNT` env var
const DEFAULT_FAIL_COUNT: u32 = 2;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    let fail_count = std::env::var("FAIL_COUNT")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(DEFAULT_FAIL_COUNT);
    sourcetransform::Server::new(ConditionalFail::new(fail_count))
        .start()
        .await
}

/// A source transformer used by the transformer `retryStrategy` e2e tests. Its
/// behavior is selected by the message payload so a single image covers every
/// scenario:
///
/// - body == "fail": always emit a FAIL-tagged message (never recovers).
/// - anything else: emit a FAIL-tagged message the first `fail_count` deliveries
///   of that exact payload, then pass it through.
///
/// The recover path is keyed per payload value so that, exactly one message
/// accumulates a deterministic `fail_count` failures before succeeding.
struct ConditionalFail {
    /// Times a recoverable payload is failed before it is allowed through.
    fail_count: u32,
    /// Per-payload count of how many times each value has been delivered so far.
    seen: Mutex<HashMap<String, u32>>,
}

impl ConditionalFail {
    fn new(fail_count: u32) -> Self {
        Self {
            fail_count,
            seen: Mutex::new(HashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl sourcetransform::SourceTransformer for ConditionalFail {
    async fn transform(
        &self,
        input: sourcetransform::SourceTransformRequest,
    ) -> Vec<sourcetransform::Message> {
        let body = String::from_utf8_lossy(&input.value).to_string();

        // Always-fail messages never recover. Event time is carried through even
        // on failure so the watermark can still advance.
        if body == ALWAYS_FAIL_PAYLOAD {
            return vec![sourcetransform::Message::message_to_fail(input.eventtime)];
        }

        // Recoverable messages fail `fail_count` times, then pass through.
        let attempts = {
            let mut seen = self.seen.lock().expect("seen mutex poisoned");
            let counter = seen.entry(body).or_insert(0);
            *counter += 1;
            *counter
        };

        if attempts <= self.fail_count {
            vec![sourcetransform::Message::message_to_fail(input.eventtime)]
        } else {
            vec![sourcetransform::Message::new(input.value, input.eventtime).with_keys(input.keys)]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use numaflow::sourcetransform::{
        SourceTransformRequest, SourceTransformer, SystemMetadata, UserMetadata,
    };

    /// Must match the FAIL constant in numaflow-core (message.rs) and the SDK
    /// (numaflow/src/shared.rs). Kept as a literal so a mismatch is caught here.
    const FAIL_TAG: &str = "U+005C__FAIL__";

    fn create_request(value: Vec<u8>) -> SourceTransformRequest {
        SourceTransformRequest {
            keys: vec!["key".to_string()],
            value,
            watermark: Utc::now(),
            eventtime: Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap(),
            headers: Default::default(),
            user_metadata: UserMetadata::new(),
            system_metadata: SystemMetadata::new(),
        }
    }

    fn is_failed(msg: &sourcetransform::Message) -> bool {
        msg.tags == Some(vec![FAIL_TAG.to_string()])
    }

    #[tokio::test]
    async fn always_fail_payload_fails_every_time() {
        let handler = ConditionalFail::new(2);
        for _ in 0..5 {
            let msgs = handler.transform(create_request(b"fail".to_vec())).await;
            assert_eq!(msgs.len(), 1);
            assert!(is_failed(&msgs[0]), "'fail' payload must always be failed");
        }
    }

    #[tokio::test]
    async fn recoverable_payload_fails_k_times_then_succeeds() {
        let handler = ConditionalFail::new(2);

        // First `fail_count` deliveries are failed.
        for attempt in 1..=2 {
            let msgs = handler.transform(create_request(b"recover".to_vec())).await;
            assert!(is_failed(&msgs[0]), "delivery {attempt} should be failed");
        }

        // The next delivery passes through unchanged.
        let msgs = handler.transform(create_request(b"recover".to_vec())).await;
        assert_eq!(msgs.len(), 1);
        assert!(
            !is_failed(&msgs[0]),
            "message should recover after fail_count"
        );
        assert_eq!(msgs[0].value, b"recover", "value should be preserved");
        assert_eq!(msgs[0].keys, Some(vec!["key".to_string()]));
    }

    #[tokio::test]
    async fn recoverable_payloads_are_tracked_independently() {
        let handler = ConditionalFail::new(1);

        // First delivery of each distinct payload fails.
        assert!(is_failed(
            &handler.transform(create_request(b"a".to_vec())).await[0]
        ));
        assert!(is_failed(
            &handler.transform(create_request(b"b".to_vec())).await[0]
        ));

        // Second delivery of "a" recovers, independent of "b".
        assert!(!is_failed(
            &handler.transform(create_request(b"a".to_vec())).await[0]
        ));
    }
}
