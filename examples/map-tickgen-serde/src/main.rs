use chrono::{SecondsFormat, TimeZone, Utc};
use numaflow::map;
use numaflow::map::Message;
use serde::Serialize;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    map::Server::new(TickGen).start().await
}

struct TickGen;

#[derive(serde::Deserialize)]
struct Data {
    value: u64,
}

#[derive(serde::Deserialize)]
struct Payload {
    #[serde(rename = "Data")]
    data: Data,
    #[serde(rename = "Createdts")]
    created_ts: i64,
}

#[derive(Serialize)]
struct ResultPayload {
    value: u64,
    time: String,
}

#[tonic::async_trait]
impl map::Mapper for TickGen {
    async fn map(&self, input: map::MapRequest) -> Vec<Message> {
        let Ok(payload) = serde_json::from_slice::<Payload>(&input.value) else {
            return vec![];
        };
        let ts = Utc
            .timestamp_nanos(payload.created_ts)
            .to_rfc3339_opts(SecondsFormat::Nanos, true);
        let message = map::Message::new(
            serde_json::to_vec(&ResultPayload {
                value: payload.data.value,
                time: ts,
            })
            .unwrap_or_default(),
        )
        .with_keys(input.keys.clone());
        vec![message]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use numaflow::map::{MapRequest, Mapper};
    use serde_json::json;

    /// Helper function to create a test MapRequest
    fn create_request(value: Vec<u8>, keys: Vec<String>) -> MapRequest {
        MapRequest {
            keys,
            value,
            watermark: std::time::SystemTime::now().into(),
            eventtime: std::time::SystemTime::now().into(),
            headers: Default::default(),
        }
    }

    #[tokio::test]
    async fn test_valid_payload_transformation() {
        let tickgen = TickGen;

        // Create valid input JSON
        let input_json = json!({
            "Data": {
                "value": 42
            },
            "Createdts": 1234567890000000000i64
        });
        let input_bytes = serde_json::to_vec(&input_json).unwrap();
        let request = create_request(input_bytes, vec!["key1".to_string()]);

        let messages = tickgen.map(request).await;

        assert_eq!(messages.len(), 1, "Should return one message");

        // Deserialize output to verify
        let output: serde_json::Value = serde_json::from_slice(&messages[0].value).unwrap();
        assert_eq!(output["value"], 42);
        assert_eq!(output["time"], "2009-02-13T23:31:30.000000000Z");
        assert_eq!(messages[0].keys, Some(vec!["key1".to_string()]));
    }

    #[tokio::test]
    async fn test_preserves_keys() {
        let tickgen = TickGen;

        let input_json = json!({
            "Data": {"value": 100},
            "Createdts": 1000000000000000000i64
        });
        let request = create_request(
            serde_json::to_vec(&input_json).unwrap(),
            vec!["key1".to_string(), "key2".to_string(), "key3".to_string()],
        );

        let messages = tickgen.map(request).await;

        assert_eq!(
            messages[0].keys,
            Some(vec![
                "key1".to_string(),
                "key2".to_string(),
                "key3".to_string()
            ])
        );
    }

    #[tokio::test]
    async fn test_zero_value() {
        let tickgen = TickGen;

        let input_json = json!({
            "Data": {"value": 0},
            "Createdts": 0i64
        });
        let request = create_request(
            serde_json::to_vec(&input_json).unwrap(),
            vec!["key1".to_string()],
        );

        let messages = tickgen.map(request).await;

        assert_eq!(messages.len(), 1);
        let output: serde_json::Value = serde_json::from_slice(&messages[0].value).unwrap();
        assert_eq!(output["value"], 0);
        assert_eq!(
            output["time"], "1970-01-01T00:00:00.000000000Z",
            "Epoch time for timestamp 0"
        );
    }

    #[tokio::test]
    async fn test_large_value() {
        let tickgen = TickGen;

        let input_json = json!({
            "Data": {"value": u64::MAX},
            "Createdts": 1234567890000000000i64
        });
        let request = create_request(
            serde_json::to_vec(&input_json).unwrap(),
            vec!["key1".to_string()],
        );

        let messages = tickgen.map(request).await;

        assert_eq!(messages.len(), 1);
        let output: serde_json::Value = serde_json::from_slice(&messages[0].value).unwrap();
        assert_eq!(output["value"], u64::MAX);
    }

    #[tokio::test]
    async fn test_invalid_json_returns_empty() {
        let tickgen = TickGen;

        // Invalid JSON
        let request = create_request(b"not valid json".to_vec(), vec!["key1".to_string()]);

        let messages = tickgen.map(request).await;

        assert_eq!(
            messages.len(),
            0,
            "Should return empty vec for invalid JSON"
        );
    }

    #[tokio::test]
    async fn test_missing_data_field_returns_empty() {
        let tickgen = TickGen;

        // Missing "Data" field
        let input_json = json!({
            "Createdts": 1234567890000000000i64
        });
        let request = create_request(
            serde_json::to_vec(&input_json).unwrap(),
            vec!["key1".to_string()],
        );

        let messages = tickgen.map(request).await;

        assert_eq!(
            messages.len(),
            0,
            "Should return empty vec for missing Data field"
        );
    }

    #[tokio::test]
    async fn test_missing_createdts_field_returns_empty() {
        let tickgen = TickGen;

        // Missing "Createdts" field
        let input_json = json!({
            "Data": {"value": 42}
        });
        let request = create_request(
            serde_json::to_vec(&input_json).unwrap(),
            vec!["key1".to_string()],
        );

        let messages = tickgen.map(request).await;

        assert_eq!(
            messages.len(),
            0,
            "Should return empty vec for missing Createdts field"
        );
    }

    #[tokio::test]
    async fn test_wrong_field_names_returns_empty() {
        let tickgen = TickGen;

        // Wrong field names (lowercase)
        let input_json = json!({
            "data": {"value": 42},
            "createdts": 1234567890000000000i64
        });
        let request = create_request(
            serde_json::to_vec(&input_json).unwrap(),
            vec!["key1".to_string()],
        );

        let messages = tickgen.map(request).await;

        assert_eq!(
            messages.len(),
            0,
            "Should return empty vec for wrong field names"
        );
    }

    #[tokio::test]
    async fn test_empty_input_returns_empty() {
        let tickgen = TickGen;

        let request = create_request(vec![], vec!["key1".to_string()]);

        let messages = tickgen.map(request).await;

        assert_eq!(messages.len(), 0, "Should return empty vec for empty input");
    }

    #[tokio::test]
    async fn test_negative_timestamp() {
        let tickgen = TickGen;

        // Negative timestamp (before epoch)
        let input_json = json!({
            "Data": {"value": 42},
            "Createdts": -1000000000000000000i64
        });
        let request = create_request(
            serde_json::to_vec(&input_json).unwrap(),
            vec!["key1".to_string()],
        );

        let messages = tickgen.map(request).await;

        assert_eq!(messages.len(), 1);
        let output: serde_json::Value = serde_json::from_slice(&messages[0].value).unwrap();
        assert_eq!(output["value"], 42);
        // Should handle negative timestamp (before 1970)
        assert!(output["time"].as_str().unwrap().starts_with("1938"));
    }

    #[tokio::test]
    async fn test_wrong_value_type_returns_empty() {
        let tickgen = TickGen;

        // value is string instead of u64
        let input_json = json!({
            "Data": {"value": "not a number"},
            "Createdts": 1234567890000000000i64
        });
        let request = create_request(
            serde_json::to_vec(&input_json).unwrap(),
            vec!["key1".to_string()],
        );

        let messages = tickgen.map(request).await;

        assert_eq!(
            messages.len(),
            0,
            "Should return empty vec for wrong value type"
        );
    }

    #[tokio::test]
    async fn test_wrong_timestamp_type_returns_empty() {
        let tickgen = TickGen;

        // Createdts is string instead of i64
        let input_json = json!({
            "Data": {"value": 42},
            "Createdts": "not a number"
        });
        let request = create_request(
            serde_json::to_vec(&input_json).unwrap(),
            vec!["key1".to_string()],
        );

        let messages = tickgen.map(request).await;

        assert_eq!(
            messages.len(),
            0,
            "Should return empty vec for wrong timestamp type"
        );
    }

    #[tokio::test]
    async fn test_nested_data_structure() {
        let tickgen = TickGen;

        let input_json = json!({
            "Data": {
                "value": 999
            },
            "Createdts": 1609459200000000000i64  // 2021-01-01 00:00:00 UTC
        });
        let request = create_request(
            serde_json::to_vec(&input_json).unwrap(),
            vec!["key1".to_string()],
        );

        let messages = tickgen.map(request).await;

        assert_eq!(messages.len(), 1);
        let output: serde_json::Value = serde_json::from_slice(&messages[0].value).unwrap();
        assert_eq!(output["value"], 999);
        assert_eq!(output["time"], "2021-01-01T00:00:00.000000000Z");
    }

    #[tokio::test]
    async fn test_extra_fields_ignored() {
        let tickgen = TickGen;

        // Extra fields should be ignored
        let input_json = json!({
            "Data": {"value": 42},
            "Createdts": 1234567890000000000i64,
            "ExtraField": "should be ignored"
        });
        let request = create_request(
            serde_json::to_vec(&input_json).unwrap(),
            vec!["key1".to_string()],
        );

        let messages = tickgen.map(request).await;

        assert_eq!(messages.len(), 1);
        let output: serde_json::Value = serde_json::from_slice(&messages[0].value).unwrap();
        assert_eq!(output["value"], 42);
    }
}
