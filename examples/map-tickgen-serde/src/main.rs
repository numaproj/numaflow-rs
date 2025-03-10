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
