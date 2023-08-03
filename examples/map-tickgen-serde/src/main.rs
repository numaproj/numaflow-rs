use numaflow::function::start_uds_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let map_handler = tickgen::TickGen::new();

    start_uds_server(map_handler).await?;

    Ok(())
}

pub(crate) mod tickgen {
    use chrono::{SecondsFormat, TimeZone, Utc};
    use numaflow::function;
    use numaflow::function::{Datum, Message, Metadata};
    use serde::Serialize;
    use tokio::sync::mpsc::Receiver;

    pub(crate) struct TickGen {}

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

    #[cfg(test)]
    mod tests {
        use super::*;
        use chrono::{SecondsFormat, TimeZone, Utc};

        #[test]
        fn deserialize() {
            let input = r#"{"Data":{"value":5},"Createdts":1689723721606016637}"#;
            let payload: Payload = serde_json::from_str(input).unwrap();
            assert_eq!(payload.data.value, 5);
            assert_eq!(payload.created_ts, 1689723721606016637);
        }

        #[test]
        fn to_rfc3339nanos() {
            let input = r#"{"Data":{"value":5},"Createdts":1689723721606016637}"#;
            let payload: Payload = serde_json::from_str(input).unwrap();
            assert_eq!(
                Utc.timestamp_nanos(payload.created_ts)
                    .to_rfc3339_opts(SecondsFormat::Nanos, true),
                "2023-07-18T23:42:01.606016637Z"
            );
        }
    }

    impl TickGen {
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    #[derive(Serialize)]
    struct ResultPayload {
        value: u64,
        time: String,
    }

    #[tonic::async_trait]
    impl function::FnHandler for TickGen {
        async fn map_handle<T: function::Datum + Send + Sync + 'static>(
            &self,
            input: T,
        ) -> Vec<function::Message> {
            let value = input.value();
            if let Ok(payload) = serde_json::from_slice::<Payload>(value) {
                let ts = Utc
                    .timestamp_nanos(payload.created_ts)
                    .to_rfc3339_opts(SecondsFormat::Nanos, true);
                vec![function::Message {
                    keys: input.keys().clone(),
                    value: serde_json::to_vec(&ResultPayload {
                        value: payload.data.value,
                        time: ts,
                    })
                    .unwrap_or(vec![]),
                    tags: vec![],
                }]
            } else {
                vec![]
            }
        }

        async fn reduce_handle<
            T: Datum + Send + Sync + 'static,
            U: Metadata + Send + Sync + 'static,
        >(
            &self,
            _: Vec<String>,
            _: Receiver<T>,
            _: &U,
        ) -> Vec<Message> {
            todo!()
        }
    }
}
