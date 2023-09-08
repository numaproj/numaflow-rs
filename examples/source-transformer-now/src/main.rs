use numaflow::sourcetransform::start_uds_server;

/// A simple source transformer which assigns event time to the current time in utc.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transformer_handler = now_transformer::Now::new();

    start_uds_server(transformer_handler).await?;

    Ok(())
}

pub(crate) mod now_transformer {
    use numaflow::sourcetransform::{Datum, Message, SourceTransformer};
    use tonic::async_trait;

    pub(crate) struct Now {}

    impl Now {
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    #[async_trait]
    impl SourceTransformer for Now {
        async fn transform<T: Datum + Send + Sync + 'static>(&self, input: T) -> Vec<Message> {
            let mut reponse = vec![];
            reponse.push(Message {
                value: input.value().clone(),
                keys: input.keys().clone(),
                tags: vec![],
                event_time: chrono::offset::Utc::now(),
            });
            reponse
        }
    }
}
