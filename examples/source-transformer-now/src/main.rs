use numaflow::sourcetransform;
use std::error::Error;

/// A simple source transformer which assigns event time to the current time in utc.

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    sourcetransform::Server::new(NowCat).start().await
}

struct NowCat;

#[tonic::async_trait]
impl sourcetransform::SourceTransformer for NowCat {
    async fn transform(
        &self,
        input: sourcetransform::SourceTransformRequest,
    ) -> Vec<sourcetransform::Message> {
        vec![sourcetransform::Message {
            keys: input.keys,
            value: input.value,
            event_time: chrono::offset::Utc::now(),
            tags: vec![],
        }]
    }
}
