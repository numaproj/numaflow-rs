use numaflow::sourcetransform;

/// A simple source transformer which assigns event time to the current time in utc.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sourcetransform::Server::new(NowCat).start().await
}

struct NowCat;

#[tonic::async_trait]
impl sourcetransform::SourceTransformer for NowCat {
    async fn transform(
        &self,
        input: sourcetransform::SourceTransformRequest,
    ) -> Vec<sourcetransform::Message> {
        let message = sourcetransform::Message::new(input.value, chrono::offset::Utc::now())
            .keys(input.keys)
            .tags(vec![]);
        vec![message]
    }
}
