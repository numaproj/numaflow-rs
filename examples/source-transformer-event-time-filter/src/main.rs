use std::error::Error;
use numaflow::sourcetransform;


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    sourcetransform::Server::new(EventTimeFilter).start().await
}

struct EventTimeFilter;

impl sourcetransform::SourceTransformer for EventTimeFilter{

}