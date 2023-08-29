use numaflow::map::start_uds_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let map_handler = cat::Cat::new();

    start_uds_server(map_handler).await?;

    Ok(())
}

pub(crate) mod cat {
    pub(crate) struct Cat {}

    impl Cat {
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    use numaflow::map;

    #[tonic::async_trait]
    impl map::Mapper for Cat {
        async fn map<T>(&self, input: T) -> Vec<map::Message>
        where
            T: map::Datum + Send + Sync + 'static,
        {
            vec![map::Message {
                keys: input.keys().clone(),
                value: input.value().clone(),
                tags: vec![],
            }]
        }
    }
}
