use numaflow::function::{start_server, start_uds_server};
use std::env;

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

    use numaflow::function;
    use numaflow::function::{Datum, Message, Metadata};
    use tokio::sync::mpsc::Receiver;

    #[tonic::async_trait]
    impl function::FnHandler for Cat {
        async fn map_handle<T>(&self, input: T) -> Vec<function::Message>
        where
            T: function::Datum + Send + Sync + 'static,
        {
            vec![function::Message {
                keys: input.keys().clone(),
                value: input.value().clone(),
                tags: vec![],
            }]
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
