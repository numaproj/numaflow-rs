use numaflow::reduce::start_uds_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reduce_handler = counter::Counter::new();

    start_uds_server(reduce_handler).await?;

    Ok(())
}

mod counter {
    use numaflow::reduce::{Datum, Message};
    use numaflow::reduce::{Metadata, Reducer};
    use tokio::sync::mpsc::Receiver;
    use tonic::async_trait;

    pub(crate) struct Counter {}

    impl Counter {
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    #[async_trait]
    impl Reducer for Counter {
        async fn reduce<T: Datum + Send + Sync + 'static, U: Metadata + Send + Sync + 'static>(
            &self,
            keys: Vec<String>,
            mut input: Receiver<T>,
            md: &U,
        ) -> Vec<Message> {
            println!(
                "Entering into UDF {:?} {:?}",
                md.start_time(),
                md.end_time()
            );

            let mut counter = 0;
            // the loop exits when input is closed which will happen only on close of book.
            while (input.recv().await).is_some() {
                counter += 1;
            }

            println!(
                "Returning from UDF {:?} {:?}",
                md.start_time(),
                md.end_time()
            );
            let message=reduce::MessageBuilder::new().keys(keys.clone()).values(counter.to_string().into_bytes()).tags(vec![]).build();
            vec![message]
        }
    }
}
