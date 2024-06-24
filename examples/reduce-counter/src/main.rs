use numaflow::reduce;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let handler_creator = counter::CounterCreator {};
    reduce::Server::new(handler_creator).start().await?;
    Ok(())
}

mod counter {
    use numaflow::reduce::{Message, ReduceRequest};
    use numaflow::reduce::{Reducer, Metadata};
    use tokio::sync::mpsc::Receiver;
    use tonic::async_trait;

    pub(crate) struct Counter {}

    pub(crate) struct CounterCreator {}

    impl numaflow::reduce::ReducerCreator for CounterCreator {
        type R = Counter;

        fn create(&self) -> Self::R {
            Counter::new()
        }
    }

    impl Counter {
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    #[async_trait]
    impl Reducer for Counter {
        async fn reduce(
            &self,
            keys: Vec<String>,
            mut input: Receiver<ReduceRequest>,
            md: &Metadata,
        ) -> Vec<Message> {
            let mut counter = 0;
            println!("Started counter reducer with metadata {:?}", md);
            // the loop exits when input is closed which will happen only on close of book.
            while input.recv().await.is_some() {
                counter += 1;
            }
            println!("Counter reducer received {} messages", counter);
            let message = Message::new(counter.to_string().into_bytes()).tags(vec![]).keys(keys.clone());
            vec![message]
        }
    }
}