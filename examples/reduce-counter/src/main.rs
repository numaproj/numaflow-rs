use numaflow::reduce;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let handler_creator = counter::CounterCreator {};
    reduce::Server::new(handler_creator).start().await?;
    Ok(())
}

mod counter {
    use numaflow::reduce::{Message, ReduceRequest};
    use numaflow::reduce::{Metadata, Reducer};
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
            _md: &Metadata,
        ) -> Vec<Message> {
            let mut counter = 0;
            // the loop exits when input is closed which will happen only on close of book.
            while input.recv().await.is_some() {
                counter += 1;
            }
            vec![Message::new(counter.to_string().into_bytes()).with_keys(keys.clone())]
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::Metadata;

    use super::counter::CounterCreator;
    use numaflow::reduce::ReduceRequest;
    use numaflow::reduce::ReducerCreator;
    use numaflow::reduce::Reducer;  
    use tokio::sync::mpsc;

    fn create_request(value: Vec<u8>, keys: Vec<String>) -> ReduceRequest {
        ReduceRequest {
            keys,
            value,
            watermark: std::time::SystemTime::now().into(),
            eventtime: std::time::SystemTime::now().into(),
            headers: Default::default(),
        }
    }

    fn create_metadata() -> Metadata {
        Metadata {
            start: std::time::SystemTime::now().into(),
            end: std::time::SystemTime::now().into(),
        }
    }

    #[tokio::test]
    async fn test_counter_single_key() {
        let counter = CounterCreator {}.create();
        let (tx, rx) = mpsc::channel(10);

        for _ in 0..5 {
            let req = create_request(b"test".to_vec(), vec!["key1".to_string()]);
            tx.send(req).await.unwrap();
        }
        drop(tx);

        let metadata = create_metadata();
        let messages = counter.reduce(vec!["key1".to_string()], rx, &metadata).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, b"5");
        assert_eq!(messages[0].keys, Some(vec!["key1".to_string()]));
    }

    #[tokio::test]
    async fn test_counter_multiple_keys() {
        let counter = CounterCreator {}.create();
        let (tx, rx) = mpsc::channel(10);
        for _ in 0..3 {
            let req = create_request(b"test".to_vec(), vec!["keyA".to_string()]);
            tx.send(req).await.unwrap();
        }
        for _ in 0..7 {
            let req = create_request(b"test".to_vec(), vec!["keyB".to_string()]);
            tx.send(req).await.unwrap();
        }
        drop(tx);
        let metadata = create_metadata();
        let messages = counter
            .reduce(vec!["keyA".to_string(), "keyB".to_string()], rx, &metadata)
            .await;
        
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, b"10");
        assert_eq!(
            messages[0].keys,
            Some(vec!["keyA".to_string(), "keyB".to_string()])
        );
    }
}
