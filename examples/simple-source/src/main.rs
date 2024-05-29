///! An example for simple User Defined Source. It generates a continuous increasing sequence of offsets and some data for each call to [`numaflow::source::sourcer::read`].

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let source_handle = simple_source::SimpleSource::new();
    numaflow::source::Server::new(source_handle).start().await
}

pub(crate) mod simple_source {
    use std::{
        collections::HashSet,
        sync::atomic::{AtomicUsize, Ordering},
        sync::RwLock,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    use numaflow::source::{Message, Offset, SourceReadRequest, Sourcer};
    use tokio::{sync::mpsc::Sender, time::Instant};
    use tonic::async_trait;

    /// SimpleSource is a data generator which generates monotonically increasing offsets and data. It is a shared state which is protected using Locks
    /// or Atomics to provide concurrent access. Numaflow actually does not require concurrent access but we are forced to do this because the SDK
    /// does not provide a mutable reference as explained in [`numaflow::source::Sourcer`]
    pub(crate) struct SimpleSource {
        read_idx: AtomicUsize,
        yet_to_ack: RwLock<HashSet<u32>>,
    }

    impl SimpleSource {
        pub fn new() -> Self {
            Self {
                read_idx: AtomicUsize::new(0),
                yet_to_ack: RwLock::new(HashSet::new()),
            }
        }
    }

    #[async_trait]
    impl Sourcer for SimpleSource {
        async fn read(&self, source_request: SourceReadRequest, transmitter: Sender<Message>) {
            if !self.yet_to_ack.read().unwrap().is_empty() {
                return;
            }
            let start = Instant::now();

            for i in 1..=source_request.count {
                // if the time elapsed is greater than the timeout, return
                if start.elapsed().as_millis() > source_request.timeout.as_millis() {
                    return;
                }

                // increment the read_idx which is used as the offset
                self.read_idx
                    .store(self.read_idx.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
                let offset = self.read_idx.load(Ordering::Relaxed);
                let mut headers=HashMap::new();
                headers.insert(String::from("key"),String::from("key"));
                let shared_headers = Arc::new(headers);
                // send the message to the transmitter
                transmitter
                    .send(Message {
                        value: format!("{i} at {offset}").into_bytes(),
                        offset: Offset {
                            offset: offset.to_be_bytes().to_vec(),
                            partition_id: 0,
                        },
                        event_time: chrono::offset::Utc::now(),
                        keys: vec![],
                        headers:Arc::clone(&shared_headers), // Cloning the Arc, not the HashMap,
                    })
                    .await
                    .unwrap();

                // add the entry to hashmap to mark the offset as pending to-be-acked
                let mut yet_to_ack = self.yet_to_ack.write().expect("lock has been poisoned");
                yet_to_ack.insert(offset as u32);
            }
        }

        async fn ack(&self, offsets: Vec<Offset>) {
            // remove the offsets from yet_to_ack since we have received an ack for it
            for offset in offsets {
                let val = u32::from_be_bytes(offset.offset[0..4].try_into().unwrap());
                // remove the entry from pending table after acquiring lock
                self.yet_to_ack.write().unwrap().remove(&val);
            }
        }

        async fn pending(&self) -> usize {
            // pending for simple source is zero since we are not reading from any external source
            0
        }

        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![1])
        }
    }
}
