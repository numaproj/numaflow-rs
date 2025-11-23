//! An example for simple User Defined Source. It generates a continuous increasing sequence of offsets
//! and some data for each call to [`numaflow::source::sourcer::read`].

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let source_handle = simple_source::SimpleSource::new();
    numaflow::source::Server::new(source_handle).start().await
}

pub(crate) mod simple_source {
    use chrono::Utc;
    use numaflow::source::{Message, Offset, SourceReadRequest, Sourcer};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{collections::HashSet, sync::RwLock};
    use tokio::sync::mpsc::Sender;

    /// SimpleSource is a data generator which generates monotonically increasing offsets and data. It is a shared state which is protected using Locks
    /// or Atomics to provide concurrent access. Numaflow actually does not require concurrent access but we are forced to do this because the SDK
    /// does not provide a mutable reference as explained in [`Sourcer`]
    pub(crate) struct SimpleSource {
        yet_to_ack: RwLock<HashSet<String>>,
        nacked: RwLock<HashSet<String>>,
        counter: AtomicUsize,
    }

    impl SimpleSource {
        pub(crate) fn new() -> Self {
            Self {
                yet_to_ack: RwLock::new(HashSet::new()),
                nacked: RwLock::new(HashSet::new()),
                counter: AtomicUsize::new(0),
            }
        }
    }

    #[tonic::async_trait]
    impl Sourcer for SimpleSource {
        /// read generates a sequence of offsets and data.
        async fn read(&self, request: SourceReadRequest, transmitter: Sender<Message>) {
            if !self.yet_to_ack.read().unwrap().is_empty() {
                return;
            }

            // if there are nacked message send them first and remove them from the nacked set
            // and return early
            let nacked = self.nacked.read().unwrap().clone();
            if !nacked.is_empty() {
                for offset in nacked {
                    transmitter
                        .send(self.create_message(offset).await)
                        .await
                        .unwrap();
                }
                // clear the nacked set
                self.nacked.write().unwrap().clear();
                return;
            }

            let event_time = Utc::now();
            let mut message_offsets = Vec::with_capacity(request.count);
            for i in 0..request.count {
                let offset = format!("{}-{}", event_time.timestamp_nanos_opt().unwrap(), i);
                transmitter
                    .send(self.create_message(offset.clone()).await)
                    .await
                    .unwrap();
                message_offsets.push(offset)
            }
            self.yet_to_ack.write().unwrap().extend(message_offsets)
        }

        async fn ack(&self, offset: Vec<Offset>) {
            for offset in offset {
                let x = &String::from_utf8(offset.offset).unwrap();
                self.yet_to_ack.write().unwrap().remove(x);
            }
        }

        /// returns the number of messages that are yet to be acknowledged & processed
        async fn pending(&self) -> Option<usize> {
            self.yet_to_ack.read().unwrap().len().into()
        }

        /// returns the partitions from which the data is being read from.
        async fn partitions(&self) -> Option<Vec<i32>> {
            Some(vec![0])
        }

        /// negatively acknowledge the offsets, removes the offset from the set and adds it back to
        /// the front of the queue
        async fn nack(&self, offset: Vec<Offset>) {
            // put these offsets to the front of the queue, so next read will pick them up
            for offset in offset {
                println!("Nacking offset: {:?}", offset.offset);
                let x = &String::from_utf8(offset.offset).unwrap();
                self.yet_to_ack.write().unwrap().remove(x);
                self.nacked.write().unwrap().insert(x.clone());
                self.counter.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    impl SimpleSource {
        async fn create_message(&self, offset: String) -> Message {
            let payload = self.counter.fetch_add(1, Ordering::Relaxed).to_string();
            Message {
                value: payload.into_bytes(),
                event_time: Utc::now(),
                offset: Offset {
                    offset: offset.clone().into_bytes(),
                    partition_id: 0,
                },
                keys: vec![],
                headers: Default::default(),
                user_metadata: None,
            }
        }
    }
}
