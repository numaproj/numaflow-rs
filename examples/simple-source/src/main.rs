#[tokio::main]
async fn main() {
    let source_handle = simple_source::SimpleSource::new();
}

pub(crate) mod simple_source {
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use numaflow::source::{Message, Offset, SourceReadRequest, Sourcer};
    use tokio::{
        sync::{mpsc::Sender, RwLock},
        time::Instant,
    };
    use tonic::async_trait;

    pub struct SimpleSource {
        read_idx: AtomicUsize,
        yet_to_ack: RwLock<HashMap<u32, bool>>,
    }

    impl SimpleSource {
        pub fn new() -> Self {
            Self {
                read_idx: AtomicUsize::new(0),
                yet_to_ack: RwLock::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl Sourcer for SimpleSource {
        async fn read(&self, source_request: SourceReadRequest, transmitter: Sender<Message>) {
            let start = Instant::now();

            for _i in 1..=source_request.count {
                // if the time elapsed is greater than the timeout, return
                if start.elapsed().as_millis() > source_request.timeout.as_millis() {
                    return;
                }

                // increment the read_idx which is used as the offset
                self.read_idx
                    .store(self.read_idx.load(Ordering::Relaxed) + 1, Ordering::Relaxed);

                // send the message to the transmitter
                transmitter
                    .send(Message {
                        value: self.read_idx.load(Ordering::Relaxed).to_be_bytes().to_vec(),
                        offset: Offset {
                            offset: self.read_idx.load(Ordering::Relaxed).to_be_bytes().to_vec(),
                            partition_id: "0".to_string(),
                        },
                        event_time: chrono::offset::Utc::now(),
                        keys: vec![],
                    })
                    .await
                    .unwrap();

                // add the offset to yet_to_ack since we have not yet received an ack for it
                self.yet_to_ack
                    .write()
                    .await
                    .insert(self.read_idx.load(Ordering::Relaxed) as u32, true);
            }
        }

        async fn ack(&self, offsets: Vec<Offset>) {
            // remove the offsets from yet_to_ack since we have received an ack for it
            for offset in offsets {
                let val = u32::from_be_bytes(offset.offset[0..4].try_into().unwrap());
                self.yet_to_ack.write().await.remove(&val);
            }
        }

        /// pending for simple source is zero since we are not reading from any external source
        async fn pending(&self) -> usize {
            0
        }
    }
}
