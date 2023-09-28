///! An example for simple User Defined Source. It generates a continuous increasing sequence of offsets and some data for each call to [`numaflow::source::sourcer::read`].
///! **NOTE**
///! As per the standard convention, both  [`numaflow::source::sourcer::read`](Read) and  [`numaflow::source::sourcer::ack`](Ack) should be mutable
///! since they have to update some state. Unfortunately the SDK provides only a shared reference self and thus makes it unmutable. This is because
///! gRPC [tonic] provides only a shared reference for its traits. This means, the implementer for trait will have use SharedState pattern to mutate
///! the values as recommended in [issue-427]. This might change in future as async traits evolves.
///!
///! [tonic]: https://github.com/hyperium/tonic/
///! [issue-427]: https://github.com/hyperium/tonic/issues/427

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source_handle = simple_source::SimpleSource::new();
    numaflow::source::start_uds_server(source_handle).await?;

    Ok(())
}

pub(crate) mod simple_source {
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
        sync::RwLock,
    };

    use numaflow::source::{Message, Offset, SourceReadRequest, Sourcer};
    use tokio::{sync::mpsc::Sender, time::Instant};
    use tonic::async_trait;

    pub(crate) struct SimpleSource {
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

            for i in 1..=source_request.count {
                // if the time elapsed is greater than the timeout, return
                if start.elapsed().as_millis() > source_request.timeout.as_millis() {
                    return;
                }

                // increment the read_idx which is used as the offset
                self.read_idx
                    .store(self.read_idx.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
                let offset = self.read_idx.load(Ordering::Relaxed);

                // send the message to the transmitter
                transmitter
                    .send(Message {
                        value: format!("{i} at {offset}").into_bytes(),
                        offset: Offset {
                            offset: offset.to_be_bytes().to_vec(),
                            partition_id: "0".to_string(),
                        },
                        event_time: chrono::offset::Utc::now(),
                        keys: vec![],
                    })
                    .await
                    .unwrap();

                // add the entry to hashmap to mark the offset as pending to-be-acked
                match self.yet_to_ack.write() {
                    Ok(mut guard) => guard.insert(offset as u32, true),
                    Err(_) => panic!("lock has been poisoned!"),
                };
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
    }
}
