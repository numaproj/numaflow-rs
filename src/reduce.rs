use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataMap;
use tonic::{async_trait, Request, Response, Status};

use crate::reduce::reducer::{
    reduce_response, reduce_server, ReadyResponse, ReduceRequest, ReduceResponse,
};
use crate::shared;

use self::reducer::reduce_server::Reduce;

mod reducer {
    tonic::include_proto!("reduce.v1");
}

struct ReduceService<T> {
    handler: Arc<T>,
}

/// Trait implemented Reduce reduce handler.
#[async_trait]
pub trait Reducer {
    /// reduce_handle is provided with a set of keys, a channel of [`Datum`], and [`Metadata`]. It
    /// returns 0, 1, or more results as a [`Vec`] of [`Message`]. Reduce is a stateful operation and
    /// the channel is for the collection of keys and for that time [Window].
    /// You can read more about reduce [here](https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/reduce/).
    ///
    /// # Example
    ///
    /// Below is a reduce code to count the number of elements for a given set of keys and window.
    ///
    /// ```rust
    // use numaflow::reduce::start_uds_server;
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let reduce_handler = counter::Counter::new();
    ///     start_uds_server(reduce_handler).await?;
    ///     Ok(())
    /// }
    /// mod counter {
    ///     use numaflow::reduce::{Datum, Message};
    ///     use numaflow::reduce::{Reducer, Metadata};
    ///     use tokio::sync::mpsc::Receiver;
    ///     use tonic::async_trait;
    ///     pub(crate) struct Counter {}
    ///     impl Counter {
    ///         pub(crate) fn new() -> Self {
    ///             Self {}
    ///         }
    ///     }
    ///     #[async_trait]
    ///     impl Reducer for Counter {
    ///         async fn reduce<
    ///             T: Datum + Send + Sync + 'static,
    ///             U: Metadata + Send + Sync + 'static,
    ///         >(
    ///             &self,
    ///             keys: Vec<String>,
    ///             mut input: Receiver<T>,
    ///             md: &U,
    ///         ) -> Vec<Message> {
    ///             let mut counter = 0;
    ///             // the loop exits when input is closed which will happen only on close of book.
    ///             while (input.recv().await).is_some() {
    ///                 counter += 1;
    ///             }
    ///             vec![Message {
    ///                 keys: keys.clone(),
    ///                 value: counter.to_string().into_bytes(),
    ///                 tags: vec![],
    ///             }]
    ///         }
    ///     }
    /// }
    ///```
    /// [Window]: https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/windowing/
    async fn reduce<T: Datum + Send + Sync + 'static, U: Metadata + Send + Sync + 'static>(
        &self,
        keys: Vec<String>,
        input: mpsc::Receiver<T>,
        md: &U,
    ) -> Vec<Message>;
}

/// IntervalWindow is the start and end boundary of the window.
struct IntervalWindow {
    // st is start time
    st: DateTime<Utc>,
    // et is end time
    et: DateTime<Utc>,
}

impl IntervalWindow {
    fn new(st: DateTime<Utc>, et: DateTime<Utc>) -> Self {
        Self { st, et }
    }
}

/// Metadata are additional information passed into the [`Reducer::reduce`].
pub trait Metadata {
    /// start_time is the window start time.
    fn start_time(&self) -> &DateTime<Utc>;
    /// end_time is the window end time.
    fn end_time(&self) -> &DateTime<Utc>;
}

impl Metadata for IntervalWindow {
    fn start_time(&self) -> &DateTime<Utc> {
        &self.st
    }

    fn end_time(&self) -> &DateTime<Utc> {
        &self.et
    }
}

/// Message is the response from the user's [`Reducer::reduce`].
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection. It is mainly used in creating a partition in [`Reducer::reduce`].
    pub keys: Vec<String>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags: Vec<String>,
}

/// Datum trait represents an incoming element into the reduce handle of [`Reducer`].
pub trait Datum {
    /// keys are the keys in the (key, value) terminology of map/reduce paradigm.
    fn keys(&self) -> &Vec<String>;
    /// value is the value in (key, value) terminology of map/reduce paradigm.
    fn value(&self) -> &Vec<u8>;
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this
    /// time.
    fn watermark(&self) -> DateTime<Utc>;
    /// event_time is the time of the element as seen at source or aligned after a reduce operation.
    fn event_time(&self) -> DateTime<Utc>;
}

/// Owned copy of ReduceRequest from Datum.
struct OwnedReduceRequest {
    keys: Vec<String>,
    value: Vec<u8>,
    watermark: DateTime<Utc>,
    eventtime: DateTime<Utc>,
}

impl OwnedReduceRequest {
    fn new(mr: ReduceRequest) -> Self {
        Self {
            keys: mr.keys,
            value: mr.value,
            watermark: shared::utc_from_timestamp(mr.watermark),
            eventtime: shared::utc_from_timestamp(mr.event_time),
        }
    }
}

impl Datum for OwnedReduceRequest {
    fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    fn value(&self) -> &Vec<u8> {
        &self.value
    }

    fn watermark(&self) -> DateTime<Utc> {
        self.watermark
    }

    fn event_time(&self) -> DateTime<Utc> {
        self.eventtime
    }
}

// key delimiter
const KEY_JOIN_DELIMITER: &str = ":";
// grpc window metadata
const WIN_START_TIME: &str = "x-numaflow-win-start-time";
const WIN_END_TIME: &str = "x-numaflow-win-end-time";

// extract start and end time from the gRPC MetadataMap
// https://youtu.be/s5S2Ed5T-dc?t=662
fn get_window_details(request: &MetadataMap) -> (DateTime<Utc>, DateTime<Utc>) {
    let (st, et) = (
        request
            .get(WIN_START_TIME)
            .unwrap_or_else(|| panic!("expected key {}", WIN_START_TIME))
            .to_str()
            .unwrap()
            .to_string()
            .parse::<i64>()
            .unwrap(),
        request
            .get(WIN_END_TIME)
            .unwrap_or_else(|| panic!("expected key {}", WIN_END_TIME))
            .to_str()
            .unwrap()
            .parse::<i64>()
            .unwrap(),
    );

    (
        Utc.timestamp_millis_opt(st).unwrap(),
        Utc.timestamp_millis_opt(et).unwrap(),
    )
}

#[async_trait]
impl<T> Reduce for ReduceService<T>
where
    T: Reducer + Send + Sync + 'static,
{
    type ReduceFnStream = ReceiverStream<Result<ReduceResponse, Status>>;
    async fn reduce_fn(
        &self,
        request: Request<tonic::Streaming<ReduceRequest>>,
    ) -> Result<Response<Self::ReduceFnStream>, Status> {
        // get gRPC window from metadata
        let (start_win, end_win) = get_window_details(request.metadata());
        let md = Arc::new(IntervalWindow::new(start_win, end_win));

        let mut key_to_tx: HashMap<String, Sender<OwnedReduceRequest>> = HashMap::new();

        // we will be creating a set of tasks for this stream
        let mut set = JoinSet::new();

        let mut stream = request.into_inner();

        while let Some(datum) = stream.message().await.unwrap() {
            let task_name = datum.keys.join(KEY_JOIN_DELIMITER);

            if let Some(tx) = key_to_tx.get(&task_name) {
                tx.send(OwnedReduceRequest::new(datum)).await.unwrap();
            } else {
                // channel to send data to the user's reduce handle
                let (tx, rx) = mpsc::channel::<OwnedReduceRequest>(1);

                // since we are calling this in a loop, we need make sure that there is reference counting
                // and the lifetime of self is more than the async function.
                // try Arc<Self> https://doc.rust-lang.org/reference/items/associated-items.html#methods ?
                let v = Arc::clone(&self.handler);
                let m = Arc::clone(&md);

                // spawn task for each unique key
                let keys = datum.keys.clone();
                set.spawn(async move { v.reduce(keys, rx, m.as_ref()).await });

                // write data into the channel
                tx.send(OwnedReduceRequest::new(datum)).await.unwrap();

                // save the key and for future look up as long as the stream is active
                key_to_tx.insert(task_name, tx);
            }
        }

        // close all the tx channels to tasks to close their corresponding rx
        key_to_tx.clear();

        // channel to respond to numaflow main car as it expects streaming results.
        let (tx, rx) = mpsc::channel::<Result<ReduceResponse, Status>>(1);

        // start the result streamer
        tokio::spawn(async move {
            while let Some(res) = set.join_next().await {
                let messages = res.unwrap();
                let mut datum_responses = vec![];
                for message in messages {
                    datum_responses.push(reduce_response::Result {
                        keys: message.keys,
                        value: message.value,
                        tags: message.tags,
                    });
                }
                // stream it out to the client
                tx.send(Ok(ReduceResponse {
                    results: datum_responses,
                }))
                .await
                .unwrap();
            }
        });

        // return the rx as the streaming endpoint
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<ReadyResponse>, Status> {
        Ok(Response::new(ReadyResponse { ready: true }))
    }
}

pub async fn start_uds_server<T>(m: T) -> Result<(), Box<dyn std::error::Error>>
where
    T: Reducer + Send + Sync + 'static,
{
    shared::write_info_file();

    let path = "/var/run/numaflow/reduce.sock";
    std::fs::create_dir_all(std::path::Path::new(path).parent().unwrap())?;

    let uds = tokio::net::UnixListener::bind(path)?;
    let _uds_stream = tokio_stream::wrappers::UnixListenerStream::new(uds);

    let reduce_svc = ReduceService {
        handler: Arc::new(m),
    };

    tonic::transport::Server::builder()
        .add_service(reduce_server::ReduceServer::new(reduce_svc))
        .serve_with_incoming(_uds_stream)
        .await?;

    Ok(())
}
