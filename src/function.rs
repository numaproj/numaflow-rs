use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use chrono::{DateTime, LocalResult, TimeZone, Utc};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataMap;
use tonic::transport::Server;
use tonic::{async_trait, Request, Response, Status};

use user_defined_function::user_defined_function_server;
use user_defined_function::user_defined_function_server::UserDefinedFunction;
use user_defined_function::{DatumRequest, DatumResponse, DatumResponseList, ReadyResponse};

use crate::shared;

mod user_defined_function {
    tonic::include_proto!("function.v1");
}

struct UserDefinedFunctionService<T: FnHandler> {
    pub handler: Arc<T>,
}

/// Trait implemented by FnHandler for Map and Reduce.
///
/// Types implementing this trait can be passed as Map and Reduce handlers.
///
/// **FIXME**: As of today, you need to implement both the traits even though your handle can be only
///  either a map or reduce. This is design decision that happened early on and we will fix it before
///  our 1.0 release. It is tracked in [#907](https://github.com/numaproj/numaflow/issues/907).
///
#[async_trait]
pub trait FnHandler {
    /// map_handle takes in an input element can can produce 0, 1, or more results. The input is a [`Datum`]
    /// and the output is a [`Vec`] of [`Message`]. In a `map` function, each element is processed
    /// independently and there is no state associated with the elements. More about map can be read
    /// [here](https://numaflow.numaproj.io/user-guide/user-defined-functions/map/map/#map-udf).
    ///
    /// # Example
    ///
    /// Following is an example of a cat container that just copies the input to output.
    ///
    /// ```rust
    /// struct Cat{}
    ///
    /// impl Cat {
    ///     pub(crate) fn new() -> Self {
    ///         Self {}
    ///     }   
    /// }   
    ///
    /// use numaflow::function::{self, Datum, Message, Metadata};
    /// use tokio::sync::mpsc::Receiver;
    ///
    /// #[tonic::async_trait]
    /// impl function::FnHandler for Cat {
    ///     async fn map_handle<T>(&self, input: T) -> Vec<Message>
    ///     where
    ///         T: Datum + Send + Sync + 'static,
    ///     {   
    ///         // return the result, please note that the value is converted to bytes.
    ///         vec![Message {
    ///             keys: input.keys().clone(),
    ///             value: input.value().clone(),
    ///             tags: vec![],
    ///         }]  
    ///     }   
    ///
    ///     // reduce_handle has to implemented because of the trait!
    ///     // luckily we have todo!() in rust :-)
    ///     async fn reduce_handle<
    ///         T: Datum + Send + Sync + 'static,
    ///         U: Metadata + Send + Sync + 'static,
    ///     >(  
    ///         &self,
    ///         _: Vec<String>,
    ///        _: Receiver<T>,
    ///         _: &U,
    ///     ) -> Vec<Message> {
    ///         todo!()
    ///     }   
    /// }
    /// ```
    async fn map_handle<T: Datum + Send + Sync + 'static>(&self, input: T) -> Vec<Message>;

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
    ///   use numaflow::function::{Datum, Message};
    ///   use numaflow::function::{FnHandler, Metadata};
    ///   use tokio::sync::mpsc::Receiver;
    ///   use tonic::async_trait;
    ///
    ///   struct Counter {}
    ///
    ///   impl Counter {
    ///       pub(crate) fn new() -> Self {
    ///           Self {}
    ///       }
    ///   }
    ///
    ///   #[async_trait]
    ///   impl FnHandler for Counter {
    ///       // damn, i do not need this, but i am forced to!        
    ///       async fn map_handle<T: Datum + Send + Sync + 'static>(&self, _: T) -> Vec<Message> {
    ///           todo!()
    ///       }
    ///
    ///       // counts the number of elements in the given partition, partition is nothing but a unique    
    ///       // combination of keys + window.
    ///       async fn reduce_handle<
    ///           T: Datum + Send + Sync + 'static,
    ///           U: Metadata + Send + Sync + 'static,
    ///       >(
    ///           &self,
    ///           keys: Vec<String>,
    ///           mut input: Receiver<T>,
    ///           md: &U,
    ///       ) -> Vec<Message> {
    ///
    ///           let mut counter = 0;
    ///           // the loop exits when input is closed which will happen only on close of book.
    ///           while let Some(_) = input.recv().await {
    ///               counter += 1;
    ///           }
    ///
    ///           // return the result, please note that the value is converted to bytes.
    ///           vec![Message {
    ///               keys: keys.clone(),
    ///               value: counter.to_string().into_bytes(),
    ///               tags: vec![],
    ///           }]
    ///       }
    ///   }
    /// ```
    ///
    /// [Window]: https://numaflow.numaproj.io/user-guide/user-defined-functions/reduce/windowing/windowing/
    async fn reduce_handle<T: Datum + Send + Sync + 'static, U: Metadata + Send + Sync + 'static>(
        &self,
        keys: Vec<String>,
        input: mpsc::Receiver<T>,
        md: &U,
    ) -> Vec<Message>;
}

// IntervalWindow is the start and end boundary of the window.
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

/// Metadata are additional information passed into the [`FnHandler::reduce_handle`].
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

/// Message is the response from the user's [`FnHandler::map_handle`] or [`FnHandler::reduce_handle`].
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection. It is mainly used in creating a partition in [`FnHandler::reduce_handle`].
    pub keys: Vec<String>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags: Vec<String>,
}

/// Datum trait represents an incoming element into the map/reduce handles of [`FnHandler`].
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

impl Datum for DatumRequest {
    fn keys(&self) -> &Vec<String> {
        &self.keys
    }

    fn value(&self) -> &Vec<u8> {
        &self.value
    }

    fn watermark(&self) -> DateTime<Utc> {
        if let Some(ref timestamp) = self.watermark {
            let t = timestamp.watermark.as_ref().unwrap();
            match Utc.timestamp_opt(t.seconds, t.nanos as u32) {
                LocalResult::Single(dt) => dt,
                _ => Utc.timestamp_nanos(-1),
            }
        } else {
            Utc.timestamp_nanos(-1)
        }
    }

    fn event_time(&self) -> DateTime<Utc> {
        if let Some(ref timestamp) = self.event_time {
            let t = timestamp.event_time.as_ref().unwrap();
            match Utc.timestamp_opt(t.seconds, t.nanos as u32) {
                LocalResult::Single(dt) => dt,
                _ => Utc.timestamp_nanos(-1),
            }
        } else {
            Utc.timestamp_nanos(-1)
        }
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
            .expect(format!("expected key {}", WIN_START_TIME).as_str())
            .to_str()
            .unwrap()
            .to_string()
            .parse::<i64>()
            .unwrap(),
        request
            .get(WIN_END_TIME)
            .expect(format!("expected key {}", WIN_END_TIME).as_str())
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
impl<T> UserDefinedFunction for UserDefinedFunctionService<T>
where
    T: FnHandler + Send + Sync + 'static,
{
    async fn map_fn(
        &self,
        request: Request<DatumRequest>,
    ) -> Result<Response<DatumResponseList>, Status> {
        let request = request.into_inner();

        // call the map handle
        let result = self.handler.map_handle(request).await;

        let mut response_list = vec![];
        // build the response struct
        for message in result {
            let datum_response = DatumResponse {
                keys: message.keys,
                value: message.value,
                tags: message.tags,
            };
            response_list.push(datum_response);
        }

        // return the result
        Ok(Response::new(DatumResponseList {
            elements: response_list,
        }))
    }

    type ReduceFnStream = ReceiverStream<Result<DatumResponseList, Status>>;

    async fn reduce_fn(
        &self,
        request: Request<tonic::Streaming<DatumRequest>>,
    ) -> Result<Response<Self::ReduceFnStream>, Status> {
        // get gRPC window from metadata
        let (start_win, end_win) = get_window_details(&request.metadata());
        let md = Arc::new(IntervalWindow::new(start_win, end_win));

        let mut key_to_tx: HashMap<String, Sender<DatumRequest>> = HashMap::new();

        // we will be creating a set of tasks for this stream
        let mut set = JoinSet::new();

        let mut stream = request.into_inner();

        while let Some(datum) = stream.message().await.unwrap() {
            let task_name = datum.keys.join(KEY_JOIN_DELIMITER);

            if let Some(tx) = key_to_tx.get(&task_name) {
                tx.send(datum).await.unwrap();
            } else {
                // channel to send data to the user's reduce handle
                let (tx, rx) = mpsc::channel::<DatumRequest>(1);

                // since we are calling this in a loop, we need make sure that there is reference counting
                // and the lifetime of self is more than the async function.
                // try Arc<Self> https://doc.rust-lang.org/reference/items/associated-items.html#methods ?
                let v = Arc::clone(&self.handler);
                let m = Arc::clone(&md);

                // spawn task for each unique key
                let keys = datum.keys.clone();
                set.spawn(async move { v.reduce_handle(keys, rx, m.as_ref()).await });

                // write data into the channel
                tx.send(datum).await.unwrap();

                // save the key and for future look up as long as the stream is active
                key_to_tx.insert(task_name, tx);
            }
        }

        // close all the tx channels to tasks to close their corresponding rx
        key_to_tx.clear();

        // channel to respond to numaflow main car as it expects streaming results.
        let (tx, rx) = mpsc::channel::<Result<DatumResponseList, Status>>(1);

        // start the result streamer
        tokio::spawn(async move {
            while let Some(res) = set.join_next().await {
                let messages = res.unwrap();
                let mut datum_responses = vec![];
                for message in messages {
                    datum_responses.push(DatumResponse {
                        keys: message.keys,
                        value: message.value,
                        tags: message.tags,
                    });
                }
                // stream it out to the client
                tx.send(Ok(DatumResponseList {
                    elements: datum_responses,
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

/// start_server starts the gRPC server at port 55551.
/// This is used mostly for local testing, hence the hard-coding of path, and port.
/// start_server method should **not** be used in production.
pub async fn start_server<T>(m: T) -> Result<(), Box<dyn std::error::Error>>
where
    T: FnHandler + Send + Sync + 'static,
{
    // TODO: make port configurable and pass it to info_file
    let addr = "0.0.0.0:55551";
    shared::write_info_file();

    let addr: SocketAddr = addr.parse().unwrap();

    let udf_service = UserDefinedFunctionService {
        handler: Arc::new(m),
    };

    Server::builder()
        .add_service(user_defined_function_server::UserDefinedFunctionServer::new(udf_service))
        .serve(addr)
        .await?;

    Ok(())
}

/// start_uds_server starts a gRPC server over an UDS (unix-domain-socket) endpoint.
///
/// # Example
///
///```rust
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     use numaflow::function::start_uds_server;
///
///     // handler can be map or reduce
///     let handler = todo!();
///
///     start_uds_server(handler).await?;
///
///     Ok(())
/// }
/// ```
pub async fn start_uds_server<T>(m: T) -> Result<(), Box<dyn std::error::Error>>
where
    T: FnHandler + Send + Sync + 'static,
{
    shared::write_info_file();

    let path = "/var/run/numaflow/function.sock";
    fs::create_dir_all(std::path::Path::new(path).parent().unwrap())?;
    use std::fs;
    use tokio::net::UnixListener;
    use tokio_stream::wrappers::UnixListenerStream;

    let uds = UnixListener::bind(path)?;
    let _uds_stream = UnixListenerStream::new(uds);

    let udf_service = UserDefinedFunctionService {
        handler: Arc::new(m),
    };

    Server::builder()
        .add_service(user_defined_function_server::UserDefinedFunctionServer::new(udf_service))
        .serve_with_incoming(_uds_stream)
        .await?;

    Ok(())
}
