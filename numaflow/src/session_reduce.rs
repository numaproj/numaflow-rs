pub use crate::servers::sessionreduce as proto;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{async_trait, Request, Response, Status, Streaming};

#[async_trait]
pub trait SessionReducer {
    async fn session_reduce(
        &self,
        keys: Vec<String>,
        request_stream: mpsc::Receiver<SessionReduceRequest>,
        response_stream: mpsc::Sender<Message>,
    );

    async fn accumulator(&self) -> Vec<u8>;

    async fn merge_accumulator(&self, accumulator: Vec<u8>);
}

pub trait SessionReducerCreator {
    type R: SessionReducer + Send + Sync + 'static;
    fn create(&self) -> Self::R;
}

/// Message is the response from the user's [`SessionReducer::reduce`].
#[derive(Debug, PartialEq)]
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection. It is mainly used in creating a partition in [`Reducer::reduce`].
    pub keys: Option<Vec<String>>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags: Option<Vec<String>>,
}

/// Represents a message that can be modified and forwarded.
impl Message {
    /// Creates a new message with the specified value.
    ///
    /// This constructor initializes the message with no keys, tags, or specific event time.
    ///
    /// # Arguments
    ///
    /// * `value` - A vector of bytes representing the message's payload.
    ///
    /// # Examples
    ///
    /// ```
    /// use numaflow::session_reduce::Message;
    /// let message = Message::new(vec![1, 2, 3, 4]);
    /// ```
    pub fn new(value: Vec<u8>) -> Self {
        Self {
            value,
            keys: None,
            tags: None,
        }
    }

    /// Sets or replaces the keys associated with this message.
    ///
    /// # Arguments
    ///
    /// * `keys` - A vector of strings representing the keys.
    ///
    /// # Examples
    ///
    /// ```
    ///  use numaflow::session_reduce::Message;
    /// let message = Message::new(vec![1, 2, 3]).with_keys(vec!["key1".to_string(), "key2".to_string()]);
    /// ```
    pub fn with_keys(mut self, keys: Vec<String>) -> Self {
        self.keys = Some(keys);
        self
    }

    /// Sets or replaces the tags associated with this message.
    ///
    /// # Arguments
    ///
    /// * `tags` - A vector of strings representing the tags.
    ///
    /// # Examples
    ///
    /// ```
    ///  use numaflow::session_reduce::Message;
    /// let message = Message::new(vec![1, 2, 3]).with_tags(vec!["tag1".to_string(), "tag2".to_string()]);
    /// ```
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = Some(tags);
        self
    }
}

/// Incoming request into the reducer handler of [`SessionReducer`].
pub struct SessionReduceRequest {
    /// Set of keys in the (key, value) terminology of map/reduce paradigm.
    pub keys: Vec<String>,
    /// The value in the (key, value) terminology of map/reduce paradigm.    /// The value in the (key, value) terminology of map/reduce paradigm.
    pub value: Vec<u8>,
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this time.    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this time.
    pub watermark: DateTime<Utc>,
    /// Time of the element as seen at source or aligned after a reduce operation.
    pub event_time: DateTime<Utc>,
    /// Headers for the message.
    pub headers: HashMap<String, String>,
}

struct SessionReduceService<C> {
    creator: Arc<C>,
    cancellation_token: CancellationToken,
}

#[async_trait]
impl<C> proto::session_reduce_server::SessionReduce for SessionReduceService<C>
where
    C: SessionReducerCreator + Send + Sync + 'static,
{
    type SessionReduceFnStream = ReceiverStream<Result<proto::SessionReduceResponse, Status>>;

    async fn session_reduce_fn(
        &self,
        request: Request<Streaming<proto::SessionReduceRequest>>,
    ) -> Result<Response<Self::SessionReduceFnStream>, Status> {
        todo!()
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}
