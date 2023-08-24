use chrono::{DateTime, Utc};
use tonic::{async_trait, Request, Response, Status};

use crate::map::mapper::{MapRequest, MapResponse, ReadyResponse};

mod mapper {
    tonic::include_proto!("map.v1");
}

struct MapService<T> {
    handler: T,
}

#[async_trait]
pub trait Mapper {
    async fn map<T: Datum + Send + Sync + 'static>(&self, input: T) -> Vec<Message>;
}

#[async_trait]
impl<T> mapper::map_server::Map for MapService<T>
where
    T: Mapper + Send + Sync + 'static,
{
    async fn map_fn(&self, _: Request<MapRequest>) -> Result<Response<MapResponse>, Status> {
        todo!()
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<ReadyResponse>, Status> {
        Ok(Response::new(ReadyResponse { ready: true }))
    }
}

pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection.
    pub keys: Vec<String>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags: Vec<String>,
}

/// Datum trait represents an incoming element into the map/reduce handles of [`FnHandler`].
pub trait Datum {
    /// keys are the keys in the (key, value) terminology of map/reduce paradigm.
    /// Once called, it will replace the content with None, so subsequent calls will return None
    fn keys(&mut self) -> Option<Vec<String>>;
    /// value is the value in (key, value) terminology of map/reduce paradigm.
    /// Once called, it will replace the content with None, so subsequent calls will return None
    fn value(&mut self) -> Option<Vec<u8>>;
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this
    /// time.
    fn watermark(&self) -> DateTime<Utc>;
    /// event_time is the time of the element as seen at source or aligned after a reduce operation.
    fn event_time(&self) -> DateTime<Utc>;
}
