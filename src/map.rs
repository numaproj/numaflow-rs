use chrono::{DateTime, Utc};
use tonic::{async_trait, Request, Response, Status};

use crate::map::mapper::{
    map_response, map_server, MapRequest as RPCMapRequest, MapResponse, ReadyResponse,
};
use crate::shared;

mod mapper {
    tonic::include_proto!("map.v1");
}

struct MapService<T> {
    handler: T,
}

/// Mapper trait for implementing Map handler.
#[async_trait]
pub trait Mapper {
    /// The `map` takes in an input element and can produce 0, 1, or more results.
    /// In a `map` function, each element is processed independently and there is no state associated with the elements.
    /// More about map can be read [here](https://numaflow.numaproj.io/user-guide/user-defined-functions/map/map/#map-udf).
    ///
    /// # Example
    ///
    /// Following is an example of a cat container that just copies the input to output.
    ///
    /// ```no_run
    /// use numaflow::map::start_uds_server;
    /// use numaflow::map;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     start_uds_server(Cat).await?;
    ///     Ok(())
    /// }
    ///
    /// struct Cat;
    ///
    /// #[tonic::async_trait]
    /// impl map::Mapper for Cat {
    ///     async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
    ///         vec![map::Message {
    ///             keys: input.keys,
    ///             value: input.value,
    ///             tags: vec![],
    ///         }]
    ///     }
    /// }
    /// ```
    async fn map(&self, input: MapRequest) -> Vec<Message>;
}

#[async_trait]
impl<T> map_server::Map for MapService<T>
where
    T: Mapper + Send + Sync + 'static,
{
    async fn map_fn(
        &self,
        request: Request<RPCMapRequest>,
    ) -> Result<Response<MapResponse>, Status> {
        let request = request.into_inner();
        let result = self.handler.map(request.into()).await;

        Ok(Response::new(MapResponse {
            results: result.into_iter().map(|msg| msg.into()).collect(),
        }))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<ReadyResponse>, Status> {
        Ok(Response::new(ReadyResponse { ready: true }))
    }
}

/// Message is the response struct from the [`Mapper::map`] .
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection.
    pub keys: Vec<String>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags: Vec<String>,
}

impl From<Message> for map_response::Result {
    fn from(value: Message) -> Self {
        map_response::Result {
            keys: value.keys,
            value: value.value,
            tags: value.tags,
        }
    }
}

/// Incoming request into the map handles of [`Mapper`].
pub struct MapRequest {
    /// Set of keys in the (key, value) terminology of map/reduce paradigm.
    pub keys: Vec<String>,
    /// The value in the (key, value) terminology of map/reduce paradigm.
    pub value: Vec<u8>,
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this time.
    pub watermark: DateTime<Utc>,
    /// Time of the element as seen at source or aligned after a reduce operation.
    pub eventtime: DateTime<Utc>,
}

impl From<RPCMapRequest> for MapRequest {
    fn from(value: RPCMapRequest) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            watermark: shared::utc_from_timestamp(value.watermark),
            eventtime: shared::utc_from_timestamp(value.event_time),
        }
    }
}

pub async fn start_uds_server<T>(m: T) -> Result<(), Box<dyn std::error::Error>>
where
    T: Mapper + Send + Sync + 'static,
{
    let listener = shared::create_listener_stream("map")?;
    let map_svc = MapService { handler: m };

    tonic::transport::Server::builder()
        .add_service(map_server::MapServer::new(map_svc))
        .serve_with_incoming(listener)
        .await
        .map_err(Into::into)
}
