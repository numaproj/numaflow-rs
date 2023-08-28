use chrono::{DateTime, Utc};
use tonic::{async_trait, Request, Response, Status};

use crate::map::mapper::{map_response, map_server, MapRequest, MapResponse, ReadyResponse};
use crate::shared;

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
impl<T> map_server::Map for MapService<T>
where
    T: Mapper + Send + Sync + 'static,
{
    async fn map_fn(&self, request: Request<MapRequest>) -> Result<Response<MapResponse>, Status> {
        let request = request.into_inner();

        // call the map handle
        let result = self.handler.map(OwnedMapRequest::new(request)).await;

        let mut response_list = vec![];
        // build the response struct
        for message in result {
            let datum_response = map_response::Result {
                keys: message.keys,
                value: message.value,
                tags: message.tags,
            };
            response_list.push(datum_response);
        }

        // return the result
        Ok(Response::new(MapResponse {
            results: response_list,
        }))
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
    fn keys(&self) -> &Vec<String>;
    /// value is the value in (key, value) terminology of map/reduce paradigm.
    /// Once called, it will replace the content with None, so subsequent calls will return None
    fn value(&self) -> &Vec<u8>;
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this
    /// time.
    fn watermark(&self) -> DateTime<Utc>;
    /// event_time is the time of the element as seen at source or aligned after a reduce operation.
    fn event_time(&self) -> DateTime<Utc>;
}

/// Owned copy of MapRequest from Datum.
struct OwnedMapRequest {
    keys: Vec<String>,
    value: Vec<u8>,
    watermark: DateTime<Utc>,
    eventtime: DateTime<Utc>,
}

impl OwnedMapRequest {
    fn new(mr: MapRequest) -> Self {
        Self {
            keys: mr.keys,
            value: mr.value,
            watermark: shared::utc_from_timestamp(mr.watermark),
            eventtime: shared::utc_from_timestamp(mr.event_time),
        }
    }
}

impl Datum for OwnedMapRequest {
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

pub async fn start_uds_server<T>(m: T) -> Result<(), Box<dyn std::error::Error>>
where
    T: Mapper + Send + Sync + 'static,
{
    shared::write_info_file();

    let path = "/var/run/numaflow/map.sock";
    std::fs::create_dir_all(std::path::Path::new(path).parent().unwrap())?;

    let uds = tokio::net::UnixListener::bind(path)?;
    let _uds_stream = tokio_stream::wrappers::UnixListenerStream::new(uds);

    let map_svc = MapService { handler: m };

    tonic::transport::Server::builder()
        .add_service(map_server::MapServer::new(map_svc))
        .serve_with_incoming(_uds_stream)
        .await?;

    Ok(())
}
