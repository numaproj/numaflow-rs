use chrono::{DateTime, Utc};
use tonic::{async_trait, Request, Response, Status};

use crate::{
    shared::{self, prost_timestamp_from_utc},
    sourcetransform::sourcetransformer::SourceTransformRequest,
};

use self::sourcetransformer::{
    source_transform_response, source_transform_server, ReadyResponse, SourceTransformResponse,
};

mod sourcetransformer {
    tonic::include_proto!("sourcetransformer.v1");
}

struct SourceTransformerService<T> {
    handler: T,
}

/// SourceTransformer trait for implementing Source Transformer Handler.
#[async_trait]
pub trait SourceTransformer {
    /// transform takes in an input element and can produce 0, 1, or more results. The input is a [`Datum`]
    /// and the output is a ['Vec`] of [`Message`]. In a `transform` each element is processed independently
    /// and there is no state associated with the elements. Source transformer can be used for transforming
    /// and assigning event time to input messages. More about source transformer can be read
    /// [here](https://numaflow.numaproj.io/user-guide/sources/transformer/overview/)
    ///
    /// #Example
    ///
    ///  ```rust,ignore
    /// use numaflow::sourcetransform::start_uds_server;
    ///
    /// // A simple source transformer which assigns event time to the current time in utc.
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///    let transformer_handler = now_transformer::Now::new();
    ///
    ///    start_uds_server(transformer_handler).await?;
    ///
    ///    Ok(())
    ///}
    ///
    ///pub(crate) mod now_transformer {
    ///    use numaflow::sourcetransform::{Datum, Message, SourceTransformer};
    ///    use tonic::async_trait;
    ///
    ///    pub(crate) struct Now {}
    ///
    ///    impl Now {
    ///        pub(crate) fn new() -> Self {
    ///            Self {}
    ///        }
    ///    }
    ///
    ///    #[async_trait]
    ///    impl SourceTransformer for Now {
    ///        async fn transform<T: Datum + Send + Sync + 'static>(&self, input: T) -> Vec<Message> {
    ///            let mut reponse = vec![];
    ///            reponse.push(Message {
    ///                value: input.value().clone(),
    ///                keys: input.keys().clone(),
    ///                tags: vec![],
    ///                event_time: chrono::offset::Utc::now(),
    ///            });
    ///            reponse
    ///        }
    ///    }
    /// }
    /// ```
    async fn transform<T: Datum + Send + Sync + 'static>(&self, input: T) -> Vec<Message>;
}

/// Message is the response struct from the [`SourceTransformer::transform`] .
pub struct Message {
    /// Keys are a collection of strings which will be passed on to the next vertex as is. It can
    /// be an empty collection.
    pub keys: Vec<String>,
    /// Value is the value passed to the next vertex.
    pub value: Vec<u8>,
    /// Time for the given event. This will be used for tracking watermarks. If cannot be derived, set it to the incoming
    /// event_time from the [`Datum`].
    pub event_time: DateTime<Utc>,
    /// Tags are used for [conditional forwarding](https://numaflow.numaproj.io/user-guide/reference/conditional-forwarding/).
    pub tags: Vec<String>,
}

/// Datum trait represents an incoming element into the source_tranfrom handles of [`SourceTransformer`].
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

impl Datum for OwnedSourceTransformRequest {
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

/// Owned copy of MapRequest from Datum.
struct OwnedSourceTransformRequest {
    keys: Vec<String>,
    value: Vec<u8>,
    watermark: DateTime<Utc>,
    eventtime: DateTime<Utc>,
}

impl OwnedSourceTransformRequest {
    fn new(str: SourceTransformRequest) -> Self {
        Self {
            keys: str.keys,
            value: str.value,
            watermark: shared::utc_from_timestamp(str.watermark),
            eventtime: shared::utc_from_timestamp(str.event_time),
        }
    }
}
#[async_trait]
impl<T> source_transform_server::SourceTransform for SourceTransformerService<T>
where
    T: SourceTransformer + Send + Sync + 'static,
{
    async fn source_transform_fn(
        &self,
        request: Request<SourceTransformRequest>,
    ) -> Result<Response<SourceTransformResponse>, Status> {
        let request = request.into_inner();

        let messages = self
            .handler
            .transform(OwnedSourceTransformRequest::new(request))
            .await;

        let results = messages
            .into_iter()
            .map(move |msg| source_transform_response::Result {
                keys: msg.keys,
                event_time: prost_timestamp_from_utc(msg.event_time),
                value: msg.value,
                tags: msg.tags,
            })
            .collect::<Vec<source_transform_response::Result>>();

        Ok(Response::new(SourceTransformResponse { results }))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<ReadyResponse>, Status> {
        Ok(Response::new(ReadyResponse { ready: true }))
    }
}

pub async fn start_uds_server<T>(m: T) -> Result<(), Box<dyn std::error::Error>>
where
    T: SourceTransformer + Send + Sync + 'static,
{
    shared::write_info_file().map_err(|e| format!("writing info file: {e:?}"))?;

    let path = "/var/run/numaflow/sourcetransform.sock";
    let path = std::path::Path::new(path);
    let parent = path.parent().unwrap();
    std::fs::create_dir_all(parent).map_err(|e| format!("creating directory {parent:?}: {e:?}"))?;

    let uds = tokio::net::UnixListener::bind(path)?;
    let _uds_stream = tokio_stream::wrappers::UnixListenerStream::new(uds);

    let source_transformer_svc = SourceTransformerService { handler: m };

    tonic::transport::Server::builder()
        .add_service(source_transform_server::SourceTransformServer::new(
            source_transformer_svc,
        ))
        .serve_with_incoming(_uds_stream)
        .await
        .map_err(Into::into)
}
