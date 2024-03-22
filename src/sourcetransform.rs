use chrono::{DateTime, Utc};
use tonic::{async_trait, Request, Response, Status};

use crate::shared::{self, prost_timestamp_from_utc};

mod proto {
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
    async fn transform(&self, input: SourceTransformRequest) -> Vec<Message>;
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

/// Owned copy of MapRequest from Datum.
pub struct SourceTransformRequest {
    /// keys are the keys in the (key, value) terminology of map/reduce paradigm.
    /// Once called, it will replace the content with None, so subsequent calls will return None
    pub keys: Vec<String>,
    /// value is the value in (key, value) terminology of map/reduce paradigm.
    /// Once called, it will replace the content with None, so subsequent calls will return None
    pub value: Vec<u8>,
    /// [watermark](https://numaflow.numaproj.io/core-concepts/watermarks/) represented by time is a guarantee that we will not see an element older than this
    /// time.
    pub watermark: DateTime<Utc>,
    /// event_time is the time of the element as seen at source or aligned after a reduce operation.
    pub eventtime: DateTime<Utc>,
}

impl From<Message> for proto::source_transform_response::Result {
    fn from(value: Message) -> Self {
        proto::source_transform_response::Result {
            keys: value.keys,
            value: value.value,
            event_time: prost_timestamp_from_utc(value.event_time),
            tags: value.tags,
        }
    }
}

impl From<proto::SourceTransformRequest> for SourceTransformRequest {
    fn from(value: proto::SourceTransformRequest) -> Self {
        Self {
            keys: value.keys,
            value: value.value,
            watermark: shared::utc_from_timestamp(value.watermark),
            eventtime: shared::utc_from_timestamp(value.event_time),
        }
    }
}

#[async_trait]
impl<T> proto::source_transform_server::SourceTransform for SourceTransformerService<T>
where
    T: SourceTransformer + Send + Sync + 'static,
{
    async fn source_transform_fn(
        &self,
        request: Request<proto::SourceTransformRequest>,
    ) -> Result<Response<proto::SourceTransformResponse>, Status> {
        let request = request.into_inner();

        let messages = self.handler.transform(request.into()).await;

        Ok(Response::new(proto::SourceTransformResponse {
            results: messages
                .into_iter()
                .map(move |msg| msg.into())
                .collect::<Vec<_>>(),
        }))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}

pub async fn start_uds_server<T>(m: T) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: SourceTransformer + Send + Sync + 'static,
{
    let server_info_file = if std::env::var_os("NUMAFLOW_POD").is_some() {
        "/var/run/numaflow/server-info"
    } else {
        "/tmp/numaflow.server-info"
    };
    let socket_file = "/var/run/numaflow/sourcetransform.sock";
    let listener = shared::create_listener_stream(socket_file, server_info_file)?;
    let source_transformer_svc = SourceTransformerService { handler: m };

    tonic::transport::Server::builder()
        .add_service(proto::source_transform_server::SourceTransformServer::new(
            source_transformer_svc,
        ))
        .serve_with_incoming(listener)
        .await
        .map_err(Into::into)
}
