// This file is @generated by prost-build.
/// *
/// MapRequest represents a request element.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MapRequest {
    #[prost(message, optional, tag = "1")]
    pub request: ::core::option::Option<map_request::Request>,
    /// This ID is used to uniquely identify a map request
    #[prost(string, tag = "2")]
    pub id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub handshake: ::core::option::Option<Handshake>,
    #[prost(message, optional, tag = "4")]
    pub status: ::core::option::Option<TransmissionStatus>,
}
/// Nested message and enum types in `MapRequest`.
pub mod map_request {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Request {
        #[prost(string, repeated, tag = "1")]
        pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        #[prost(bytes = "vec", tag = "2")]
        pub value: ::prost::alloc::vec::Vec<u8>,
        #[prost(message, optional, tag = "3")]
        pub event_time: ::core::option::Option<::prost_types::Timestamp>,
        #[prost(message, optional, tag = "4")]
        pub watermark: ::core::option::Option<::prost_types::Timestamp>,
        #[prost(map = "string, string", tag = "5")]
        pub headers: ::std::collections::HashMap<
            ::prost::alloc::string::String,
            ::prost::alloc::string::String,
        >,
    }
}
///
/// Handshake message between client and server to indicate the start of transmission.
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Handshake {
    /// Required field indicating the start of transmission.
    #[prost(bool, tag = "1")]
    pub sot: bool,
}
///
/// Status message to indicate the status of the message.
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct TransmissionStatus {
    #[prost(bool, tag = "1")]
    pub eot: bool,
}
/// *
/// MapResponse represents a response element.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MapResponse {
    #[prost(message, repeated, tag = "1")]
    pub results: ::prost::alloc::vec::Vec<map_response::Result>,
    /// This ID is used to refer the responses to the request it corresponds to.
    #[prost(string, tag = "2")]
    pub id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub handshake: ::core::option::Option<Handshake>,
    #[prost(message, optional, tag = "4")]
    pub status: ::core::option::Option<TransmissionStatus>,
}
/// Nested message and enum types in `MapResponse`.
pub mod map_response {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Result {
        #[prost(string, repeated, tag = "1")]
        pub keys: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
        #[prost(bytes = "vec", tag = "2")]
        pub value: ::prost::alloc::vec::Vec<u8>,
        #[prost(string, repeated, tag = "3")]
        pub tags: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    }
}
/// *
/// ReadyResponse is the health check result.
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ReadyResponse {
    #[prost(bool, tag = "1")]
    pub ready: bool,
}
/// Generated client implementations.
pub mod map_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct MapClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl MapClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> MapClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> MapClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + std::marker::Send + std::marker::Sync,
        {
            MapClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// MapFn applies a function to each map request element.
        pub async fn map_fn(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::MapRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::MapResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/map.v1.Map/MapFn");
            let mut req = request.into_streaming_request();
            req.extensions_mut().insert(GrpcMethod::new("map.v1.Map", "MapFn"));
            self.inner.streaming(req, path, codec).await
        }
        /// IsReady is the heartbeat endpoint for gRPC.
        pub async fn is_ready(
            &mut self,
            request: impl tonic::IntoRequest<()>,
        ) -> std::result::Result<tonic::Response<super::ReadyResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/map.v1.Map/IsReady");
            let mut req = request.into_request();
            req.extensions_mut().insert(GrpcMethod::new("map.v1.Map", "IsReady"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod map_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with MapServer.
    #[async_trait]
    pub trait Map: std::marker::Send + std::marker::Sync + 'static {
        /// Server streaming response type for the MapFn method.
        type MapFnStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::MapResponse, tonic::Status>,
            >
            + std::marker::Send
            + 'static;
        /// MapFn applies a function to each map request element.
        async fn map_fn(
            &self,
            request: tonic::Request<tonic::Streaming<super::MapRequest>>,
        ) -> std::result::Result<tonic::Response<Self::MapFnStream>, tonic::Status>;
        /// IsReady is the heartbeat endpoint for gRPC.
        async fn is_ready(
            &self,
            request: tonic::Request<()>,
        ) -> std::result::Result<tonic::Response<super::ReadyResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct MapServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> MapServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for MapServer<T>
    where
        T: Map,
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/map.v1.Map/MapFn" => {
                    #[allow(non_camel_case_types)]
                    struct MapFnSvc<T: Map>(pub Arc<T>);
                    impl<T: Map> tonic::server::StreamingService<super::MapRequest>
                    for MapFnSvc<T> {
                        type Response = super::MapResponse;
                        type ResponseStream = T::MapFnStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<super::MapRequest>>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Map>::map_fn(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = MapFnSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/map.v1.Map/IsReady" => {
                    #[allow(non_camel_case_types)]
                    struct IsReadySvc<T: Map>(pub Arc<T>);
                    impl<T: Map> tonic::server::UnaryService<()> for IsReadySvc<T> {
                        type Response = super::ReadyResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(&mut self, request: tonic::Request<()>) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Map>::is_ready(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = IsReadySvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        let mut response = http::Response::new(empty_body());
                        let headers = response.headers_mut();
                        headers
                            .insert(
                                tonic::Status::GRPC_STATUS,
                                (tonic::Code::Unimplemented as i32).into(),
                            );
                        headers
                            .insert(
                                http::header::CONTENT_TYPE,
                                tonic::metadata::GRPC_CONTENT_TYPE,
                            );
                        Ok(response)
                    })
                }
            }
        }
    }
    impl<T> Clone for MapServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    /// Generated gRPC service name
    pub const SERVICE_NAME: &str = "map.v1.Map";
    impl<T> tonic::server::NamedService for MapServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}