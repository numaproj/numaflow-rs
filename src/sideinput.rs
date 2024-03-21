use tonic::{async_trait, Request, Response, Status};

mod proto {
    tonic::include_proto!("sideinput.v1");
}

struct SideInputService<T> {
    handler: T,
}

#[async_trait]
pub trait SideInputer {
    async fn retrieve_sideinput(&self) -> Option<Vec<u8>>;
}

#[async_trait]
impl<T> proto::side_input_server::SideInput for SideInputService<T>
where
    T: SideInputer + Send + Sync + 'static,
{
    async fn retrieve_side_input(
        &self,
        _: Request<()>,
    ) -> Result<Response<proto::SideInputResponse>, Status> {
        let msg = self.handler.retrieve_sideinput().await;
        let si = match msg {
            Some(value) => proto::SideInputResponse {
                value,
                no_broadcast: false,
            },
            None => proto::SideInputResponse {
                value: Vec::new(),
                no_broadcast: true,
            },
        };

        Ok(Response::new(si))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<proto::ReadyResponse>, Status> {
        Ok(Response::new(proto::ReadyResponse { ready: true }))
    }
}

pub async fn start_uds_server<T>(m: T) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: SideInputer + Send + Sync + 'static,
{
    let server_info_file = if std::env::var_os("NUMAFLOW_POD").is_some() {
        "/var/run/numaflow/server-info"
    } else {
        "/tmp/numaflow.server-info"
    };
    let socket_file = "/var/run/numaflow/sideinput.sock";
    let listener = crate::shared::create_listener_stream(socket_file, server_info_file)?;
    let si_svc = SideInputService { handler: m };

    tonic::transport::Server::builder()
        .add_service(proto::side_input_server::SideInputServer::new(si_svc))
        .serve_with_incoming(listener)
        .await
        .map_err(Into::into)
}
