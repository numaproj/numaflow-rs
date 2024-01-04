use tonic::{async_trait, Request, Response, Status};

use crate::sideinput::sideinputer::{side_input_server, ReadyResponse, SideInputResponse};

mod sideinputer {
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
impl<T> side_input_server::SideInput for SideInputService<T>
where
    T: SideInputer + Send + Sync + 'static,
{
    async fn retrieve_side_input(
        &self,
        _: Request<()>,
    ) -> Result<Response<SideInputResponse>, Status> {
        let msg = self.handler.retrieve_sideinput().await;
        let si = match msg {
            Some(value) => SideInputResponse {
                value,
                no_broadcast: false,
            },
            None => SideInputResponse {
                value: Vec::new(),
                no_broadcast: true,
            },
        };

        Ok(Response::new(si))
    }

    async fn is_ready(&self, _: Request<()>) -> Result<Response<ReadyResponse>, Status> {
        Ok(Response::new(ReadyResponse { ready: true }))
    }
}

pub async fn start_uds_server<T>(m: T) -> Result<(), Box<dyn std::error::Error>>
where
    T: SideInputer + Send + Sync + 'static,
{
    crate::shared::write_info_file().map_err(|e| format!("writing info file: {e:?}"))?;

    let path = "/var/run/numaflow/sideinput.sock";
    let path = std::path::Path::new(path);
    std::fs::create_dir_all(path.parent().unwrap())
        .map_err(|e| format!("creating directory /var/run/numaflow : {e:?}"))?;

    let uds = tokio::net::UnixListener::bind(path)?;
    let _uds_stream = tokio_stream::wrappers::UnixListenerStream::new(uds);

    let si_svc = SideInputService { handler: m };

    tonic::transport::Server::builder()
        .add_service(side_input_server::SideInputServer::new(si_svc))
        .serve_with_incoming(_uds_stream)
        .await
        .map_err(Into::into)
}
