use numaflow::sink::start_uds_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sink_handler = log_sink::Logger::new();

    start_uds_server(sink_handler).await?;

    Ok(())
}

mod log_sink {
    use numaflow::sink;
    use numaflow::sink::{Datum, Response};
    use tonic::async_trait;

    pub(crate) struct Logger {}

    impl Logger {
        pub(crate) fn new() -> Self {
            Self {}
        }
    }

    #[async_trait]
    impl sink::Sinker for Logger {
        async fn sink<T: Datum + Send + Sync + 'static>(
            &self,
            mut input: tokio::sync::mpsc::Receiver<T>,
        ) -> Vec<Response> {
            let mut responses: Vec<Response> = Vec::new();

            while let Some(datum) = input.recv().await {
                // do something better, but for now let's just log it.
                // please note that `from_utf8` is working because the input in this
                // example uses utf-8 data.
                let response = match std::str::from_utf8(datum.value()) {
                    Ok(v) => {
                        println!("{}", v);
                        // record the response
                        Response {
                            id: datum.id().to_string(),
                            success: true,
                            err: "".to_string(),
                        }
                    }
                    Err(e) => Response {
                        id: datum.id().to_string(),
                        success: true, // there is no point setting success to false as retrying is not going to help
                        err: format!("Invalid UTF-8 sequence: {}", e),
                    },
                };

                // return the responses
                responses.push(response);
            }

            responses
        }
    }
}
