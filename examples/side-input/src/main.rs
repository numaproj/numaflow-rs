use std::sync::Mutex;

use numaflow::sideinput::SideInputer;
use numaflow::sideinput::start_uds_server;
use tonic::async_trait;

struct SideInputHandler {
    counter: Mutex<u32>,
}

impl SideInputHandler {
    pub fn new() -> Self {
        SideInputHandler {
            counter: Mutex::new(0),
        }
    }
}

#[async_trait]
impl SideInputer for SideInputHandler {
    async fn retrieve_sideinput(&self) -> Option<Vec<u8>> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let message = format!("an example: {:?}", current_time);

        let mut counter = self.counter.lock().unwrap();
        *counter = (*counter + 1) % 10;
        if *counter % 2 == 0 {
            None
        } else {
            Some(message.into_bytes())
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let side_input_handler = SideInputHandler::new();
    start_uds_server(side_input_handler).await?;
    Ok(())
}
