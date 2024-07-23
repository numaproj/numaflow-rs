use std::path::Path;

use notify::{RecursiveMode, Result, Watcher};
use numaflow::map::{MapRequest, Mapper, Message, Server};
use tokio::spawn;
use tonic::async_trait;

const DIR_PATH: &str = "/var/numaflow/sideinputs";

struct UdfMapper;

#[async_trait]
impl Mapper for UdfMapper {
    async fn map(&self, _input: MapRequest) -> Vec<Message> {
        let message = Message {
            keys: vec![].into(),
            value: b"some_value".to_vec(),
            tags: vec![].into(),
        };
        vec![message]
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Spawn the file watcher task
    spawn(async {
        match file_watcher().await {
            Ok(_) => println!("File watcher is running"),
            Err(e) => println!("File watcher error: {:?}", e),
        }
    });
    Server::new(UdfMapper).start().await
}

async fn file_watcher() -> Result<()> {
    let mut watcher = notify::recommended_watcher(|res| match res {
        Ok(event) => println!("event: {:?}", event),
        Err(e) => println!("watch error: {:?}", e),
    })?;
    watcher.watch(Path::new(DIR_PATH), RecursiveMode::Recursive)?;
    Ok(())
}
