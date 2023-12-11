use notify::{Watcher, RecursiveMode, Result};
use numaflow::map::{Mapper,Message,start_uds_server,Datum};
use std::path::Path;
use tonic::{async_trait};
use tokio::spawn;

const DIR_PATH: &str = "/var/numaflow/side-inputs";
struct UdfMapper {}
#[async_trait]
impl Mapper for UdfMapper {
    async fn map<T: Datum + Send + Sync + 'static>(&self, _request:T) -> Vec<Message> {
        let message = Message {
            keys: vec![],
            value: b"some_value".to_vec(),
            tags: vec![],
        };
        vec![message]
    }
}
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Spawn the file watcher task
    spawn(async {
        match file_watcher().await {
            Ok(_) => println!("File watcher is running"),
            Err(e) => println!("File watcher error: {:?}", e),
        }
    });

    let udf_map=UdfMapper{};
    start_uds_server(udf_map).await?;

    Ok(())
}


async fn file_watcher() -> Result<()>{
    let mut watcher = notify::recommended_watcher(|res| {
        match res {
            Ok(event) => println!("event: {:?}", event),
            Err(e) => println!("watch error: {:?}", e),
        }
    })?;
    watcher.watch(Path::new(DIR_PATH), RecursiveMode::Recursive)?;
    Ok(())
}