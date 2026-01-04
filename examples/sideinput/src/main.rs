use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use numaflow::sideinput::{self, SideInputer};
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
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    sideinput::Server::new(SideInputHandler::new())
        .start()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    #[tokio::test]
    async fn test_sideinput_handler() {
        let handler = SideInputHandler::new();
        let (tx, mut rx) = mpsc::channel(10);
        for _ in 0..5 {
            let sideinput = handler.retrieve_sideinput().await;
            tx.send(sideinput).await.unwrap();
        }
        drop(tx);
        let mut results = Vec::new();
        while let Some(result) = rx.recv().await {
            results.push(result);
        }
        assert_eq!(results.len(), 5);
        for (i, res) in results.iter().enumerate() {
            if i % 2 == 0 {
                assert!(res.is_none());
            } else {
                assert!(res.is_some());
                let msg = String::from_utf8(res.as_ref().unwrap().clone()).unwrap();
                assert!(msg.starts_with("an example: "));
            }
        }
    }
}