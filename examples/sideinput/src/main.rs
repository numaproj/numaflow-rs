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
        // Counter starts at 0, increments before check:
        // Call 1: counter becomes 1, 1%2=1 (odd) -> Some
        // Call 2: counter becomes 2, 2%2=0 (even) -> None
        // Call 3: counter becomes 3, 3%2=1 (odd) -> Some
        // Call 4: counter becomes 4, 4%2=0 (even) -> None
        // Call 5: counter becomes 5, 5%2=1 (odd) -> Some
        for (i, res) in results.iter().enumerate() {
            if i % 2 == 0 {
                // i=0,2,4 -> calls 1,3,5 -> counter 1,3,5 (odd) -> Some
                assert!(res.is_some(), "Call {} should return Some", i + 1);
                let msg = String::from_utf8(res.as_ref().unwrap().clone()).unwrap();
                assert!(msg.starts_with("an example: "));
            } else {
                // i=1,3 -> calls 2,4 -> counter 2,4 (even) -> None
                assert!(res.is_none(), "Call {} should return None", i + 1);
            }
        }
    }
}
