use numaflow::map;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    map::Server::new(EvenOdd).start().await
}

struct EvenOdd;

#[tonic::async_trait]
impl map::Mapper for EvenOdd {
    async fn map(&self, input: map::MapRequest) -> Vec<map::Message> {
        let num = String::from_utf8(input.value.clone())
            .unwrap_or_default()
            .parse::<i32>()
            .unwrap_or_default();
        if num % 2 == 0 {
            vec![map::Message::new(input.value).with_keys(vec!["even".to_string()])]
        } else {
            vec![map::Message::new(input.value).with_keys(vec!["odd".to_string()])]
        }
    }
}
