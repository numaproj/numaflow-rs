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

#[cfg(test)]
mod tests {
    use super::*;
    use numaflow::map::{MapRequest, Mapper, SystemMetadata, UserMetadata};

    fn create_request(value: Vec<u8>, keys: Vec<String>) -> MapRequest {
        MapRequest {
            keys,
            value,
            watermark: std::time::SystemTime::now().into(),
            eventtime: std::time::SystemTime::now().into(),
            headers: Default::default(),
            user_metadata: UserMetadata::new(),
            system_metadata: SystemMetadata::new(),
        }
    }

    #[tokio::test]
    async fn test_even_number() {
        let even_odd = EvenOdd;
        let request = create_request(b"42".to_vec(), vec!["original-key".to_string()]);

        let messages = even_odd.map(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, b"42");
        assert_eq!(
            messages[0].keys,
            Some(vec!["even".to_string()]),
            "Should route to 'even'"
        );
    }

    #[tokio::test]
    async fn test_odd_number() {
        let even_odd = EvenOdd;
        let request = create_request(b"43".to_vec(), vec!["original-key".to_string()]);

        let messages = even_odd.map(request).await;

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].value, b"43");
        assert_eq!(
            messages[0].keys,
            Some(vec!["odd".to_string()]),
            "Should route to 'odd'"
        );
    }

    #[tokio::test]
    async fn test_zero_is_even() {
        let even_odd = EvenOdd;
        let request = create_request(b"0".to_vec(), vec!["key".to_string()]);

        let messages = even_odd.map(request).await;

        assert_eq!(
            messages[0].keys,
            Some(vec!["even".to_string()]),
            "Zero should be even"
        );
    }

    #[tokio::test]
    async fn test_negative_even() {
        let even_odd = EvenOdd;
        let request = create_request(b"-42".to_vec(), vec!["key".to_string()]);

        let messages = even_odd.map(request).await;

        assert_eq!(
            messages[0].keys,
            Some(vec!["even".to_string()]),
            "Negative even should route to 'even'"
        );
    }

    #[tokio::test]
    async fn test_negative_odd() {
        let even_odd = EvenOdd;
        let request = create_request(b"-43".to_vec(), vec!["key".to_string()]);

        let messages = even_odd.map(request).await;

        assert_eq!(
            messages[0].keys,
            Some(vec!["odd".to_string()]),
            "Negative odd should route to 'odd'"
        );
    }
}
