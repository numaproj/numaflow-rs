use filter_impl::filter_event_time;
use numaflow::sourcetransform;
use numaflow::sourcetransform::{Message, SourceTransformRequest};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    sourcetransform::Server::new(EventTimeFilter).start().await
}

struct EventTimeFilter;

#[tonic::async_trait]
impl sourcetransform::SourceTransformer for EventTimeFilter {
    /// Asynchronously transforms input messages based on their event time.
    /// Calls `filter_event_time` to determine how messages are transformed.
    async fn transform(&self, input: SourceTransformRequest) -> Vec<Message> {
        filter_event_time(input)
    }
}

mod filter_impl {
    use numaflow::sourcetransform::{Message, SourceTransformRequest};
    use std::time::{Duration, UNIX_EPOCH};

    /// Filters messages based on their event time.
    /// Returns different types of messages depending on the event time comparison.
    pub fn filter_event_time(input: SourceTransformRequest) -> Vec<Message> {
        let jan_first_2022 = UNIX_EPOCH + Duration::new(1_640_995_200, 0); // 2022-01-01 00:00:00 UTC
        let jan_first_2023 = UNIX_EPOCH + Duration::new(1_672_348_800, 0); // 2023-01-01 00:00:00 UTC

        if input.eventtime < jan_first_2022 {
            vec![Message::message_to_drop(input.eventtime)]
        } else if input.eventtime < jan_first_2023 {
            vec![Message::new(input.value, jan_first_2022)
                .with_tags(vec![String::from("within_year_2022")])]
        } else {
            vec![Message::new(input.value, jan_first_2023)
                .with_tags(vec![String::from("after_year_2022")])]
        }
    }
}

#[cfg(test)]
mod tests {
    use numaflow::sourcetransform::SourceTransformRequest;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use crate::filter_impl::filter_event_time;

    /// Tests that events from 2022 are tagged as within the year 2022.
    #[test]
    fn test_filter_event_time_should_return_within_year_2022() {
        let time = UNIX_EPOCH + Duration::new(1_656_732_000, 0); // 2022-07-02 02:00:00 UTC
        let source_request = SourceTransformRequest {
            keys: vec![],
            value: vec![],
            watermark: SystemTime::now(),
            eventtime: time,
            headers: Default::default(),
        };

        let messages = filter_event_time(source_request);

        assert_eq!(messages.len(), 1);
        assert_eq!((&messages)[0].tags.as_ref().unwrap()[0], "within_year_2022")
    }

    /// Tests that events from 2023 are tagged as after the year 2022.
    #[test]
    fn test_filter_event_time_should_return_after_year_2022() {
        let time = UNIX_EPOCH + Duration::new(1_682_348_800, 0); // 2023-07-02 02:00:00 UTC
        let source_request = SourceTransformRequest {
            keys: vec![],
            value: vec![],
            watermark: SystemTime::now(),
            eventtime: time,
            headers: Default::default(),
        };

        let messages = filter_event_time(source_request);

        assert_eq!(messages.len(), 1);
        assert_eq!((&messages)[0].tags.as_ref().unwrap()[0], "after_year_2022")
    }

    /// Tests that events before 2022 are dropped.
    #[test]
    fn test_filter_event_time_should_drop() {
        let time = UNIX_EPOCH + Duration::new(1_594_732_000, 0); // 2021-07-02 02:00:00 UTC
        let source_request = SourceTransformRequest {
            keys: vec![],
            value: vec![],
            watermark: SystemTime::now(),
            eventtime: time,
            headers: Default::default(),
        };

        let messages = filter_event_time(source_request);

        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages.first().unwrap().tags.as_ref().unwrap()[0],
            "U+005C__DROP__"
        )
    }
}
