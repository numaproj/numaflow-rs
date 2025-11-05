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
    use chrono::{TimeZone, Utc};
    use numaflow::sourcetransform::{Message, SourceTransformRequest};

    /// Filters messages based on their event time.
    /// Returns different types of messages depending on the event time comparison.
    pub fn filter_event_time(input: SourceTransformRequest) -> Vec<Message> {
        let jan_first_2022 = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();
        let jan_first_2023 = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        if input.eventtime < jan_first_2022 {
            vec![Message::message_to_drop(input.eventtime)]
        } else if input.eventtime < jan_first_2023 {
            vec![
                Message::new(input.value, jan_first_2022)
                    .with_tags(vec![String::from("within_year_2022")]),
            ]
        } else {
            vec![
                Message::new(input.value, jan_first_2023)
                    .with_tags(vec![String::from("after_year_2022")]),
            ]
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::filter_impl::filter_event_time;
    use chrono::{TimeZone, Utc};
    use numaflow::sourcetransform::{SourceTransformRequest, SystemMetadata, UserMetadata};

    /// Tests that events from 2022 are tagged as within the year 2022.
    #[test]
    fn test_filter_event_time_should_return_within_year_2022() {
        let time = Utc.with_ymd_and_hms(2022, 7, 2, 2, 0, 0).unwrap();
        let source_request = SourceTransformRequest {
            keys: vec![],
            value: vec![],
            watermark: Default::default(),
            eventtime: time,
            headers: Default::default(),
            user_metadata: UserMetadata::new(),
            system_metadata: SystemMetadata::new(),
        };

        let messages = filter_event_time(source_request);

        assert_eq!(messages.len(), 1);
        assert_eq!((&messages)[0].tags.as_ref().unwrap()[0], "within_year_2022")
    }

    /// Tests that events from 2023 are tagged as after the year 2022.
    #[test]
    fn test_filter_event_time_should_return_after_year_2022() {
        let time = Utc.with_ymd_and_hms(2023, 7, 2, 2, 0, 0).unwrap();
        let source_request = SourceTransformRequest {
            keys: vec![],
            value: vec![],
            watermark: Default::default(),
            eventtime: time,
            headers: Default::default(),
            user_metadata: UserMetadata::new(),
            system_metadata: SystemMetadata::new(),
        };

        let messages = filter_event_time(source_request);

        assert_eq!(messages.len(), 1);
        assert_eq!((&messages)[0].tags.as_ref().unwrap()[0], "after_year_2022")
    }

    /// Tests that events before 2022 are dropped.
    #[test]
    fn test_filter_event_time_should_drop() {
        let time = Utc.with_ymd_and_hms(2021, 7, 2, 2, 0, 0).unwrap();
        let source_request = SourceTransformRequest {
            keys: vec![],
            value: vec![],
            watermark: Default::default(),
            eventtime: time,
            headers: Default::default(),
            user_metadata: UserMetadata::new(),
            system_metadata: SystemMetadata::new(),
        };

        let messages = filter_event_time(source_request);

        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages.first().unwrap().tags.as_ref().unwrap()[0],
            "U+005C__DROP__"
        )
    }
}
