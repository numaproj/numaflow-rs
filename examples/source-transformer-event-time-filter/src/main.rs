use std::error::Error;
use numaflow::sourcetransform;
use numaflow::sourcetransform::{Message, SourceTransformRequest};
use filter_impl::filter_event_time;
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    sourcetransform::Server::new(EventTimeFilter).start().await
}




struct EventTimeFilter;
#[tonic::async_trait]
impl sourcetransform::SourceTransformer for EventTimeFilter {
    async fn transform(&self, input: SourceTransformRequest) -> Vec<Message> {
        filter_event_time(input)
    }
}

mod filter_impl {
    use numaflow::sourcetransform::{Message, SourceTransformRequest};
    use chrono::{TimeZone, Utc};
    pub  fn filter_event_time(input: SourceTransformRequest) ->Vec<Message>{
        let jan_first_2022 = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();
        let jan_first_2023 = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        if input.eventtime < jan_first_2022 {
            vec![ Message::message_to_drop(input.eventtime)]
        } else if input.eventtime < jan_first_2023 {
            vec![Message::new(input.value,jan_first_2022).tags(vec![String::from("within_year_2022")])]
        } else {
            vec![Message::new(input.value,jan_first_2023).tags(vec![String::from("after_year_2022")])]
        }
    }

}