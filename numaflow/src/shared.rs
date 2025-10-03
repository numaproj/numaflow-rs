//! Shared utilities, and common functionality
//!
//! This module contains utilities, constants, types, and server configuration
//! that are shared across different parts of the Numaflow SDK.

use chrono::{DateTime, TimeZone, Timelike, Utc};
use prost_types::Timestamp;

pub mod grpc_server;
pub(crate) mod panic;
pub(crate) mod server;

/// Environment variable for the container type
pub(crate) const ENV_CONTAINER_TYPE: &str = "NUMAFLOW_UD_CONTAINER_TYPE";

/// Drop message constant
pub const DROP: &str = "U+005C__DROP__";

// Re-export commonly used items
pub(crate) use grpc_server::{Server, ServerExtras, ServerStarter};
pub(crate) use panic::{build_panic_status, get_panic_info, init_panic_hook};
pub use server::ServerConfig;
pub(crate) use server::{
    ContainerType, ServerInfo, SocketCleanup, create_listener_stream, shutdown_signal,
};

/// Convert a protobuf Timestamp to a UTC DateTime
pub(crate) fn utc_from_timestamp(t: Option<Timestamp>) -> DateTime<Utc> {
    t.map_or(Utc.timestamp_nanos(-1), |t| {
        DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap_or(Utc.timestamp_nanos(-1))
    })
}

/// Convert a UTC DateTime to a protobuf Timestamp
pub(crate) fn prost_timestamp_from_utc(t: DateTime<Utc>) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: t.timestamp(),
        nanos: t.nanosecond() as i32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utc_from_timestamp() {
        let specific_date = Utc.with_ymd_and_hms(2022, 7, 2, 2, 0, 0).unwrap();

        let timestamp = Timestamp {
            seconds: specific_date.timestamp(),
            nanos: specific_date.timestamp_subsec_nanos() as i32,
        };

        let utc_ts = utc_from_timestamp(Some(timestamp));
        assert_eq!(utc_ts, specific_date)
    }

    #[test]
    fn test_utc_from_timestamp_epoch_0() {
        let specific_date = Utc.timestamp_nanos(-1);

        let utc_ts = utc_from_timestamp(None);
        assert_eq!(utc_ts, specific_date)
    }

    #[test]
    fn test_prost_timestamp_from_utc() {
        let specific_date = Utc.with_ymd_and_hms(2022, 7, 2, 2, 0, 0).unwrap();
        let timestamp = Timestamp {
            seconds: specific_date.timestamp(),
            nanos: specific_date.timestamp_subsec_nanos() as i32,
        };
        let prost_ts = prost_timestamp_from_utc(specific_date);
        assert_eq!(prost_ts, Some(timestamp))
    }

    #[test]
    fn test_prost_timestamp_from_utc_epoch_0() {
        let specific_date = Utc.timestamp_nanos(0);
        let timestamp = Timestamp {
            seconds: 0,
            nanos: 0,
        };
        let prost_ts = prost_timestamp_from_utc(specific_date);
        assert_eq!(prost_ts, Some(timestamp));
    }
}
