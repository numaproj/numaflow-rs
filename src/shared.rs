use std::fs;
use std::path::Path;
use std::{collections::HashMap, io};

use chrono::{DateTime, TimeZone, Timelike, Utc};
use prost_types::Timestamp;
use tokio::signal;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnixListenerStream;
use tracing::info;

// #[tracing::instrument(skip(path), fields(path = ?path.as_ref()))]
#[tracing::instrument(fields(path = ? path.as_ref()))]
fn write_info_file(path: impl AsRef<Path>) -> io::Result<()> {
    let parent = path.as_ref().parent().unwrap();
    std::fs::create_dir_all(parent)?;

    // TODO: make port-number and CPU meta-data configurable, e.g., ("CPU_LIMIT", "1")
    let metadata: HashMap<String, String> = HashMap::new();
    let info = serde_json::json!({
        "protocol": "uds",
        "language": "rust",
        "version": "0.0.1",
        "metadata": metadata,
    });

    // Convert to a string of JSON and print it out
    let content = format!("{}U+005C__END__", info);
    info!(content, "Writing to file");
    fs::write(path, content)
}

pub(crate) fn create_listener_stream(
    socket_file: impl AsRef<Path>,
    server_info_file: impl AsRef<Path>,
) -> Result<UnixListenerStream, Box<dyn std::error::Error + Send + Sync>> {
    write_info_file(server_info_file).map_err(|e| format!("writing info file: {e:?}"))?;

    let parent = socket_file.as_ref().parent().unwrap();
    fs::create_dir_all(parent).map_err(|e| format!("creating directory {parent:?}: {e:?}"))?;

    let uds = tokio::net::UnixListener::bind(socket_file)?;
    Ok(UnixListenerStream::new(uds))
}

pub(crate) fn utc_from_timestamp(t: Option<Timestamp>) -> DateTime<Utc> {
    t.map_or(Utc.timestamp_nanos(-1), |t| {
        DateTime::from_timestamp(t.seconds, t.nanos as u32).unwrap_or(Utc.timestamp_nanos(-1))
    })
}

pub(crate) fn prost_timestamp_from_utc(t: DateTime<Utc>) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: t.timestamp(),
        nanos: t.nanosecond() as i32,
    })
}

pub(crate) async fn shutdown_signal(
    mut internal_rx: mpsc::Receiver<()>,
    user_rx: Option<oneshot::Receiver<()>>,
) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install SIGINT handler");
    };

    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    let custom1 = async {
        internal_rx.recv().await;
    };

    let custom2 = async {
        if let Some(rx) = user_rx {
            rx.await.ok();
        }
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
        _ = custom1 => {},
        _ = custom2 => {},
    }
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
        let specific_date = Utc.timestamp(0, 0);
        let timestamp = Timestamp {
            seconds: 0,
            nanos: 0,
        };
        let prost_ts = prost_timestamp_from_utc(specific_date);
        assert_eq!(prost_ts, Some(timestamp));
    }
}
