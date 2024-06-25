use std::fs;
use std::path::Path;
use std::{collections::HashMap, io};

use chrono::{DateTime, TimeZone, Timelike, Utc};
use prost_types::Timestamp;
use tokio::signal;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
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
    mut abort_request: mpsc::Receiver<()>,
    user_rx: Option<oneshot::Receiver<()>>,
    cancel_token: CancellationToken,
) {
    // will call cancel_token.cancel() when the function exits
    // because of abort request, ctrl-c, or SIGTERM signal
    let _drop_guard = cancel_token.drop_guard();

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

    let abort_req_future = async {
        abort_request.recv().await;
    };

    let user_req_future = async {
        if let Some(rx) = user_rx {
            rx.await.ok();
        }
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
        _ = abort_req_future => {},
        _ = user_req_future => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Read;
    use tempfile::NamedTempFile;

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

    #[tokio::test]
    async fn test_write_info_file() -> io::Result<()> {
        // Create a temporary file
        let temp_file = NamedTempFile::new()?;

        // Call write_info_file with the path of the temporary file
        write_info_file(temp_file.path())?;

        // Open the file and read its contents
        let mut file = File::open(temp_file.path())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        // Check if the contents of the file are as expected
        assert!(contents.contains(r#""protocol":"uds""#));
        assert!(contents.contains(r#""language":"rust""#));
        assert!(contents.contains(r#""version":"0.0.1""#));
        assert!(contents.contains(r#""metadata":{}"#));

        Ok(())
    }

    #[tokio::test]
    async fn test_shutdown_signal() {
        // Create a channel to send shutdown signal
        let (internal_shutdown_tx, internal_shutdown_rx) = mpsc::channel(1);
        let (_user_shutdown_tx, user_shutdown_rx) = oneshot::channel();

        // Spawn a new task to call shutdown_signal
        let shutdown_signal_task = tokio::spawn(async move {
            shutdown_signal(
                internal_shutdown_rx,
                Some(user_shutdown_rx),
                CancellationToken::new(),
            )
            .await;
        });

        // Send a shutdown signal
        internal_shutdown_tx.send(()).await.unwrap();

        // Wait for the shutdown_signal function to finish
        let result = shutdown_signal_task.await;

        // If we reach this point, it means that the shutdown_signal function has correctly handled the shutdown signal
        assert!(result.is_ok());
    }
}
