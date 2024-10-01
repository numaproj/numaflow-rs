use std::fs;
use std::path::Path;
use std::{collections::HashMap, io};

use chrono::{DateTime, TimeZone, Timelike, Utc};
use once_cell::sync::Lazy;
use prost_types::Timestamp;
use serde::{Deserialize, Serialize};
use tokio::net::UnixListener;
use tokio::signal;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnixListenerStream;
use tracing::info;

pub(crate) const MAP_MODE_KEY: &str = "MAP_MODE";
pub(crate) const UNARY_MAP: &str = "unary-map";
pub(crate) const BATCH_MAP: &str = "batch-map";

#[derive(Eq, PartialEq, Hash)]
pub(crate) enum ContainerType {
    Map,
    Reduce,
    Sink,
    Source,
    SourceTransformer,
    SideInput,
}

// Minimum version of Numaflow required by the current SDK version
//
// Updating this value:
// 1. For release candidate (RC) versions, use the RC version string directly.
//    Example: For version 1.3.1-rc1, enter "1.3.1-rc1"
// 2. For stable versions, append "-z" to the stable version string.
//    Example: For version 1.3.1, enter "1.3.1-z"
//
// Why use "-z"?
// The "-z" suffix allows validation of pre-release versions (e.g., rc1, rc2) against the minimum version.
// In semantic versioning, a pre-release version like a.b.c-rc1 is considered less than its stable counterpart a.b.c.
// However, the semantic versioning library (https://github.com/Masterminds/semver) does not support directly validating
// a pre-release version against a constraint like ">= a.b.c".
// For it to work, a pre-release must be specified in the constraint.
// Therefore, we translate ">= a.b.c" into ">= a.b.c-z".
// The character 'z' is the largest in the ASCII table, ensuring that all RC versions are recognized as
// smaller than any stable version suffixed with '-z'.
pub(crate) static MINIMUM_NUMAFLOW_VERSION: Lazy<HashMap<ContainerType, &'static str>> =
    Lazy::new(|| {
        let mut m = HashMap::new();
        m.insert(ContainerType::Source, "1.3.1-z");
        m.insert(ContainerType::Map, "1.3.1-z");
        m.insert(ContainerType::Reduce, "1.3.1-z");
        m.insert(ContainerType::Sink, "1.3.1-z");
        m.insert(ContainerType::SourceTransformer, "1.3.1-z");
        m.insert(ContainerType::SideInput, "1.3.1-z");
        m
    });

const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

// ServerInfo structure to store server-related information
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct ServerInfo {
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    language: String,
    #[serde(default)]
    minimum_numaflow_version: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    metadata: Option<HashMap<String, String>>, // Metadata is optional
}
impl ServerInfo {
    // default_info_file is a function to get a default server info json
    // file content. This is used to write the server info file.
    // This function is used in the write_info_file function.
    // This function is not exposed to the user.
    pub fn default() -> Self {
        let metadata: HashMap<String, String> = HashMap::new();
        // Return the default server info json content
        // Create a ServerInfo object with default values
        ServerInfo {
            protocol: "uds".to_string(),
            language: "rust".to_string(),
            minimum_numaflow_version: "".to_string(),
            version: SDK_VERSION.to_string(),
            metadata: Option::from(metadata),
        }
    }

    // Check if the struct is empty
    pub fn is_empty(&self) -> bool {
        self.protocol.is_empty()
            && self.language.is_empty()
            && self.minimum_numaflow_version.is_empty()
            && self.version.is_empty()
            && self.metadata.is_none()
    }

    // Set metadata key-value pair
    pub fn set_metadata(&mut self, key: &str, value: &str) {
        if let Some(metadata) = &mut self.metadata {
            metadata.insert(key.to_string(), value.to_string());
        } else {
            let mut metadata = HashMap::new();
            metadata.insert(key.to_string(), value.to_string());
            self.metadata = Some(metadata);
        }
    }

    // Set minimum numaflow version
    pub fn set_minimum_numaflow_version(&mut self, version: &str) {
        self.minimum_numaflow_version = version.to_string();
    }
}

// #[tracing::instrument(skip(path), fields(path = ?path.as_ref()))]
#[tracing::instrument(fields(path = ? path.as_ref()))]
fn write_info_file(path: impl AsRef<Path>, mut server_info: ServerInfo) -> io::Result<()> {
    let parent = path.as_ref().parent().unwrap();
    fs::create_dir_all(parent)?;

    // TODO: make port-number and CPU meta-data configurable, e.g., ("CPU_LIMIT", "1")
    // If the server_info is empty, set it to the default
    if server_info.is_empty() {
        server_info = ServerInfo::default();
    }
    // Convert to a string of JSON and print it out
    let serialized = serde_json::to_string(&server_info)?;
    let content = format!("{}U+005C__END__", serialized);
    info!(content, "Writing to file");
    fs::write(path, content)
}

pub(crate) fn create_listener_stream(
    socket_file: impl AsRef<Path>,
    server_info_file: impl AsRef<Path>,
    server_info: ServerInfo,
) -> Result<UnixListenerStream, Box<dyn std::error::Error + Send + Sync>> {
    write_info_file(server_info_file, server_info)
        .map_err(|e| format!("writing info file: {e:?}"))?;

    let uds_stream = UnixListener::bind(socket_file)?;
    Ok(UnixListenerStream::new(uds_stream))
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

/// shuts downs the gRPC server. This happens in 2 cases
/// 1. there has been an internal error (one of the tasks failed) and we need to shutdown
/// 2. user is explicitly asking us to shutdown
/// Once the request for shutdown has be invoked, server will broadcast shutdown to all tasks
/// through the cancellation-token.
pub(crate) async fn shutdown_signal(
    mut shutdown_on_err: mpsc::Receiver<()>,
    shutdown_from_user: Option<oneshot::Receiver<()>>,
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

    let shutdown_on_err_future = async {
        shutdown_on_err.recv().await;
    };

    let shutdown_from_user_future = async {
        if let Some(rx) = shutdown_from_user {
            rx.await.ok();
        }
    };

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
        _ = shutdown_on_err_future => {},
        _ = shutdown_from_user_future => {},
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

        // Get a default server info file content
        let mut info = ServerInfo::default();
        // update the info json metadata field, and add the map mode key value pair
        info.set_metadata(MAP_MODE_KEY, BATCH_MAP);

        // Call write_info_file with the path of the temporary file
        write_info_file(temp_file.path(), info)?;

        // Open the file and read its contents
        let mut file = File::open(temp_file.path())?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        // Check if the contents of the file are as expected
        assert!(contents.contains(r#""protocol":"uds""#));
        assert!(contents.contains(r#""language":"rust""#));
        assert!(contents.contains(r#""metadata":{"MAP_MODE":"batch-map"}"#));

        Ok(())
    }

    #[tokio::test]
    async fn test_shutdown_signal() {
        // Create a channel to send shutdown signal
        let (internal_shutdown_tx, internal_shutdown_rx) = mpsc::channel(1);
        let (_user_shutdown_tx, user_shutdown_rx) = oneshot::channel();

        // Spawn a new task to call shutdown_signal
        let shutdown_signal_task = tokio::spawn(async move {
            shutdown_signal(internal_shutdown_rx, Some(user_shutdown_rx)).await;
        });

        // Send a shutdown signal
        internal_shutdown_tx.send(()).await.unwrap();

        // Wait for the shutdown_signal function to finish
        let result = shutdown_signal_task.await;

        // If we reach this point, it means that the shutdown_signal function has correctly handled the shutdown signal
        assert!(result.is_ok());
    }
}
