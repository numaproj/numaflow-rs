use std::fs;
use std::{collections::HashMap, io};

use chrono::{DateTime, TimeZone, Timelike, Utc};
use prost_types::Timestamp;
use tracing::info;

#[tracing::instrument]
pub(crate) fn write_info_file() -> io::Result<()> {
    let path = if std::env::var_os("NUMAFLOW_POD").is_some() {
        "/var/run/numaflow/server-info"
    } else {
        "/tmp/numaflow.server-info"
    };

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
    info!(path, content, "Writing to file");
    fs::write(path, content)
}

pub(crate) fn utc_from_timestamp(t: Option<Timestamp>) -> DateTime<Utc> {
    if let Some(ref t) = t {
        Utc.timestamp_nanos(t.seconds * (t.nanos as i64))
    } else {
        Utc.timestamp_nanos(-1)
    }
}

pub(crate) fn prost_timestamp_from_utc(t: DateTime<Utc>) -> Option<Timestamp> {
    Some(Timestamp {
        seconds: t.timestamp(),
        nanos: t.nanosecond() as i32,
    })
}
