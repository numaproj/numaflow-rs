use std::collections::HashMap;
use std::fs;

use chrono::{DateTime, TimeZone, Utc};
use prost_types::Timestamp;

pub(crate) fn write_info_file() {
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
    let content = info.to_string();
    let content = format!("{}U+005C__END__", content);
    println!("wrote to {} {}", path, content);
    fs::write(path, content).unwrap();
}

pub(crate) fn utc_from_timestamp(t: Option<Timestamp>) -> DateTime<Utc> {
    if let Some(ref t) = t {
        Utc.timestamp_nanos(t.seconds * (t.nanos as i64))
    } else {
        Utc.timestamp_nanos(-1)
    }
}
