use std::collections::HashMap;
use std::fs;

pub(crate) fn write_info_file() {
    let path = if let Some(_) = std::env::var_os("NUMAFLOW_POD") {
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
