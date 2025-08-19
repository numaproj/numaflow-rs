//! Types and constants used across the Numaflow SDK
//!
//! This module contains common types, enums, and constants that are shared
//! across different parts of the SDK.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;

// Map mode constants
const MAP_MODE_KEY: &str = "MAP_MODE";
const UNARY_MAP: &str = "unary-map";
const BATCH_MAP: &str = "batch-map";
const STREAM_MAP: &str = "stream-map";

/// Environment variable for the container type
pub const ENV_CONTAINER_TYPE: &str = "NUMAFLOW_UD_CONTAINER_TYPE";

/// Drop message constant
pub const DROP: &str = "U+005C__DROP__";

/// Container types supported by the SDK
#[derive(Eq, PartialEq, Hash)]
pub enum ContainerType {
    Map,
    BatchMap,
    MapStream,
    Reduce,
    SessionReduce,
    Accumulator,
    Sink,
    Source,
    SourceTransformer,
    SideInput,
    Serving,
}

// Minimum version of Numaflow required by the current SDK version
//
// Updating this value:
// 1. For release candidate (RC) versions, use the RC version string directly.
//    Example: For version 1.4.1-rc1, enter "1.4.1-rc1"
// 2. For stable versions, append "-z" to the stable version string.
//    Example: For version 1.4.1, enter "1.4.0-z"
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
pub static MINIMUM_NUMAFLOW_VERSION: LazyLock<HashMap<ContainerType, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert(ContainerType::Source, "1.4.0-z");
        m.insert(ContainerType::Map, "1.4.0-z");
        m.insert(ContainerType::BatchMap, "1.4.0-z");
        m.insert(ContainerType::Reduce, "1.4.0-z");
        m.insert(ContainerType::SessionReduce, "1.4.0-z");
        m.insert(ContainerType::Accumulator, "1.4.0-z");
        m.insert(ContainerType::Sink, "1.4.0-z");
        m.insert(ContainerType::SourceTransformer, "1.4.0-z");
        m.insert(ContainerType::SideInput, "1.4.0-z");
        m.insert(ContainerType::Serving, "1.5.0-z");
        m
    });

/// SDK version from Cargo.toml
const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Server information structure for storing server-related metadata
#[derive(Serialize, Deserialize, Debug)]
pub struct ServerInfo {
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
    pub fn new(container_type: ContainerType) -> Self {
        let mut metadata: HashMap<String, String> = HashMap::new();
        if container_type == ContainerType::Map
            || container_type == ContainerType::BatchMap
            || container_type == ContainerType::MapStream
        {
            metadata.insert(
                MAP_MODE_KEY.to_string(),
                match container_type {
                    ContainerType::Map => UNARY_MAP.to_string(),
                    ContainerType::BatchMap => BATCH_MAP.to_string(),
                    ContainerType::MapStream => STREAM_MAP.to_string(),
                    _ => "".to_string(),
                },
            );
        }
        ServerInfo {
            protocol: "uds".to_string(),
            language: "rust".to_string(),
            minimum_numaflow_version: MINIMUM_NUMAFLOW_VERSION
                .get(&container_type)
                .map(|&version| version.to_string())
                .unwrap_or_default(),
            version: SDK_VERSION.to_string(),
            metadata: Option::from(metadata),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_info_new_batch_map() {
        let info = ServerInfo::new(ContainerType::BatchMap);
        assert_eq!(info.protocol, "uds");
        assert_eq!(info.language, "rust");
        assert_eq!(info.minimum_numaflow_version, "1.4.0-z");
        assert!(info.metadata.is_some());

        let metadata = info.metadata.unwrap();
        assert_eq!(metadata.get("MAP_MODE"), Some(&"batch-map".to_string()));
    }

    #[test]
    fn test_server_info_new_source() {
        let info = ServerInfo::new(ContainerType::Source);
        assert_eq!(info.protocol, "uds");
        assert_eq!(info.language, "rust");
        assert_eq!(info.minimum_numaflow_version, "1.4.0-z");
        assert!(info.metadata.is_some());

        let metadata = info.metadata.unwrap();
        assert!(metadata.is_empty()); // Source doesn't have MAP_MODE
    }
}
