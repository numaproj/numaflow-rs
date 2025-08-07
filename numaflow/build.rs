fn main() {
    // Only generate protobuf code when PROTO_CODE_GEN=1 is set
    if std::env::var("PROTO_CODE_GEN").unwrap_or("0".to_string()) != "1" {
        return;
    }

    // Ensure the proto output directory exists
    let proto_out_dir = "src/proto";
    std::fs::create_dir_all(proto_out_dir)
        .unwrap_or_else(|e| panic!("failed to create proto output directory: {:?}", e));

    // Generate protobuf code
    tonic_build::configure()
        .build_server(true)
        .build_client(true) // Enable client generation for tests
        .out_dir(proto_out_dir)
        .compile_protos(
            &[
                "proto/source.proto",
                "proto/sourcetransform.proto",
                "proto/map.proto",
                "proto/reduce.proto",
                "proto/sink.proto",
                "proto/sideinput.proto",
                "proto/store.proto",
            ],
            &["proto"],
        )
        .unwrap_or_else(|e| panic!("failed to compile the proto, {:?}", e));

    // Generate the proto module file
    generate_proto_mod_file(proto_out_dir);
}

fn generate_proto_mod_file(proto_out_dir: &str) {
    let mod_content = r#"//! Generated protobuf modules
//! 
//! This module contains all the generated protobuf code for the Numaflow SDK.

#[path = "map.v1.rs"]
pub mod map;

#[path = "reduce.v1.rs"] 
pub mod reduce;

#[path = "source.v1.rs"]
pub mod source;

#[path = "sink.v1.rs"]
pub mod sink;

#[path = "sideinput.v1.rs"]
pub mod side_input;

#[path = "sourcetransformer.v1.rs"]
pub mod source_transformer;

#[path = "serving.v1.rs"]
pub mod serving_store;
"#;

    let mod_file_path = format!("{}/mod.rs", proto_out_dir);
    std::fs::write(&mod_file_path, mod_content)
        .unwrap_or_else(|e| panic!("failed to write proto mod.rs: {:?}", e));
}
