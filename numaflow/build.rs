use std::env;
use tonic_prost_build::Config;

fn main() {
    if env::var("PROTO_CODE_GEN").unwrap_or("0".to_string()) != "1" {
        return;
    }
    let generated_out_dir = "src/generated";
    std::fs::create_dir_all(generated_out_dir)
        .unwrap_or_else(|e| panic!("failed to create generated output directory: {:?}", e));

    Config::new()
        .out_dir("src/common")
        .compile_protos(&["proto/metadata.proto"], &["proto"])
        .expect("failed to compile common protos");

    tonic_prost_build::configure()
        .out_dir(generated_out_dir)
        .extern_path(".metadata", "crate::proto::metadata")
        .compile_protos(
            &[
                "proto/source.proto",
                "proto/sourcetransform.proto",
                "proto/map.proto",
                "proto/reduce.proto",
                "proto/sink.proto",
                "proto/sideinput.proto",
                "proto/store.proto",
                "proto/sessionreduce.proto",
                "proto/accumulator.proto",
            ],
            &["proto"],
        )
        .unwrap_or_else(|e| panic!("failed to compile the proto, {:?}", e));
}
