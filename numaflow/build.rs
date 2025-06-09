use std::env;

fn main() {
    if env::var("PROTO_CODE_GEN").unwrap_or("0".to_string()) != "1" {
        return;
    }
    tonic_build::configure()
        .build_server(true)
        .out_dir("src/servers")
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
            ],
            &["proto"],
        )
        .unwrap_or_else(|e| panic!("failed to compile the proto, {:?}", e))
}
