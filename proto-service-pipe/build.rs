fn main() {
    println!("cargo:rerun-if-changed=../proto");
    prost_build::compile_protos(
        &["../proto/proto_pipe/v1/packet.proto"],
        &["../proto"],
    )
    .unwrap();
}
