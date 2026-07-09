fn main() {
    let mut config = prost_build::Config::new();
    config.service_generator(Box::new(proto_service_build::CodeGenerator));
    config
        .compile_protos(&["proto/greeter.proto"], &["proto"])
        .unwrap();
}
