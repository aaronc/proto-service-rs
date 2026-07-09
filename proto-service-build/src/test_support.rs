use prost_build::{Comments, Method, Service};

pub(crate) fn example_service() -> Service {
    Service {
        name: "Greeter".into(),
        proto_name: "Greeter".into(),
        package: "example.v1".into(),
        comments: comments("Greeter service."),
        methods: vec![
            method("unary", "Unary", false, false, "Says hello."),
            method("server_stream", "ServerStream", false, true, ""),
            method("client_stream", "ClientStream", true, false, ""),
            method("bidi", "Bidi", true, true, ""),
        ],
        options: Default::default(),
    }
}

fn method(
    name: &str,
    proto_name: &str,
    client_streaming: bool,
    server_streaming: bool,
    doc: &str,
) -> Method {
    Method {
        name: name.into(),
        proto_name: proto_name.into(),
        comments: comments(doc),
        input_type: "Ping".into(),
        output_type: "Pong".into(),
        input_proto_type: ".example.v1.Ping".into(),
        output_proto_type: ".example.v1.Pong".into(),
        options: Default::default(),
        client_streaming,
        server_streaming,
    }
}

fn comments(leading: &str) -> Comments {
    Comments {
        leading: leading.lines().map(str::to_string).collect(),
        ..Default::default()
    }
}
