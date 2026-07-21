//! End-to-end tests: generated `GreeterClient` -> pipe transport over in-process
//! channels -> `PipeServer` -> generated `GreeterServer` -> `Greeter` impl.

use std::sync::Arc;

use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use proto_service::server::{Request, ServiceRouter, StreamingRequest, StreamingResponse};
use proto_service::{Code, MetadataMap, Response, Result, Status};
use proto_service_examples::{Greeter, GreeterClient, GreeterServer, Ping, Pong};
use proto_service_pipe::client::PipeConnection;
use proto_service_pipe::packet::{RequestPacket, ResponsePacket};
use proto_service_pipe::spawner::TokioSpawner;
use proto_service_pipe::transport::PipeServer;

struct EchoGreeter;

impl Greeter for EchoGreeter {
    async fn unary(&self, request: Request<Ping>) -> Result<Response<Pong>> {
        if request.message.msg == "boom" {
            return Err(Status::invalid_argument("boom"));
        }
        let mut response = Response::new(Pong {
            msg: request.message.msg,
        });
        if let Some(echo) = request.headers.get_first("x-echo") {
            response.headers.insert("x-echo", echo);
        }
        response.trailers.insert("x-unary", "done");
        Ok(response)
    }

    async fn server_stream(
        &self,
        request: Request<Ping>,
        mut response: StreamingResponse<Pong>,
    ) -> Result<MetadataMap> {
        let mut headers = MetadataMap::new();
        headers.insert("x-stream", "start");
        response
            .send_headers(headers)
            .await
            .map_err(|_| Status::internal("response closed"))?;
        for i in 0..3 {
            response
                .send_message(Pong {
                    msg: format!("{}-{i}", request.message.msg),
                })
                .await?;
        }
        let mut trailers = MetadataMap::new();
        trailers.insert("x-count", "3");
        Ok(trailers)
    }

    async fn client_stream(&self, mut request: StreamingRequest<Ping>) -> Result<Response<Pong>> {
        let mut all = String::new();
        while let Some(item) = request.rx.next().await {
            all.push_str(&item?.msg);
        }
        let mut response = Response::new(Pong { msg: all });
        response.trailers.insert("x-collected", "true");
        Ok(response)
    }

    async fn bidi(
        &self,
        mut request: StreamingRequest<Ping>,
        mut response: StreamingResponse<Pong>,
    ) -> Result<MetadataMap> {
        while let Some(item) = request.rx.next().await {
            response.send_message(Pong { msg: item?.msg }).await?;
        }
        let mut trailers = MetadataMap::new();
        trailers.insert("x-bidi", "over");
        Ok(trailers)
    }
}

fn connect_to_router(router: ServiceRouter) -> GreeterClient {
    let (req_tx, req_rx) = mpsc::unbounded::<RequestPacket>();
    let (res_tx, res_rx) = mpsc::unbounded::<ResponsePacket>();

    let server = PipeServer::new(Arc::new(router), TokioSpawner);
    tokio::spawn(async move { server.serve(req_rx, res_tx).await });

    let conn = PipeConnection::new(res_rx, req_tx);
    let client = GreeterClient::from(conn.new_client());
    tokio::spawn(conn.run());
    client
}

fn greeter_client() -> GreeterClient {
    let mut router = ServiceRouter::default();
    router.register(GreeterServer::new(EchoGreeter));
    connect_to_router(router)
}

#[tokio::test]
async fn unary_roundtrips_message_and_metadata() {
    let client = greeter_client();
    let mut headers = MetadataMap::new();
    headers.insert("x-echo", "meta");
    let response = client
        .unary(Ping { msg: "hi".into() }, headers)
        .await
        .unwrap();
    assert_eq!(response.message.msg, "hi");
    assert_eq!(response.headers.get_first("x-echo"), Some("meta"));
    assert_eq!(response.trailers.get_first("x-unary"), Some("done"));
}

#[tokio::test]
async fn unary_propagates_error_status() {
    let client = greeter_client();
    let err = client
        .unary(Ping { msg: "boom".into() }, MetadataMap::new())
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(err.message(), "boom");
}

#[tokio::test]
async fn server_stream_yields_headers_messages_trailers() {
    let client = greeter_client();
    let mut stream = client
        .server_stream(Ping { msg: "s".into() }, MetadataMap::new())
        .await
        .unwrap();
    assert_eq!(stream.headers().get_first("x-stream"), Some("start"));
    assert!(stream.trailers().is_none());
    let mut messages = vec![];
    while let Some(item) = stream.next().await {
        messages.push(item.unwrap().msg);
    }
    assert_eq!(messages, ["s-0", "s-1", "s-2"]);
    assert_eq!(stream.trailers().unwrap().get_first("x-count"), Some("3"));
}

#[tokio::test]
async fn client_stream_collects_and_finishes() {
    let client = greeter_client();
    let mut call = client.client_stream(MetadataMap::new());
    for part in ["a", "b", "c"] {
        call.send(Ping { msg: part.into() }).await.unwrap();
    }
    let response = call.finish().await.unwrap();
    assert_eq!(response.message.msg, "abc");
    assert_eq!(response.trailers.get_first("x-collected"), Some("true"));
}

#[tokio::test]
async fn bidi_echoes_interleaved_then_ends_clean() {
    let client = greeter_client();
    let (mut sink, response) = client.bidi(MetadataMap::new());
    sink.send(Ping { msg: "x".into() }).await.unwrap();
    let mut stream = response.await.unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().msg, "x");
    sink.send(Ping { msg: "y".into() }).await.unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().msg, "y");
    sink.done().await.unwrap();
    assert!(stream.next().await.is_none());
    assert_eq!(stream.trailers().unwrap().get_first("x-bidi"), Some("over"));
}

#[tokio::test]
async fn unknown_service_is_rejected_at_open() {
    let client = connect_to_router(ServiceRouter::default());
    let err = client
        .unary(Ping { msg: "hi".into() }, MetadataMap::new())
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), Code::Unimplemented);
    let err = client
        .server_stream(Ping { msg: "hi".into() }, MetadataMap::new())
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), Code::Unimplemented);
}

#[tokio::test]
async fn dropping_response_stream_cancels_without_killing_connection() {
    let client = greeter_client();
    let stream = client
        .server_stream(Ping { msg: "s".into() }, MetadataMap::new())
        .await
        .unwrap();
    drop(stream);
    let response = client
        .unary(
            Ping {
                msg: "alive".into(),
            },
            MetadataMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(response.message.msg, "alive");
}

#[tokio::test]
async fn dropping_client_stream_without_finish_cancels() {
    let client = greeter_client();
    let mut call = client.client_stream(MetadataMap::new());
    call.send(Ping { msg: "a".into() }).await.unwrap();
    drop(call);
    // Connection still serves new calls after the cancel.
    let response = client
        .unary(
            Ping {
                msg: "alive".into(),
            },
            MetadataMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(response.message.msg, "alive");
}
