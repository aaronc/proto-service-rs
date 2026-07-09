include!(concat!(env!("OUT_DIR"), "/example.v1.rs"));

use proto_service::{
    MetadataMap, Request, Response, Result, StreamExt, StreamingRequest, StreamingResponse,
};

pub struct MyGreeter;

impl Greeter for MyGreeter {
    async fn unary(&self, request: Request<Ping>) -> Result<Response<Pong>> {
        Ok(Response::new(Pong {
            msg: request.message.msg,
        }))
    }

    async fn client_stream(&self, mut request: StreamingRequest<Ping>) -> Result<Response<Pong>> {
        let mut all = String::new();
        while let Some(item) = request.rx.next().await {
            all.push_str(&item?.msg);
        }
        Ok(Response::new(Pong { msg: all }))
    }

    async fn server_stream(
        &self,
        request: Request<Ping>,
        mut response: StreamingResponse<Pong>,
    ) -> Result<MetadataMap> {
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

    async fn bidi(
        &self,
        mut request: StreamingRequest<Ping>,
        mut response: StreamingResponse<Pong>,
    ) -> Result<MetadataMap> {
        while let Some(item) = request.rx.next().await {
            response.send_message(Pong { msg: item?.msg }).await?;
        }
        Ok(MetadataMap::new())
    }
}
