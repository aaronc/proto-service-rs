use crate::doc::doc_comments;
use crate::generator::CodeGenerator;
use crate::util::{rust_type, service_name};
use proc_macro2::TokenStream as TokenStream2;
use prost_build::{Method, Service};
use quote::{format_ident, quote};

impl CodeGenerator {
    pub(crate) fn gen_client_struct(&self, service: &Service) -> manyhow::Result<TokenStream2> {
        let client_ident = format_ident!("{}Client", service.name);
        let doc = doc_comments(&service.comments.leading);
        let service_name = service_name(service);
        let mut methods = vec![];
        for method in service.methods.iter() {
            methods.push(self.gen_client_method(method, &service_name)?);
        }
        Ok(quote! {
            #doc
            #[derive(Clone)]
            pub struct #client_ident {
                conn: proto_service::client::Connection,
            }

            impl<T: proto_service::client::ClientConnection + 'static> From<T> for #client_ident {
                fn from(conn: T) -> Self {
                    Self { conn: proto_service::client::Connection::new(conn) }
                }
            }

            impl #client_ident {
                #(#methods)*
            }
        })
    }

    fn gen_client_method(
        &self,
        method: &Method,
        service_name: &str,
    ) -> manyhow::Result<TokenStream2> {
        match (method.client_streaming, method.server_streaming) {
            (false, false) => self.gen_client_unary_method(method, service_name),
            (false, true) => self.gen_client_server_streaming_method(method, service_name),
            (true, false) => self.gen_client_client_streaming_method(method, service_name),
            (true, true) => self.gen_client_bidi_method(method, service_name),
        }
    }

    fn gen_client_unary_method(
        &self,
        method: &Method,
        service_name: &str,
    ) -> manyhow::Result<TokenStream2> {
        let doc = doc_comments(&method.comments.leading);
        let name = format_ident!("{}", method.name);
        let req = rust_type(&method.input_type)?;
        let res = rust_type(&method.output_type)?;
        let path = full_method(service_name, method);
        let decode = decode_closure(method)?;
        Ok(quote! {
            #doc
            pub async fn #name(
                &self,
                request: #req,
                headers: proto_service::MetadataMap,
            ) -> proto_service::Result<proto_service::Response<#res>> {
                let request = proto_service::Bytes::from(prost::Message::encode_to_vec(&request));
                self.conn.unary(#path, headers, request).await.into_response(#decode)
            }
        })
    }

    fn gen_client_server_streaming_method(
        &self,
        method: &Method,
        service_name: &str,
    ) -> manyhow::Result<TokenStream2> {
        let doc = doc_comments(&method.comments.leading);
        let name = format_ident!("{}", method.name);
        let req = rust_type(&method.input_type)?;
        let res = rust_type(&method.output_type)?;
        let path = full_method(service_name, method);
        let decode = decode_closure(method)?;
        Ok(quote! {
            #doc
            pub async fn #name(
                &self,
                request: #req,
                headers: proto_service::MetadataMap,
            ) -> proto_service::Result<proto_service::client::ResponseStream<#res>> {
                let request = proto_service::Bytes::from(prost::Message::encode_to_vec(&request));
                let response = self.conn.server_stream(#path, headers, request);
                proto_service::client::ResponseStream::read(response, #decode).await
            }
        })
    }

    fn gen_client_client_streaming_method(
        &self,
        method: &Method,
        service_name: &str,
    ) -> manyhow::Result<TokenStream2> {
        let doc = doc_comments(&method.comments.leading);
        let name = format_ident!("{}", method.name);
        let req = rust_type(&method.input_type)?;
        let res = rust_type(&method.output_type)?;
        let path = full_method(service_name, method);
        let encode = encode_closure(method)?;
        let decode = decode_closure(method)?;
        Ok(quote! {
            #doc
            pub fn #name(
                &self,
                headers: proto_service::MetadataMap,
            ) -> proto_service::client::ClientStream<#req, #res> {
                let (sink, response) = self.conn.client_stream(#path, headers);
                let sink = proto_service::client::RequestSink::new(sink, #encode);
                proto_service::client::ClientStream::new(sink, response, #decode)
            }
        })
    }

    fn gen_client_bidi_method(
        &self,
        method: &Method,
        service_name: &str,
    ) -> manyhow::Result<TokenStream2> {
        let doc = doc_comments(&method.comments.leading);
        let name = format_ident!("{}", method.name);
        let req = rust_type(&method.input_type)?;
        let res = rust_type(&method.output_type)?;
        let path = full_method(service_name, method);
        let encode = encode_closure(method)?;
        let decode = decode_closure(method)?;
        Ok(quote! {
            #doc
            pub fn #name(
                &self,
                headers: proto_service::MetadataMap,
            ) -> (
                proto_service::client::RequestSink<#req>,
                impl core::future::Future<
                    Output = proto_service::Result<proto_service::client::ResponseStream<#res>>,
                >,
            ) {
                let (sink, response) = self.conn.bidi(#path, headers);
                let sink = proto_service::client::RequestSink::new(sink, #encode);
                let response = proto_service::client::ResponseStream::read(response, #decode);
                (sink, response)
            }
        })
    }
}

fn encode_closure(method: &Method) -> manyhow::Result<TokenStream2> {
    let req = rust_type(&method.input_type)?;
    Ok(quote! {
        |message: #req| {
            proto_service::Bytes::from(prost::Message::encode_to_vec(&message))
        }
    })
}

fn decode_closure(method: &Method) -> manyhow::Result<TokenStream2> {
    let res = rust_type(&method.output_type)?;
    Ok(quote! {
        |bytes| {
            <#res as prost::Message>::decode(bytes)
                .map_err(|_| proto_service::Status::internal("failed to decode response"))
        }
    })
}

fn full_method(service_name: &str, method: &Method) -> String {
    format!("/{}/{}", service_name, method.proto_name)
}

#[cfg(test)]
mod tests {
    use crate::generator::CodeGenerator;
    use crate::test_support::example_service;
    use crate::util::pretty;
    use quote::quote;

    #[test]
    fn gen_client_struct_matches_expected() {
        let generated = CodeGenerator.gen_client_struct(&example_service()).unwrap();
        let expected = quote! {
            #[doc = " Greeter service."]
            #[derive(Clone)]
            pub struct GreeterClient {
                conn: proto_service::client::Connection,
            }
            impl<T: proto_service::client::ClientConnection + 'static> From<T> for GreeterClient {
                fn from(conn: T) -> Self {
                    Self { conn: proto_service::client::Connection::new(conn) }
                }
            }
            impl GreeterClient {
                #[doc = " Says hello."]
                pub async fn unary(
                    &self,
                    request: Ping,
                    headers: proto_service::MetadataMap,
                ) -> proto_service::Result<proto_service::Response<Pong>> {
                    let request = proto_service::Bytes::from(prost::Message::encode_to_vec(&request));
                    self.conn.unary("/example.v1.Greeter/Unary", headers, request).await.into_response(|bytes| {
                        <Pong as prost::Message>::decode(bytes)
                            .map_err(|_| proto_service::Status::internal("failed to decode response"))
                    })
                }
                pub async fn server_stream(
                    &self,
                    request: Ping,
                    headers: proto_service::MetadataMap,
                ) -> proto_service::Result<proto_service::client::ResponseStream<Pong>> {
                    let request = proto_service::Bytes::from(prost::Message::encode_to_vec(&request));
                    let response =
                        self.conn.server_stream("/example.v1.Greeter/ServerStream", headers, request);
                    proto_service::client::ResponseStream::read(response, |bytes| {
                        <Pong as prost::Message>::decode(bytes)
                            .map_err(|_| proto_service::Status::internal("failed to decode response"))
                    }).await
                }
                pub fn client_stream(
                    &self,
                    headers: proto_service::MetadataMap,
                ) -> proto_service::client::ClientStream<Ping, Pong> {
                    let (sink, response) =
                        self.conn.client_stream("/example.v1.Greeter/ClientStream", headers);
                    let sink = proto_service::client::RequestSink::new(sink, |message: Ping| {
                        proto_service::Bytes::from(prost::Message::encode_to_vec(&message))
                    });
                    proto_service::client::ClientStream::new(sink, response, |bytes| {
                        <Pong as prost::Message>::decode(bytes)
                            .map_err(|_| proto_service::Status::internal("failed to decode response"))
                    })
                }
                pub fn bidi(
                    &self,
                    headers: proto_service::MetadataMap,
                ) -> (
                    proto_service::client::RequestSink<Ping>,
                    impl core::future::Future<
                        Output = proto_service::Result<proto_service::client::ResponseStream<Pong>>,
                    >,
                ) {
                    let (sink, response) = self.conn.bidi("/example.v1.Greeter/Bidi", headers);
                    let sink = proto_service::client::RequestSink::new(sink, |message: Ping| {
                        proto_service::Bytes::from(prost::Message::encode_to_vec(&message))
                    });
                    let response = proto_service::client::ResponseStream::read(response, |bytes| {
                        <Pong as prost::Message>::decode(bytes)
                            .map_err(|_| proto_service::Status::internal("failed to decode response"))
                    });
                    (sink, response)
                }
            }
        };
        assert_eq!(pretty(generated).unwrap(), pretty(expected).unwrap());
    }
}
