use crate::generator::CodeGenerator;
use crate::util::{rust_type, service_name};
use proc_macro2::TokenStream as TokenStream2;
use prost_build::{Method, Service};
use quote::{format_ident, quote};

impl CodeGenerator {
    pub(crate) fn gen_server_struct(&self, service: &Service) -> manyhow::Result<TokenStream2> {
        let trait_ident = format_ident!("{}", service.name);
        let server_ident = format_ident!("{}Server", service.name);
        Ok(quote! {
            pub struct #server_ident<T: #trait_ident> {
                inner: T,
            }

            impl<T: #trait_ident> #server_ident<T> {
                pub fn new(inner: T) -> Self {
                    Self { inner }
                }

                pub fn into_inner(self) -> T {
                    self.inner
                }
            }
        })
    }

    pub(crate) fn gen_service_impl(&self, service: &Service) -> manyhow::Result<TokenStream2> {
        let trait_ident = format_ident!("{}", service.name);
        let server_ident = format_ident!("{}Server", service.name);
        let service_name = service_name(service);
        let mut arms = vec![];
        let mut descriptors = vec![];
        for method in service.methods.iter() {
            arms.push(self.gen_service_impl_handle_method(method)?);
            descriptors.push(describe_arm(method));
        }
        Ok(quote! {
            impl<T: #trait_ident> proto_service::server::Service for #server_ident<T> {
                const SERVICE_NAME: &'static str = #service_name;

                async fn handle(&self, call: proto_service::server::Call) -> proto_service::CallEnd {
                    match call.method_name.as_str() {
                        #(#arms)*
                        _ => proto_service::CallEnd::error(
                            proto_service::Status::unimplemented("method not found"),
                        ),
                    }
                }

                fn describe_method(
                    &self,
                    method_name: &str,
                ) -> Option<proto_service::server::MethodDescriptor> {
                    match method_name {
                        #(#descriptors)*
                        _ => None,
                    }
                }
            }
        })
    }

    fn gen_service_impl_handle_method(&self, method: &Method) -> manyhow::Result<TokenStream2> {
        let key = method.proto_name.as_str();
        match (method.client_streaming, method.server_streaming) {
            (false, false) => self.gen_service_impl_handle_unary(method, key),
            (false, true) => self.gen_service_impl_handle_server_streaming(method, key),
            (true, false) => self.gen_service_impl_handle_client_streaming(method, key),
            (true, true) => self.gen_service_impl_handle_bidi(method, key),
        }
    }

    fn gen_service_impl_handle_unary(
        &self,
        method: &Method,
        key: &str,
    ) -> manyhow::Result<TokenStream2> {
        let decode = decode_one_request(method)?;
        let respond = send_unary_response(method);
        Ok(quote! { #key => { #decode #respond } })
    }

    fn gen_service_impl_handle_server_streaming(
        &self,
        method: &Method,
        key: &str,
    ) -> manyhow::Result<TokenStream2> {
        let decode = decode_one_request(method)?;
        let respond = stream_response(method)?;
        Ok(quote! { #key => { #decode #respond } })
    }

    fn gen_service_impl_handle_client_streaming(
        &self,
        method: &Method,
        key: &str,
    ) -> manyhow::Result<TokenStream2> {
        let decode = decode_stream_request(method)?;
        let respond = send_unary_response(method);
        Ok(quote! { #key => { #decode #respond } })
    }

    fn gen_service_impl_handle_bidi(
        &self,
        method: &Method,
        key: &str,
    ) -> manyhow::Result<TokenStream2> {
        let decode = decode_stream_request(method)?;
        let respond = stream_response(method)?;
        Ok(quote! { #key => { #decode #respond } })
    }
}

fn decode_one_request(method: &Method) -> manyhow::Result<TokenStream2> {
    let req = rust_type(&method.input_type)?;
    Ok(quote! {
        let bytes = match call.req_payload.into_single() {
            Ok(bytes) => bytes,
            Err(end) => return end,
        };
        let message = match <#req as prost::Message>::decode(bytes) {
            Ok(message) => message,
            Err(_) => {
                return proto_service::CallEnd::error(
                    proto_service::Status::internal("failed to decode request"),
                );
            }
        };
        let request = proto_service::server::Request {
            headers: call.headers,
            extensions: call.extensions,
            message,
        };
    })
}

fn decode_stream_request(method: &Method) -> manyhow::Result<TokenStream2> {
    let req = rust_type(&method.input_type)?;
    Ok(quote! {
        let stream = match call.req_payload.into_streaming() {
            Ok(stream) => stream,
            Err(end) => return end,
        };
        let request = proto_service::server::StreamingRequest::new(
            call.headers,
            call.extensions,
            stream,
            |bytes| {
                <#req as prost::Message>::decode(bytes)
                    .map_err(|_| proto_service::Status::internal("failed to decode request"))
            },
        );
    })
}

fn send_unary_response(method: &Method) -> TokenStream2 {
    let method_ident = format_ident!("{}", method.name);
    quote! {
        match self.inner.#method_ident(request).await {
            Ok(response) => proto_service::CallEnd::single(
                response.headers,
                proto_service::Bytes::from(prost::Message::encode_to_vec(&response.message)),
                response.trailers,
            ),
            Err(status) => proto_service::CallEnd::error(status),
        }
    }
}

fn stream_response(method: &Method) -> manyhow::Result<TokenStream2> {
    let method_ident = format_ident!("{}", method.name);
    let res = rust_type(&method.output_type)?;
    Ok(quote! {
        let sink = match call.streaming_response {
            Some(sink) => sink,
            None => {
                return proto_service::CallEnd::error(
                    proto_service::Status::internal("missing response channel"),
                );
            }
        };
        let response = proto_service::server::StreamingResponse::new(sink, |message: #res| {
            proto_service::Bytes::from(prost::Message::encode_to_vec(&message))
        });
        match self.inner.#method_ident(request, response).await {
            Ok(trailers) => proto_service::CallEnd::streaming(trailers),
            Err(status) => proto_service::CallEnd::error(status),
        }
    })
}

fn describe_arm(method: &Method) -> TokenStream2 {
    let key = method.proto_name.as_str();
    let client_streaming = method.client_streaming;
    let server_streaming = method.server_streaming;
    quote! {
        #key => Some(proto_service::server::MethodDescriptor {
            client_streaming: #client_streaming,
            server_streaming: #server_streaming,
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::generator::CodeGenerator;
    use crate::test_support::example_service;
    use crate::util::pretty;
    use quote::quote;

    #[test]
    fn gen_server_struct_matches_expected() {
        let generated = CodeGenerator.gen_server_struct(&example_service()).unwrap();
        let expected = quote! {
            pub struct GreeterServer<T: Greeter> {
                inner: T,
            }
            impl<T: Greeter> GreeterServer<T> {
                pub fn new(inner: T) -> Self {
                    Self { inner }
                }
                pub fn into_inner(self) -> T {
                    self.inner
                }
            }
        };
        assert_eq!(pretty(generated).unwrap(), pretty(expected).unwrap());
    }

    #[test]
    fn gen_service_impl_matches_expected() {
        let generated = CodeGenerator.gen_service_impl(&example_service()).unwrap();
        let expected = quote! {
            impl<T: Greeter> proto_service::server::Service for GreeterServer<T> {
                const SERVICE_NAME: &'static str = "example.v1.Greeter";
                async fn handle(&self, call: proto_service::server::Call) -> proto_service::CallEnd {
                    match call.method_name.as_str() {
                        "Unary" => {
                            let bytes = match call.req_payload.into_single() {
                                Ok(bytes) => bytes,
                                Err(end) => return end,
                            };
                            let message = match <Ping as prost::Message>::decode(bytes) {
                                Ok(message) => message,
                                Err(_) => {
                                    return proto_service::CallEnd::error(
                                        proto_service::Status::internal("failed to decode request"),
                                    );
                                }
                            };
                            let request = proto_service::server::Request {
                                headers: call.headers,
                                extensions: call.extensions,
                                message,
                            };
                            match self.inner.unary(request).await {
                                Ok(response) => proto_service::CallEnd::single(
                                    response.headers,
                                    proto_service::Bytes::from(prost::Message::encode_to_vec(&response.message)),
                                    response.trailers,
                                ),
                                Err(status) => proto_service::CallEnd::error(status),
                            }
                        }
                        "ServerStream" => {
                            let bytes = match call.req_payload.into_single() {
                                Ok(bytes) => bytes,
                                Err(end) => return end,
                            };
                            let message = match <Ping as prost::Message>::decode(bytes) {
                                Ok(message) => message,
                                Err(_) => {
                                    return proto_service::CallEnd::error(
                                        proto_service::Status::internal("failed to decode request"),
                                    );
                                }
                            };
                            let request = proto_service::server::Request {
                                headers: call.headers,
                                extensions: call.extensions,
                                message,
                            };
                            let sink = match call.streaming_response {
                                Some(sink) => sink,
                                None => {
                                    return proto_service::CallEnd::error(
                                        proto_service::Status::internal("missing response channel"),
                                    );
                                }
                            };
                            let response = proto_service::server::StreamingResponse::new(sink, |message: Pong| {
                                proto_service::Bytes::from(prost::Message::encode_to_vec(&message))
                            });
                            match self.inner.server_stream(request, response).await {
                                Ok(trailers) => proto_service::CallEnd::streaming(trailers),
                                Err(status) => proto_service::CallEnd::error(status),
                            }
                        }
                        "ClientStream" => {
                            let stream = match call.req_payload.into_streaming() {
                                Ok(stream) => stream,
                                Err(end) => return end,
                            };
                            let request = proto_service::server::StreamingRequest::new(
                                call.headers,
                                call.extensions,
                                stream,
                                |bytes| {
                                    <Ping as prost::Message>::decode(bytes)
                                        .map_err(|_| proto_service::Status::internal("failed to decode request"))
                                },
                            );
                            match self.inner.client_stream(request).await {
                                Ok(response) => proto_service::CallEnd::single(
                                    response.headers,
                                    proto_service::Bytes::from(prost::Message::encode_to_vec(&response.message)),
                                    response.trailers,
                                ),
                                Err(status) => proto_service::CallEnd::error(status),
                            }
                        }
                        "Bidi" => {
                            let stream = match call.req_payload.into_streaming() {
                                Ok(stream) => stream,
                                Err(end) => return end,
                            };
                            let request = proto_service::server::StreamingRequest::new(
                                call.headers,
                                call.extensions,
                                stream,
                                |bytes| {
                                    <Ping as prost::Message>::decode(bytes)
                                        .map_err(|_| proto_service::Status::internal("failed to decode request"))
                                },
                            );
                            let sink = match call.streaming_response {
                                Some(sink) => sink,
                                None => {
                                    return proto_service::CallEnd::error(
                                        proto_service::Status::internal("missing response channel"),
                                    );
                                }
                            };
                            let response = proto_service::server::StreamingResponse::new(sink, |message: Pong| {
                                proto_service::Bytes::from(prost::Message::encode_to_vec(&message))
                            });
                            match self.inner.bidi(request, response).await {
                                Ok(trailers) => proto_service::CallEnd::streaming(trailers),
                                Err(status) => proto_service::CallEnd::error(status),
                            }
                        }
                        _ => proto_service::CallEnd::error(
                            proto_service::Status::unimplemented("method not found"),
                        ),
                    }
                }
                fn describe_method(
                    &self,
                    method_name: &str,
                ) -> Option<proto_service::server::MethodDescriptor> {
                    match method_name {
                        "Unary" => Some(proto_service::server::MethodDescriptor {
                            client_streaming: false,
                            server_streaming: false,
                        }),
                        "ServerStream" => Some(proto_service::server::MethodDescriptor {
                            client_streaming: false,
                            server_streaming: true,
                        }),
                        "ClientStream" => Some(proto_service::server::MethodDescriptor {
                            client_streaming: true,
                            server_streaming: false,
                        }),
                        "Bidi" => Some(proto_service::server::MethodDescriptor {
                            client_streaming: true,
                            server_streaming: true,
                        }),
                        _ => None,
                    }
                }
            }
        };
        assert_eq!(pretty(generated).unwrap(), pretty(expected).unwrap());
    }
}
