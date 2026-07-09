use crate::doc::doc_comments;
use crate::generator::CodeGenerator;
use crate::util::rust_type;
use proc_macro2::TokenStream as TokenStream2;
use prost_build::{Method, Service};
use quote::{format_ident, quote};

impl CodeGenerator {
    pub(crate) fn gen_server_trait(&self, service: &Service) -> manyhow::Result<TokenStream2> {
        let trait_ident = format_ident!("{}", service.name);
        let doc = doc_comments(&service.comments.leading);
        let mut methods = vec![];
        for method in service.methods.iter() {
            methods.push(self.gen_server_trait_method(method)?);
        }
        Ok(quote! {
            #doc
            pub trait #trait_ident: core::marker::Send + core::marker::Sync + 'static {
                #(#methods)*
            }
        })
    }

    fn gen_server_trait_method(&self, method: &Method) -> manyhow::Result<TokenStream2> {
        match (method.client_streaming, method.server_streaming) {
            (false, false) => self.gen_server_trait_unary_method(method),
            (false, true) => self.gen_server_trait_server_streaming_method(method),
            (true, false) => self.gen_server_trait_client_streaming_method(method),
            (true, true) => self.gen_server_trait_bidi_method(method),
        }
    }

    fn gen_server_trait_unary_method(&self, method: &Method) -> manyhow::Result<TokenStream2> {
        let doc = doc_comments(&method.comments.leading);
        let name = format_ident!("{}", method.name);
        let req = rust_type(&method.input_type)?;
        let res = rust_type(&method.output_type)?;
        Ok(quote! {
            #doc
            fn #name(&self, request: proto_service::server::Request<#req>)
                -> impl core::future::Future<
                    Output = proto_service::Result<proto_service::server::Response<#res>>,
                > + Send;
        })
    }

    fn gen_server_trait_server_streaming_method(
        &self,
        method: &Method,
    ) -> manyhow::Result<TokenStream2> {
        let doc = doc_comments(&method.comments.leading);
        let name = format_ident!("{}", method.name);
        let req = rust_type(&method.input_type)?;
        let res = rust_type(&method.output_type)?;
        Ok(quote! {
            #doc
            fn #name(
                &self,
                request: proto_service::server::Request<#req>,
                response: proto_service::server::StreamingResponse<#res>,
            ) -> impl core::future::Future<
                Output = proto_service::Result<proto_service::MetadataMap>,
            > + Send;
        })
    }

    fn gen_server_trait_client_streaming_method(
        &self,
        method: &Method,
    ) -> manyhow::Result<TokenStream2> {
        let doc = doc_comments(&method.comments.leading);
        let name = format_ident!("{}", method.name);
        let req = rust_type(&method.input_type)?;
        let res = rust_type(&method.output_type)?;
        Ok(quote! {
            #doc
            fn #name(&self, request: proto_service::server::StreamingRequest<#req>)
                -> impl core::future::Future<
                    Output = proto_service::Result<proto_service::server::Response<#res>>,
                > + Send;
        })
    }

    fn gen_server_trait_bidi_method(&self, method: &Method) -> manyhow::Result<TokenStream2> {
        let doc = doc_comments(&method.comments.leading);
        let name = format_ident!("{}", method.name);
        let req = rust_type(&method.input_type)?;
        let res = rust_type(&method.output_type)?;
        Ok(quote! {
            #doc
            fn #name(
                &self,
                request: proto_service::server::StreamingRequest<#req>,
                response: proto_service::server::StreamingResponse<#res>,
            ) -> impl core::future::Future<
                Output = proto_service::Result<proto_service::MetadataMap>,
            > + Send;
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::generator::CodeGenerator;
    use crate::test_support::example_service;
    use crate::util::pretty;
    use quote::quote;

    #[test]
    fn gen_server_trait_matches_expected() {
        let generated = CodeGenerator.gen_server_trait(&example_service()).unwrap();
        let expected = quote! {
            #[doc = " Greeter service."]
            pub trait Greeter: core::marker::Send + core::marker::Sync + 'static {
                #[doc = " Says hello."]
                fn unary(&self, request: proto_service::server::Request<Ping>)
                    -> impl core::future::Future<
                        Output = proto_service::Result<proto_service::server::Response<Pong>>,
                    > + Send;
                fn server_stream(
                    &self,
                    request: proto_service::server::Request<Ping>,
                    response: proto_service::server::StreamingResponse<Pong>,
                ) -> impl core::future::Future<
                    Output = proto_service::Result<proto_service::MetadataMap>,
                > + Send;
                fn client_stream(&self, request: proto_service::server::StreamingRequest<Ping>)
                    -> impl core::future::Future<
                        Output = proto_service::Result<proto_service::server::Response<Pong>>,
                    > + Send;
                fn bidi(
                    &self,
                    request: proto_service::server::StreamingRequest<Ping>,
                    response: proto_service::server::StreamingResponse<Pong>,
                ) -> impl core::future::Future<
                    Output = proto_service::Result<proto_service::MetadataMap>,
                > + Send;
            }
        };
        assert_eq!(pretty(generated).unwrap(), pretty(expected).unwrap());
    }
}
