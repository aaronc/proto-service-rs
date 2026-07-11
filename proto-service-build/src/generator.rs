use crate::util::pretty;
use manyhow::ToTokensError;
use proc_macro2::TokenStream as TokenStream2;
use prost_build::Service;
use quote::quote;

pub struct CodeGenerator;

impl CodeGenerator {
    fn gen_service(&self, service: &Service) -> manyhow::Result<TokenStream2> {
        let server_trait = self.gen_server_trait(service)?;
        let server_struct = self.gen_server_struct(service)?;
        let service_impl = self.gen_service_impl(service)?;
        let client_struct = self.gen_client_struct(service)?;
        Ok(quote! {
            #server_trait
            #server_struct
            #service_impl
            #client_struct
        })
    }
}

impl prost_build::ServiceGenerator for CodeGenerator {
    fn generate(&mut self, service: Service, buf: &mut String) {
        match self.gen_service(&service).and_then(pretty) {
            Ok(rendered) => buf.push_str(&rendered),
            Err(err) => buf.push_str(&err.into_token_stream().to_string()),
        }
    }
}
