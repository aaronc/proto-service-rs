use proc_macro2::TokenStream as TokenStream2;
use prost_build::Service;
use quote::ToTokens;

pub(crate) fn rust_type(prost_type: &str) -> manyhow::Result<TokenStream2> {
    Ok(syn::parse_str::<syn::Type>(prost_type)?.to_token_stream())
}

pub(crate) fn service_name(service: &Service) -> String {
    if service.package.is_empty() {
        service.proto_name.clone()
    } else {
        format!("{}.{}", service.package, service.proto_name)
    }
}

pub(crate) fn pretty(tokens: TokenStream2) -> manyhow::Result<String> {
    Ok(prettyplease::unparse(&syn::parse2::<syn::File>(tokens)?))
}
