use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

pub(crate) fn doc_comments(lines: &[String]) -> TokenStream2 {
    lines
        .iter()
        .map(|line| {
            let text = if line.starts_with(' ') {
                line.clone()
            } else {
                format!(" {line}")
            };
            quote! { #[doc = #text] }
        })
        .collect()
}
