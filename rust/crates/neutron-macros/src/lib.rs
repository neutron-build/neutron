//! Procedural macros for the Neutron web framework.
//!
//! Provides `#[derive(FromRef)]`, which generates `neutron::FromRef<S>` impls
//! extracting each field of a composite application-state struct as a sub-state.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Derive `neutron::FromRef<Self>` for each field of a struct.
///
/// For `#[derive(FromRef)] struct AppState { db: Db, cache: Cache }` this emits:
///
/// ```ignore
/// impl ::neutron::FromRef<AppState> for Db    { fn from_ref(s: &AppState) -> Db    { s.db.clone() } }
/// impl ::neutron::FromRef<AppState> for Cache { fn from_ref(s: &AppState) -> Cache { s.cache.clone() } }
/// ```
///
/// Fields whose type is the struct itself are skipped (the blanket identity
/// `impl FromRef<S> for S` already covers that case). Only named-field structs
/// are supported; tuple structs, unit structs, enums, and generic targets emit
/// a `compile_error!`.
#[proc_macro_derive(FromRef)]
pub fn derive_from_ref(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Out of scope: generic state structs (the impl-target identity check below
    // assumes a concrete `name`).
    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &input.generics,
            "#[derive(FromRef)] does not support generic state structs",
        )
        .to_compile_error()
        .into();
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return syn::Error::new_spanned(
                    name,
                    "#[derive(FromRef)] requires a struct with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                name,
                "#[derive(FromRef)] can only be applied to structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let impls = fields.iter().filter_map(|field| {
        let field_name = field.ident.as_ref()?;
        let field_ty = &field.ty;

        // Skip fields whose type equals the struct itself — the identity impl
        // (`impl<S: Clone> FromRef<S> for S`) already covers `S: S`, and emitting
        // a second impl here would conflict.
        if let syn::Type::Path(tp) = field_ty {
            if tp.qself.is_none() && tp.path.is_ident(name) {
                return None;
            }
        }

        Some(quote! {
            impl ::neutron::FromRef<#name> for #field_ty {
                fn from_ref(state: &#name) -> #field_ty {
                    ::core::clone::Clone::clone(&state.#field_name)
                }
            }
        })
    });

    quote! { #(#impls)* }.into()
}
