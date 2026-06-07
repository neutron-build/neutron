//! Procedural macros for the Neutron web framework.
//!
//! Provides `#[derive(FromRef)]`, which generates `neutron::FromRef<S>` impls
//! extracting each field of a composite application-state struct as a sub-state.

use proc_macro::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{parse_macro_input, Data, DeriveInput, Fields, FnArg, ItemFn, ReturnType};

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

/// Derive `neutron::openapi::ApiSchema` for a struct of `serde`-friendly fields.
///
/// Generates `fn schema() -> Schema` that builds a JSON Schema `object`: each
/// field becomes a property whose schema is `<FieldType as ApiSchema>::schema()`,
/// and every non-`Option` field is marked `required`. Field names follow the
/// Rust identifiers (matching `serde`'s default).
///
/// ```ignore
/// #[derive(ApiSchema)]
/// struct User { id: u64, name: String, nickname: Option<String> }
/// // => object { id: integer(int64), name: string, nickname: string|nullable }
/// //    required: ["id", "name"]
/// ```
///
/// Only named-field structs are supported; tuple/unit structs, enums, and
/// generic targets emit a `compile_error!`.
#[proc_macro_derive(ApiSchema)]
pub fn derive_api_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &input.generics,
            "#[derive(ApiSchema)] does not support generic types",
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
                    "#[derive(ApiSchema)] requires a struct with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(
                name,
                "#[derive(ApiSchema)] can only be applied to structs",
            )
            .to_compile_error()
            .into();
        }
    };

    let mut properties = Vec::new();
    let mut required = Vec::new();
    for field in fields {
        let Some(ident) = field.ident.as_ref() else {
            continue;
        };
        let field_name = ident.to_string();
        let ty = &field.ty;
        properties.push(quote! {
            .property(
                #field_name,
                <#ty as ::neutron::openapi::ApiSchema>::schema(),
            )
        });
        // A field is required unless its type is `Option<...>`.
        if !is_option(ty) {
            required.push(field_name);
        }
    }

    let required_lit = &required;
    quote! {
        impl ::neutron::openapi::ApiSchema for #name {
            fn schema() -> ::neutron::openapi::Schema {
                ::neutron::openapi::Schema::object()
                    #(#properties)*
                    .required(&[#(#required_lit),*])
                    .build()
            }
        }
    }
    .into()
}

/// Returns `true` if `ty` is syntactically `Option<...>`.
fn is_option(ty: &syn::Type) -> bool {
    if let syn::Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            return seg.ident == "Option";
        }
    }
    false
}

/// Improve the compile errors for a Neutron handler `async fn`.
///
/// Neutron handlers are plain `async fn`s; whether one is a valid handler is
/// decided by a blanket trait impl. When a handler *doesn't* satisfy that impl —
/// an argument isn't an extractor, the return type isn't a response, the future
/// isn't `Send` — the error normally surfaces deep inside the router's generic
/// machinery (`the trait Handler<_> is not implemented`), pointing at the call
/// site rather than the offending code.
///
/// Annotating the handler with `#[debug_handler]` emits hidden, span-targeted
/// assertions so the error lands on the exact argument or return type:
///
/// ```ignore
/// #[neutron::debug_handler]
/// async fn create(payload: NotAnExtractor) -> impl IntoResponse { /* ... */ }
/// //                       ^^^^^^^^^^^^^^^ the trait `FromRequest` is not implemented
/// ```
///
/// The macro is a no-op on the generated code path: the original `fn` is emitted
/// unchanged, so removing the attribute never changes behavior. It is intended
/// for development; leaving it on is harmless (the assertions compile away).
#[proc_macro_attribute]
pub fn debug_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);

    // The function must be async — handlers return a future.
    if func.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            func.sig.fn_token,
            "#[debug_handler] expects an `async fn` — Neutron handlers return a future",
        )
        .to_compile_error()
        .into();
    }

    // One `FromRequest` assertion per argument, span-located on the argument's
    // type so the error points at the offending parameter.
    let arg_asserts = func.sig.inputs.iter().enumerate().map(|(i, arg)| {
        match arg {
            FnArg::Receiver(recv) => quote_spanned! {recv.span()=>
                compile_error!("#[debug_handler] handlers are free functions and cannot take `self`");
            },
            FnArg::Typed(pat) => {
                let ty = &pat.ty;
                let assert_name = syn::Ident::new(
                    &format!("__neutron_assert_arg_{i}"),
                    ty.span(),
                );
                quote_spanned! {ty.span()=>
                    fn #assert_name<T: ::neutron::extract::FromRequest>() {}
                    let _ = #assert_name::<#ty>;
                }
            }
        }
    });

    // The return type must implement `IntoResponse`. For `-> impl IntoResponse`
    // the bound is already explicit at the function signature, and `impl Trait`
    // cannot appear as a turbofish argument, so we only assert concrete types.
    let ret_assert = match &func.sig.output {
        ReturnType::Default => quote! {
            // `-> ()` is a valid response (empty body); nothing to assert.
        },
        ReturnType::Type(_, ty) if matches!(**ty, syn::Type::ImplTrait(_)) => quote! {
            // `impl Trait` return: the IntoResponse bound is already on the
            // signature; nothing to add.
        },
        ReturnType::Type(_, ty) => {
            let assert_name = syn::Ident::new("__neutron_assert_ret", ty.span());
            quote_spanned! {ty.span()=>
                fn #assert_name<T: ::neutron::handler::IntoResponse>() {}
                let _ = #assert_name::<#ty>;
            }
        }
    };

    let fn_name = &func.sig.ident;
    let check_fn = syn::Ident::new(
        &format!("__neutron_debug_handler_check_{fn_name}"),
        fn_name.span(),
    );

    quote! {
        #func

        #[doc(hidden)]
        #[allow(non_snake_case, dead_code, unused_variables)]
        fn #check_fn() {
            #(#arg_asserts)*
            #ret_assert
        }
    }
    .into()
}
