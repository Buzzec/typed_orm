use convert_case::{Case, Casing};
use darling::{FromDeriveInput, FromField, util::Flag};
use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Data, DeriveInput, Fields, Ident, Type, parse_macro_input, parse_quote, spanned::Spanned,
};

fn get_crate_name() -> TokenStream2 {
    let generator_crate = crate_name("typed_orm").expect("Could not find `typed_orm`");
    match generator_crate {
        FoundCrate::Itself => quote! { typed_orm },
        FoundCrate::Name(name) => {
            let ident = format_ident!("{}", name);
            quote! { #ident }
        }
    }
}

#[derive(Debug, FromField)]
#[darling(attributes(table), forward_attrs(doc))]
struct TableFieldOpts {
    ident: Option<Ident>,
    ty: Type,
    primary_key: Flag,
    flatten: Flag,
}

#[derive(Debug, Default, FromDeriveInput)]
#[darling(attributes(table), supports(struct_named))]
struct TableOpts {
    table_name: Option<String>,
    name_separator: Option<String>,
    if_not_exists: Option<bool>,
    strict: Option<bool>,
    without_rowid: Option<bool>,
}

#[proc_macro_derive(TableData, attributes(table))]
pub fn derive_table_data(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_table_data(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_derive(Table, attributes(table))]
pub fn derive_table(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_table(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_table_data(input: DeriveInput) -> syn::Result<TokenStream2> {
    let crate_name = get_crate_name();

    let ident = input.ident;
    let mut generics = input.generics;
    let fields = parse_named_fields(&input.data)?;
    {
        let where_clause = generics.make_where_clause();
        for field in &fields {
            let field_ty = &field.ty;
            if field.flatten.is_present() {
                where_clause
                    .predicates
                    .push(parse_quote!(#field_ty: ::#crate_name::TableData));
            } else {
                where_clause
                    .predicates
                    .push(parse_quote!(for<'a> #field_ty: ::#crate_name::DataType<'a>));
            }
        }
    }

    let column_iters = fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_ty = &field.ty;
        let field_name = field_ident.to_string();

        if field.flatten.is_present() {
            quote! {
                <#field_ty as ::#crate_name::TableData>::columns(
                    Some(::#crate_name::util::format_column_name(
                        parent_name.as_ref().map(::std::borrow::Cow::as_ref),
                        #field_name,
                        name_separator,
                    )),
                    name_separator,
                )
            }
        } else {
            quote! {
                ::std::iter::once(<#field_ty as ::#crate_name::DataType>::sql_data_type(
                    ::#crate_name::util::format_column_name(
                        parent_name.as_ref().map(::std::borrow::Cow::as_ref),
                        #field_name,
                        name_separator,
                    ),
                ))
            }
        }
    });

    let from_row_fields = fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_ty = &field.ty;

        if field.flatten.is_present() {
            quote! {
                #field_ident: <#field_ty as ::#crate_name::TableData>::from_row_inner(row_offset, row)?
            }
        } else {
            quote! {
                #field_ident: <#field_ty as ::#crate_name::DataType>::from_sql_value(
                    row.get_ref(row_offset.next_idx())?
                )?
            }
        }
    });

    let sql_output_iters = fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_ty = &field.ty;

        if field.flatten.is_present() {
            quote! {
                <#field_ty as ::#crate_name::TableData>::sql_output_iter(&self.#field_ident)?
            }
        } else {
            quote! {
                ::std::iter::once(<#field_ty as ::#crate_name::DataType>::to_sql_value(&self.#field_ident)?.into())
            }
        }
    });

    let column_counts = fields.iter().map(|field| {
        let field_ty = &field.ty;

        if field.flatten.is_present() {
            quote! { <#field_ty as ::#crate_name::TableData>::column_count() }
        } else {
            quote! { 1usize }
        }
    });

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::#crate_name::TableData for #ident #ty_generics #where_clause {
            fn columns(
                parent_name: ::std::option::Option<::std::borrow::Cow<str>>,
                name_separator: &str,
            ) -> impl ::std::iter::Iterator<Item = ::#crate_name::sqlite::SQLiteColumn> {
                ::std::iter::empty()
                    #(.chain(#column_iters))*
            }

            fn from_row_inner(
                row_offset: &mut ::#crate_name::util::RowIdxAccumulator,
                row: &::#crate_name::rusqlite::Row<'_>,
            ) -> ::#crate_name::error::Result<Self> {
                Ok(Self {
                    #(#from_row_fields),*
                })
            }

            fn sql_output_iter(
                &self,
            ) -> ::#crate_name::error::Result<impl ::std::iter::Iterator<Item = ::#crate_name::rusqlite::types::ToSqlOutput<'_>> + '_> {
                Ok(::std::iter::empty()
                    #(.chain(#sql_output_iters))*)
            }

            fn column_count() -> usize {
                0usize #(+ #column_counts)*
            }
        }
    })
}

fn expand_table(input: DeriveInput) -> syn::Result<TokenStream2> {
    let crate_name = get_crate_name();

    let opts = TableOpts::from_derive_input(&input)?;
    let ident = input.ident;
    let mut generics = input.generics.clone();
    let fields = parse_named_fields(&input.data)?;
    {
        let where_clause = generics.make_where_clause();
        where_clause
            .predicates
            .push(parse_quote!(Self: ::#crate_name::TableData));
        for field in &fields {
            let field_ty = &field.ty;
            if field.flatten.is_present() {
                where_clause
                    .predicates
                    .push(parse_quote!(#field_ty: ::#crate_name::TableData));
            } else {
                where_clause
                    .predicates
                    .push(parse_quote!(for<'a> #field_ty: ::#crate_name::DataType<'a>));
            }
        }
    }
    let primary_key_fields = fields
        .iter()
        .filter(|field| field.primary_key.is_present())
        .collect::<Vec<_>>();

    if primary_key_fields.is_empty() {
        return Err(syn::Error::new(
            ident.span(),
            "Table derive requires at least one #[table(primary_key)] field",
        ));
    }
    if primary_key_fields
        .iter()
        .any(|field| field.flatten.is_present())
    {
        return Err(syn::Error::new(
            ident.span(),
            "primary key fields cannot be #[table(flatten)]",
        ));
    }

    let table_name_base = opts
        .table_name
        .unwrap_or_else(|| ident.to_string().to_case(Case::Snake));
    let name_separator = opts.name_separator.unwrap_or_else(|| "__".to_string());
    let if_not_exists = opts.if_not_exists.unwrap_or(true);
    let strict = opts.strict.unwrap_or(true);
    let without_rowid = opts.without_rowid.unwrap_or(false);

    let pk_ref_ident = format_ident!("{ident}PrimaryKey");
    let pk_owned_ident = format_ident!("{ident}PrimaryKeyOwned");

    let pk_ref_fields = primary_key_fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_ty = &field.ty;
        quote! { pub #field_ident: &'a #field_ty }
    });
    let pk_owned_fields = primary_key_fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field");
        let field_ty = &field.ty;
        quote! { pub #field_ident: #field_ty }
    });
    let pk_ref_init = primary_key_fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field");
        quote! { #field_ident: &self.#field_ident }
    });
    let pk_clone_fields = primary_key_fields.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field");
        quote! { #field_ident: self.#field_ident.clone() }
    });
    let pk_clone_bounds = primary_key_fields.iter().map(|field| {
        let field_ty = &field.ty;
        quote! { #field_ty: ::std::clone::Clone }
    });

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        pub struct #pk_ref_ident<'a> {
            #(#pk_ref_fields),*
        }

        pub struct #pk_owned_ident {
            #(#pk_owned_fields),*
        }

        impl<'a> ::#crate_name::util::ReferenceStruct for #pk_ref_ident<'a>
        where
            #(#pk_clone_bounds),*
        {
            type Owned = #pk_owned_ident;

            fn clone_to_owned(&self) -> Self::Owned {
                #pk_owned_ident {
                    #(#pk_clone_fields),*
                }
            }
        }

        impl #impl_generics ::#crate_name::Table for #ident #ty_generics #where_clause {
            type PrimaryKey<'a> = #pk_ref_ident<'a>
            where
                Self: 'a;

            fn table_name() -> ::std::borrow::Cow<'static, str> {
                let generic_suffix = ::#crate_name::table_name::generic_suffix::<Self>();
                if generic_suffix.is_empty() {
                    ::std::borrow::Cow::Borrowed(#table_name_base)
                } else {
                    ::std::borrow::Cow::Owned(format!("{}__{}", #table_name_base, generic_suffix))
                }
            }

            fn primary_key(&self) -> Self::PrimaryKey<'_> {
                #pk_ref_ident {
                    #(#pk_ref_init),*
                }
            }

            fn from_row(row: &::#crate_name::rusqlite::Row<'_>) -> ::#crate_name::error::Result<Self> {
                <Self as ::#crate_name::TableData>::from_row_inner(
                    &mut ::#crate_name::util::RowIdxAccumulator::default(),
                    row,
                )
            }

            fn to_params(&self) -> ::#crate_name::error::Result<impl ::#crate_name::rusqlite::Params> {
                Ok(::#crate_name::rusqlite::params_from_iter(<Self as ::#crate_name::TableData>::sql_output_iter(self)?))
            }

            fn create_table_stmt() -> impl ::#crate_name::SqlStatement {
                ::#crate_name::sqlite::SQLiteCreateTableStmt {
                    table_name: Self::table_name(),
                    columns: Self::columns(None, #name_separator),
                    if_not_exists: #if_not_exists,
                    strict: #strict,
                    without_rowid: #without_rowid,
                }
            }
        }
    })
}

fn parse_named_fields(data: &Data) -> syn::Result<Vec<TableFieldOpts>> {
    let Data::Struct(data_struct) = data else {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "only structs are supported",
        ));
    };
    let Fields::Named(fields) = &data_struct.fields else {
        return Err(syn::Error::new(
            data_struct.fields.span(),
            "only named-field structs are supported",
        ));
    };

    fields
        .named
        .iter()
        .map(|field| TableFieldOpts::from_field(field).map_err(|err| err.into()))
        .collect()
}
