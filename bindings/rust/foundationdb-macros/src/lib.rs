//! Macro definitions used to maintain the crate
use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashMap;
use syn::__private::TokenStream2;
use syn::parse::Parser;
use syn::{Item, LitInt};
use try_map::FallibleMapExt;

/// Allow to compute the range of supported api versions for a functionality.
///
/// This macro came out from the frustration of bumping fdb's version, where
/// we are spending most of our time searching for things like:
/// `#[cfg(any(feature = "fdb-5_1", feature = "fdb-5_2", feature = "fdb-6_0"))]`
/// and adding the new version manually.
///
/// Thanks to the macro, we can now specify a `minimum` and an optional `max` version, and
/// generate the right list of any. Not specifying a `max` allow easy bump to a new version.
///
/// `#[cfg_api_versions(min = 510, max = 600)]` will be translated to:
/// `#[cfg(any(feature = "fdb-5_1", feature = "fdb-5_2", feature = "fdb-6_0"))]`
#[proc_macro_attribute]
pub fn cfg_api_versions(args: TokenStream, input: TokenStream) -> TokenStream {
    let args = proc_macro2::TokenStream::from(args);
    let input = proc_macro2::TokenStream::from(input);
    cfg_api_versions_impl(args, input).into()
}

fn cfg_api_versions_impl(
    args: proc_macro2::TokenStream,
    input: proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let mut min: Option<LitInt> = None;
    let mut max: Option<LitInt> = None;
    let version_parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("min") {
            min = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("max") {
            max = Some(meta.value()?.parse()?);
            Ok(())
        } else {
            Err(meta.error("unsupported cfg_api_versions property"))
        }
    });

    Parser::parse2(version_parser, args).expect("Unable to parse attribute cfg_api_versions");

    let input: Item = syn::parse2(input).expect("Unable to parse input");

    let minimum_version = min
        .expect("min property must be provided")
        .base10_parse::<i32>()
        .expect("Unable to parse min version");
    let maximum_version = max
        .try_map(|x| x.base10_parse::<i32>())
        .expect("Unable to parse max version");
    generate_feature_range(&input, minimum_version, maximum_version)
}

fn generate_feature_range(
    input: &Item,
    minimum_version: i32,
    maximum_version: Option<i32>,
) -> proc_macro2::TokenStream {
    let allowed_fdb_versions: Vec<TokenStream2> =
        get_supported_feature_range(minimum_version, maximum_version)
            .iter()
            .map(|fdb_version| quote!(feature = #fdb_version))
            .collect();

    quote!(
        #[cfg(any(#(#allowed_fdb_versions),*))]
        #input
    )
}

/// Given a range of version, this function will generate the appropriate macro text.
fn get_supported_feature_range(minimum_version: i32, maximum_version: Option<i32>) -> Vec<String> {
    let mut values: Vec<String> = get_version_mapping()
        .iter()
        .filter(|(_, version)| match maximum_version {
            None => minimum_version <= **version,
            Some(maximum) => minimum_version <= **version && version <= &&maximum,
        })
        .map(|(feature, _)| feature.to_owned())
        .collect();
    values.sort();

    values
}

// TODO: Should we import something like lazy_static?
fn get_version_mapping() -> HashMap<String, i32> {
    let mut version_mapping = HashMap::with_capacity(9);
    version_mapping.insert("fdb-7_4".into(), 740);
    version_mapping.insert("fdb-7_3".into(), 730);
    version_mapping.insert("fdb-7_1".into(), 710);
    version_mapping.insert("fdb-7_0".into(), 700);
    version_mapping.insert("fdb-6_3".into(), 630);
    version_mapping.insert("fdb-6_2".into(), 620);
    version_mapping.insert("fdb-6_1".into(), 610);
    version_mapping.insert("fdb-6_0".into(), 600);
    version_mapping.insert("fdb-5_2".into(), 520);
    version_mapping.insert("fdb-5_1".into(), 510);
    version_mapping.insert("fdb-5_0".into(), 500);
    version_mapping
}

#[cfg(test)]
mod tests {
    use crate::cfg_api_versions_impl;
    use crate::get_supported_feature_range;
    use proc_macro2::TokenStream;
    use quote::quote;

    #[test]
    fn test_create_supported_list() {
        let v = get_supported_feature_range(700, None);
        assert_eq!(v.len(), 4);
        assert!(v.contains(&String::from("fdb-7_0")));
        assert!(v.contains(&String::from("fdb-7_1")));
        assert!(v.contains(&String::from("fdb-7_3")));
        assert!(v.contains(&String::from("fdb-7_4")));

        let v = get_supported_feature_range(600, Some(700));
        assert_eq!(v.len(), 5);
        assert!(v.contains(&String::from("fdb-7_0")));
        assert!(v.contains(&String::from("fdb-6_3")));
        assert!(v.contains(&String::from("fdb-6_2")));
        assert!(v.contains(&String::from("fdb-6_1")));
        assert!(v.contains(&String::from("fdb-6_0")));

        let v = get_supported_feature_range(500, Some(610));
        assert_eq!(v.len(), 5);
        assert!(v.contains(&String::from("fdb-6_1")));
        assert!(v.contains(&String::from("fdb-6_0")));
        assert!(v.contains(&String::from("fdb-5_2")));
        assert!(v.contains(&String::from("fdb-5_1")));
        assert!(v.contains(&String::from("fdb-5_0")));

        let v = get_supported_feature_range(500, None);
        assert_eq!(v.len(), 11);
        assert!(v.contains(&String::from("fdb-7_4")));
        assert!(v.contains(&String::from("fdb-7_3")));
        assert!(v.contains(&String::from("fdb-7_1")));
        assert!(v.contains(&String::from("fdb-7_0")));
        assert!(v.contains(&String::from("fdb-6_3")));
        assert!(v.contains(&String::from("fdb-6_2")));
        assert!(v.contains(&String::from("fdb-6_1")));
        assert!(v.contains(&String::from("fdb-6_0")));
        assert!(v.contains(&String::from("fdb-5_2")));
        assert!(v.contains(&String::from("fdb-5_1")));
        assert!(v.contains(&String::from("fdb-5_0")));
    }

    fn test_cfg_versions(expected_versions: TokenStream, attrs: TokenStream) {
        let input = quote! {
            fn ma_fonction() {}
        };

        let expected = quote! {
            #[cfg(any(#expected_versions))]
            fn ma_fonction() {}
        };

        let result = cfg_api_versions_impl(attrs, input);
        assert_eq!(result.to_string(), expected.to_string())
    }

    #[test]
    fn test_min_700_no_max_version() {
        let data = quote!(
            feature = "fdb-7_0",
            feature = "fdb-7_1",
            feature = "fdb-7_3",
            feature = "fdb-7_4"
        );

        let attrs = quote!(min = 700);

        test_cfg_versions(data, attrs)
    }

    #[test]
    fn test_min_600_max_700() {
        let expected_versions = quote!(
            feature = "fdb-6_0",
            feature = "fdb-6_1",
            feature = "fdb-6_2",
            feature = "fdb-6_3",
            feature = "fdb-7_0"
        );

        let attrs = quote!(min = 600, max = 700);

        test_cfg_versions(expected_versions, attrs)
    }

    #[test]
    fn test_min_500_max_610() {
        let expected_versions = quote!(
            feature = "fdb-5_0",
            feature = "fdb-5_1",
            feature = "fdb-5_2",
            feature = "fdb-6_0",
            feature = "fdb-6_1"
        );

        let attrs = quote!(min = 500, max = 610);

        test_cfg_versions(expected_versions, attrs)
    }

    #[test]
    fn test_min_500_no_max() {
        let expected_versions = quote!(
            feature = "fdb-5_0",
            feature = "fdb-5_1",
            feature = "fdb-5_2",
            feature = "fdb-6_0",
            feature = "fdb-6_1",
            feature = "fdb-6_2",
            feature = "fdb-6_3",
            feature = "fdb-7_0",
            feature = "fdb-7_1",
            feature = "fdb-7_3",
            feature = "fdb-7_4"
        );

        let attrs = quote!(min = 500);

        test_cfg_versions(expected_versions, attrs)
    }

    #[test]
    #[should_panic]
    fn test_no_min_version() {
        let expected_versions = quote!(feature = "fdb-5_0",);

        let attrs = quote!(max = 500);

        test_cfg_versions(expected_versions, attrs)
    }
}
