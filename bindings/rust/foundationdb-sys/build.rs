extern crate bindgen;

use std::env;
use std::path::PathBuf;

#[cfg(all(not(feature = "embedded-fdb-include"), target_os = "linux"))]
const INCLUDE_PATH: &str = "/usr/include/foundationdb/";

#[cfg(all(not(feature = "embedded-fdb-include"), target_os = "macos"))]
const INCLUDE_PATH: &str = "/usr/local/include/foundationdb/";

#[cfg(all(not(feature = "embedded-fdb-include"), target_os = "windows"))]
const INCLUDE_PATH: &str = "C:/Program Files/foundationdb/include/foundationdb";

#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-5_1"))]
const INCLUDE_PATH: &str = "./include/510";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-5_2"))]
const INCLUDE_PATH: &str = "./include/520";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-6_0"))]
const INCLUDE_PATH: &str = "./include/600";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-6_1"))]
const INCLUDE_PATH: &str = "./include/610";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-6_2"))]
const INCLUDE_PATH: &str = "./include/620";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-6_3"))]
const INCLUDE_PATH: &str = "./include/630";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-7_0"))]
const INCLUDE_PATH: &str = "./include/700";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-7_1"))]
const INCLUDE_PATH: &str = "./include/710";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-7_3"))]
const INCLUDE_PATH: &str = "./include/730";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-7_4"))]
const INCLUDE_PATH: &str = "./include/740";

// Compile error when no version feature is specified
#[cfg(not(any(
    feature = "fdb-5_1",
    feature = "fdb-5_2",
    feature = "fdb-6_0",
    feature = "fdb-6_1",
    feature = "fdb-6_2",
    feature = "fdb-6_3",
    feature = "fdb-7_0",
    feature = "fdb-7_1",
    feature = "fdb-7_3",
    feature = "fdb-7_4",
)))]
compile_error!(
    "foundationdb-sys requires a version feature to be specified.\n\
     \n\
     Available version features: fdb-5_1, fdb-5_2, fdb-6_0, fdb-6_1, fdb-6_2, fdb-6_3, fdb-7_0, fdb-7_1, fdb-7_3, fdb-7_4\n\
     \n\
     Examples:\n\
     - With embedded include: features = [\"embedded-fdb-include\", \"fdb-7_4\"]\n\
     - With system install: features = [\"fdb-7_4\"]"
);

fn main() {
    // Link against fdb_c.
    println!("cargo:rustc-link-lib=fdb_c");

    println!("cargo:rerun-if-env-changed=FDB_CLIENT_LIB_PATH");
    if let Ok(lib_path) = env::var("FDB_CLIENT_LIB_PATH") {
        println!("cargo:rustc-link-search=native={lib_path}");
    }

    // Include the link directory for macOS.
    // Since Rust 1.91, /usr/local/lib is no longer searched automatically on macOS.
    // See: https://github.com/rust-lang/rust/pull/131477
    // See: https://github.com/rust-lang/rust/issues/147010
    #[cfg(target_os = "macos")]
    println!("cargo:rustc-link-search=/usr/local/lib");

    // Include the link directory for the .lib file on windows (which will resolve to
    // the shared library, at runtime)
    #[cfg(target_os = "windows")]
    println!("cargo:rustc-link-search=C:/Program Files/foundationdb/lib/foundationdb");

    let out_path = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is not defined!"));

    // We need to have FDB_API_VERSION set to a constant so that bindgen will
    // generate a const value for it. We could try to pass -DFDB_TRICKY_VERSION=510
    // to the driver and then '#define FDB_API_VERSION FDB_TRICKY_VERSION', but
    // bindgen isn't smart enough to resolve that from the arguments. Instead, write
    // out a src/wrapper.h file with the chosen version instead.
    let api_version;

    #[cfg(feature = "fdb-5_1")]
    {
        api_version = 510;
    }
    #[cfg(feature = "fdb-5_2")]
    {
        api_version = 520;
    }
    #[cfg(feature = "fdb-6_0")]
    {
        api_version = 600;
    }
    #[cfg(feature = "fdb-6_1")]
    {
        api_version = 610;
    }
    #[cfg(feature = "fdb-6_2")]
    {
        api_version = 620;
    }
    #[cfg(feature = "fdb-6_3")]
    {
        api_version = 630;
    }
    #[cfg(feature = "fdb-7_0")]
    {
        api_version = 700;
    }
    #[cfg(feature = "fdb-7_1")]
    {
        api_version = 710;
    }
    #[cfg(feature = "fdb-7_3")]
    {
        api_version = 730;
    }
    #[cfg(feature = "fdb-7_4")]
    {
        api_version = 740;
    }

    let bindings = bindgen::Builder::default()
        // TODO: there must be a way to get foundationdb from pkg-config...
        .clang_arg(format!("-I{INCLUDE_PATH}"))
        .clang_arg(format!("-D FDB_API_VERSION={api_version}"))
        .header(format!("{INCLUDE_PATH}/fdb_c.h"))
        .generate_comments(true)
        .layout_tests(false)
        .generate()
        .expect("Unable to generate FoundationDB bindings");
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
