use std::{env, path::PathBuf};

extern crate cc;

fn main() {
    println!("cargo:rustc-check-cfg=cfg(coverage)");

    let bindings = bindgen::Builder::default()
        .header("src/headers/CWorkload.h")
        .generate()
        .expect("generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("write bindings");

    let _api_version: usize;

    #[cfg(feature = "fdb-7_1")]
    {
        _api_version = 710;
    }
    #[cfg(feature = "fdb-7_3")]
    {
        _api_version = 730;
    }
    #[cfg(feature = "fdb-7_4")]
    {
        _api_version = 740;
    }

    #[cfg(feature = "cpp-abi")]
    {
        if cfg!(feature = "fdb-docker") {
            if _api_version < 730 {
                build_with_gcc(_api_version);
            } else {
                // FDB 7.3+ is built with clang, so we need to do the same, including the linker
                build_with_clang(_api_version);
            }
        } else {
            // The Cpp API is not FFI safe, not compiling in the exact same environment leads to undefined behavior
            display_build_warnings();
            build_with_gcc(_api_version);
        }
    }
}

#[allow(dead_code)]
fn display_build_warnings() {
    println!("cargo:warning=---------------------");
    println!("cargo:warning=------ Warning ------");
    println!("cargo:warning=---------------------");
    println!(
        "cargo:warning=Building the C++ bindings without the `fdb-docker` feature will be valid from Rust's perspective but not from C++ and FoundationDB"
    );
    println!("cargo:warning=Please follow the instructions in the associated README");
}

#[allow(dead_code)]
fn build_with_gcc(api_version: usize) {
    cc::Build::new()
        .cpp(true)
        .std("c++14")
        .define("FDB_API_VERSION", api_version.to_string().as_str())
        .file("src/CppWorkload.cpp")
        .compile("ctx");
}

#[allow(dead_code)]
fn build_with_clang(api_version: usize) {
    cc::Build::new()
        .compiler("clang")
        .cpp_set_stdlib("c++")
        .cpp(true)
        .std("c++14")
        .define("FDB_API_VERSION", api_version.to_string().as_str())
        .file("src/CppWorkload.cpp")
        .compile("ctx");
}
