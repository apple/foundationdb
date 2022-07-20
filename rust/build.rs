fn main() {
    let _build = cxx_build::bridge("src/lib.rs")
        .flag_if_supported("-std=c++17")
        .flag_if_supported("-fPIC")
        .flag_if_supported("-I/root/build_output/flow/include/")
        .compile("fdb_rust");

    println!("cargo:rerun-if-changed=src/lib.rs");
}