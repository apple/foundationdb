fn main() {
    let _build = cxx_build::bridge("src/lib.rs")
        .flag_if_supported("-std=c++17")
        .flag_if_supported("-fPIC")
        .compile("fdb_rust");

    println!("cargo:rerun-if-changed=src/lib.rs");
}