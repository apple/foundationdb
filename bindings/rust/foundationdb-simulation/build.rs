use std::{env, path::PathBuf};

fn main() {
    println!("cargo:rustc-check-cfg=cfg(coverage)");
    println!("cargo:rustc-check-cfg=cfg(fdb_simulation_loom)");
    println!("cargo:rerun-if-env-changed=FDB_INCLUDE_DIR");

    let include_dir = env::var_os("FDB_INCLUDE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("../../c/foundationdb")
        });
    let header = include_dir.join("CWorkload.h");
    println!("cargo:rerun-if-changed={}", header.display());
    let bindings = bindgen::Builder::default()
        .header(header.to_str().expect("UTF-8 workload header path"))
        .generate()
        .expect("generate workload bindings");

    let out_path = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("write workload bindings");
}
