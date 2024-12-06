use std::{env, path::PathBuf};

fn main() {
    let c_workload_h = "../../../foundationdb/CWorkload.h";
    println!("cargo:rerun-if-changed={c_workload_h}");

    let bindings = bindgen::Builder::default()
        .header(c_workload_h)
        .generate()
        .expect("generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect(" write bindings");
}
