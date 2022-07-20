use cc;
use std::path::PathBuf;

fn push_src_include(builder: &mut cc::Build, dir: &str) {
    // Convert, e.g., '-I../flow/include' into an absolute directory in a portable way.
    let mut path = std::env::current_dir().unwrap();
    path.pop();
    path.push(dir);
    path.push("include");
    let flag = format!("-I{}", path.to_str().unwrap());
    builder.flag_if_supported(&flag);
}

fn push_build_include(builder: &mut cc::Build, dir: &str) {
    // and for "-I/root/build_output/flow/include"
    let path = std::env::var("CARGO_TARGET_DIR").unwrap();
    let mut path: PathBuf = path.parse().unwrap();
    path.pop();
    path.push(dir);
    path.push("include");
    let flag = format!("-I{}", path.to_str().unwrap());
    builder.flag_if_supported(&flag);
}

fn main() {
    let envs: Vec<(String, String)> = std::env::vars().collect();
    println!("{:?}", envs);

    let mut builder = cxx_build::bridge("src/lib.rs");
    builder
        .flag_if_supported("-std=c++17")
        .flag_if_supported("-fPIC");

    for dir in [
        "fdbbackup",
        "fdbclient",
        "fdbcli",
        "fdbrpc",
        "fdbserver",
        "flowbench",
        "flow",
    ] {
        push_src_include(&mut builder, dir);
        push_build_include(&mut builder, dir);
    }
    for dir in ["boost_install", "jemalloc"] {
        push_build_include(&mut builder, dir);
    }

    let _build = builder.compile("fdb_rust");

    println!("cargo:rerun-if-changed=src/lib.rs");
}
