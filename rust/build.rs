use clap::CommandFactory;
use clap_complete::{generate_to, shells::Bash};
use flatc_rust;

use std::path::Path;

include!("src/fdb/cli.rs");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=flatbuffers/*.fbs");

    let paths = std::fs::read_dir("flatbuffers/").unwrap();
    // let mut path_vec = Vec::<&Path>::new();

    // let paths = paths.map(|path| -> Path { Path::new("") }).collect();
    let mut path_vec: Vec<String> = Vec::new();
    let mut paths_vec: Vec<&Path> = Vec::new();
    for path in paths {
        path_vec.push(path.unwrap().path().display().to_string());
    }
    for path in &path_vec {
        paths_vec.push(Path::new(path));
    }
    flatc_rust::run(flatc_rust::Args {
        inputs: &paths_vec[..],
        out_dir: Path::new("target/flatbuffers/"),
        ..Default::default()
    })
    .expect("flatc");

    cxx_build::bridge("src/lib.rs")
        .file("src/ffi.cc")
        .warnings(false)
        .extra_warnings(false)
        // .file("src/blobstore.cc")
        // .flag("-w")
        .flag("-std=c++2a")
        // .flag("-Wno-unused-parameter")
        .flag("-Wno-attributes")
        .flag("-L/root/build_output/lib")
        .flag("-lflow")
        // .flag("/root/build_output/lib/libflow.a")
        .flag("-D").flag("FLOW_LOADBALANCE_ACTOR_G_H")
        .flag("-iquote").flag("/root/src/foundationdb")
        .flag("-iquote").flag("/root/build_output")
        .flag("-I").flag("/root/src/foundationdb")
        .flag("-I").flag("/opt/boost_1_78_0/include")
        .compile("cxx-demo");

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/ffi.cc");
    println!("cargo:rerun-if-changed=fdbclient");
    println!("cargo:rustc-link-search=/root/build_output/lib/");
    println!("cargo:rustc-link-lib=flow");
    match std::env::var_os("BASH_COMPLETION_DIR") {
        Some(out) => {
            // let mut cmd = build_cli();
            let mut cmd = Cli::command();
            let path = generate_to(Bash, &mut cmd, "fdb", out)?;
            println!("cargo:warning=completion file is generated: {:?}", path);
        }
        None => (),
    };

    Ok(())
}
