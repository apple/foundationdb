use cc;
use std::path::PathBuf;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn push_src_include(builder: &mut cc::Build, dir: &str) -> Result<()> {
    // Convert, e.g., '-I../flow/include' into an absolute directory in a portable way.
    let mut path = std::env::current_dir()?;
    path.pop();
    path.push(dir);
    path.push("include");
    let flag = format!("-I{}", path.to_str().unwrap());
    builder.flag_if_supported(&flag);
    Ok(())
}

fn push_build_include(builder: &mut cc::Build, dir: &str) -> Result<()> {
    // and for "-I/root/build_output/flow/include"
    let path = std::env::var("CARGO_TARGET_DIR")?;
    let mut path: PathBuf = path.parse()?;
    path.pop();
    path.push(dir);
    path.push("include");
    let flag = format!("-I{}", path.to_str().unwrap());
    builder.flag_if_supported(&flag);
    Ok(())
}

fn build_package(pkg: &str, deps: &[&str]) -> Result<()> {
    let mut builder = cxx_build::bridge(format!("src/{}.rs", pkg));
    builder
        .extra_warnings(false)
        .warnings(false)
        .flag_if_supported("-std=c++17")
        // .flag_if_supported("-Wno-unknown-pragmas")
        // .flag_if_supported("-Wno-comment")
        // .flag_if_supported("-Wno-unused-parameter")
        // .flag_if_supported("-Wno-attributes")
        // .flag_if_supported("-Wno-sign-compare")
        // .flag_if_supported("-Wno-delete-non-virtual-dtor")
        .flag_if_supported("-fPIC");

    push_src_include(&mut builder, pkg)?;
    push_build_include(&mut builder, pkg)?;

    for dir in deps {
        push_src_include(&mut builder, dir)?;
        push_build_include(&mut builder, dir)?;
    }
    for dir in ["boost_install", "jemalloc"] {
        push_build_include(&mut builder, dir)?;
    }

    let _build = builder.compile(&format!("{}_rust", pkg));
    println!("cargo:rerun-if-changed=src/{}.rs", pkg);
    Ok(())
}

fn main() -> Result<()> {
    let envs: Vec<(String, String)> = std::env::vars().collect();
    println!("{:?}", envs);
    // TODO: The dependency lists here are from trial and error.  Ideally, they'd be derived from
    // the same source of truth as cmake uses.
    build_package("flow", &[])?;
    build_package("fdbclient", &["flow", "contrib/fmt-8.1.1"])?;
    build_package("fdbserver", &["fdbrpc", "fdbclient", "flow", "contrib/fmt-8.1.1"])?;
    println!("cargo:rerun-if-changed=src/lib.rs");
    Ok(())
}
