use flatc_rust;

use std::path::Path;

fn main() {
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
}
