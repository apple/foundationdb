extern crate foundationdb_gen;

use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;

fn main() {
    let out_path = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is undefined!"));
    let options_file = out_path.join("options.rs");
    let mut options = String::new();
    foundationdb_gen::emit(&mut options).expect("couldn't emit options.rs code!");

    File::create(options_file)
        .expect("couldn't create options.rs!")
        .write_all(options.as_bytes())
        .expect("couldn't write options.rs!");
}
