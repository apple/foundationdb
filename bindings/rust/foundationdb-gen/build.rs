use std::{env, fs, path::PathBuf};

#[cfg(all(not(feature = "embedded-fdb-include"), target_os = "linux"))]
const OPTIONS_FILE: &str = "/usr/include/foundationdb/fdb.options";

#[cfg(all(not(feature = "embedded-fdb-include"), target_os = "macos"))]
const OPTIONS_FILE: &str = "/usr/local/include/foundationdb/fdb.options";

#[cfg(all(not(feature = "embedded-fdb-include"), target_os = "windows"))]
const OPTIONS_FILE: &str = "C:/Program Files/foundationdb/include/foundationdb/fdb.options";

#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-5_1"))]
const OPTIONS_FILE: &str = "include/510/fdb.options";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-5_2"))]
const OPTIONS_FILE: &str = "include/520/fdb.options";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-6_0"))]
const OPTIONS_FILE: &str = "include/600/fdb.options";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-6_1"))]
const OPTIONS_FILE: &str = "include/610/fdb.options";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-6_2"))]
const OPTIONS_FILE: &str = "include/620/fdb.options";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-6_3"))]
const OPTIONS_FILE: &str = "include/630/fdb.options";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-7_0"))]
const OPTIONS_FILE: &str = "include/700/fdb.options";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-7_1"))]
const OPTIONS_FILE: &str = "include/710/fdb.options";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-7_3"))]
const OPTIONS_FILE: &str = "include/730/fdb.options";
#[cfg(all(feature = "embedded-fdb-include", feature = "fdb-7_4"))]
const OPTIONS_FILE: &str = "include/740/fdb.options";

// Compile error when no version feature is specified
#[cfg(not(any(
    feature = "fdb-5_1",
    feature = "fdb-5_2",
    feature = "fdb-6_0",
    feature = "fdb-6_1",
    feature = "fdb-6_2",
    feature = "fdb-6_3",
    feature = "fdb-7_0",
    feature = "fdb-7_1",
    feature = "fdb-7_3",
    feature = "fdb-7_4",
)))]
compile_error!(
    "foundationdb-gen requires a version feature to be specified.\n\
     \n\
     Available version features: fdb-5_1, fdb-5_2, fdb-6_0, fdb-6_1, fdb-6_2, fdb-6_3, fdb-7_0, fdb-7_1, fdb-7_3, fdb-7_4\n\
     \n\
     Examples:\n\
     - With embedded include: features = [\"embedded-fdb-include\", \"fdb-7_4\"]\n\
     - With system install: features = [\"fdb-7_4\"]"
);

fn main() {
    println!("cargo:rerun-if-env-changed=FDB_OPTIONS_FILE");
    println!("cargo:rerun-if-env-changed=FDB_INCLUDE_DIR");
    let options_file = env::var_os("FDB_OPTIONS_FILE")
        .map(PathBuf::from)
        .or_else(|| {
            env::var_os("FDB_INCLUDE_DIR").map(|path| PathBuf::from(path).join("fdb.options"))
        })
        .unwrap_or_else(|| PathBuf::from(OPTIONS_FILE));
    println!("cargo:rerun-if-changed={}", options_file.display());
    let out_path = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is undefined!"));
    fs::copy(&options_file, out_path.join("fdb.options"))
        .unwrap_or_else(|err| panic!("couldn't read {}: {err}", options_file.display()));
}
