# RustWorkload

This repository showcases an example workload written in Rust, designed to
interact with the FoundationDB simulation environment via the C API. The
workload provided here is not integrated into the CMake build system and serves
purely as a reference implementation.

## Building the Workload

You can build this crate in any standard Rust environment using the following command:

```shell
cargo build --release
```

This will generate a shared library that can be used as an `ExternalWorkload`
within the FoundationDB simulation. To inject this library into the simulation,
use the command:

```shell
fdbserver -r simulation -f test_file.toml
```

Its behavior should be equivalent to the `CWorkload.c` test workload.

## Limitations and Further Examples

This example workload does not include functionality for interacting with the
database. For examples with working database bindings, refer to the
[foundationdb-rs](https://github.com/foundationdb-rs/foundationdb-rs/tree/main/foundationdb-simulation)
project.
