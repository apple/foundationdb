[![Discord](https://img.shields.io/discord/925467557903884349)](https://discord.gg/zkgtbtFfWY)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/foundationdb-rs/foundationdb-rs/ci.yml?branch=main)](https://github.com/foundationdb-rs/foundationdb-rs/actions)
[![dependency status](https://deps.rs/repo/github/foundationdb-rs/foundationdb-rs/status.svg)](https://deps.rs/repo/github/foundationdb-rs/foundationdb-rs)
[![Codecov](https://img.shields.io/codecov/c/github/foundationdb-rs/foundationdb-rs)](https://codecov.io/gh/foundationdb-rs/foundationdb-rs)
![Rustc 1.85.1+](https://img.shields.io/badge/rustc-1.85.1+-lightgrey)

# FoundationDB Rust Client

The repo consists of multiple crates:

| Library                                            | Status                                                                                                                                                                                                          | Description                                                 |
|----------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------|
| [**foundationdb**](foundationdb/README.md)         | [![Crates.io](https://img.shields.io/crates/v/foundationdb)](https://crates.io/crates/foundationdb) [![foundationdb](https://docs.rs/foundationdb/badge.svg)](https://docs.rs/foundationdb)                     | High level FoundationDB client API                          |
| [**foundationdb-sys**](foundationdb-sys/README.md) | [![Crates.io](https://img.shields.io/crates/v/foundationdb-sys)](https://crates.io/crates/foundationdb-sys) [![foundationdb-sys](https://docs.rs/foundationdb-sys/badge.svg)](https://docs.rs/foundationdb-sys) | C API bindings for FoundationDB                             |
| **foundationdb-gen**                               | n/a                                                                                                                                                                                                             | Code generator for common options and types of FoundationDB |

The current version requires rustc 1.85.1+ to work.
The previous version (0.3) is still maintained and is available within the 0.3 branch.

You can access the `main` branch documentation [here](https://foundationdb-rs.github.io/foundationdb-rs/foundationdb/index.html).

## Supported platforms

Supported platforms are listed on the [foundationdb's README](foundationdb/README.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

## Correctness

Special care has been set up to be sure that the crate is correct, like official bindings. Every hour, we are running thousands of seeds on the [BindingTester](https://github.com/apple/foundationdb/blob/master/bindings/bindingtester/spec/bindingApiTester.md).

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
