# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-06-24

### <!-- 1 -->🚀 Features

- Feat(simulation): auto-capitalize trace detail names
- Feat(simulation): WorkloadContext::delay takes Duration, exercised in noop
- Feat: migrate workspace to Rust 2024 edition (#255)

### <!-- 2 -->🐛 Bug Fixes

- Fix(simulation): static storage for C-ABI vtables, target fdbserver 7.4.6

### <!-- 9 -->📦 Other Changes

- Rewrite fdb_rt to split wakeup and polling phase

[0.3.0]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/0.2.3..0.3.0


## [0.2.3] - 2025-10-24

### <!-- 1 -->🚀 Features

- Add support 7.4
- Add 7.4 simulation in CI

### <!-- 2 -->🐛 Bug Fixes

- Remove unwrap on checks
- Recover simulation trace public api breaking change
- Fix(foundationdb-simulation): add docs.rs metadata to fix build

### <!-- 4 -->🚜 Refactor

- Remove unnecessary simulation headers

### <!-- 8 -->⚙️ Miscellaneous

- Clippy fixes

### <!-- 9 -->📦 Other Changes

- Update CI and bindingtester to 7.4
- Some spaces and tabs doc formalization
- Update workload bindings
- Update README
- Async RustWorkload
- Tidy up foundationdb-simulation API:

[0.2.3]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/0.2.2..0.2.3


## [0.2.2] - 2025-01-06

### <!-- 0 -->🚀 added

- Add unit for now()

### <!-- 3 -->🆙 Bump

- Bump cc from 1.1.30 to 1.2.1
- Bump cc from 1.1.24 to 1.1.30

[0.2.2]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/0.2.1..0.2.2

## [0.2.1] - 2024-10-08

### <!-- 1 -->🐛 Bug Fixes

- Fix foundationdb-simulation compilation with fdb-7_1

### <!-- 3 -->🆙 Bump

- Bump all dependencies and setup dependabot

### <!-- 4 -->⚙️ Other changes

- Use C++14 spec

[0.2.1]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/0.2.0..0.2.1

