# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.11.0] - 2026-06-24

### <!-- 1 -->🚀 Features

- Feat: first draft of leader election recipe implementation
- Use Deref<Target=Transaction>
- Feat(leader_election): implement O(1) ballot-based Active Disk Paxos algorithm
- Feat(recipes): add ranked register recipe
- Feat: add transaction conflict reporting and unified retry runner
- Feat: migrate workspace to Rust 2024 edition (#255)

### <!-- 2 -->🐛 Bug Fixes

- Fix(directory): use get_ranges_keyvalues for proper pagination
- Fix: replace version-based timeouts with Duration-based approach
- Fix(simulation): simplify leader election invariants to core algorithm properties
- Fix(leader-election): internalize versionstamp to prevent unregistered leadership claims
- Fix(simulation): use microsecond precision for log entry ordering
- Use get_ranges_keyvalues for complete range reads (fixes #72)

### <!-- 4 -->🚜 Refactor

- Refactor(simulation): remove redundant invariants and unused types

### <!-- 5 -->📚 Documentation

- Docs(leader-election): add simulation testing documentation
- Docs(leader-election): improve module and struct documentation

### <!-- 8 -->⚙️ Miscellaneous

- Format code and update invariants
- Clarify infinite-retry behavior and add timeout/retry safeguards (fixes
- Chore: align nix toolchain with CI stable (Rust 1.96) and fix 1.96 clippy lints

### <!-- 9 -->📦 Other Changes

- Rewrite fdb_rt to split wakeup and polling phase
- Review comments

[0.11.0]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/0.10.0..0.11.0


## [0.10.0] - 2025-10-24

### <!-- 1 -->🚀 Features

- Feat(foundationdb) : Expose Timekeeper API
- Profiling Keyspace Scanner
- Add support 7.4
- Impl std::error::Error for TransactionCommitError
- Add optional tracing feature
- Add `intrumented_run`
- Add unsafe to new_from_pointer function

### <!-- 2 -->🐛 Bug Fixes

- Fix(foundationdb) : Replace unmaintained ring crate with sha2 in examples
- Add  on fields in MetricKey
- Don't log as error all commit errors
- Fix: resolve clippy warnings and add doc alias

### <!-- 5 -->📚 Documentation

- Doc, multi-statements support
- Doc-related clippy fixes

### <!-- 6 -->🧪 Testing

- Add a test on failed closure

### <!-- 8 -->⚙️ Miscellaneous

- Bump rand to 0.9.0
- Bump MSRV to 1.82
- Test on Apple silicon
- Clippy --fix
- Fix warnings
- Bump msrv to 1.85

### <!-- 9 -->📦 Other Changes

- Use cfg_api_versions and if_cfg_api_versions
- Specifically use C ABI for packed structs
- Some spaces and tabs doc formalization
- Rename instrumentation by trace

[0.10.0]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/0.9.2..0.10.0


## [0.9.2] - 2025-01-06

### <!-- 0 -->🚀 added

- Add multi_version compatibility

### <!-- 1 -->🐛 Bug Fixes

- Test get_metadata_version on 7.3

### <!-- 3 -->🆙 Bump

- Bump async-trait from 0.1.83 to 0.1.84
- Bump tokio from 1.40.0 to 1.41.0
- Bump uuid from 1.10.0 to 1.11.0
- Bump futures from 0.3.30 to 0.3.31

### <!-- 4 -->⚙️ Other changes

- Merge pull request [#211](https://github.com/foundationdb-rs/foundationdb-rs/pull/211) from foundationdb-rs/dependabot/cargo/serde-1.0.217
- Merge pull request [#190](https://github.com/foundationdb-rs/foundationdb-rs/pull/190) from spiraldb/ji/split-tuple-into-crate
- Merge pull request [#173](https://github.com/foundationdb-rs/foundationdb-rs/pull/173) from foundationdb-rs/dependabot/cargo/serde_json-1.0.132
- Apply review changes
- Improve libfdb transaction error

[0.9.2]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/0.9.1..0.9.2

## [0.9.1] - 2024-10-08

### <!-- 0 -->🚀 added

- Add examples for using versionstamps
- Add Database.get_main_thread_busyness

### <!-- 3 -->🆙 Bump

- Bump MSRV to 1.71.1
- Bump all dependencies and setup dependabot

### <!-- 4 -->⚙️ Other changes

- Generate CHANGELOG per crate
- Handle nested FdbBindingErrors
- Expose FdbBindingError::get_fdb_error

[0.9.1]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/0.9.0..0.9.1

## [0.9.0] - 2024-03-15

### <!-- 0 -->🚀 added

- Add more useful version of get_mapped_ranges
- Add perform_no_op method
- Introduce db_run
- Introduce FdbBindingError
- Add hello_world using db.run
- Use cargo add in doc
- Remove panic during RetryableTransaction::commit
- Feat(foundationdb): rework RetryableTransaction
- Feat(foundationdb): add raw method to create a Database
- Initial commit for Tenant
- Implement Tenant{Create,SetActive,Delete}
- Implement list_tenant
- Get TenantManagement::get_tenant
- Fix `list_range` and add tenant::run
- Feat(tuple) add function to pack a subspace key with a versionstamp
- Add integ tests for tuple subspaces with versionstamps
- Add support for 7.3
- Add more feature flag
- Add Database.get_client_status
- Add support for tenant info in 7.3
- Add MicroQueue examples
- Bump fdb crate to 0.9.0 and sim to 0.2.0

### <!-- 1 -->🐛 Bug Fixes

- Clippy warnings
- Run tenants tests when feature is used
- Avoid infinite recursion on debug impl
- Fix: Mark Fdb struct wrappers as packed
- Use Debug impl as Display
- Make the db_run non capturing
- Apply cargo fmt recommendation
- Fix std::slice::from_raw_parts behavior on nightly
- Fix(foundationdb): remove flappy tests

### <!-- 2 -->🚜 Refactor

- Remove unused StreamingMode
- Remove unnecessary casts.

### <!-- 3 -->🆙 Bump

- Chore: bump rust to 1.57
- Bump msrv
- Bump crates version
- Bump msrv for bindgen and env_logger
- Bump dependencies
- Bump dep

### <!-- 4 -->⚙️ Other changes

- Build on docs.rs with the required features
- Reformat table
- Clippy
- Rename examples
- Hide tenant api behind a feature
- Introduce a pass-through transaction option setter
- Cargo {fmt,clippy}
- Prepare release
- Clippy: Simplify non-minimal-cfg.
- Clippy: Suppress "should_implement_trait" for `Database::default()`.
- Cargo fmt
- Removing default fdb version
- Cargo fmt/clippy
- Replace get(0) by first()
- Return errors instead of expect
- Use higher-level get_ranges
- Create MicroQueue struct
- Simplify first_item method and prevent extra allocations
- Use local variable instead of struct ffield for random seed
- Default Unit value for FdbResult

## [0.7.0] - 2022-07-11

### <!-- 0 -->🚀 added

- Add micro index example
- Add tested example to explain how to use fdb to store blob data.
- Add missing async keyword on simple-index example
- Add explanation about blob example
- Add a simple blob example
- Add support for api 700
- Initial commit for get_range_split_points
- Implement Iterator for FdbKeys
- FdbAddress should be #[repr(transparent)]
- Expose FdbKey and other only in 7.0
- Implement range split point
- Introduce get/set metatadaVersion
- Introduce run method
- Add Hash, Eq and PartialEq to Subspace
- Add example about atomic_op
- Initial commit to support fdb 7.1
- Initial commit for get_mapped_range
- Allow retrieval of embedded kvs in mapped_range
- Allow KeySelector in mapped_range
- Improve doc about mapped_range

### <!-- 2 -->🚜 Refactor

- Remove clikengo link in Cargo.toml
- Use macro cfg_api_versions when possible

### <!-- 3 -->🆙 Bump

- Bump dependencies
- Bump deps
- Bump rust patch version

### <!-- 4 -->⚙️ Other changes

- Improve blob example display
- Use tokio reactor in examples
- Improve blob example with a manifest system
- Reduce unsafe scope
- Limit FdbKeys and FdbKey to fdb700+
- Reduce use of `cfg_api_versions`
- Clippy
- Revert "feat(foundationdb): introduce run method"
- Split modules in separate files
- Move test mapped_range into range's tests
- Improve documentation
- Prepare for 0.7.0

## [0.6.0] - 2022-04-08

### <!-- 0 -->🚀 added

- Add support for api 630
- Introduce platform tier
- Feat(foundationdb,bindingtester): Add directory
- Add warning on create
- Fix compilation with 1.46.0 and add back install on script
- Bump crate version, maintainers and documentation
- Bump rust edition to 2021

### <!-- 1 -->🐛 Bug Fixes

- Fix doc artifacts of removed methods
- Fix: Bump minimum version to rust 1.46
- Cargo clippy --fix and fmt
- Run estimate range tests only on 630
- Fix several panics during some seeds
- Fix clippy warnings
- Typo
- Fix(directory): avoid throwing bad CannotMoveBetweenPartition
- Fix code in documentation
- Fix(directory): avoid initializing the version key on create_or_open
- Fix(directory): Scan old and new path during Directory.Move

### <!-- 2 -->🚜 Refactor

- Accept a slice instead of a Vec as path
- Refactor(directory): Node are always loaded and existing

### <!-- 3 -->🆙 Bump

- Bump version to 5.0.1
- Bump deps

### <!-- 4 -->⚙️ Other changes

- Cargo fmt
- Make tuple::hca::HighContentionAllocator be Send (#214)
- Removing reference to master branch
- Code-review
- Chore: code-review
- Rollback breaking change on Subspace::from_bytes
- Chore(directory): cleanup tuple looking syntax
- Clippy

## [0.5.0] - 2020-09-17

### <!-- 0 -->🚀 added

- Add versionstamp support to tuple
- Add docs and description of algorithm
- Get_addresses_for_key test
- Add NEGINSTART and POSINTEND to tuple
- Add boot_async method
- Add support for pack with versionstamp
- Simplify add_assign logic for VerstionstampOffset
- Add transact limits test and make the code easier to read
- Add tests for boot_async

### <!-- 1 -->🐛 Bug Fixes

- Fixed flaky networking test
- Fix variable does not need to be mutable in bindingtester
- Fix integer cast possible undefined behavior
- Fix missing rename for fdb_api to api
- Fix default feature compilation
- Fix Element::tuple deserialization
- Fix build on linux/osx
- Clippy fixes
- Fix tests
- Fix build after naming changes
- Fix build without uuid feature
- Fix bindingtester scripted seed 674776253
- Fixed i64 encoding index out of range error
- Fix panic on i64
- Fix #136: Segfault when pending future dropped
- Fix foundationdb-gen features
- Cargo.toml fix docs.rs features
- Fix copyright link
- Cargo.toml fix badges links
- Fix spurious test_future_discard panic
- Fix DoubleEndedIterator for FdbValuesIter
- Fix Cargo.toml repository links
- Fixes from review
- Fix #170: protect boot from undefined behavior
- Fix #181: boot can still trigger undefined behavior if `f` panic and does not abort.
- Fix some cargo clippy issues
- Fix use after free in Database::new
- Fix doc in KeySelector first_greater_than*
- Fix bigint support
- Fix bigint support on element
- Fix Element order between BigInt and Int
- Fix versionstamp offset and bindingtester
- Fix unpack of ix that doesn't fix in ix
- Fix build for rust < 1.42
- Fix broken transact() limits
- Fix cargo clippy warning
- Fix missing tokio features for tokio_async test
- Simplify and fix UB in boot API
- Fix even `run` can lead to UB
- Fix api_ub test on API <610

### <!-- 2 -->🚜 Refactor

- Remove unnecessary cast in unsafe blocks
- Remove is_maybe_committed as it is already accessible
- Remove failure dependency
- Remove useless import
- Bindingtester refactoring
- Remove dbg!

### <!-- 3 -->🆙 Bump

- Bump version to 0.4.1

### <!-- 4 -->⚙️ Other changes

- Core: auto-generate all code at build time
- Upgrade rand from 0.4 to 0.6 (#104)
- Upgrade uuid from 0.6 to 0.7 (#105)
- Cleanup the fdb version features from primary crate (#99)
- Rust edition 2018
- Async/await: complete rewrite of foundationdb
- New get_ranges_key_values -> Stream<KeyValue> api on Transaction
- Renamed foundationdb_sys import from fdb to fdb_sys for clarity
- Rename fdb_api to api
- Foundationdb api 510, 520, 600 support
- Working threaded bindingtester
- Bindingtester is now complete and serde is replaced by a solution close to the original
- Make RangeOption::next_range public
- Fdb 620 support
- Futures 0.3 is stable now
- Cleanup dependencies and rename Error as FdbError
- Documentation and API improvements
- Transaction documentation and some API simplifications
- Impl Send/Sync for some future results
- Make FdbFuture and FdbFutureHandle private
- Rename tuple::Error and tuple::Result to PackError and PackResult
- Hca implementation and tests
- Foundationdb-sys version is now 0.4.0
- Returned Future/Stream are now publicly Unpin
- Restore abort future test
- :transact data is now passed as mutable
- Changed back to i64
- Handle when encoded value overflows
- Hca outline
- Lib updates + tweaks to hca
- Consider each key via fold
- Bahgawd it works
- Working tests
- Tweaks to doc formatting
- Update rand requirement from 0.6.5 to 0.7.0
- Mark FdbFuture as Send + Sync
- Make comment more accurate
- Update README links and requirements
- Force foundationdb-gen to version 0.4.0+
- Update Cargo.toml authors
- :transact api is now generic and should be future proof.
- RangeOption can now be constructed from (Vec<u8>, Vec<u8>)
- RangeOption tests & from RangeInclusive
- Embed 6.2.10 fdb_c.h and fdb.options
- Embed all supported version includes
- Default to locally available foundationdb include
- Test_set_conflict now checks the error message
- Cargo.toml badges & docs.rs metadata
- Impl std::error::Error for FdbError
- Bigint tuple suport enhancements
- Disable tokio test until #170 is resolved
- Renamed BadLength to UnsupportedIntLength
- Pack/unpack optional bigint support
- Limit risks of transmute errors of fdb_sys::FDBKeyValue to FdbKeyValue
- Impl std::error::Error for PackError
- Use `#[non_exhaustive]` on RangeOption and generated enums
- Typo in documentation
- Few api improvements:
- Default to foundation 620 api
- Cargo fmt
- Ignore test_ub test due to panic hook issue in the code coverage CI test
- Update README and CHANGELOG

## [0.3.0] - 2018-11-16

### <!-- 0 -->🚀 added

- Add docs link to Cargo.toml
- Add `futures` dependency to example.
- Add futures to docs
- `GetKeyResult` and `GetAddressResult` return value

### <!-- 1 -->🐛 Bug Fixes

- Fix imports in example

### <!-- 4 -->⚙️ Other changes

- Win64 support
- Change `TrxGet::value` to return `Option<&[u8]>`
- Prepare 0.3 release

## [0.2] - 2018-05-09

### <!-- 0 -->🚀 added

- Add some READMEs
- Add Readme for the fdb crate
- Add metadata
- Add FdbError and conversion helpers
- Add first working example
- Add `example_get_multi`
- Add comments
- Add set_option() api to Database/Transaction
- Add run and stop
- Add network set_option
- Add init functions
- Add library level documentation and examples
- Add Apple copyright due to most of the docs from FoundationDB docs
- Add docs for options
- Cancel/committed_version/get_address_for_key
- Add `Transaction::watch`, #19
- Add `Transaction::atomic_op`, #15
- Add `Transaction::get_range`, #14
- Add testcases with conflict scenario
- Add `Database::transact`, #29
- Add docs about retrylimit/timeout, fix watch
- Add floating-point support
- Add `bindingtester`, #39
- Add nested tuple/single value
- Add `Transaction::add_conflict_range`, #20
- Add Subspace, #36
- Add benchmark
- Add support for threading/batching
- Add into FdbError for tuple::Error
- Add some tests to validate recursive tuples
- Add some debug lines
- Add decode/encode for Option
- Add single tuple support back

### <!-- 1 -->🐛 Bug Fixes

- Fix version
- Fix some urls
- Fix doc tests for stable
- Fix typo on comment
- Fix compile error on tests
- Fix crash with uninitialized memory access
- Fix bug on binary decoding
- Fix off-by-one on integer encoding
- Support nested Empty, fix overflow on int
- Fix segfault on watch test, #58
- Fix errors to updated format
- Publicize TupleDepth::increment, fix typos

### <!-- 2 -->🚜 Refactor

- Remove unsafe from futures
- Remove tokio from the example
- Refactor context and network
- Remove debug printlns
- Remove `Send` from `FdbFuture`
- Remove bad test
- Remove the redundant `new` method
- Impl `From` and remove `RangeOptionBuilder::from_tuple`
- Remove single tuple variant

### <!-- 3 -->🆙 Bump

- Bump versions

### <!-- 4 -->⚙️ Other changes

- Initial library create for foundationdb rust bindings
- Initial binding generation working
- Context initialization
- Generate options types from fdb.options
- Use consts from sys crate
- Move examples code to lib
- Default config path based on OS
- Change `TrxCommit::Item` to `Transaction`
- Delete outdated comment
- Cleanup errors and warnings
- Update readme and cargo.toml
- Use defualt path for all examples
- Update hello example
- Copy the library level docs to the crate Readme
- Copied docs from C api
- Call `fdb_future_destory` on FdbFuture::drop
- Track bindings.rs in Git
- Revise `Tranasction::get_range`
- Move examples into tests, cleanup tests
- Versionstamp, {get,set}_read_version, #20
- Support rust-style tuple
- Integer encoding
- Support tuple encoding
- A type safe SingleType and conversions
- Switch to named constants
- Check edge conditions
- Switch nested to use TupleValue
- Cleanup subspace
- Rename `covers` to `is_start_of`
- Extract `Single` types to `tuple::single`
- Rename TupleError to tuple::Error
- Rename TupleValue to tuple::Value
- Rename SingleValue to tuple::single::Value
- Rename SingleType to tuple::single::Type
- Split `Single` and `Tuple` traits into `Encode` and `Decode`
- Rename `single` to `item`
- Use `Uuid` from the `uuid` crate
- Dedup Tuple::{Decode,Encode} and Item::{Decode,Encode}
- Make `tuple::item` private and re-export `tuple::item::Value` as `tuple::Item`
- Make `tuple::item::Value` variants more Rusty
- Make `tuple::item::Value` non-exhaustive
- Rename `tuple::Value` to `Tuple` and `tuple::Item` to `Element`
- Impl Encode for &str
- Revert "impl Encode for &str"
- Impl Encode for some borrowed types
- Subspace tests for tuple variants
- Reduce allocations when decoding
- Allow the association of other errors to FdbError
- Reexport Error from the lib
- Dont force conversion of error in transact
- Allow custom error type
- Drop Other from Error
- Initialization of class-scheduling
- Implement simulator
- Cleanup
- Cleanup with new tuple semantics
- All working
- Use transact on all DB interactions
- Don't force borrowing
- Make the `subspace` module private
- Clean up tests with single-element tuples
- Revert "clean up tests with single-element tuples"
- Rename `keyvalues` to `key_values`
- Rename `encode_to_vec` to `to_vec`
- Rename `decode_full` to `try_from`
- Track when inside tuples during encoding
- Drop the single tuple variant
- Rebase to Rust named fns
- Bounds checks and more tests for Option...
- Encode_to and decode_from
- Clean up one last reference to str
- Update changelog for 0.2
- Document key selector

[unreleased]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/v0.9.0..HEAD
[0.9.0]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/v0.7.0..v0.9.0
[0.7.0]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/v0.6.0..v0.7.0
[0.6.0]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/0.5.0..v0.6.0
[0.5.0]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/v0.3.0..0.5.0
[0.3.0]: https://github.com/foundationdb-rs}/foundationdb-rs/compare/0.2..v0.3.0

<!-- generated by git-cliff -->
