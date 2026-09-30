# FoundationDB-profiling

Read and decode FoundationDB client transaction profiling data
(`\xff\x02/fdbClientInfo/client_latency/`) from your own application.

The crate does no I/O and does not depend on the `foundationdb` crate: you read the
range given by `ProfileScanner::range` with your own FoundationDB bindings, and
`ProfileScanner::read_page` reassembles and decodes the records into typed events,
returning a cursor to resume from. `Aggregator` counts the hottest keys and ranges.

See the [documentation](https://docs.rs/foundationdb-profiling) and
[`examples/top_keys.rs`](examples/top_keys.rs).
