//! Read FoundationDB client transaction profiling data.
//!
//! When client profiling is enabled (`fdbcli> profile client set <rate> <size limit>`),
//! every FoundationDB client samples transactions at the given rate and records their
//! operations (read version, gets, range reads, commits and their errors). Every
//! `CSI_STATUS_DELAY` (10 seconds by default) the client flushes the recorded
//! transactions into the system keyspace under [`PROFILE_PREFIX`]
//! (`\xff\x02/fdbClientInfo/client_latency/`), split in chunks keyed by versionstamp and
//! transaction id. See the [transaction profiler documentation].
//!
//! This crate turns that keyspace into typed [`Event`]s. It does no I/O and depends on no
//! version of the FoundationDB bindings: the application reads the rows with its own
//! client, in its own transaction and retry loop, and hands them to
//! [`ProfileScanner::read_page`], which reassembles and decodes the chunks of one page,
//! decides where the page stops, and returns a resumable [`Cursor`]. Only records
//! written by 7.1+ clients are decoded (see [`decode_events`]).
//!
//! # Example
//!
//! With the `foundationdb` crate:
//!
//! ```no_run
//! use foundationdb::options::TransactionOption;
//! use foundationdb::{ClientBudget, Database, FdbBindingError, KeySelector, RangeOption};
//! use foundationdb_profiling::{Cursor, Page, ProfileScanner};
//! use futures_util::TryStreamExt;
//! use std::time::Duration;
//!
//! # async fn example(db: &Database) -> Result<(), FdbBindingError> {
//! let scanner = ProfileScanner::new();
//! let cursor = Cursor::beginning();
//! let page: Page = db
//!     .run(|trx, _| {
//!         let scanner = scanner.clone();
//!         let cursor = cursor.clone();
//!         async move {
//!             // Options are the caller's job: this crate never sets any.
//!             trx.set_option(TransactionOption::ReadSystemKeys)?;
//!             // Bound every page, well under the 5 second transaction lifetime.
//!             trx.set_client_budget(ClientBudget {
//!                 time_limit: Some(Duration::from_secs(2)),
//!                 ..ClientBudget::default()
//!             });
//!             let range = scanner.range(&cursor);
//!             let opt = RangeOption::from((
//!                 KeySelector::first_greater_or_equal(range.begin.as_slice()),
//!                 KeySelector::first_greater_or_equal(range.end.as_slice()),
//!             ));
//!             let rows = trx
//!                 .get_ranges_keyvalues(opt, true)
//!                 .map_ok(|kv| (kv.key().to_vec(), kv.value().to_vec()));
//!             let page = scanner
//!                 .read_page(&cursor, rows, || trx.check_client_budget().is_err())
//!                 .await
//!                 // FoundationDB errors go back to the retry loop, other errors are
//!                 // boxed as a custom error.
//!                 .map_err(|err| err.into_error(FdbBindingError::new_custom_error))?;
//!             Ok::<_, FdbBindingError>(page)
//!         }
//!     })
//!     .await?;
//! for tx in &page.transactions {
//!     println!("{} {:?}: {} events", tx.version, tx.id, tx.events.len());
//! }
//! // Persist `page.next.as_bytes()` to resume from there later.
//! # Ok(())
//! # }
//! ```
//!
//! See `examples/top_keys.rs` for a runnable end-to-end example that pages through the
//! whole keyspace and prints the hottest keys, ranges and write hot spots.
//!
//! # The caller's contract
//!
//! For each page, the caller reads exactly [`ProfileScanner::range`] of the page's
//! cursor: forward, with snapshot reads and no row limit, both ends as
//! `first_greater_or_equal` key selectors, and gives [`ProfileScanner::read_page`] the
//! rows as a [`Stream`](futures_core::Stream) of `Result<(key, value), E>`. A row
//! outside that range or out of key order fails the page with
//! [`ScanError::InvalidRows`], and a stream error comes back as [`ScanError::Source`] so
//! the caller can hand it to its retry loop.
//!
//! On that transaction:
//!
//! - `TransactionOption::ReadSystemKeys` (or its equivalent) is required, the data lives
//!   in the system keyspace.
//! - `TransactionOption::ReadLockAware` is required if the cluster may be locked (for
//!   instance a DR secondary).
//!
//! # Bounding a page: the stop check
//!
//! The `should_stop` closure given to [`ProfileScanner::read_page`] is required, and is
//! the only place where the caller bounds a page: a transaction budget as above, a row
//! count, anything. [`ProfileScanner::read_page`] calls it after every row and stops as
//! soon as it returns `true`; otherwise the page reads to the end of its range. Without
//! a real bound, a large range can outlive the transaction and hit
//! `transaction_too_old`. Base the check on a caller-pluggable clock rather than the
//! wall clock when the code must be reproducible, for instance under simulation.
//!
//! # Paging and tailing
//!
//! [`Page::next`] is always a valid place to resume from, and [`Page::exhausted`] tells
//! whether the page reached the end of its range (`false` when the stop check stopped
//! it). To read a range in several transactions, loop on [`ProfileScanner::read_page`]
//! with `cursor = page.next` until [`Page::exhausted`]: every transaction is returned
//! once over the sequence of pages, even when the client wrote its chunks in two commits
//! with other records in between, and even when a page stops between them: the next
//! page resumes at the first chunk of the oldest incomplete record. A record is lost
//! (reported as [`SkipReason::BrokenChunks`]) only when the stop check does not let a
//! single page read from its cursor to the end of the records that overlap it (in
//! practice, a single record cannot be read from the cursor before the caller says
//! stop): the records still incomplete at that stop are broken, and the page still
//! advances.
//!
//! To tail the keyspace, persist `page.next` ([`Cursor::as_bytes`] /
//! [`Cursor::from_bytes`]) and keep polling from it: an exhausted page's cursor picks up
//! the records flushed after it, including the second half of a record split across two
//! commits. [`Cursor::at_version`] starts at a commit version, and
//! [`ProfileScanner::end_version`] bounds a page by one. A transaction whose chunks
//! straddle `end_version` is not returned by that bounded read, the cursor stays on its
//! first chunk.
//!
//! Note that the version of a record is the one at which the client flushed it, some time
//! after the profiled transaction ran.
//!
//! # When profiling data is flushed
//!
//! The C++ client records a sampled transaction's events when its native transaction is
//! destroyed. With the `foundationdb` Rust bindings, that happens when the `Transaction`
//! (or the `RetryableTransaction` of a `Database::run` closure) is dropped. In particular a
//! `TransactionCommitError` owns the transaction and keeps it alive until the error is
//! dropped (or recovered with `on_error`), so holding on to such errors delays the
//! profiling data of the failed transaction. The data then reaches the keyspace at the
//! client's next flush, up to `CSI_STATUS_DELAY` later.
//!
//! # Aggregation
//!
//! [`Aggregator`] counts the keys and ranges read and written by the transactions of one
//! or more pages, like the Python `transaction_profiling_analyzer`.
//!
//! [transaction profiler documentation]: https://apple.github.io/foundationdb/transaction-profiler-analyzer.html

#![warn(missing_docs)]

mod aggregate;
mod decode;
mod event;
mod reader;

pub use aggregate::{Aggregator, Bucket, KeyCounts};
pub use decode::{DecodeError, decode_events};
pub use event::{
    Commit, CommitError, CommitRequest, Event, EventHeader, Get, GetError, GetRange, GetRangeError,
    GetVersion, KeyRange, Mutation, ProtocolVersion, SpanContext,
};
pub use reader::{
    Cursor, InvalidCursor, PROFILE_PREFIX, Page, ProfileScanner, ProfiledTransaction, ScanError,
    ScanRange, SkipReason, Skipped,
};
