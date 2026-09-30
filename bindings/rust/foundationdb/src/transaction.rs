// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementations of the FDBTransaction C API
//!
//! <https://apple.github.io/foundationdb/api-c.html#transaction>

use foundationdb_sys as fdb_sys;
use std::fmt;
use std::ops::{Deref, Range, RangeInclusive};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use crate::budget::{AttemptUsage, BudgetExceeded, ClientBudget, UsageSlot, UsageSnapshot};
use crate::env::Clock;
use crate::future::*;
use crate::keyselector::*;
use crate::metrics::{AttemptOutcome, MetricKey, TransactionMetrics};
use crate::options;

use crate::{FdbError, FdbResult, error};
use foundationdb_macros::cfg_api_versions;

use crate::error::FdbBindingError;

use futures::{
    Future, FutureExt, Stream, TryFutureExt, TryStreamExt, future, future::Either, stream,
};

#[cfg_api_versions(min = 610)]
const METADATA_VERSION_KEY: &[u8] = b"\xff/metadataVersion";

/// Special keyspace prefix for conflicting keys.
const CONFLICTING_KEYS_PREFIX: &[u8] = b"\xff\xff/transaction/conflicting_keys/";
// Matches C++ SystemData.cpp conflictingKeysRange end key.
const CONFLICTING_KEYS_END: &[u8] = b"\xff\xff/transaction/conflicting_keys/\xff\xff";

/// Special keyspace prefix for the read conflict ranges of a transaction.
#[cfg_api_versions(min = 630)]
const READ_CONFLICT_RANGE_PREFIX: &[u8] = b"\xff\xff/transaction/read_conflict_range/";
#[cfg_api_versions(min = 630)]
const READ_CONFLICT_RANGE_END: &[u8] = b"\xff\xff/transaction/read_conflict_range/\xff\xff";

/// Special keyspace prefix for the write conflict ranges of a transaction.
#[cfg_api_versions(min = 630)]
const WRITE_CONFLICT_RANGE_PREFIX: &[u8] = b"\xff\xff/transaction/write_conflict_range/";
#[cfg_api_versions(min = 630)]
const WRITE_CONFLICT_RANGE_END: &[u8] = b"\xff\xff/transaction/write_conflict_range/\xff\xff";

/// A key range reported by one of the conflict-range special keyspaces.
///
/// Returned by [`Transaction::conflicting_keys`] (the ranges that made a commit
/// fail), [`Transaction::read_conflict_ranges`] and
/// [`Transaction::write_conflict_ranges`] (the ranges the transaction has
/// accumulated so far).
///
/// Those keyspaces all encode ranges using boundary markers:
/// - Value `b"1"` marks the inclusive start of a range
/// - Value `b"0"` marks the exclusive end of a range
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConflictRange {
    begin: Vec<u8>,
    end: Vec<u8>,
}

impl ConflictRange {
    /// The inclusive begin of the range.
    pub fn begin(&self) -> &[u8] {
        &self.begin
    }

    /// The exclusive end of the range.
    pub fn end(&self) -> &[u8] {
        &self.end
    }
}

impl fmt::Display for ConflictRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}..{}",
            String::from_utf8_lossy(&self.begin),
            String::from_utf8_lossy(&self.end),
        )
    }
}

/// The range type returned by [`Transaction::conflicting_keys`], an alias of
/// [`ConflictRange`] which all three conflict-range readers share.
pub type ConflictingKeyRange = ConflictRange;

/// Incremental parser of the `b"1"` / `b"0"` boundary encoding used by the
/// `\xff\xff/transaction/*` conflict-range special keyspaces.
///
/// A range is only emitted once its end marker arrives, and the open begin
/// marker survives across [`feed`](Self::feed) calls: a `b"1"` and its matching
/// `b"0"` can land in two different `get_range` batches, so the parser is fed
/// batch after batch and the caller paginates until the keyspace is exhausted.
///
/// A begin marker whose end marker never arrives is dropped: only complete
/// ranges are returned, in every build profile.
struct ConflictRangeParser {
    prefix_len: usize,
    ranges: Vec<ConflictRange>,
    open_begin: Option<Vec<u8>>,
}

impl ConflictRangeParser {
    /// `prefix` is the special keyspace prefix to strip from the keys fed in.
    fn new(prefix: &[u8]) -> Self {
        Self {
            prefix_len: prefix.len(),
            ranges: Vec::new(),
            open_begin: None,
        }
    }

    /// Feeds one batch of `(key, value)` pairs, in keyspace order.
    fn feed<'a, I>(&mut self, batch: I)
    where
        I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
    {
        for (raw_key, value) in batch {
            let key = raw_key.get(self.prefix_len..).unwrap_or_default().to_vec();

            match value {
                b"1" => {
                    #[cfg(feature = "trace")]
                    if self.open_begin.is_some() {
                        tracing::warn!(
                            "'1' marker following an unpaired one in a conflict range keyspace, range dropped"
                        );
                    }

                    self.open_begin = Some(key);
                }
                b"0" => {
                    if let Some(begin) = self.open_begin.take() {
                        self.ranges.push(ConflictRange { begin, end: key });
                    }
                }
                _ => {}
            }
        }
    }

    /// Returns the complete ranges parsed so far, dropping a begin marker left
    /// open by the end of the stream.
    fn finish(self) -> Vec<ConflictRange> {
        #[cfg(feature = "trace")]
        if self.open_begin.is_some() {
            tracing::warn!(
                "unpaired '1' marker at the end of a conflict range keyspace, range dropped"
            );
        }

        self.ranges
    }
}

/// A committed transaction.
#[derive(Debug)]
#[repr(transparent)]
pub struct TransactionCommitted {
    tr: Transaction,
}

impl TransactionCommitted {
    /// Retrieves the database version number at which a given transaction was committed.
    ///
    /// Read-only transactions do not modify the database when committed and will have a committed
    /// version of -1. Keep in mind that a transaction which reads keys and then sets them to their
    /// current values may be optimized to a read-only transaction.
    ///
    /// Note that database versions are not necessarily unique to a given transaction and so cannot
    /// be used to determine in what order two transactions completed. The only use for this
    /// function is to manually enforce causal consistency when calling `set_read_version()` on
    /// another subsequent transaction.
    ///
    /// Most applications will not call this function.
    pub fn committed_version(&self) -> FdbResult<i64> {
        let mut version: i64 = 0;
        error::eval(unsafe {
            fdb_sys::fdb_transaction_get_committed_version(self.tr.inner.as_ptr(), &mut version)
        })?;
        Ok(version)
    }

    /// Reset the transaction to its initial state.
    ///
    /// This will not affect previously committed data.
    ///
    /// This is similar to dropping the transaction and creating a new one.
    /// User versions allocated before reset are invalid for the new attempt,
    /// which starts allocating again at zero.
    pub fn reset(mut self) -> Transaction {
        self.tr.reset();
        self.tr
    }
}
impl From<TransactionCommitted> for Transaction {
    fn from(tc: TransactionCommitted) -> Transaction {
        tc.reset()
    }
}

/// A failed to commit transaction.
pub struct TransactionCommitError {
    tr: Transaction,
    err: FdbError,
}

impl TransactionCommitError {
    /// Implements the recommended retry and backoff behavior for a transaction. This function knows
    /// which of the error codes generated by other `Transaction` functions represent temporary
    /// error conditions and which represent application errors that should be handled by the
    /// application. It also implements an exponential backoff strategy to avoid swamping the
    /// database cluster with excessive retries when there is a high level of conflict between
    /// transactions.
    ///
    /// You should not call this method most of the times and use `Database::transact` which
    /// implements a retry loop strategy for you.
    ///
    /// On success the transaction enters a new attempt: its
    /// [usage](Transaction::attempt_usage) restarts from zero, while its
    /// [client budget](Transaction::set_client_budget) is kept. User versions
    /// allocated by [`Transaction::allocate_user_version`] are invalid for the
    /// new attempt, which starts allocating again at zero.
    pub fn on_error(self) -> impl Future<Output = FdbResult<Transaction>> {
        self.tr.mark_attempt_end();
        let cause = self.err;

        FdbFuture::<()>::new(unsafe {
            fdb_sys::fdb_transaction_on_error(self.tr.inner.as_ptr(), self.err.code())
        })
        .map_ok(move |()| {
            self.tr.end_attempt(AttemptOutcome::Retried { cause });
            self.tr.begin_attempt_usage();
            self.tr.reset_user_version_allocator();
            self.tr
        })
    }

    /// Reads the conflicting key ranges that caused this commit failure.
    ///
    /// Only returns meaningful results if
    /// [`TransactionOption::ReportConflictingKeys`](crate::options::TransactionOption::ReportConflictingKeys)
    /// was set on the transaction **and** the error is `not_committed` (code 1020).
    ///
    /// Must be called **before** [`on_error`](Self::on_error) which resets the transaction.
    ///
    /// # Errors
    ///
    /// Returns an `FdbError` if the special keyspace read fails.
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub async fn conflicting_keys(&self) -> FdbResult<Vec<ConflictingKeyRange>> {
        self.tr.conflicting_keys().await
    }

    /// Reset the transaction to its initial state.
    ///
    /// This is similar to dropping the transaction and creating a new one.
    /// User versions allocated before reset are invalid for the new attempt,
    /// which starts allocating again at zero.
    pub fn reset(mut self) -> Transaction {
        self.tr.reset();
        self.tr
    }

    /// Splits the error into the transaction it failed on and the error itself,
    /// without resetting anything.
    ///
    /// Used by the retry runner to hand `on_error` an error chosen by a
    /// [`RetryPolicy`](crate::runner::RetryPolicy) rather than the commit error
    /// itself.
    pub(crate) fn into_parts(self) -> (Transaction, FdbError) {
        (self.tr, self.err)
    }
}

impl Deref for TransactionCommitError {
    type Target = FdbError;
    fn deref(&self) -> &FdbError {
        &self.err
    }
}

impl From<TransactionCommitError> for FdbError {
    fn from(tce: TransactionCommitError) -> FdbError {
        tce.err
    }
}

impl fmt::Debug for TransactionCommitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TransactionCommitError({})", self.err)
    }
}

impl fmt::Display for TransactionCommitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.err.fmt(f)
    }
}

impl std::error::Error for TransactionCommitError {}

/// The result of `Transaction::Commit`
type TransactionResult = Result<TransactionCommitted, TransactionCommitError>;

/// A cancelled transaction
#[derive(Debug)]
#[repr(transparent)]
pub struct TransactionCancelled {
    tr: Transaction,
}
impl TransactionCancelled {
    /// Reset the transaction to its initial state.
    ///
    /// This is similar to dropping the transaction and creating a new one.
    /// User versions allocated before reset are invalid for the new attempt,
    /// which starts allocating again at zero.
    pub fn reset(mut self) -> Transaction {
        self.tr.reset();
        self.tr
    }
}
impl From<TransactionCancelled> for Transaction {
    fn from(tc: TransactionCancelled) -> Transaction {
        tc.reset()
    }
}

/// In FoundationDB, a transaction is a mutable snapshot of a database.
///
/// All read and write operations on a transaction see and modify an otherwise-unchanging version of the database and only change the underlying database if and when the transaction is committed. Read operations do see the effects of previous write operations on the same transaction. Committing a transaction usually succeeds in the absence of conflicts.
///
/// Applications must provide error handling and an appropriate retry loop around the application code for a transaction. See the documentation for [fdb_transaction_on_error()](https://apple.github.io/foundationdb/api-c.html#transaction).
///
/// Transactions group operations into a unit with the properties of atomicity, isolation, and durability. Transactions also provide the ability to maintain an application’s invariants or integrity constraints, supporting the property of consistency. Together these properties are known as ACID.
///
/// Transactions are also causally consistent: once a transaction has been successfully committed, all subsequently created transactions will see the modifications made by it.
#[derive(Debug)]
pub struct Transaction {
    // Order of fields should not be changed, because Rust drops field top-to-bottom, and
    // transaction should be dropped before cluster.
    inner: NonNull<fdb_sys::FDBTransaction>,
    /// Metrics collector of the transaction, attached at creation or by a
    /// runner hook, see [`Transaction::attach_metrics`]. Set at most once.
    metrics: OnceLock<TransactionMetrics>,
    /// Always-on accounting of the current attempt, see [`crate::budget`].
    usage: UsageSlot,
    /// Client-side limits applied to the current attempt. Unlike `usage`, they
    /// are configuration: they survive `on_error` and `reset`.
    budget: Mutex<ClientBudget>,
    /// The next user version to issue for an incomplete versionstamp in the
    /// current transaction attempt.
    user_version: AtomicU32,
}
unsafe impl Send for Transaction {}
unsafe impl Sync for Transaction {}

/// Converts Rust `bool` into `fdb_sys::fdb_bool_t`
#[inline]
fn fdb_bool(v: bool) -> fdb_sys::fdb_bool_t {
    if v { 1 } else { 0 }
}
#[inline]
fn fdb_len(len: usize, context: &'static str) -> std::os::raw::c_int {
    assert!(len <= i32::MAX as usize, "{context}.len() > i32::MAX");
    len as i32
}
#[inline]
fn fdb_iteration(iteration: usize) -> std::os::raw::c_int {
    if iteration > i32::MAX as usize {
        0 // this will cause client_invalid_operation
    } else {
        iteration as i32
    }
}
#[inline]
fn fdb_limit(v: usize) -> std::os::raw::c_int {
    if v > i32::MAX as usize {
        i32::MAX
    } else {
        v as i32
    }
}

/// `RangeOption` represents a query parameters for range scan query.
///
/// You can construct `RangeOption` easily:
///
/// ```
/// use foundationdb::RangeOption;
///
/// let opt = RangeOption::from((b"begin".as_ref(), b"end".as_ref()));
/// let opt: RangeOption = (b"begin".as_ref()..b"end".as_ref()).into();
/// let opt = RangeOption {
///     limit: Some(10),
///     ..RangeOption::from((b"begin".as_ref(), b"end".as_ref()))
/// };
/// ```
#[derive(Debug, Clone)]
pub struct RangeOption<'a> {
    /// The beginning of the range.
    pub begin: KeySelector<'a>,
    /// The end of the range.
    pub end: KeySelector<'a>,
    /// If non-zero, indicates the maximum number of key-value pairs to return.
    /// `None` and `Some(0)` both mean unlimited.
    pub limit: Option<usize>,
    /// If non-zero, indicates a (soft) cap on the combined number of bytes of keys and values to
    /// return for each item.
    pub target_bytes: usize,
    /// One of the options::StreamingMode values indicating how the caller would like the data in
    /// the range returned.
    pub mode: options::StreamingMode,
    /// If true, key-value pairs will be returned in reverse lexicographical order beginning at
    /// the end of the range.
    pub reverse: bool,
    #[doc(hidden)]
    pub __non_exhaustive: std::marker::PhantomData<()>,
}

impl RangeOption<'_> {
    /// Reverses the range direction.
    pub fn rev(mut self) -> Self {
        self.reverse = !self.reverse;
        self
    }

    pub fn next_range(mut self, kvs: &FdbValues) -> Option<Self> {
        if !kvs.more() {
            return None;
        }

        let last = kvs.last()?;
        let last_key = last.key();

        if let Some(limit) = self.limit.as_mut().filter(|limit| **limit != 0) {
            *limit = limit.saturating_sub(kvs.len());
            if *limit == 0 {
                return None;
            }
        }

        if self.reverse {
            self.end.make_first_greater_or_equal(last_key);
        } else {
            self.begin.make_first_greater_than(last_key);
        }
        Some(self)
    }

    #[cfg_api_versions(min = 710)]
    pub(crate) fn next_mapped_range(mut self, kvs: &MappedKeyValues) -> Option<Self> {
        if !kvs.more() {
            return None;
        }

        let last = kvs.last()?;
        let last_key = last.parent_key();

        if let Some(limit) = self.limit.as_mut().filter(|limit| **limit != 0) {
            *limit = limit.saturating_sub(kvs.len());
            if *limit == 0 {
                return None;
            }
        }

        if self.reverse {
            self.end.make_first_greater_or_equal(last_key);
        } else {
            self.begin.make_first_greater_than(last_key);
        }
        Some(self)
    }
}

impl Default for RangeOption<'_> {
    fn default() -> Self {
        Self {
            begin: KeySelector::first_greater_or_equal([].as_ref()),
            end: KeySelector::first_greater_or_equal([].as_ref()),
            limit: None,
            target_bytes: 0,
            mode: options::StreamingMode::Iterator,
            reverse: false,
            __non_exhaustive: std::marker::PhantomData,
        }
    }
}

impl<'a> From<(KeySelector<'a>, KeySelector<'a>)> for RangeOption<'a> {
    fn from((begin, end): (KeySelector<'a>, KeySelector<'a>)) -> Self {
        Self {
            begin,
            end,
            ..Self::default()
        }
    }
}
impl From<(Vec<u8>, Vec<u8>)> for RangeOption<'static> {
    fn from((begin, end): (Vec<u8>, Vec<u8>)) -> Self {
        Self {
            begin: KeySelector::first_greater_or_equal(begin),
            end: KeySelector::first_greater_or_equal(end),
            ..Self::default()
        }
    }
}
impl<'a> From<(&'a [u8], &'a [u8])> for RangeOption<'a> {
    fn from((begin, end): (&'a [u8], &'a [u8])) -> Self {
        Self {
            begin: KeySelector::first_greater_or_equal(begin),
            end: KeySelector::first_greater_or_equal(end),
            ..Self::default()
        }
    }
}
impl<'a> From<std::ops::Range<KeySelector<'a>>> for RangeOption<'a> {
    fn from(range: Range<KeySelector<'a>>) -> Self {
        RangeOption::from((range.start, range.end))
    }
}

impl<'a> From<std::ops::Range<&'a [u8]>> for RangeOption<'a> {
    fn from(range: Range<&'a [u8]>) -> Self {
        RangeOption::from((range.start, range.end))
    }
}

impl From<std::ops::Range<std::vec::Vec<u8>>> for RangeOption<'static> {
    fn from(range: Range<Vec<u8>>) -> Self {
        RangeOption::from((range.start, range.end))
    }
}

impl<'a> From<std::ops::RangeInclusive<&'a [u8]>> for RangeOption<'a> {
    fn from(range: RangeInclusive<&'a [u8]>) -> Self {
        let (start, end) = range.into_inner();
        (KeySelector::first_greater_or_equal(start)..KeySelector::first_greater_than(end)).into()
    }
}

impl From<std::ops::RangeInclusive<std::vec::Vec<u8>>> for RangeOption<'static> {
    fn from(range: RangeInclusive<Vec<u8>>) -> Self {
        let (start, end) = range.into_inner();
        (KeySelector::first_greater_or_equal(start)..KeySelector::first_greater_than(end)).into()
    }
}

impl Transaction {
    pub(crate) fn new(inner: NonNull<fdb_sys::FDBTransaction>) -> Self {
        Self {
            inner,
            metrics: OnceLock::new(),
            usage: UsageSlot::default(),
            budget: Mutex::new(ClientBudget::default()),
            user_version: AtomicU32::new(0),
        }
    }

    /// Allocates a user version for an incomplete [`Versionstamp`](crate::tuple::Versionstamp).
    ///
    /// Values start at zero and increase through `65535`. Values issued by
    /// this allocator are unique within the current transaction attempt, across
    /// all handles that share that transaction. Explicit user versions supplied
    /// to `Versionstamp::incomplete` remain supported, but can collide with
    /// allocator-issued versions or with each other.
    ///
    /// The allocator performs no database operations and adds no conflict
    /// ranges. It has no reservation mechanism: allocate a value when building
    /// the versionstamped mutation that will use it.
    ///
    /// `reset` and a successful `on_error` start a new transaction attempt and
    /// restart allocation at zero. A cancelled transaction retains its issued
    /// values until it is reset. Versions from a previous attempt therefore
    /// must not be reused in mutations for the new attempt.
    ///
    /// # Errors
    ///
    /// Returns [`FdbBindingError::UserVersionExhausted`] after all 65536 user
    /// versions have been issued for this attempt.
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn allocate_user_version(&self) -> Result<u16, FdbBindingError> {
        self.user_version
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                (current <= u16::MAX as u32).then_some(current + 1)
            })
            .map(|current| current as u16)
            .map_err(|_| FdbBindingError::UserVersionExhausted)
    }

    /// Attaches a metrics collector to this transaction and opens its first
    /// attempt on the accounting generation currently in use.
    ///
    /// A transaction collects metrics for at most one
    /// [`TransactionMetrics`]: attaching a second one does nothing, the first
    /// collector keeps receiving everything. This is what
    /// [`MetricsHooks`](crate::runner::MetricsHooks) uses to wire itself onto a
    /// transaction created by the runner.
    pub(crate) fn attach_metrics(&self, metrics: &TransactionMetrics) {
        if self.metrics.set(metrics.clone()).is_ok() {
            self.open_metrics_attempt();
        }
    }

    /// The metrics collector of the transaction, if one is attached.
    fn metrics(&self) -> Option<&TransactionMetrics> {
        self.metrics.get()
    }

    /// Points the metrics at the accounting generation of the current attempt,
    /// so that whatever happens from now on is recorded there.
    fn open_metrics_attempt(&self) {
        if let Some(metrics) = self.metrics() {
            metrics.begin_attempt(self.usage.current());
        }
    }

    /// Freezes the duration of the current attempt: it is over, only the retry
    /// machinery runs from here.
    fn mark_attempt_end(&self) {
        if let Some(metrics) = self.metrics() {
            metrics.mark_attempt_end();
        }
    }

    /// Pushes the current attempt to the metrics report.
    fn end_attempt(&self, outcome: AttemptOutcome) {
        if let Some(metrics) = self.metrics() {
            metrics.finish_attempt(outcome);
        }
    }

    /// The accounting generation an operation issued now must record into.
    ///
    /// Callers of asynchronous operations must capture it when the operation is
    /// **issued** and record into that clone on completion, so that a future
    /// outliving its attempt cannot pollute the next one.
    fn usage(&self) -> Arc<AttemptUsage> {
        self.usage.current()
    }

    /// Starts a fresh accounting generation measured with `clock`.
    ///
    /// This never touches the budget mutex: callers take the clock out of the
    /// budget themselves, in the same critical section as whatever else they
    /// need from it.
    fn begin_attempt_with_clock(&self, clock: Option<Arc<dyn Clock>>) {
        self.usage.begin(clock);
        self.open_metrics_attempt();
    }

    /// Starts a fresh accounting generation for a new transaction attempt.
    ///
    /// Usage counters restart from zero and the elapsed time is measured from
    /// here, with the clock of the current budget. The client budget, being
    /// configuration, is left untouched.
    ///
    /// An instrumented transaction also starts recording a new attempt: call
    /// [`end_attempt`](Self::end_attempt) first when the attempt that is ending
    /// must appear in the report.
    pub(crate) fn begin_attempt_usage(&self) {
        let clock = self
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clock
            .clone();
        self.begin_attempt_with_clock(clock);
    }

    /// Returns the usage accounted for the current transaction attempt.
    ///
    /// Accounting is always on, no instrumentation needed. The counters are
    /// client-side estimates and are reset on every new attempt, see
    /// [`crate::budget`].
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn attempt_usage(&self) -> UsageSnapshot {
        self.usage().snapshot()
    }

    /// Sets the client-side budget of this transaction.
    ///
    /// The limits are **not** enforced by FoundationDB and not enforced
    /// automatically: they are checked when you call
    /// [`check_client_budget`](Self::check_client_budget). See [`crate::budget`]
    /// for what is counted and how precise it is.
    ///
    /// Setting a budget starts a fresh accounting generation, so the limits
    /// apply to what happens from now on. They then survive `on_error` and
    /// `reset`, and apply to each subsequent attempt with usage back at zero.
    /// That generation, and every later one, is measured with the
    /// [`clock`](ClientBudget::clock) of this budget.
    ///
    /// On an instrumented transaction, set the budget before doing anything
    /// else: the new generation is also the one the metrics record into, so
    /// operations performed before this call are not reported.
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn set_client_budget(&self, budget: ClientBudget) {
        // One critical section: store the budget and take its clock out, so the
        // generation below cannot be stamped with the clock of a budget that a
        // concurrent call has already replaced.
        let clock = {
            let mut slot = self
                .budget
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *slot = budget;
            slot.clock.clone()
        };
        self.begin_attempt_with_clock(clock);
    }

    /// Removes every client-side limit of this transaction.
    ///
    /// Accounting keeps running: only the limits are dropped.
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn clear_client_budget(&self) {
        *self
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = ClientBudget::default();
    }

    /// Checks the usage of the current attempt against the client-side budget.
    ///
    /// This is a synchronous, cheap check: a few atomic loads and one reading of
    /// the [`clock`](ClientBudget::clock) of the budget, the wall clock by
    /// default, no call to the database. Nothing calls it for you, so call it
    /// between the operations of your transaction, typically inside a
    /// [`Database::run`](crate::Database::run) closure where
    /// [`FdbBindingError`](crate::FdbBindingError) makes `?` work:
    ///
    /// ```
    /// # use foundationdb::*;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let db = Database::default()?;
    /// # let keys: Vec<Vec<u8>> = vec![];
    /// db.run(|trx, _| {
    ///     let keys = keys.clone();
    ///     async move {
    ///         trx.set_client_budget(ClientBudget {
    ///             max_bytes_read: Some(1024 * 1024),
    ///             ..ClientBudget::default()
    ///         });
    ///         for key in keys {
    ///             trx.get(&key, false).await?;
    ///             trx.check_client_budget()?;
    ///         }
    ///         Ok::<_, FdbBindingError>(())
    ///     }
    /// })
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the first [`BudgetExceeded`] limit found, checked in order:
    /// time, bytes read, bytes written.
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn check_client_budget(&self) -> Result<(), BudgetExceeded> {
        let budget = self
            .budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        budget.check(&self.usage())
    }

    /// Called to set an option on an FDBTransaction.
    pub fn set_option(&self, opt: options::TransactionOption) -> FdbResult<()> {
        unsafe { opt.apply(self.inner.as_ptr()) }
    }

    /// Pass through an option given a code and raw data. Useful when creating a passthrough layer
    /// where the code and data will be provided as raw, in order to avoid deserializing to an option
    /// and serializing it back to code and data.
    /// In general, you should use `set_option`.
    pub fn set_raw_option(
        &self,
        code: fdb_sys::FDBTransactionOption,
        data: Option<Vec<u8>>,
    ) -> FdbResult<()> {
        let (data_ptr, size) = data
            .as_ref()
            .map(|data| {
                (
                    data.as_ptr(),
                    i32::try_from(data.len()).expect("len to fit in i32"),
                )
            })
            .unwrap_or_else(|| (std::ptr::null(), 0));
        let err = unsafe {
            fdb_sys::fdb_transaction_set_option(self.inner.as_ptr(), code, data_ptr, size)
        };
        if err != 0 {
            Err(FdbError::from_code(err))
        } else {
            Ok(())
        }
    }

    /// Modify the database snapshot represented by transaction to change the given
    /// key to have the given value.
    ///
    /// If the given key was not previously present in the database it is inserted.
    /// The modification affects the actual database only if transaction is later
    /// committed with `Transaction::commit`.
    ///
    /// # Arguments
    ///
    /// * `key` - the name of the key to be inserted into the database.
    /// * `value` - the value to be inserted into the database
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, key, value))
    )]
    pub fn set(&self, key: &[u8], value: &[u8]) {
        unsafe {
            fdb_sys::fdb_transaction_set(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
                value.as_ptr(),
                fdb_len(value.len(), "value"),
            )
        }

        self.usage().record_set((key.len() + value.len()) as u64);
    }

    /// Modify the database snapshot represented by transaction to remove the given key from the
    /// database.
    ///
    /// If the key was not previously present in the database, there is no effect. The modification
    /// affects the actual database only if transaction is later committed with
    /// `Transaction::commit`.
    ///
    /// # Arguments
    ///
    /// * `key` - the name of the key to be removed from the database.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, key))
    )]
    pub fn clear(&self, key: &[u8]) {
        unsafe {
            fdb_sys::fdb_transaction_clear(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
            )
        }

        self.usage().record_clear(key.len() as u64);
    }

    /// Reads a value from the database snapshot represented by transaction.
    ///
    /// Returns an FDBFuture which will be set to the value of key in the database if there is any.
    ///
    /// # Arguments
    ///
    /// * `key` - the name of the key to be looked up in the database
    /// * `snapshot` - `true` if this is a [snapshot read](https://apple.github.io/foundationdb/api-c.html#snapshots)
    ///
    /// The [attempt usage](Self::attempt_usage) is recorded when the future
    /// resolves successfully, into the attempt that issued the read: a read
    /// still in flight is not visible to
    /// [`check_client_budget`](Self::check_client_budget) yet.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, key))
    )]
    pub fn get(
        &self,
        key: &[u8],
        snapshot: bool,
    ) -> impl Future<Output = FdbResult<Option<FdbSlice>>> + Send + Sync + Unpin + use<> {
        let usage = self.usage();
        let lenght_key = key.len();

        FdbFuture::<Option<FdbSlice>>::new(unsafe {
            fdb_sys::fdb_transaction_get(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
                fdb_bool(snapshot),
            )
        })
        .map(move |result| {
            if let Ok(value) = &result {
                let (bytes_count, kv_fetched) = if let Some(values) = value {
                    ((lenght_key + values.len()) as u64, 1)
                } else {
                    (lenght_key as u64, 0)
                };

                usage.record_get(bytes_count, kv_fetched);
            }
            result
        })
    }

    /// Modify the database snapshot represented by transaction to perform the operation indicated
    /// by operationType with operand param to the value stored by the given key.
    ///
    /// An atomic operation is a single database command that carries out several logical steps:
    /// reading the value of a key, performing a transformation on that value, and writing the
    /// result. Different atomic operations perform different transformations. Like other database
    /// operations, an atomic operation is used within a transaction; however, its use within a
    /// transaction will not cause the transaction to conflict.
    ///
    /// Atomic operations do not expose the current value of the key to the client but simply send
    /// the database the transformation to apply. In regard to conflict checking, an atomic
    /// operation is equivalent to a write without a read. It can only cause other transactions
    /// performing reads of the key to conflict.
    ///
    /// By combining these logical steps into a single, read-free operation, FoundationDB can
    /// guarantee that the transaction will not conflict due to the operation. This makes atomic
    /// operations ideal for operating on keys that are frequently modified. A common example is
    /// the use of a key-value pair as a counter.
    ///
    /// # Warning
    ///
    /// If a transaction uses both an atomic operation and a strictly serializable read on the same
    /// key, the benefits of using the atomic operation (for both conflict checking and performance)
    /// are lost.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, key, param))
    )]
    pub fn atomic_op(&self, key: &[u8], param: &[u8], op_type: options::MutationType) {
        unsafe {
            fdb_sys::fdb_transaction_atomic_op(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
                param.as_ptr(),
                fdb_len(param.len(), "param"),
                op_type.code(),
            )
        }

        self.usage()
            .record_atomic_op((key.len() + param.len()) as u64);
    }

    /// Resolves a key selector against the keys in the database snapshot represented by
    /// transaction.
    ///
    /// Returns an FDBFuture which will be set to the key in the database matching the key
    /// selector.
    ///
    /// # Arguments
    ///
    /// * `selector`: the key selector
    /// * `snapshot`: `true` if this is a [snapshot read](https://apple.github.io/foundationdb/api-c.html#snapshots)
    ///
    /// In the [attempt usage](Self::attempt_usage), this counts as a `get` of
    /// the selector key plus the resolved key, recorded when the future
    /// resolves successfully.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, selector, snapshot))
    )]
    pub fn get_key(
        &self,
        selector: &KeySelector,
        snapshot: bool,
    ) -> impl Future<Output = FdbResult<FdbSlice>> + Send + Sync + Unpin + use<> {
        let key = selector.key();
        let usage = self.usage();
        let length_key = key.len();

        FdbFuture::<FdbSlice>::new(unsafe {
            fdb_sys::fdb_transaction_get_key(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
                fdb_bool(selector.or_equal()),
                selector.offset(),
                fdb_bool(snapshot),
            )
        })
        .map(move |result| {
            if let Ok(resolved_key) = &result {
                usage.record_get((length_key + resolved_key.len()) as u64, 0);
            }
            result
        })
    }

    /// Reads all key-value pairs in the database snapshot represented by transaction (potentially
    /// limited by limit, target_bytes, or mode) which have a key lexicographically greater than or
    /// equal to the key resolved by the begin key selector and lexicographically less than the key
    /// resolved by the end key selector.
    ///
    /// Returns a stream of KeyValue slices.
    ///
    /// This method is a little more efficient than `get_ranges_keyvalues` but a little harder to
    /// use.
    ///
    /// # Arguments
    ///
    /// * `opt`: the range, limit, target_bytes and mode
    /// * `snapshot`: `true` if this is a [snapshot read](https://apple.github.io/foundationdb/api-c.html#snapshots)
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, opt, snapshot))
    )]
    pub fn get_ranges<'a>(
        &'a self,
        opt: RangeOption<'a>,
        snapshot: bool,
    ) -> impl Stream<Item = FdbResult<FdbValues>> + Send + Sync + Unpin + 'a {
        stream::unfold((1, Some(opt)), move |(iteration, maybe_opt)| {
            if let Some(opt) = maybe_opt {
                Either::Left(self.get_range(&opt, iteration as usize, snapshot).map(
                    move |maybe_values| {
                        let next_opt = match &maybe_values {
                            Ok(values) => opt.next_range(values),
                            Err(..) => None,
                        };
                        Some((maybe_values, (iteration + 1, next_opt)))
                    },
                ))
            } else {
                Either::Right(future::ready(None))
            }
        })
    }

    /// Reads all key-value pairs in the database snapshot represented by transaction (potentially
    /// limited by limit, target_bytes, or mode) which have a key lexicographically greater than or
    /// equal to the key resolved by the begin key selector and lexicographically less than the key
    /// resolved by the end key selector.
    ///
    /// Returns a stream of KeyValue.
    ///
    /// # Arguments
    ///
    /// * `opt`: the range, limit, target_bytes and mode
    /// * `snapshot`: `true` if this is a [snapshot read](https://apple.github.io/foundationdb/api-c.html#snapshots)
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, opt, snapshot))
    )]
    pub fn get_ranges_keyvalues<'a>(
        &'a self,
        opt: RangeOption<'a>,
        snapshot: bool,
    ) -> impl Stream<Item = FdbResult<FdbValue>> + Unpin + 'a {
        self.get_ranges(opt, snapshot)
            .map_ok(|values| stream::iter(values.into_iter().map(Ok)))
            .try_flatten()
    }

    /// Reads all key-value pairs in the database snapshot represented by transaction (potentially
    /// limited by limit, target_bytes, or mode) which have a key lexicographically greater than or
    /// equal to the key resolved by the begin key selector and lexicographically less than the key
    /// resolved by the end key selector.
    ///
    /// <div class="warning">
    /// This method returns <strong>only a single batch</strong> of results, not necessarily all
    /// key-value pairs in the range. It is a low-level primitive for manual pagination. Most
    /// callers should use
    /// <a href="struct.Transaction.html#method.get_ranges_keyvalues"><code>get_ranges_keyvalues</code></a>,
    /// which automatically pages through the full range and yields every key-value pair as a stream.
    /// </div>
    ///
    /// # Arguments
    ///
    /// * `opt`: the range, limit, target_bytes and mode
    /// * `iteration`: If opt.mode is Iterator, this parameter should start at 1 and be incremented
    ///   by 1 for each successive call while reading this range. In all other cases it is ignored.
    /// * `snapshot`: `true` if this is a [snapshot read](https://apple.github.io/foundationdb/api-c.html#snapshots)
    ///
    /// In the [attempt usage](Self::attempt_usage), each resolved batch counts
    /// as one `call_get_range`: a full range scan through
    /// [`get_ranges`](Self::get_ranges) counts once per underlying batch.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, opt, snapshot))
    )]
    pub fn get_range(
        &self,
        opt: &RangeOption,
        iteration: usize,
        snapshot: bool,
    ) -> impl Future<Output = FdbResult<FdbValues>> + Send + Sync + Unpin + use<> {
        self.get_range_impl(opt, iteration, snapshot, Some(self.usage()))
    }

    /// Same as [`get_range`](Self::get_range), but not accounted in the
    /// [attempt usage](Self::attempt_usage).
    ///
    /// For reads the binding performs on behalf of the user, on the special
    /// keyspace, which should neither consume the client budget nor show up in
    /// the usage of the user's own operations.
    pub(crate) fn get_range_unmetered(
        &self,
        opt: &RangeOption,
        iteration: usize,
        snapshot: bool,
    ) -> impl Future<Output = FdbResult<FdbValues>> + Send + Sync + Unpin + use<> {
        self.get_range_impl(opt, iteration, snapshot, None)
    }

    /// `usage` is the accounting generation to record into, `None` to skip
    /// accounting entirely.
    fn get_range_impl(
        &self,
        opt: &RangeOption,
        iteration: usize,
        snapshot: bool,
        usage: Option<Arc<AttemptUsage>>,
    ) -> impl Future<Output = FdbResult<FdbValues>> + Send + Sync + Unpin + use<> {
        let begin = &opt.begin;
        let end = &opt.end;
        let key_begin = begin.key();
        let key_end = end.key();

        FdbFuture::<FdbValues>::new(unsafe {
            fdb_sys::fdb_transaction_get_range(
                self.inner.as_ptr(),
                key_begin.as_ptr(),
                fdb_len(key_begin.len(), "key_begin"),
                fdb_bool(begin.or_equal()),
                begin.offset(),
                key_end.as_ptr(),
                fdb_len(key_end.len(), "key_end"),
                fdb_bool(end.or_equal()),
                end.offset(),
                fdb_limit(opt.limit.unwrap_or(0)),
                fdb_limit(opt.target_bytes),
                opt.mode.code(),
                fdb_iteration(iteration),
                fdb_bool(snapshot),
                fdb_bool(opt.reverse),
            )
        })
        .map(move |result| {
            if let Ok(values) = &result {
                let kv_fetched = values.len();
                let mut bytes_count = 0;

                for key_value in values.as_ref() {
                    let key_len = key_value.key().len();
                    let value_len = key_value.value().len();

                    bytes_count += (key_len + value_len) as u64
                }

                if let Some(usage) = usage.as_ref() {
                    usage.record_get_range(bytes_count, kv_fetched as u64);
                }
            };

            result
        })
    }

    /// Mapped Range is an experimental feature introduced in FDB 7.1.
    /// It is intended to improve the client throughput and reduce latency for querying data through a Subspace used as a "index".
    /// In such a case, querying records by scanning an index in relational databases can be
    /// translated to a GetRange request on the index entries followed up by multiple GetValue requests for the record entries in FDB.
    ///
    /// This method is allowing FoundationDB "follow up" a GetRange request with GetValue requests,
    /// this can happen in one request without additional back and forth. Considering the overhead
    /// of each request, this saves time and resources on serialization, deserialization, and network.
    ///
    /// A mapped request will:
    ///
    /// * Do a range query (same as a `Transaction.get_range` request) and get the result. We call it the primary query.
    /// * For each key-value pair in the primary query result, translate it to a `get_range` query and get the result. We call them secondary queries.
    /// * Put all results in a nested structure and return them.
    ///
    /// **WARNING** : This feature is considered experimental at this time. It is only allowed when
    /// using snapshot isolation AND disabling read-your-writes.
    ///
    /// More info can be found in the relevant [documentation](https://github.com/apple/foundationdb/wiki/Everything-about-GetMappedRange#input).
    ///
    /// This is the "raw" version, users are expected to use [Transaction::get_mapped_ranges]
    ///
    /// In the [attempt usage](Self::attempt_usage), a resolved batch counts as
    /// one `call_get_range` and as the bytes of the primary key-values plus the
    /// nested key-values returned by the secondary queries.
    #[cfg_api_versions(min = 710)]
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, opt, mapper, snapshot))
    )]
    pub fn get_mapped_range(
        &self,
        opt: &RangeOption,
        mapper: &[u8],
        iteration: usize,
        snapshot: bool,
    ) -> impl Future<Output = FdbResult<MappedKeyValues>> + Send + Sync + Unpin + use<> {
        let begin = &opt.begin;
        let end = &opt.end;
        let key_begin = begin.key();
        let key_end = end.key();

        let usage = self.usage();

        FdbFuture::<MappedKeyValues>::new(unsafe {
            fdb_sys::fdb_transaction_get_mapped_range(
                self.inner.as_ptr(),
                key_begin.as_ptr(),
                fdb_len(key_begin.len(), "key_begin"),
                fdb_bool(begin.or_equal()),
                begin.offset(),
                key_end.as_ptr(),
                fdb_len(key_end.len(), "key_end"),
                fdb_bool(end.or_equal()),
                end.offset(),
                mapper.as_ptr(),
                fdb_len(mapper.len(), "mapper_length"),
                fdb_limit(opt.limit.unwrap_or(0)),
                fdb_limit(opt.target_bytes),
                opt.mode.code(),
                fdb_iteration(iteration),
                fdb_bool(snapshot),
                fdb_bool(opt.reverse),
            )
        })
        .map(move |result| {
            if let Ok(values) = &result {
                let mut bytes_count = 0;
                let mut kv_fetched = 0;

                for mapped_key_value in values.as_ref() {
                    bytes_count += (mapped_key_value.parent_key().len()
                        + mapped_key_value.parent_value().len())
                        as u64;
                    kv_fetched += 1;

                    for key_value in mapped_key_value.key_values() {
                        bytes_count += (key_value.key().len() + key_value.value().len()) as u64;
                        kv_fetched += 1;
                    }
                }

                usage.record_get_range(bytes_count, kv_fetched);
            }

            result
        })
    }

    /// Mapped Range is an experimental feature introduced in FDB 7.1.
    /// It is intended to improve the client throughput and reduce latency for querying data through a Subspace used as a "index".
    /// In such a case, querying records by scanning an index in relational databases can be
    /// translated to a GetRange request on the index entries followed up by multiple GetValue requests for the record entries in FDB.
    ///
    /// This method is allowing FoundationDB "follow up" a GetRange request with GetValue requests,
    /// this can happen in one request without additional back and forth. Considering the overhead
    /// of each request, this saves time and resources on serialization, deserialization, and network.
    ///
    /// A mapped request will:
    ///
    /// * Do a range query (same as a `Transaction.get_range` request) and get the result. We call it the primary query.
    /// * For each key-value pair in the primary query result, translate it to a `get_range` query and get the result. We call them secondary queries.
    /// * Put all results in a nested structure and return them.
    ///
    /// **WARNING** : This feature is considered experimental at this time. It is only allowed when
    /// using snapshot isolation AND disabling read-your-writes.
    ///
    /// More info can be found in the relevant [documentation](https://github.com/apple/foundationdb/wiki/Everything-about-GetMappedRange#input).
    #[cfg_api_versions(min = 710)]
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, opt, mapper, snapshot))
    )]
    pub fn get_mapped_ranges<'a>(
        &'a self,
        opt: RangeOption<'a>,
        mapper: &'a [u8],
        snapshot: bool,
    ) -> impl Stream<Item = FdbResult<MappedKeyValues>> + Send + Sync + Unpin + 'a {
        stream::unfold((1, Some(opt)), move |(iteration, maybe_opt)| {
            if let Some(opt) = maybe_opt {
                Either::Left(
                    self.get_mapped_range(&opt, mapper, iteration as usize, snapshot)
                        .map(move |maybe_values| {
                            let next_opt = match &maybe_values {
                                Ok(values) => opt.next_mapped_range(values),
                                Err(..) => None,
                            };
                            Some((maybe_values, (iteration + 1, next_opt)))
                        }),
                )
            } else {
                Either::Right(future::ready(None))
            }
        })
    }

    /// Modify the database snapshot represented by transaction to remove all keys (if any) which
    /// are lexicographically greater than or equal to the given begin key and lexicographically
    /// less than the given end_key.
    ///
    /// The modification affects the actual database only if transaction is later committed with
    /// `Transaction::commit`.
    ///
    /// In the [attempt usage](Self::attempt_usage), this counts as the two
    /// boundary keys only: the volume of data actually deleted is unknown to
    /// the client.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, begin, end))
    )]
    pub fn clear_range(&self, begin: &[u8], end: &[u8]) {
        unsafe {
            fdb_sys::fdb_transaction_clear_range(
                self.inner.as_ptr(),
                begin.as_ptr(),
                fdb_len(begin.len(), "begin"),
                end.as_ptr(),
                fdb_len(end.len(), "end"),
            )
        }

        self.usage()
            .record_clear_range((begin.len() + end.len()) as u64);
    }

    /// Get the estimated byte size of the key range based on the byte sample collected by FDB
    #[cfg_api_versions(min = 630)]
    pub fn get_estimated_range_size_bytes(
        &self,
        begin: &[u8],
        end: &[u8],
    ) -> impl Future<Output = FdbResult<i64>> + Send + Sync + Unpin + use<> {
        FdbFuture::<i64>::new(unsafe {
            fdb_sys::fdb_transaction_get_estimated_range_size_bytes(
                self.inner.as_ptr(),
                begin.as_ptr(),
                fdb_len(begin.len(), "begin"),
                end.as_ptr(),
                fdb_len(end.len(), "end"),
            )
        })
    }

    /// Attempts to commit the sets and clears previously applied to the database snapshot
    /// represented by transaction to the actual database.
    ///
    /// The commit may or may not succeed – in particular, if a conflicting transaction previously
    /// committed, then the commit must fail in order to preserve transactional isolation. If the
    /// commit does succeed, the transaction is durably committed to the database and all
    /// subsequently started transactions will observe its effects.
    ///
    /// It is not necessary to commit a read-only transaction – you can simply drop it.
    ///
    /// Callers will usually want to retry a transaction if the commit or a another method on the
    /// transaction returns a retryable error (see `on_error` and/or `Database::transact`).
    ///
    /// As with other client/server databases, in some failure scenarios a client may be unable to
    /// determine whether a transaction succeeded. In these cases, `Transaction::commit` will return
    /// an error and `is_maybe_committed()` will returns true on that error. The `on_error` function
    /// treats this error as retryable, so retry loops that don’t check for `is_maybe_committed()`
    /// could execute the transaction twice. In these cases, you must consider the idempotence of
    /// the transaction. For more information, see [Transactions with unknown results](https://apple.github.io/foundationdb/developer-guide.html#developer-guide-unknown-results).
    ///
    /// Normally, commit will wait for outstanding reads to return. However, if those reads were
    /// snapshot reads or the transaction option for disabling “read-your-writes” has been invoked,
    /// any outstanding reads will immediately return errors.
    ///
    /// On an instrumented transaction, the commit ends the current attempt: its
    /// duration is recorded whatever the result, and a successful commit pushes
    /// the attempt to the metrics report as
    /// [`AttemptOutcome::Committed`](crate::metrics::AttemptOutcome::Committed).
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn commit(self) -> impl Future<Output = TransactionResult> + Send + Sync + Unpin {
        let metrics = self.metrics().cloned();
        let started_at = Instant::now();

        FdbFuture::<()>::new(unsafe { fdb_sys::fdb_transaction_commit(self.inner.as_ptr()) }).map(
            move |r| {
                if let Some(metrics) = &metrics {
                    metrics.record_commit(started_at.elapsed());
                }
                match r {
                    Ok(()) => {
                        self.end_attempt(AttemptOutcome::Committed);
                        Ok(TransactionCommitted { tr: self })
                    }
                    Err(err) => Err(TransactionCommitError { tr: self, err }),
                }
            },
        )
    }

    /// Implements the recommended retry and backoff behavior for a transaction. This function knows
    /// which of the error codes generated by other `Transaction` functions represent temporary
    /// error conditions and which represent application errors that should be handled by the
    /// application. It also implements an exponential backoff strategy to avoid swamping the
    /// database cluster with excessive retries when there is a high level of conflict between
    /// transactions.
    ///
    /// It is not necessary to call `reset()` when handling an error with `on_error()` since the
    /// transaction has already been reset.
    ///
    /// You should not call this method most of the times and use `Database::transact` which
    /// implements a retry loop strategy for you.
    ///
    /// On success the transaction enters a new attempt: its
    /// [usage](Self::attempt_usage) restarts from zero, while its
    /// [client budget](Self::set_client_budget) is kept. User versions
    /// allocated by [`Self::allocate_user_version`] are invalid for the new
    /// attempt, which starts allocating again at zero.
    pub fn on_error(
        self,
        err: FdbError,
    ) -> impl Future<Output = FdbResult<Transaction>> + Send + Sync + Unpin {
        self.mark_attempt_end();

        FdbFuture::<()>::new(unsafe {
            fdb_sys::fdb_transaction_on_error(self.inner.as_ptr(), err.code())
        })
        .map_ok(move |()| {
            self.end_attempt(AttemptOutcome::Retried { cause: err });
            self.begin_attempt_usage();
            self.reset_user_version_allocator();
            self
        })
    }

    /// Cancels the transaction. All pending or future uses of the transaction will return a
    /// transaction_cancelled error. The transaction can be used again after it is reset. User
    /// versions already allocated remain consumed until that reset starts a new attempt.
    pub fn cancel(self) -> TransactionCancelled {
        unsafe {
            fdb_sys::fdb_transaction_cancel(self.inner.as_ptr());
        }
        TransactionCancelled { tr: self }
    }

    /// Records an application metric for the current attempt, replacing any
    /// value previously recorded under the same name and labels.
    ///
    /// Custom metrics are always on, like the [usage](Self::attempt_usage)
    /// counters, and scoped to the current attempt: a retry starts from an
    /// empty set, and the values of the attempt that ended stay attached to it
    /// in the report. Recording one on a transaction nobody collects metrics
    /// from is free of consequence: the values are dropped along with the
    /// attempt.
    ///
    /// # Arguments
    /// * `name` - The name of the metric (e.g., "query_time", "cache_hits")
    /// * `value` - The value to record
    /// * `labels` - Key-value pairs for labeling the metric, allowing for dimensional metrics
    ///   (e.g., `[("operation", "read"), ("region", "us-west")]`)
    ///
    /// # Example
    /// ```
    /// # use foundationdb::*;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let db = Database::default()?;
    /// let (_, metrics) = db
    ///     .instrumented_run(|txn, _| async move {
    ///         txn.set_custom_metric("documents_indexed", 42, &[("kind", "user")]);
    ///         Ok::<_, FdbBindingError>(())
    ///     })
    ///     .await
    ///     .map_err(|(err, _)| err)?;
    ///
    /// // The values are attached to the attempt that recorded them.
    /// let attempt = metrics.attempts.last().expect("one attempt");
    /// # let _ = attempt;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, labels))
    )]
    pub fn set_custom_metric(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
        self.usage().set_custom(MetricKey::new(name, labels), value);
    }

    /// Adds `amount` to an application metric of the current attempt, starting
    /// from zero if it was not recorded yet.
    ///
    /// Same per-attempt semantics as [`set_custom_metric`](Self::set_custom_metric).
    ///
    /// # Arguments
    /// * `name` - The name of the metric to increment (e.g., "requests", "bytes_processed")
    /// * `amount` - The amount to increment the metric by
    /// * `labels` - Key-value pairs for labeling the metric, allowing for dimensional metrics
    ///   (e.g., `[("status", "success"), ("endpoint", "api/v1/users")]`)
    ///
    /// # Example
    /// ```
    /// # use foundationdb::*;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let db = Database::default()?;
    /// let txn = db.create_trx()?;
    ///
    /// txn.increment_custom_metric("cache_misses", 1, &[("cache", "user_data")]);
    /// txn.increment_custom_metric("cache_misses", 1, &[("cache", "user_data")]);
    /// // The metric of the current attempt is now 2.
    /// # Ok(())
    /// # }
    /// ```
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, labels))
    )]
    pub fn increment_custom_metric(&self, name: &str, amount: u64, labels: &[(&str, &str)]) {
        self.usage()
            .increment_custom(MetricKey::new(name, labels), amount);
    }

    /// Returns a list of public network addresses as strings, one for each of the storage servers
    /// responsible for storing key_name and its associated value.
    pub fn get_addresses_for_key(
        &self,
        key: &[u8],
    ) -> impl Future<Output = FdbResult<FdbAddresses>> + Send + Sync + Unpin + use<> {
        FdbFuture::new(unsafe {
            fdb_sys::fdb_transaction_get_addresses_for_key(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
            )
        })
    }

    /// A watch's behavior is relative to the transaction that created it. A watch will report a
    /// change in relation to the key’s value as readable by that transaction. The initial value
    /// used for comparison is either that of the transaction’s read version or the value as
    /// modified by the transaction itself prior to the creation of the watch. If the value changes
    /// and then changes back to its initial value, the watch might not report the change.
    ///
    /// Until the transaction that created it has been committed, a watch will not report changes
    /// made by other transactions. In contrast, a watch will immediately report changes made by
    /// the transaction itself. Watches cannot be created if the transaction has set the
    /// READ_YOUR_WRITES_DISABLE transaction option, and an attempt to do so will return an
    /// watches_disabled error.
    ///
    /// If the transaction used to create a watch encounters an error during commit, then the watch
    /// will be set with that error. A transaction whose commit result is unknown will set all of
    /// its watches with the commit_unknown_result error. If an uncommitted transaction is reset or
    /// destroyed, then any watches it created will be set with the transaction_cancelled error.
    ///
    /// Returns an future representing an empty value that will be set once the watch has
    /// detected a change to the value at the specified key.
    ///
    /// By default, each database connection can have no more than 10,000 watches that have not yet
    /// reported a change. When this number is exceeded, an attempt to create a watch will return a
    /// too_many_watches error. This limit can be changed using the MAX_WATCHES database option.
    /// Because a watch outlives the transaction that creates it, any watch that is no longer
    /// needed should be cancelled by dropping its future.
    pub fn watch(
        &self,
        key: &[u8],
    ) -> impl Future<Output = FdbResult<()>> + Send + Sync + Unpin + use<> {
        FdbFuture::new(unsafe {
            fdb_sys::fdb_transaction_watch(
                self.inner.as_ptr(),
                key.as_ptr(),
                fdb_len(key.len(), "key"),
            )
        })
    }

    /// Returns an FDBFuture which will be set to the approximate transaction size so far in the
    /// returned future, which is the summation of the estimated size of mutations, read conflict
    /// ranges, and write conflict ranges.
    ///
    /// This can be called multiple times before the transaction is committed.
    #[cfg_api_versions(min = 620)]
    pub fn get_approximate_size(
        &self,
    ) -> impl Future<Output = FdbResult<i64>> + Send + Sync + Unpin + use<> {
        FdbFuture::new(unsafe {
            fdb_sys::fdb_transaction_get_approximate_size(self.inner.as_ptr())
        })
    }

    /// Gets a list of keys that can split the given range into (roughly) equally sized chunks based on chunk_size.
    /// Note: the returned split points contain the start key and end key of the given range.
    #[cfg_api_versions(min = 700)]
    pub fn get_range_split_points(
        &self,
        begin: &[u8],
        end: &[u8],
        chunk_size: i64,
    ) -> impl Future<Output = FdbResult<FdbKeys>> + Send + Sync + Unpin + use<> {
        FdbFuture::<FdbKeys>::new(unsafe {
            fdb_sys::fdb_transaction_get_range_split_points(
                self.inner.as_ptr(),
                begin.as_ptr(),
                fdb_len(begin.len(), "begin"),
                end.as_ptr(),
                fdb_len(end.len(), "end"),
                chunk_size,
            )
        })
    }

    /// Returns an FDBFuture which will be set to the versionstamp which was used by any
    /// versionstamp operations in this transaction.
    ///
    /// The future will be ready only after the successful completion of a call to `commit()` on
    /// this Transaction. Read-only transactions do not modify the database when committed and will
    /// result in the future completing with an error. Keep in mind that a transaction which reads
    /// keys and then sets them to their current values may be optimized to a read-only transaction.
    ///
    /// Most applications will not call this function.
    pub fn get_versionstamp(
        &self,
    ) -> impl Future<Output = FdbResult<FdbSlice>> + Send + Sync + Unpin + use<> {
        FdbFuture::new(unsafe { fdb_sys::fdb_transaction_get_versionstamp(self.inner.as_ptr()) })
    }

    /// The transaction obtains a snapshot read version automatically at the time of the first call
    /// to `get_*()` (including this one) and (unless causal consistency has been deliberately
    /// compromised by transaction options) is guaranteed to represent all transactions which were
    /// reported committed before that call.
    ///
    /// On an instrumented transaction, the version is recorded in the metrics of
    /// the current attempt. It is only ever recorded when this method is called:
    /// the binding never fetches a read version on its own.
    pub fn get_read_version(
        &self,
    ) -> impl Future<Output = FdbResult<i64>> + Send + Sync + Unpin + use<> {
        let metrics = self.metrics().cloned();
        let started_at = metrics.as_ref().map(|_| Instant::now());

        FdbFuture::<i64>::new(unsafe {
            fdb_sys::fdb_transaction_get_read_version(self.inner.as_ptr())
        })
        .map(move |result| {
            if let (Some(metrics), Some(started_at)) = (&metrics, started_at) {
                metrics.record_grv(started_at.elapsed());
                if let Ok(version) = result {
                    metrics.set_read_version(version);
                }
            }
            result
        })
    }

    /// Sets the snapshot read version used by a transaction.
    ///
    /// This is not needed in simple cases.
    /// If the given version is too old, subsequent reads will fail with error_code_past_version;
    /// if it is too new, subsequent reads may be delayed indefinitely and/or fail with
    /// error_code_future_version. If any of get_*() have been called on this transaction already,
    /// the result is undefined.
    pub fn set_read_version(&self, version: i64) {
        unsafe { fdb_sys::fdb_transaction_set_read_version(self.inner.as_ptr(), version) }
    }

    /// The metadata version key `\xff/metadataVersion` is a key intended to help layers deal with hot keys.
    /// The value of this key is sent to clients along with the read version from the proxy,
    /// so a client can read its value without communicating with a storage server.
    /// To retrieve the metadataVersion, you need to set `TransactionOption::ReadSystemKeys`
    #[cfg_api_versions(min = 610)]
    pub async fn get_metadata_version(&self, snapshot: bool) -> FdbResult<Option<i64>> {
        match self.get(METADATA_VERSION_KEY, snapshot).await {
            Ok(Some(fdb_slice)) => {
                let value = fdb_slice.deref();
                // as we cannot write the metadata-key directly(we must mutate with an atomic_op),
                // can we assume that it will always be the correct size?
                if value.len() < 8 {
                    return Ok(None);
                }

                // The 80-bits versionstamps are 10 bytes longs, and are composed of:
                // * 8 bytes (Transaction Version)
                // * followed by 2 bytes (Transaction Batch Order)
                // More details can be found here: https://forums.foundationdb.org/t/implementing-versionstamps-in-bindings/250
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&value[0..8]);
                let transaction_version: i64 = i64::from_be_bytes(arr);

                Ok(Some(transaction_version))
            }
            Ok(None) => Ok(None),

            Err(err) => Err(err),
        }
    }

    #[cfg_api_versions(min = 610)]
    pub fn update_metadata_version(&self) {
        // The param is transformed by removing the final four bytes from ``param`` and reading
        // those as a little-Endian 32-bit integer to get a position ``pos``.
        // The 10 bytes of the parameter from ``pos`` to ``pos + 10`` are replaced with the
        // versionstamp of the transaction used. The first byte of the parameter is position 0.
        // As we only have the metadata value, we can just create an 14-bytes Vec filled with 0u8.
        let param = vec![0u8; 14];
        self.atomic_op(
            METADATA_VERSION_KEY,
            param.as_slice(),
            options::MutationType::SetVersionstampedValue,
        )
    }

    /// Reset transaction to its initial state.
    ///
    /// In order to protect against a race condition with cancel(), this call require a mutable
    /// access to the transaction.
    ///
    /// This is similar to dropping the transaction and creating a new one.
    ///
    /// It is not necessary to call `reset()` when handling an error with `on_error()` since the
    /// transaction has already been reset.
    ///
    /// This starts a new attempt: the [usage](Self::attempt_usage) restarts from
    /// zero, while the [client budget](Self::set_client_budget) is kept. On an
    /// instrumented transaction, the attempt being recorded is abandoned rather
    /// than reported: it reached no conclusion. User versions allocated before
    /// reset are invalid for the new attempt, which starts allocating again at
    /// zero.
    pub fn reset(&mut self) {
        unsafe { fdb_sys::fdb_transaction_reset(self.inner.as_ptr()) }
        self.begin_attempt_usage();
        self.reset_user_version_allocator();
    }

    fn reset_user_version_allocator(&self) {
        self.user_version.store(0, Ordering::Relaxed);
    }

    /// Reads the conflicting key ranges from the special keyspace after a commit conflict.
    ///
    /// This method reads from `\xff\xff/transaction/conflicting_keys/` and parses the
    /// boundary encoding where `b"1"` marks range starts and `b"0"` marks range ends.
    ///
    /// The special keyspace read is resolved client-side — no network round-trip to the
    /// cluster. The future still goes through the FDB network thread event loop, but the
    /// data comes from an in-memory map populated during the commit response. Returns
    /// an empty `Vec` if
    /// [`TransactionOption::ReportConflictingKeys`](crate::options::TransactionOption::ReportConflictingKeys)
    /// was not set.
    ///
    /// Requires API version 630 or later: the special keyspace does not exist on
    /// older versions, where this read fails.
    ///
    /// Only complete ranges are returned. A begin marker whose end marker never
    /// arrives is dropped, identically in debug and release builds.
    ///
    /// # Errors
    ///
    /// Returns an `FdbError` if the special keyspace read fails.
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub async fn conflicting_keys(&self) -> FdbResult<Vec<ConflictingKeyRange>> {
        self.read_conflict_range_keyspace(CONFLICTING_KEYS_PREFIX, CONFLICTING_KEYS_END)
            .await
    }

    /// Reads the read conflict ranges accumulated by this transaction so far.
    ///
    /// These are the ranges the resolver will check for conflicts at commit
    /// time: every key read by the transaction, plus the ranges added with
    /// [`add_conflict_range`](Self::add_conflict_range). A point read of `k`
    /// shows up as `k..k\x00`.
    ///
    /// <div class="warning">
    /// Reading this keyspace waits for every pending read of the transaction to
    /// settle, since a read that has not returned yet has not registered its
    /// conflict range. Called in the middle of a batch of concurrent reads, it
    /// is therefore as slow as the slowest of them.
    /// </div>
    ///
    /// The read itself is performed by the binding on your behalf: it is not
    /// accounted in the [attempt usage](Self::attempt_usage) and does not
    /// consume the [client budget](Self::set_client_budget).
    ///
    /// Only complete ranges are returned, see [`conflicting_keys`](Self::conflicting_keys).
    ///
    /// # Errors
    ///
    /// Returns an `FdbError` if the special keyspace read fails.
    #[cfg_api_versions(min = 630)]
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub async fn read_conflict_ranges(&self) -> FdbResult<Vec<ConflictRange>> {
        self.read_conflict_range_keyspace(READ_CONFLICT_RANGE_PREFIX, READ_CONFLICT_RANGE_END)
            .await
    }

    /// Reads the write conflict ranges accumulated by this transaction so far.
    ///
    /// These are the ranges the transaction will conflict *other* transactions
    /// on: every key it wrote, plus the ranges added with
    /// [`add_conflict_range`](Self::add_conflict_range). A single `set` of `k`
    /// shows up as `k..k\x00`.
    ///
    /// <div class="warning">
    /// Before the commit, this is an approximate <em>superset</em> of the final
    /// write conflict ranges when the transaction uses versionstamped keys: the
    /// versionstamp is only known at commit time, so the incomplete key is
    /// reported with its placeholder bytes.
    /// </div>
    ///
    /// The read itself is performed by the binding on your behalf: it is not
    /// accounted in the [attempt usage](Self::attempt_usage) and does not
    /// consume the [client budget](Self::set_client_budget).
    ///
    /// Only complete ranges are returned, see [`conflicting_keys`](Self::conflicting_keys).
    ///
    /// # Errors
    ///
    /// Returns an `FdbError` if the special keyspace read fails.
    #[cfg_api_versions(min = 630)]
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub async fn write_conflict_ranges(&self) -> FdbResult<Vec<ConflictRange>> {
        self.read_conflict_range_keyspace(WRITE_CONFLICT_RANGE_PREFIX, WRITE_CONFLICT_RANGE_END)
            .await
    }

    /// Reads one conflict-range special keyspace to its end and parses it.
    ///
    /// The reads go through the unmetered path: they are performed by the
    /// binding itself and must not show up in the user's usage or budget. The
    /// keyspace is paginated because a marker pair can straddle two batches,
    /// which [`ConflictRangeParser`] handles by carrying the open begin marker
    /// from one batch to the next.
    async fn read_conflict_range_keyspace(
        &self,
        prefix: &[u8],
        end: &[u8],
    ) -> FdbResult<Vec<ConflictRange>> {
        let mut parser = ConflictRangeParser::new(prefix);
        let mut next = Some(RangeOption::from((prefix, end)));
        let mut iteration = 1;

        while let Some(opt) = next {
            let values = self.get_range_unmetered(&opt, iteration, false).await?;
            parser.feed(values.iter().map(|kv| (kv.key(), kv.value())));
            next = opt.next_range(&values);
            iteration += 1;
        }

        Ok(parser.finish())
    }

    /// Adds a conflict range to a transaction without performing the associated read or write.
    ///
    /// # Note
    ///
    /// Most applications will use the serializable isolation that transactions provide by default
    /// and will not need to manipulate conflict ranges.
    pub fn add_conflict_range(
        &self,
        begin: &[u8],
        end: &[u8],
        ty: options::ConflictRangeType,
    ) -> FdbResult<()> {
        error::eval(unsafe {
            fdb_sys::fdb_transaction_add_conflict_range(
                self.inner.as_ptr(),
                begin.as_ptr(),
                fdb_len(begin.len(), "begin"),
                end.as_ptr(),
                fdb_len(end.len(), "end"),
                ty.code(),
            )
        })
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        unsafe {
            fdb_sys::fdb_transaction_destroy(self.inner.as_ptr());
        }
    }
}

/// A retryable transaction, generated by Database.run
#[derive(Clone)]
pub struct RetryableTransaction {
    inner: Arc<Transaction>,
}

impl Deref for RetryableTransaction {
    type Target = Transaction;
    fn deref(&self) -> &Transaction {
        self.inner.deref()
    }
}

impl RetryableTransaction {
    pub(crate) fn new(t: Transaction) -> RetryableTransaction {
        RetryableTransaction { inner: Arc::new(t) }
    }

    pub(crate) fn take(self) -> Result<Transaction, FdbBindingError> {
        // checking weak references
        if Arc::weak_count(&self.inner) != 0 {
            return Err(FdbBindingError::ReferenceToTransactionKept);
        }
        Arc::try_unwrap(self.inner).map_err(|_| FdbBindingError::ReferenceToTransactionKept)
    }

    pub(crate) async fn on_error(
        self,
        err: FdbError,
    ) -> Result<Result<RetryableTransaction, FdbError>, FdbBindingError> {
        Ok(self
            .take()?
            .on_error(err)
            .await
            .map(RetryableTransaction::new))
    }

    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub(crate) async fn commit(
        self,
    ) -> Result<Result<TransactionCommitted, TransactionCommitError>, FdbBindingError> {
        Ok(self.take()?.commit().await)
    }
}

#[cfg(test)]
mod tests {
    use super::{CONFLICTING_KEYS_PREFIX, ConflictRangeParser};

    /// Builds a marker as the special keyspace returns it: the prefix, the key
    /// it is a boundary of, and the `1`/`0` value.
    fn marker(key: &str, value: &'static [u8]) -> (Vec<u8>, &'static [u8]) {
        let mut full = CONFLICTING_KEYS_PREFIX.to_vec();
        full.extend_from_slice(key.as_bytes());
        (full, value)
    }

    fn feed(parser: &mut ConflictRangeParser, batch: &[(Vec<u8>, &'static [u8])]) {
        parser.feed(batch.iter().map(|(k, v)| (k.as_slice(), *v)));
    }

    #[test]
    fn parses_ranges_within_one_batch() {
        let mut parser = ConflictRangeParser::new(CONFLICTING_KEYS_PREFIX);
        feed(
            &mut parser,
            &[
                marker("a", b"1"),
                marker("b", b"0"),
                marker("c", b"1"),
                marker("d", b"0"),
            ],
        );

        let ranges = parser.finish();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].begin(), b"a");
        assert_eq!(ranges[0].end(), b"b");
        assert_eq!(ranges[1].begin(), b"c");
        assert_eq!(ranges[1].end(), b"d");
    }

    /// The whole point of the incremental parser: a begin marker in one batch
    /// and its end marker in the next one still make a single range.
    #[test]
    fn open_range_survives_across_batches() {
        let mut parser = ConflictRangeParser::new(CONFLICTING_KEYS_PREFIX);
        feed(&mut parser, &[marker("a", b"1")]);
        feed(&mut parser, &[marker("b", b"0"), marker("c", b"1")]);
        feed(&mut parser, &[marker("d", b"0")]);

        let ranges = parser.finish();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].begin(), b"a");
        assert_eq!(ranges[0].end(), b"b");
        assert_eq!(ranges[1].begin(), b"c");
        assert_eq!(ranges[1].end(), b"d");
    }

    #[test]
    fn empty_input_yields_no_range() {
        let mut parser = ConflictRangeParser::new(CONFLICTING_KEYS_PREFIX);
        feed(&mut parser, &[]);
        assert!(parser.finish().is_empty());
    }

    /// A begin marker whose end marker never arrives is dropped, in every build
    /// profile: the guarantee is that only complete ranges come out.
    #[test]
    fn unmatched_begin_at_end_of_stream_is_dropped() {
        let mut parser = ConflictRangeParser::new(CONFLICTING_KEYS_PREFIX);
        feed(
            &mut parser,
            &[marker("a", b"1"), marker("b", b"0"), marker("c", b"1")],
        );

        let ranges = parser.finish();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].begin(), b"a");
        assert_eq!(ranges[0].end(), b"b");
    }

    /// A begin marker arriving while one is still open replaces it: the range
    /// the dropped marker opened is never emitted.
    #[test]
    fn a_second_begin_marker_replaces_the_open_one() {
        let mut parser = ConflictRangeParser::new(CONFLICTING_KEYS_PREFIX);
        feed(
            &mut parser,
            &[marker("a", b"1"), marker("b", b"1"), marker("c", b"0")],
        );

        let ranges = parser.finish();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].begin(), b"b");
        assert_eq!(ranges[0].end(), b"c");
    }

    /// Values that are neither `1` nor `0` are not boundaries and are ignored.
    #[test]
    fn unknown_marker_values_are_ignored() {
        let mut parser = ConflictRangeParser::new(CONFLICTING_KEYS_PREFIX);
        feed(
            &mut parser,
            &[marker("a", b"1"), marker("x", b"?"), marker("b", b"0")],
        );

        let ranges = parser.finish();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].begin(), b"a");
        assert_eq!(ranges[0].end(), b"b");
    }
}
