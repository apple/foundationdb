// Copyright 2026 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! The retry runner behind [`Database::run`](crate::Database::run).
//!
//! The runner is built from three independent pieces:
//!
//! * the **attempt**: run the closure on a transaction, then commit it. One
//!   attempt does exactly one pass and never retries.
//! * the **hooks** ([`RunnerHooks`]): purely observational callbacks fired
//!   around each attempt. They cannot change what the runner decides.
//! * the **policy** ([`RetryPolicy`]): the only thing that can change a
//!   decision. It sees the failure and the decision the runner proposes, and
//!   returns the decision to apply.
//!
//! They are assembled by [`TransactionRunner`], the builder returned by
//! [`Database::runner`](crate::Database::runner):
//!
//! ```no_run
//! # use foundationdb::*;
//! # use foundationdb::runner::MetricsHooks;
//! # async fn example(db: &Database) -> Result<(), FdbBindingError> {
//! let metrics = TransactionMetrics::new();
//! db.runner()
//!     .hooks(&MetricsHooks::new(&metrics))
//!     .run(|trx, _maybe_committed| async move {
//!         trx.set(b"key", b"value");
//!         Ok::<_, FdbBindingError>(())
//!     })
//!     .await?;
//! let report = metrics.get_metrics_data();
//! # let _ = report;
//! # Ok(())
//! # }
//! ```
//!
//! # Retry semantics
//!
//! `fdb_transaction_on_error` remains the single retry governor: backoff, max
//! retry delay and [`TransactionOption::RetryLimit`](crate::options::TransactionOption::RetryLimit)
//! are applied by the C API, never by a Rust-side budget. A closure error asking
//! for a retry without a native error underneath
//! ([`RetryDecision::Retry`]) is routed through `on_error` with code 1020
//! (`not_committed`) so that it obeys the same budget.

use std::time::{Duration, Instant};

use crate::database::{Database, MaybeCommitted};
use crate::error::{FdbBindingError, FdbError, FdbResult, RetryDecision, RetryableError};
use crate::metrics::{AttemptOutcome, ConflictKeys, TransactionMetrics};
use crate::transaction::{
    RetryableTransaction, Transaction, TransactionCommitError, TransactionCommitted,
};

/// `not_committed`: the retryable error code a [`RetryDecision::Retry`] is
/// routed through.
const NOT_COMMITTED: i32 = 1020;

/// Observation points of the retry runner.
///
/// Hooks are **purely observational**: nothing they do or return changes what
/// the runner decides. Use a [`RetryPolicy`] to influence retries. Every method
/// has a no-op default, so an implementation only overrides what it needs.
///
/// # Ordering
///
/// Every callback receives the index of the attempt it belongs to, starting at
/// `0` and matching [`AttemptMetrics::index`](crate::metrics::AttemptMetrics::index).
/// Per attempt, in order:
///
/// 1. [`on_attempt_start`](Self::on_attempt_start)
/// 2. the closure runs
/// 3. if the closure succeeded: [`before_commit`](Self::before_commit), then
///    the commit
///    * commit succeeded: [`on_commit_success`](Self::on_commit_success), the
///      run is over
///    * commit failed: [`on_commit_error`](Self::on_commit_error), then
///      `on_error`, then [`on_error_duration`](Self::on_error_duration) and
///      [`on_retry`](Self::on_retry) if the transaction was retried
/// 4. if the closure failed: [`on_closure_error`](Self::on_closure_error) (only
///    when the error maps to an [`FdbError`], that is when the runner is about
///    to call `on_error`), then `on_error`, then
///    [`on_error_duration`](Self::on_error_duration) and
///    [`on_retry`](Self::on_retry) if the transaction was retried
///
/// [`on_complete`](Self::on_complete) fires exactly once per run, on **every**
/// exit path: success, non-retryable error, exhausted retries or binding error.
///
/// The two fallible hooks ([`before_commit`](Self::before_commit) and
/// [`on_commit_error`](Self::on_commit_error)) never abort the runner: their
/// error is reported to [`on_hook_error`](Self::on_hook_error) and the run
/// continues exactly as if they had succeeded.
///
/// # Transaction access
///
/// Hooks see a `&Transaction`, never the clonable
/// [`RetryableTransaction`]: a hook that could
/// keep a reference to it would make the runner fail with
/// [`FdbBindingError::ReferenceToTransactionKept`].
///
/// # Composition
///
/// [`RunnerHooks`] is implemented for `()` (no-op), `&H`, `Option<H>` (`None`
/// is a no-op) and `(A, B)`, which nest for any arity: `(a, (b, c))`. In a
/// tuple, callbacks fire left to right, and both sides always run even if the
/// left one returned an error, the first error being the one reported.
pub trait RunnerHooks {
    /// Called when an attempt starts, before the closure runs.
    fn on_attempt_start(&self, _trx: &Transaction, _attempt: usize) {}

    /// Called after the closure succeeded, before the transaction is committed.
    ///
    /// This is the last point where the transaction can be read or written
    /// inside the attempt. Returning an error does **not** abort the commit: it
    /// is reported through [`on_hook_error`](Self::on_hook_error) and the
    /// transaction is committed anyway.
    fn before_commit(
        &self,
        _trx: &Transaction,
        _attempt: usize,
    ) -> impl Future<Output = FdbResult<()>> + Send {
        async { Ok(()) }
    }

    /// Called with the error of a hook that failed, right after it failed.
    fn on_hook_error(&self, _err: &FdbError, _attempt: usize) {}

    /// Called after a successful commit, with the time the commit took.
    fn on_commit_success(
        &self,
        _committed: &TransactionCommitted,
        _commit_duration_ms: u64,
        _attempt: usize,
    ) {
    }

    /// Called when the commit failed, **before** `on_error` resets the
    /// transaction. This is the only window to read conflicting keys or inspect
    /// the state of the failed attempt.
    ///
    /// It fires whatever the runner does next, including when the
    /// [`RetryPolicy`] turns the failure into a fatal one. An error returned
    /// here is reported through [`on_hook_error`](Self::on_hook_error) and
    /// changes nothing else.
    fn on_commit_error(
        &self,
        _err: &TransactionCommitError,
        _attempt: usize,
    ) -> impl Future<Output = FdbResult<()>> + Send {
        async { Ok(()) }
    }

    /// Called when a closure error is about to be handed to `on_error`.
    ///
    /// A closure error classified as [`RetryDecision::Retry`] surfaces here as
    /// a synthetic `FdbError` with code 1020 (`not_committed`). A fatal error
    /// never reaches this hook.
    fn on_closure_error(&self, _err: &FdbError, _attempt: usize) {}

    /// Called after `on_error` returned, with the time it took, backoff
    /// included.
    fn on_error_duration(&self, _duration_ms: u64, _attempt: usize) {}

    /// Called when the attempt is about to be retried, that is once `on_error`
    /// accepted to retry it.
    fn on_retry(&self, _attempt: usize) {}

    /// Called exactly once when the run ends, whatever its outcome.
    fn on_complete(&self) {}
}

impl RunnerHooks for () {}

impl<H: RunnerHooks + Sync> RunnerHooks for &H {
    fn on_attempt_start(&self, trx: &Transaction, attempt: usize) {
        (*self).on_attempt_start(trx, attempt)
    }

    fn before_commit(
        &self,
        trx: &Transaction,
        attempt: usize,
    ) -> impl Future<Output = FdbResult<()>> + Send {
        (*self).before_commit(trx, attempt)
    }

    fn on_hook_error(&self, err: &FdbError, attempt: usize) {
        (*self).on_hook_error(err, attempt)
    }

    fn on_commit_success(
        &self,
        committed: &TransactionCommitted,
        commit_duration_ms: u64,
        attempt: usize,
    ) {
        (*self).on_commit_success(committed, commit_duration_ms, attempt)
    }

    fn on_commit_error(
        &self,
        err: &TransactionCommitError,
        attempt: usize,
    ) -> impl Future<Output = FdbResult<()>> + Send {
        (*self).on_commit_error(err, attempt)
    }

    fn on_closure_error(&self, err: &FdbError, attempt: usize) {
        (*self).on_closure_error(err, attempt)
    }

    fn on_error_duration(&self, duration_ms: u64, attempt: usize) {
        (*self).on_error_duration(duration_ms, attempt)
    }

    fn on_retry(&self, attempt: usize) {
        (*self).on_retry(attempt)
    }

    fn on_complete(&self) {
        (*self).on_complete()
    }
}

impl<H: RunnerHooks + Sync> RunnerHooks for Option<H> {
    fn on_attempt_start(&self, trx: &Transaction, attempt: usize) {
        if let Some(hooks) = self {
            hooks.on_attempt_start(trx, attempt);
        }
    }

    async fn before_commit(&self, trx: &Transaction, attempt: usize) -> FdbResult<()> {
        match self {
            Some(hooks) => hooks.before_commit(trx, attempt).await,
            None => Ok(()),
        }
    }

    fn on_hook_error(&self, err: &FdbError, attempt: usize) {
        if let Some(hooks) = self {
            hooks.on_hook_error(err, attempt);
        }
    }

    fn on_commit_success(
        &self,
        committed: &TransactionCommitted,
        commit_duration_ms: u64,
        attempt: usize,
    ) {
        if let Some(hooks) = self {
            hooks.on_commit_success(committed, commit_duration_ms, attempt);
        }
    }

    async fn on_commit_error(&self, err: &TransactionCommitError, attempt: usize) -> FdbResult<()> {
        match self {
            Some(hooks) => hooks.on_commit_error(err, attempt).await,
            None => Ok(()),
        }
    }

    fn on_closure_error(&self, err: &FdbError, attempt: usize) {
        if let Some(hooks) = self {
            hooks.on_closure_error(err, attempt);
        }
    }

    fn on_error_duration(&self, duration_ms: u64, attempt: usize) {
        if let Some(hooks) = self {
            hooks.on_error_duration(duration_ms, attempt);
        }
    }

    fn on_retry(&self, attempt: usize) {
        if let Some(hooks) = self {
            hooks.on_retry(attempt);
        }
    }

    fn on_complete(&self) {
        if let Some(hooks) = self {
            hooks.on_complete();
        }
    }
}

impl<A: RunnerHooks + Sync, B: RunnerHooks + Sync> RunnerHooks for (A, B) {
    fn on_attempt_start(&self, trx: &Transaction, attempt: usize) {
        self.0.on_attempt_start(trx, attempt);
        self.1.on_attempt_start(trx, attempt);
    }

    async fn before_commit(&self, trx: &Transaction, attempt: usize) -> FdbResult<()> {
        let first = self.0.before_commit(trx, attempt).await;
        let second = self.1.before_commit(trx, attempt).await;
        first.and(second)
    }

    fn on_hook_error(&self, err: &FdbError, attempt: usize) {
        self.0.on_hook_error(err, attempt);
        self.1.on_hook_error(err, attempt);
    }

    fn on_commit_success(
        &self,
        committed: &TransactionCommitted,
        commit_duration_ms: u64,
        attempt: usize,
    ) {
        self.0
            .on_commit_success(committed, commit_duration_ms, attempt);
        self.1
            .on_commit_success(committed, commit_duration_ms, attempt);
    }

    async fn on_commit_error(&self, err: &TransactionCommitError, attempt: usize) -> FdbResult<()> {
        let first = self.0.on_commit_error(err, attempt).await;
        let second = self.1.on_commit_error(err, attempt).await;
        first.and(second)
    }

    fn on_closure_error(&self, err: &FdbError, attempt: usize) {
        self.0.on_closure_error(err, attempt);
        self.1.on_closure_error(err, attempt);
    }

    fn on_error_duration(&self, duration_ms: u64, attempt: usize) {
        self.0.on_error_duration(duration_ms, attempt);
        self.1.on_error_duration(duration_ms, attempt);
    }

    fn on_retry(&self, attempt: usize) {
        self.0.on_retry(attempt);
        self.1.on_retry(attempt);
    }

    fn on_complete(&self) {
        self.0.on_complete();
        self.1.on_complete();
    }
}

/// Hooks collecting a [`MetricsReport`](crate::metrics::MetricsReport) into a
/// [`TransactionMetrics`].
///
/// They are what [`Database::instrumented_run`](crate::Database::instrumented_run)
/// is made of, and can be stacked on any other hooks to get the same report out
/// of a plain [`run`](crate::Database::run):
///
/// ```no_run
/// # use foundationdb::*;
/// # use foundationdb::runner::MetricsHooks;
/// # struct MyHooks;
/// # impl foundationdb::runner::RunnerHooks for MyHooks {}
/// # async fn example(db: &Database) -> Result<(), FdbBindingError> {
/// let metrics = TransactionMetrics::new();
/// db.run_with_hooks(&(MetricsHooks::new(&metrics), MyHooks), |trx, _| async move {
///     trx.set(b"key", b"value");
///     Ok::<_, FdbBindingError>(())
/// })
/// .await?;
///
/// let report = metrics.get_metrics_data();
/// # let _ = report;
/// # Ok(())
/// # }
/// ```
///
/// The run is timed from the construction of the hooks, so build one per run.
///
/// The collector is attached to the transaction when the first attempt starts,
/// which is what makes the counters of a plain `run` land in the report. A
/// transaction only ever reports to one collector: stacking two `MetricsHooks`
/// leaves the second one empty.
pub struct MetricsHooks {
    metrics: TransactionMetrics,
    start: Instant,
}

impl MetricsHooks {
    /// Collects the metrics of a run into `metrics`.
    ///
    /// The handle is cloned, so the caller keeps reading the report from its
    /// own [`TransactionMetrics`] once the run is over.
    pub fn new(metrics: &TransactionMetrics) -> Self {
        Self {
            metrics: metrics.clone(),
            start: Instant::now(),
        }
    }
}

impl RunnerHooks for MetricsHooks {
    fn on_attempt_start(&self, trx: &Transaction, _attempt: usize) {
        // Wires the collector onto the transaction the runner created, which is
        // what makes the counters of every attempt land in the report. A no-op
        // from the second attempt on: the transaction keeps its collector
        // across retries.
        trx.attach_metrics(&self.metrics);
    }

    async fn on_commit_error(
        &self,
        err: &TransactionCommitError,
        _attempt: usize,
    ) -> FdbResult<()> {
        // not_committed (1020) = commit conflict
        if err.code() == NOT_COMMITTED {
            self.metrics.increment_conflict_count();
        }
        // Reading from the \xff\xff/transaction/conflicting_keys/ special keyspace is
        // resolved client-side — no network round-trip to the cluster. The future still
        // goes through the FDB network thread event loop, but the data comes from an
        // in-memory map populated during the commit response. Returns empty if
        // ReportConflictingKeys was not set.
        match err.conflicting_keys().await {
            Ok(keys) => {
                self.metrics
                    .set_conflicting_keys(ConflictKeys::Available(keys));
                Ok(())
            }
            Err(read_error) => {
                self.metrics
                    .set_conflicting_keys(ConflictKeys::ReadFailed(read_error));
                Err(read_error)
            }
        }
    }

    fn on_error_duration(&self, duration_ms: u64, _attempt: usize) {
        // `on_error` already pushed the attempt it ended, so this lands on the
        // last attempt of the report.
        self.metrics
            .set_on_error_duration(Duration::from_millis(duration_ms));
    }

    fn on_commit_success(
        &self,
        committed: &TransactionCommitted,
        _commit_duration_ms: u64,
        _attempt: usize,
    ) {
        // The attempt itself was pushed by `Transaction::commit`, which also
        // recorded how long the commit took.
        if let Ok(version) = committed.committed_version() {
            self.metrics.set_commit_version(version);
        }
    }

    fn on_complete(&self) {
        // An attempt still open when the run ends is the one that failed:
        // attempts that committed or were retried were pushed by the
        // transaction itself, which makes this a no-op for them.
        self.metrics.finish_attempt(AttemptOutcome::Failed);
        self.metrics.set_total_duration(self.start.elapsed());
    }
}

/// The failure a [`RetryPolicy`] is asked about.
#[derive(Debug)]
#[non_exhaustive]
pub enum AttemptFailure<'a, E> {
    /// The closure returned an error, the transaction was not committed.
    Closure(&'a E),
    /// The closure succeeded but the commit failed.
    Commit(&'a TransactionCommitError),
}

/// Decides what the runner does with a failed attempt.
///
/// The runner classifies the failure on its own and passes its verdict as
/// `proposed`; the policy returns the decision to apply, which is `proposed`
/// itself unless it wants to override it:
///
/// * [`RetryDecision::Fatal`] ends the run and returns the original error, the
///   one the closure produced or the commit error, never something the policy
///   made up.
/// * [`RetryDecision::Fdb`] hands that error to `fdb_transaction_on_error`,
///   which judges retryability and applies the backoff.
/// * [`RetryDecision::Retry`] does the same with code 1020 (`not_committed`),
///   which is retryable by definition.
///
/// A policy cannot corrupt the
/// [`MaybeCommitted`] flag handed to the closure: it is
/// computed from the original error before the policy is consulted.
///
/// # Example: cap the number of attempts
///
/// ```
/// use foundationdb::runner::{AttemptFailure, RetryPolicy};
/// use foundationdb::{FdbBindingError, RetryDecision};
///
/// struct MaxAttempts(usize);
///
/// impl RetryPolicy<FdbBindingError> for MaxAttempts {
///     fn decide(
///         &self,
///         _failure: AttemptFailure<'_, FdbBindingError>,
///         proposed: RetryDecision,
///         attempt: usize,
///     ) -> RetryDecision {
///         if attempt + 1 >= self.0 {
///             RetryDecision::Fatal
///         } else {
///             proposed
///         }
///     }
/// }
/// ```
pub trait RetryPolicy<E> {
    /// Returns the decision to apply for `failure`, `proposed` being what the
    /// runner would do on its own.
    fn decide(
        &self,
        failure: AttemptFailure<'_, E>,
        proposed: RetryDecision,
        attempt: usize,
    ) -> RetryDecision;
}

/// The default policy: it always applies what the runner proposes, leaving
/// `fdb_transaction_on_error` the last word on retries.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativeRetryPolicy;

impl<E> RetryPolicy<E> for NativeRetryPolicy {
    fn decide(
        &self,
        _failure: AttemptFailure<'_, E>,
        proposed: RetryDecision,
        _attempt: usize,
    ) -> RetryDecision {
        proposed
    }
}

/// How a single attempt ended, carrying whatever still owns the transaction.
enum Attempt<T, E> {
    /// The closure succeeded and the transaction committed.
    Committed {
        committed: TransactionCommitted,
        value: T,
        commit_duration_ms: u64,
    },
    /// The closure failed, the transaction is untouched and can be retried.
    ClosureFailed {
        transaction: RetryableTransaction,
        error: E,
    },
    /// The commit failed, the transaction is owned by the error.
    CommitFailed(TransactionCommitError),
    /// The runner could not get the transaction back from the closure.
    BindingFailed(FdbBindingError),
}

/// Runs the closure once and commits, without any retry logic.
///
/// The closure future is built by the caller so that the runner keeps owning
/// the closure itself: an attempt that borrowed it would make every `run`
/// future require `F: Sync`.
async fn run_attempt<Fut, T, E, H>(
    transaction: RetryableTransaction,
    hooks: &H,
    closure_result: Fut,
    attempt: usize,
) -> Attempt<T, E>
where
    Fut: Future<Output = Result<T, E>>,
    H: RunnerHooks,
{
    hooks.on_attempt_start(&transaction, attempt);

    let value = match closure_result.await {
        Ok(value) => value,
        Err(error) => return Attempt::ClosureFailed { transaction, error },
    };

    // Observational: a failing hook is reported, the commit happens anyway.
    if let Err(hook_error) = hooks.before_commit(&transaction, attempt).await {
        hooks.on_hook_error(&hook_error, attempt);
    }

    let now_commit = Instant::now();
    match transaction.commit().await {
        Ok(Ok(committed)) => Attempt::Committed {
            committed,
            value,
            commit_duration_ms: now_commit.elapsed().as_millis() as u64,
        },
        Ok(Err(commit_error)) => Attempt::CommitFailed(commit_error),
        Err(binding_error) => Attempt::BindingFailed(binding_error),
    }
}

/// The retry loop: classification, policy, `on_error` and backoff on top of
/// [`run_attempt`].
///
/// Wrapped by [`run_with_hooks`], which owns firing
/// [`RunnerHooks::on_complete`] on every exit path.
async fn retry_loop<F, Fut, T, E, H, P>(
    initial_transaction: RetryableTransaction,
    hooks: &H,
    policy: &P,
    closure: F,
) -> Result<T, E>
where
    F: Fn(RetryableTransaction, MaybeCommitted) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: RetryableError,
    H: RunnerHooks,
    P: RetryPolicy<E>,
{
    let mut maybe_committed = false;
    let mut transaction = initial_transaction;
    let mut attempt: usize = 0;

    loop {
        let closure_result = closure(transaction.clone(), MaybeCommitted::new(maybe_committed));

        let (retried_transaction, _fdb_err) =
            match run_attempt(transaction, hooks, closure_result, attempt).await {
                Attempt::Committed {
                    committed,
                    value,
                    commit_duration_ms,
                } => {
                    hooks.on_commit_success(&committed, commit_duration_ms, attempt);

                    #[cfg(feature = "trace")]
                    tracing::info!(attempt, "success, returning result");

                    return Ok(value);
                }

                Attempt::BindingFailed(binding_error) => {
                    #[cfg(feature = "trace")]
                    tracing::error!(attempt, "transaction reference kept, aborting transaction");

                    return Err(E::from(binding_error));
                }

                Attempt::ClosureFailed { transaction, error } => {
                    let proposed = error.retry_decision();
                    // maybe_committed is computed from the original error only,
                    // before the policy is consulted: a previous maybe-committed
                    // attempt must stay visible to the closure whatever the
                    // policy decides. The synthetic 1020 of `Retry` leaves it
                    // untouched on purpose.
                    if let RetryDecision::Fdb(fdb_err) = proposed {
                        maybe_committed = fdb_err.is_maybe_committed();
                    }

                    let fdb_err =
                        match policy.decide(AttemptFailure::Closure(&error), proposed, attempt) {
                            RetryDecision::Fdb(fdb_err) => fdb_err,
                            // Synthetic retryable code (1020, not_committed): backoff and
                            // RetryLimit stay governed by fdb_transaction_on_error.
                            RetryDecision::Retry => FdbError::from_code(NOT_COMMITTED),
                            RetryDecision::Fatal => return Err(error),
                        };
                    hooks.on_closure_error(&fdb_err, attempt);

                    let now_on_error = Instant::now();
                    match transaction.on_error(fdb_err).await {
                        Ok(Ok(transaction)) => {
                            hooks.on_error_duration(
                                now_on_error.elapsed().as_millis() as u64,
                                attempt,
                            );
                            (transaction, fdb_err)
                        }
                        // Retries exhausted or non-retryable: return the original
                        // closure error, which carries the caller's context.
                        Ok(Err(_non_retryable)) => return Err(error),
                        Err(binding_error) => return Err(E::from(binding_error)),
                    }
                }

                Attempt::CommitFailed(commit_error) => {
                    maybe_committed = commit_error.is_maybe_committed();
                    let proposed = RetryDecision::Fdb(*commit_error);

                    // Fires before the policy is consulted: a hook reading the
                    // conflicting keys must see them even when the run is about
                    // to end.
                    if let Err(hook_error) = hooks.on_commit_error(&commit_error, attempt).await {
                        hooks.on_hook_error(&hook_error, attempt);
                    }

                    let fdb_err = match policy.decide(
                        AttemptFailure::Commit(&commit_error),
                        proposed,
                        attempt,
                    ) {
                        RetryDecision::Fdb(fdb_err) => fdb_err,
                        RetryDecision::Retry => FdbError::from_code(NOT_COMMITTED),
                        RetryDecision::Fatal => {
                            return Err(E::from(FdbBindingError::from(*commit_error)));
                        }
                    };

                    // The transaction is taken out of the error so that the
                    // error handed to `on_error` is the one the policy chose.
                    let (transaction, _commit_err) = commit_error.into_parts();
                    let now_on_error = Instant::now();
                    match transaction.on_error(fdb_err).await {
                        Ok(transaction) => {
                            hooks.on_error_duration(
                                now_on_error.elapsed().as_millis() as u64,
                                attempt,
                            );
                            (RetryableTransaction::new(transaction), fdb_err)
                        }
                        Err(non_retryable) => {
                            #[cfg(feature = "trace")]
                            tracing::error!(
                                attempt,
                                error_code = non_retryable.code(),
                                "could not commit, non retryable error"
                            );

                            return Err(E::from(FdbBindingError::from(non_retryable)));
                        }
                    }
                }
            };

        #[cfg(feature = "trace")]
        tracing::warn!(
            attempt,
            error_code = _fdb_err.code(),
            "restarting transaction"
        );

        hooks.on_retry(attempt);
        transaction = retried_transaction;
        attempt += 1;
    }
}

/// The retry runner: [`retry_loop`] plus the guarantee that
/// [`RunnerHooks::on_complete`] fires exactly once.
#[cfg_attr(
    feature = "trace",
    tracing::instrument(level = "debug", skip(initial_transaction, hooks, policy, closure))
)]
pub(crate) async fn run_with_hooks<F, Fut, T, E, H, P>(
    initial_transaction: RetryableTransaction,
    hooks: &H,
    policy: &P,
    closure: F,
) -> Result<T, E>
where
    F: Fn(RetryableTransaction, MaybeCommitted) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: RetryableError,
    H: RunnerHooks,
    P: RetryPolicy<E>,
{
    let result = retry_loop(initial_transaction, hooks, policy, closure).await;
    hooks.on_complete();
    result
}

/// Builder for a transactional run, returned by
/// [`Database::runner`](crate::Database::runner).
///
/// It assembles the [`RunnerHooks`] and the [`RetryPolicy`] of a single call to
/// [`run`](Self::run): no hooks and [`NativeRetryPolicy`] by default. Both are
/// borrowed, so the same hooks and policy can be reused across runs (mind
/// [`MetricsHooks`], which times the run from its own construction).
///
/// ```no_run
/// # use foundationdb::*;
/// # async fn example(db: &Database) -> Result<(), FdbBindingError> {
/// db.runner()
///     .run(|trx, _| async move {
///         trx.set(b"key", b"value");
///         Ok::<_, FdbBindingError>(())
///     })
///     .await
/// # }
/// ```
pub struct TransactionRunner<'a, H = (), P = NativeRetryPolicy> {
    db: &'a Database,
    hooks: &'a H,
    policy: &'a P,
}

impl<'a> TransactionRunner<'a, (), NativeRetryPolicy> {
    /// A runner on `db` with no hooks and the native retry policy.
    pub(crate) fn new(db: &'a Database) -> Self {
        Self {
            db,
            hooks: &(),
            policy: &NativeRetryPolicy,
        }
    }
}

impl<'a, H, P> TransactionRunner<'a, H, P> {
    /// Observes the run with `hooks`, replacing any hooks set before.
    ///
    /// Stack several of them with a tuple: `.hooks(&(first, second))`.
    pub fn hooks<H2>(self, hooks: &'a H2) -> TransactionRunner<'a, H2, P> {
        TransactionRunner {
            db: self.db,
            hooks,
            policy: self.policy,
        }
    }

    /// Decides the retries with `policy` instead of [`NativeRetryPolicy`].
    pub fn retry_policy<P2>(self, policy: &'a P2) -> TransactionRunner<'a, H, P2> {
        TransactionRunner {
            db: self.db,
            hooks: self.hooks,
            policy,
        }
    }

    /// Runs `closure` in a transaction, retrying it until it commits or the
    /// error is fatal.
    ///
    /// See [`Database::run`](crate::Database::run) for the semantics of the
    /// closure, its error type and the retries.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, closure))
    )]
    pub async fn run<F, Fut, T, E>(self, closure: F) -> Result<T, E>
    where
        F: Fn(RetryableTransaction, MaybeCommitted) -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: RetryableError,
        H: RunnerHooks,
        P: RetryPolicy<E>,
    {
        let transaction = match self.db.create_retryable_trx() {
            Ok(transaction) => transaction,
            // The run is over before it started, but it did start: hooks that
            // report on completion must still be called exactly once.
            Err(err) => {
                self.hooks.on_complete();
                return Err(E::from(err));
            }
        };

        run_with_hooks(transaction, self.hooks, self.policy, closure).await
    }
}
