// Copyright 2026 foundationdb-rs developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Retry behavior of `Database::run` for closure errors that wrap an
//! FdbError, request an app-level retry, or are fatal (#479).

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use foundationdb::runner::{AttemptFailure, RetryPolicy};
use foundationdb::{FdbBindingError, FdbError, RetryDecision, RetryableError, options};

mod common;

/// A layer error keeping the FdbError as its source, like a thiserror enum
/// with `#[source]`/`#[from]` would.
#[derive(Debug)]
struct WrappedFdbError {
    source: FdbError,
}

impl fmt::Display for WrappedFdbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "layer error: {}", self.source)
    }
}

impl std::error::Error for WrappedFdbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// A typed layer error with an app-level retry condition.
#[derive(Debug)]
enum RetryTestError {
    /// App-level retry request, no native error underneath.
    NeedsRetry,
    /// A domain error with no FdbError anywhere in its chain.
    InvalidDocument,
    Fdb(FdbError),
    Binding(FdbBindingError),
}

impl fmt::Display for RetryTestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for RetryTestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Fdb(e) => Some(e),
            Self::Binding(e) => Some(e),
            Self::NeedsRetry | Self::InvalidDocument => None,
        }
    }
}

impl From<FdbError> for RetryTestError {
    fn from(e: FdbError) -> Self {
        Self::Fdb(e)
    }
}

impl From<FdbBindingError> for RetryTestError {
    fn from(e: FdbBindingError) -> Self {
        Self::Binding(e)
    }
}

impl RetryableError for RetryTestError {
    fn retry_decision(&self) -> RetryDecision {
        match self {
            Self::NeedsRetry => RetryDecision::Retry,
            Self::Fdb(e) => RetryDecision::Fdb(*e),
            Self::InvalidDocument | Self::Binding(_) => RetryDecision::Fatal,
        }
    }
}

/// The end-to-end regression for #479: a retryable FdbError wrapped inside a
/// layer error and boxed into CustomError is retried (port of the Go
/// binding's TestErrorWrapping).
#[tokio::test]
async fn run_retries_wrapped_closure_error() {
    let db = common::database().await.expect("failed to open database");
    let attempt = AtomicU8::new(0);
    let attempt_ref = &attempt;

    let result = db
        .run(|trx, _| async move {
            if attempt_ref.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(FdbBindingError::new_custom_error(Box::new(
                    WrappedFdbError {
                        source: FdbError::from_code(1020),
                    },
                )));
            }
            trx.set(b"run_retry_wrapped", b"ok");
            Ok(())
        })
        .await;

    assert!(result.is_ok(), "wrapped retryable error must be retried");
    assert_eq!(attempt.load(Ordering::SeqCst), 2);
}

/// Same regression through a typed closure error: the default source() walk
/// makes the wrapped FdbError retryable and the caller gets the typed error
/// back on fatal paths.
#[tokio::test]
async fn run_retries_typed_wrapped_error() {
    let db = common::database().await.expect("failed to open database");
    let attempt = AtomicU8::new(0);
    let attempt_ref = &attempt;

    let result = db
        .run(|trx, _| async move {
            if attempt_ref.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(RetryTestError::from(FdbError::from_code(1020)));
            }
            trx.set(b"run_retry_typed", b"ok");
            Ok(())
        })
        .await;

    assert!(result.is_ok(), "typed wrapped error must be retried");
    assert_eq!(attempt.load(Ordering::SeqCst), 2);
}

/// RetryDecision::Retry re-runs the closure through on_error(1020).
#[tokio::test]
async fn run_retry_decision_reruns_closure() {
    let db = common::database().await.expect("failed to open database");
    let attempt = AtomicU8::new(0);
    let attempt_ref = &attempt;

    let result = db
        .run(|trx, _| async move {
            if attempt_ref.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(RetryTestError::NeedsRetry);
            }
            trx.set(b"run_retry_decision", b"ok");
            Ok(())
        })
        .await;

    assert!(result.is_ok(), "Retry decision must re-run the closure");
    assert_eq!(attempt.load(Ordering::SeqCst), 2);
}

/// RetryDecision::Retry is governed by the C API retry budget: with
/// RetryLimit(n) the closure runs n + 1 times and exhaustion returns the
/// ORIGINAL typed error, not a synthetic FdbError.
#[tokio::test]
async fn run_retry_decision_honors_retry_limit() {
    let db = common::database().await.expect("failed to open database");
    let attempt = AtomicU8::new(0);
    let attempt_ref = &attempt;
    let retry_limit = 2;

    let result: Result<(), RetryTestError> = db
        .run(|trx, _| async move {
            trx.set_option(options::TransactionOption::RetryLimit(retry_limit))?;
            attempt_ref.fetch_add(1, Ordering::SeqCst);
            Err(RetryTestError::NeedsRetry)
        })
        .await;

    assert!(
        matches!(result, Err(RetryTestError::NeedsRetry)),
        "exhaustion must return the original typed error, got {result:?}"
    );
    assert_eq!(attempt.load(Ordering::SeqCst) as i32, retry_limit + 1);
}

/// An error with no FdbError in its chain is fatal: the closure runs exactly
/// once and the caller gets the original error back.
#[tokio::test]
async fn run_typed_error_fatal_returns_original() {
    let db = common::database().await.expect("failed to open database");
    let attempt = AtomicU8::new(0);
    let attempt_ref = &attempt;

    let result: Result<(), RetryTestError> = db
        .run(|_trx, _| async move {
            attempt_ref.fetch_add(1, Ordering::SeqCst);
            Err(RetryTestError::InvalidDocument)
        })
        .await;

    assert!(
        matches!(result, Err(RetryTestError::InvalidDocument)),
        "fatal error must be returned as-is, got {result:?}"
    );
    assert_eq!(attempt.load(Ordering::SeqCst), 1);
}

/// `MaybeCommitted` is computed from the closure error itself: an error
/// wrapping `commit_unknown_result` (1021) tells the next attempt that the
/// previous one may have committed.
#[tokio::test]
async fn run_reports_maybe_committed_from_the_closure_error() {
    let db = common::database().await.expect("failed to open database");
    let attempt = AtomicU8::new(0);
    let attempt_ref = &attempt;
    let seen = AtomicBool::new(false);
    let seen_ref = &seen;

    let result: Result<(), RetryTestError> = db
        .run(|trx, maybe_committed| async move {
            if attempt_ref.fetch_add(1, Ordering::SeqCst) == 0 {
                // commit_unknown_result: retryable and maybe committed.
                return Err(RetryTestError::from(FdbError::from_code(1021)));
            }
            seen_ref.store(maybe_committed.into(), Ordering::SeqCst);
            trx.set(b"run_retry_maybe_committed", b"ok");
            Ok(())
        })
        .await;

    assert!(result.is_ok(), "1021 must be retried: {result:?}");
    assert_eq!(attempt.load(Ordering::SeqCst), 2);
    assert!(
        seen.load(Ordering::SeqCst),
        "the retried closure must see maybe_committed"
    );
}

/// A [`RetryPolicy`] rewriting the decision cannot clear `MaybeCommitted`: the
/// flag is computed from the original error, before the policy is consulted.
#[tokio::test]
async fn retry_policy_cannot_clear_maybe_committed() {
    /// Turns every failure into a plain retry, which on its own would leave
    /// `MaybeCommitted` untouched.
    struct AlwaysRetry;

    impl RetryPolicy<RetryTestError> for AlwaysRetry {
        fn decide(
            &self,
            _failure: AttemptFailure<'_, RetryTestError>,
            _proposed: RetryDecision,
            _attempt: usize,
        ) -> RetryDecision {
            RetryDecision::Retry
        }
    }

    let db = common::database().await.expect("failed to open database");
    let attempt = AtomicU8::new(0);
    let attempt_ref = &attempt;
    let seen = AtomicBool::new(false);
    let seen_ref = &seen;

    let result: Result<(), RetryTestError> = db
        .runner()
        .retry_policy(&AlwaysRetry)
        .run(|trx, maybe_committed| async move {
            if attempt_ref.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(RetryTestError::from(FdbError::from_code(1021)));
            }
            seen_ref.store(maybe_committed.into(), Ordering::SeqCst);
            trx.set(b"run_retry_policy_maybe_committed", b"ok");
            Ok(())
        })
        .await;

    assert!(result.is_ok(), "the policy must retry: {result:?}");
    assert_eq!(attempt.load(Ordering::SeqCst), 2);
    assert!(
        seen.load(Ordering::SeqCst),
        "the policy must not be able to clear maybe_committed"
    );
}
