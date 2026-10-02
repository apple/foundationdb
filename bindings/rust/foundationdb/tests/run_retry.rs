// Copyright 2026 foundationdb-rs developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Retry behavior of `Database::run` for closure errors that wrap an
//! FdbError or are fatal (#479).

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use foundationdb::{FdbBindingError, FdbError, options};

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

/// A typed layer error preserving the native error as its source.
#[derive(Debug)]
enum RetryTestError {
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
            Self::InvalidDocument => None,
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

/// Native RetryLimit exhausts the retries while preserving the typed closure
/// error and its source, rather than replacing it with the on_error result.
#[tokio::test]
async fn run_wrapped_error_honors_retry_limit() {
    let db = common::database().await.expect("failed to open database");
    let attempt = AtomicU8::new(0);
    let attempt_ref = &attempt;
    let retry_limit = 2;

    let result: Result<(), RetryTestError> = db
        .run(|trx, _| async move {
            trx.set_option(options::TransactionOption::RetryLimit(retry_limit))?;
            attempt_ref.fetch_add(1, Ordering::SeqCst);
            Err(RetryTestError::from(FdbError::from_code(1020)))
        })
        .await;

    assert!(matches!(result, Err(RetryTestError::Fdb(e)) if e.code() == 1020));
    assert_eq!(attempt.load(Ordering::SeqCst) as i64, retry_limit + 1);
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

/// A later `transaction_too_old` cannot resolve an earlier uncertain commit.
#[tokio::test]
async fn run_keeps_maybe_committed_after_later_closure_errors() {
    let db = common::database().await.expect("failed to open database");
    let attempt = AtomicU8::new(0);
    let attempt_ref = &attempt;

    let result: Result<(), RetryTestError> = db
        .run(|trx, maybe_committed| async move {
            let attempt = attempt_ref.fetch_add(1, Ordering::SeqCst);
            assert_eq!(bool::from(maybe_committed), attempt != 0);
            match attempt {
                0 => Err(RetryTestError::from(FdbError::from_code(1021))),
                1 => Err(RetryTestError::from(FdbError::from_code(1007))),
                _ => {
                    trx.set(b"run_retry_sticky_closure", b"ok");
                    Ok(())
                }
            }
        })
        .await;

    assert!(result.is_ok(), "both errors must be retried: {result:?}");
    assert_eq!(attempt.load(Ordering::SeqCst), 3);
}

/// A commit conflict cannot resolve an earlier uncertain commit either.
#[tokio::test]
async fn run_keeps_maybe_committed_after_commit_conflicts() {
    let db = common::database().await.expect("failed to open database");
    let db_ref = &db;
    let attempt = AtomicU8::new(0);
    let attempt_ref = &attempt;
    let key = b"run_retry_sticky_commit";

    let result: Result<(), RetryTestError> = db
        .run(|trx, maybe_committed| async move {
            let attempt = attempt_ref.fetch_add(1, Ordering::SeqCst);
            assert_eq!(bool::from(maybe_committed), attempt != 0);
            if attempt == 0 {
                return Err(RetryTestError::from(FdbError::from_code(1021)));
            }
            if attempt == 1 {
                trx.get(key, false).await?;
                // Commit a write after the outer transaction's read to force
                // its commit, rather than its closure, to fail with a conflict.
                db_ref
                    .run(|other, _| async move {
                        other.set(key, b"conflict");
                        Ok::<_, FdbBindingError>(())
                    })
                    .await?;
            }
            trx.set(key, b"ok");
            Ok(())
        })
        .await;

    assert!(
        result.is_ok(),
        "the commit conflict must be retried: {result:?}"
    );
    assert_eq!(attempt.load(Ordering::SeqCst), 3);
}

/// A retained clone must not permit commit or reset while user code can still
/// use the transaction. Exercise both the success and native-retry paths.
#[tokio::test]
async fn run_rejects_retained_transaction_clones() {
    let db = common::database().await.expect("failed to open database");
    for retry in [false, true] {
        let retained = std::sync::Mutex::new(None);
        let result: Result<(), FdbBindingError> = db
            .run(|trx, _| {
                *retained.lock().expect("retained transaction mutex") = Some(trx.clone());
                async move {
                    trx.set(b"run_retained_transaction", b"uncommitted");
                    if retry {
                        Err(FdbBindingError::from(FdbError::from_code(1020)))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;
        assert!(matches!(
            result,
            Err(FdbBindingError::ReferenceToTransactionKept)
        ));
    }
}
