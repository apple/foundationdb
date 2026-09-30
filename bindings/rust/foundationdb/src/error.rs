// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Error types for the Fdb crate

use crate::budget::BudgetExceeded;
use crate::directory::DirectoryError;
use crate::options;
use crate::tuple::PackError;
use crate::tuple::hca::HcaError;
use foundationdb_sys as fdb_sys;
use std::ffi::CStr;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};

pub(crate) fn eval(error_code: fdb_sys::fdb_error_t) -> FdbResult<()> {
    let rust_code: i32 = error_code;
    if rust_code == 0 {
        Ok(())
    } else {
        Err(FdbError::from_code(error_code))
    }
}

/// The Standard Error type of FoundationDB
#[derive(Debug, Copy, Clone)]
pub struct FdbError {
    /// The FoundationDB error code
    error_code: i32,
}

impl FdbError {
    /// Converts from a raw foundationDB error code
    pub fn from_code(error_code: fdb_sys::fdb_error_t) -> Self {
        Self { error_code }
    }

    pub fn message(self) -> &'static str {
        let error_str =
            unsafe { CStr::from_ptr::<'static>(fdb_sys::fdb_get_error(self.error_code)) };
        error_str
            .to_str()
            .expect("bad error string from FoundationDB")
    }

    fn is_error_predicate(self, predicate: options::ErrorPredicate) -> bool {
        // This cast to `i32` isn't unnecessary in all configurations.
        #[allow(clippy::unnecessary_cast)]
        let check =
            unsafe { fdb_sys::fdb_error_predicate(predicate.code() as i32, self.error_code) };

        check != 0
    }

    /// Indicates the transaction may have succeeded, though not in a way the system can verify.
    pub fn is_maybe_committed(self) -> bool {
        self.is_error_predicate(options::ErrorPredicate::MaybeCommitted)
    }

    /// Indicates the operations in the transactions should be retried because of transient error.
    pub fn is_retryable(self) -> bool {
        self.is_error_predicate(options::ErrorPredicate::Retryable)
    }

    /// Indicates the transaction has not committed, though in a way that can be retried.
    pub fn is_retryable_not_committed(self) -> bool {
        self.is_error_predicate(options::ErrorPredicate::RetryableNotCommitted)
    }

    /// Raw foundationdb error code
    pub fn code(self) -> i32 {
        self.error_code
    }
}

impl fmt::Display for FdbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        std::fmt::Display::fmt(&self.message(), f)
    }
}

impl std::error::Error for FdbError {}

/// Alias for `Result<..., FdbError>`
pub type FdbResult<T = ()> = Result<T, FdbError>;

/// This error represent all errors that can be throwed by `db.run`.
/// Layer developers may use the `CustomError`.
#[non_exhaustive]
pub enum FdbBindingError {
    NonRetryableFdbError(FdbError),
    HcaError(HcaError),
    DirectoryError(DirectoryError),
    PackError(PackError),
    /// A reference to the `RetryableTransaction` has been kept
    ReferenceToTransactionKept,
    /// All 65536 user versions have been allocated for the transaction attempt.
    UserVersionExhausted,
    /// A custom error that layer developers can use
    ///
    /// The retry loop of [`crate::Database::run`] recovers the underlying
    /// [`FdbError`] by walking the boxed error's `source()` chain, so box the
    /// error itself (or a type that keeps the `FdbError` as its `source()`).
    /// Never box a stringified error (`e.to_string().into()`): the conversion
    /// destroys the source chain, and a retryable error becomes fatal.
    CustomError(Box<dyn std::error::Error + Send + Sync>),
    /// The client-side budget of the transaction attempt was exceeded, as
    /// reported by [`crate::Transaction::check_client_budget`]
    ClientBudgetExceeded(BudgetExceeded),
    #[cfg(feature = "recipes-leader-election")]
    /// Leader election specific error
    LeaderElectionError(crate::recipes::leader_election::LeaderElectionError),
}

/// Walks an error's `source()` chain, returning the first `FdbError` found.
///
/// The depth cap guards against pathological or cyclic source chains.
pub(crate) fn find_fdb_error(err: &(dyn std::error::Error + 'static)) -> Option<FdbError> {
    const MAX_DEPTH: usize = 128;
    let mut current = Some(err);
    for _ in 0..=MAX_DEPTH {
        let e = current?;
        if let Some(fdb) = e.downcast_ref::<FdbError>() {
            return Some(*fdb);
        }
        current = e.source();
    }
    None
}

impl FdbBindingError {
    /// Returns the underlying `FdbError`, if any.
    ///
    /// A `CustomError` boxing an `FdbError` or an `FdbBindingError` is checked
    /// first, then the whole `source()` chain is walked, so any error that
    /// carries an `FdbError` as its `source()` (however deeply nested) is found.
    pub fn get_fdb_error(&self) -> Option<FdbError> {
        if let Self::CustomError(e) = self {
            if let Some(e) = e.downcast_ref::<FdbError>() {
                return Some(*e);
            }
            if let Some(e) = e.downcast_ref::<FdbBindingError>() {
                return e.get_fdb_error();
            }
        }
        find_fdb_error(self)
    }
}

impl From<FdbError> for FdbBindingError {
    fn from(e: FdbError) -> Self {
        Self::NonRetryableFdbError(e)
    }
}

impl From<HcaError> for FdbBindingError {
    fn from(e: HcaError) -> Self {
        Self::HcaError(e)
    }
}

impl From<DirectoryError> for FdbBindingError {
    fn from(e: DirectoryError) -> Self {
        Self::DirectoryError(e)
    }
}

impl From<BudgetExceeded> for FdbBindingError {
    fn from(e: BudgetExceeded) -> Self {
        Self::ClientBudgetExceeded(e)
    }
}

#[cfg(feature = "recipes-leader-election")]
impl From<crate::recipes::leader_election::LeaderElectionError> for FdbBindingError {
    fn from(error: crate::recipes::leader_election::LeaderElectionError) -> Self {
        Self::LeaderElectionError(error)
    }
}

impl FdbBindingError {
    /// create a new custom error
    pub fn new_custom_error(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::CustomError(e)
    }
}

impl Debug for FdbBindingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FdbBindingError::NonRetryableFdbError(err) => write!(f, "{err:?}"),
            FdbBindingError::HcaError(err) => write!(f, "{err:?}"),
            FdbBindingError::DirectoryError(err) => write!(f, "{err:?}"),
            FdbBindingError::PackError(err) => write!(f, "{err:?}"),
            FdbBindingError::ReferenceToTransactionKept => {
                write!(f, "Reference to transaction kept")
            }
            FdbBindingError::UserVersionExhausted => {
                write!(f, "Transaction user version allocator exhausted")
            }
            FdbBindingError::CustomError(err) => write!(f, "{err:?}"),
            FdbBindingError::ClientBudgetExceeded(err) => write!(f, "{err}"),
            #[cfg(feature = "recipes-leader-election")]
            FdbBindingError::LeaderElectionError(err) => write!(f, "{err:?}"),
        }
    }
}

impl Display for FdbBindingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}

impl std::error::Error for FdbBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NonRetryableFdbError(e) => Some(e),
            Self::HcaError(e) => Some(e),
            Self::DirectoryError(e) => Some(e),
            Self::PackError(e) => Some(e),
            Self::CustomError(e) => Some(e.as_ref()),
            Self::ClientBudgetExceeded(e) => Some(e),
            Self::ReferenceToTransactionKept => None,
            Self::UserVersionExhausted => None,
            #[cfg(feature = "recipes-leader-election")]
            Self::LeaderElectionError(e) => Some(e),
        }
    }
}

/// What the retry loop of [`crate::Database::run`] does with a closure error.
///
/// Produced by [`RetryableError::retry_decision`]. In every case the C API
/// remains the single retry governor: backoff, max retry delay and
/// `TransactionOption::RetryLimit` are applied by `fdb_transaction_on_error`,
/// never by a Rust-side budget.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub enum RetryDecision {
    /// The error is, or wraps, this `FdbError`: hand it to `on_error` and let
    /// the C API judge retryability.
    Fdb(FdbError),
    /// App-level retry request with no native error underneath (lock
    /// contention, optimistic-concurrency conditions). Routed through
    /// `on_error` with code 1020 (not_committed, retryable by definition), so
    /// backoff and retry limits apply uniformly.
    Retry,
    /// Not retryable: the loop returns the original error to the caller as-is.
    Fatal,
}

/// What the retry loop of [`crate::Database::run`] asks of a closure error.
///
/// The default `retry_decision` walks the `source()` chain looking for an
/// [`FdbError`], so any error type that keeps its `FdbError` as a source (the
/// idiomatic thiserror `#[from]`/`#[source]` pattern) is retry-transparent
/// without overriding anything. Override only to add [`RetryDecision::Retry`]
/// arms for app-level retry conditions.
///
/// The `From` bounds let the loop surface its own failures through your type:
/// `From<FdbError>` supports `?` on `FdbResult` inside the closure, and
/// `From<FdbBindingError>` carries loop-level failures such as
/// [`FdbBindingError::ReferenceToTransactionKept`].
///
/// `FdbError` itself cannot implement this trait (there is no lossless
/// `From<FdbBindingError> for FdbError`); use a wrapper type such as
/// [`FdbBindingError`] or your own enum.
///
/// # Example
///
/// ```
/// use foundationdb::{FdbBindingError, FdbError, RetryableError};
///
/// #[derive(Debug)]
/// enum MyLayerError {
///     Fdb(FdbError),
///     Binding(FdbBindingError),
///     InvalidDocument,
/// }
///
/// impl std::fmt::Display for MyLayerError {
///     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
///         write!(f, "{self:?}")
///     }
/// }
///
/// impl std::error::Error for MyLayerError {
///     fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
///         match self {
///             Self::Fdb(e) => Some(e),
///             Self::Binding(e) => Some(e),
///             Self::InvalidDocument => None,
///         }
///     }
/// }
///
/// impl From<FdbError> for MyLayerError {
///     fn from(e: FdbError) -> Self {
///         Self::Fdb(e)
///     }
/// }
///
/// impl From<FdbBindingError> for MyLayerError {
///     fn from(e: FdbBindingError) -> Self {
///         Self::Binding(e)
///     }
/// }
///
/// // The default source() walk makes wrapped FdbErrors retryable.
/// impl RetryableError for MyLayerError {}
/// ```
pub trait RetryableError:
    std::error::Error + Send + Sync + Sized + 'static + From<FdbError> + From<FdbBindingError>
{
    /// Classifies this error for the retry loop.
    fn retry_decision(&self) -> RetryDecision {
        match find_fdb_error(self) {
            Some(e) => RetryDecision::Fdb(e),
            None => RetryDecision::Fatal,
        }
    }
}

impl RetryableError for FdbBindingError {
    fn retry_decision(&self) -> RetryDecision {
        match self.get_fdb_error() {
            Some(e) => RetryDecision::Fdb(e),
            None => RetryDecision::Fatal,
        }
    }
}
