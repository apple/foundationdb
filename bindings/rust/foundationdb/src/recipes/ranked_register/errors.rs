// Copyright 2024 foundationdb-rs developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Error types for the ranked register

use crate::tuple::PackError;
use crate::{FdbBindingError, FdbError, RetryableError};
use std::fmt;

/// Ranked register specific errors
#[derive(Debug)]
pub enum RankedRegisterError {
    /// Database error
    Fdb(FdbError),
    /// Retry loop error
    Binding(FdbBindingError),
    /// Serialization error
    PackError(PackError),
    /// Invalid state encountered in the register
    InvalidState(String),
    /// The requested raw value exceeds this handle's configured limit.
    ///
    /// This is a non-retryable caller error. The limit is local configuration
    /// and is never persisted in FoundationDB.
    ValueTooLarge {
        /// Requested raw value size in bytes.
        value_size: usize,
        /// Configured maximum raw value size in bytes.
        limit: usize,
    },
}

impl fmt::Display for RankedRegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fdb(e) => write!(f, "Database error: {e}"),
            Self::Binding(e) => write!(f, "Retry loop error: {e}"),
            Self::PackError(e) => write!(f, "Pack error: {e:?}"),
            Self::InvalidState(msg) => write!(f, "Invalid state: {msg}"),
            Self::ValueTooLarge { value_size, limit } => {
                write!(
                    f,
                    "Register value is {value_size} bytes, above the {limit}-byte limit"
                )
            }
        }
    }
}

impl std::error::Error for RankedRegisterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Fdb(e) => Some(e),
            Self::Binding(e) => Some(e),
            Self::PackError(e) => Some(e),
            Self::InvalidState(_) | Self::ValueTooLarge { .. } => None,
        }
    }
}

impl From<FdbError> for RankedRegisterError {
    fn from(error: FdbError) -> Self {
        Self::Fdb(error)
    }
}

impl From<FdbBindingError> for RankedRegisterError {
    fn from(error: FdbBindingError) -> Self {
        Self::Binding(error)
    }
}

impl From<PackError> for RankedRegisterError {
    fn from(error: PackError) -> Self {
        Self::PackError(error)
    }
}

/// The `source()` chain exposes the wrapped `FdbError`, so the default
/// `retry_decision` makes this error retry-transparent in `db.run`.
impl RetryableError for RankedRegisterError {}

/// Result type for ranked register operations
pub type Result<T> = std::result::Result<T, RankedRegisterError>;
