// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
#![doc = include_str!("../README.md")]

use foundationdb_sys::if_cfg_api_versions;

#[macro_use]
extern crate static_assertions;

pub mod api;
#[deny(missing_docs)]
pub mod budget;
if_cfg_api_versions! {min = 510, max = 600 =>
    pub mod cluster;
}
mod database;
pub mod directory;
#[deny(missing_docs)]
pub mod env;
mod error;
if_cfg_api_versions! {min = 700 =>
    #[deny(missing_docs)]
    pub mod fdb_keys;
}
pub mod future;
mod keyselector;
if_cfg_api_versions! {min = 710 =>
    #[deny(missing_docs)]
    pub mod mapped_key_values;
}

pub mod metrics;

/// Generated configuration types for use with the various `set_option` functions
#[allow(clippy::all)]
pub mod options;

#[cfg(any(feature = "recipes", feature = "recipes-leader-election"))]
pub mod recipes;

// Re-export metrics types for convenience
pub use crate::metrics::{
    AttemptMetrics, AttemptOutcome, ConflictKeys, MetricsReport, TransactionMetrics,
};

#[deny(missing_docs)]
pub mod runner;

pub mod timekeeper;
mod transaction;
mod tuple_ext;

pub mod tuple {
    pub use crate::tuple_ext::*;
    pub use foundationdb_tuple::*;
}

if_cfg_api_versions! {min = 510, max = 600 =>
    pub use crate::cluster::Cluster;
}

pub use crate::budget::{AttemptUsage, BudgetExceeded, BudgetKind, ClientBudget, UsageSnapshot};
pub use crate::database::*;
pub use crate::env::{Clock, Environment, Rng, SeededRng, WallClock};
pub use crate::error::{FdbBindingError, RetryDecision, RetryableError};
pub use crate::error::{FdbError, FdbResult};
pub use crate::keyselector::*;
pub use crate::runner::{
    AttemptFailure, MetricsHooks, NativeRetryPolicy, RetryPolicy, RunnerHooks, TransactionRunner,
};
pub use crate::transaction::*;

/// Initialize the FoundationDB Client API and start the network thread.
///
/// Safe and idempotent: calling it multiple times returns `Ok` as long as the
/// (default) API version matches. Calling it is optional: creating a [`Database`]
/// initializes the client automatically. The network runs until process exit,
/// where an atexit hook stops it and joins the network thread, or until an
/// explicit [`api::stop_network`] call.
///
/// The returned guard is kept for backward compatibility, dropping it does
/// nothing.
///
/// <div class="warning">
///
/// The automatic stop at process exit is a convenience suited to tests and
/// short-lived tools. In a production application, prefer handling the network
/// stop yourself: the network thread is the event loop driving every
/// transaction, you may still have on-going operations at exit time, and you
/// usually want a clean teardown. Finish or cancel your work, drop your
/// [`Database`] handles, then call [`api::stop_network`].
///
/// </div>
///
/// # Errors
///
/// - error 2201 (`api_version_already_set`) if a different API version was
///   selected before
/// - error 2025 (`network_cannot_be_restarted`) if the network was stopped
///
/// # Examples
///
/// ```rust
/// foundationdb::boot().expect("failed to initialize FoundationDB");
/// // do some interesting things with the API...
/// ```
///
/// ```rust
/// #[tokio::main]
/// async fn main() {
///     foundationdb::boot().expect("failed to initialize FoundationDB");
///     // do some interesting things with the API...
/// }
/// ```
pub fn boot() -> FdbResult<api::NetworkAutoStop> {
    api::FdbApiBuilder::default().build()?.boot()
}

/// Returns the default Fdb cluster configuration file path
#[cfg(target_os = "linux")]
pub fn default_config_path() -> &'static str {
    "/etc/foundationdb/fdb.cluster"
}

/// Returns the default Fdb cluster configuration file path
#[cfg(target_os = "macos")]
pub fn default_config_path() -> &'static str {
    "/usr/local/etc/foundationdb/fdb.cluster"
}

/// Returns the default Fdb cluster configuration file path
#[cfg(target_os = "windows")]
pub fn default_config_path() -> &'static str {
    "C:/ProgramData/foundationdb/fdb.cluster"
}

/// slice::from_raw_parts assumes the pointer to be aligned and non-null.
/// Since Rust nightly (mid-February 2024), it is enforced with debug asserts,
/// but the FDBServer can return a null pointer if the slice is empty:
/// ptr: 0x0, len: 0
/// To avoid the assert, an empty slice is directly returned in that situation
fn from_raw_fdb_slice<T, U: Into<usize>>(ptr: *const T, len: U) -> &'static [T] {
    if ptr.is_null() {
        return &[];
    }
    unsafe { std::slice::from_raw_parts(ptr, len.into()) }
}
