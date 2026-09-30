// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! The directory layer offers subspace indirection, where logical application subspaces are mapped to short, auto-generated key prefixes. This prefix assignment is done by the High Contention Allocator, which allows many clients to allocate short directory prefixes efficiently.
//!
//! The allocation process works over candidate value windows. It uses two subspaces to operate, the "counters" subspace and "recents" subspace (derived from the subspace used to create the HCA).
//!
//! "counters" contains a single key : "counters : window_start", whose value is the number of allocations in the current window. "window_start" is an integer that marks the lower bound of values that can be assigned from the current window.
//! "recents" can contain many keys : "recents : candidate", where each "candidate" is an integer that has been assigned to some client
//!
//! Assignment has two stages that are executed in a loop until they both succeed.
//!
//! 1. Find the current window. The client scans "counters : *" to get the current "window_start" and how many allocations have been made in the current window.
//!    If the window is more than half-full (using pre-defined window sizes), the window is advanced: "counters : *" and "recents : *" are both cleared, and a new "counters : window_start + window_size" key is created with a value of 0. (1) is retried
//!    If the window still has space, it moves to (2).
//!
//! 2. Find a candidate value inside that window. The client picks a candidate number between "[window_start, window_start + window_size)" and tries to set the key "recents : candidate".
//!    If the write succeeds, the candidate is returned as the allocated value. Success!
//!    If the write fails because the window has been advanced, it repeats (1).
//!    If the write fails because the value was already set, it repeats (2).

use std::fmt;
use std::sync::{Mutex, PoisonError};

use futures::future;
use rand::{RngExt, rngs::SmallRng};

use crate::options::{ConflictRangeType, MutationType, TransactionOption};
use crate::tuple::{PackError, Subspace};
use crate::*;

const ONE_BYTES: &[u8] = &[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

pub enum HcaError {
    FdbError(FdbError),
    PackError(PackError),
    InvalidDirectoryLayerMetadata,
    PoisonError,
}

impl fmt::Debug for HcaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            HcaError::FdbError(err) => err.fmt(f),
            HcaError::PackError(err) => err.fmt(f),
            HcaError::InvalidDirectoryLayerMetadata => {
                write!(f, "invalid directory layer metadata")
            }
            HcaError::PoisonError => write!(f, "mutex poisoned"),
        }
    }
}

impl fmt::Display for HcaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for HcaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HcaError::FdbError(e) => Some(e),
            HcaError::PackError(e) => Some(e),
            HcaError::InvalidDirectoryLayerMetadata | HcaError::PoisonError => None,
        }
    }
}

impl From<FdbError> for HcaError {
    fn from(err: FdbError) -> Self {
        Self::FdbError(err)
    }
}
impl From<PackError> for HcaError {
    fn from(err: PackError) -> Self {
        Self::PackError(err)
    }
}
impl<T> From<PoisonError<T>> for HcaError {
    fn from(_err: PoisonError<T>) -> Self {
        Self::PoisonError
    }
}

impl TransactError for HcaError {
    fn try_into_fdb_error(self) -> Result<FdbError, Self> {
        match self {
            HcaError::FdbError(err) => Ok(err),
            _ => Err(self),
        }
    }
}

/// Represents a High Contention Allocator for a given subspace
#[derive(Debug)]
pub struct HighContentionAllocator {
    counters: Subspace,
    recent: Subspace,
    allocation_mutex: Mutex<()>,
}

impl HighContentionAllocator {
    /// Constructs an allocator that will use the input subspace for assigning values.
    /// The given subspace should not be used by anything other than the allocator
    pub fn new(subspace: Subspace) -> HighContentionAllocator {
        HighContentionAllocator {
            counters: subspace.subspace(&0i64),
            recent: subspace.subspace(&1i64),
            allocation_mutex: Mutex::new(()),
        }
    }

    /// Returns a byte string that
    ///   1) has never and will never be returned by another call to this method on the same subspace
    ///   2) is nearly as short as possible given the above
    pub async fn allocate(&self, trx: &Transaction) -> Result<i64, HcaError> {
        let (begin, end) = self.counters.range();
        let begin = KeySelector::first_greater_or_equal(begin);
        let end = KeySelector::first_greater_than(end);
        let counters_range = RangeOption {
            begin,
            end,
            limit: Some(1),
            reverse: true,
            ..RangeOption::default()
        };
        let mut rng: SmallRng = rand::make_rng();

        loop {
            let kvs = trx.get_range(&counters_range, 1, true).await?;

            let mut start: i64 = if let Some(first) = kvs.first() {
                self.counters.unpack(first.key())?
            } else {
                0
            };

            let mut window_advanced = false;

            let window = loop {
                let counters_start = self.counters.subspace(&start);

                let count_future = {
                    let _mutex_guard = self.allocation_mutex.lock()?;
                    if window_advanced {
                        trx.clear_range(self.counters.bytes(), counters_start.bytes());
                        trx.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                        trx.clear_range(self.recent.bytes(), self.recent.subspace(&start).bytes());
                    };

                    // Increment the allocation count for the current window
                    trx.atomic_op(counters_start.bytes(), ONE_BYTES, MutationType::Add);
                    trx.get(counters_start.bytes(), true)
                };

                let count_value = count_future.await?;
                let count = if let Some(count_value) = count_value {
                    if count_value.len() != 8 {
                        return Err(HcaError::InvalidDirectoryLayerMetadata);
                    }
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&count_value);
                    i64::from_le_bytes(bytes)
                } else {
                    0
                };

                let window = Self::window_size(start);
                if count * 2 < window {
                    break window;
                }

                start += window;
                window_advanced = true;
            };

            loop {
                // As of the snapshot being read from, the window is less than half
                // full, so this should be expected to take 2 tries.  Under high
                // contention (and when the window advances), there is an additional
                // subsequent risk of conflict for this transaction.
                let candidate: i64 = rng.random_range(start..start + window);
                let recent_candidate = self.recent.subspace(&candidate);

                let (latest_counter, candidate_value) = {
                    let _mutex_guard = self.allocation_mutex.lock()?;
                    let latest_counter = trx.get_range(&counters_range, 1, true);
                    let candidate_value = trx.get(recent_candidate.bytes(), false);
                    trx.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                    trx.set(recent_candidate.bytes(), &[]);
                    (latest_counter, candidate_value)
                };

                let (latest_counter, candidate_value) =
                    future::try_join(latest_counter, candidate_value).await?;

                let current_window_start: i64 = if let Some(first) = latest_counter.first() {
                    self.counters.unpack(first.key())?
                } else {
                    0
                };

                if current_window_start > start {
                    break;
                }

                if candidate_value.is_none() {
                    let mut after = recent_candidate.bytes().to_vec();
                    after.push(0x00);
                    trx.add_conflict_range(
                        recent_candidate.bytes(),
                        &after,
                        ConflictRangeType::Write,
                    )?;
                    return Ok(candidate);
                }
            }
        }
    }

    fn window_size(start: i64) -> i64 {
        // Larger window sizes are better for high contention, smaller sizes for
        // keeping the keys small.  But if there are many allocations, the keys
        // can't be too small.  So start small and scale up.  We don't want this to
        // ever get *too* big because we have to store about window_size/2 recent
        // items.
        match start {
            _ if start < 255 => 64,
            _ if start < 65535 => 1024,
            _ => 8192,
        }
    }
}
