// Copyright 2024 foundationdb-rs developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Core data structures for the ranked register
//!
//! Implements the ranked register abstraction from Chockler & Malkhi's
//! "Active Disk Paxos with infinitely many processes" (PODC 2002).

use std::fmt;

/// A rank value for ordering register operations
///
/// Encodes both a process identifier and a sequence number into a single `u64`.
/// The high 32 bits hold the sequence number, and the low 32 bits hold the
/// process ID. This ensures that ranks from the same process are ordered by
/// sequence, and ties between different processes are broken by process ID.
///
/// # Encoding
///
/// ```text
/// |--- sequence (32 bits) ---|--- process_id (32 bits) ---|
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Rank(u64);

impl Rank {
    /// The zero rank, representing the bottom/uninitialized state
    pub const ZERO: Rank = Rank(0);

    /// Create a new rank from a process ID and sequence number
    ///
    /// The sequence occupies the high 32 bits and the process ID the low 32 bits,
    /// so ranks are ordered primarily by sequence, then by process ID.
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug"))]
    pub fn new(process_id: u32, sequence: u32) -> Self {
        Self(((sequence as u64) << 32) | process_id as u64)
    }

    /// Returns the process ID component of this rank
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn process_id(&self) -> u32 {
        self.0 as u32
    }

    /// Returns the sequence number component of this rank
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn sequence(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// Returns the raw `u64` representation
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Rank {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl fmt::Display for Rank {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Rank(seq={}, pid={})",
            self.sequence(),
            self.process_id()
        )
    }
}

/// Logical state of the ranked register
///
/// Tracks the maximum read and write ranks alongside the current value. The
/// implementation stores metadata and value chunks separately.
/// Private fields enforce invariants through the algorithm module.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RegisterState {
    pub(crate) max_read_rank: Rank,
    pub(crate) max_write_rank: Rank,
    pub(crate) value: Option<Vec<u8>>,
}

impl RegisterState {
    /// Returns the highest rank that has performed a read
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn max_read_rank(&self) -> Rank {
        self.max_read_rank
    }

    /// Returns the highest rank that has successfully written
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn max_write_rank(&self) -> Rank {
        self.max_write_rank
    }

    /// Returns the current value, if any
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

/// Result of a ranked read operation
///
/// Contains the write rank and value at the time of the read.
/// The read also installs a fence at the given rank, preventing
/// lower-ranked writes from succeeding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResult {
    pub(crate) write_rank: Rank,
    pub(crate) value: Option<Vec<u8>>,
}

impl ReadResult {
    /// Returns the rank of the last successful write
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn write_rank(&self) -> Rank {
        self.write_rank
    }

    /// Returns a reference to the current value, if any
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Consumes self and returns the value
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn into_value(self) -> Option<Vec<u8>> {
        self.value
    }
}

/// Result of a ranked write operation
#[must_use = "a committed write result controls whether co-staged application writes are valid"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteResult {
    /// The write was accepted (rank was high enough)
    Committed,
    /// The write was rejected (rank too low)
    Aborted,
}

impl WriteResult {
    /// Returns `true` if the write was committed
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn is_committed(&self) -> bool {
        matches!(self, WriteResult::Committed)
    }
}
