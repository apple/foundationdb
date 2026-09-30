// Copyright 2024 foundationdb-rs developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! # Ranked Register for FoundationDB
//!
//! A shared memory abstraction that encapsulates Paxos ballots, based on
//! Chockler & Malkhi's "Active Disk Paxos with infinitely many processes"
//! (PODC 2002). A ranked register is a mutable register with conflict detection
//! via ranks, supporting unbounded processes with finite storage.
//!
//! Sections 4.2 and 4.3 of the paper use one logical ranked register. Section
//! 5.1 implements it from one read-modify-write object, while Section 5.2 uses
//! `n` registers as replicas to emulate one fault-tolerant logical register.
//! This implementation is the single logical cell because FoundationDB already
//! supplies replication and transactions. Multiple child-subspace registers are
//! application sharding, not a replication requirement from the paper.
//!
//! ## Operations
//!
//! | Operation | Who | Effect |
//! |-----------|-----|--------|
//! | [`read(rank)`](crate::recipes::ranked_register::RankedRegister::read) | Leader | Raises max_read_rank only for a higher rank, returns current value |
//! | [`write(rank, value)`](crate::recipes::ranked_register::RankedRegister::write) | Leader | Commits only if rank is high enough |
//! | [`value()`](crate::recipes::ranked_register::RankedRegister::value) | Followers | Plain read, no fence installed |
//!
//! ## Addressing, schema, and capacity
//!
//! One [`RankedRegister`](crate::recipes::ranked_register::RankedRegister) owns
//! one [`Subspace`](crate::tuple::Subspace). Its internal `"state"` key stores
//! a versioned metadata tuple, while raw value bytes are stored in sequential
//! `"value"/<u64 index>` child keys. For a keyed collection, derive a child
//! subspace from each logical key before constructing the register:
//!
//! ```rust
//! use foundationdb::{recipes::ranked_register::RankedRegister, tuple::Subspace};
//!
//! let registers = Subspace::all().subspace(&"document-registers");
//! let document_id = "document-42";
//! let register = RankedRegister::new(registers.subspace(&(document_id,)));
//! # let _ = register;
//! ```
//!
//! Each value chunk is raw bytes and may be exactly
//! [`MAX_VALUE_CHUNK_BYTES`](crate::recipes::ranked_register::MAX_VALUE_CHUNK_BYTES)
//! bytes. [`RankedRegister::new`](crate::recipes::ranked_register::RankedRegister::new)
//! imposes no recipe aggregate limit, although
//! FoundationDB transaction limits remain the backend boundary. Use
//! [`RankedRegister::with_max_value_bytes`](crate::recipes::ranked_register::RankedRegister::with_max_value_bytes)
//! to impose a local aggregate limit
//! on one handle. That limit is never stored in FoundationDB, so all handles
//! that write through the same subspace should use compatible limits.
//!
//! This schema is intentionally incompatible with ranked-register state from
//! v0.11 and earlier. It does not decode the former unversioned tuple layout.
//! Start with a fresh subspace, or clear an existing register subspace before
//! using this version.
//!
//! Ranked reads and writes for one register contend on that single key and are
//! serialized by FoundationDB conflicts. Use separate child subspaces to shard
//! independently updated logical items.
//!
//! ## Rank domains
//!
//! A register rank space has one authority. Do not mix
//! [`Rank::new`](crate::recipes::ranked_register::Rank::new) values with
//! leader-election ranks or values issued by another rank allocator in the
//! same register. A rank from a different domain can permanently fence valid
//! future writes from the intended authority.
//!
//! Although the primitive stores one optional value, a successful ranked write
//! can fence additional application-key writes staged in the same transaction.
//! Stage those writes only when
//! [`WriteResult::Committed`](crate::recipes::ranked_register::WriteResult::Committed)
//! is returned. One rank can commit only once per register because each write
//! requires a rank strictly greater than the stored maximum write rank.
//!
//! ## Composing with Leader Election
//!
//! The ranked register is designed to work with the leader election recipe.
//! Every successful leader poll returns a fencing rank derived from the durable
//! revision, providing automatic fencing against stale leaders. This includes
//! same-owner renewal: installing the new rank fences delayed work using the
//! prior rank from that same process.
//!
//! ```rust,no_run
//! # #[cfg(feature = "recipes-leader-election")]
//! # mod leader_election_example {
//! # async fn example(db: &foundationdb::Database) -> Result<(), foundationdb::FdbBindingError> {
//! use std::time::Duration;
//!
//! use foundationdb::{
//!     env::{Clock, Environment},
//!     options::TransactionOption,
//!     recipes::{
//!         leader_election::{LeaderElection, LocalState, ParticipantId, PollOutcome},
//!         ranked_register::{RankedRegister, WriteResult},
//!     },
//!     tuple::Subspace,
//!     FdbBindingError,
//! };
//!
//! let election = LeaderElection::new(
//!     Subspace::all().subspace(&"my-election"),
//!     Duration::from_secs(10),
//! )?;
//! let register = RankedRegister::new(Subspace::all().subspace(&"my-state"));
//! let participant = ParticipantId::new("process-incarnation")?;
//! let local_state = LocalState::unknown();
//! let env = Environment::default();
//!
//! // The application owns retries, options, scheduling, and local observation.
//! let result = db.run(|txn, _maybe_committed| {
//!     let election = election.clone();
//!     let register = register.clone();
//!     let participant = participant.clone();
//!     let local_state = local_state.clone();
//!     let env = env.clone();
//!     async move {
//!         txn.set_option(TransactionOption::AutomaticIdempotency)?;
//!         let attempt_started_at = env.clock().monotonic();
//!         let poll = election
//!             .poll(&txn, &participant, &local_state, attempt_started_at)
//!             .await?;
//!         if let PollOutcome::Leader { rank, .. } = poll.outcome() {
//!             register
//!                 .read(&txn, *rank)
//!                 .await
//!                 .map_err(|error| FdbBindingError::new_custom_error(Box::new(error)))?;
//!             let write_result = register
//!                 .write(&txn, *rank, b"new_value")
//!                 .await
//!                 .map_err(|error| FdbBindingError::new_custom_error(Box::new(error)))?;
//!             if write_result == WriteResult::Committed {
//!                 txn.set(b"application-key", b"new_value");
//!             }
//!         }
//!         Ok::<_, FdbBindingError>(poll)
//!     }
//! }).await?;
//! // Adopt this only after db.run succeeded.
//! let local_state = result.into_next_state(env.clock().monotonic());
//! # let _ = local_state;
//! # Ok(())
//! # }
//! # }
//! ```
//!
//! ### Why This Works
//!
//! - Durable leader-election revisions increase monotonically
//! - A renewal is a new fencing epoch, even for the same leader
//! - `read(rank)` installs a fence at that fencing rank
//! - Any write with a lower fencing rank is automatically rejected
//! - `value()` is safe for followers, it never installs a fence

mod algorithm;
mod errors;
mod keys;
mod types;

pub use errors::{RankedRegisterError, Result};
pub use types::{Rank, ReadResult, RegisterState, WriteResult};

use crate::{Transaction, tuple::Subspace};
use std::ops::Deref;

/// Maximum size of one raw register-value chunk.
///
/// This is FoundationDB's exact 100,000-byte value limit.
pub const MAX_VALUE_CHUNK_BYTES: usize = 100_000;

/// A ranked register backed by FoundationDB
///
/// Provides a mutable register with conflict detection via ranks.
/// No initialization is needed — an absent key represents the bottom state
/// (zero ranks, no value).
///
/// # Thread Safety
///
/// `RankedRegister` is [`Clone`], [`Send`], and [`Sync`]. It holds only a
/// [`Subspace`] and can be safely shared across tasks.
#[derive(Clone, Debug)]
pub struct RankedRegister {
    subspace: Subspace,
    max_value_bytes: Option<usize>,
}

impl RankedRegister {
    /// Create a new ranked register instance
    ///
    /// The subspace isolates this register from other data in the database.
    /// No initialization step is required — the register starts in the
    /// bottom state (zero ranks, no value) until the first write.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(subspace))
    )]
    pub fn new(subspace: Subspace) -> Self {
        Self {
            subspace,
            max_value_bytes: None,
        }
    }

    /// Create a ranked register with a local aggregate value-size limit.
    ///
    /// The limit applies only to writes through this handle. It is not durable
    /// state, so use compatible limits for all writers of the same subspace.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(subspace))
    )]
    pub fn with_max_value_bytes(subspace: Subspace, limit: usize) -> Self {
        Self {
            subspace,
            max_value_bytes: Some(limit),
        }
    }

    /// Returns a reference to the underlying subspace
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn subspace(&self) -> &Subspace {
        &self.subspace
    }

    /// Perform a ranked read
    ///
    /// Raises `max_read_rank` only when the given rank is higher, installing a
    /// fence that prevents lower-ranked writes. A superseded rank returns the
    /// current write rank and value without changing the installed fence.
    ///
    /// Used by the leader before writing to ensure consistency.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, txn))
    )]
    pub async fn read<T>(&self, txn: &T, rank: Rank) -> Result<ReadResult>
    where
        T: Deref<Target = Transaction>,
    {
        algorithm::read(txn, &self.subspace, rank).await
    }

    /// Perform a ranked write
    ///
    /// Commits the value only if:
    /// - `rank >= max_read_rank` (no higher fence)
    /// - `rank > max_write_rank` (no equal-or-higher write)
    ///
    /// Returns [`WriteResult::Committed`] or [`WriteResult::Aborted`].
    /// Returns [`RankedRegisterError::ValueTooLarge`] when this handle has a
    /// configured limit and `value` exceeds it.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, txn, value))
    )]
    pub async fn write<T>(&self, txn: &T, rank: Rank, value: &[u8]) -> Result<WriteResult>
    where
        T: Deref<Target = Transaction>,
    {
        algorithm::write(txn, &self.subspace, rank, value, self.max_value_bytes).await
    }

    /// Read the current value without updating ranks
    ///
    /// Safe for followers and observers: it installs no durable fence. Its
    /// normal non-snapshot FoundationDB read still adds a conflict range for
    /// this register key, so it can conflict with a concurrent leader write.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, txn))
    )]
    pub async fn value<T>(&self, txn: &T) -> Result<Option<Vec<u8>>>
    where
        T: Deref<Target = Transaction>,
    {
        algorithm::value(txn, &self.subspace).await
    }
}
