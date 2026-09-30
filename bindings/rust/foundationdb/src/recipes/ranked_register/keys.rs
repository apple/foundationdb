// Copyright 2024 foundationdb-rs developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Key management for the ranked register
//!
//! Each [`RankedRegister`](super::RankedRegister) stores versioned metadata at
//! one key and its value in sequential child keys in its own [`Subspace`].
//!
//! # Key Schema
//!
//! ```text
//! <subspace>/state       -> (schema_version: u64, max_read_rank: u64, max_write_rank: u64)
//! <subspace>/value/<u64> -> raw value chunk
//! ```
//!
//! Value chunks use consecutive indices starting at zero. Each is raw bytes,
//! up to FoundationDB's exact 100,000-byte value limit. The metadata schema is
//! decoded strictly. Older unversioned state is unsupported, so use a fresh
//! subspace or clear an existing subspace before adopting this schema.
//!
//! Every ranked read or write for one subspace contends on this single key, so
//! those updates serialize through FoundationDB conflicts. Model a keyed
//! collection with one child subspace per logical key.

use crate::tuple::Subspace;

/// Key prefix for register state
const STATE_PREFIX: &str = "state";
const VALUE_PREFIX: &str = "value";

/// Generate the key for the register state
///
/// Returns the single key where all register state is stored.
///
/// # Key Structure
/// `<subspace>/state`
pub fn state_key(subspace: &Subspace) -> Vec<u8> {
    subspace.pack(&(STATE_PREFIX,))
}

/// Returns the child subspace containing raw value chunks.
pub(crate) fn value_subspace(subspace: &Subspace) -> Subspace {
    subspace.subspace(&(VALUE_PREFIX,))
}

/// Returns the key for one raw value chunk.
pub(crate) fn value_key(subspace: &Subspace, index: u64) -> Vec<u8> {
    value_subspace(subspace).pack(&(index,))
}
