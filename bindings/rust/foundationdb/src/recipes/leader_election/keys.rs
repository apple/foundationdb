// Copyright 2024 foundationdb-rs developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Key management for leader election.
//!
//! A single key holds the durable coordination state. It stores the relative
//! lease duration, but no clock reading, absolute timestamp, renewal time, or
//! expiry deadline. Takeover timing is a local caller concern.
//!
//! # Value schema
//!
//! ```text
//! (schema_version: u64, revision: u64, has_owner: bool, owner: String,
//!  has_lease_duration: bool, lease_duration_secs: u64,
//!  lease_duration_subsec_nanos: u32)
//! ```
//!
//! The schema version is decoded strictly. Untagged layouts from development
//! versions of this recipe are unsupported: clear the election subspace before
//! using this version of the recipe. The election revision is also the fencing
//! rank for every paired ranked register, so clear those register subspaces
//! before reuse as well. Leaving an older register fence in place rejects the
//! new election's lower ranks.

use crate::tuple::Subspace;

const STATE_PREFIX: &str = "state";

/// Returns the single durable election-state key.
pub(crate) fn state_key(subspace: &Subspace) -> Vec<u8> {
    subspace.pack(&(STATE_PREFIX,))
}
