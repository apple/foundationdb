// Copyright 2024 foundationdb-rs developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Commit-log types for the leader-lease simulation.

use std::time::Duration;

use foundationdb::tuple::Versionstamp;

pub(crate) const OP_POLL: i64 = 0;
pub(crate) const OP_RESIGN: i64 = 1;
pub(crate) const OP_OBSERVE: i64 = 2;
pub(crate) const OP_STALE_WRITE: i64 = 3;

pub(crate) const LOCAL_UNKNOWN: i64 = 0;
pub(crate) const LOCAL_OBSERVATION: i64 = 1;
pub(crate) const LOCAL_LEADERSHIP: i64 = 2;

pub(crate) const TRANSITION_ACQUIRED: i64 = 0;
pub(crate) const TRANSITION_RENEWED: i64 = 1;
pub(crate) const TRANSITION_TOOK_OVER: i64 = 2;
pub(crate) const TRANSITION_REACQUIRED: i64 = 3;
pub(crate) const TRANSITION_FOLLOWED: i64 = 4;
pub(crate) const TRANSITION_NONE: i64 = 5;

/// One durable election state exposed by the recipe's public API.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct DurableState {
    pub(crate) rank: u64,
    pub(crate) owner: Option<String>,
    pub(crate) lease_duration: Option<Duration>,
}

/// Caller-owned state supplied to an operation, represented without recipe
/// private fields so commit-order replay can independently validate it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocalInput {
    Unknown,
    Observation {
        owner: String,
        rank: u64,
        lease_duration: Duration,
        observed_at: Duration,
    },
    Leadership {
        participant: String,
        rank: u64,
        lease_duration: Duration,
        renewed_at: Duration,
    },
}

/// One versionstamp-ordered simulation operation.
#[derive(Debug)]
pub(crate) struct LogEntry {
    pub(crate) versionstamp: Versionstamp,
    pub(crate) client_id: i32,
    pub(crate) incarnation: u64,
    pub(crate) op_num: u64,
    pub(crate) kind: i64,
    pub(crate) actor: String,
    pub(crate) prior: DurableState,
    pub(crate) current: DurableState,
    pub(crate) local_input: LocalInput,
    pub(crate) tracks_local_state: bool,
    pub(crate) attempt_started_at: Duration,
    pub(crate) configured_lease_duration: Duration,
    pub(crate) planned_adoption_delay: Option<Duration>,
    pub(crate) transition: i64,
    pub(crate) result: bool,
    pub(crate) requested_write_rank: u64,
    pub(crate) observed_write_rank: u64,
    pub(crate) observed_value: Option<Vec<u8>>,
    pub(crate) protected_write_committed: bool,
    pub(crate) payload: Vec<u8>,
}

pub(crate) struct Snapshot {
    pub(crate) election: DurableState,
    pub(crate) protected_rank: u64,
    pub(crate) protected_value: Option<Vec<u8>>,
}
