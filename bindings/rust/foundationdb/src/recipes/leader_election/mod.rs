// Copyright 2024 foundationdb-rs developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Poll-based Dynamo-style leader leases with FoundationDB fencing.
//!
//! This recipe adapts the client-side timing model of the
//! [Amazon DynamoDB lock client](https://aws.amazon.com/blogs/aws/new-amazon-dynamodb-lock-client/).
//! A caller observes a durable owner record and uses elapsed time from its own
//! monotonic clock to decide when that unchanged record is suspicious.
//! FoundationDB's serializable transaction on one state key, not local time,
//! resolves concurrent acquisition, renewal, reacquisition, and takeover.
//! The protocol is Dynamo-inspired, not wire-compatible with the DynamoDB lock
//! client.
//!
//! ## Durable state and local time
//!
//! Durable state contains an optional owner, a monotonically increasing
//! revision, and the last persisted relative lease duration. It contains no
//! wall-clock or monotonic-clock reading, last-renewed timestamp, deadline, or
//! expiry. A released record retains its revision and duration, while a
//! never-created record has revision zero and no duration.
//!
//! [`Leadership`](crate::recipes::leader_election::Leadership) and
//! [`Observation`](crate::recipes::leader_election::Observation) are caller-local
//! state. A leadership token may renew only while elapsed time on that caller's
//! clock is below the duration stored in its exact durable revision. A follower
//! waits the duration stored in its exact observation, never this handle's
//! configured duration.
//! If the observed owner, revision, or duration changes, the caller starts a
//! new observation window. If it is unchanged, the original observation time
//! is retained.
//!
//! Clocks are never persisted or compared across processes. They only measure
//! elapsed time for the caller that recorded them. On process restart, use a
//! fresh [`ParticipantId`](crate::recipes::leader_election::ParticipantId) and
//! begin again with
//! [`LocalState::Unknown`](crate::recipes::leader_election::LocalState::Unknown),
//! so the new incarnation observes the durable state and waits anew before
//! attempting takeover.
//!
//! ## Cutover from v0.11 durable state
//!
//! This release's durable state is intentionally incompatible with v0.11.
//! v0.11 and new clients use different keys, so a mixed deployment is unsafe:
//! each population can elect a leader without observing or fencing the other.
//!
//! Upgrade by a destructive cutover, not an in-place migration. Stop or
//! quiesce every old participant and all protected work before starting the
//! new deployment. Then allocate fresh subspaces or epochs for the new
//! election, its [`RankedRegister`](crate::recipes::ranked_register::RankedRegister),
//! and the protected sink or rank namespace. Only then start clients using
//! this release. This recipe neither reads nor migrates v0.11 durable state.
//!
//! ## Poll lifecycle
//!
//! Call [`LeaderElection::poll`](crate::recipes::leader_election::LeaderElection::poll)
//! inside the closure passed to
//! [`Database::run`](crate::Database::run). Read `attempt_started_at` from the
//! caller's monotonic clock immediately before each `poll` call, including
//! every retry attempt. It controls renewal and takeover eligibility and stamps
//! new leadership, so time spent reading, retrying, or committing only shortens
//! local validity.
//!
//! A returned [`PollResult`](crate::recipes::leader_election::PollResult) is
//! only prepared state. Adopt it with
//! [`PollResult::into_next_state`](crate::recipes::leader_election::PollResult::into_next_state)
//! after the outer `Database::run` succeeds. Use a fresh `adopted_at` reading
//! then: it starts timing for a new or reset observation only after its durable
//! read is known to have committed. An unchanged observation and a new
//! leadership token retain their original attempt-local times. This prevents
//! retries, cancellation, and unknown commits from authorizing work based on
//! uncommitted caller-local state.
//!
//! The poll transaction reads and, for a leadership transition, writes the
//! same durable key. FoundationDB conflict resolution serializes competing
//! transitions. Local time only permits an attempted conditional takeover; it
//! does not prove that the prior process stopped.
//!
//! ## Renewal cadence and fencing epochs
//!
//! Renew with substantial headroom before local expiry. A cadence around one
//! third of a lease leaves more room than polling at half a lease for
//! scheduling delay, transaction retries, and commit latency. Tune it to the
//! application's latency budget. This is availability guidance, not a safety
//! condition or a durable expiry.
//!
//! A successful acquisition, renewal, reacquisition, or takeover returns a
//! fresh [`Rank`](crate::recipes::ranked_register::Rank). Each is a new fencing
//! epoch, including renewal by the same participant. Once the newer rank is
//! installed, protected work using an older rank, even from that same process,
//! can be rejected. Leadership status alone never authorizes an unfenced
//! external side effect.
//!
//! Ranks returned by this recipe are opaque durable revisions. Do not mix them
//! with [`Rank::new`](crate::recipes::ranked_register::Rank::new) values in the
//! same ranked register or rank space. A manually constructed rank can exceed
//! every future election revision and permanently fence election-backed work.
//!
//! Correctness-sensitive FoundationDB work must use the rank with a
//! [`RankedRegister`](crate::recipes::ranked_register::RankedRegister) in the
//! same enclosing transaction. An external sink must atomically enforce the
//! rank and reject older ranks. See the
//! [`RankedRegister` composition example](crate::recipes::ranked_register#composing-with-leader-election)
//! rather than treating a successful poll as sufficient authorization.
//!
//! ## Safety versus liveness
//!
//! Local expiry does not revoke durable ownership. It only prevents this caller
//! from renewing with its local token and can lead a follower with an unchanged
//! observation to attempt takeover. A failed or unavailable poll cannot renew
//! leadership, so callers must stop protected work that depends on a stale
//! local token. Fencing ranks, not timing alone, protect against a delayed or
//! partitioned process.
//!
//! Do not rely on a background heartbeat that cannot interrupt, fence, or stop
//! in-progress protected work. It can renew a lease, but the protected-work
//! path must still stop when it cannot obtain and use a current fencing rank.
//!
//! ## Protocol walkthrough
//!
//! 1. Create a non-zero-duration
//!    [`LeaderElection`](crate::recipes::leader_election::LeaderElection) and a
//!    fresh [`ParticipantId`](crate::recipes::leader_election::ParticipantId)
//!    for this process incarnation. Start with
//!    [`LocalState::Unknown`](crate::recipes::leader_election::LocalState::Unknown).
//! 2. Poll in a `Database::run` attempt. A released or never-created state
//!    produces [`PollOutcome::Leader`](crate::recipes::leader_election::PollOutcome::Leader)
//!    with [`PollTransition::Acquired`](crate::recipes::leader_election::PollTransition::Acquired).
//!    After the outer transaction succeeds, adopt its
//!    [`Leadership`](crate::recipes::leader_election::Leadership) through
//!    [`PollResult::into_next_state`](crate::recipes::leader_election::PollResult::into_next_state).
//! 3. A caller that sees another owner receives
//!    [`PollOutcome::Follower`](crate::recipes::leader_election::PollOutcome::Follower).
//!    Its next [`LocalState`](crate::recipes::leader_election::LocalState)
//!    contains an [`Observation`](crate::recipes::leader_election::Observation)
//!    of that exact owner, revision, duration, and local observation time.
//!    Repeated polls preserve that time only while the durable record is
//!    unchanged.
//! 4. The current holder polls with its matching, locally unexpired
//!    [`Leadership`](crate::recipes::leader_election::Leadership) and receives
//!    [`PollTransition::Renewed`](crate::recipes::leader_election::PollTransition::Renewed)
//!    with a new rank. An unchanged observation that has waited at least its
//!    persisted duration permits
//!    [`PollTransition::TookOver`](crate::recipes::leader_election::PollTransition::TookOver),
//!    or [`PollTransition::Reacquired`](crate::recipes::leader_election::PollTransition::Reacquired)
//!    when the observer is the same participant.
//! 5. For every leader outcome, co-commit the protected FoundationDB work with
//!    the returned [`Rank`](crate::recipes::ranked_register::Rank), including
//!    [`RankedRegister::read`](crate::recipes::ranked_register::RankedRegister::read)
//!    to install its fence. A later rank fences delayed work using every older
//!    rank.
//! 6. A holder may call
//!    [`LeaderElection::resign`](crate::recipes::leader_election::LeaderElection::resign)
//!    with its exact leadership token. The conditional release preserves the
//!    revision, so the next acquisition receives a strictly newer rank. A stale
//!    resignation is rejected.
//! 7. If the outer run retries, fails, is cancelled, or has an unknown commit,
//!    do not adopt its `PollResult`. The next successful run rediscovers the
//!    durable state. After restart, discard all local state, generate a fresh
//!    participant ID, and follow the observation path again.
//!
//! ## Caller responsibilities
//!
//! The caller owns scheduling, retry policy, transaction options, sleeping,
//! randomization, background work, and caller-local state. This component does
//! not call `Database::run`, retry, set transaction options, sleep, draw random
//! values, start background work, or read wall-clock time.
//!
//! ## Further reading
//!
//! - [AWS Builders Library: Leader Election in Distributed Systems](https://aws.amazon.com/builders-library/leader-election-in-distributed-systems/)
//! - [Martin Kleppmann: How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
//! - Mike Burrows, "The Chubby Lock Service for Loosely-Coupled Distributed
//!   Systems" (OSDI 2006).
//! - Gregory Chockler and Dahlia Malkhi, "Active Disk Paxos with Infinitely
//!   Many Processes" (PODC 2002).
//! - Salman Niazi, Mahmoud Ismail, Gautier Berthou, and Jim Dowling, "Leader
//!   Election Using NewSQL Database Systems" (DAIS 2015, LNCS 9038).
//! - [AWS Labs: Amazon DynamoDB Lock Client](https://github.com/awslabs/amazon-dynamodb-lock-client)
//! - [Apache ZooKeeper Recipes, GUID note](https://zookeeper.apache.org/doc/r3.5.5/recipes.html)

mod algorithm;
mod errors;
mod keys;
mod types;

pub use errors::{LeaderElectionError, Result};
pub use types::{
    ElectionState, Leadership, LocalState, Observation, ParticipantId, PollOutcome, PollResult,
    PollTransition, ResignOutcome,
};

use crate::{Transaction, tuple::Subspace};
use std::ops::Deref;
use std::time::Duration;

/// A handle for one independently scoped leader lease.
///
/// `lease_duration` is this caller's non-zero desired duration. It is
/// persisted on every successful acquisition, takeover, and renewal. Existing
/// foreign records remain governed by their own persisted duration. The handle
/// holds no caller-local state and performs no scheduling or retries.
#[derive(Clone, Debug)]
pub struct LeaderElection {
    subspace: Subspace,
    lease_duration: Duration,
}

impl LeaderElection {
    /// Creates an election handle with the non-zero duration it will persist on
    /// successful ownership changes.
    ///
    /// Constructing a handle does not read or initialize durable state.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(subspace))
    )]
    pub fn new(subspace: Subspace, lease_duration: Duration) -> Result<Self> {
        if lease_duration.is_zero() {
            return Err(LeaderElectionError::InvalidLeaseDuration);
        }
        Ok(Self {
            subspace,
            lease_duration,
        })
    }

    /// Returns this handle's desired duration for its future owner records.
    #[cfg_attr(feature = "trace", tracing::instrument(level = "debug", skip(self)))]
    pub fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    /// Polls the durable lease state in the caller's transaction.
    ///
    /// A released state is acquired immediately. An exact, locally unexpired
    /// [`Leadership`] token renews ownership with a fresh revision. A first or
    /// changed [`Observation`] never steals; only an exact unchanged observation
    /// may take over, or same-owner reacquire, after the observed record's
    /// persisted duration. An expired or mismatched leadership token becomes
    /// observation/reacquisition state and cannot renew directly.
    ///
    /// `attempt_started_at` must be read from the caller's monotonic clock
    /// immediately before this call in each retry attempt. It is deliberately
    /// before the durable read, making renewal and takeover decisions
    /// conservative relative to read and commit delay. After the enclosing
    /// `Database::run` succeeds, pass a fresh caller-clock reading to
    /// [`PollResult::into_next_state`] to adopt the returned state.
    ///
    /// Renew well before the local deadline represented by [`Leadership`],
    /// leaving headroom for scheduling delay, retries, and commit latency.
    /// This affects availability only; it is not a durable expiry or safety
    /// condition.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(
            level = "debug",
            skip(self, txn, participant, local_state),
            fields(participant = participant.as_str())
        )
    )]
    pub async fn poll<T>(
        &self,
        txn: &T,
        participant: &ParticipantId,
        local_state: &LocalState,
        attempt_started_at: Duration,
    ) -> Result<PollResult>
    where
        T: Deref<Target = Transaction>,
    {
        algorithm::poll(
            txn,
            &self.subspace,
            self.lease_duration,
            participant,
            local_state,
            attempt_started_at,
        )
        .await
    }

    /// Reads durable state without making any liveness or leadership-validity claim.
    ///
    /// This is a snapshot only. It does not create a local observation, permit
    /// renewal or takeover, or authorize protected work.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, txn))
    )]
    pub async fn state<T>(&self, txn: &T) -> Result<ElectionState>
    where
        T: Deref<Target = Transaction>,
    {
        algorithm::state(txn, &self.subspace).await
    }

    /// Releases ownership only when `leadership` still exactly matches durable state.
    ///
    /// The revision and persisted duration remain, so a later acquisition has
    /// a strictly newer fencing rank. A stale delayed resignation is rejected.
    /// This conditional operation does not make a liveness claim and may be
    /// used to relinquish an otherwise exact durable token after its local
    /// renewal window has elapsed.
    #[cfg_attr(
        feature = "trace",
        tracing::instrument(
            level = "debug",
            skip(self, txn, leadership),
            fields(
                participant = leadership.participant().as_str(),
                leadership_revision = leadership.rank().as_u64()
            )
        )
    )]
    pub async fn resign<T>(&self, txn: &T, leadership: &Leadership) -> Result<ResignOutcome>
    where
        T: Deref<Target = Transaction>,
    {
        algorithm::resign(txn, &self.subspace, leadership).await
    }
}
