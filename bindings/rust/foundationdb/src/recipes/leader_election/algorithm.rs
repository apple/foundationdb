// Copyright 2024 foundationdb-rs developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Transactional implementation of the Dynamo-style lease state machine.

use crate::{
    Transaction,
    recipes::ranked_register::Rank,
    tuple::{Subspace, pack, unpack},
};
use std::ops::Deref;
use std::time::Duration;

use super::types::PendingNextState;
use super::{
    ElectionState, LeaderElectionError, Leadership, LocalState, Observation, ParticipantId,
    PollOutcome, PollResult, PollTransition, ResignOutcome, Result, keys::state_key,
};

const STATE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct DurableState {
    revision: u64,
    owner: Option<ParticipantId>,
    lease_duration: Option<Duration>,
}

async fn read_state<T>(txn: &T, key: &[u8]) -> Result<DurableState>
where
    T: Deref<Target = Transaction>,
{
    let Some(value) = txn.get(key, false).await? else {
        return Ok(DurableState::default());
    };

    decode_state(&value)
}

fn decode_state(value: &[u8]) -> Result<DurableState> {
    let (
        schema_version,
        revision,
        has_owner,
        owner,
        has_lease_duration,
        lease_duration_secs,
        lease_duration_subsec_nanos,
    ): (u64, u64, bool, String, bool, u64, u32) = unpack(value)?;
    if schema_version != STATE_SCHEMA_VERSION {
        return Err(LeaderElectionError::InvalidState(format!(
            "unknown durable state schema version {schema_version}, expected {STATE_SCHEMA_VERSION}"
        )));
    }
    let owner = if has_owner {
        Some(ParticipantId::new(owner)?)
    } else if owner.is_empty() {
        None
    } else {
        return Err(LeaderElectionError::InvalidState(
            "released state contains an owner ID".to_owned(),
        ));
    };
    if !has_lease_duration && (lease_duration_secs != 0 || lease_duration_subsec_nanos != 0) {
        return Err(LeaderElectionError::InvalidState(
            "missing lease duration has non-zero fields".to_owned(),
        ));
    }
    if has_lease_duration && lease_duration_subsec_nanos >= 1_000_000_000 {
        return Err(LeaderElectionError::InvalidState(
            "lease duration subsecond nanos is out of range".to_owned(),
        ));
    }
    let lease_duration =
        has_lease_duration.then(|| Duration::new(lease_duration_secs, lease_duration_subsec_nanos));
    if revision == 0 && (owner.is_some() || lease_duration.is_some()) {
        return Err(LeaderElectionError::InvalidState(
            "zero revision state is not empty".to_owned(),
        ));
    }
    if revision > 0 && lease_duration.is_none() {
        return Err(LeaderElectionError::InvalidState(
            "created state has no lease duration".to_owned(),
        ));
    }
    if lease_duration == Some(Duration::ZERO) {
        return Err(LeaderElectionError::InvalidState(
            "persisted lease duration is zero".to_owned(),
        ));
    }
    Ok(DurableState {
        revision,
        owner,
        lease_duration,
    })
}

fn write_state<T>(txn: &T, key: &[u8], state: &DurableState)
where
    T: Deref<Target = Transaction>,
{
    txn.set(key, &encode_state(state));
}

fn encode_state(state: &DurableState) -> Vec<u8> {
    let owner = state.owner.as_ref().map_or("", ParticipantId::as_str);
    let (lease_duration_secs, lease_duration_subsec_nanos) =
        state.lease_duration.map_or((0, 0), |duration| {
            (duration.as_secs(), duration.subsec_nanos())
        });
    pack(&(
        STATE_SCHEMA_VERSION,
        state.revision,
        state.owner.is_some(),
        owner,
        state.lease_duration.is_some(),
        lease_duration_secs,
        lease_duration_subsec_nanos,
    ))
}

fn next_revision(state: &DurableState) -> Result<u64> {
    state
        .revision
        .checked_add(1)
        .ok_or(LeaderElectionError::RevisionExhausted)
}

fn same_observation(state: &DurableState, observation: &Observation) -> bool {
    state.owner.as_ref() == Some(observation.owner())
        && state.revision == observation.rank().as_u64()
        && state.lease_duration == Some(observation.lease_duration())
}

fn valid_leadership(
    state: &DurableState,
    participant: &ParticipantId,
    leadership: &Leadership,
    now: Duration,
) -> bool {
    leadership.participant() == participant
        && state.owner.as_ref() == Some(participant)
        && state.revision == leadership.rank().as_u64()
        && state.lease_duration == Some(leadership.lease_duration())
        && now.saturating_sub(leadership.last_renewed_at()) < leadership.lease_duration()
}

fn leader_result<T>(
    txn: &T,
    key: &[u8],
    state: &DurableState,
    participant: &ParticipantId,
    lease_duration: Duration,
    attempt_started_at: Duration,
    transition: PollTransition,
) -> Result<PollResult>
where
    T: Deref<Target = Transaction>,
{
    let revision = next_revision(state)?;
    let next_state = DurableState {
        revision,
        owner: Some(participant.clone()),
        lease_duration: Some(lease_duration),
    };
    write_state(txn, key, &next_state);
    let rank = Rank::from(revision);

    #[cfg(feature = "trace")]
    let action = match transition {
        PollTransition::Acquired => "acquisition",
        PollTransition::Renewed => "renewal",
        PollTransition::TookOver => "takeover",
        PollTransition::Reacquired => "reacquisition",
        PollTransition::Followed => "follower",
    };

    #[cfg(feature = "trace")]
    tracing::debug!(
        poll_outcome = "leader",
        poll_action = action,
        participant = participant.as_str(),
        revision,
        takeover = transition == PollTransition::TookOver,
        reacquisition = transition == PollTransition::Reacquired,
        "leader-election poll staged in transaction"
    );

    Ok(PollResult::new(
        PollOutcome::Leader { rank, transition },
        PendingNextState::leadership(Leadership::new(
            participant.clone(),
            rank,
            lease_duration,
            attempt_started_at,
        )),
    ))
}

fn follower_result(
    state: &DurableState,
    previous: Option<&Observation>,
    _reason: &'static str,
) -> Result<PollResult> {
    let owner = state.owner.clone().ok_or_else(|| {
        LeaderElectionError::InvalidState(
            "follower result requested from released state".to_owned(),
        )
    })?;
    let lease_duration = state.lease_duration.ok_or_else(|| {
        LeaderElectionError::InvalidState(
            "follower result requested without lease duration".to_owned(),
        )
    })?;
    let rank = Rank::from(state.revision);
    let next_observation = match previous {
        Some(previous) if same_observation(state, previous) => {
            PendingNextState::preserve_observation(previous.clone())
        }
        Some(_) | None => PendingNextState::new_observation(owner.clone(), rank, lease_duration),
    };

    #[cfg(feature = "trace")]
    tracing::debug!(
        poll_outcome = "follower",
        poll_reason = _reason,
        owner = owner.as_str(),
        revision = rank.as_u64(),
        "leader-election poll observed owner"
    );

    let outcome = PollOutcome::Follower {
        owner,
        rank,
        lease_duration,
    };

    Ok(PollResult::new(outcome, next_observation))
}

pub(crate) async fn poll<T>(
    txn: &T,
    subspace: &Subspace,
    lease_duration: Duration,
    participant: &ParticipantId,
    local_state: &LocalState,
    attempt_started_at: Duration,
) -> Result<PollResult>
where
    T: Deref<Target = Transaction>,
{
    let key = state_key(subspace);
    let state = read_state(txn, &key).await?;

    match local_state {
        LocalState::Leadership(leadership)
            if valid_leadership(&state, participant, leadership, attempt_started_at) =>
        {
            leader_result(
                txn,
                &key,
                &state,
                participant,
                lease_duration,
                attempt_started_at,
                PollTransition::Renewed,
            )
        }
        LocalState::Observation(observation) if state.owner.is_some() => {
            if same_observation(&state, observation)
                && attempt_started_at.saturating_sub(observation.first_observed_at())
                    >= observation.lease_duration()
            {
                let transition = if observation.owner() == participant {
                    PollTransition::Reacquired
                } else {
                    PollTransition::TookOver
                };
                leader_result(
                    txn,
                    &key,
                    &state,
                    participant,
                    lease_duration,
                    attempt_started_at,
                    transition,
                )
            } else {
                follower_result(&state, Some(observation), "observation")
            }
        }
        LocalState::Unknown if state.owner.is_some() => follower_result(&state, None, "unknown"),
        LocalState::Leadership(_) if state.owner.is_some() => {
            follower_result(&state, None, "leadership_not_renewable")
        }
        LocalState::Observation(_) | LocalState::Unknown | LocalState::Leadership(_) => {
            leader_result(
                txn,
                &key,
                &state,
                participant,
                lease_duration,
                attempt_started_at,
                PollTransition::Acquired,
            )
        }
    }
}

pub(crate) async fn state<T>(txn: &T, subspace: &Subspace) -> Result<ElectionState>
where
    T: Deref<Target = Transaction>,
{
    let state = read_state(txn, &state_key(subspace)).await?;
    Ok(ElectionState::new(
        state.owner,
        Rank::from(state.revision),
        state.lease_duration,
    ))
}

pub(crate) async fn resign<T>(
    txn: &T,
    subspace: &Subspace,
    leadership: &Leadership,
) -> Result<ResignOutcome>
where
    T: Deref<Target = Transaction>,
{
    let key = state_key(subspace);
    let state = read_state(txn, &key).await?;
    if state.owner.as_ref() != Some(leadership.participant())
        || state.revision != leadership.rank().as_u64()
        || state.lease_duration != Some(leadership.lease_duration())
    {
        #[cfg(feature = "trace")]
        let rejection_reason = if state.owner.as_ref() != Some(leadership.participant()) {
            "owner_changed"
        } else if state.revision != leadership.rank().as_u64() {
            "revision_changed"
        } else {
            "lease_duration_changed"
        };
        #[cfg(feature = "trace")]
        tracing::debug!(
            resign_outcome = "rejected",
            resign_reason = rejection_reason,
            participant = leadership.participant().as_str(),
            leadership_revision = leadership.rank().as_u64(),
            revision = state.revision,
            "leader-election stale resignation rejected"
        );
        return Ok(ResignOutcome::Rejected);
    }

    let released = DurableState {
        revision: state.revision,
        owner: None,
        lease_duration: state.lease_duration,
    };
    write_state(txn, &key, &released);
    #[cfg(feature = "trace")]
    tracing::debug!(
        resign_outcome = "resigned",
        revision = released.revision,
        "leader-election resignation staged in transaction"
    );
    Ok(ResignOutcome::Resigned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_state_version_one_round_trips() {
        let state = DurableState {
            revision: 42,
            owner: Some(ParticipantId::new("alice-incarnation").unwrap()),
            lease_duration: Some(Duration::new(5, 123)),
        };

        assert_eq!(decode_state(&encode_state(&state)).unwrap(), state);
    }

    #[test]
    fn maximum_participant_id_keeps_durable_state_below_fdb_value_limit() {
        let owner =
            ParticipantId::new("\0".repeat((ParticipantId::MAX_ENCODED_BYTES - 2) / 2)).unwrap();
        assert_eq!(
            pack(&owner.as_str()).len(),
            ParticipantId::MAX_ENCODED_BYTES
        );
        let state = DurableState {
            revision: u64::MAX,
            owner: Some(owner),
            lease_duration: Some(Duration::new(u64::MAX, 999_999_999)),
        };

        assert!(encode_state(&state).len() <= 100_000);
    }

    #[test]
    fn unknown_durable_state_schema_version_is_rejected() {
        let value = pack(&(2_u64, 1_u64, true, "alice", true, 5_u64, 0_u32));

        assert!(matches!(
            decode_state(&value),
            Err(LeaderElectionError::InvalidState(message))
                if message.contains("unknown durable state schema version 2")
        ));
    }

    #[test]
    fn untagged_six_field_durable_state_is_rejected() {
        let value = pack(&(1_u64, true, "alice", true, 5_u64, 0_u32));

        assert!(matches!(
            decode_state(&value),
            Err(LeaderElectionError::PackError(_))
        ));
    }
}
