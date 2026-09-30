// Copyright 2024 foundationdb-rs developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Ranked register algorithm
//!
//! Implements the core read/write/value operations from Chockler & Malkhi's
//! "Active Disk Paxos with infinitely many processes" (PODC 2002, Section 5.1, Figure 3).
//!
//! All functions operate within a FoundationDB transaction for atomicity.

use crate::{
    RangeOption, Transaction,
    tuple::{Subspace, pack, unpack},
};
use futures::TryStreamExt;
use std::ops::Deref;

use super::{
    MAX_VALUE_CHUNK_BYTES,
    errors::{RankedRegisterError, Result},
    keys,
    types::*,
};

const STATE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct MetadataState {
    max_read_rank: Rank,
    max_write_rank: Rank,
}

/// Reads versioned durable metadata. An absent key is the bottom state.
async fn read_metadata<T>(txn: &T, key: &[u8]) -> Result<Option<MetadataState>>
where
    T: Deref<Target = Transaction>,
{
    let Some(value) = txn.get(key, false).await? else {
        return Ok(None);
    };

    decode_metadata(&value).map(Some)
}

fn decode_metadata(value: &[u8]) -> Result<MetadataState> {
    let (schema_version, max_read_rank, max_write_rank): (u64, u64, u64) =
        unpack(value).map_err(|error| {
            RankedRegisterError::InvalidState(format!(
                "metadata does not match the versioned ranked-register schema: {error:?}"
            ))
        })?;
    if schema_version != STATE_SCHEMA_VERSION {
        return Err(RankedRegisterError::InvalidState(format!(
            "unknown metadata schema version {schema_version}, expected {STATE_SCHEMA_VERSION}"
        )));
    }

    Ok(MetadataState {
        max_read_rank: Rank::from(max_read_rank),
        max_write_rank: Rank::from(max_write_rank),
    })
}

fn write_metadata<T>(txn: &T, key: &[u8], state: MetadataState)
where
    T: Deref<Target = Transaction>,
{
    txn.set(
        key,
        &pack(&(
            STATE_SCHEMA_VERSION,
            state.max_read_rank.as_u64(),
            state.max_write_rank.as_u64(),
        )),
    );
}

/// Reads and validates all raw value chunks.
async fn read_value<T>(txn: &T, subspace: &Subspace, has_metadata: bool) -> Result<Option<Vec<u8>>>
where
    T: Deref<Target = Transaction>,
{
    let value_subspace = keys::value_subspace(subspace);
    let mut chunks = txn.get_ranges_keyvalues(RangeOption::from(&value_subspace), false);
    let mut expected_index = 0_u64;
    let mut value = Vec::new();
    let mut has_chunks = false;

    while let Some(chunk) = chunks.try_next().await? {
        if !has_metadata {
            return Err(RankedRegisterError::InvalidState(
                "value chunks exist without register metadata".to_owned(),
            ));
        }

        let index = decode_value_index(&value_subspace, chunk.key())?;
        validate_value_index(expected_index, index)?;
        expected_index = expected_index.checked_add(1).ok_or_else(|| {
            RankedRegisterError::InvalidState(
                "value chunk index sequence exceeds the supported u64 domain".to_owned(),
            )
        })?;
        value.extend_from_slice(chunk.value());
        has_chunks = true;
    }

    Ok(has_chunks.then_some(value))
}

fn decode_value_index(value_subspace: &Subspace, key: &[u8]) -> Result<u64> {
    let (index,): (u64,) = value_subspace.unpack(key).map_err(|error| {
        RankedRegisterError::InvalidState(format!("malformed value chunk key: {error:?}"))
    })?;
    Ok(index)
}

fn validate_value_index(expected: u64, actual: u64) -> Result<()> {
    if actual != expected {
        return Err(RankedRegisterError::InvalidState(format!(
            "value chunk index {actual} is not the expected contiguous index {expected}"
        )));
    }
    Ok(())
}

fn write_value<T>(txn: &T, subspace: &Subspace, value: &[u8]) -> Result<usize>
where
    T: Deref<Target = Transaction>,
{
    let value_subspace = keys::value_subspace(subspace);
    txn.clear_subspace_range(&value_subspace);

    if value.is_empty() {
        txn.set(&keys::value_key(subspace, 0), value);
        return Ok(1);
    }

    for (index, chunk) in value.chunks(MAX_VALUE_CHUNK_BYTES).enumerate() {
        let index = u64::try_from(index).map_err(|_| {
            RankedRegisterError::InvalidState(
                "value requires more chunks than the supported u64 index domain".to_owned(),
            )
        })?;
        txn.set(&keys::value_key(subspace, index), chunk);
    }

    Ok(value_chunk_count(value.len()))
}

fn value_chunk_count(value_len: usize) -> usize {
    if value_len == 0 {
        1
    } else {
        value_len / MAX_VALUE_CHUNK_BYTES + usize::from(value_len % MAX_VALUE_CHUNK_BYTES != 0)
    }
}

/// Perform a ranked read on the register.
///
/// Updates `max_read_rank` if the given rank is higher than the current one,
/// effectively installing a fence that prevents lower-ranked writes.
/// Returns the current write rank and value.
pub async fn read<T>(txn: &T, subspace: &Subspace, rank: Rank) -> Result<ReadResult>
where
    T: Deref<Target = Transaction>,
{
    let metadata_key = keys::state_key(subspace);
    let maybe_state = read_metadata(txn, &metadata_key).await?;
    let mut state = maybe_state.unwrap_or_default();
    let value = read_value(txn, subspace, maybe_state.is_some()).await?;

    if rank > state.max_read_rank {
        #[cfg(feature = "trace")]
        tracing::debug!(
            register_action = "fence_installed",
            rank = rank.as_u64(),
            previous_max_read_rank = state.max_read_rank.as_u64(),
            "ranked-register fence staged in transaction"
        );
        state.max_read_rank = rank;
        write_metadata(txn, &metadata_key, state);
    }

    Ok(ReadResult {
        write_rank: state.max_write_rank,
        value,
    })
}

/// Perform a ranked write on the register.
///
/// Commits the value only if:
/// - `rank >= max_read_rank` (no higher-ranked read has installed a fence)
/// - `rank > max_write_rank` (no equal-or-higher-ranked write has occurred)
///
/// Returns `Committed` if the write succeeds, `Aborted` otherwise.
pub async fn write<T>(
    txn: &T,
    subspace: &Subspace,
    rank: Rank,
    value: &[u8],
    max_value_bytes: Option<usize>,
) -> Result<WriteResult>
where
    T: Deref<Target = Transaction>,
{
    if let Some(limit) = max_value_bytes.filter(|limit| value.len() > *limit) {
        return Err(RankedRegisterError::ValueTooLarge {
            value_size: value.len(),
            limit,
        });
    }

    let metadata_key = keys::state_key(subspace);
    let maybe_state = read_metadata(txn, &metadata_key).await?;
    let mut state = maybe_state.unwrap_or_default();

    if rank < state.max_read_rank || rank <= state.max_write_rank {
        #[cfg(feature = "trace")]
        tracing::debug!(
            register_action = "write_aborted",
            rank = rank.as_u64(),
            max_read_rank = state.max_read_rank.as_u64(),
            max_write_rank = state.max_write_rank.as_u64(),
            fenced_by_read = rank < state.max_read_rank,
            fenced_by_write = rank <= state.max_write_rank,
            "ranked-register write rejected by fencing rank"
        );
        return Ok(WriteResult::Aborted);
    }

    #[cfg(feature = "trace")]
    let previous_max_write_rank = state.max_write_rank;
    state.max_write_rank = rank;
    write_metadata(txn, &metadata_key, state);
    #[cfg(feature = "trace")]
    let chunk_count = write_value(txn, subspace, value)?;
    #[cfg(not(feature = "trace"))]
    write_value(txn, subspace, value)?;

    #[cfg(feature = "trace")]
    tracing::debug!(
        register_action = "write_staged",
        rank = rank.as_u64(),
        previous_max_read_rank = state.max_read_rank.as_u64(),
        previous_max_write_rank = previous_max_write_rank.as_u64(),
        value_bytes = value.len(),
        value_chunks = chunk_count,
        "ranked-register write staged in transaction"
    );

    Ok(WriteResult::Committed)
}

/// Read the current value without updating ranks.
///
/// This is a plain read for followers and observers. It does not update
/// `max_read_rank`, so it installs no durable fence. As non-snapshot reads,
/// metadata and value chunks add conflict ranges for this register.
///
/// Safe to call from any process at any time.
pub async fn value<T>(txn: &T, subspace: &Subspace) -> Result<Option<Vec<u8>>>
where
    T: Deref<Target = Transaction>,
{
    let metadata_key = keys::state_key(subspace);
    let maybe_state = read_metadata(txn, &metadata_key).await?;
    read_value(txn, subspace, maybe_state.is_some()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_chunk_count_preserves_empty_values() {
        assert_eq!(value_chunk_count(0), 1);
        assert_eq!(value_chunk_count(MAX_VALUE_CHUNK_BYTES), 1);
        assert_eq!(value_chunk_count(MAX_VALUE_CHUNK_BYTES + 1), 2);
    }

    #[test]
    fn non_contiguous_value_index_is_invalid_state() {
        let error = validate_value_index(0, 1).expect_err("gapped value chunks must fail");
        assert!(matches!(error, RankedRegisterError::InvalidState(_)));
    }

    #[test]
    fn malformed_metadata_is_invalid_state() {
        let error = decode_metadata(&pack(&(0_u64, 0_u64)))
            .expect_err("untagged metadata must not be decoded");
        assert!(matches!(error, RankedRegisterError::InvalidState(_)));
    }

    #[test]
    fn unknown_metadata_version_is_invalid_state() {
        let error = decode_metadata(&pack(&(STATE_SCHEMA_VERSION + 1, 0_u64, 0_u64)))
            .expect_err("unknown schema versions must fail");
        assert!(matches!(error, RankedRegisterError::InvalidState(_)));
    }
}
