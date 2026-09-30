// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! The read and write conflict range readers, against a live cluster.
//!
//! The `\xff\xff/transaction/{read,write}_conflict_range/` special keyspaces
//! exist from API version 630.

#[allow(unused_imports)]
use foundationdb::*;
#[allow(unused_imports)]
use foundationdb_macros::cfg_api_versions;

mod common;

/// Whether `ranges` holds exactly `begin..end`.
#[allow(dead_code)]
fn contains(ranges: &[ConflictRange], begin: &[u8], end: &[u8]) -> bool {
    ranges
        .iter()
        .any(|range| range.begin() == begin && range.end() == end)
}

/// A point read registers `key..key\x00`, a range read the range itself.
#[cfg_api_versions(min = 630)]
#[tokio::test]
async fn read_conflict_ranges_report_reads() -> FdbResult<()> {
    const POINT: &[u8] = b"test-rcr-point";
    const RANGE_BEGIN: &[u8] = b"test-rcr-range-a";
    const RANGE_END: &[u8] = b"test-rcr-range-z";

    let db = common::database().await?;
    let trx = db.create_trx()?;

    trx.get(POINT, false).await?;
    trx.get_range(
        &RangeOption::from((RANGE_BEGIN, RANGE_END)),
        1,
        false, // not a snapshot read: it must register a conflict range
    )
    .await?;

    let ranges = trx.read_conflict_ranges().await?;

    let mut point_key = POINT.to_vec();
    point_key.push(0);
    assert!(
        contains(&ranges, POINT, &point_key),
        "point read missing from {ranges:?}"
    );
    assert!(
        contains(&ranges, RANGE_BEGIN, RANGE_END),
        "range read missing from {ranges:?}"
    );

    Ok(())
}

/// A snapshot read is not serializable and registers no read conflict range.
#[cfg_api_versions(min = 630)]
#[tokio::test]
async fn snapshot_reads_register_no_read_conflict_range() -> FdbResult<()> {
    const KEY: &[u8] = b"test-rcr-snapshot";

    let db = common::database().await?;
    let trx = db.create_trx()?;

    trx.get(KEY, true).await?;

    assert!(trx.read_conflict_ranges().await?.is_empty());

    Ok(())
}

/// Writes register write conflict ranges, and only those.
#[cfg_api_versions(min = 630)]
#[tokio::test]
async fn write_conflict_ranges_report_writes() -> FdbResult<()> {
    const KEY: &[u8] = b"test-wcr-key";
    const CLEAR_BEGIN: &[u8] = b"test-wcr-cleared-a";
    const CLEAR_END: &[u8] = b"test-wcr-cleared-z";

    let db = common::database().await?;
    let trx = db.create_trx()?;

    trx.set(KEY, b"value");
    trx.clear_range(CLEAR_BEGIN, CLEAR_END);

    let ranges = trx.write_conflict_ranges().await?;

    let mut key_end = KEY.to_vec();
    key_end.push(0);
    assert!(
        contains(&ranges, KEY, &key_end),
        "set missing from {ranges:?}"
    );
    assert!(
        contains(&ranges, CLEAR_BEGIN, CLEAR_END),
        "cleared range missing from {ranges:?}"
    );

    // A write is not a read.
    assert!(trx.read_conflict_ranges().await?.is_empty());

    Ok(())
}

/// A range added without the associated read or write shows up in the keyspace
/// matching its type.
#[cfg_api_versions(min = 630)]
#[tokio::test]
async fn add_conflict_range_shows_up() -> FdbResult<()> {
    const READ_BEGIN: &[u8] = b"test-acr-read-a";
    const READ_END: &[u8] = b"test-acr-read-z";
    const WRITE_BEGIN: &[u8] = b"test-acr-write-a";
    const WRITE_END: &[u8] = b"test-acr-write-z";

    let db = common::database().await?;
    let trx = db.create_trx()?;

    trx.add_conflict_range(READ_BEGIN, READ_END, options::ConflictRangeType::Read)?;
    trx.add_conflict_range(WRITE_BEGIN, WRITE_END, options::ConflictRangeType::Write)?;

    let read_ranges = trx.read_conflict_ranges().await?;
    let write_ranges = trx.write_conflict_ranges().await?;

    assert!(
        contains(&read_ranges, READ_BEGIN, READ_END),
        "added read range missing from {read_ranges:?}"
    );
    assert!(
        contains(&write_ranges, WRITE_BEGIN, WRITE_END),
        "added write range missing from {write_ranges:?}"
    );
    assert!(!contains(&read_ranges, WRITE_BEGIN, WRITE_END));
    assert!(!contains(&write_ranges, READ_BEGIN, READ_END));

    Ok(())
}

/// A fresh transaction has no conflict range at all.
#[cfg_api_versions(min = 630)]
#[tokio::test]
async fn fresh_transaction_has_no_conflict_range() -> FdbResult<()> {
    let db = common::database().await?;
    let trx = db.create_trx()?;

    assert!(trx.read_conflict_ranges().await?.is_empty());
    assert!(trx.write_conflict_ranges().await?.is_empty());

    Ok(())
}

/// Hundreds of disjoint ranges do not fit in one `get_range` batch, so the
/// reader has to paginate and to carry a begin marker whose end marker lands in
/// the next batch. A single-batch read truncates the result here.
#[cfg_api_versions(min = 630)]
#[tokio::test]
async fn many_conflict_ranges_are_fully_paginated() -> FdbResult<()> {
    const RANGES: usize = 500;

    let db = common::database().await?;
    let trx = db.create_trx()?;

    for index in 0..RANGES {
        let begin = format!("test-many-rcr/{index:05}/a");
        let end = format!("test-many-rcr/{index:05}/b");
        trx.add_conflict_range(
            begin.as_bytes(),
            end.as_bytes(),
            options::ConflictRangeType::Read,
        )?;
    }

    let ranges = trx.read_conflict_ranges().await?;

    assert_eq!(ranges.len(), RANGES, "conflict ranges were truncated");
    for (index, range) in ranges.iter().enumerate() {
        assert_eq!(
            range.begin(),
            format!("test-many-rcr/{index:05}/a").as_bytes()
        );
        assert_eq!(
            range.end(),
            format!("test-many-rcr/{index:05}/b").as_bytes()
        );
    }

    Ok(())
}

/// Reading the conflict ranges is a binding-internal read: it must not move the
/// usage counters of the attempt, nor consume the client budget.
#[cfg_api_versions(min = 630)]
#[tokio::test]
async fn reading_conflict_ranges_is_unmetered() -> FdbResult<()> {
    const KEY: &[u8] = b"test-rcr-unmetered";

    let db = common::database().await?;
    let trx = db.create_trx()?;

    trx.set(KEY, b"value");
    let before = trx.attempt_usage();

    trx.read_conflict_ranges().await?;
    trx.write_conflict_ranges().await?;

    let after = trx.attempt_usage();
    assert_eq!(after.bytes_read, before.bytes_read);
    assert_eq!(after.call_get_range, before.call_get_range);
    assert_eq!(after.keys_values_fetched, before.keys_values_fetched);

    Ok(())
}
