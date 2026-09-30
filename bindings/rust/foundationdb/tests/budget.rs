// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Per-attempt usage accounting and client-side budget, against a live cluster.

use foundationdb::options::StreamingMode;
use foundationdb::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

mod common;

/// `transaction_too_old`, retryable, retried without any backoff.
const TRANSACTION_TOO_OLD: i32 = 1007;

/// A clock the test moves by hand, in milliseconds, standing in for the
/// simulated time a workload would run on.
#[derive(Debug, Clone, Default)]
struct FakeClock(Arc<AtomicU64>);

impl FakeClock {
    fn set_millis(&self, millis: u64) {
        self.0.store(millis, Ordering::Relaxed);
    }
}

impl Clock for FakeClock {
    fn monotonic(&self) -> Duration {
        Duration::from_millis(self.0.load(Ordering::Relaxed))
    }

    fn wall(&self) -> Duration {
        self.monotonic()
    }
}

/// Writes are accounted as soon as they are issued, no commit needed.
#[tokio::test]
async fn usage_counts_writes() -> FdbResult<()> {
    const KEY: &[u8] = b"test-budget-writes-key";
    const VALUE: &[u8] = b"value";
    const PARAM: &[u8] = b"\x01\x00\x00\x00\x00\x00\x00\x00";
    const BEGIN: &[u8] = b"test-budget-writes-";
    const END: &[u8] = b"test-budget-writes.";

    let db = common::database().await?;
    let trx = db.create_trx()?;

    trx.set(KEY, VALUE);
    trx.clear(KEY);
    trx.clear_range(BEGIN, END);
    trx.atomic_op(KEY, PARAM, options::MutationType::Add);

    let usage = trx.attempt_usage();
    assert_eq!(usage.call_set, 1);
    assert_eq!(usage.call_clear, 1);
    assert_eq!(usage.call_clear_range, 1);
    assert_eq!(usage.call_atomic_op, 1);
    assert_eq!(
        usage.bytes_written,
        (KEY.len() + VALUE.len() + KEY.len() + BEGIN.len() + END.len() + KEY.len() + PARAM.len())
            as u64
    );
    assert_eq!(usage.bytes_read, 0);

    Ok(())
}

/// Reads are accounted when their future resolves.
#[tokio::test]
async fn usage_counts_reads() -> FdbResult<()> {
    const PREFIX: &[u8] = b"test-budget-reads-";
    const MISSING: &[u8] = b"test-budget-reads-missing-key";
    const VALUE: &[u8] = b"value";

    let keys: Vec<Vec<u8>> = (0..3)
        .map(|i| [PREFIX, format!("{i}").as_bytes()].concat())
        .collect();

    let db = common::database().await?;

    {
        let trx = db.create_trx()?;
        trx.clear_range(PREFIX, b"test-budget-reads.");
        for key in &keys {
            trx.set(key, VALUE);
        }
        trx.commit().await.expect("failed to commit");
    }

    // get, on an existing then on a missing key.
    let trx = db.create_trx()?;
    trx.get(&keys[0], false).await?;
    trx.get(MISSING, false).await?;

    let usage = trx.attempt_usage();
    assert_eq!(usage.call_get, 2);
    assert_eq!(usage.keys_values_fetched, 1);
    assert_eq!(
        usage.bytes_read,
        (keys[0].len() + VALUE.len() + MISSING.len()) as u64
    );

    // get_key counts as a get of the selector key plus the resolved key.
    let trx = db.create_trx()?;
    let selector = KeySelector::first_greater_or_equal(keys[0].as_slice());
    let resolved = trx.get_key(&selector, false).await?;

    let usage = trx.attempt_usage();
    assert_eq!(usage.call_get, 1);
    assert_eq!(usage.keys_values_fetched, 0);
    assert_eq!(usage.bytes_read, (keys[0].len() + resolved.len()) as u64);

    // get_range counts one call per resolved batch.
    let trx = db.create_trx()?;
    let opt = RangeOption {
        mode: StreamingMode::WantAll,
        ..RangeOption::from((PREFIX, b"test-budget-reads.".as_ref()))
    };
    let values = trx.get_range(&opt, 1, false).await?;
    assert_eq!(values.len(), keys.len());

    let usage = trx.attempt_usage();
    assert_eq!(usage.call_get_range, 1);
    assert_eq!(usage.call_get, 0);
    assert_eq!(usage.keys_values_fetched, keys.len() as u64);
    assert_eq!(
        usage.bytes_read,
        keys.iter().map(|k| (k.len() + VALUE.len()) as u64).sum()
    );

    Ok(())
}

#[tokio::test]
async fn budget_exceeded_on_bytes_written() -> FdbResult<()> {
    const KEY: &[u8] = b"test-budget-bytes-written-key";
    const VALUE: &[u8] = b"a-value-larger-than-the-limit";

    let db = common::database().await?;
    let trx = db.create_trx()?;

    trx.set_client_budget(ClientBudget {
        max_bytes_written: Some(8),
        ..ClientBudget::default()
    });
    assert!(trx.check_client_budget().is_ok());

    trx.set(KEY, VALUE);

    let err = trx.check_client_budget().unwrap_err();
    assert_eq!(err.kind, BudgetKind::BytesWritten);
    assert_eq!(err.used, (KEY.len() + VALUE.len()) as u64);
    assert_eq!(err.limit, 8);

    Ok(())
}

#[tokio::test]
async fn budget_exceeded_on_bytes_read() -> FdbResult<()> {
    const KEY: &[u8] = b"test-budget-bytes-read-key";
    const VALUE: &[u8] = b"a-value-larger-than-the-limit";

    let db = common::database().await?;

    {
        let trx = db.create_trx()?;
        trx.set(KEY, VALUE);
        trx.commit().await.expect("failed to commit");
    }

    let trx = db.create_trx()?;
    trx.set_client_budget(ClientBudget {
        max_bytes_read: Some(8),
        ..ClientBudget::default()
    });
    assert!(trx.check_client_budget().is_ok());

    trx.get(KEY, false).await?;

    let err = trx.check_client_budget().unwrap_err();
    assert_eq!(err.kind, BudgetKind::BytesRead);
    assert_eq!(err.used, (KEY.len() + VALUE.len()) as u64);
    assert_eq!(err.limit, 8);

    Ok(())
}

#[tokio::test]
async fn budget_exceeded_on_time() -> FdbResult<()> {
    let db = common::database().await?;
    let trx = db.create_trx()?;

    trx.set_client_budget(ClientBudget {
        time_limit: Some(Duration::from_millis(20)),
        ..ClientBudget::default()
    });
    assert!(trx.check_client_budget().is_ok());

    tokio::time::sleep(Duration::from_millis(50)).await;

    let err = trx.check_client_budget().unwrap_err();
    assert_eq!(err.kind, BudgetKind::Time);
    assert!(err.used >= 20, "used {} ms", err.used);
    assert_eq!(err.limit, 20);

    Ok(())
}

/// With a [`Clock`] the time limit no longer depends on how fast the machine
/// is: the test advances time itself and the numbers are exact.
#[tokio::test]
async fn budget_exceeded_on_time_with_a_custom_clock() -> FdbResult<()> {
    let db = common::database().await?;
    let trx = db.create_trx()?;

    let clock = FakeClock::default();
    clock.set_millis(1_000);

    trx.set_client_budget(
        ClientBudget {
            time_limit: Some(Duration::from_millis(20)),
            ..ClientBudget::default()
        }
        .with_clock(clock.clone()),
    );
    assert!(trx.check_client_budget().is_ok());

    // The limit itself is allowed, anything past it is not.
    clock.set_millis(1_020);
    assert!(trx.check_client_budget().is_ok());

    clock.set_millis(1_050);

    let err = trx.check_client_budget().unwrap_err();
    assert_eq!(err.kind, BudgetKind::Time);
    assert_eq!(err.used, 50);
    assert_eq!(err.limit, 20);
    assert_eq!(trx.attempt_usage().elapsed, Duration::from_millis(50));

    Ok(())
}

/// The clock is configuration too: the generation a new attempt starts is
/// measured with the clock of the budget, not with the wall clock.
#[tokio::test]
async fn the_clock_survives_a_new_attempt() -> FdbResult<()> {
    let db = common::database().await?;
    let mut trx = db.create_trx()?;

    let clock = FakeClock::default();
    clock.set_millis(1_000);

    trx.set_client_budget(
        ClientBudget {
            time_limit: Some(Duration::from_millis(20)),
            ..ClientBudget::default()
        }
        .with_clock(clock.clone()),
    );

    clock.set_millis(1_100);
    assert!(trx.check_client_budget().is_err());

    trx.reset();

    // The new attempt was stamped at 1_100 ms, so it starts from zero.
    assert_eq!(trx.attempt_usage().elapsed, Duration::ZERO);
    assert!(trx.check_client_budget().is_ok());

    clock.set_millis(1_130);

    let err = trx.check_client_budget().unwrap_err();
    assert_eq!(err.kind, BudgetKind::Time);
    assert_eq!(err.used, 30);
    assert_eq!(err.limit, 20);

    // Same contract after an `on_error` restart.
    let trx = trx
        .on_error(FdbError::from_code(TRANSACTION_TOO_OLD))
        .await?;
    assert_eq!(trx.attempt_usage().elapsed, Duration::ZERO);

    clock.set_millis(1_175);

    let err = trx.check_client_budget().unwrap_err();
    assert_eq!(err.kind, BudgetKind::Time);
    assert_eq!(err.used, 45);
    assert_eq!(err.limit, 20);

    Ok(())
}

/// The budget is configuration, the usage is per-attempt: `on_error` keeps the
/// former and resets the latter.
#[tokio::test]
async fn budget_survives_on_error_while_usage_resets() -> FdbResult<()> {
    const KEY: &[u8] = b"test-budget-on-error-key";
    const VALUE: &[u8] = b"a-value-larger-than-the-limit";

    let db = common::database().await?;
    let trx = db.create_trx()?;

    trx.set_client_budget(ClientBudget {
        max_bytes_written: Some(8),
        ..ClientBudget::default()
    });
    trx.set(KEY, VALUE);
    assert!(trx.check_client_budget().is_err());

    let trx = trx
        .on_error(FdbError::from_code(TRANSACTION_TOO_OLD))
        .await?;

    let usage = trx.attempt_usage();
    assert_eq!(usage.bytes_written, 0);
    assert_eq!(usage.call_set, 0);
    assert!(
        trx.check_client_budget().is_ok(),
        "usage should have been reset by the new attempt"
    );

    // The limit is still armed for the new attempt.
    trx.set(KEY, VALUE);
    assert_eq!(
        trx.check_client_budget().unwrap_err().kind,
        BudgetKind::BytesWritten
    );

    Ok(())
}

/// Same contract for an explicit `reset`.
#[tokio::test]
async fn reset_starts_a_new_attempt() -> FdbResult<()> {
    const KEY: &[u8] = b"test-budget-reset-key";
    const VALUE: &[u8] = b"a-value-larger-than-the-limit";

    let db = common::database().await?;
    let mut trx = db.create_trx()?;

    trx.set_client_budget(ClientBudget {
        max_bytes_written: Some(8),
        ..ClientBudget::default()
    });
    trx.set(KEY, VALUE);
    assert!(trx.check_client_budget().is_err());

    trx.reset();

    assert_eq!(trx.attempt_usage().bytes_written, 0);
    assert!(trx.check_client_budget().is_ok());

    trx.clear_client_budget();
    trx.set(KEY, VALUE);
    assert!(trx.check_client_budget().is_ok());

    Ok(())
}

/// An exceeded budget converts into an `FdbBindingError`, so `?` works inside a
/// `run` closure, and it is not retried.
#[tokio::test]
async fn budget_exceeded_is_fatal_in_run() -> FdbResult<()> {
    const KEY: &[u8] = b"test-budget-run-key";
    const VALUE: &[u8] = b"a-value-larger-than-the-limit";

    let db = common::database().await?;

    let result: Result<(), FdbBindingError> = db
        .run(|trx, _| async move {
            trx.set_client_budget(ClientBudget {
                max_bytes_written: Some(8),
                ..ClientBudget::default()
            });
            trx.set(KEY, VALUE);
            trx.check_client_budget()?;
            Ok(())
        })
        .await;

    match result {
        Err(FdbBindingError::ClientBudgetExceeded(err)) => {
            assert_eq!(err.kind, BudgetKind::BytesWritten);
            assert!(err.to_string().contains("client-side estimate"));
        }
        other => panic!("expected a ClientBudgetExceeded error, got {other:?}"),
    }

    Ok(())
}

/// The pattern of `examples/budgeted_scan.rs`: a scan cut into pages by the
/// budget, each page resuming exclusively after the last key of the previous
/// one. Every row must be seen exactly once across the pages.
#[tokio::test]
async fn budgeted_scan_resumes_without_losing_or_repeating_rows() -> FdbResult<()> {
    const PREFIX: &[u8] = b"test-budget-scan/";
    const END: &[u8] = b"test-budget-scan0";
    const ROWS: usize = 400;
    const VALUE_SIZE: usize = 1024;

    fn key_of(index: usize) -> Vec<u8> {
        let mut key = PREFIX.to_vec();
        key.extend_from_slice(format!("{index:05}").as_bytes());
        key
    }

    let db = common::database().await?;

    db.run(|trx, _| async move {
        trx.clear_range(PREFIX, END);
        for index in 0..ROWS {
            trx.set(&key_of(index), &vec![b'x'; VALUE_SIZE]);
        }
        Ok::<_, FdbBindingError>(())
    })
    .await
    .expect("setup");

    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut continuation: Option<Vec<u8>> = None;
    let mut transactions = 0;

    loop {
        let after = continuation.clone();
        // A budget smaller than one batch: each transaction reads a single
        // batch, then stops on the check that follows it.
        let (keys, complete) = db
            .run(move |trx, _| {
                let after = after.clone();
                async move {
                    trx.set_client_budget(ClientBudget {
                        max_bytes_read: Some(1),
                        ..ClientBudget::default()
                    });

                    let begin = match &after {
                        Some(last) => KeySelector::first_greater_than(last.as_slice()),
                        None => KeySelector::first_greater_or_equal(PREFIX),
                    };
                    let opt = RangeOption {
                        begin,
                        end: KeySelector::first_greater_or_equal(END),
                        mode: StreamingMode::Serial,
                        target_bytes: 1 << 20,
                        ..RangeOption::default()
                    };

                    let mut keys: Vec<Vec<u8>> = Vec::new();
                    let mut complete = true;
                    let mut batches = trx.get_ranges(opt, false);
                    while let Some(batch) =
                        futures_util::TryStreamExt::try_next(&mut batches).await?
                    {
                        for kv in batch.iter() {
                            keys.push(kv.key().to_vec());
                        }
                        // Matched, not propagated: the page is kept and the
                        // caller resumes from its last key.
                        if trx.check_client_budget().is_err() {
                            complete = false;
                            break;
                        }
                    }

                    Ok::<_, FdbBindingError>((keys, complete))
                }
            })
            .await
            .expect("scan page");

        transactions += 1;
        let last = keys.last().cloned();
        seen.extend(keys);

        if complete {
            break;
        }
        match last {
            Some(last) => continuation = Some(last),
            None => break,
        }
    }

    assert!(
        transactions > 1,
        "the budget should have cut the scan into several transactions, got {transactions}"
    );
    assert_eq!(seen.len(), ROWS, "a row was lost or read twice");
    let expected: Vec<Vec<u8>> = (0..ROWS).map(key_of).collect();
    assert_eq!(seen, expected, "rows are not the expected ones, in order");

    db.run(|trx, _| async move {
        trx.clear_range(PREFIX, END);
        Ok::<_, FdbBindingError>(())
    })
    .await
    .expect("cleanup");

    Ok(())
}
