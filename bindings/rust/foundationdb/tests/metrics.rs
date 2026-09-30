use foundationdb::metrics::{AttemptOutcome, MetricKey, TransactionMetrics};
use foundationdb::runner::MetricsHooks;
use foundationdb::*;
mod common;
use std::borrow::Cow;
use std::sync::{Arc, Mutex};

/// Tests a successful transaction using `instrumented_run`.
///
/// A run without retry reports exactly one attempt, holding the operations of
/// the transaction and ending as committed.
#[tokio::test]
async fn instrumented_run_success() -> FdbResult<()> {
    const KEY: &[u8] = b"test_metrics_success";
    const VALUE: &[u8] = b"value";
    const SUCCESS: u64 = 42;

    let db = common::database().await?;

    let (result, metrics) = db
        .instrumented_run(|txn, _| async move {
            txn.set(KEY, VALUE);
            Ok::<_, FdbBindingError>(SUCCESS)
        })
        .await
        .expect("transaction should succeed");

    assert_eq!(result, SUCCESS);

    assert_eq!(metrics.attempts.len(), 1);
    let attempt = &metrics.attempts[0];
    assert_eq!(attempt.index, 0);
    assert_eq!(attempt.usage.call_set, 1);
    assert_eq!(
        attempt.usage.bytes_written,
        (KEY.len() + VALUE.len()) as u64
    );
    assert!(matches!(attempt.outcome, AttemptOutcome::Committed));
    assert!(attempt.commit_duration.is_some());
    assert!(attempt.grv_duration.is_none());
    assert!(attempt.on_error_duration.is_none());

    let total = metrics.total_usage();
    assert_eq!(total.call_set, 1);
    assert_eq!(total.bytes_written, (KEY.len() + VALUE.len()) as u64);

    assert_eq!(metrics.transaction.retries, 0);
    assert!(metrics.total_duration.is_some());

    Ok(())
}

/// Tests the retry mechanism of `instrumented_run`.
///
/// A forced retryable error gives one attempt per try: every attempt keeps its
/// own counters and custom metrics instead of being wiped by the next one, and
/// only the last one is committed.
#[tokio::test]
async fn instrumented_run_with_n_retries() -> FdbResult<()> {
    const KEY: &[u8] = b"test_metrics_retry";
    const VALUE: &[u8] = b"value";
    const SUCCESS: u64 = 42;

    // Number of retries we want to force
    const EXPECTED_RETRIES: u64 = 3;

    let db = common::database().await?;

    // Use Arc<Mutex<>> to share and modify the counter across async calls
    let attempt_counter = Arc::new(Mutex::new(0u64));

    let (result, metrics) = db
        .instrumented_run(|txn, _| {
            let counter = attempt_counter.clone();
            async move {
                // Set a key to verify metrics
                txn.set(KEY, VALUE);

                // Increment the counter and check if we should still fail
                let mut attempts = counter.lock().unwrap();
                *attempts += 1;

                // Each attempt tags itself, so a wiped attempt would show up.
                txn.set_custom_metric("attempt_number", *attempts, &[("kind", "forced")]);

                if *attempts <= EXPECTED_RETRIES {
                    // Return a retryable error (not_committed) for the first N attempts
                    Err(FdbBindingError::from(FdbError::from_code(1020)))
                } else {
                    // Succeed on attempt N+1
                    Ok(SUCCESS)
                }
            }
        })
        .await
        .expect("transaction should succeed after retries");

    assert_eq!(result, SUCCESS);

    // One attempt per try, nothing lost across the retries.
    assert_eq!(metrics.attempts.len(), EXPECTED_RETRIES as usize + 1);
    assert_eq!(metrics.transaction.retries, EXPECTED_RETRIES);

    let custom_key = MetricKey::new("attempt_number", &[("kind", "forced")]);
    for (index, attempt) in metrics.attempts.iter().enumerate() {
        assert_eq!(attempt.index, index);

        // The per-attempt usage is retained, not wiped by the following attempt.
        assert_eq!(attempt.usage.call_set, 1, "attempt {index}");
        assert_eq!(
            attempt.usage.bytes_written,
            (KEY.len() + VALUE.len()) as u64,
            "attempt {index}"
        );
        assert_eq!(
            attempt.custom_metrics.get(&custom_key).copied(),
            Some(index as u64 + 1),
            "attempt {index}"
        );

        let is_last = index == EXPECTED_RETRIES as usize;
        match (&attempt.outcome, is_last) {
            (AttemptOutcome::Committed, true) => {}
            (AttemptOutcome::Retried { cause }, false) => {
                assert_eq!(cause.code(), 1020, "attempt {index}");
                assert!(
                    attempt.on_error_duration.is_some(),
                    "attempt {index} went through on_error"
                );
            }
            (outcome, _) => panic!("unexpected outcome for attempt {index}: {outcome:?}"),
        }
    }

    // Totals are the sum of the attempts.
    let total = metrics.total_usage();
    assert_eq!(total.call_set, EXPECTED_RETRIES + 1);
    assert_eq!(
        total.bytes_written,
        (EXPECTED_RETRIES + 1) * (KEY.len() + VALUE.len()) as u64
    );

    // Verify the counter
    let final_attempts = *attempt_counter.lock().unwrap();
    assert_eq!(final_attempts, EXPECTED_RETRIES + 1);

    Ok(())
}

/// Performs a comprehensive test of all counter metrics.
///
/// This test executes a single transaction that performs a variety of operations:
/// - Multiple `SET` operations
/// - A `GET` operation
/// - A `GET_RANGE` operation
/// - `CLEAR` and `CLEAR_RANGE` operations
/// - An `ATOMIC_OP`
///
/// It then performs precise assertions on all relevant counter metrics, including
/// operation counts, exact bytes written, and exact bytes read, ensuring they are
/// all tracked correctly within a single attempt.
#[tokio::test]
async fn test_counter_metrics() -> FdbResult<()> {
    let db = common::database().await?;

    const PREFIX: &[u8] = b"test_counter_metrics_";
    const SET_OPS: usize = 3;
    let mut bytes_written: u64 = 0;
    for i in 0..SET_OPS {
        let key = format!("{}_key{}", std::str::from_utf8(PREFIX).unwrap(), i);
        let value = format!("value{}", i);
        bytes_written += (key.len() + value.len()) as u64;
    }
    // clear, clear_range and atomic_op of the closure below.
    let clear_key = format!("{}_key2", std::str::from_utf8(PREFIX).unwrap());
    let clear_range_begin = format!("{}_key1", std::str::from_utf8(PREFIX).unwrap());
    let clear_range_end = format!("{}_key3", std::str::from_utf8(PREFIX).unwrap());
    let atomic_key = format!("{}_atomic", std::str::from_utf8(PREFIX).unwrap());
    bytes_written +=
        (clear_key.len() + clear_range_begin.len() + clear_range_end.len() + atomic_key.len() + 8)
            as u64;

    let ((fetched_count, bytes_read), metrics) = match db
        .instrumented_run(|txn, _| {
            async move {
                // 1. SET operations
                for i in 0..SET_OPS {
                    let key = format!("{}_key{}", std::str::from_utf8(PREFIX).unwrap(), i);
                    let value = format!("value{}", i);
                    txn.set(key.as_bytes(), value.as_bytes());
                }

                // 2. GET operation
                let get_key = format!("{}_key1", std::str::from_utf8(PREFIX).unwrap()).into_bytes();
                let mut bytes_read_acc = 0;
                if let Some(value_slice) = txn.get(&get_key, false).await? {
                    bytes_read_acc += (get_key.len() + value_slice.len()) as u64;
                }
                let get_count = 1;

                // 3. GET_RANGE operation
                let range_begin =
                    format!("{}_key", std::str::from_utf8(PREFIX).unwrap()).into_bytes();
                let range_end =
                    format!("{}_key4", std::str::from_utf8(PREFIX).unwrap()).into_bytes();
                let range_option = RangeOption {
                    begin: KeySelector::first_greater_or_equal(Cow::from(range_begin)),
                    end: KeySelector::first_greater_or_equal(Cow::from(range_end)),
                    limit: Some(100),
                    ..Default::default()
                };
                let range_result = txn.get_range(&range_option, 1, false).await?;
                for kv in range_result.iter() {
                    bytes_read_acc += (kv.key().len() + kv.value().len()) as u64;
                }

                let range_count = range_result.len();

                // 4. CLEAR operation
                let clear_key =
                    format!("{}_key2", std::str::from_utf8(PREFIX).unwrap()).into_bytes();
                txn.clear(&clear_key);

                // 5. CLEAR_RANGE operation
                let clear_range_begin =
                    format!("{}_key1", std::str::from_utf8(PREFIX).unwrap()).into_bytes();
                let clear_range_end =
                    format!("{}_key3", std::str::from_utf8(PREFIX).unwrap()).into_bytes();
                txn.clear_range(&clear_range_begin[..], &clear_range_end[..]);

                // 6. ATOMIC operation (add)
                let atomic_key =
                    format!("{}_atomic", std::str::from_utf8(PREFIX).unwrap()).into_bytes();
                txn.atomic_op(
                    &atomic_key,
                    &[1, 0, 0, 0, 0, 0, 0, 0],
                    options::MutationType::Add,
                );

                Ok((get_count + range_count, bytes_read_acc))
            }
        })
        .await
    {
        Ok(val) => val,
        Err((err, _)) => match err {
            FdbBindingError::NonRetryableFdbError(fdb_err) => return Err(fdb_err),
            _ => panic!("Test failed with unexpected error type: {:?}", err),
        },
    };

    // A single attempt: its usage is the whole story.
    assert_eq!(metrics.attempts.len(), 1);
    let usage = metrics.attempts[0].usage;

    assert_eq!(
        usage.call_set, SET_OPS as u64,
        "Should have {} SET operations",
        SET_OPS
    );
    assert_eq!(usage.call_get, 1, "Should have 1 GET operation");
    assert_eq!(usage.call_get_range, 1, "Should have 1 GET_RANGE operation");

    // Verify the number of key-values fetched matches our result count
    assert_eq!(
        usage.keys_values_fetched, fetched_count as u64,
        "Should have fetched {} key-values",
        fetched_count
    );

    assert_eq!(
        usage.bytes_written, bytes_written,
        "Should have written {} bytes",
        bytes_written
    );
    assert_eq!(
        usage.bytes_read, bytes_read,
        "Should have read {} bytes",
        bytes_read
    );

    assert_eq!(usage.call_clear, 1, "Should have 1 CLEAR operation");
    assert_eq!(
        usage.call_clear_range, 1,
        "Should have 1 CLEAR_RANGE operation"
    );
    assert_eq!(usage.call_atomic_op, 1, "Should have 1 ATOMIC operation");

    // Without a retry, the totals are that single attempt.
    let total = metrics.total_usage();
    assert_eq!(total.call_set, usage.call_set);
    assert_eq!(total.call_get, usage.call_get);
    assert_eq!(total.keys_values_fetched, usage.keys_values_fetched);
    assert_eq!(total.bytes_read, usage.bytes_read);
    assert_eq!(total.bytes_written, usage.bytes_written);
    assert_eq!(total.call_clear, usage.call_clear);
    assert_eq!(total.call_clear_range, usage.call_clear_range);
    assert_eq!(total.call_atomic_op, usage.call_atomic_op);

    // Verify transaction info
    assert_eq!(metrics.transaction.retries, 0, "Should have no retries");
    assert!(
        metrics.transaction.commit_version.is_some(),
        "Should have a commit version"
    );

    Ok(())
}

/// Tests the `TransactionInfo` fields within the metrics report.
#[tokio::test]
async fn test_transaction_info() -> FdbResult<()> {
    let db = common::database().await?;

    // read_version: recorded when the user asks for it, on the attempt and on
    // the transaction information.
    {
        let metrics = TransactionMetrics::new();
        let hooks = MetricsHooks::new(&metrics);

        let read_version = db
            .run_with_hooks(&hooks, |txn, _| {
                let metrics = metrics.clone();
                async move {
                    let read_version = txn.get_read_version().await?;
                    // Visible on the transaction information as soon as it is
                    // read, without waiting for the run to end.
                    assert_eq!(
                        metrics.get_transaction_info().read_version,
                        Some(read_version)
                    );
                    let again = txn.get_read_version().await?;
                    assert_eq!(again, read_version);
                    Ok::<_, FdbBindingError>(read_version)
                }
            })
            .await
            .expect("transaction should succeed");

        let report = metrics.get_metrics_data();
        assert_eq!(report.attempts.len(), 1);
        assert_eq!(report.attempts[0].read_version, Some(read_version));
        assert!(report.attempts[0].grv_duration.is_some());
    }

    // commit_version
    {
        let (_result, metrics) = db
            .instrumented_run(|txn, _| async move {
                txn.set(b"test_commit_version", b"value");
                Ok::<_, FdbBindingError>(())
            })
            .await
            .expect("Transaction failed");

        assert!(metrics.transaction.commit_version.is_some());
        // Not asked for, not fetched.
        assert!(metrics.transaction.read_version.is_none());
        assert!(metrics.attempts[0].grv_duration.is_none());
    }

    // retries
    {
        const EXPECTED_RETRIES: u64 = 2;

        let attempt_counter = Arc::new(Mutex::new(0u64));

        let (_, metrics) = db
            .instrumented_run(|_txn, _| {
                let counter = attempt_counter.clone();
                async move {
                    let mut attempts = counter.lock().unwrap();
                    *attempts += 1;

                    if *attempts <= EXPECTED_RETRIES {
                        Err(FdbBindingError::from(FdbError::from_code(1020)))
                    } else {
                        Ok(())
                    }
                }
            })
            .await
            .expect("Transaction should have succeeded after retries");

        assert_eq!(metrics.transaction.retries, EXPECTED_RETRIES);
        assert_eq!(metrics.attempts.len(), EXPECTED_RETRIES as usize + 1);
    }

    Ok(())
}

/// Setting a client budget mid-attempt starts a fresh accounting generation,
/// and the attempt being recorded goes with it: only what happens after the call
/// is reported.
#[tokio::test]
async fn set_client_budget_mid_attempt_drops_what_was_recorded() -> FdbResult<()> {
    const KEY: &[u8] = b"test_metrics_budget_mid_attempt";
    const VALUE: &[u8] = b"value";

    let db = common::database().await?;

    let (_, metrics) = db
        .instrumented_run(|txn, _| async move {
            txn.set(b"test_metrics_budget_before", VALUE);
            txn.set_custom_metric("before_budget", 1, &[]);

            txn.set_client_budget(ClientBudget::default());

            txn.set(KEY, VALUE);
            txn.set_custom_metric("after_budget", 1, &[]);
            Ok::<_, FdbBindingError>(())
        })
        .await
        .expect("transaction should succeed");

    assert_eq!(metrics.attempts.len(), 1);
    let attempt = &metrics.attempts[0];

    // The write itself did happen, only its accounting was dropped.
    assert_eq!(attempt.usage.call_set, 1);
    assert_eq!(
        attempt.usage.bytes_written,
        (KEY.len() + VALUE.len()) as u64
    );

    assert!(
        attempt
            .custom_metrics
            .contains_key(&MetricKey::new("after_budget", &[]))
    );
    assert!(
        !attempt
            .custom_metrics
            .contains_key(&MetricKey::new("before_budget", &[]))
    );

    Ok(())
}

/// A run that never commits reports its last attempt as failed.
#[tokio::test]
async fn instrumented_run_failure_reports_the_last_attempt() -> FdbResult<()> {
    let db = common::database().await?;

    #[derive(Debug)]
    struct Fatal;
    impl std::fmt::Display for Fatal {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "fatal")
        }
    }
    impl std::error::Error for Fatal {}

    let (_err, metrics) = db
        .instrumented_run(|txn, _| async move {
            txn.set(b"test_metrics_failure", b"value");
            // A custom error with no FdbError underneath is not retryable.
            Err::<(), _>(FdbBindingError::new_custom_error(Box::new(Fatal)))
        })
        .await
        .expect_err("transaction should fail");

    assert_eq!(metrics.attempts.len(), 1);
    let attempt = &metrics.attempts[0];
    assert!(matches!(attempt.outcome, AttemptOutcome::Failed));
    assert_eq!(attempt.usage.call_set, 1);
    assert!(attempt.commit_duration.is_none(), "commit never ran");
    assert_eq!(metrics.transaction.retries, 0);

    Ok(())
}

/// Exhausting the retry limit consumes the transaction inside `on_error`: the
/// attempt it was running must still be reported.
#[tokio::test]
async fn instrumented_run_reports_the_attempt_that_exhausted_the_retries() -> FdbResult<()> {
    let db = common::database().await?;

    let (_err, metrics) = db
        .instrumented_run(|txn, _| async move {
            txn.set_option(options::TransactionOption::RetryLimit(0))?;
            txn.set(b"test_metrics_retry_limit", b"value");
            Err::<(), _>(FdbBindingError::from(FdbError::from_code(1020)))
        })
        .await
        .expect_err("the retry limit should be reached");

    assert_eq!(metrics.attempts.len(), 1);
    let attempt = &metrics.attempts[0];
    assert!(matches!(attempt.outcome, AttemptOutcome::Failed));
    assert_eq!(attempt.usage.call_set, 1);
    assert_eq!(metrics.transaction.retries, 0);

    Ok(())
}

/// Tests that the timings of the attempts are recorded.
#[tokio::test]
async fn test_time_metrics() -> FdbResult<()> {
    let db = common::database().await?;

    // A committed attempt has a duration and a commit duration, and never went
    // through on_error.
    {
        let (_result, metrics) = db
            .instrumented_run(|txn, _| async move {
                for i in 0..10 {
                    let key = format!("test_time_metrics_{}", i).into_bytes();
                    txn.set(&key, b"value");
                }
                let _ = txn.get(b"test_time_metrics_0", false).await?;
                Ok::<_, FdbBindingError>(())
            })
            .await
            .expect("Transaction failed");

        assert_eq!(metrics.attempts.len(), 1);
        let attempt = &metrics.attempts[0];
        assert!(attempt.duration.is_some(), "attempt duration");
        assert!(attempt.commit_duration.is_some(), "commit duration");
        assert!(
            attempt.grv_duration.is_none(),
            "get_read_version was not called"
        );
        assert!(attempt.on_error_duration.is_none(), "on_error did not run");
        assert!(metrics.total_duration >= attempt.duration);
    }

    // A retried attempt has its on_error duration recorded, and its own
    // duration excludes the retry backoff.
    {
        let attempt_counter = Arc::new(Mutex::new(0u64));
        let (_, metrics) = db
            .instrumented_run(|_txn, _| {
                let counter = attempt_counter.clone();
                async move {
                    let mut attempts = counter.lock().unwrap();
                    *attempts += 1;

                    if *attempts == 1 {
                        Err(FdbBindingError::from(FdbError::from_code(1020)))
                    } else {
                        Ok(())
                    }
                }
            })
            .await
            .expect("Transaction should have succeeded after retry");

        assert_eq!(metrics.attempts.len(), 2);
        assert!(
            metrics.attempts[0].on_error_duration.is_some(),
            "Error handling time should be recorded"
        );
        assert!(metrics.attempts[1].on_error_duration.is_none());
    }

    Ok(())
}

/// Custom metrics recorded on the transaction are reported per attempt, and are
/// infallible: an uninstrumented transaction simply drops them.
#[tokio::test]
async fn test_transaction_custom_metrics() -> Result<(), FdbBindingError> {
    let db = common::database().await?;

    // No metrics consumer: recording is a no-op, not an error.
    let txn = db.create_trx()?;
    txn.set_custom_metric("dropped", 1, &[]);
    txn.increment_custom_metric("dropped", 1, &[]);
    drop(txn);

    let (_, metrics) = db
        .instrumented_run(|txn, _| async move {
            txn.set_custom_metric("txn_counter", 100, &[("operation", "read")]);
            txn.set_custom_metric("txn_timer", 200, &[("component", "storage")]);

            txn.increment_custom_metric("txn_incremented", 10, &[("type", "query")]);
            txn.increment_custom_metric("txn_incremented", 15, &[("type", "query")]);

            // Read a value to make sure the transaction does something
            let _value = txn.get(b"test_key", false).await?;

            Ok::<_, FdbBindingError>(())
        })
        .await
        .map_err(|(err, _)| err)?;

    assert_eq!(metrics.attempts.len(), 1);
    let custom = &metrics.attempts[0].custom_metrics;

    let key = MetricKey::new("txn_counter", &[("operation", "read")]);
    assert_eq!(custom.get(&key).copied(), Some(100));

    let key = MetricKey::new("txn_timer", &[("component", "storage")]);
    assert_eq!(custom.get(&key).copied(), Some(200));

    // Increments accumulate within the attempt.
    let key = MetricKey::new("txn_incremented", &[("type", "query")]);
    assert_eq!(custom.get(&key).copied(), Some(25));

    Ok(())
}
