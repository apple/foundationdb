// Copyright 2024 foundationdb-rs developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

mod common;

#[cfg(feature = "recipes-ranked-register")]
mod ranked_register_tests {
    use std::sync::atomic::{AtomicU8, Ordering};

    use foundationdb::{
        Database, FdbBindingError, FdbError, RetryDecision, RetryableError,
        options::TransactionOption,
        recipes::ranked_register::{
            MAX_VALUE_CHUNK_BYTES, Rank, RankedRegister, RankedRegisterError, WriteResult,
        },
        tuple::Subspace,
    };

    // RankedRegisterError implements RetryableError, so closures return it
    // directly with plain `?` and the retry loop still recognizes wrapped
    // FdbErrors through the source chain.

    async fn setup_test(
        db: &Database,
        test_name: &str,
    ) -> Result<RankedRegister, RankedRegisterError> {
        let subspace = Subspace::all().subspace(&test_name);
        let (from, to) = subspace.range();

        let from_ref = &from;
        let to_ref = &to;
        db.run(|txn, _| async move {
            txn.clear_range(from_ref, to_ref);
            Ok::<_, RankedRegisterError>(())
        })
        .await?;

        Ok(RankedRegister::new(subspace))
    }

    #[tokio::test]
    async fn test_basic_write_and_read() -> Result<(), RankedRegisterError> {
        let db = crate::common::database().await?;
        let rr = setup_test(&db, "test_basic_write_and_read").await?;

        // Write with rank 1
        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move {
                let r = rr_ref.write(&txn, Rank::from(1u64), b"hello").await?;
                Ok::<_, RankedRegisterError>(r)
            })
            .await?;
        assert_eq!(result, WriteResult::Committed);

        // Read with rank 2 should return the value
        let rr_ref = &rr;
        let read_result = db
            .run(|txn, _| async move {
                let r = rr_ref.read(&txn, Rank::from(2u64)).await?;
                Ok::<_, RankedRegisterError>((r.write_rank(), r.into_value()))
            })
            .await?;
        assert_eq!(read_result.0, Rank::from(1u64));
        assert_eq!(read_result.1.as_deref(), Some(b"hello".as_slice()));

        Ok(())
    }

    #[tokio::test]
    async fn test_rank_fencing() -> Result<(), RankedRegisterError> {
        let db = crate::common::database().await?;
        let rr = setup_test(&db, "test_rank_fencing").await?;

        // Read with rank 10 installs a fence
        let rr_ref = &rr;
        db.run(|txn, _| async move {
            rr_ref.read(&txn, Rank::from(10u64)).await?;
            Ok::<_, RankedRegisterError>(())
        })
        .await?;

        // Write with rank 5 should be aborted (below the fence)
        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move {
                let r = rr_ref.write(&txn, Rank::from(5u64), b"blocked").await?;
                Ok::<_, RankedRegisterError>(r)
            })
            .await?;
        assert_eq!(result, WriteResult::Aborted);

        // Write with rank 10 should succeed (equal to fence)
        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move {
                let r = rr_ref.write(&txn, Rank::from(10u64), b"accepted").await?;
                Ok::<_, RankedRegisterError>(r)
            })
            .await?;
        assert_eq!(result, WriteResult::Committed);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_ordering() -> Result<(), RankedRegisterError> {
        let db = crate::common::database().await?;
        let rr = setup_test(&db, "test_write_ordering").await?;

        // Write rank 1 value "A"
        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move {
                let r = rr_ref.write(&txn, Rank::from(1u64), b"A").await?;
                Ok::<_, RankedRegisterError>(r)
            })
            .await?;
        assert_eq!(result, WriteResult::Committed);

        // Write rank 2 value "B"
        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move {
                let r = rr_ref.write(&txn, Rank::from(2u64), b"B").await?;
                Ok::<_, RankedRegisterError>(r)
            })
            .await?;
        assert_eq!(result, WriteResult::Committed);

        // Read should return "B"
        let rr_ref = &rr;
        let read_result = db
            .run(|txn, _| async move {
                let r = rr_ref.read(&txn, Rank::from(3u64)).await?;
                Ok::<_, RankedRegisterError>(r.into_value())
            })
            .await?;
        assert_eq!(read_result.as_deref(), Some(b"B".as_slice()));

        // Write rank 1 value "C" should be aborted (max_write_rank is 2)
        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move {
                let r = rr_ref.write(&txn, Rank::from(1u64), b"C").await?;
                Ok::<_, RankedRegisterError>(r)
            })
            .await?;
        assert_eq!(result, WriteResult::Aborted);

        Ok(())
    }

    #[tokio::test]
    async fn test_stale_write_rejected() -> Result<(), RankedRegisterError> {
        let db = crate::common::database().await?;
        let rr = setup_test(&db, "test_stale_write_rejected").await?;

        // Write with rank 5
        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move {
                let r = rr_ref.write(&txn, Rank::from(5u64), b"initial").await?;
                Ok::<_, RankedRegisterError>(r)
            })
            .await?;
        assert_eq!(result, WriteResult::Committed);

        // Read with rank 10 installs fence
        let rr_ref = &rr;
        db.run(|txn, _| async move {
            rr_ref.read(&txn, Rank::from(10u64)).await?;
            Ok::<_, RankedRegisterError>(())
        })
        .await?;

        // Write with rank 7 should be aborted even though 7 > 5,
        // because max_read_rank is 10
        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move {
                let r = rr_ref.write(&txn, Rank::from(7u64), b"stale").await?;
                Ok::<_, RankedRegisterError>(r)
            })
            .await?;
        assert_eq!(result, WriteResult::Aborted);

        Ok(())
    }

    #[tokio::test]
    async fn test_follower_value_read() -> Result<(), RankedRegisterError> {
        let db = crate::common::database().await?;
        let rr = setup_test(&db, "test_follower_value_read").await?;

        // Write with rank 5
        let rr_ref = &rr;
        db.run(|txn, _| async move {
            let _ = rr_ref.write(&txn, Rank::from(5u64), b"X").await?;
            Ok::<_, RankedRegisterError>(())
        })
        .await?;

        // value() returns "X"
        let rr_ref = &rr;
        let val = db
            .run(|txn, _| async move {
                let v = rr_ref.value(&txn).await?;
                Ok::<_, RankedRegisterError>(v)
            })
            .await?;
        assert_eq!(val.as_deref(), Some(b"X".as_slice()));

        // Write with rank 10 should succeed (value() didn't install a fence)
        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move {
                let r = rr_ref.write(&txn, Rank::from(10u64), b"Y").await?;
                Ok::<_, RankedRegisterError>(r)
            })
            .await?;
        assert_eq!(result, WriteResult::Committed);

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_register() -> Result<(), RankedRegisterError> {
        let db = crate::common::database().await?;
        let rr = setup_test(&db, "test_empty_register").await?;

        // Read on fresh register
        let rr_ref = &rr;
        let read_result = db
            .run(|txn, _| async move {
                let r = rr_ref.read(&txn, Rank::from(1u64)).await?;
                Ok::<_, RankedRegisterError>((r.write_rank(), r.into_value()))
            })
            .await?;
        assert_eq!(read_result.0, Rank::ZERO);
        assert_eq!(read_result.1, None);

        // value() on fresh register
        let rr_ref = &rr;
        let val = db
            .run(|txn, _| async move {
                let v = rr_ref.value(&txn).await?;
                Ok::<_, RankedRegisterError>(v)
            })
            .await?;
        assert_eq!(val, None);

        // Write with rank 1 should succeed
        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move {
                let r = rr_ref.write(&txn, Rank::from(1u64), b"first").await?;
                Ok::<_, RankedRegisterError>(r)
            })
            .await?;
        assert_eq!(result, WriteResult::Committed);

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_chunk_round_trip() -> Result<(), RankedRegisterError> {
        let db = crate::common::database().await?;
        let rr = setup_test(&db, "test_multi_chunk_round_trip").await?;
        let value = vec![0x5a; MAX_VALUE_CHUNK_BYTES + 1];
        let value_ref = &value;

        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move { rr_ref.write(&txn, Rank::from(1_u64), value_ref).await })
            .await?;
        assert_eq!(result, WriteResult::Committed);

        let rr_ref = &rr;
        let round_trip = db
            .run(|txn, _| async move { rr_ref.value(&txn).await })
            .await?;
        assert_eq!(round_trip.as_deref(), Some(value.as_slice()));

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_payload_is_present() -> Result<(), RankedRegisterError> {
        let db = crate::common::database().await?;
        let rr = setup_test(&db, "test_empty_payload_is_present").await?;

        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move { rr_ref.write(&txn, Rank::from(1_u64), b"").await })
            .await?;
        assert_eq!(result, WriteResult::Committed);

        let rr_ref = &rr;
        let value = db
            .run(|txn, _| async move { rr_ref.value(&txn).await })
            .await?;
        assert_eq!(value.as_deref(), Some(b"".as_slice()));

        Ok(())
    }

    #[tokio::test]
    async fn test_shrinking_value_clears_stale_chunks() -> Result<(), RankedRegisterError> {
        let db = crate::common::database().await?;
        let rr = setup_test(&db, "test_shrinking_value_clears_stale_chunks").await?;
        let initial = vec![0x31; MAX_VALUE_CHUNK_BYTES * 2 + 1];
        let initial_ref = &initial;

        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move { rr_ref.write(&txn, Rank::from(1_u64), initial_ref).await })
            .await?;
        assert_eq!(result, WriteResult::Committed);

        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move { rr_ref.write(&txn, Rank::from(2_u64), b"small").await })
            .await?;
        assert_eq!(result, WriteResult::Committed);

        let rr_ref = &rr;
        let value = db
            .run(|txn, _| async move { rr_ref.value(&txn).await })
            .await?;
        assert_eq!(value.as_deref(), Some(b"small".as_slice()));

        Ok(())
    }

    #[tokio::test]
    async fn test_configured_max_value_rejects_write() -> Result<(), RankedRegisterError> {
        let db = crate::common::database().await?;
        let subspace = Subspace::all().subspace(&("test_configured_max_value_rejects_write",));
        let rr = RankedRegister::with_max_value_bytes(subspace, 3);

        let rr_ref = &rr;
        let error = db
            .run(|txn, _| async move { rr_ref.write(&txn, Rank::from(1_u64), b"four").await })
            .await
            .expect_err("configured limit must reject oversized values");
        assert!(matches!(
            error,
            RankedRegisterError::ValueTooLarge {
                value_size: 4,
                limit: 3,
            }
        ));
        assert!(matches!(error.retry_decision(), RetryDecision::Fatal));

        Ok(())
    }

    #[tokio::test]
    async fn test_independent_handle_read_installs_fence() -> Result<(), RankedRegisterError> {
        let db = crate::common::database().await?;
        let reader = setup_test(&db, "test_independent_handle_read_installs_fence").await?;
        let writer = RankedRegister::new(reader.subspace().clone());

        let reader_ref = &reader;
        db.run(|txn, _| async move {
            reader_ref.read(&txn, Rank::from(10_u64)).await?;
            Ok::<_, RankedRegisterError>(())
        })
        .await?;

        let writer_ref = &writer;
        let result = db
            .run(|txn, _| async move { writer_ref.write(&txn, Rank::from(9_u64), b"fenced").await })
            .await?;
        assert_eq!(result, WriteResult::Aborted);

        Ok(())
    }

    #[tokio::test]
    async fn test_equal_rank_recommit_is_aborted() -> Result<(), RankedRegisterError> {
        let db = crate::common::database().await?;
        let rr = setup_test(&db, "test_equal_rank_recommit_is_aborted").await?;

        let rr_ref = &rr;
        let first = db
            .run(|txn, _| async move { rr_ref.write(&txn, Rank::from(1_u64), b"first").await })
            .await?;
        assert_eq!(first, WriteResult::Committed);

        let rr_ref = &rr;
        let second = db
            .run(|txn, _| async move { rr_ref.write(&txn, Rank::from(1_u64), b"second").await })
            .await?;
        assert_eq!(second, WriteResult::Aborted);

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_writes_conflict_on_register_state() -> Result<(), RankedRegisterError>
    {
        let db = crate::common::database().await?;
        let rr = setup_test(&db, "test_concurrent_writes_conflict_on_register_state").await?;
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));

        let first = rr.clone();
        let first_barrier = barrier.clone();
        let first_write = db.run(|txn, _| {
            let first = first.clone();
            let barrier = first_barrier.clone();
            async move {
                txn.set_option(TransactionOption::AutomaticIdempotency)?;
                txn.set_option(TransactionOption::RetryLimit(0))?;
                let result = first.write(&txn, Rank::from(1_u64), b"a").await?;
                barrier.wait().await;
                Ok::<_, RankedRegisterError>(result)
            }
        });
        let second = rr.clone();
        let second_barrier = barrier.clone();
        let second_write = db.run(|txn, _| {
            let second = second.clone();
            let barrier = second_barrier.clone();
            async move {
                txn.set_option(TransactionOption::AutomaticIdempotency)?;
                txn.set_option(TransactionOption::RetryLimit(0))?;
                let result = second.write(&txn, Rank::from(1_u64), b"b").await?;
                barrier.wait().await;
                Ok::<_, RankedRegisterError>(result)
            }
        });
        let (write_a, write_b) = tokio::join!(first_write, second_write);
        assert_eq!(
            usize::from(write_a.is_ok()) + usize::from(write_b.is_ok()),
            1
        );

        let rr_ref = &rr;
        let value = db
            .run(|txn, _| async move { rr_ref.value(&txn).await })
            .await?;
        assert!(matches!(value.as_deref(), Some(b"a") | Some(b"b")));

        Ok(())
    }

    /// Regression freezing the old anti-pattern this file used to demonstrate:
    /// stringifying an error into CustomError destroys the source chain, so
    /// even the chain-walking retry detection cannot rescue it. A retryable
    /// FdbError flattened to a String is fatal; that is correct, documented
    /// behavior, and the fix is rewriting the wrapping (as the helpers above
    /// now do), not the detection.
    #[tokio::test]
    async fn stringified_error_is_not_retried() {
        let db = crate::common::database().await.expect("failed to open db");
        let attempt = AtomicU8::new(0);
        let attempt_ref = &attempt;

        let result: Result<(), FdbBindingError> = db
            .run(|_txn, _| async move {
                attempt_ref.fetch_add(1, Ordering::SeqCst);
                let err = RankedRegisterError::Fdb(FdbError::from_code(1020));
                Err(FdbBindingError::CustomError(err.to_string().into()))
            })
            .await;

        assert!(result.is_err(), "stringified errors must stay fatal");
        assert_eq!(
            attempt.load(Ordering::SeqCst),
            1,
            "a stringified retryable error must not be retried"
        );
    }

    /// With the blessed pattern, a retryable FdbError injected through the
    /// recipe's own error type is retried and the operation completes.
    #[tokio::test]
    async fn injected_fdb_error_retries_through_recipe_error() {
        let db = crate::common::database().await.expect("failed to open db");
        let rr = setup_test(&db, "test_injected_retry")
            .await
            .expect("setup failed");
        let attempt = AtomicU8::new(0);
        let attempt_ref = &attempt;

        let rr_ref = &rr;
        let result = db
            .run(|txn, _| async move {
                if attempt_ref.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(RankedRegisterError::Fdb(FdbError::from_code(1020)));
                }
                let r = rr_ref.write(&txn, Rank::from(1u64), b"retried").await?;
                Ok(r)
            })
            .await
            .expect("injected retryable error must be retried");

        assert_eq!(result, WriteResult::Committed);
        assert!(attempt.load(Ordering::SeqCst) >= 2);
    }
}
