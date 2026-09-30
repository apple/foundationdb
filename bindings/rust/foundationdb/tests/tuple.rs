// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use foundationdb::{
    ClientBudget, Database, FdbBindingError, FdbError,
    options::MutationType,
    tuple::{Subspace, Versionstamp},
};
use futures::StreamExt;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

mod common;

#[tokio::test]
// testing subspace with versionstamps.
async fn test_tuples() {
    let db = common::database().await.expect("cannot open fdb");

    // Clear only this test's subspace: versionstamped keys accumulate across
    // runs, and the rest of the cluster is none of our business.
    eprintln!("clearing the test subspace");
    let trx = db.create_trx().expect("cannot create txn");
    trx.clear_subspace_range(&Subspace::from("test-tuple"));
    trx.commit().await.expect("could not clear keys");

    eprintln!("creating directories");
    test_subspace_with_versionstamp(&db).await;
}

async fn test_subspace_with_versionstamp(db: &Database) {
    let trx = db.create_trx().expect("cannot create txn");

    // In this example we will create a subspace starting with a versionstamp.
    let subspace = Subspace::from("test-tuple");
    let subspace = subspace.subspace(&Versionstamp::incomplete(0));
    let key = subspace.pack_with_versionstamp(&"key");

    eprintln!("writing key {key:?}");
    trx.atomic_op(&key, b"hello", MutationType::SetVersionstampedKey);

    // we want to get the versionstamp back to be able to read the subspace.
    let versionstamp = trx.get_versionstamp();

    trx.commit().await.expect("cannot commit");

    let versionstamp = versionstamp
        .await
        .expect("cannot get versionstamp after commit");
    let versionstamp = Versionstamp::complete(
        (*versionstamp)
            .try_into()
            .expect("versionstamp is the wrong size"),
        0,
    );
    // Now that we have the versionstamp we can rebuild the subspace and read the key.
    let trx = db.create_trx().expect("cannot create txn");

    // In this example we will create a subspace starting with a versionstamp.
    let subspace = Subspace::from("test-tuple");
    let subspace = subspace.subspace(&versionstamp);

    {
        // we can read the whole subspace to get the chance to unpack the key too.
        let mut elements = trx.get_ranges_keyvalues(subspace.range().into(), false);
        while let Some(key_value) = elements.next().await {
            let key_value = key_value.expect("cannot read keyvalue");
            let key = subspace
                .unpack::<String>(key_value.key())
                .expect("cannot unpack key");
            assert_eq!(key, "key");
            assert_eq!(key_value.value(), b"hello");
        }
    }
    trx.commit().await.expect("cannot commit");

    // Now that the subspace already exists, we can add more versionstamped keys in there.
    let trx = db.create_trx().expect("cannot create txn");
    let key = subspace.pack_with_versionstamp(&Versionstamp::incomplete(0));
    trx.atomic_op(&key, b"hello2", MutationType::SetVersionstampedKey);
    let key_versionstamp = trx.get_versionstamp();
    trx.commit().await.expect("cannot commit");
    let key_versionstamp = key_versionstamp
        .await
        .expect("cannot get versionstamp after commit");
    let key_versionstamp = Versionstamp::complete(
        (*key_versionstamp)
            .try_into()
            .expect("versionstamp is the wrong size"),
        0,
    );
    // we can read the key back by re-packing with the subspace:
    let trx = db.create_trx().expect("cannot create txn");
    let key = subspace.pack_with_versionstamp(&key_versionstamp);
    let value = trx.get(&key, false).await.expect("cannot read key");
    assert_eq!(value.as_deref(), Some(b"hello2".as_ref()));
    trx.commit().await.expect("cannot commit");
}

#[tokio::test]
async fn user_version_allocator_is_per_transaction_and_shared_by_runner_clones()
-> Result<(), FdbBindingError> {
    let db = common::database().await?;

    let first = db.create_trx()?;
    let second = db.create_trx()?;
    assert_eq!(first.allocate_user_version()?, 0);
    assert_eq!(second.allocate_user_version()?, 0);
    assert_eq!(first.allocate_user_version()?, 1);

    let versions = db
        .run(|trx, _| async move {
            let other_handle = trx.clone();
            let versions = [
                trx.allocate_user_version()?,
                other_handle.allocate_user_version()?,
                trx.allocate_user_version()?,
            ];
            drop(other_handle);
            Ok::<_, FdbBindingError>(versions)
        })
        .await?;
    assert_eq!(versions, [0, 1, 2]);

    Ok(())
}

#[tokio::test]
async fn user_version_allocator_is_concurrent_and_exhaustible() -> Result<(), FdbBindingError> {
    const THREADS: usize = 8;
    const VERSIONS_PER_THREAD: usize = 8_192;

    let db = common::database().await?;
    let trx = Arc::new(db.create_trx()?);
    let start = Arc::new(Barrier::new(THREADS));
    let mut threads = Vec::with_capacity(THREADS);
    for _ in 0..THREADS {
        let trx = trx.clone();
        let start = start.clone();
        threads.push(thread::spawn(move || {
            start.wait();
            (0..VERSIONS_PER_THREAD)
                .map(|_| {
                    trx.allocate_user_version()
                        .expect("allocator must not exhaust early")
                })
                .collect::<Vec<_>>()
        }));
    }

    let mut versions = threads
        .into_iter()
        .flat_map(|thread| thread.join().expect("allocator thread panicked"))
        .collect::<Vec<_>>();
    versions.sort_unstable();
    assert_eq!(versions, (0..=u16::MAX).collect::<Vec<_>>());
    assert!(matches!(
        trx.allocate_user_version(),
        Err(FdbBindingError::UserVersionExhausted)
    ));
    assert!(matches!(
        trx.allocate_user_version(),
        Err(FdbBindingError::UserVersionExhausted)
    ));

    let mut trx = Arc::try_unwrap(trx).expect("allocator references must be released");
    trx.reset();
    assert_eq!(trx.allocate_user_version()?, 0);

    Ok(())
}

#[tokio::test]
async fn user_version_allocator_restarts_with_transaction_attempts() -> Result<(), FdbBindingError>
{
    let db = common::database().await?;

    let mut trx = db.create_trx()?;
    assert_eq!(trx.allocate_user_version()?, 0);
    trx.reset();
    assert_eq!(trx.allocate_user_version()?, 0);

    let trx = trx.on_error(FdbError::from_code(1020)).await?;
    assert_eq!(trx.allocate_user_version()?, 0);

    let trx = trx.cancel().reset();
    assert_eq!(trx.allocate_user_version()?, 0);

    let trx = trx.commit().await.map_err(FdbError::from)?.reset();
    assert_eq!(trx.allocate_user_version()?, 0);

    Ok(())
}

#[tokio::test]
async fn user_version_allocator_restarts_after_commit_error_paths() -> Result<(), FdbBindingError> {
    let db = common::database().await?;

    let trx = commit_error_after_allocation(&db, b"test-user-version-reset-error").await;
    let trx = trx.reset();
    assert_eq!(trx.allocate_user_version()?, 0);

    let trx = commit_error_after_allocation(&db, b"test-user-version-on-error").await;
    let trx = trx.on_error().await?;
    assert_eq!(trx.allocate_user_version()?, 0);

    Ok(())
}

async fn commit_error_after_allocation(
    db: &Database,
    key: &[u8],
) -> foundationdb::TransactionCommitError {
    let trx = db.create_trx().expect("cannot create transaction");
    assert_eq!(
        trx.allocate_user_version()
            .expect("cannot allocate user version"),
        0
    );
    trx.get(key, false).await.expect("cannot read conflict key");

    let conflicting = db
        .create_trx()
        .expect("cannot create conflicting transaction");
    conflicting.set(key, b"conflicting value");
    conflicting
        .commit()
        .await
        .expect("cannot commit conflicting transaction");

    trx.set(key, b"original value");
    trx.commit().await.expect_err("transaction must conflict")
}

#[tokio::test]
async fn user_version_allocator_survives_budget_changes_and_does_not_affect_accounting()
-> Result<(), FdbBindingError> {
    let db = common::database().await?;

    let trx = db.create_trx()?;
    assert_eq!(trx.allocate_user_version()?, 0);
    trx.set_client_budget(ClientBudget::default());
    assert_eq!(trx.allocate_user_version()?, 1);

    let (version, metrics) = db
        .instrumented_run(|trx, _| async move {
            assert_eq!(trx.allocate_user_version()?, 0);
            trx.allocate_user_version()
        })
        .await
        .map_err(|(error, _metrics)| error)?;
    assert_eq!(version, 1);
    let usage = metrics.total_usage();
    assert_eq!(usage.bytes_read, 0);
    assert_eq!(usage.bytes_written, 0);
    assert_eq!(usage.call_set, 0);
    assert_eq!(usage.call_atomic_op, 0);

    Ok(())
}

#[tokio::test]
async fn user_version_allocator_runner_retry_restarts_and_keeps_clones_shared()
-> Result<(), FdbBindingError> {
    let db = common::database().await?;
    let attempts = Arc::new(Mutex::new(Vec::new()));

    db.run(|trx, _| {
        let attempts = attempts.clone();
        async move {
            let clone = trx.clone();
            let versions = [trx.allocate_user_version()?, clone.allocate_user_version()?];
            drop(clone);

            let mut attempts = attempts.lock().expect("attempt list mutex poisoned");
            let retry = attempts.is_empty();
            attempts.push(versions);
            if retry {
                Err(FdbBindingError::from(FdbError::from_code(1020)))
            } else {
                Ok(())
            }
        }
    })
    .await?;

    assert_eq!(
        *attempts.lock().expect("attempt list mutex poisoned"),
        [[0, 1], [0, 1]]
    );
    Ok(())
}

#[tokio::test]
async fn explicit_versionstamps_remain_independent_from_allocation() -> Result<(), FdbBindingError>
{
    let db = common::database().await?;
    let trx = db.create_trx()?;

    let explicit = Versionstamp::incomplete(0);
    assert_eq!(explicit.user_version(), 0);
    assert_eq!(trx.allocate_user_version()?, 0);
    assert_eq!(Versionstamp::incomplete(42).user_version(), 42);

    Ok(())
}

#[tokio::test]
async fn allocated_versionstamps_create_distinct_keys_in_one_transaction()
-> Result<(), FdbBindingError> {
    let db = common::database().await?;
    let subspace = Subspace::from("test-user-version-allocator");

    let clear = db.create_trx()?;
    clear.clear_subspace_range(&subspace);
    clear.commit().await.map_err(FdbError::from)?;

    let trx = db.create_trx()?;
    let a = &trx;
    let b = &trx;
    for (handle, value) in [
        (a, b"a".as_slice()),
        (b, b"b".as_slice()),
        (a, b"c".as_slice()),
    ] {
        let user_version = handle.allocate_user_version()?;
        let key = subspace.pack_with_versionstamp(&Versionstamp::incomplete(user_version));
        handle.atomic_op(&key, value, MutationType::SetVersionstampedKey);
    }
    let transaction_version = trx.get_versionstamp();
    trx.commit().await.map_err(FdbError::from)?;
    let transaction_version: [u8; 10] = (*transaction_version.await?)
        .try_into()
        .expect("versionstamp is the wrong size");

    let reader = db.create_trx()?;
    for (user_version, value) in [
        (0, b"a".as_slice()),
        (1, b"b".as_slice()),
        (2, b"c".as_slice()),
    ] {
        let versionstamp = Versionstamp::complete(transaction_version, user_version);
        assert_eq!(
            versionstamp.transaction_version(),
            transaction_version.as_slice()
        );
        let key = subspace.pack(&versionstamp);
        assert_eq!(reader.get(&key, false).await?.as_deref(), Some(value));
    }
    let mut entries = reader.get_ranges_keyvalues(subspace.range().into(), false);
    let mut count = 0;
    while let Some(entry) = entries.next().await {
        entry?;
        count += 1;
    }
    assert_eq!(count, 3);

    Ok(())
}
