// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use foundationdb::directory::{DirectoryError, DirectoryLayer};

use foundationdb::directory::Directory;

use foundationdb::tuple::Subspace;
use foundationdb::*;

mod common;

#[tokio::test]
// testing basic features of the Directory, everything is tracked using with the BindingTester.
async fn test_directory() {
    let db = common::database().await.expect("cannot open fdb");

    // Scope the directory layer to a test-specific subspace: create() fails on
    // an already existing path, so reruns need a fresh state, and clearing only
    // our own subspace keeps the rest of the cluster untouched.
    let test_root = Subspace::from("test-directory");
    let directory = DirectoryLayer::new(
        test_root.subspace(&"node"),
        test_root.subspace(&"content"),
        false,
    );

    eprintln!("clearing the test subspace");
    let trx = db.create_trx().expect("cannot create txn");
    trx.clear_subspace_range(&test_root);
    trx.commit().await.expect("could not clear keys");

    eprintln!("creating directories");

    test_create_then_open_then_delete(&db, &directory, vec![String::from("application")])
        .await
        .expect("failed to run");

    test_create_then_open_then_delete(&db, &directory, vec![String::from("1"), String::from("2")])
        .await
        .expect("failed to run");
}

#[tokio::test]
async fn test_directory_rejects_occupied_bare_prefix() {
    test_directory_rejects_occupied_raw_prefix("test-directory-bare-prefix", &[]).await;
}

#[tokio::test]
async fn test_directory_rejects_occupied_ff_suffix() {
    test_directory_rejects_occupied_raw_prefix("test-directory-ff-suffix", &[0xff]).await;
}

#[tokio::test]
async fn test_directory_newer_minor_version_is_read_only() {
    let db = common::database().await.expect("cannot open fdb");
    let test_root = Subspace::from("test-directory-newer-minor");
    let nodes = test_root.subspace(&"node");
    let directory = DirectoryLayer::new(nodes.clone(), test_root.subspace(&"content"), false);
    let existing = vec![String::from("existing")];
    let missing = vec![String::from("missing")];

    let trx = db.create_trx().expect("cannot create txn");
    trx.clear_subspace_range(&test_root);
    let created = directory
        .create(&trx, &existing, None, None)
        .await
        .expect("cannot create directory");
    let version_key = nodes.subspace(&nodes.bytes()).pack(&b"version".as_slice());
    let version: Vec<_> = [1_u32, 1, 0]
        .into_iter()
        .flat_map(u32::to_le_bytes)
        .collect();
    trx.set(&version_key, &version);
    trx.commit().await.expect("cannot commit directory fixture");

    let trx = db.create_trx().expect("cannot create txn");
    let opened = directory
        .open(&trx, &existing, None)
        .await
        .expect("newer minor version must allow reads");
    assert_eq!(created.bytes().unwrap(), opened.bytes().unwrap());
    assert!(directory.exists(&trx, &existing).await.unwrap());
    assert!(!directory.exists(&trx, &missing).await.unwrap());
    assert_eq!(directory.list(&trx, &[]).await.unwrap(), existing);
    assert!(
        directory
            .create_or_open(&trx, &existing, None, None)
            .await
            .is_ok()
    );

    assert!(matches!(
        directory.create(&trx, &missing, None, None).await,
        Err(DirectoryError::Version(_))
    ));
    assert!(matches!(
        directory.move_to(&trx, &existing, &missing).await,
        Err(DirectoryError::Version(_))
    ));
    assert!(matches!(
        directory.remove(&trx, &existing).await,
        Err(DirectoryError::Version(_))
    ));
}

async fn test_directory_rejects_occupied_raw_prefix(name: &str, suffix: &[u8]) {
    let db = common::database().await.expect("cannot open fdb");
    let test_root = Subspace::from(name);
    let content = test_root.subspace(&"content");
    let directory = DirectoryLayer::new(test_root.subspace(&"node"), content.clone(), false);
    let path = vec![String::from("occupied")];
    let value = b"existing application data";

    // Occupy every candidate in the initial allocator window so this does not
    // depend on which random candidate the allocator chooses.
    let keys: Vec<_> = (0..64_i64)
        .map(|candidate| {
            let mut key = content.pack(&candidate);
            key.extend_from_slice(suffix);
            key
        })
        .collect();
    let trx = db.create_trx().expect("cannot create txn");
    trx.clear_subspace_range(&test_root);
    for key in &keys {
        trx.set(key, value);
    }
    trx.commit()
        .await
        .expect("cannot populate occupied prefixes");

    let trx = db.create_trx().expect("cannot create txn");
    assert!(matches!(
        directory.create(&trx, &path, None, None).await,
        Err(DirectoryError::PrefixNotEmpty)
    ));
    drop(trx);

    let trx = db.create_trx().expect("cannot create txn");
    assert!(
        !directory
            .remove_if_exists(&trx, &path)
            .await
            .expect("cannot check rejected directory")
    );
    trx.commit().await.expect("cannot commit removal check");

    let trx = db.create_trx().expect("cannot create txn");
    for key in &keys {
        let stored = trx
            .get(key, false)
            .await
            .expect("cannot read occupied prefix")
            .expect("existing key was removed");
        assert_eq!(&*stored, value);
    }
}

async fn test_create_then_open_then_delete(
    db: &Database,
    directory: &DirectoryLayer,
    path: Vec<String>,
) -> FdbResult<()> {
    let trx = db.create_trx()?;

    eprintln!("creating {:?}", path);
    let create_output = directory.create(&trx, &path, None, None).await;
    assert!(
        create_output.is_ok(),
        "cannot create: {:?}",
        create_output.err().unwrap()
    );
    trx.commit().await.expect("cannot commit");
    let trx = db.create_trx()?;

    eprintln!("opening {:?}", path);
    let open_output = directory.open(&trx, &path, None).await;
    assert!(
        open_output.is_ok(),
        "cannot create: {:?}",
        open_output.err().unwrap()
    );

    assert_eq!(
        create_output.unwrap().bytes().unwrap(),
        open_output.unwrap().bytes().unwrap()
    );
    trx.commit().await.expect("cannot commit");

    // removing folder
    Ok(())
}
