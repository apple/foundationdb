// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use foundationdb::directory::DirectoryLayer;

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
