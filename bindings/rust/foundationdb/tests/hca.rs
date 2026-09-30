// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use foundationdb::tuple::{Subspace, hca::HighContentionAllocator};
use foundationdb::{FdbResult, TransactOption};
use futures::prelude::*;
use std::collections::HashSet;
use std::iter::FromIterator;

mod common;

#[tokio::test]
async fn test_hca_many_sequential_allocations() -> FdbResult<()> {
    const N: usize = 1000;
    // Not a byte-prefix of the concurrent test's subspace: both tests run in
    // parallel and clear their own subspace range at startup.
    const KEY: &[u8] = b"test-hca-seq";

    let db = common::database().await?;

    {
        let tx = db.create_trx()?;
        tx.clear_subspace_range(&Subspace::from_bytes(KEY));
        tx.commit().await?;
    }

    let hca = HighContentionAllocator::new(Subspace::from_bytes(KEY));

    let mut all_ints = Vec::new();

    for _ in 0..N {
        let tx = db.create_trx()?;

        let next_int: i64 = hca
            .allocate(&tx)
            .await
            .expect("could not allocate with HCA");
        all_ints.push(next_int);

        tx.commit().await?;
    }

    check_hca_result_uniqueness(&all_ints);

    eprintln!("ran test {all_ints:?}");

    Ok(())
}

#[tokio::test]
async fn test_hca_concurrent_allocations() -> FdbResult<()> {
    const N: usize = 1000;
    const KEY: &[u8] = b"test-hca-conc";

    let db = common::database().await?;

    {
        let tx = db.create_trx()?;
        tx.clear_subspace_range(&Subspace::from_bytes(KEY));
        tx.commit().await?;
    }

    let hca = HighContentionAllocator::new(Subspace::from_bytes(KEY));

    let all_ints: Vec<i64> = future::try_join_all((0..N).map(|_| {
        db.transact_boxed(
            &hca,
            move |tx, hca| hca.allocate(tx).boxed(),
            TransactOption::default(),
        )
    }))
    .await
    .unwrap();
    check_hca_result_uniqueness(&all_ints);

    eprintln!("ran test {all_ints:?}");

    Ok(())
}

fn check_hca_result_uniqueness(results: &[i64]) {
    let result_set: HashSet<i64> = HashSet::from_iter(results.to_owned());

    if results.len() != result_set.len() {
        panic!(
            "Set size does not much, got duplicates from HCA. Set: {:?}, List: {:?}",
            result_set.len(),
            results.len(),
        );
    }
}
