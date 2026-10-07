// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
use byteorder::ByteOrder;
use foundationdb::*;
use futures::future::*;

mod common;

async fn atomic_add(db: &Database, key: &[u8], value: i64) -> FdbResult<()> {
    let trx = db.create_trx()?;

    let val = {
        let mut buf = [0u8; 8];
        byteorder::LE::write_i64(&mut buf, value);
        buf
    };
    trx.atomic_op(key, &val, options::MutationType::Add);

    trx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_atomic() -> FdbResult<()> {
    const KEY: &[u8] = b"test-atomic";

    let db = common::database().await?;

    println!("clear!");
    {
        let trx = db.create_trx()?;
        trx.clear(KEY);
        trx.commit().await?;
    }

    println!("concurrent!");
    {
        let n = 1000usize;

        let fut_add = try_join_all((0..n).map(|_| atomic_add(&db, KEY, 1)));
        let fut_sub = try_join_all((0..n).map(|_| atomic_add(&db, KEY, -1)));

        // Wait for all atomic operations
        try_join(fut_add, fut_sub).await?;
    }

    println!("check!");
    {
        let trx = db.create_trx()?;
        let value = trx.get(KEY, false).await?.expect("value should exists");
        let v: i64 = byteorder::LE::read_i64(&value);
        if v != 0 {
            panic!("expected 0, found {v}");
        }
    }
    Ok(())
}
