// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

use foundationdb::*;

mod common;

#[tokio::test]
async fn test_watch() -> FdbResult<()> {
    const KEY: &[u8] = b"test-watch";

    let db = common::database().await?;

    eprintln!("setting watch");
    let trx = db.create_trx()?;
    let watch = trx.watch(KEY);
    trx.commit().await?;
    eprintln!("watch committed");

    eprintln!("writing value");
    let trx = db.create_trx()?;
    let value = common::random_str(10);
    trx.set(KEY, value.as_bytes());
    trx.commit().await?;
    eprintln!("write committed");

    watch.await?;

    Ok(())
}

#[tokio::test]
async fn test_watch_without_commit() -> FdbResult<()> {
    const KEY: &[u8] = b"test-watch-2";

    let db = common::database().await?;

    eprintln!("setting watch");
    let trx = db.create_trx()?;
    let watch = trx.watch(KEY);

    drop(trx);
    assert!(watch.await.is_err());

    Ok(())
}
