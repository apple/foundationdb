// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

#[cfg_api_versions(min = 710)]
use crate::tuple::Subspace;

use foundationdb::*;
use foundationdb_macros::cfg_api_versions;
use foundationdb_sys::if_cfg_api_versions;
use futures::future;
use futures::prelude::*;

use std::borrow::Cow;

mod common;

// Kept as a single sequential #[test] on purpose: test_range_option_async and
// test_get_estimate_range share the "test-rangeoption-" prefix, and the two
// mapped-range tests clear the same data/index subspaces.
#[test]
fn test_range() {
    let _guard = foundationdb::boot().expect("failed to initialize FoundationDB");
    futures::executor::block_on(test_get_range_async()).expect("failed to run");
    futures::executor::block_on(test_range_option_async()).expect("failed to run");
    futures::executor::block_on(test_get_ranges_async()).expect("failed to run");
    if_cfg_api_versions!(min = 630 =>
        futures::executor::block_on(test_get_estimate_range()).expect("failed to run")
    );
    if_cfg_api_versions!(min = 700 =>
        futures::executor::block_on(test_get_range_split_points()).expect("failed to run")
    );
    if_cfg_api_versions!(min = 710 =>
            futures::executor::block_on(test_mapped_value()).expect("failed to run");
            futures::executor::block_on(test_mapped_values()).expect("failed to run")
    )
}

async fn test_get_range_async() -> FdbResult<()> {
    const N: usize = 10000;

    let db = common::database().await?;

    {
        let trx = db.create_trx()?;
        let key_begin = "test-range-";
        let key_end = "test-range.";

        eprintln!("clearing...");
        trx.clear_range(key_begin.as_bytes(), key_end.as_bytes());

        eprintln!("inserting...");
        for _ in 0..N {
            let key = format!("{}-{}", key_begin, common::random_str(10));
            let value = common::random_str(10);
            trx.set(key.as_bytes(), value.as_bytes());
        }

        eprintln!("counting...");
        let begin = KeySelector::first_greater_or_equal(Cow::Borrowed(key_begin.as_bytes()));
        let end = KeySelector::first_greater_than(Cow::Borrowed(key_end.as_bytes()));
        let opt = RangeOption::from((begin, end));

        let range = trx.get_range(&opt, 1, false).await?;
        assert!(!range.is_empty());
        assert!(range.more());
        let len = range.len();
        let mut i = 0;
        for kv in &range {
            assert!(!kv.key().is_empty());
            assert!(!kv.value().is_empty());
            i += 1;
        }
        assert_eq!(i, len);

        let refs_asc = (&range).into_iter().collect::<Vec<_>>();
        assert_eq!(
            refs_asc,
            (&range).into_iter().rev().rev().collect::<Vec<_>>()
        );

        let owned_asc = trx
            .get_range(&opt, 1, false)
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(owned_asc, range.into_iter().rev().rev().collect::<Vec<_>>());
    }

    Ok(())
}

async fn test_get_ranges_async() -> FdbResult<()> {
    const N: usize = 10000;

    let db = common::database().await?;

    {
        let trx = db.create_trx()?;
        let key_begin = "test-ranges-";
        let key_end = "test-ranges.";

        eprintln!("clearing...");
        trx.clear_range(key_begin.as_bytes(), key_end.as_bytes());

        eprintln!("inserting...");
        for _ in 0..N {
            let key = format!("{}-{}", key_begin, common::random_str(10));
            let value = common::random_str(10);
            trx.set(key.as_bytes(), value.as_bytes());
        }

        eprintln!("counting...");
        let begin = KeySelector::first_greater_or_equal(Cow::Borrowed(key_begin.as_bytes()));
        let end = KeySelector::first_greater_than(Cow::Borrowed(key_end.as_bytes()));
        let opt = RangeOption::from((begin, end));

        let count = trx
            .get_ranges(opt.clone(), false)
            .try_fold(0usize, |count, kvs| future::ok(count + kvs.as_ref().len()))
            .await?;

        assert_eq!(count, N);
        eprintln!("count: {count:?}");

        let unlimited = RangeOption {
            limit: Some(0),
            ..opt
        };
        for reverse in [false, true] {
            let opt = RangeOption {
                reverse,
                ..unlimited.clone()
            };
            let (pages, count) = trx
                .get_ranges(opt, false)
                .try_fold((0usize, 0usize), |(pages, count), kvs| {
                    future::ok((pages + 1, count + kvs.len()))
                })
                .await?;
            assert!(pages > 1, "expected a paginated range scan");
            assert_eq!(count, N);
        }
    }

    Ok(())
}

async fn test_range_option_async() -> FdbResult<()> {
    let db = common::database().await?;

    {
        let trx = db.create_trx()?;
        let key_begin = "test-rangeoption-";
        let key_end = "test-rangeoption.";
        let k = |i: u32| format!("{key_begin}-{i:010}");

        eprintln!("clearing...");
        trx.clear_range(key_begin.as_bytes(), key_end.as_bytes());

        eprintln!("inserting...");
        for i in 0..10000 {
            let value = common::random_str(10);
            trx.set(k(i).as_bytes(), value.as_bytes());
        }
        assert_eq!(
            trx.get_ranges(
                (KeySelector::first_greater_or_equal(k(100).into_bytes())
                    ..KeySelector::first_greater_or_equal(k(5000).as_bytes()))
                    .into(),
                false
            )
            .try_fold(0usize, |count, kvs| future::ok(count + kvs.as_ref().len()))
            .await?,
            4900
        );
        assert_eq!(
            trx.get_ranges(
                (
                    KeySelector::first_greater_or_equal(k(100).into_bytes()),
                    KeySelector::first_greater_or_equal(k(5000).as_bytes())
                )
                    .into(),
                false
            )
            .try_fold(0usize, |count, kvs| future::ok(count + kvs.as_ref().len()))
            .await?,
            4900
        );
        assert_eq!(
            trx.get_ranges((k(100).into_bytes()..k(5000).into_bytes()).into(), false)
                .try_fold(0usize, |count, kvs| future::ok(count + kvs.as_ref().len()))
                .await?,
            4900
        );
        assert_eq!(
            trx.get_ranges((k(100).into_bytes(), k(5000).into_bytes()).into(), false)
                .try_fold(0usize, |count, kvs| future::ok(count + kvs.as_ref().len()))
                .await?,
            4900
        );
        assert_eq!(
            trx.get_ranges((k(100).as_bytes()..k(5000).as_bytes()).into(), false)
                .try_fold(0usize, |count, kvs| future::ok(count + kvs.as_ref().len()))
                .await?,
            4900
        );
        assert_eq!(
            trx.get_ranges((k(100).as_bytes(), k(5000).as_bytes()).into(), false)
                .try_fold(0usize, |count, kvs| future::ok(count + kvs.as_ref().len()))
                .await?,
            4900
        );

        assert_eq!(
            trx.get_ranges(
                (KeySelector::first_greater_or_equal(k(100).into_bytes())
                    ..KeySelector::first_greater_than(k(5000).as_bytes()))
                    .into(),
                false
            )
            .try_fold(0usize, |count, kvs| future::ok(count + kvs.as_ref().len()))
            .await?,
            4901
        );
        assert_eq!(
            trx.get_ranges((k(100).into_bytes()..=k(5000).into_bytes()).into(), false)
                .try_fold(0usize, |count, kvs| future::ok(count + kvs.as_ref().len()))
                .await?,
            4901
        );
        assert_eq!(
            trx.get_ranges((k(100).as_bytes()..=k(5000).as_bytes()).into(), false)
                .try_fold(0usize, |count, kvs| future::ok(count + kvs.as_ref().len()))
                .await?,
            4901
        );
    }

    Ok(())
}

#[cfg_api_versions(min = 630)]
async fn test_get_estimate_range() -> FdbResult<()> {
    const N: u32 = 10000;

    let db = common::database().await?;
    let trx = db.create_trx()?;
    let key_begin = "test-rangeoption-";
    let key_end = "test-rangeoption.";
    let k = |i: u32| format!("{key_begin}-{i:010}");

    eprintln!("clearing...");
    trx.clear_range(key_begin.as_bytes(), key_end.as_bytes());

    eprintln!("inserting...");
    for i in 0..N {
        let value = common::random_str(10);
        trx.set(k(i).as_bytes(), value.as_bytes());
    }
    trx.commit().await?;

    let trx = db.create_trx()?;
    let estimate = trx
        .get_estimated_range_size_bytes(key_begin.as_bytes(), key_end.as_bytes())
        .await?;
    eprintln!("get an estimate of {estimate} bytes");
    assert!(estimate > 0);

    Ok(())
}

#[cfg_api_versions(min = 700)]
async fn test_get_range_split_points() -> FdbResult<()> {
    const N: u32 = 10000;

    let db = common::database().await?;
    let trx = db.create_trx()?;
    let key_begin = "test-split-point-";
    let key_end = "test-split-point.";
    let k = |i: u32| format!("{key_begin}-{i:010}");

    eprintln!("clearing...");
    trx.clear_range(key_begin.as_bytes(), key_end.as_bytes());

    eprintln!("inserting...");
    for i in 0..N {
        let value = common::random_str(10);
        trx.set(k(i).as_bytes(), value.as_bytes());
    }
    trx.commit().await?;

    let trx = db.create_trx()?;
    let splits = trx
        .get_range_split_points(key_begin.as_bytes(), key_end.as_bytes(), 100)
        .await?;
    assert!(!splits.is_empty());
    for split in splits {
        eprintln!("split point: {:?}", split.key());
    }
    Ok(())
}

#[cfg_api_versions(min = 710)]
async fn test_mapped_value() -> FdbResult<()> {
    use foundationdb::tuple::{Subspace, pack};

    let db = common::database().await?;

    let data_subspace = Subspace::all().subspace(&("test-range-data"));
    let index_subspace = Subspace::all().subspace(&("test-range-index"));
    let number_of_records: i32 = 20;

    clear_mapped_data(&db, &data_subspace, &index_subspace).await;

    let blue_counter =
        setup_mapped_data(&db, &data_subspace, &index_subspace, number_of_records).await?;

    let t = db.create_trx()?;
    let range_option = RangeOption::from(&index_subspace.subspace(&("blue")));

    // The mapper is a Tuple that allow to transform keys.
    // This one is allowing fdb to convert a key like `("test-range-index", "blue", PRIMARY_KEY)`
    // to generate a scan in the range `("test-range-data", PRIMARY_KEY, ...)`
    // More info can be found here: https://github.com/apple/foundationdb/wiki/Everything-about-GetMappedRange
    let mapper = pack(&("test-range-data", "{K[2]}", "{...}"));

    let mapped_key_values = t
        .get_mapped_range(&range_option, &mapper, 1024, false)
        .await?;

    verify_mapped_values(blue_counter, vec![mapped_key_values]);

    Ok(())
}

#[cfg_api_versions(min = 710)]
async fn clear_mapped_data(db: &Database, data_subspace: &Subspace, index_subspace: &Subspace) {
    let t = db.create_trx().expect("could not create transaction");
    t.clear_subspace_range(data_subspace);
    t.clear_subspace_range(index_subspace);
    t.commit().await.expect("could not commit");
}

#[cfg_api_versions(min = 710)]
fn verify_mapped_values(
    blue_counter: i32,
    vec_mapped_key_values: Vec<mapped_key_values::MappedKeyValues>,
) {
    use foundationdb::tuple::{Element, unpack};

    let mut value_counter = 0;

    for mapped_key_values in vec_mapped_key_values {
        for mapped_key_value in mapped_key_values {
            // checking the parent key that generated the scan
            let parent_key: Vec<Element> =
                unpack(mapped_key_value.parent_key()).expect("could not unpack index key");
            assert!(parent_key.starts_with(&[
                Element::String(Cow::from("test-range-index")),
                Element::String(Cow::from("blue"))
            ]));

            let mapped_values = mapped_key_value.key_values();
            assert_eq!(
                mapped_values.len(),
                2,
                "bad length, expecting 2, got {}",
                mapped_values.len()
            );

            for kv in mapped_values {
                let key: Vec<Element> = unpack(kv.key()).expect("could not unpack key");
                assert!(key.starts_with(&[Element::String(Cow::from("test-range-data"))]));
            }
            value_counter += 1;
        }
    }
    assert_eq!(
        value_counter, blue_counter,
        "found {value_counter} elements instead of {blue_counter}"
    );
}

#[cfg_api_versions(min = 710)]
async fn test_mapped_values() -> FdbResult<()> {
    use foundationdb::tuple::{Subspace, pack};

    let db = common::database().await?;

    let data_subspace = Subspace::all().subspace(&("test-range-data"));
    let index_subspace = Subspace::all().subspace(&("test-range-index"));
    let number_of_records: i32 = 10_000;

    clear_mapped_data(&db, &data_subspace, &index_subspace).await;

    let blue_counter =
        setup_mapped_data(&db, &data_subspace, &index_subspace, number_of_records).await?;

    let t = db.create_trx()?;
    let range_option = RangeOption::from(&index_subspace.subspace(&("blue")));

    // The mapper is a Tuple that allow to transform keys.
    // This one is allowing fdb to convert a key like `("test-range-index", "blue", PRIMARY_KEY)`
    // to generate a scan in the range `("test-range-data", PRIMARY_KEY, ...)`
    // More info can be found here: https://github.com/apple/foundationdb/wiki/Everything-about-GetMappedRange
    let mapper = pack(&("test-range-data", "{K[2]}", "{...}"));

    let key_values = t
        .get_mapped_ranges(range_option.clone(), &mapper, false)
        .try_collect::<Vec<mapped_key_values::MappedKeyValues>>()
        .await?;

    dbg!(t.get_approximate_size().await?);

    verify_mapped_values(blue_counter, key_values);

    let unlimited = RangeOption {
        limit: Some(0),
        ..range_option
    };
    let (pages, parents) = t
        .get_mapped_ranges(unlimited, &mapper, false)
        .try_fold((0usize, 0usize), |(pages, parents), values| {
            future::ok((pages + 1, parents + values.len()))
        })
        .await?;
    assert!(pages > 1, "expected a paginated mapped range scan");
    assert_eq!(parents, blue_counter as usize);

    Ok(())
}

#[cfg_api_versions(min = 710)]
async fn setup_mapped_data(
    db: &Database,
    data_subspace: &Subspace,
    index_subspace: &Subspace,
    number_of_records: i32,
) -> Result<i32, FdbError> {
    use foundationdb::tuple::pack;

    let mut written_records = 0;
    let mut blue_counter = 0;

    loop {
        let trx = db.create_trx()?;
        eprintln!(
            "Creating a new transaction, {} records left to write",
            number_of_records - written_records
        );
        loop {
            let primary_key = written_records;
            let eye_color = match primary_key % 3 {
                0 => {
                    blue_counter += 1;
                    "blue"
                }
                1 => "brown",
                2 => "green",
                _ => unreachable!(),
            };

            // write into the data subspace
            trx.set(
                &data_subspace.pack(&(primary_key, "eye_color", eye_color)),
                eye_color.as_bytes(),
            );
            // write another key next to it
            trx.set(
                &data_subspace.pack(&(primary_key, "some_data")),
                &pack(&("fdb-rs")),
            );
            // write into the index subspace
            trx.set(&index_subspace.pack(&(eye_color, primary_key)), &[]);

            written_records += 1;
            if written_records == number_of_records {
                eprintln!("committing batch, because we have all records");
                break;
            }

            let size_trx = trx.get_approximate_size().await?;
            if size_trx >= 100_000 {
                eprintln!("committing batch, because we have already written ~{size_trx}b");
                break;
            }
        }
        trx.commit().await.expect("could not commit");
        if written_records == number_of_records {
            eprintln!("records are now ready");
            break;
        }
    }

    Ok(blue_counter)
}
