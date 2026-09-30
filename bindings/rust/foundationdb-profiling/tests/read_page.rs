//! Integration tests of `ProfileScanner::read_page` against a live cluster.
//!
//! The test enables client profiling for the whole cluster (sample rate 1.0, like
//! `fdbcli> profile client set 1.0 default`), writes a few transactions, waits for the
//! client to flush them, reads them back, then restores the previous profiling settings
//! and clears the keys it wrote. Everything runs in a single test so that no other test
//! can switch profiling off under it.

use foundationdb::options::TransactionOption;
use foundationdb::tuple::pack;
use foundationdb::{ClientBudget, Database, FdbBindingError, KeySelector, RangeOption};
use foundationdb_profiling::{
    Cursor, Event, Page, ProfileScanner, ProfiledTransaction, SkipReason,
};
use futures_util::{FutureExt, TryStreamExt};
use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, SystemTime};

const SAMPLE_RATE_KEY: &[u8] =
    b"\xff\xff/global_config/config/fdb_client_info/client_txn_sample_rate";
const SIZE_LIMIT_KEY: &[u8] =
    b"\xff\xff/global_config/config/fdb_client_info/client_txn_size_limit";

/// Number of profiled transactions written by the test.
const TXNS: usize = 5;
/// Value size of each write, so the records span several range read batches.
const VALUE_LEN: usize = 2048;

type Id = ([u8; 10], [u8; 16]);

#[tokio::test]
async fn read_page_against_live_cluster() {
    let db = Database::default().expect("database");
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("fdbrs_profiling_test/{}_{nanos}/", std::process::id()).into_bytes();

    let previous = read_profiling_config(&db).await;
    set_profiling_config(&db, Some(pack(&1.0f64)), Some(pack(&-1i64))).await;

    let result = AssertUnwindSafe(scenarios(&db, &prefix))
        .catch_unwind()
        .await;

    set_profiling_config(&db, previous.0, previous.1).await;
    let end = [prefix.as_slice(), b"\xff"].concat();
    db.run(|trx, _| {
        let (begin, end) = (prefix.clone(), end.clone());
        async move {
            trx.clear_range(&begin, &end);
            Ok::<_, FdbBindingError>(())
        }
    })
    .await
    .expect("cleanup");

    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn scenarios(db: &Database, prefix: &[u8]) {
    let start = read_version(db).await;
    let ours = write_and_wait(db, prefix, start).await;
    let max_version = ours.iter().map(|t| t.version).max().unwrap();
    let end_version = max_version + 1;

    // decoded contents of our transactions
    for (i, tx) in ours.iter().enumerate() {
        let key = test_key(prefix, i);
        assert!(
            matches!(tx.events.first(), Some(Event::GetVersion(_))),
            "{tx:?}"
        );
        assert!(
            tx.events
                .iter()
                .any(|e| matches!(e, Event::Get(g) if g.key == key))
        );
        let commit = tx
            .events
            .iter()
            .find_map(|e| match e {
                Event::Commit(c) => Some(c),
                _ => None,
            })
            .expect("commit event");
        assert_eq!(commit.request.mutations.len(), 1);
        assert_eq!(commit.request.mutations[0].mutation_type, 0);
        assert_eq!(commit.request.mutations[0].param1, key);
        assert_eq!(commit.request.mutations[0].param2.len(), VALUE_LEN);
    }

    // one big page over a fixed window
    let window_cursor = Cursor::at_version(start);
    let window_scanner = ProfileScanner::new().end_version(end_version);
    let full = page(db, &window_scanner, &window_cursor, Stop::Time).await;
    assert!(full.exhausted);
    let all = ids(&full.transactions);
    for tx in &ours {
        assert!(all.contains(&(tx.versionstamp, tx.id)));
    }
    assert!(full.transactions.iter().all(|t| t.version < end_version));

    // pagination through the caller's budget: a tiny, deterministic byte budget is
    // exceeded as soon as the first batch is read, so every page stops after its first
    // row; and through a row count
    let byte_budget = Stop::Budget(ClientBudget {
        max_bytes_read: Some(1),
        ..ClientBudget::default()
    });
    for stop in [byte_budget, Stop::Rows(2), Stop::Rows(3)] {
        paginate(db, &window_scanner, &window_cursor, stop, &all, &ours).await;
    }

    // end_version is exclusive, at_version inclusive
    let mut versions: Vec<i64> = ours.iter().map(|t| t.version).collect();
    versions.sort();
    let split = versions[versions.len() / 2];
    let before = page(
        db,
        &window_scanner.clone().end_version(split),
        &window_cursor,
        Stop::Time,
    )
    .await;
    assert!(before.exhausted);
    assert!(before.transactions.iter().all(|t| t.version < split));
    let after = page(db, &window_scanner, &Cursor::at_version(split), Stop::Time).await;
    assert!(after.transactions.iter().all(|t| t.version >= split));
    let mut both = ids(&before.transactions);
    both.extend(ids(&after.transactions));
    assert_eq!(both, all);
    for tx in &ours {
        let in_before = before.transactions.iter().any(|t| t.id == tx.id);
        assert_eq!(in_before, tx.version < split);
    }
}

fn test_key(prefix: &[u8], i: usize) -> Vec<u8> {
    [prefix, format!("{i}").as_bytes()].concat()
}

/// Writes the test transactions and waits until all of them are flushed, rewriting the
/// ones not seen yet (the new sample rate reaches the client asynchronously).
async fn write_and_wait(db: &Database, prefix: &[u8], start: i64) -> Vec<ProfiledTransaction> {
    let mut found: Vec<Option<ProfiledTransaction>> = vec![None; TXNS];
    for _round in 0..6 {
        for (i, slot) in found.iter().enumerate() {
            if slot.is_none() {
                let key = test_key(prefix, i);
                db.run(|trx, _| {
                    let key = key.clone();
                    async move {
                        trx.get(&key, false).await?;
                        trx.set(&key, &[b'v'; VALUE_LEN]);
                        Ok::<_, FdbBindingError>(())
                    }
                })
                .await
                .expect("write");
            }
        }
        // the client flushes every CSI_STATUS_DELAY (10s)
        for _ in 0..15 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let cursor = Cursor::at_version(start);
            let scanner = ProfileScanner::new();
            let p = page(db, &scanner, &cursor, Stop::Time).await;
            for tx in p.transactions {
                for (i, slot) in found.iter_mut().enumerate() {
                    let key = test_key(prefix, i);
                    let ours = tx
                        .events
                        .iter()
                        .any(|e| matches!(e, Event::Get(g) if g.key == key));
                    if ours && slot.is_none() {
                        *slot = Some(tx.clone());
                    }
                }
            }
            if found.iter().all(Option::is_some) {
                return found.into_iter().flatten().collect();
            }
        }
    }
    panic!("profiling data was not flushed: {found:?}");
}

/// How the caller bounds a page.
#[derive(Debug, Clone)]
enum Stop {
    /// A 2 second time budget, like the crate docs example.
    Time,
    /// This client budget.
    Budget(ClientBudget),
    /// This many rows.
    Rows(usize),
}

/// Reads one page in one `db.run`, the way the crate docs describe it.
async fn page(db: &Database, scanner: &ProfileScanner, cursor: &Cursor, stop: Stop) -> Page {
    db.run(|trx, _| {
        let scanner = scanner.clone();
        let cursor = cursor.clone();
        let stop = stop.clone();
        async move {
            trx.set_option(TransactionOption::ReadSystemKeys)?;
            let (budget, max_rows) = match stop {
                Stop::Time => (
                    ClientBudget {
                        time_limit: Some(Duration::from_secs(2)),
                        ..ClientBudget::default()
                    },
                    None,
                ),
                Stop::Budget(budget) => (budget, None),
                Stop::Rows(rows) => (ClientBudget::default(), Some(rows)),
            };
            trx.set_client_budget(budget);
            let range = scanner.range(&cursor);
            let opt = RangeOption::from((
                KeySelector::first_greater_or_equal(range.begin.as_slice()),
                KeySelector::first_greater_or_equal(range.end.as_slice()),
            ));
            let rows = trx
                .get_ranges_keyvalues(opt, true)
                .map_ok(|kv| (kv.key().to_vec(), kv.value().to_vec()));
            let mut read = 0;
            let should_stop = || {
                read += 1;
                trx.check_client_budget().is_err() || max_rows.is_some_and(|max| read >= max)
            };
            let page = scanner
                .read_page(&cursor, rows, should_stop)
                .await
                .map_err(|err| err.into_error(FdbBindingError::new_custom_error))?;
            Ok::<_, FdbBindingError>(page)
        }
    })
    .await
    .expect("read_page")
}

/// Pages through the window with `stop` until exhausted, and checks that the pages
/// return every transaction of `all` once: at most once each, all of ours (single row
/// records) exactly once, and any other one missing only by being reported broken.
async fn paginate(
    db: &Database,
    scanner: &ProfileScanner,
    start: &Cursor,
    stop: Stop,
    all: &BTreeSet<Id>,
    ours: &[ProfiledTransaction],
) {
    let mut cursor = start.clone();
    let mut returned = Vec::new();
    let mut broken = BTreeSet::new();
    let mut stops = 0;
    let mut exhausted = false;
    for _ in 0..10_000 {
        let p = page(db, scanner, &cursor, stop.clone()).await;
        returned.extend(p.transactions);
        for skipped in p.skipped {
            assert_eq!(skipped.reason, SkipReason::BrokenChunks, "{stop:?}");
            broken.insert(skipped.id);
        }
        // round trip through the persisted form
        let restored = Cursor::from_bytes(p.next.as_bytes().to_vec()).expect("valid cursor");
        assert_eq!(restored, p.next);
        if p.exhausted {
            exhausted = true;
            break;
        }
        assert!(
            restored.as_bytes() > cursor.as_bytes(),
            "no progress {stop:?}"
        );
        cursor = restored;
        stops += 1;
    }
    assert!(exhausted, "pages never exhausted {stop:?}");
    assert!(stops > 0, "the window fits in one page {stop:?}");
    let got = ids(&returned);
    assert_eq!(got.len(), returned.len(), "returned twice {stop:?}");
    for tx in ours {
        assert!(got.contains(&(tx.versionstamp, tx.id)), "{stop:?}");
    }
    for id in all.difference(&got) {
        assert!(
            broken.contains(&id.1),
            "{id:?} lost without a report {stop:?}"
        );
    }
    assert!(got.is_subset(all), "{stop:?}");
}

fn ids(txs: &[ProfiledTransaction]) -> BTreeSet<Id> {
    txs.iter().map(|t| (t.versionstamp, t.id)).collect()
}

async fn read_version(db: &Database) -> i64 {
    let trx = db.create_trx().expect("trx");
    trx.get_read_version().await.expect("read version")
}

async fn read_profiling_config(db: &Database) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    db.run(|trx, _| async move {
        let rate = trx.get(SAMPLE_RATE_KEY, false).await?.map(|v| v.to_vec());
        let size = trx.get(SIZE_LIMIT_KEY, false).await?.map(|v| v.to_vec());
        Ok::<_, FdbBindingError>((rate, size))
    })
    .await
    .expect("read profiling config")
}

/// Writes the global profiling configuration, `None` meaning `default` (what
/// `fdbcli> profile client set default default` writes).
async fn set_profiling_config(db: &Database, rate: Option<Vec<u8>>, size: Option<Vec<u8>>) {
    let rate = rate.unwrap_or_else(|| pack(&f64::INFINITY));
    let size = size.unwrap_or_else(|| pack(&-1i64));
    db.run(|trx, _| {
        let (rate, size) = (rate.clone(), size.clone());
        async move {
            trx.set_option(TransactionOption::SpecialKeySpaceEnableWrites)?;
            trx.set(SAMPLE_RATE_KEY, &rate);
            trx.set(SIZE_LIMIT_KEY, &size);
            Ok::<_, FdbBindingError>(())
        }
    })
    .await
    .expect("set profiling config");
}
