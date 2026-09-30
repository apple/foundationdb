//! Decodes every transaction currently in the client profiling keyspace, as a check that
//! the decoder handles real client-written data, not just the fixtures and the small
//! scenarios `read_page.rs` writes itself.
//!
//! The C++ client only flushes sampled transactions every `CSI_STATUS_DELAY` (10 seconds
//! by default) and drops whatever is still queued at process exit, so a short `cargo test
//! -p foundationdb` run is not a dependable source of profiling data on its own. This test
//! therefore generates its own varied traffic (gets, a range read, a plain set, a clear, a
//! clear range, several atomic ops, a commit conflict, and a best-effort read error) under
//! a unique key prefix before decoding, so it always has something of its own to find. It
//! enables client profiling itself (and restores the previous setting) when it finds it
//! disabled, the way `read_page.rs` does, so it also works against a fresh local cluster.
//! On CI, `.github/workflows/profiling.yml` already enables profiling cluster-wide and
//! runs the `foundationdb` crate's test suite first as extra background traffic. It is
//! `#[ignore]`d so that a plain `cargo test -p foundationdb-profiling` does not depend on a
//! live cluster.

use foundationdb::options::{MutationType, TransactionOption};
use foundationdb::tuple::pack;
use foundationdb::{ClientBudget, Database, FdbBindingError, KeySelector, RangeOption};
use foundationdb_profiling::{
    Aggregator, Cursor, Event, Mutation, Page, ProfileScanner, ProfiledTransaction, SkipReason,
};
use futures_util::{FutureExt, TryStreamExt};
use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, SystemTime};

/// Upper bound on the number of pages read, so a bug that never exhausts the range fails
/// fast instead of hanging.
const MAX_PAGES: usize = 100_000;
/// How long to wait, and how many times, for our own traffic to be flushed and show up in
/// a page starting at the version read before we wrote anything.
const MARKER_POLL_INTERVAL: Duration = Duration::from_secs(2);
const MARKER_POLL_ATTEMPTS: usize = 30;

const SAMPLE_RATE_KEY: &[u8] =
    b"\xff\xff/global_config/config/fdb_client_info/client_txn_sample_rate";
const SIZE_LIMIT_KEY: &[u8] =
    b"\xff\xff/global_config/config/fdb_client_info/client_txn_size_limit";

#[tokio::test]
#[ignore = "needs a live cluster, see profiling.yml"]
async fn decode_all_client_latency_transactions() {
    let db = Database::default().expect("database");
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("fdbrs_profiling_decode_all/{}_{nanos}/", std::process::id()).into_bytes();

    let previous = read_profiling_config(&db).await;
    let restore_profiling = previous.0.is_none();
    if restore_profiling {
        set_profiling_config(&db, Some(pack(&1.0f64)), Some(pack(&-1i64))).await;
        // The client picks up a global config change asynchronously; give it a moment
        // before generating traffic, so the very first transaction is not sampled out.
        tokio::time::sleep(MARKER_POLL_INTERVAL).await;
    }

    let result = AssertUnwindSafe(run(&db, prefix.clone()))
        .catch_unwind()
        .await;

    if restore_profiling {
        set_profiling_config(&db, previous.0, previous.1).await;
    }
    cleanup(&db, &prefix).await;

    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

/// Generates the test's own traffic, waits for it to show up, then decodes and checks the
/// whole profiling keyspace.
async fn run(db: &Database, prefix: Vec<u8>) {
    let start = read_version(db).await;
    let markers = generate_traffic(db, prefix).await;
    wait_for_markers(db, start, &markers.prefix).await;
    scan_and_assert(db, &markers).await;
}

/// Unique keys and ranges used to positively identify our own traffic among everything
/// else that may be in the profiling keyspace (other tests, other CI traffic).
struct Markers {
    prefix: Vec<u8>,
    hit_key: Vec<u8>,
    miss_key: Vec<u8>,
    range: (Vec<u8>, Vec<u8>),
    set_key: Vec<u8>,
    clear_key: Vec<u8>,
    clear_range: (Vec<u8>, Vec<u8>),
    add_key: Vec<u8>,
    max_key: Vec<u8>,
    byte_min_key: Vec<u8>,
    versionstamp_key: Vec<u8>,
    conflict_key: Vec<u8>,
    too_old_key: Vec<u8>,
}

impl Markers {
    fn new(prefix: Vec<u8>) -> Self {
        let key = |name: &str| [prefix.as_slice(), name.as_bytes()].concat();
        Markers {
            hit_key: key("hit"),
            miss_key: key("miss"),
            range: (key("range/"), key("range0")),
            set_key: key("set"),
            clear_key: key("clear"),
            clear_range: (key("clear_range/"), key("clear_range0")),
            add_key: key("add"),
            max_key: key("max"),
            byte_min_key: key("byte_min"),
            versionstamp_key: key("versionstamp"),
            conflict_key: key("conflict"),
            too_old_key: key("too_old"),
            prefix,
        }
    }

    /// Marks off which of our own scenarios a decoded event belongs to.
    fn record(&self, event: &Event, found: &mut Found) {
        match event {
            Event::Get(g) if g.key == self.hit_key => found.hit = true,
            Event::Get(g) if g.key == self.miss_key => found.miss = true,
            Event::GetRange(gr)
                if gr.range.begin == self.range.0 && gr.range.end == self.range.1 =>
            {
                found.range = true;
            }
            Event::GetError(e) if e.key == self.too_old_key && e.error_code == 1007 => {
                found.too_old = true;
            }
            Event::Commit(c) => {
                for mutation in &c.request.mutations {
                    self.record_mutation(mutation, found);
                }
            }
            Event::CommitError(c) if c.error_code == 1020 => {
                if c.request
                    .mutations
                    .iter()
                    .any(|m| m.param1 == self.conflict_key)
                {
                    found.conflict = true;
                }
            }
            _ => {}
        }
    }

    fn record_mutation(&self, m: &Mutation, found: &mut Found) {
        if m.mutation_type == Mutation::SET_VALUE && m.param1 == self.set_key {
            found.set = true;
        } else if m.mutation_type == Mutation::CLEAR_RANGE && m.param1 == self.clear_key {
            found.clear = true;
        } else if m.mutation_type == Mutation::CLEAR_RANGE
            && m.param1 == self.clear_range.0
            && m.param2 == self.clear_range.1
        {
            found.clear_range = true;
        } else if m.mutation_type == Mutation::ADD_VALUE && m.param1 == self.add_key {
            found.add = true;
        } else if m.mutation_type == Mutation::MAX && m.param1 == self.max_key {
            found.max = true;
        } else if m.mutation_type == Mutation::BYTE_MIN && m.param1 == self.byte_min_key {
            found.byte_min = true;
        } else if m.mutation_type == Mutation::SET_VERSIONSTAMPED_VALUE
            && m.param1 == self.versionstamp_key
        {
            found.versionstamp = true;
        }
    }
}

/// Which of the marker scenarios have been seen decoded so far.
#[derive(Default)]
struct Found {
    hit: bool,
    miss: bool,
    range: bool,
    set: bool,
    clear: bool,
    clear_range: bool,
    add: bool,
    max: bool,
    byte_min: bool,
    versionstamp: bool,
    conflict: bool,
    /// Best-effort: a freshly started cluster may still have version 1 inside its MVCC
    /// window, in which case the read never errors. Never asserted on its own.
    too_old: bool,
}

impl Found {
    /// Names of the scenarios (other than the best-effort read error) not yet observed.
    fn missing(&self) -> Vec<&'static str> {
        let checks: [(&str, bool); 11] = [
            ("get (hit)", self.hit),
            ("get (miss)", self.miss),
            ("get_range", self.range),
            ("set", self.set),
            ("clear", self.clear),
            ("clear_range", self.clear_range),
            ("atomic add", self.add),
            ("atomic max", self.max),
            ("atomic byte_min", self.byte_min),
            ("atomic set_versionstamped_value", self.versionstamp),
            ("commit conflict", self.conflict),
        ];
        checks
            .into_iter()
            .filter(|(_, ok)| !ok)
            .map(|(name, _)| name)
            .collect()
    }
}

/// Writes the test's own traffic: gets (hit and miss), a range read, a plain set, a clear,
/// a clear range, atomic add/max/byte_min/set_versionstamped_value, one commit conflict,
/// and a best-effort read error. Every transaction is dropped as soon as it is done with
/// (committed, or explicitly dropped for read-only ones), since the C++ client only
/// records a sampled transaction's events when its native transaction is destroyed.
async fn generate_traffic(db: &Database, prefix: Vec<u8>) -> Markers {
    let m = Markers::new(prefix);

    // get: hit and miss.
    let trx = db.create_trx().expect("trx");
    trx.set(&m.hit_key, b"present");
    trx.commit().await.expect("commit hit seed");

    let trx = db.create_trx().expect("trx");
    trx.get(&m.hit_key, false).await.expect("get hit");
    drop(trx);

    let trx = db.create_trx().expect("trx");
    let miss = trx.get(&m.miss_key, false).await.expect("get miss");
    assert!(miss.is_none(), "miss key unexpectedly present");
    drop(trx);

    // get_range.
    let trx = db.create_trx().expect("trx");
    trx.set(&[m.range.0.as_slice(), b"x"].concat(), b"v");
    trx.commit().await.expect("commit range seed");

    let trx = db.create_trx().expect("trx");
    let range = RangeOption::from((m.range.0.clone(), m.range.1.clone()));
    trx.get_range(&range, 1, false).await.expect("get_range");
    drop(trx);

    // set.
    let trx = db.create_trx().expect("trx");
    trx.set(&m.set_key, b"set-value");
    trx.commit().await.expect("commit set");

    // clear (single key).
    let trx = db.create_trx().expect("trx");
    trx.set(&m.clear_key, b"to-clear");
    trx.commit().await.expect("commit clear seed");
    let trx = db.create_trx().expect("trx");
    trx.clear(&m.clear_key);
    trx.commit().await.expect("commit clear");

    // clear_range.
    let trx = db.create_trx().expect("trx");
    trx.set(&m.clear_range.0, b"v");
    trx.commit().await.expect("commit clear_range seed");
    let trx = db.create_trx().expect("trx");
    trx.clear_range(&m.clear_range.0, &m.clear_range.1);
    trx.commit().await.expect("commit clear_range");

    // atomic ops.
    let trx = db.create_trx().expect("trx");
    trx.atomic_op(&m.add_key, &1i64.to_le_bytes(), MutationType::Add);
    trx.commit().await.expect("commit add");

    let trx = db.create_trx().expect("trx");
    trx.atomic_op(&m.max_key, &1i64.to_le_bytes(), MutationType::Max);
    trx.commit().await.expect("commit max");

    let trx = db.create_trx().expect("trx");
    trx.atomic_op(&m.byte_min_key, b"m", MutationType::ByteMin);
    trx.commit().await.expect("commit byte_min");

    let trx = db.create_trx().expect("trx");
    // Versionstamp placeholder (10 zero bytes) plus the little-endian offset (0) pointing
    // at it: any valid transform works, this test does not read the value back.
    let mut versionstamped_value = vec![0u8; 10];
    versionstamped_value.extend_from_slice(&0i32.to_le_bytes());
    trx.atomic_op(
        &m.versionstamp_key,
        &versionstamped_value,
        MutationType::SetVersionstampedValue,
    );
    trx.commit().await.expect("commit versionstamped value");

    // One commit conflict: the reader reads the key, the writer commits a change to it
    // first, then the reader's own commit fails with not_committed (1020).
    let reader = db.create_trx().expect("trx");
    reader
        .get(&m.conflict_key, false)
        .await
        .expect("get conflict key");
    let writer = db.create_trx().expect("trx");
    writer.set(&m.conflict_key, b"writer");
    writer.commit().await.expect("commit writer");
    reader.set(&m.conflict_key, b"reader");
    let conflict_result = reader.commit().await;
    match &conflict_result {
        Err(e) if e.code() == 1020 => {}
        other => panic!("expected a commit conflict (error 1020), got {other:?}"),
    }
    drop(conflict_result);

    // Best-effort read error: a read version far enough in the past to have left the MVCC
    // window gives transaction_too_old (1007). Not asserted here: on a cluster started
    // just for this test run, version 1 may still be inside the window.
    let trx = db.create_trx().expect("trx");
    trx.set_read_version(1);
    let _ = trx.get(&m.too_old_key, false).await;
    drop(trx);

    m
}

/// Polls `read_page` from `start` until a transaction touching `prefix` shows up, or gives
/// up after [`MARKER_POLL_ATTEMPTS`]. The scan that follows checks the actual outcome.
async fn wait_for_markers(db: &Database, start: i64, prefix: &[u8]) {
    let cursor = Cursor::at_version(start);
    for attempt in 0..MARKER_POLL_ATTEMPTS {
        let page = read_one_page(db, &cursor).await;
        if page
            .transactions
            .iter()
            .any(|tx| transaction_touches_prefix(tx, prefix))
        {
            return;
        }
        if attempt + 1 < MARKER_POLL_ATTEMPTS {
            tokio::time::sleep(MARKER_POLL_INTERVAL).await;
        }
    }
}

fn transaction_touches_prefix(tx: &ProfiledTransaction, prefix: &[u8]) -> bool {
    tx.events
        .iter()
        .any(|event| transaction_touches_prefix_event(event, prefix))
}

fn transaction_touches_prefix_event(event: &Event, prefix: &[u8]) -> bool {
    match event {
        Event::Get(g) => g.key.starts_with(prefix),
        Event::GetRange(gr) => {
            gr.range.begin.starts_with(prefix) || gr.range.end.starts_with(prefix)
        }
        Event::GetError(e) => e.key.starts_with(prefix),
        Event::GetRangeError(e) => {
            e.range.begin.starts_with(prefix) || e.range.end.starts_with(prefix)
        }
        Event::Commit(c) => c
            .request
            .mutations
            .iter()
            .any(|m| m.param1.starts_with(prefix)),
        Event::CommitError(c) => c
            .request
            .mutations
            .iter()
            .any(|m| m.param1.starts_with(prefix)),
        Event::GetVersion(_) => false,
        _ => false,
    }
}

/// Decodes the whole profiling keyspace and checks it: no decode errors, and every one of
/// our own marker scenarios (other than the best-effort read error) was decoded.
async fn scan_and_assert(db: &Database, markers: &Markers) {
    let mut cursor = Cursor::beginning();
    let mut aggregator = Aggregator::default();
    let mut total_transactions = 0usize;
    let mut event_counts: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut decode_errors = Vec::new();
    let mut broken_chunks = 0usize;
    let mut found = Found::default();

    for _ in 0..MAX_PAGES {
        let page = read_one_page(db, &cursor).await;

        for tx in &page.transactions {
            total_transactions += 1;
            for event in &tx.events {
                *event_counts.entry(event_name(event)).or_insert(0) += 1;
                markers.record(event, &mut found);
            }
            aggregator.record(tx);
        }
        for skipped in &page.skipped {
            match &skipped.reason {
                SkipReason::Decode(err) => {
                    decode_errors.push((skipped.versionstamp, skipped.id, err.clone()));
                }
                SkipReason::BrokenChunks => {
                    broken_chunks += 1;
                    println!(
                        "broken chunks: versionstamp={:?} id={:?}",
                        skipped.versionstamp, skipped.id
                    );
                }
                _ => {}
            }
        }

        cursor = page.next;
        if page.exhausted {
            break;
        }
    }

    println!("transactions decoded: {total_transactions}");
    println!("broken chunks skipped: {broken_chunks}");
    println!("event counts:");
    for (name, count) in &event_counts {
        println!("  {name}: {count}");
    }
    println!("reads total: {}", aggregator.reads().total());
    println!("writes total: {}", aggregator.writes().total());

    if !decode_errors.is_empty() {
        for (versionstamp, id, err) in &decode_errors {
            println!("decode error: versionstamp={versionstamp:?} id={id:?} error={err}");
        }
        panic!(
            "{} transaction(s) failed to decode, see printed ids/errors above",
            decode_errors.len()
        );
    }
    assert!(
        total_transactions >= 1,
        "no transactions were read; is client profiling enabled and has the cluster served traffic?"
    );

    let missing = found.missing();
    assert!(
        missing.is_empty(),
        "marker transaction(s) were not decoded: {missing:?}"
    );
    if !found.too_old {
        println!("note: the best-effort transaction_too_old marker was not observed, skipping it");
    }
}

/// Reads one page bounded by a 2 second time budget, like the crate docs example.
async fn read_one_page(db: &Database, cursor: &Cursor) -> Page {
    let scanner = ProfileScanner::new();
    db.run(|trx, _| {
        let scanner = scanner.clone();
        let cursor = cursor.clone();
        async move {
            trx.set_option(TransactionOption::ReadSystemKeys)?;
            trx.set_client_budget(ClientBudget {
                time_limit: Some(Duration::from_secs(2)),
                ..ClientBudget::default()
            });
            let range = scanner.range(&cursor);
            let opt = RangeOption::from((
                KeySelector::first_greater_or_equal(range.begin.as_slice()),
                KeySelector::first_greater_or_equal(range.end.as_slice()),
            ));
            let rows = trx
                .get_ranges_keyvalues(opt, true)
                .map_ok(|kv| (kv.key().to_vec(), kv.value().to_vec()));
            let page = scanner
                .read_page(&cursor, rows, || trx.check_client_budget().is_err())
                .await
                .map_err(|err| err.into_error(FdbBindingError::new_custom_error))?;
            Ok::<_, FdbBindingError>(page)
        }
    })
    .await
    .expect("read_page")
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

async fn cleanup(db: &Database, prefix: &[u8]) {
    let end = [prefix, b"\xff".as_slice()].concat();
    db.run(|trx, _| {
        let (begin, end) = (prefix.to_vec(), end.clone());
        async move {
            trx.clear_range(&begin, &end);
            Ok::<_, FdbBindingError>(())
        }
    })
    .await
    .expect("cleanup");
}

fn event_name(event: &Event) -> &'static str {
    match event {
        Event::GetVersion(_) => "GetVersion",
        Event::Get(_) => "Get",
        Event::GetRange(_) => "GetRange",
        Event::Commit(_) => "Commit",
        Event::GetError(_) => "GetError",
        Event::GetRangeError(_) => "GetRangeError",
        Event::CommitError(_) => "CommitError",
        _ => "Unknown",
    }
}
