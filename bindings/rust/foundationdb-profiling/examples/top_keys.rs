//! Reads the whole client profiling keyspace and prints the hottest keys, ranges and
//! write hot spots, the way an application embedding this crate would.
//!
//! Enable client profiling on the cluster first, for instance for 1% of transactions:
//!
//! ```text
//! fdbcli --exec "profile client set 0.01 default"
//! ```
//!
//! Then, once the client has served some traffic and flushed its samples, run this
//! example against the same cluster (optionally pass a cluster file as the first
//! argument, otherwise the default one is used):
//!
//! ```text
//! cargo run -p foundationdb-profiling --example top_keys
//! ```

use foundationdb::options::TransactionOption;
use foundationdb::tuple::Bytes;
use foundationdb::{ClientBudget, Database, FdbBindingError, KeySelector, RangeOption};
use foundationdb_profiling::{Aggregator, Cursor, ProfileScanner, SkipReason};
use futures_util::TryStreamExt;
use std::time::Duration;

const TOP_N: usize = 10;
const BUCKET_COUNT: usize = 10;

#[tokio::main]
async fn main() {
    foundationdb::boot().expect("failed to initialize FoundationDB");

    let cluster_file = std::env::args().nth(1);
    let db = Database::new(cluster_file.as_deref()).expect("failed to open database");

    let mut cursor = Cursor::beginning();
    let scanner = ProfileScanner::new();
    let mut aggregator = Aggregator::default();
    let mut read = 0usize;
    let (mut decode_errors, mut broken_chunks) = (0usize, 0usize);

    loop {
        let page = db
            .run(|trx, _maybe_committed| {
                let scanner = scanner.clone();
                let cursor = cursor.clone();
                async move {
                    // This crate never sets transaction options itself: that is the
                    // caller's job, see the crate docs.
                    trx.set_option(TransactionOption::ReadSystemKeys)?;
                    // Every page is bounded by this budget, checked after every row.
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
            .expect("failed to read a page of profiling data");

        read += page.transactions.len();
        for tx in &page.transactions {
            aggregator.record(tx);
        }
        for skipped in &page.skipped {
            match skipped.reason {
                SkipReason::Decode(_) => decode_errors += 1,
                SkipReason::BrokenChunks => broken_chunks += 1,
                _ => {}
            }
        }

        cursor = page.next;
        if page.exhausted {
            break;
        }
    }

    println!("transactions read: {read}");
    println!(
        "transactions skipped: {} ({decode_errors} decode error(s), {broken_chunks} broken chunk(s))",
        decode_errors + broken_chunks
    );

    println!("\ntop {TOP_N} read keys:");
    for (key, count) in aggregator.reads().top_keys(TOP_N) {
        println!("  {count:>8}  {}", Bytes::from(key));
    }

    println!("\ntop {TOP_N} read ranges:");
    for (range, count) in aggregator.reads().top_ranges(TOP_N) {
        println!(
            "  {count:>8}  {} .. {}",
            Bytes::from(range.begin),
            Bytes::from(range.end)
        );
    }

    println!("\ntop {TOP_N} written keys:");
    for (key, count) in aggregator.writes().top_keys(TOP_N) {
        println!("  {count:>8}  {}", Bytes::from(key));
    }

    println!("\n{BUCKET_COUNT} read buckets:");
    for bucket in aggregator.reads().buckets(BUCKET_COUNT) {
        println!("  {:>8}  {}", bucket.count, Bytes::from(bucket.start));
    }
}
