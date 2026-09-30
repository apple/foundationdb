//! Scanning a large range across several transactions, each one bounded by a
//! [`ClientBudget`].
//!
//! FoundationDB transactions may not live longer than five seconds, so a scan
//! that does not fit in one transaction has to be cut into pages, each page
//! resuming where the previous one stopped. This example shows the pattern:
//!
//! * the closure sets a budget (2.5s, comfortably below the five second limit),
//! * it reads batch after batch and checks the budget between batches,
//! * when the budget is exhausted it *stops* instead of failing: the closure
//!   returns the progress it made, and the transaction commits normally,
//! * `main` reopens a transaction from the continuation key and keeps going
//!   until the scan reports it reached the end of the range.
//!
//! There is deliberately no helper API for this: the budget is a client-side
//! estimate you check where it makes sense for your workload, and what to do
//! when it runs out (stop, commit partial work, checkpoint elsewhere) is an
//! application decision.

use std::time::{Duration, Instant};

use foundationdb::options::StreamingMode;
use foundationdb::*;
use futures_util::TryStreamExt;

/// Keys written by the setup, `PREFIX` .. `END`.
const PREFIX: &[u8] = b"budgeted_scan/";
const END: &[u8] = b"budgeted_scan0";

/// Number of rows and value size of the data set. 4000 x 1 KiB is around 4 MiB,
/// so a scan with a 1 MiB target per batch takes a handful of batches.
const ROWS: usize = 4000;
const VALUE_SIZE: usize = 1024;
/// Rows written per setup transaction, to stay well below the 10 MiB
/// transaction size limit.
const ROWS_PER_SETUP_TRX: usize = 500;

fn key_of(index: usize) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.extend_from_slice(format!("{index:08}").as_bytes());
    key
}

/// What one budgeted transaction managed to do.
struct Page {
    /// Rows read by this transaction.
    rows: usize,
    /// Last key processed, or `None` if the page read nothing. The next
    /// transaction resumes *after* it, with `first_greater_than`.
    last_key: Option<Vec<u8>>,
    /// `true` when the range was fully consumed, that is when the batch stream
    /// ended on its own rather than the budget cutting it short.
    complete: bool,
}

/// Scans from `after` (exclusive, `None` to start at the beginning of the
/// range) for as long as `budget` allows.
async fn scan_page(
    db: &Database,
    after: Option<Vec<u8>>,
    budget: ClientBudget,
) -> Result<Page, FdbBindingError> {
    db.run(|trx, _maybe_committed| {
        let after = after.clone();
        let budget = budget.clone();
        async move {
            // Set the budget first: it starts a fresh accounting generation, and
            // on a retry the new attempt gets the full allowance again.
            trx.set_client_budget(budget);

            let begin = match &after {
                // Exclusive continuation: resume strictly after the last key we
                // processed, so no row is read twice and none is skipped.
                Some(last) => KeySelector::first_greater_than(last.as_slice()),
                None => KeySelector::first_greater_or_equal(PREFIX),
            };
            let opt = RangeOption {
                begin,
                end: KeySelector::first_greater_or_equal(END),
                // Serial asks the cluster for the largest batches it will send
                // (around 80 KiB), which is what we want when the loop, not the
                // batch size, is deciding when to stop. `target_bytes` is only
                // an upper cap: it never grows a batch beyond what the mode
                // asks for.
                mode: StreamingMode::Serial,
                target_bytes: 1 << 20,
                ..RangeOption::default()
            };

            let mut rows = 0;
            let mut last_key = None;
            // The stream ending is the only proof the range is exhausted.
            let mut complete = true;
            let mut batches = trx.get_ranges(opt, false);

            while let Some(batch) = batches.try_next().await? {
                for kv in batch.iter() {
                    rows += 1;
                    last_key = Some(kv.key().to_vec());
                }

                // The budget is checked *between* batches, never inside one: the
                // batch that was in flight when the budget expired still lands and
                // is fully processed. Expect an overshoot of up to one batch, and
                // in time, of however long that batch took to arrive: nothing here
                // interrupts an await. Size the budget with that margin in mind
                // (2.5s against a 5s transaction limit).
                if let Err(exceeded) = trx.check_client_budget() {
                    // Matched, not propagated with `?`. Returning the error would
                    // abort the whole `db.run` and throw away both the work of
                    // this page and the continuation key. Breaking instead lets
                    // the closure return normally: the transaction commits and the
                    // caller resumes from `last_key`.
                    println!("    budget reached: {exceeded}");
                    complete = false;
                    break;
                }
            }

            Ok::<_, FdbBindingError>(Page {
                rows,
                last_key,
                complete,
            })
        }
    })
    .await
}

/// Runs the full scan, one transaction per page, and returns how many rows and
/// how many transactions it took.
async fn scan_all(db: &Database, budget: ClientBudget) -> Result<(usize, usize), FdbBindingError> {
    let mut continuation = None;
    let mut total_rows = 0;
    let mut transactions = 0;

    loop {
        let page = scan_page(db, continuation.clone(), budget.clone()).await?;
        transactions += 1;
        total_rows += page.rows;
        println!(
            "  transaction {}: {} row(s), {} rows so far",
            transactions, page.rows, total_rows
        );

        if page.complete {
            break;
        }
        // A page cut short by the budget always has a continuation key, unless
        // it read nothing at all, which means there was nothing left to read.
        match page.last_key {
            Some(last) => continuation = Some(last),
            None => break,
        }
    }

    Ok((total_rows, transactions))
}

async fn setup(db: &Database) -> Result<(), FdbBindingError> {
    let value = vec![b'x'; VALUE_SIZE];

    db.run(|trx, _| async move {
        trx.clear_range(PREFIX, END);
        Ok::<_, FdbBindingError>(())
    })
    .await?;

    for chunk_start in (0..ROWS).step_by(ROWS_PER_SETUP_TRX) {
        let value = value.clone();
        db.run(move |trx, _| {
            let value = value.clone();
            async move {
                for index in chunk_start..(chunk_start + ROWS_PER_SETUP_TRX).min(ROWS) {
                    trx.set(&key_of(index), &value);
                }
                Ok::<_, FdbBindingError>(())
            }
        })
        .await?;
    }

    Ok(())
}

async fn run_example() -> Result<(), FdbBindingError> {
    let db = Database::default()?;

    println!("Writing {ROWS} rows of {VALUE_SIZE} bytes...");
    setup(&db).await?;

    println!("Scanning with a 2.5s per-transaction budget:");
    let started = Instant::now();
    let (rows, transactions) = scan_all(
        &db,
        ClientBudget {
            time_limit: Some(Duration::from_millis(2500)),
            ..ClientBudget::default()
        },
    )
    .await?;
    println!(
        "  {rows} row(s) in {transactions} transaction(s), {:?}",
        started.elapsed()
    );

    // On a local cluster 4 MiB is read well within 2.5s, so the scan above ends
    // in a single transaction. The same loop with a byte budget of half a
    // mebibyte shows the continuation actually resuming, a few batches per
    // transaction.
    println!("Scanning again with a 512 KiB read budget, to force continuations:");
    let (rows, transactions) = scan_all(
        &db,
        ClientBudget {
            max_bytes_read: Some(512 * 1024),
            ..ClientBudget::default()
        },
    )
    .await?;
    println!("  {rows} row(s) in {transactions} transaction(s)");

    db.run(|trx, _| async move {
        trx.clear_range(PREFIX, END);
        Ok::<_, FdbBindingError>(())
    })
    .await?;

    Ok(())
}

#[tokio::main]
async fn main() {
    foundationdb::boot().expect("failed to initialize FoundationDB");
    // The network is stopped and joined automatically at process exit, which is
    // fine for tests and short-lived tools like this example. In a production
    // application, prefer a clean teardown: the network thread is the event loop
    // driving every transaction and you may still have on-going operations at
    // exit time. Finish or cancel your work, drop the Database handles, then
    // call `foundationdb::api::stop_network()` yourself (terminal: any
    // FoundationDB use afterwards fails with error 2025).

    if let Err(e) = run_example().await {
        eprintln!("Error: {e:?}");
    }
}

/*
// Expected output:
//
// Writing 4000 rows of 1024 bytes...
// Scanning with a 2.5s per-transaction budget:
//   transaction 1: 4000 row(s), 4000 rows so far
//   4000 row(s) in 1 transaction(s), 12.147459ms
// Scanning again with a 512 KiB read budget, to force continuations:
//     budget reached: client budget exceeded (bytes read): used 549150 bytes, limit 524288 bytes. ...
//   transaction 1: 525 row(s), 525 rows so far
//   ...
//   transaction 8: 325 row(s), 4000 rows so far
//   4000 row(s) in 8 transaction(s)
*/
