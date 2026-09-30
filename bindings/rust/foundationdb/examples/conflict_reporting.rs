//! This example demonstrates how to use custom [`RunnerHooks`] to observe
//! the transaction retry loop lifecycle, including conflict reporting.
//!
//! It creates two transactions that conflict on the same key, showing
//! the full hook lifecycle: commit error → conflicting keys → retry → success.
//!
//! The user hook is stacked with [`MetricsHooks`] in a tuple, so the same run
//! also produces the per-attempt metrics report of `instrumented_run`.

use foundationdb::options::TransactionOption;
use foundationdb::runner::MetricsHooks;
use foundationdb::*;
use foundationdb_macros::cfg_api_versions;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Prints the write conflict ranges the attempt accumulated, which is what the
/// resolver will check other transactions against.
///
/// Before the commit these are an approximate superset when versionstamped keys
/// are used: the versionstamp is only resolved at commit time.
///
/// The `\xff\xff/transaction/write_conflict_range/` special keyspace exists from
/// API version 630, hence the two variants below.
#[cfg_api_versions(min = 630)]
async fn print_write_conflict_ranges(trx: &Transaction, attempt: usize) -> FdbResult<()> {
    let ranges = trx.write_conflict_ranges().await?;
    println!(
        "  before_commit (attempt {attempt}): {} write conflict range(s)",
        ranges.len()
    );
    for range in &ranges {
        println!(
            "    {:?} .. {:?}",
            String::from_utf8_lossy(range.begin()),
            String::from_utf8_lossy(range.end()),
        );
    }
    Ok(())
}

#[cfg_api_versions(min = 510, max = 620)]
async fn print_write_conflict_ranges(_trx: &Transaction, _attempt: usize) -> FdbResult<()> {
    Ok(())
}

/// A simple hook implementation that prints each lifecycle event.
struct PrintHooks;

impl RunnerHooks for PrintHooks {
    fn on_attempt_start(&self, _trx: &Transaction, attempt: usize) {
        println!("  on_attempt_start: attempt {attempt}");
    }

    /// Last point where the transaction can be inspected inside the attempt:
    /// the write conflict ranges are complete here, while `conflicting_keys`
    /// below only tells what actually clashed, and only once the commit failed.
    async fn before_commit(&self, trx: &Transaction, attempt: usize) -> FdbResult<()> {
        print_write_conflict_ranges(trx, attempt).await
    }

    async fn on_commit_error(&self, err: &TransactionCommitError, attempt: usize) -> FdbResult<()> {
        let keys = err.conflicting_keys().await?;
        println!(
            "  on_commit_error (attempt {attempt}): {} range(s)",
            keys.len()
        );
        for range in &keys {
            println!(
                "    {:?} .. {:?}",
                String::from_utf8_lossy(range.begin()),
                String::from_utf8_lossy(range.end()),
            );
        }
        Ok(())
    }

    fn on_hook_error(&self, err: &FdbError, attempt: usize) {
        println!("  on_hook_error (attempt {attempt}): {}", err.message());
    }

    fn on_closure_error(&self, err: &FdbError, attempt: usize) {
        println!("  on_closure_error (attempt {attempt}): {}", err.message());
    }

    fn on_error_duration(&self, ms: u64, attempt: usize) {
        println!("  on_error_duration (attempt {attempt}): {ms}ms");
    }

    fn on_commit_success(&self, _committed: &TransactionCommitted, ms: u64, attempt: usize) {
        println!("  on_commit_success (attempt {attempt}): committed in {ms}ms");
    }

    fn on_retry(&self, attempt: usize) {
        println!("  on_retry: attempt {attempt} is over");
    }

    fn on_complete(&self) {
        println!("  on_complete");
    }
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

async fn run_example() -> Result<(), FdbBindingError> {
    let db = Database::default()?;
    let attempt = Arc::new(AtomicU64::new(0));

    println!("Running transaction with PrintHooks (forcing a conflict)...");

    // Both hooks observe the same run: the metrics ones fill the report, the
    // printing ones comment on it. Callbacks fire left to right.
    let metrics = TransactionMetrics::new();
    let hooks = (MetricsHooks::new(&metrics), PrintHooks);

    db.run_with_hooks(&hooks, |trx, _| {
        let attempt = attempt.clone();
        async move {
            let current = attempt.fetch_add(1, Ordering::SeqCst);

            // Enable conflict reporting
            trx.set_option(TransactionOption::ReportConflictingKeys)?;

            // Read a key to establish a read conflict range
            let _ = trx.get(b"example_conflict_key", false).await?;

            if current == 0 {
                // On first attempt, have another transaction write to the same key
                let db2 = Database::default()?;
                let other = db2.create_trx()?;
                other.set(b"example_conflict_key", b"sneaky_write");
                other
                    .commit()
                    .await
                    .map_err(|e| FdbBindingError::NonRetryableFdbError(FdbError::from(e)))?;
                println!("  (injected conflicting write)");
            }

            trx.set(b"example_conflict_key", b"my_value");
            Ok::<_, FdbBindingError>(())
        }
    })
    .await?;

    let report = metrics.get_metrics_data();
    println!("Transaction succeeded after conflict!");
    println!(
        "{} attempt(s), {} conflict(s), total usage: {:?}",
        report.attempts.len(),
        report.transaction.conflict_count,
        report.total_usage(),
    );
    for attempt in &report.attempts {
        println!(
            "  attempt {}: {:?}, {} conflicting range(s)",
            attempt.index,
            attempt.outcome,
            attempt.conflicting_keys.ranges().len(),
        );
    }

    Ok(())
}

/*
// Expected output:
//
// Running transaction with PrintHooks (forcing a conflict)...
//   on_attempt_start: attempt 0
//   (injected conflicting write)
//   before_commit (attempt 0): 1 write conflict range(s)
//     "example_conflict_key" .. "example_conflict_key\0"
//   on_commit_error (attempt 0): 1 range(s)
//     "example_conflict_key" .. "example_conflict_key\0"
//   on_error_duration (attempt 0): 0ms
//   on_retry: attempt 0 is over
//   on_attempt_start: attempt 1
//   before_commit (attempt 1): 1 write conflict range(s)
//     "example_conflict_key" .. "example_conflict_key\0"
//   on_commit_success (attempt 1): committed in 1ms
//   on_complete
// Transaction succeeded after conflict!
// 2 attempt(s), 1 conflict(s), total usage: UsageSnapshot { .. }
//   attempt 0: Retried { cause: FdbError { error_code: 1020 } }, 1 conflicting range(s)
//   attempt 1: Committed, 0 conflicting range(s)
*/
