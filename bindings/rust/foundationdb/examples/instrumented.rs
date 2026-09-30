//! This example demonstrates how to use the `instrumented_run` method to execute
//! a transaction and receive a detailed metrics report upon completion.
//!
//! The `instrumented_run` method is a powerful tool for monitoring and debugging
//! transaction performance, as it provides precise counters for operations, byte
//! counts for reads and writes, timing information, and support for custom metrics.

use foundationdb::{Database, FdbBindingError};

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

    // Run the instrumented example
    if let Err(e) = instrumented_example().await {
        eprintln!("Error running instrumented example: {e:?}");
    }
}

async fn instrumented_example() -> Result<(), FdbBindingError> {
    let db = Database::default()?;

    println!("Running an instrumented transaction...");

    // Use `instrumented_run` to execute a transaction and get metrics.
    let result = db
        .instrumented_run(|txn, _| async move {
            // Perform a few simple operations
            txn.set(b"instrumented_key", b"instrumented_value");
            let _ = txn.get(b"instrumented_key", false).await?;

            // Register a custom metric directly on the transaction
            txn.set_custom_metric("operations_count", 1, &[("type", "write")]);

            Ok(())
        })
        .await;

    // Handle the result and print the metrics report
    match result {
        Ok((_, metrics)) => {
            println!("Transaction successful!");
            println!("--- Metrics Report ---");
            println!("{metrics:#?}");
            println!("----------------------");
        }
        Err((err, metrics)) => {
            eprintln!("Transaction failed: {err:?}");
            eprintln!("--- Metrics Report (on failure) ---");
            println!("{metrics:#?}");
            println!("-----------------------------------");
            return Err(err);
        }
    }

    Ok(())
}

/*
// Example output:

Running an instrumented transaction...
Transaction successful!
--- Metrics Report ---
MetricsReport {
    attempts: [
        AttemptMetrics {
            index: 0,
            usage: UsageSnapshot {
                elapsed: 11.703552ms,
                bytes_read: 34,
                bytes_written: 34,
                keys_values_fetched: 1,
                call_get: 1,
                call_get_range: 0,
                call_set: 1,
                call_clear: 0,
                call_clear_range: 0,
                call_atomic_op: 0,
            },
            custom_metrics: {
                MetricKey {
                    name: "operations_count",
                    labels: [
                        (
                            "type",
                            "write",
                        ),
                    ],
                }: 1,
            },
            duration: Some(
                11.701935ms,
            ),
            commit_duration: Some(
                10.609832ms,
            ),
            grv_duration: None,
            on_error_duration: None,
            outcome: Committed,
            conflicting_keys: NotRequested,
            read_version: None,
        },
    ],
    total_duration: Some(
        11.726852ms,
    ),
    transaction: TransactionInfo {
        retries: 0,
        conflict_count: 0,
        read_version: None,
        commit_version: Some(
            70442567195,
        ),
    },
}
----------------------
*/
