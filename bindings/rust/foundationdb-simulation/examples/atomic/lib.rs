use std::time::Duration;

use foundationdb::{
    BudgetExceeded, BudgetKind, ClientBudget, FdbBindingError, FdbResult, Transaction,
    env::Environment,
    options::{MutationType, TransactionOption},
    tuple::Subspace,
};
use foundationdb_simulation::{
    Metric, Metrics, RustWorkload, Severity, SimDatabase, SingleRustWorkload, WorkloadContext,
    details, register_workload,
};

pub struct AtomicWorkload {
    context: WorkloadContext,
    client_id: i32,
    counter: AtomicCounter,
    // how many transactions will be run
    expected_count: usize,
    // how many transactions succeeded
    success_count: usize,
    // how many transactions failed
    error_count: usize,
    // how many maybe_committed transactions we encountered
    maybe_committed_count: usize,
    // how much simulated time start() took
    elapsed: Duration,
    // how many rows the budgeted scan read before it stopped
    scan_iterations: usize,
    // the limit that scan tripped on, None if it never did
    scan_outcome: Option<BudgetExceeded>,
}

const COUNT_KEY: &[u8] = b"count";

// How many rows setup() writes, and the modulus of the budgeted scan.
const ROW_COUNT: usize = 500;
// What one scan attempt may spend, measured with the simulator clock. A simulated
// read costs enough that this trips after a few dozen of them, well before the
// five seconds FoundationDB gives a transaction.
const TIME_LIMIT: Duration = Duration::from_secs(1);
// Ends the scan even if the budget never trips, so the workload terminates.
const MAX_ITERATIONS: usize = 100_000;

/// The counter itself, reading time from the environment it was given instead of
/// from the machine. It never sets transaction options: that belongs to the
/// caller.
struct AtomicCounter {
    env: Environment,
    subspace: Subspace,
}

impl AtomicCounter {
    fn new(env: Environment, subspace: Subspace) -> Self {
        Self { env, subspace }
    }

    /// The current reading of the environment clock, simulated time here.
    fn now(&self) -> Duration {
        self.env.clock().monotonic()
    }

    fn key(&self) -> Vec<u8> {
        self.subspace.pack(&COUNT_KEY)
    }

    fn increment(&self, trx: &Transaction) {
        let buf: [u8; 8] = 1i64.to_le_bytes();
        trx.atomic_op(&self.key(), &buf, MutationType::Add);
    }

    fn decode_count(&self, bytes: &[u8]) -> usize {
        i64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize
    }

    /// The key of one of the rows the budgeted scan walks, kept apart from the
    /// counter key.
    fn row_key(&self, index: usize) -> Vec<u8> {
        self.subspace.pack(&("rows", index as i64))
    }

    /// Reads one row, returning how many bytes came back.
    ///
    /// The await is where simulated time passes: the simulator decides the
    /// latency of the storage round trip, and that is what moves the clock a time
    /// budget is measured against.
    async fn read_row(&self, trx: &Transaction, index: usize) -> FdbResult<usize> {
        let value = trx.get(&self.row_key(index), false).await?;
        Ok(value.map_or(0, |bytes| bytes.len()))
    }
}

impl SingleRustWorkload for AtomicWorkload {
    fn new(_name: String, context: WorkloadContext) -> Self {
        // The same struct runs in production with `Environment::default()`, in tests
        // with `Environment::with_seed(..)` and here with the simulator's
        // `context.environment()`: only the environment swaps.
        let counter = AtomicCounter::new(context.environment(), Subspace::all());
        Self {
            client_id: context.client_id(),
            expected_count: context.get_option("count").expect("Could not get count"),
            context,
            counter,
            success_count: 0,
            error_count: 0,
            maybe_committed_count: 0,
            elapsed: Duration::ZERO,
            scan_iterations: 0,
            scan_outcome: None,
        }
    }
}

impl RustWorkload for AtomicWorkload {
    async fn setup(&mut self, db: SimDatabase) {
        println!("rust_setup({})", self.client_id);
        // Only use a single client
        if self.client_id == 0 {
            // The rows the budgeted scan of start() walks.
            let counter = &self.counter;
            db.run(|trx, _maybe_committed| async move {
                for index in 0..ROW_COUNT {
                    trx.set(&counter.row_key(index), &(index as i64).to_le_bytes());
                }
                Ok::<_, FdbBindingError>(())
            })
            .await
            .expect("could not write the rows to scan");
        }
    }
    async fn start(&mut self, db: SimDatabase) {
        println!("rust_start({})", self.client_id);
        let started_at = self.counter.now();
        // Only use a single client
        if self.client_id == 0 {
            for _ in 0..self.expected_count {
                let trx = db.create_trx().expect("Could not create transaction");

                // Enable idempotent txn
                trx.set_option(TransactionOption::AutomaticIdempotency)
                    .expect("could not setup automatic idempotency");

                self.counter.increment(&trx);

                match trx.commit().await {
                    Ok(_) => self.success_count += 1,
                    Err(err) => {
                        if err.is_maybe_committed() {
                            self.context.trace(
                                Severity::Warn,
                                "Detected an maybe_committed transactions with idempotency",
                                details![
                                    "Layer" => "Rust",
                                    "Client" => self.client_id
                                ],
                            );
                            self.maybe_committed_count += 1;
                        } else {
                            self.error_count += 1;
                        }
                    }
                }

                self.context.trace(
                    Severity::Info,
                    "Successfully setup workload",
                    details![
                        "Layer" => "Rust",
                        "Client" => self.client_id
                    ],
                );
            }

            // The budgeted scan: one long loop of reads that the client budget is
            // meant to end. db.run wraps it because buggify injects retryable read
            // errors, and the budget is per attempt: a retry restarts the scan with
            // a fresh allowance, which is exactly the semantics being demonstrated.
            let counter = &self.counter;
            let (iterations, outcome) = db
                .run(|trx, _maybe_committed| async move {
                    // Measuring the attempt with the simulator clock is what makes
                    // the moment the budget trips part of the deterministic state
                    // of the run.
                    trx.set_client_budget(ClientBudget {
                        time_limit: Some(TIME_LIMIT),
                        clock: Some(counter.env.clock().clone()),
                        ..ClientBudget::default()
                    });

                    let mut iterations = 0;
                    let mut outcome = None;
                    for index in 0..MAX_ITERATIONS {
                        // Distinct keys matter: a key already read by this attempt
                        // comes back from the read-your-writes cache without a
                        // simulated round trip, so the clock would never move.
                        counter
                            .read_row(&trx, index % ROW_COUNT)
                            .await
                            .map_err(FdbBindingError::from)?;
                        iterations += 1;

                        // Exceeding the budget is the expected outcome, data rather
                        // than a transactional error, so it leaves the closure as an
                        // Ok and db.run does not retry it.
                        if let Err(exceeded) = trx.check_client_budget() {
                            outcome = Some(exceeded);
                            break;
                        }
                    }

                    Ok::<_, FdbBindingError>((iterations, outcome))
                })
                .await
                .expect("could not run the budgeted scan");
            self.scan_iterations = iterations;
            self.scan_outcome = outcome;
        }
        // Simulated time, so this is deterministic across runs of the same seed.
        self.elapsed = self.counter.now() - started_at;
    }
    async fn check(&mut self, db: SimDatabase) {
        println!("rust_check({})", self.client_id);
        if self.client_id == 0 {
            // even if buggify is off in checks, transactions can failed because of the randomized knob,
            // so we need to wrap the check in a db.run
            let counter = &self.counter;
            let count = db
                .run(|trx, _maybe_committed| async move {
                    match trx.get(&counter.key(), true).await {
                        Err(e) => Err(FdbBindingError::from(e)),
                        Ok(None) => Ok(0),
                        Ok(Some(byte_count)) => Ok(counter.decode_count(&byte_count)),
                    }
                })
                .await
                .expect("could not check using db.run");

            if self.success_count == count {
                self.context.trace(
                    Severity::Info,
                    "Atomic count match",
                    details![
                        "Layer" => "Rust",
                        "Client" => self.client_id,
                        "Expected" => self.expected_count,
                        "Found" => count,
                        "CommittedCount" => self.success_count,
                        "MaybeCommitted" => self.maybe_committed_count,
                    ],
                );
            } else {
                self.context.trace(
                    Severity::Error,
                    "Atomic count doesn't match",
                    details![
                        "Layer" => "Rust",
                        "Client" => self.client_id,
                        "Expected" => self.expected_count,
                        "Found" => count,
                        "CommittedCount" => self.success_count,
                        "MaybeCommitted" => self.maybe_committed_count,
                    ],
                );
            }

            // A Severity::Error trace fails the simulation, so every way the budget
            // could have failed to do its job is reported that way.
            let (severity, message) = match self.scan_outcome {
                Some(BudgetExceeded { kind, used, limit })
                    if kind == BudgetKind::Time && used >= limit =>
                {
                    (Severity::Info, "Client budget tripped on time as expected")
                }
                Some(_) => (Severity::Error, "Client budget tripped on the wrong limit"),
                None => (Severity::Error, "Client budget never tripped"),
            };

            self.context.trace(
                severity,
                message,
                details![
                    "Layer" => "Rust",
                    "Client" => self.client_id,
                    "Iterations" => self.scan_iterations,
                    "Kind" => self.scan_outcome.map_or("none".to_string(), |e| e.kind.to_string()),
                    "Used" => self.scan_outcome.map_or(0, |e| e.used),
                    "Limit" => self.scan_outcome.map_or(0, |e| e.limit),
                ],
            );
        }
    }
    fn get_metrics(&self, mut out: Metrics) {
        println!("rust_get_metrics({})", self.client_id);
        out.extend([
            Metric::val("expected_count", self.expected_count as f64),
            Metric::val("success_count", self.success_count as f64),
            Metric::val("error_count", self.error_count as f64),
            Metric::val("elapsed_simulated_seconds", self.elapsed.as_secs_f64()),
            Metric::val("iterations", self.scan_iterations as f64),
            Metric::val("used_ms", self.scan_outcome.map_or(0, |e| e.used) as f64),
        ]);
    }
    fn get_check_timeout(&self) -> f64 {
        println!("rust_get_check_timeout({})", self.client_id);
        5000.0
    }
}

register_workload!(AtomicWorkload);
