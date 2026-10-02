use foundationdb::{
    FdbBindingError,
    options::{MutationType, TransactionOption},
};
use foundationdb_simulation::{
    Metric, Metrics, RustWorkload, Severity, SimDatabase, SingleRustWorkload, WorkloadContext,
    details, register_workload,
};

const COUNT_KEY: &[u8] = b"rust/atomic/count";

pub struct AtomicWorkload {
    context: WorkloadContext,
    client_id: i32,
    expected_count: usize,
    success_count: usize,
    error_count: usize,
    maybe_committed_count: usize,
    elapsed: f64,
}

impl SingleRustWorkload for AtomicWorkload {
    fn new(_name: String, context: WorkloadContext) -> Self {
        Self {
            client_id: context.client_id(),
            expected_count: context.get_option("count").expect("Could not get count"),
            context,
            success_count: 0,
            error_count: 0,
            maybe_committed_count: 0,
            elapsed: 0.0,
        }
    }
}

impl RustWorkload for AtomicWorkload {
    async fn setup(&mut self, db: SimDatabase) {
        if self.client_id == 0 {
            db.run(|trx, _| async move {
                trx.clear(COUNT_KEY);
                Ok::<_, FdbBindingError>(())
            })
            .await
            .expect("could not clear the counter");
        }
    }

    async fn start(&mut self, db: SimDatabase) {
        let started_at = self.context.now();
        if self.client_id == 0 {
            for _ in 0..self.expected_count {
                let trx = db.create_trx().expect("Could not create transaction");
                trx.set_option(TransactionOption::AutomaticIdempotency)
                    .expect("could not set automatic idempotency");
                trx.atomic_op(COUNT_KEY, &1_i64.to_le_bytes(), MutationType::Add);
                match trx.commit().await {
                    Ok(_) => self.success_count += 1,
                    Err(error) if error.is_maybe_committed() => self.maybe_committed_count += 1,
                    Err(_) => self.error_count += 1,
                }
            }
        }
        self.elapsed = self.context.now() - started_at;
    }

    async fn check(&mut self, db: SimDatabase) {
        if self.client_id == 0 {
            let count = db
                .run(|trx, _| async move {
                    let bytes = trx.get(COUNT_KEY, true).await?;
                    Ok::<_, FdbBindingError>(bytes.map_or(0, |value| {
                        i64::from_le_bytes(value[..8].try_into().unwrap()) as usize
                    }))
                })
                .await
                .expect("could not read the counter");
            let severity = if self.success_count == count {
                Severity::Info
            } else {
                Severity::Error
            };
            self.context.trace(
                severity,
                "AtomicCountCheck",
                details![
                    "Client" => self.client_id,
                    "Expected" => self.success_count,
                    "Found" => count,
                    "MaybeCommitted" => self.maybe_committed_count
                ],
            );
        }
    }

    fn get_metrics(&self, mut out: Metrics<'_>) {
        out.extend([
            Metric::val("expected_count", self.expected_count as f64),
            Metric::val("success_count", self.success_count as f64),
            Metric::val("error_count", self.error_count as f64),
            Metric::val("maybe_committed_count", self.maybe_committed_count as f64),
            Metric::val("elapsed_simulated_seconds", self.elapsed),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        5000.0
    }
}

register_workload!(AtomicWorkload);
