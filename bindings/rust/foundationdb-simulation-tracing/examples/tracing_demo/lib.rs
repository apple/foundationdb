use std::sync::atomic::{AtomicBool, Ordering};

use foundationdb::{FdbBindingError, FdbError, tuple::Subspace};
use foundationdb_simulation::{
    Metric, Metrics, RustWorkload, SimDatabase, SingleRustWorkload, WorkloadContext,
    register_workload,
};
use foundationdb_simulation_tracing::{TracingGuard, install};
use tracing::Instrument;

/// Demo workload that emits `tracing` events across its phases and forces one
/// transaction conflict, so both the workload's own events and the SDK's
/// retry-loop events land in the simulator trace log as `RustTracingEvent`.
pub struct TracingDemoWorkload {
    // Keep the tracing binding alive for as long as this workload exists.
    _tracing: TracingGuard,
    context: WorkloadContext,
    client_id: i32,
    committed: bool,
}

const DEMO_KEY: &[u8] = b"tracing_demo_key";

impl SingleRustWorkload for TracingDemoWorkload {
    fn new(_name: String, context: WorkloadContext) -> Self {
        let _tracing = install(&context);
        Self {
            client_id: context.client_id(),
            _tracing,
            context,
            committed: false,
        }
    }
}

impl RustWorkload for TracingDemoWorkload {
    async fn setup(&mut self, _db: SimDatabase) {
        tracing::info!(client = self.client_id, phase = "setup", "workload setup");
        tracing::debug!(
            client = self.client_id,
            "debug-level event for severity filtering verification"
        );
    }

    async fn start(&mut self, db: SimDatabase) {
        tracing::info!(client = self.client_id, phase = "start", "workload start");
        // Only a single client drives the transactions.
        if self.client_id != 0 {
            return;
        }

        let client_id = self.client_id;
        let key = Subspace::all().pack(&DEMO_KEY);
        // Interferes exactly once, on the first attempt, to force a single
        // `not_committed` conflict so the SDK's retry events fire deterministically.
        let interfere = AtomicBool::new(true);

        // Wrap the retry loop in a span so forwarded events (including the SDK's
        // own retry diagnostics) carry the Span/SpanPath and the `client` field.
        // Attached with `.instrument` rather than `span.enter()` because the
        // future is awaited.
        let span = tracing::info_span!("demo_transaction", client = client_id);
        let result = db
            .run(|trx, _maybe_committed| {
                let key = key.clone();
                let db = &db;
                let interfere = &interfere;
                async move {
                    // Establish a read conflict range on the key.
                    let _ = trx.get(&key, false).await.map_err(FdbBindingError::from)?;

                    if interfere.swap(false, Ordering::SeqCst) {
                        // A competing transaction commits a write to the same key,
                        // invalidating this transaction's read on commit.
                        let other = db.create_trx().map_err(FdbBindingError::from)?;
                        other.set(&key, b"competing");
                        other
                            .commit()
                            .await
                            .map_err(FdbError::from)
                            .map_err(FdbBindingError::from)?;
                        tracing::warn!(
                            client = client_id,
                            key = "tracing_demo_key",
                            "injected a competing write to force a conflict"
                        );
                    }

                    trx.set(&key, b"value");
                    Ok(())
                }
            })
            .instrument(span)
            .await;

        match result {
            Ok(()) => {
                self.committed = true;
                tracing::info!(client = self.client_id, "transaction committed after retry");
            }
            Err(FdbBindingError::NonRetryableFdbError(error)) => {
                tracing::error!(
                    client = self.client_id,
                    error_code = error.code(),
                    "transaction failed"
                );
            }
            Err(_) => {
                tracing::error!(client = self.client_id, "transaction failed");
            }
        }
    }

    async fn check(&mut self, _db: SimDatabase) {
        if self.client_id == 0 && !self.committed {
            // Report the failure through the workload's own context so the run
            // is flagged; the tracing bridge never emits Severity::Error.
            self.context.trace(
                foundationdb_simulation::Severity::Error,
                "TracingDemoCommitFailed",
                &[("Client", self.client_id.to_string())],
            );
        }
        tracing::info!(client = self.client_id, phase = "check", "workload check");
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([Metric::val("committed", self.committed as i32 as f64)]);
    }

    fn get_check_timeout(&self) -> f64 {
        5000.0
    }
}

register_workload!(TracingDemoWorkload);
