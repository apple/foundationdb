use std::time::Duration;

use foundationdb::env::Environment;
use foundationdb_simulation::{
    Metrics, RustWorkload, SimDatabase, SingleRustWorkload, WorkloadContext, register_workload,
};
use futures_util::FutureExt;

/// Tells at which simulated time a workload phase runs, reading the environment
/// clock instead of the machine one.
struct PhaseClock {
    env: Environment,
}

impl PhaseClock {
    fn new(env: Environment) -> Self {
        Self { env }
    }

    fn now(&self) -> Duration {
        self.env.clock().monotonic()
    }
}

pub struct SharedWorkload {
    client_id: i32,
    phase_clock: PhaseClock,
}

impl SingleRustWorkload for SharedWorkload {
    fn new(_name: String, context: WorkloadContext) -> Self {
        // The same struct runs in production with `Environment::default()`, in tests
        // with `Environment::with_seed(..)` and here with the simulator's
        // `context.environment()`: only the environment swaps.
        Self {
            client_id: context.client_id(),
            phase_clock: PhaseClock::new(context.environment()),
        }
    }
}

impl RustWorkload for SharedWorkload {
    async fn setup(&mut self, _db: SimDatabase) {
        println!(
            "rust_setup({}) at {:?}",
            self.client_id,
            self.phase_clock.now()
        );
    }
    async fn start(&mut self, db: SimDatabase) {
        println!(
            "rust_start({}) at {:?}",
            self.client_id,
            self.phase_clock.now()
        );
        let trx = db.create_trx().expect("Could not create transaction");
        let future = trx.get_read_version();
        let shared = future.shared();
        let version = shared.await;
        println!("read_version({}) = {version:?}", self.client_id);
    }
    async fn check(&mut self, _db: SimDatabase) {
        println!(
            "rust_check({}) at {:?}",
            self.client_id,
            self.phase_clock.now()
        );
    }
    fn get_metrics(&self, mut _out: Metrics) {
        println!("rust_get_metrics({})", self.client_id);
    }
    fn get_check_timeout(&self) -> f64 {
        println!("rust_get_check_timeout({})", self.client_id);
        5000.0
    }
}

register_workload!(SharedWorkload);
