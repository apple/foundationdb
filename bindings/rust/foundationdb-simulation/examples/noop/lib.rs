use std::time::Duration;

use foundationdb::env::Environment;
use foundationdb_simulation::{
    Metric, Metrics, RustWorkload, RustWorkloadFactory, Severity, SimDatabase, WorkloadContext,
    WrappedWorkload, register_factory,
};

/// Measures elapsed time through the environment it was given, never through the
/// machine clock.
struct SimulatedStopwatch {
    env: Environment,
}

impl SimulatedStopwatch {
    fn new(env: Environment) -> Self {
        Self { env }
    }

    /// A reading to compare with a later one, simulated time here.
    fn mark(&self) -> Duration {
        self.env.clock().monotonic()
    }
}

struct NoopWorkload {
    name: String,
    client_id: i32,
    context: WorkloadContext,
    stopwatch: SimulatedStopwatch,
}

impl RustWorkload for NoopWorkload {
    async fn setup(&mut self, _db: SimDatabase) {
        println!("rust_setup({}_{})", self.name, self.client_id);
        self.context.trace(
            Severity::Debug,
            "Test",
            &[("Layer", "Rust"), ("Stage", "Setup")],
        );
    }
    async fn start(&mut self, _db: SimDatabase) {
        println!("rust_start({}_{})", self.name, self.client_id);
        self.context.trace(
            Severity::Debug,
            "Test",
            &[("Layer", "Rust"), ("Stage", "Start")],
        );
        // Exercise WorkloadContext::delay (requires fdbserver 7.4.6+, the C API path).
        let before = self.stopwatch.mark();
        self.context
            .delay(Duration::from_secs(1))
            .await
            .expect("delay future should resolve");
        let after = self.stopwatch.mark();
        // The difference is simulated time, not machine time: the simulator decides
        // when the clock advances, so these readings are deterministic.
        println!(
            "clock({}_{}): before={before:?} after={after:?} difference={:?}",
            self.name,
            self.client_id,
            after - before
        );
    }
    async fn check(&mut self, _db: SimDatabase) {
        println!("rust_check({}_{})", self.name, self.client_id);
        self.context.trace(
            Severity::Debug,
            "Test",
            &[("Layer", "Rust"), ("Stage", "Check")],
        );
    }
    fn get_metrics(&self, mut out: Metrics) {
        println!("rust_get_metrics({}_{})", self.name, self.client_id);
        out.reserve(8);
        out.push(Metric::val("test", 42));
    }
    fn get_check_timeout(&self) -> f64 {
        println!("rust_get_check_timeout({}_{})", self.name, self.client_id);
        3000.
    }
}
impl NoopWorkload {
    fn new(name: String, client_id: i32, context: WorkloadContext) -> Self {
        // The same struct runs in production with `Environment::default()`, in tests
        // with `Environment::with_seed(..)` and here with the simulator's
        // `context.environment()`: only the environment swaps.
        let stopwatch = SimulatedStopwatch::new(context.environment());
        Self {
            name,
            client_id,
            context,
            stopwatch,
        }
    }
}
impl Drop for NoopWorkload {
    fn drop(&mut self) {
        println!("rust_free({}_{})", self.name, self.client_id);
    }
}

struct NoopFactory;
impl RustWorkloadFactory for NoopFactory {
    fn create(name: String, context: WorkloadContext) -> WrappedWorkload {
        let client_id = context.client_id();
        let client_count = context.client_count();
        println!("RustWorkloadFactory::create({name})[{client_id}/{client_count}]");
        println!(
            "my_c_option: {:?}",
            context.get_option::<String>("my_c_option")
        );
        println!(
            "my_c_option: {:?}",
            context.get_option::<String>("my_c_option")
        );
        match name.as_str() {
            "NoopWorkload" => NoopWorkload::new(name, client_id, context).wrap(),
            _ => panic!("Unknown workload name: {name}"),
        }
    }
}

register_factory!(NoopFactory);
