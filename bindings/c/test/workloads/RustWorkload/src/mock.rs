use crate::{
    register_workload, Metric, MockDatabase, Promise, RustWorkload, Severity, WorkloadContext,
};

struct MockWorkload {
    name: String,
    client_id: i32,
    context: WorkloadContext,
}

impl RustWorkload for MockWorkload {
    fn new(name: String, context: WorkloadContext) -> Self {
        let client_id = context.client_id();
        Self {
            name,
            client_id,
            context,
        }
    }
    fn setup(&'static mut self, _db: MockDatabase, done: Promise) {
        println!("workload_setup({}_{})", self.name, self.client_id);
        self.context.trace(
            Severity::Debug,
            "Test",
            &[("Layer", "Rust"), ("Stage", "Setup")],
        );
        done.send(true);
    }
    fn start(&'static mut self, _db: MockDatabase, done: Promise) {
        println!("workload_start({}_{})", self.name, self.client_id);
        self.context.trace(
            Severity::Debug,
            "Test",
            &[("Layer", "Rust"), ("Stage", "Start")],
        );
        done.send(true);
    }
    fn check(&'static mut self, _db: MockDatabase, done: Promise) {
        println!("workload_check({}_{})", self.name, self.client_id);
        self.context.trace(
            Severity::Debug,
            "Test",
            &[("Layer", "Rust"), ("Stage", "Check")],
        );
        done.send(true);
    }
    fn get_metrics(&self) -> Vec<Metric> {
        Vec::new()
    }
    fn get_check_timeout(&self) -> f64 {
        3000.
    }
}

register_workload!(MockWorkload);
