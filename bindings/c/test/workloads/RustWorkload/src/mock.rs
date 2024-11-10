use crate::{
    register_factory, wrap, FDBWorkload, Metric, Metrics, MockDatabase, Promise, RustWorkload,
    RustWorkloadFactory, Severity, WorkloadContext,
};

struct MockWorkload {
    name: String,
    client_id: i32,
    context: WorkloadContext,
}

impl RustWorkload for MockWorkload {
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
    fn get_metrics(&self, mut out: Metrics) {
        out.reserve(8);
        out.push(Metric::val("test", 42));
    }
    fn get_check_timeout(&self) -> f64 {
        3000.
    }
}
impl MockWorkload {
    fn new(name: String, client_id: i32, context: WorkloadContext) -> Self {
        Self {
            name,
            client_id,
            context,
        }
    }
}
impl Drop for MockWorkload {
    fn drop(&mut self) {
        println!("workload_free({}_{})", self.name, self.client_id);
    }
}

struct MockFactory;
impl RustWorkloadFactory for MockFactory {
    fn create(name: String, context: WorkloadContext) -> FDBWorkload {
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
            "MockWorkload" => wrap(MockWorkload::new(name, client_id, context)),
            _ => panic!("Unknown workload name: {name}"),
        }
    }
}

register_factory!(MockFactory);
