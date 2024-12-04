use crate::{
    register_factory, Metric, Metrics, MockDatabase, Promise, RustWorkload, RustWorkloadFactory,
    Severity, WorkloadContext, WrappedWorkload,
};

struct MockWorkload {
    name: String,
    client_id: i32,
    context: WorkloadContext,
}

impl RustWorkload for MockWorkload {
    fn setup(&'static mut self, _db: MockDatabase, done: Promise) {
        println!("rust_setup({}_{})", self.name, self.client_id);
        self.context.trace(
            Severity::Debug,
            "Test",
            &[("Layer", "Rust"), ("Stage", "Setup")],
        );
        done.send(true);
    }
    fn start(&'static mut self, _db: MockDatabase, done: Promise) {
        println!("rust_start({}_{})", self.name, self.client_id);
        self.context.trace(
            Severity::Debug,
            "Test",
            &[("Layer", "Rust"), ("Stage", "Start")],
        );
        done.send(true);
    }
    fn check(&'static mut self, _db: MockDatabase, done: Promise) {
        println!("rust_check({}_{})", self.name, self.client_id);
        self.context.trace(
            Severity::Debug,
            "Test",
            &[("Layer", "Rust"), ("Stage", "Check")],
        );
        done.send(true);
    }
    fn get_metrics(&self, mut out: Metrics) {
        println!("rust_getMetrics({}_{})", self.name, self.client_id);
        out.reserve(8);
        out.push(Metric::val("test", 42));
    }
    fn get_check_timeout(&self) -> f64 {
        println!("rust_getCheckTimeout({}_{})", self.name, self.client_id);
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
        println!("rust_free({}_{})", self.name, self.client_id);
    }
}

struct MockFactory;
impl RustWorkloadFactory for MockFactory {
    fn create(name: String, context: WorkloadContext) -> WrappedWorkload {
        let client_id = context.client_id();
        let client_count = context.client_count();
        println!("RustWorkloadFactory::create({name})[{client_id}/{client_count}]");
        println!(
            "my_c_option: {:?}",
            context.get_option::<String>("my_rust_option")
        );
        println!(
            "my_c_option: {:?}",
            context.get_option::<String>("my_rust_option")
        );
        match name.as_str() {
            "MockWorkload" => WrappedWorkload::new(MockWorkload::new(name, client_id, context)),
            _ => panic!("Unknown workload name: {name}"),
        }
    }
}

register_factory!(MockFactory);
