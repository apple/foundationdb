use std::ptr::NonNull;

mod bindings;
mod mock;

use bindings::{
    str_from_c, FDBDatabase, FDBMetrics, FDBPromise, FDBWorkload, FDBWorkloadContext,
    FDBWorkload_VT, Metric, Metrics, OpaqueWorkload, Promise, Severity, WorkloadContext,
    FDB_WORKLOAD_API_VERSION,
};

/// Should be replaced by a Rust wrapper over the `FDBDatabase` bindings, like the one provided by foundationdb-rs
pub type MockDatabase = NonNull<FDBDatabase>;
/// FFI-safe wrapprer around a specific `RustWorkload` implementation
pub type WrappedWorkload = FDBWorkload;

/// Equivalent to the C++ abstract class `FDBWorkload`
pub trait RustWorkload: Sized {
    const VT: FDBWorkload_VT = FDBWorkload_VT {
        setup: Some(workload_setup::<Self>),
        start: Some(workload_start::<Self>),
        check: Some(workload_check::<Self>),
        getMetrics: Some(workload_get_metrics::<Self>),
        getCheckTimeout: Some(workload_get_check_timeout::<Self>),
        free: Some(workload_drop::<Self>),
    };

    /// Wrap the underlying Rust type so it can be passed to the C API
    fn wrap(self) -> WrappedWorkload {
        let inner = Box::into_raw(Box::new(self));
        WrappedWorkload {
            api_version: FDB_WORKLOAD_API_VERSION,
            inner: inner as *mut _,
            vt: &Self::VT as *const _ as *mut _,
        }
    }

    /// This method is called by the tester during the setup phase.
    /// It should be used to populate the database.
    ///
    /// # Arguments
    ///
    /// * `db` - The simulated database.
    /// * `done` - A promise that should be resolved to indicate completion
    fn setup(&mut self, db: MockDatabase, done: Promise);

    /// This method should run the actual test.
    ///
    /// # Arguments
    ///
    /// * `db` - The simulated database.
    /// * `done` - A promise that should be resolved to indicate completion
    fn start(&mut self, db: MockDatabase, done: Promise);

    /// This method is called when the tester completes.
    /// A workload should run any consistency/correctness tests during this phase.
    ///
    /// # Arguments
    ///
    /// * `db` - The simulated database.
    /// * `done` - A promise that should be resolved to indicate completion
    fn check(&mut self, db: MockDatabase, done: Promise);

    /// If a workload collects metrics (like latencies or throughput numbers), these should be reported back here.
    /// The multitester (or test orchestrator) will collect all metrics from all test clients and it will aggregate them.
    ///
    /// # Arguments
    ///
    /// * `out` - A metric sink
    fn get_metrics(&self, out: Metrics);

    /// Set the check timeout in simulated seconds for this workload.
    fn get_check_timeout(&self) -> f64;
}

/// Equivalent to the C++ abstract class `FDBWorkloadFactory`
pub trait RustWorkloadFactory {
    /// If the test file contains a key-value pair workloadName the value will be passed to this method (empty string otherwise).
    /// This way, a library author can implement many workloads in one library and use the test file to chose which one to run
    /// (or run multiple workloads either concurrently or serially).
    fn create(name: String, context: WorkloadContext) -> WrappedWorkload;
}

unsafe extern "C" fn workload_setup<W: RustWorkload>(
    raw_workload: *mut OpaqueWorkload,
    raw_database: *mut FDBDatabase,
    raw_promise: FDBPromise,
) {
    let workload = &mut *(raw_workload as *mut W);
    let database = NonNull::new_unchecked(raw_database);
    let done = Promise::new(raw_promise);
    workload.setup(database, done)
}
unsafe extern "C" fn workload_start<W: RustWorkload>(
    raw_workload: *mut OpaqueWorkload,
    raw_database: *mut FDBDatabase,
    raw_promise: FDBPromise,
) {
    let workload = &mut *(raw_workload as *mut W);
    let database = NonNull::new_unchecked(raw_database);
    let done = Promise::new(raw_promise);
    workload.start(database, done)
}
unsafe extern "C" fn workload_check<W: RustWorkload>(
    raw_workload: *mut OpaqueWorkload,
    raw_database: *mut FDBDatabase,
    raw_promise: FDBPromise,
) {
    let workload = &mut *(raw_workload as *mut W);
    let database = NonNull::new_unchecked(raw_database);
    let done = Promise::new(raw_promise);
    workload.check(database, done)
}
unsafe extern "C" fn workload_get_metrics<W: RustWorkload>(
    raw_workload: *mut OpaqueWorkload,
    raw_metrics: FDBMetrics,
) {
    let workload = &*(raw_workload as *mut W);
    let out = Metrics::new(raw_metrics);
    workload.get_metrics(out)
}
unsafe extern "C" fn workload_get_check_timeout<W: RustWorkload>(
    raw_workload: *mut OpaqueWorkload,
) -> f64 {
    let workload = &*(raw_workload as *mut W);
    workload.get_check_timeout()
}
unsafe extern "C" fn workload_drop<W: RustWorkload>(raw_workload: *mut OpaqueWorkload) {
    unsafe { drop(Box::from_raw(raw_workload as *mut W)) };
}

/// Register a `RustWorkloadFactory` in the FoundationDB simulation.
/// This macro must be invoked exactly once in a program.
#[macro_export]
macro_rules! register_factory {
    ($name:ident) => {
        #[no_mangle]
        extern "C" fn workloadCFactory(
            raw_name: *const i8,
            raw_context: $crate::FDBWorkloadContext,
        ) -> $crate::WrappedWorkload {
            let name = $crate::str_from_c(raw_name);
            let context = $crate::WorkloadContext::new(raw_context);
            <$name as $crate::RustWorkloadFactory>::create(name, context)
        }
    };
}
