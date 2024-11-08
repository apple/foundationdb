use std::{os::raw::c_char, ptr::NonNull};

mod bindings;
mod mock;

use bindings::{
    str_from_c, BridgeToClient, BridgeToServer, BridgeToServer_PromiseImpl, CVector, FDBDatabase,
    FDBPromise, FDBWorkload, FDBWorkloadContext, Metric, Promise, Severity, WorkloadContext,
};

/// RustWorkload trait provides a one to one equivalent to the C++ abstract class `FDBWorkload`
pub trait RustWorkload {
    fn new(name: String, context: WorkloadContext) -> Self;

    /// This method is called by the tester during the setup phase.
    /// It should be used to populate the database.
    ///
    /// # Arguments
    ///
    /// * `db` - The simulated database.
    /// * `done` - A promise that should be resolved to indicate completion
    fn setup(&'static mut self, db: MockDatabase, done: Promise);

    /// This method should run the actual test.
    ///
    /// # Arguments
    ///
    /// * `db` - The simulated database.
    /// * `done` - A promise that should be resolved to indicate completion
    fn start(&'static mut self, db: MockDatabase, done: Promise);

    /// This method is called when the tester completes.
    /// A workload should run any consistency/correctness tests during this phase.
    ///
    /// # Arguments
    ///
    /// * `db` - The simulated database.
    /// * `done` - A promise that should be resolved to indicate completion
    fn check(&'static mut self, db: MockDatabase, done: Promise);

    /// If a workload collects metrics (like latencies or throughput numbers), these should be reported back here.
    /// The multitester (or test orchestrator) will collect all metrics from all test clients and it will aggregate them.
    fn get_metrics(&self) -> Vec<Metric>;

    /// Set the check timeout for this workload.
    fn get_check_timeout(&self) -> f64;
}

// Should be replaced by a Rust wrapper over the FDBDatabase bindings, like the one provided by
// foundationdb-rs
type MockDatabase = NonNull<FDBDatabase>;

struct Bridge<W: RustWorkload> {
    workload: W,
    promise: BridgeToServer_PromiseImpl,
}

unsafe extern "C" fn workload_setup<W: RustWorkload + 'static>(
    raw_bridge: *mut FDBWorkload,
    raw_database: *mut FDBDatabase,
    raw_promise: *mut FDBPromise,
) {
    let bridge = &mut *(raw_bridge as *mut Bridge<W>);
    let database = NonNull::new_unchecked(raw_database);
    let done = Promise::new(raw_promise, bridge.promise);
    bridge.workload.setup(database, done)
}
unsafe extern "C" fn workload_start<W: RustWorkload + 'static>(
    raw_bridge: *mut FDBWorkload,
    raw_database: *mut FDBDatabase,
    raw_promise: *mut FDBPromise,
) {
    let bridge = &mut *(raw_bridge as *mut Bridge<W>);
    let database = NonNull::new_unchecked(raw_database);
    let done = Promise::new(raw_promise, bridge.promise);
    bridge.workload.start(database, done)
}
unsafe extern "C" fn workload_check<W: RustWorkload + 'static>(
    raw_bridge: *mut FDBWorkload,
    raw_database: *mut FDBDatabase,
    raw_promise: *mut FDBPromise,
) {
    let bridge: &mut Bridge<W> = &mut *(raw_bridge as *mut Bridge<W>);
    let database = NonNull::new_unchecked(raw_database);
    let done = Promise::new(raw_promise, bridge.promise);
    bridge.workload.check(database, done)
}
unsafe extern "C" fn workload_get_metrics<W: RustWorkload>(
    raw_bridge: *mut FDBWorkload,
) -> CVector {
    let bridge = &*(raw_bridge as *mut Bridge<W>);
    let mut metrics = bridge.workload.get_metrics();
    // TODO: Metric to CMetric
    // FIXME: dangling pointers
    CVector {
        elements: metrics.as_mut_ptr() as *mut _,
        n: metrics.len() as i32,
    }
}
unsafe extern "C" fn workload_get_check_timeout<W: RustWorkload>(
    raw_bridge: *mut FDBWorkload,
) -> f64 {
    let bridge = &*(raw_bridge as *mut Bridge<W>);
    bridge.workload.get_check_timeout()
}
unsafe extern "C" fn workload_drop<W: RustWorkload>(raw_bridge: *mut FDBWorkload) {
    unsafe { drop(Box::from_raw(raw_bridge as *mut Bridge<W>)) };
}

extern "C" fn workload_instantiate<W: RustWorkload + 'static>(
    raw_name: *const c_char,
    raw_context: *mut FDBWorkloadContext,
    bridge_to_server: BridgeToServer,
) -> BridgeToClient {
    let name = str_from_c(raw_name);
    let context = WorkloadContext::new(raw_context, bridge_to_server.context);
    let workload = Box::into_raw(Box::new(Bridge {
        workload: W::new(name, context),
        promise: bridge_to_server.promise,
    }));
    BridgeToClient {
        workload: workload as *mut _,
        setup: Some(workload_setup::<W>),
        start: Some(workload_start::<W>),
        check: Some(workload_check::<W>),
        getMetrics: Some(workload_get_metrics::<W>),
        getCheckTimeout: Some(workload_get_check_timeout::<W>),
        free: Some(workload_drop::<W>),
    }
}

// FIXME: workloadFactory
#[macro_export]
macro_rules! register_workload {
    ($name:ident) => {
        #[no_mangle]
        extern "C" fn workloadInstantiate(
            raw_name: *const $crate::c_char,
            raw_context: *mut $crate::FDBWorkloadContext,
            bridge_to_server: $crate::BridgeToServer,
        ) -> $crate::BridgeToClient {
            $crate::workload_instantiate::<$name>(raw_name, raw_context, bridge_to_server)
        }
    };
}
