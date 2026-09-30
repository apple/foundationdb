#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

use std::{ptr::NonNull, sync::Arc};

use foundationdb::Database;
use foundationdb_sys::FDBDatabase as FDBDatabaseAlias;

mod bindings;
pub mod env;
mod fdb_rt;

use bindings::{
    FDB_WORKLOAD_API_VERSION, FDBDatabase, FDBMetrics, FDBPromise, FDBWorkload, FDBWorkload_VT,
    OpaqueWorkload, Promise,
};
pub use bindings::{Metric, Metrics, Severity, WorkloadContext};
pub use env::{SimClock, SimRng};
use fdb_rt::fdb_spawn;

// -----------------------------------------------------------------------------
// User friendly types

/// Rust representation of a simulated FoundationDB database
pub type SimDatabase = Arc<Database>;
/// Rust representation of a FoundationDB workload
pub type WrappedWorkload = FDBWorkload;

/// Equivalent to the C++ abstract class `FDBWorkload`
#[allow(async_fn_in_trait)]
pub trait RustWorkload: Sized + 'static {
    /// This method is called by the tester during the setup phase.
    /// It should be used to populate the database.
    ///
    /// # Arguments
    ///
    /// * `db` - The simulated database.
    async fn setup(&mut self, db: SimDatabase);

    /// This method should run the actual test.
    ///
    /// # Arguments
    ///
    /// * `db` - The simulated database.
    async fn start(&mut self, db: SimDatabase);

    /// This method is called when the tester completes.
    /// A workload should run any consistency/correctness tests during this phase.
    ///
    /// # Arguments
    ///
    /// * `db` - The simulated database.
    async fn check(&mut self, db: SimDatabase);

    /// If a workload collects metrics (like latencies or throughput numbers), these should be reported back here.
    /// The multitester (or test orchestrator) will collect all metrics from all test clients and it will aggregate them.
    ///
    /// # Arguments
    ///
    /// * `out` - A metric sink
    fn get_metrics(&self, out: Metrics);

    /// Set the check timeout in simulated seconds for this workload.
    fn get_check_timeout(&self) -> f64;

    /// Virtual Table used by the C API
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
}

/// Equivalent to the C++ abstract class `FDBWorkloadFactory`
pub trait RustWorkloadFactory {
    /// The runtime FDB_API_VERSION to use
    const FDB_API_VERSION: u32 = foundationdb_sys::FDB_API_VERSION;
    /// If the test file contains a key-value pair workloadName the value will be passed to this method (empty string otherwise).
    /// This way, a library author can implement many workloads in one library and use the test file to chose which one to run
    /// (or run multiple workloads either concurrently or serially).
    fn create(name: String, context: WorkloadContext) -> WrappedWorkload;
}

/// Automatically implements a WorkloadFactory for a single workload
pub trait SingleRustWorkload: RustWorkload {
    /// The runtime FDB_API_VERSION to use
    const FDB_API_VERSION: u32 = foundationdb_sys::FDB_API_VERSION;
    /// The implicit WorkloadFactory will call this method uppon each instantiation
    fn new(name: String, context: WorkloadContext) -> Self;
}

// -----------------------------------------------------------------------------
// C to Rust bindings

fn check_database_ref(database: SimDatabase) {
    if Arc::strong_count(&database) != 1 || Arc::weak_count(&database) != 0 {
        eprintln!(
            "Reference to Database kept between phases (setup/start/check). All references should be dropped."
        );
        std::process::exit(1);
    }
    std::mem::forget(database);
}

#[cfg(coverage)]
fn write_coverage_profile() {
    unsafe extern "C" {
        fn __llvm_profile_reset_counters();
        fn __llvm_profile_write_file() -> i32;
    }

    // fdbserver exits with `_exit`, so LLVM's normal process-exit hook cannot
    // persist a profile from this dynamically loaded workload.
    let result = unsafe { __llvm_profile_write_file() };
    if result == 0 {
        // Every simulator client shares the DSO's counters. Reset only after a
        // successful snapshot so later client callbacks add, rather than repeat,
        // counts in LLVM_PROFILE_FILE's %m-merged profile.
        unsafe { __llvm_profile_reset_counters() };
    } else {
        eprintln!("failed to write LLVM coverage profile (status {result})");
    }
}

unsafe fn database_new(raw_database: *mut FDBDatabase) -> SimDatabase {
    unsafe {
        Arc::new(Database::new_from_pointer(NonNull::new_unchecked(
            raw_database as *mut FDBDatabaseAlias,
        )))
    }
}
unsafe extern "C" fn workload_setup<W: RustWorkload + 'static>(
    raw_workload: *mut OpaqueWorkload,
    raw_database: *mut FDBDatabase,
    raw_promise: FDBPromise,
) {
    unsafe {
        let workload = &mut *(raw_workload as *mut W);
        let database = database_new(raw_database);
        let done = Promise::new(raw_promise);
        fdb_spawn(async move {
            workload.setup(database.clone()).await;
            check_database_ref(database);
            done.send(true);
        });
    }
}
unsafe extern "C" fn workload_start<W: RustWorkload + 'static>(
    raw_workload: *mut OpaqueWorkload,
    raw_database: *mut FDBDatabase,
    raw_promise: FDBPromise,
) {
    unsafe {
        let workload = &mut *(raw_workload as *mut W);
        let database = database_new(raw_database);
        let done = Promise::new(raw_promise);
        fdb_spawn(async move {
            workload.start(database.clone()).await;
            check_database_ref(database);
            done.send(true);
        });
    }
}
unsafe extern "C" fn workload_check<W: RustWorkload + 'static>(
    raw_workload: *mut OpaqueWorkload,
    raw_database: *mut FDBDatabase,
    raw_promise: FDBPromise,
) {
    unsafe {
        let workload = &mut *(raw_workload as *mut W);
        let database = database_new(raw_database);
        let done = Promise::new(raw_promise);
        fdb_spawn(async move {
            workload.check(database.clone()).await;
            check_database_ref(database);
            done.send(true);
        });
    }
}
unsafe extern "C" fn workload_get_metrics<W: RustWorkload>(
    raw_workload: *mut OpaqueWorkload,
    raw_metrics: FDBMetrics,
) {
    unsafe {
        let workload = &*(raw_workload as *mut W);
        let out = Metrics::new(raw_metrics);
        workload.get_metrics(out);
        #[cfg(coverage)]
        write_coverage_profile();
    }
}
unsafe extern "C" fn workload_get_check_timeout<W: RustWorkload>(
    raw_workload: *mut OpaqueWorkload,
) -> f64 {
    unsafe {
        let workload = &*(raw_workload as *mut W);
        workload.get_check_timeout()
    }
}
unsafe extern "C" fn workload_drop<W: RustWorkload>(raw_workload: *mut OpaqueWorkload) {
    unsafe { drop(Box::from_raw(raw_workload as *mut W)) };
}

// -----------------------------------------------------------------------------
// Registration hooks

#[doc(hidden)]
/// Primitives exposed for the registrations hooks, should not be used otherwise
pub mod internals {
    pub use crate::bindings::{FDBWorkloadContext, str_from_c};
    pub use crate::fdb_rt::poll_pending_tasks;

    #[cfg(feature = "cpp-abi")]
    unsafe extern "C" {
        pub fn workloadCppFactory(logger: *const u8) -> *const u8;
    }

    #[allow(non_snake_case)]
    #[cfg(not(feature = "cpp-abi"))]
    pub unsafe extern "C" fn workloadCppFactory(_logger: *const u8) -> *const u8 {
        eprintln!(
            "This Rust workload was compiled without the C++ shim adapter. To fix this, either:

- Re-run the simulation with `useCAPI = true` (FoundationDB 7.4 or newer), or
- Recompile the workload with FoundationDB versions prior to 7.4 or the `cpp-abi` feature"
        );
        std::process::exit(1);
    }
}

/// Register a [RustWorkloadFactory].
/// /!\ Should be called only once.
#[macro_export]
macro_rules! register_factory {
    ($name:ident) => {
        #[unsafe(no_mangle)]
        extern "C" fn workloadCFactory(
            raw_name: *const std::ffi::c_char,
            raw_context: $crate::internals::FDBWorkloadContext,
        ) -> $crate::WrappedWorkload {
            use std::sync::atomic::{AtomicBool, Ordering};
            static DONE: AtomicBool = AtomicBool::new(false);
            if DONE
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let version = <$name as $crate::RustWorkloadFactory>::FDB_API_VERSION;
                let _ = foundationdb::api::FdbApiBuilder::default()
                    .set_runtime_version(version as i32)
                    .build();
                println!("FDB API version selected: {version}");
                foundationdb::future::CUSTOM_EXECUTOR_HOOK
                    .set($crate::internals::poll_pending_tasks)
                    .unwrap();
            }
            let name = $crate::internals::str_from_c(raw_name);
            let context = $crate::WorkloadContext::new(raw_context);
            <$name as $crate::RustWorkloadFactory>::create(name, context)
        }
        #[unsafe(no_mangle)]
        extern "C" fn workloadFactory(logger: *const u8) -> *const u8 {
            unsafe { $crate::internals::workloadCppFactory(logger) }
        }
    };
}

/// Register a [SingleRustWorkload] and creates an implicit WorkloadFactory.
/// /!\ Should be called only once.
#[macro_export]
macro_rules! register_workload {
    ($name:ident) => {
        #[unsafe(no_mangle)]
        extern "C" fn workloadCFactory(
            raw_name: *const std::ffi::c_char,
            raw_context: $crate::internals::FDBWorkloadContext,
        ) -> $crate::WrappedWorkload {
            use std::sync::atomic::{AtomicBool, Ordering};
            static DONE: AtomicBool = AtomicBool::new(false);
            if DONE
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let version = <$name as $crate::SingleRustWorkload>::FDB_API_VERSION;
                let _ = foundationdb::api::FdbApiBuilder::default()
                    .set_runtime_version(version as i32)
                    .build();
                println!("FDB API version selected: {version}");
                foundationdb::future::CUSTOM_EXECUTOR_HOOK
                    .set($crate::internals::poll_pending_tasks)
                    .unwrap();
            }
            let name = $crate::internals::str_from_c(raw_name);
            let context = $crate::WorkloadContext::new(raw_context);
            $crate::RustWorkload::wrap(<$name as $crate::SingleRustWorkload>::new(name, context))
        }
        #[unsafe(no_mangle)]
        extern "C" fn workloadFactory(logger: *const u8) -> *const u8 {
            unsafe { $crate::internals::workloadCppFactory(logger) }
        }
    };
}
