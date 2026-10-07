//! Owns the lifetime shared by a registered workload and its context handles.

use crate::bindings::{
    ContextOwner, FDB_WORKLOAD_API_VERSION, FDBDatabase, FDBMetrics, FDBPromise, FDBWorkload,
    FDBWorkload_VT, FDBWorkloadContext, OpaqueWorkload,
};
use crate::{WorkloadContext, WrappedWorkload};

struct RegisteredWorkload {
    // Field order keeps the context live through the user's workload destructor.
    workload: WrappedWorkload,
    _context: ContextOwner,
}

/// Registers a workload and invalidates its context when the simulator releases it.
///
/// # Safety
/// `raw_context` must be a valid context on its owning simulator thread, with a
/// complete vtable. The simulator must keep it alive until it calls the returned
/// workload's `free` callback. All callbacks must run on that same thread, follow
/// the workload ABI contracts, and `free` must be called at most once.
///
/// ```compile_fail,E0133
/// use foundationdb_simulation::internals::{FDBWorkloadContext, register_workload_context};
/// let raw = FDBWorkloadContext {
///     api_version: 1,
///     inner: std::ptr::null_mut(),
///     vt: std::ptr::null_mut(),
/// };
/// register_workload_context(raw, |_| unreachable!());
/// ```
#[doc(hidden)]
pub unsafe fn register_workload_context(
    raw_context: FDBWorkloadContext,
    create: impl FnOnce(WorkloadContext) -> WrappedWorkload,
) -> FDBWorkload {
    // SAFETY: This function's caller supplies the native lifetime; ContextOwner
    // revokes every clone when this lifetime ends, including factory unwinding.
    let context = ContextOwner(unsafe { WorkloadContext::new(raw_context) });
    let workload = create(context.0.clone());
    FDBWorkload {
        api_version: FDB_WORKLOAD_API_VERSION,
        inner: Box::into_raw(Box::new(RegisteredWorkload {
            workload,
            _context: context,
        })) as *mut _,
        vt: &VT as *const _ as *mut _,
    }
}

const VT: FDBWorkload_VT = FDBWorkload_VT {
    setup: Some(setup),
    start: Some(start),
    check: Some(check),
    getMetrics: Some(get_metrics),
    getCheckTimeout: Some(get_check_timeout),
    free: Some(free),
};

macro_rules! forward {
    ($raw:ident, $method:ident $(, $arg:ident)*) => {{
        // SAFETY: The simulator returns the registered allocation to its matching
        // callback with arguments satisfying the external-workload ABI.
        unsafe {
            let registered = &*($raw as *const RegisteredWorkload);
            let workload = &registered.workload.0;
            (*workload.vt).$method.unwrap()(workload.inner $(, $arg)*)
        }
    }};
}

unsafe extern "C" fn setup(raw: *mut OpaqueWorkload, db: *mut FDBDatabase, done: FDBPromise) {
    forward!(raw, setup, db, done);
}

unsafe extern "C" fn start(raw: *mut OpaqueWorkload, db: *mut FDBDatabase, done: FDBPromise) {
    forward!(raw, start, db, done);
}

unsafe extern "C" fn check(raw: *mut OpaqueWorkload, db: *mut FDBDatabase, done: FDBPromise) {
    forward!(raw, check, db, done);
}

unsafe extern "C" fn get_metrics(raw: *mut OpaqueWorkload, out: FDBMetrics) {
    forward!(raw, getMetrics, out);
}

unsafe extern "C" fn get_check_timeout(raw: *mut OpaqueWorkload) -> f64 {
    forward!(raw, getCheckTimeout)
}

unsafe extern "C" fn free(raw: *mut OpaqueWorkload) {
    // SAFETY: The simulator releases this registered allocation exactly once.
    unsafe { drop(Box::from_raw(raw as *mut RegisteredWorkload)) };
}
