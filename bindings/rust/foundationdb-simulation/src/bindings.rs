//! Wrapper module
//!
//! This module defines all C and Rust structures.
//! It also provides bindings and wrappers to map behavior from Rust to C.

use std::{
    ffi::{self, c_char},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, ThreadId},
    time::Duration,
};

use foundationdb as fdb;

mod raw_bindings {
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(missing_docs)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
pub use raw_bindings::{
    FDBDatabase, FDBMetrics, FDBPromise, FDBWorkload, FDBWorkloadContext, OpaqueWorkload,
};
use raw_bindings::{
    FDBMetric, FDBSeverity, FDBSeverity_FDBSeverity_Debug, FDBSeverity_FDBSeverity_Error,
    FDBSeverity_FDBSeverity_Info, FDBSeverity_FDBSeverity_Warn, FDBSeverity_FDBSeverity_WarnAlways,
    FDBStringPair,
};

pub use raw_bindings::FDBWorkload_FDBWorkload_VT as FDBWorkload_VT;
pub const FDB_WORKLOAD_API_VERSION: i32 = raw_bindings::FDB_WORKLOAD_API_VERSION as i32;

// -----------------------------------------------------------------------------
// String conversions

#[doc(hidden)]
/// # Safety
/// `c_buf` must point to a readable, NUL-terminated string for this call.
///
/// ```compile_fail,E0133
/// foundationdb_simulation::internals::str_from_c(std::ptr::null());
/// ```
pub unsafe fn str_from_c(c_buf: *const c_char) -> String {
    let c_str = unsafe { ffi::CStr::from_ptr(c_buf) };
    c_str.to_str().unwrap().to_string()
}
#[doc(hidden)]
pub fn str_for_c<T>(buf: T) -> ffi::CString
where
    T: Into<Vec<u8>>,
{
    let mut buf = buf.into();
    if buf.contains(&0) {
        let mut escaped = Vec::with_capacity(buf.len());
        for byte in buf {
            if byte == 0 {
                escaped.extend_from_slice(br"\0");
            } else {
                escaped.push(byte);
            }
        }
        buf = escaped;
    }

    // SAFETY: Every interior NUL byte was escaped above.
    unsafe { ffi::CString::from_vec_unchecked(buf) }
}

/// Capitalizes the first letter of a string.
/// Used to ensure trace detail names start with a capital letter.
/// Returns `None` if the string is empty.
fn capitalize_first(s: &str) -> Option<String> {
    let mut chars = s.chars();
    chars
        .next()
        .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
}

/// ASCII-uppercases the first byte of a trace event name, matching FDB's convention that
/// event types start with a capital letter. Leaves an empty name unchanged.
fn capitalize_first_byte(mut name: Vec<u8>) -> Vec<u8> {
    if let Some(first) = name.first_mut() {
        first.make_ascii_uppercase();
    }
    name
}

/// Macro that can be used to create log "details" more easily.
#[macro_export]
macro_rules! details {
    ($($k:expr_2021 => $v:expr_2021),* $(,)?) => {
        &[
            $((
                &$k.to_string(), &$v.to_string()
            )),*
        ]
    };
}

// -----------------------------------------------------------------------------
// Rust Types

/// A simulator context shared by one workload and its environment handles.
///
/// Context operations panic outside the creating thread or after the registered
/// workload is released. Cloning the context does not extend the native lifetime.
#[derive(Clone)]
pub struct WorkloadContext(Arc<ContextState>);

struct ContextState {
    raw: FDBWorkloadContext,
    owner: ThreadId,
    active: AtomicBool,
}

// SAFETY: Raw context access is private and always checks the owning thread and
// lifetime first. No other thread can call the native vtable or invalidate the
// context, and dropping the state does not dereference its raw pointers.
unsafe impl Send for ContextState {}
// SAFETY: The same owner-thread check prevents concurrent native access.
unsafe impl Sync for ContextState {}

pub(crate) struct ContextOwner(pub(crate) WorkloadContext);

impl Drop for ContextOwner {
    fn drop(&mut self) {
        self.0.assert_owner();
        self.0.0.active.store(false, Ordering::Release);
    }
}
/// Wrapper around the C FDBPromise
pub struct Promise(FDBPromise);
/// A metrics sink borrowed for one [`crate::RustWorkload::get_metrics`] callback.
///
/// The native sink is destroyed after the callback returns, so workloads cannot
/// retain it for later use:
///
/// ```compile_fail,E0521
/// use std::cell::RefCell;
/// use foundationdb_simulation::{Metrics, RustWorkload, SimDatabase};
///
/// struct RetainingWorkload {
///     saved: RefCell<Option<Metrics<'static>>>,
/// }
///
/// impl RustWorkload for RetainingWorkload {
///     async fn setup(&mut self, _: SimDatabase) {}
///     async fn start(&mut self, _: SimDatabase) {}
///     async fn check(&mut self, _: SimDatabase) {}
///     fn get_metrics(&self, out: Metrics<'_>) {
///         *self.saved.borrow_mut() = Some(out);
///     }
///     fn get_check_timeout(&self) -> f64 { 1.0 }
/// }
/// ```
pub struct Metrics<'callback>(&'callback mut FDBMetrics);

/// A single metric entry
#[derive(Clone)]
pub struct Metric<'a> {
    /// The name of the metric
    pub key: &'a str,
    /// The value of the metric
    pub val: f64,
    /// Indicates if the value represents an average or not
    pub avg: bool,
    /// C `printf` format for one `double`, defaulting to `%.3g`.
    ///
    /// Supported formats contain exactly one `a`, `A`, `e`, `E`, `f`, `F`, `g`,
    /// or `G` conversion, with optional `-`, `+`, space, `#`, and `0` flags and
    /// decimal width and precision no greater than `i32::MAX`. Literal text and
    /// `%%` are allowed. Length modifiers, positional arguments, `*`, and other
    /// conversions are rejected by [`Metrics::push`] before entering native code.
    pub fmt: Option<&'a str>,
}

/// Indicates the severity of a FoundationDB log entry
#[derive(Clone, Copy)]
#[repr(u32)]
pub enum Severity {
    /// debug
    Debug = FDBSeverity_FDBSeverity_Debug,
    /// info
    Info = FDBSeverity_FDBSeverity_Info,
    /// warn
    Warn = FDBSeverity_FDBSeverity_Warn,
    /// warn always
    WarnAlways = FDBSeverity_FDBSeverity_WarnAlways,
    /// error, this severity automatically breaks execution. `WorkloadContext::trace` also
    /// appends a `RustFailure="1"` detail on top of the `RustWorkload="1"` detail added to
    /// every event, so trace consumers can tell a Rust workload failure apart from an
    /// FDB-internal Sev40 event.
    Error = FDBSeverity_FDBSeverity_Error,
}

// -----------------------------------------------------------------------------
// Implementations

macro_rules! with {
    ($this:expr_2021=>$method:ident($($args:expr_2021),* $(,)?)) => {
        {
            let raw = $this;
            unsafe { (*raw.vt).$method.unwrap_unchecked()(raw.inner $(, $args)*) }
        }
    };
}

/// Detail key automatically appended to `Severity::Error` trace events.
const RUST_FAILURE_KEY: &str = "RustFailure";
/// Detail key automatically appended to every trace event, regardless of severity.
const RUST_WORKLOAD_KEY: &str = "RustWorkload";

/// Appends a `key="1"` detail to `details_storage` unless a detail with that key (already
/// capitalized) is present.
fn push_marker_if_absent(details_storage: &mut Vec<(ffi::CString, ffi::CString)>, key: &str) {
    if !details_storage
        .iter()
        .any(|(k, _)| k.as_bytes() == key.as_bytes())
    {
        details_storage.push((str_for_c(key), str_for_c("1")));
    }
}

/// Builds the trace detail storage for [`WorkloadContext::trace`].
///
/// Applies `capitalize_first` to every caller-supplied key (dropping empty values), then
/// appends a `RustWorkload="1"` detail to every event and, for [`Severity::Error`], a
/// `RustFailure="1"` detail as well, unless the caller already supplied one under that key.
fn prepare_trace_details<S2, S3>(
    severity: Severity,
    details: &[(S2, S3)],
) -> Vec<(ffi::CString, ffi::CString)>
where
    S2: AsRef<str>,
    S3: AsRef<str>,
{
    let mut details_storage = details
        .iter()
        .filter_map(|(key, val)| {
            let val = val.as_ref();
            if val.is_empty() {
                return None;
            }
            capitalize_first(key.as_ref()).map(|k| (str_for_c(k), str_for_c(val)))
        })
        .collect::<Vec<_>>();
    push_marker_if_absent(&mut details_storage, RUST_WORKLOAD_KEY);
    if matches!(severity, Severity::Error) {
        push_marker_if_absent(&mut details_storage, RUST_FAILURE_KEY);
    }
    details_storage
}

impl WorkloadContext {
    #[doc(hidden)]
    /// # Safety
    /// Every operation on this context or its clones requires `raw` to identify
    /// a live native context with a complete vtable owned by the creating thread.
    /// The caller must uphold that lifetime for all handles. Construction,
    /// cloning, and dropping do not access the native pointers. Registration
    /// hooks invalidate all clones before the native context is destroyed.
    ///
    /// ```compile_fail,E0133
    /// use foundationdb_simulation::{WorkloadContext, internals::FDBWorkloadContext};
    /// let raw = FDBWorkloadContext {
    ///     api_version: 1,
    ///     inner: std::ptr::null_mut(),
    ///     vt: std::ptr::null_mut(),
    /// };
    /// WorkloadContext::new(raw);
    /// ```
    pub unsafe fn new(raw: FDBWorkloadContext) -> Self {
        Self(Arc::new(ContextState {
            raw,
            owner: thread::current().id(),
            active: AtomicBool::new(true),
        }))
    }

    fn assert_owner(&self) {
        assert_eq!(
            self.0.owner,
            thread::current().id(),
            "simulation context accessed outside its owning thread"
        );
    }

    fn raw(&self) -> FDBWorkloadContext {
        self.assert_owner();
        assert!(
            self.0.active.load(Ordering::Acquire),
            "simulation context accessed after workload release"
        );
        self.0.raw
    }

    /// Get the server FDB_WORKLOAD_API_VERSION
    pub fn get_workload_api_version(&self) -> i32 {
        self.raw().api_version
    }

    /// Add a log entry in the FoundationDB logs.
    ///
    /// The event `name`'s first byte is uppercased automatically, matching FDB's convention
    /// for event types. A `RustWorkload="1"` detail is appended to every event (unless the
    /// caller already provided one), so trace consumers can grep all Rust-origin trace lines
    /// with a single token. When `severity` is [`Severity::Error`], a `RustFailure="1"` detail
    /// is appended too (same rule), so a Rust-detected failure can be told apart from an
    /// FDB-internal Sev40 event.
    pub fn trace<S, S2, S3>(&self, severity: Severity, name: S, details: &[(S2, S3)])
    where
        S: Into<Vec<u8>>,
        S2: AsRef<str>,
        S3: AsRef<str>,
    {
        let name = str_for_c(capitalize_first_byte(name.into()));
        let details_storage = prepare_trace_details(severity, details);
        let details = details_storage
            .iter()
            .map(|(key, val)| FDBStringPair {
                key: key.as_ptr(),
                val: val.as_ptr(),
            })
            .collect::<Vec<_>>();
        with! {
            self.raw() => trace(
                severity as FDBSeverity,
                name.as_ptr(),
                details.as_ptr(),
                details.len() as i32,
            )
        }
    }
    /// Get the process id of the workload
    pub fn get_process_id(&self) -> u64 {
        with! { self.raw() => getProcessID() }
    }
    /// Switch the simulator's current process using a native process handle.
    ///
    /// # Safety
    /// During simulation, `id` must be an unchanged handle previously obtained
    /// from [`Self::get_process_id`] in the same simulator instance, and its
    /// native process must remain alive while selected. It is a pointer-valued
    /// handle, not an arbitrary numeric process identifier. The caller must
    /// ensure intervening operations are valid for that process and restore the
    /// previous process before yielding, returning, or unwinding.
    ///
    /// ```compile_fail,E0133
    /// use foundationdb_simulation::WorkloadContext;
    /// fn change_process(context: &WorkloadContext) {
    ///     context.set_process_id(1);
    /// }
    /// ```
    pub unsafe fn set_process_id(&self, id: u64) {
        with! { self.raw() => setProcessID(id) }
    }
    /// Get the current simulated time in seconds (starts at zero)
    pub fn now(&self) -> f64 {
        with! { self.raw() => now() }
    }
    /// Get a determinist 32-bit random number
    pub fn rnd(&self) -> u32 {
        with! { self.raw() => rnd() }
    }
    /// Get the value of a parameter from the simulation config file
    ///
    /// /!\ getting an option consumes it, following call on that option will return `None`
    pub fn get_option<T>(&self, name: &str) -> Option<T>
    where
        T: FromStr,
    {
        self.get_option_raw(name)
            .and_then(|value| value.parse::<T>().ok())
    }
    fn get_option_raw(&self, name: &str) -> Option<String> {
        let null = "";
        let name = str_for_c(name);
        let default_value = str_for_c(null);
        let raw_value = with! {
            self.raw() => getOption(name.as_ptr(), default_value.as_ptr())
        };
        // SAFETY: getOption returns an owned, NUL-terminated string.
        let value = unsafe { str_from_c(raw_value.inner) };
        with! { raw_value => free() };
        if value == null { None } else { Some(value) }
    }
    /// Get the client id of the workload
    pub fn client_id(&self) -> i32 {
        with! { self.raw() => clientId() }
    }
    /// Get the client id of the workload
    pub fn client_count(&self) -> i32 {
        with! { self.raw() => clientCount() }
    }
    /// Get a determinist 64-bit random number
    pub fn shared_random_number(&self) -> i64 {
        with! { self.raw() => sharedRandomNumber() }
    }
    /// Return a future that will be ready after a given (simulated) duration
    pub fn delay(
        &self,
        duration: Duration,
    ) -> impl std::future::Future<Output = fdb::FdbResult<()>> + Send + Sync + 'static + use<> {
        let f = with! { self.raw() => delay(duration.as_secs_f64()) };
        // SAFETY: delay returns a new, owned C future with no result value.
        unsafe { fdb::future::FdbFuture::new(f as *mut _) }
    }
}

impl Promise {
    pub(crate) fn new(raw: FDBPromise) -> Self {
        Self(raw)
    }
    /// Resolve a FoundationDB promise by setting its value to a boolean.
    /// You can resolve a Promise only once.
    ///
    /// note: FoundationDB disregards the value sent, so sending `true` or `false` is equivalent
    pub fn send(self, value: bool) {
        with! { self.0 => send(value) };
    }
}
impl Drop for Promise {
    fn drop(&mut self) {
        with! { self.0 => free() };
    }
}

// Native metric formatting passes exactly one double to printf. Keep this
// grammar narrow so neither the conversion nor width/precision consumes a
// differently typed or additional variadic argument.
fn valid_metric_format(format: &str) -> bool {
    fn decimal(input: &mut &[u8]) -> bool {
        let mut value = 0_i32;
        while let Some((&digit, rest)) = input.split_first() {
            if !digit.is_ascii_digit() {
                break;
            }
            let Some(next) = value
                .checked_mul(10)
                .and_then(|value| value.checked_add(i32::from(digit - b'0')))
            else {
                return false;
            };
            value = next;
            *input = rest;
        }
        true
    }

    let mut input = format.as_bytes();
    let mut conversion = false;
    while let Some((&byte, rest)) = input.split_first() {
        input = rest;
        if byte != b'%' {
            continue;
        }
        if input.first() == Some(&b'%') {
            input = &input[1..];
            continue;
        }
        if conversion {
            return false;
        }
        while matches!(input.first(), Some(b'-' | b'+' | b' ' | b'#' | b'0')) {
            input = &input[1..];
        }
        if !decimal(&mut input) {
            return false;
        }
        if input.first() == Some(&b'.') {
            input = &input[1..];
            if !decimal(&mut input) {
                return false;
            }
        }
        match input.split_first() {
            Some((b'a' | b'A' | b'e' | b'E' | b'f' | b'F' | b'g' | b'G', rest)) => {
                conversion = true;
                input = rest;
            }
            _ => return false,
        }
    }
    conversion
}

impl<'callback> Metrics<'callback> {
    pub(crate) fn new(raw: &'callback mut FDBMetrics) -> Self {
        Self(raw)
    }
    /// Call std::vector::reserve on the underlying C++ sink
    pub fn reserve(&mut self, n: usize) {
        with! { &*self.0 => reserve(n as i32) }
    }
    /// Push a [Metric] entry in the underlying C++ sink.
    ///
    /// # Panics
    /// Panics before calling the native sink if [`Metric::fmt`] does not follow
    /// its supported single-double format grammar.
    pub fn push(&mut self, metric: Metric) {
        let format = metric.fmt.unwrap_or("%.3g");
        assert!(
            valid_metric_format(format),
            "invalid metric format: {format:?}"
        );
        let key_storage = str_for_c(metric.key);
        let fmt_storage = str_for_c(format);
        with! {
            &*self.0 => push(FDBMetric {
                key: key_storage.as_ptr(),
                fmt: fmt_storage.as_ptr(),
                val: metric.val,
                avg: metric.avg,
            })
        }
    }
    /// Push several [Metric] entries in the underlying C++ sink
    pub fn extend<'a, T>(&mut self, metrics: T)
    where
        T: IntoIterator<Item = Metric<'a>>,
    {
        let metrics = metrics.into_iter();
        let (min, max) = metrics.size_hint();
        self.reserve(max.unwrap_or(min));
        for metric in metrics {
            self.push(metric);
        }
    }
}

impl<'a> Metric<'a> {
    /// Create a metric value entry
    pub fn val<V>(key: &'a str, val: V) -> Self
    where
        V: TryInto<f64>,
    {
        Self {
            key,
            val: val.try_into().ok().expect("convertion failed"),
            avg: false,
            fmt: None,
        }
    }
    /// Create a metric average entry
    pub fn avg<V>(key: &'a str, val: V) -> Self
    where
        V: TryInto<f64>,
    {
        Self {
            key,
            val: val.try_into().ok().expect("convertion failed"),
            avg: true,
            fmt: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Severity, capitalize_first_byte, prepare_trace_details, str_for_c};

    use std::cell::Cell;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use super::WorkloadContext;
    use super::raw_bindings::*;
    use crate::registration::register_workload_context;
    use crate::{Metric, Metrics, RustWorkload, SimDatabase};

    struct NativeContext {
        calls: Arc<AtomicUsize>,
        next_random: Cell<u32>,
    }

    unsafe extern "C" fn now(raw: *mut OpaqueWorkloadContext) -> f64 {
        // SAFETY: The test keeps NativeContext alive until workload release.
        let context = unsafe { &*(raw as *const NativeContext) };
        context.calls.fetch_add(1, Ordering::Relaxed);
        12.0
    }

    unsafe extern "C" fn rnd(raw: *mut OpaqueWorkloadContext) -> u32 {
        // SAFETY: The test keeps NativeContext alive until workload release.
        let context = unsafe { &*(raw as *const NativeContext) };
        context.calls.fetch_add(1, Ordering::Relaxed);
        let value = context.next_random.get();
        context.next_random.set(value + 1);
        value
    }

    // The unused callbacks are still present so the fake has a complete vtable.
    unsafe extern "C" fn trace(
        _: *mut OpaqueWorkloadContext,
        _: FDBSeverity,
        _: *const std::ffi::c_char,
        _: *const FDBStringPair,
        _: i32,
    ) {
    }
    unsafe extern "C" fn get_process_id(_: *mut OpaqueWorkloadContext) -> u64 {
        0
    }
    unsafe extern "C" fn set_process_id(_: *mut OpaqueWorkloadContext, _: u64) {}
    unsafe extern "C" fn free_string(_: *const std::ffi::c_char) {}
    unsafe extern "C" fn get_option(
        _: *mut OpaqueWorkloadContext,
        _: *const std::ffi::c_char,
        default: *const std::ffi::c_char,
    ) -> FDBString {
        FDBString {
            inner: default,
            vt: &FDBString_FDBString_VT {
                free: Some(free_string),
            } as *const _ as *mut _,
        }
    }
    unsafe extern "C" fn client_id(_: *mut OpaqueWorkloadContext) -> i32 {
        0
    }
    unsafe extern "C" fn client_count(_: *mut OpaqueWorkloadContext) -> i32 {
        1
    }
    unsafe extern "C" fn shared_random_number(_: *mut OpaqueWorkloadContext) -> i64 {
        42
    }
    unsafe extern "C" fn delay(_: *mut OpaqueWorkloadContext, _: f64) -> *mut FDBFuture {
        unreachable!("test does not schedule delays")
    }

    const CONTEXT_VT: FDBWorkloadContext_FDBWorkloadContext_VT =
        FDBWorkloadContext_FDBWorkloadContext_VT {
            trace: Some(trace),
            getProcessID: Some(get_process_id),
            setProcessID: Some(set_process_id),
            now: Some(now),
            rnd: Some(rnd),
            getOption: Some(get_option),
            clientId: Some(client_id),
            clientCount: Some(client_count),
            sharedRandomNumber: Some(shared_random_number),
            delay: Some(delay),
        };

    fn native_context(calls: Arc<AtomicUsize>) -> (Box<NativeContext>, FDBWorkloadContext) {
        let mut native = Box::new(NativeContext {
            calls,
            next_random: Cell::new(42),
        });
        let raw = FDBWorkloadContext {
            api_version: super::FDB_WORKLOAD_API_VERSION,
            inner: &mut *native as *mut NativeContext as *mut _,
            vt: &CONTEXT_VT as *const _ as *mut _,
        };
        (native, raw)
    }

    struct TestWorkload(Option<WorkloadContext>);

    impl RustWorkload for TestWorkload {
        async fn setup(&mut self, _: SimDatabase) {}
        async fn start(&mut self, _: SimDatabase) {}
        async fn check(&mut self, _: SimDatabase) {}
        fn get_metrics(&self, mut out: Metrics<'_>) {
            out.reserve(3);
            out.push(Metric::val("operations", 7));
            out.extend([
                Metric::avg("latency", 2.5),
                Metric {
                    key: "completion",
                    val: 1.0,
                    avg: false,
                    fmt: Some("%.0f"),
                },
            ]);
        }
        fn get_check_timeout(&self) -> f64 {
            7.0
        }
    }

    impl Drop for TestWorkload {
        fn drop(&mut self) {
            if let Some(context) = &self.0 {
                assert_eq!(context.now(), 12.0);
            }
        }
    }

    #[derive(Default)]
    struct NativeMetrics {
        reservations: Vec<i32>,
        entries: Vec<(String, f64, bool, String)>,
    }

    unsafe extern "C" fn reserve_metrics(raw: *mut OpaqueMetrics, n: i32) {
        // SAFETY: The metrics callback keeps the native sink alive and exclusive.
        let sink = unsafe { &mut *(raw as *mut NativeMetrics) };
        sink.reservations.push(n);
        sink.entries.reserve(n as usize);
    }

    unsafe extern "C" fn push_metric(raw: *mut OpaqueMetrics, metric: FDBMetric) {
        // SAFETY: The callback owns the sink and borrows these strings for this call.
        let sink = unsafe { &mut *(raw as *mut NativeMetrics) };
        let key = unsafe { super::str_from_c(metric.key) };
        let format = unsafe { super::str_from_c(metric.fmt) };
        sink.entries.push((key, metric.val, metric.avg, format));
    }

    #[test]
    fn metrics_callback_writes_to_borrowed_native_sink() {
        let workload = TestWorkload(None).wrap();
        let mut sink = NativeMetrics::default();
        let mut metrics_vt = FDBMetrics_FDBMetrics_VT {
            reserve: Some(reserve_metrics),
            push: Some(push_metric),
        };
        let raw_metrics = FDBMetrics {
            inner: &mut sink as *mut NativeMetrics as *mut _,
            vt: &mut metrics_vt,
        };

        // SAFETY: The workload, sink, and vtable all remain live for this callback.
        unsafe { (*workload.0.vt).getMetrics.unwrap()(workload.0.inner, raw_metrics) };

        assert_eq!(sink.reservations.first(), Some(&3));
        assert_eq!(
            sink.entries,
            vec![
                ("operations".into(), 7.0, false, "%.3g".into()),
                ("latency".into(), 2.5, true, "%.3g".into()),
                ("completion".into(), 1.0, false, "%.0f".into()),
            ]
        );
    }

    #[test]
    fn metrics_reject_unsafe_formats_before_calling_the_native_sink() {
        let mut sink = NativeMetrics::default();
        let mut metrics_vt = FDBMetrics_FDBMetrics_VT {
            reserve: Some(reserve_metrics),
            push: Some(push_metric),
        };
        let mut raw_metrics = FDBMetrics {
            inner: &mut sink as *mut NativeMetrics as *mut _,
            vt: &mut metrics_vt,
        };

        for format in [
            None,
            Some("%a"),
            Some("%A"),
            Some("%e"),
            Some("%E"),
            Some("%f"),
            Some("%F"),
            Some("%g"),
            Some("%G"),
            Some("%.0f"),
            Some("load=%-+#010.3f%%"),
            Some("% .f"),
            Some("%% latency %.6g ms %%"),
        ] {
            super::Metrics::new(&mut raw_metrics).push(Metric {
                key: "value",
                val: 1.25,
                avg: false,
                fmt: format,
            });
            assert_eq!(
                sink.entries.last(),
                Some(&("value".into(), 1.25, false, format.unwrap_or("%.3g").into(),)),
            );
        }

        let forwarded = sink.entries.len();
        for format in [
            "",
            "value %%",
            "%n",
            "%s",
            "%d",
            "%c",
            "%p",
            "%Lf",
            "%lf",
            "%llf",
            "%hf",
            "%zf",
            "%jf",
            "%tf",
            "%1$f",
            "%1$.*2$f",
            "%*f",
            "%.*f",
            "%*.*f",
            "%f %g",
            "%f %n",
            "%f%",
            "%",
            "%.",
            "%.2",
            "%.2.3f",
            "%2147483648f",
            "%.2147483648f",
            "%99999999999999999999999g",
            "%g\0%n",
        ] {
            // Catch on the Rust side: a panic must never cross the C callback.
            let rejected = catch_unwind(AssertUnwindSafe(|| {
                super::Metrics::new(&mut raw_metrics).push(Metric {
                    key: "rejected",
                    val: 1.25,
                    avg: false,
                    fmt: Some(format),
                });
            }));
            assert!(rejected.is_err(), "accepted {format:?}");
            assert_eq!(sink.entries.len(), forwarded, "forwarded {format:?}");
        }
    }

    #[test]
    fn environment_rejects_access_after_registered_workload_release() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (native, raw) = native_context(calls.clone());
        let mut escaped = None;
        // SAFETY: Native context stays alive through the same-thread free call.
        let workload = unsafe {
            register_workload_context(raw, |context| {
                escaped = Some(context.clone());
                TestWorkload(Some(context)).wrap()
            })
        };
        let context = escaped.unwrap();
        let environment = context.environment();
        assert_eq!(environment.clock().monotonic(), Duration::from_secs(12));
        assert_eq!(environment.rng().next_u32(), 42);
        // SAFETY: These are the callbacks on the registered, live allocation.
        unsafe {
            assert_eq!((*workload.vt).getCheckTimeout.unwrap()(workload.inner), 7.0);
            (*workload.vt).free.unwrap()(workload.inner);
        }
        assert_eq!(
            calls.load(Ordering::Relaxed),
            3,
            "destructor retains live context"
        );
        drop(native);
        assert!(catch_unwind(AssertUnwindSafe(|| context.now())).is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| environment.clock().wall())).is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| environment.rng().next_u64())).is_err());
        assert_eq!(
            calls.load(Ordering::Relaxed),
            3,
            "rejected access never enters native context"
        );
    }

    #[test]
    fn environment_rejects_other_threads_without_consuming_randomness() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (_native, raw) = native_context(calls.clone());
        let mut escaped = None;
        // SAFETY: Native context stays alive through the same-thread free call.
        let workload = unsafe {
            register_workload_context(raw, |context| {
                escaped = Some(context.environment());
                TestWorkload(None).wrap()
            })
        };
        let environment = escaped.unwrap();
        let other_thread = environment.clone();
        std::thread::spawn(move || {
            assert!(catch_unwind(AssertUnwindSafe(|| other_thread.clock().monotonic())).is_err());
            assert!(catch_unwind(AssertUnwindSafe(|| other_thread.rng().next_u32())).is_err());
        })
        .join()
        .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(environment.rng().next_u64(), (42_u64 << 32) | 43);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
        // SAFETY: This is the unique release of the registered allocation.
        unsafe { (*workload.vt).free.unwrap()(workload.inner) };
        assert!(catch_unwind(AssertUnwindSafe(|| environment.rng().next_u32())).is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn factory_unwind_invalidates_escaped_context() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (native, raw) = native_context(calls.clone());
        let mut escaped = None;
        assert!(
            catch_unwind(AssertUnwindSafe(|| unsafe {
                // SAFETY: The native context outlives registration, which unwinds.
                register_workload_context(raw, |context| {
                    escaped = Some(context.environment());
                    panic!("factory failed");
                })
            }))
            .is_err()
        );
        drop(native);
        assert!(catch_unwind(AssertUnwindSafe(|| escaped.unwrap().rng().next_u32())).is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn str_for_c_escapes_interior_nul() {
        assert_eq!(str_for_c("a\0b").to_bytes(), br"a\0b");
    }

    #[test]
    fn error_severity_gets_rust_failure_marker() {
        let details: &[(&str, &str)] = &[("Reason", "boom")];
        let result = prepare_trace_details(Severity::Error, details);
        assert!(
            result
                .iter()
                .any(|(k, v)| k.as_bytes() == b"RustFailure" && v.as_bytes() == b"1")
        );
    }

    #[test]
    fn non_error_severity_has_no_rust_failure_marker() {
        let details: &[(&str, &str)] = &[("Reason", "boom")];
        let result = prepare_trace_details(Severity::Warn, details);
        assert!(!result.iter().any(|(k, _)| k.as_bytes() == b"RustFailure"));
    }

    #[test]
    fn caller_lowercase_rust_failure_key_is_not_duplicated() {
        let details: &[(&str, &str)] = &[("rustFailure", "custom")];
        let result = prepare_trace_details(Severity::Error, details);
        let matching: Vec<_> = result
            .iter()
            .filter(|(k, _)| k.as_bytes() == b"RustFailure")
            .collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].1.as_bytes(), b"custom");
    }

    #[test]
    fn caller_capitalized_rust_failure_key_is_not_duplicated() {
        let details: &[(&str, &str)] = &[("RustFailure", "custom")];
        let result = prepare_trace_details(Severity::Error, details);
        let matching: Vec<_> = result
            .iter()
            .filter(|(k, _)| k.as_bytes() == b"RustFailure")
            .collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].1.as_bytes(), b"custom");
    }

    #[test]
    fn every_severity_gets_rust_workload_marker() {
        let details: &[(&str, &str)] = &[];
        for severity in [
            Severity::Debug,
            Severity::Info,
            Severity::Warn,
            Severity::WarnAlways,
            Severity::Error,
        ] {
            let result = prepare_trace_details(severity, details);
            assert!(
                result
                    .iter()
                    .any(|(k, v)| k.as_bytes() == b"RustWorkload" && v.as_bytes() == b"1")
            );
        }
    }

    #[test]
    fn error_severity_gets_both_markers() {
        let details: &[(&str, &str)] = &[];
        let result = prepare_trace_details(Severity::Error, details);
        assert!(
            result
                .iter()
                .any(|(k, v)| k.as_bytes() == b"RustWorkload" && v.as_bytes() == b"1")
        );
        assert!(
            result
                .iter()
                .any(|(k, v)| k.as_bytes() == b"RustFailure" && v.as_bytes() == b"1")
        );
    }

    #[test]
    fn caller_lowercase_rust_workload_key_is_not_duplicated() {
        let details: &[(&str, &str)] = &[("rustWorkload", "custom")];
        let result = prepare_trace_details(Severity::Info, details);
        let matching: Vec<_> = result
            .iter()
            .filter(|(k, _)| k.as_bytes() == b"RustWorkload")
            .collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].1.as_bytes(), b"custom");
    }

    #[test]
    fn caller_capitalized_rust_workload_key_is_not_duplicated() {
        let details: &[(&str, &str)] = &[("RustWorkload", "custom")];
        let result = prepare_trace_details(Severity::Info, details);
        let matching: Vec<_> = result
            .iter()
            .filter(|(k, _)| k.as_bytes() == b"RustWorkload")
            .collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].1.as_bytes(), b"custom");
    }

    #[test]
    fn capitalize_first_byte_uppercases_lowercase_first_byte() {
        assert_eq!(capitalize_first_byte(b"event".to_vec()), b"Event".to_vec());
    }

    #[test]
    fn capitalize_first_byte_leaves_already_capitalized_name_unchanged() {
        assert_eq!(capitalize_first_byte(b"Event".to_vec()), b"Event".to_vec());
    }

    #[test]
    fn capitalize_first_byte_leaves_empty_name_unchanged() {
        assert_eq!(capitalize_first_byte(Vec::new()), Vec::new());
    }
}
