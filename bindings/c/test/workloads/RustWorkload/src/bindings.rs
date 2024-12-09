use std::ffi::{CStr, CString};
use std::str::FromStr;

mod raw_bindings {
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
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

// -----------------------------------------------------------------------------
// String conversions

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn str_from_c(c_buf: *const i8) -> String {
    let c_str = unsafe { CStr::from_ptr(c_buf) };
    c_str.to_str().unwrap().to_string()
}
pub fn str_for_c<T>(buf: T) -> CString
where
    T: Into<Vec<u8>>,
{
    CString::new(buf).unwrap()
}

// -----------------------------------------------------------------------------
// Rust Types

/// Wrapper around a FoundationDB simulation context
pub struct WorkloadContext(FDBWorkloadContext);
/// Wrapper around a FoundationDB promise
pub struct Promise(FDBPromise);
/// Wrapper around a FoundationDB metric sink
pub struct Metrics(FDBMetrics);

/// A single metric entry
#[derive(Clone, Copy)]
pub struct Metric<'a> {
    /// The name of the metric
    pub key: &'a str,
    /// The value of the metric
    pub val: f64,
    /// Specifies whether the metric value should be aggregated as a sum or an average
    pub avg: bool,
    /// C++ string formatter of the metric
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
    /// error, this severity automatically breaks execution
    Error = FDBSeverity_FDBSeverity_Error,
}

// -----------------------------------------------------------------------------
// Implementations

impl WorkloadContext {
    pub fn new(raw: FDBWorkloadContext) -> Self {
        Self(raw)
    }
    /// Add a log entry in the FoundationDB logs
    pub fn trace<S>(&self, severity: Severity, name: S, details: &[(&str, &str)])
    where
        S: Into<Vec<u8>>,
    {
        let name = str_for_c(name);
        let details_storage = details
            .iter()
            .map(|(key, val)| {
                let key = str_for_c(*key);
                let val = str_for_c(*val);
                (key, val)
            })
            .collect::<Vec<_>>();
        let details = details_storage
            .iter()
            .map(|(key, val)| FDBStringPair {
                key: key.as_ptr(),
                val: val.as_ptr(),
            })
            .collect::<Vec<_>>();
        unsafe {
            self.0.trace.unwrap_unchecked()(
                self.0.inner,
                severity as FDBSeverity,
                name.as_ptr(),
                details.as_ptr(),
                details.len() as i32,
            )
        }
    }
    /// Get the process id of the workload
    pub fn get_process_id(&self) -> u64 {
        unsafe { self.0.getProcessID.unwrap_unchecked()(self.0.inner) }
    }
    /// Set the process id of the workload
    pub fn set_process_id(&self, id: u64) {
        unsafe { self.0.setProcessID.unwrap_unchecked()(self.0.inner, id) }
    }
    /// Get the current simulated time in seconds (starts at zero)
    pub fn now(&self) -> f64 {
        unsafe { self.0.now.unwrap_unchecked()(self.0.inner) }
    }
    /// Get a determinist 32-bit random number
    pub fn rnd(&self) -> u32 {
        unsafe { self.0.rnd.unwrap_unchecked()(self.0.inner) }
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
        let raw_value = unsafe {
            self.0.getOption.unwrap_unchecked()(self.0.inner, name.as_ptr(), default_value.as_ptr())
        };
        let value = str_from_c(raw_value.inner);
        unsafe { raw_value.free.unwrap_unchecked()(raw_value.inner) };
        if value == null {
            None
        } else {
            Some(value)
        }
    }
    /// Get the client id of the workload
    pub fn client_id(&self) -> i32 {
        unsafe { self.0.clientId.unwrap_unchecked()(self.0.inner) }
    }
    /// Get the number of clients of the workload
    pub fn client_count(&self) -> i32 {
        unsafe { self.0.clientCount.unwrap_unchecked()(self.0.inner) }
    }
    /// Get a determinist 64-bit random number
    pub fn shared_random_number(&self) -> i64 {
        unsafe { self.0.sharedRandomNumber.unwrap_unchecked()(self.0.inner) }
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
        unsafe { self.0.send.unwrap_unchecked()(self.0.inner, value) };
    }
}
impl Drop for Promise {
    fn drop(&mut self) {
        unsafe { self.0.free.unwrap_unchecked()(self.0.inner) };
    }
}

impl Metrics {
    pub(crate) fn new(raw: FDBMetrics) -> Self {
        Self(raw)
    }
    /// Call `reserve` on the underlying C++ vector of metrics
    pub fn reserve(&mut self, n: usize) {
        unsafe { self.0.reserve.unwrap_unchecked()(self.0.inner, n as i32) }
    }
    /// Add a single metric to the sink
    pub fn push(&mut self, metric: Metric) {
        let key_storage = str_for_c(metric.key);
        let fmt_storage = str_for_c(metric.fmt.as_deref().unwrap_or("%.3g"));
        unsafe {
            self.0.push.unwrap_unchecked()(
                self.0.inner,
                FDBMetric {
                    key: key_storage.as_ptr(),
                    fmt: fmt_storage.as_ptr(),
                    val: metric.val,
                    avg: metric.avg,
                },
            )
        }
    }
    /// Add multiple metrics, ensuring the sink reallocates at most once
    pub fn extend(&mut self, metrics: &[Metric]) {
        self.reserve(metrics.len());
        for metric in metrics {
            self.push(*metric);
        }
    }
}

impl<'a> Metric<'a> {
    /// Create a summed metric entry
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
    /// Create an averaged metric entry
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
