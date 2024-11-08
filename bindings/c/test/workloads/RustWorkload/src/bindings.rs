mod raw_bindings {
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
use raw_bindings::*;
pub use raw_bindings::{
    BridgeToClient, BridgeToServer, BridgeToServer_ContextImpl, BridgeToServer_PromiseImpl,
    CVector, FDBDatabase, FDBPromise, FDBWorkload, FDBWorkloadContext,
};

use std::{
    ffi::{CStr, CString},
    os::raw::c_char,
    str::FromStr,
};

// -----------------------------------------------------------------------------
// String conversions

#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub(crate) fn str_from_c(c_buf: *const c_char) -> String {
    let c_str = unsafe { CStr::from_ptr(c_buf) };
    c_str.to_str().unwrap().to_string()
}
pub(crate) fn str_for_c<T>(buf: T) -> CString
where
    T: Into<Vec<u8>>,
{
    CString::new(buf).unwrap()
}

// -----------------------------------------------------------------------------
// Rust Types

/// A wrapper around a FoundationDB context
pub struct WorkloadContext {
    inner: *mut FDBWorkloadContext,
    bridge: BridgeToServer_ContextImpl,
}

/// A wrapper around a FoundationDB promise
pub struct Promise {
    inner: *mut FDBPromise,
    bridge: BridgeToServer_PromiseImpl,
}

/// A single metric entry
pub struct Metric {
    /// The name of the metric
    pub name: String,
    /// The value of the metric
    pub value: f64,
    /// Indicates if the value represents an average or not
    pub averaged: bool,
    /// C++ string formatter of the metric
    pub format_code: Option<String>,
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

impl WorkloadContext {
    pub(crate) fn new(inner: *mut FDBWorkloadContext, bridge: BridgeToServer_ContextImpl) -> Self {
        Self { inner, bridge }
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
            .map(|(key, val)| CStringPair {
                key: key.as_ptr(),
                val: val.as_ptr(),
            })
            .collect::<Vec<_>>();
        unsafe {
            self.bridge.trace.unwrap_unchecked()(
                self.inner,
                severity as FDBSeverity,
                name.as_ptr(),
                CVector {
                    elements: details.as_ptr() as *mut _,
                    n: details.len() as i32,
                },
            )
        }
    }
    /// Get the process id of the workload
    pub fn get_process_id(&self) -> u64 {
        unsafe { self.bridge.getProcessID.unwrap_unchecked()(self.inner) }
    }
    /// Set the process id of the workload
    pub fn set_process_id(&self, id: u64) {
        unsafe { self.bridge.setProcessID.unwrap_unchecked()(self.inner, id) }
    }
    /// Get the current time
    pub fn now(&self) -> f64 {
        unsafe { self.bridge.now.unwrap_unchecked()(self.inner) }
    }
    /// Get a determinist 32-bit random number
    pub fn rnd(&self) -> u32 {
        unsafe { self.bridge.rnd.unwrap_unchecked()(self.inner) }
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
        let null = "null";
        let name = str_for_c(name);
        let default_value = str_for_c(null);
        let value_ptr = unsafe {
            self.bridge.getOption.unwrap_unchecked()(
                self.inner,
                name.as_ptr(),
                default_value.as_ptr(),
            )
        };
        let value = str_from_c(value_ptr);
        // FIXME: value leak
        if value == null {
            None
        } else {
            Some(value)
        }
    }
    /// Get the client id of the workload
    pub fn client_id(&self) -> i32 {
        unsafe { self.bridge.clientId.unwrap_unchecked()(self.inner) }
    }
    /// Get the client id of the workload
    pub fn client_count(&self) -> i32 {
        unsafe { self.bridge.clientCount.unwrap_unchecked()(self.inner) }
    }
    /// Get a determinist 64-bit random number
    pub fn shared_random_number(&self) -> i64 {
        unsafe { self.bridge.sharedRandomNumber.unwrap_unchecked()(self.inner) }
    }
}

impl Promise {
    pub(crate) fn new(inner: *mut FDBPromise, bridge: BridgeToServer_PromiseImpl) -> Self {
        Self { inner, bridge }
    }
    /// Resolve a FoundationDB promise by setting its value to a boolean.
    /// You can resolve a Promise only once.
    ///
    /// note: FoundationDB disregards the value sent, so sending `true` or `false` is equivalent
    pub fn send(self, value: bool) {
        unsafe { self.bridge.send.unwrap_unchecked()(self.inner, value) };
    }
}
impl Drop for Promise {
    fn drop(&mut self) {
        unsafe { self.bridge.free.unwrap_unchecked()(self.inner) };
    }
}

impl Metric {
    /// Create a metric value entry
    pub fn val<S, V>(name: S, value: V) -> Self
    where
        S: Into<String>,
        f64: From<V>,
    {
        Self {
            name: name.into(),
            value: value.into(),
            averaged: false,
            format_code: None,
        }
    }
    /// Create a metric average entry
    pub fn avg<S, V>(name: S, value: V) -> Self
    where
        S: Into<String>,
        V: TryInto<f64>,
    {
        Self {
            name: name.into(),
            value: value.try_into().ok().expect("convertion failed"),
            averaged: true,
            format_code: None,
        }
    }
}
