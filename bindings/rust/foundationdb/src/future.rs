// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Most functions in the FoundationDB API are asynchronous, meaning that they
//! may return to the caller before actually delivering their Fdbresult.
//!
//! These functions always return FDBFuture*. An FDBFuture object represents a
//! Fdbresult value or error to be delivered at some future time. You can wait for
//! a Future to be “ready” – to have a value or error delivered – by setting a
//! callback function, or by blocking a thread, or by polling. Once a Future is
//! ready, you can extract either an error code or a value of the appropriate
//! type (the documentation for the original function will tell you which
//! fdb_future_get_*() function you should call).
//!
//! Futures make it easy to do multiple operations in parallel, by calling several
//! asynchronous functions before waiting for any of the Fdbresults. This can be
//! important for reducing the latency of transactions.
//!

use std::convert::TryFrom;
use std::ffi::CStr;
use std::fmt;
use std::ops::Deref;
use std::os::raw::c_char;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;

#[cfg_api_versions(min = 700)]
pub use crate::fdb_keys::FdbKeys;
use crate::from_raw_fdb_slice;
#[cfg_api_versions(min = 710)]
pub use crate::mapped_key_values::MappedKeyValues;
use fdb_sys::if_cfg_api_versions;
use foundationdb_macros::cfg_api_versions;
use foundationdb_sys as fdb_sys;
use futures::prelude::*;
use futures::task::{AtomicWaker, Context, Poll};

use crate::{FdbError, FdbResult, error};

/// An opaque type that represents a Future in the FoundationDB C API.
pub struct FdbFutureHandle(NonNull<fdb_sys::FDBFuture>);

impl FdbFutureHandle {
    pub const fn as_ptr(&self) -> *mut fdb_sys::FDBFuture {
        self.0.as_ptr()
    }
}
unsafe impl Sync for FdbFutureHandle {}
unsafe impl Send for FdbFutureHandle {}
impl Drop for FdbFutureHandle {
    fn drop(&mut self) {
        // `fdb_future_destroy` cancels the future, so we don't need to call
        // `fdb_future_cancel` explicitly.
        unsafe { fdb_sys::fdb_future_destroy(self.as_ptr()) }
    }
}

/// An opaque type that represents a pending Future that will be converted to a
/// predefined result type.
///
/// Non owned result type (Fdb
pub struct FdbFuture<T> {
    f: Option<FdbFutureHandle>,
    waker: Option<Arc<AtomicWaker>>,
    phantom: std::marker::PhantomData<T>,
}

impl<T> FdbFuture<T>
where
    T: TryFrom<FdbFutureHandle, Error = FdbError> + Unpin,
{
    pub fn new(f: *mut fdb_sys::FDBFuture) -> Self {
        Self {
            f: Some(FdbFutureHandle(
                NonNull::new(f).expect("FDBFuture to not be null"),
            )),
            waker: None,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Future for FdbFuture<T>
where
    T: TryFrom<FdbFutureHandle, Error = FdbError> + Unpin,
{
    type Output = FdbResult<T>;

    #[cfg_attr(
        feature = "trace",
        tracing::instrument(level = "debug", skip(self, cx))
    )]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<FdbResult<T>> {
        let f = self.f.as_ref().expect("cannot poll after resolve");
        let ready = unsafe { fdb_sys::fdb_future_is_ready(f.as_ptr()) };
        if ready == 0 {
            let f_ptr = f.as_ptr();
            let mut register = false;
            let waker = self.waker.get_or_insert_with(|| {
                register = true;
                Arc::new(AtomicWaker::new())
            });
            waker.register(cx.waker());
            if register {
                let network_waker: Arc<AtomicWaker> = waker.clone();
                let network_waker_ptr = Arc::into_raw(network_waker);
                unsafe {
                    fdb_sys::fdb_future_set_callback(
                        f_ptr,
                        Some(fdb_future_callback),
                        network_waker_ptr as *mut _,
                    );
                }
            }
            Poll::Pending
        } else {
            Poll::Ready(
                error::eval(unsafe { fdb_sys::fdb_future_get_error(f.as_ptr()) })
                    .and_then(|()| T::try_from(self.f.take().expect("self.f.is_some()"))),
            )
        }
    }
}

pub static CUSTOM_EXECUTOR_HOOK: std::sync::OnceLock<fn()> = std::sync::OnceLock::new();

// The callback from fdb C API can be called from multiple threads. so this callback should be
// thread-safe.
extern "C" fn fdb_future_callback(
    _f: *mut fdb_sys::FDBFuture,
    callback_parameter: *mut ::std::os::raw::c_void,
) {
    // resolve all wakeup chain
    let network_waker: Arc<AtomicWaker> = unsafe { Arc::from_raw(callback_parameter as *const _) };
    network_waker.wake();

    // polling phase for a custom executor (e.g. the simulation)
    if let Some(poll_pending_tasks) = CUSTOM_EXECUTOR_HOOK.get() {
        poll_pending_tasks();
    }
}

/// A slice of bytes owned by a foundationDB future
pub struct FdbSlice {
    _f: FdbFutureHandle,
    value: *const u8,
    len: i32,
}
unsafe impl Sync for FdbSlice {}
unsafe impl Send for FdbSlice {}

impl Deref for FdbSlice {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        from_raw_fdb_slice(self.value, self.len as usize)
    }
}
impl AsRef<[u8]> for FdbSlice {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl TryFrom<FdbFutureHandle> for FdbSlice {
    type Error = FdbError;

    fn try_from(f: FdbFutureHandle) -> FdbResult<Self> {
        let mut value = std::ptr::null();
        let mut len = 0;

        error::eval(unsafe { fdb_sys::fdb_future_get_key(f.as_ptr(), &mut value, &mut len) })?;

        Ok(FdbSlice { _f: f, value, len })
    }
}

impl TryFrom<FdbFutureHandle> for Option<FdbSlice> {
    type Error = FdbError;

    fn try_from(f: FdbFutureHandle) -> FdbResult<Self> {
        let mut present = 0;
        let mut value = std::ptr::null();
        let mut len = 0;

        error::eval(unsafe {
            fdb_sys::fdb_future_get_value(f.as_ptr(), &mut present, &mut value, &mut len)
        })?;

        Ok(if present == 0 {
            None
        } else {
            Some(FdbSlice { _f: f, value, len })
        })
    }
}

/// A slice of addresses owned by a foundationDB future
pub struct FdbAddresses {
    _f: FdbFutureHandle,
    strings: *const FdbAddress,
    len: i32,
}
unsafe impl Sync for FdbAddresses {}
unsafe impl Send for FdbAddresses {}

impl TryFrom<FdbFutureHandle> for FdbAddresses {
    type Error = FdbError;

    fn try_from(f: FdbFutureHandle) -> FdbResult<Self> {
        let mut strings: *mut *const c_char = std::ptr::null_mut();
        let mut len = 0;

        error::eval(unsafe {
            fdb_sys::fdb_future_get_string_array(f.as_ptr(), &mut strings, &mut len)
        })?;

        Ok(FdbAddresses {
            _f: f,
            strings: strings as *const FdbAddress,
            len,
        })
    }
}

impl Deref for FdbAddresses {
    type Target = [FdbAddress];

    fn deref(&self) -> &Self::Target {
        assert_eq_size!(FdbAddress, *const c_char);
        assert_eq_align!(FdbAddress, u8);
        from_raw_fdb_slice(self.strings, self.len as usize)
    }
}
impl AsRef<[FdbAddress]> for FdbAddresses {
    fn as_ref(&self) -> &[FdbAddress] {
        self.deref()
    }
}

/// An address owned by a foundationDB future
///
/// Because the data it represent is owned by the future in FdbAddresses, you
/// can never own a FdbAddress directly, you can only have references to it.
/// This way, you can never obtain a lifetime greater than the lifetime of the
/// slice that gave you access to it.
#[repr(C, packed)]
pub struct FdbAddress {
    c_str: *const c_char,
}

impl Deref for FdbAddress {
    type Target = CStr;

    fn deref(&self) -> &CStr {
        unsafe { std::ffi::CStr::from_ptr(self.c_str) }
    }
}
impl AsRef<CStr> for FdbAddress {
    fn as_ref(&self) -> &CStr {
        self.deref()
    }
}

/// An slice of keyvalues owned by a foundationDB future
pub struct FdbValues {
    _f: FdbFutureHandle,
    keyvalues: *const FdbKeyValue,
    len: i32,
    more: bool,
}
unsafe impl Sync for FdbValues {}
unsafe impl Send for FdbValues {}

impl FdbValues {
    /// `true` if there is another range after this one
    pub fn more(&self) -> bool {
        self.more
    }
}

impl TryFrom<FdbFutureHandle> for FdbValues {
    type Error = FdbError;
    fn try_from(f: FdbFutureHandle) -> FdbResult<Self> {
        let mut keyvalues = std::ptr::null();
        let mut len = 0;
        let mut more = 0;

        unsafe {
            error::eval(fdb_sys::fdb_future_get_keyvalue_array(
                f.as_ptr(),
                &mut keyvalues,
                &mut len,
                &mut more,
            ))?
        }

        Ok(FdbValues {
            _f: f,
            keyvalues: keyvalues as *const FdbKeyValue,
            len,
            more: more != 0,
        })
    }
}

impl Deref for FdbValues {
    type Target = [FdbKeyValue];
    fn deref(&self) -> &Self::Target {
        assert_eq_size!(FdbKeyValue, fdb_sys::FDBKeyValue);
        assert_eq_align!(FdbKeyValue, u8);

        from_raw_fdb_slice(self.keyvalues, self.len as usize)
    }
}
impl AsRef<[FdbKeyValue]> for FdbValues {
    fn as_ref(&self) -> &[FdbKeyValue] {
        self.deref()
    }
}

impl<'a> IntoIterator for &'a FdbValues {
    type Item = &'a FdbKeyValue;
    type IntoIter = std::slice::Iter<'a, FdbKeyValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.deref().iter()
    }
}
impl IntoIterator for FdbValues {
    type Item = FdbValue;
    type IntoIter = FdbValuesIter;

    fn into_iter(self) -> Self::IntoIter {
        FdbValuesIter {
            f: Arc::new(self._f),
            keyvalues: self.keyvalues,
            len: self.len,
            pos: 0,
        }
    }
}

/// An iterator of keyvalues owned by a foundationDB future
pub struct FdbValuesIter {
    f: Arc<FdbFutureHandle>,
    keyvalues: *const FdbKeyValue,
    len: i32,
    pos: i32,
}

unsafe impl Send for FdbValuesIter {}

impl Iterator for FdbValuesIter {
    type Item = FdbValue;
    fn next(&mut self) -> Option<Self::Item> {
        #[allow(clippy::iter_nth_zero)]
        self.nth(0)
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let pos = (self.pos as usize).checked_add(n);
        match pos {
            Some(pos) if pos < self.len as usize => {
                // safe because pos < self.len
                let keyvalue = unsafe { self.keyvalues.add(pos) };
                self.pos = pos as i32 + 1;

                Some(FdbValue {
                    _f: self.f.clone(),
                    keyvalue,
                })
            }
            _ => {
                self.pos = self.len;
                None
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = (self.len - self.pos) as usize;
        (rem, Some(rem))
    }
}
impl ExactSizeIterator for FdbValuesIter {
    #[inline]
    fn len(&self) -> usize {
        (self.len - self.pos) as usize
    }
}
impl DoubleEndedIterator for FdbValuesIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.nth_back(0)
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        if n < self.len() {
            self.len -= 1 + n as i32;
            // safe because len < original len
            let keyvalue = unsafe { self.keyvalues.add(self.len as usize) };
            Some(FdbValue {
                _f: self.f.clone(),
                keyvalue,
            })
        } else {
            self.pos = self.len;
            None
        }
    }
}

/// A keyvalue you can own
///
/// Until dropped, this might prevent multiple key/values from beeing freed.
/// (i.e. the future that own the data is dropped once all data it provided is dropped)
pub struct FdbValue {
    _f: Arc<FdbFutureHandle>,
    keyvalue: *const FdbKeyValue,
}

unsafe impl Send for FdbValue {}

impl Deref for FdbValue {
    type Target = FdbKeyValue;
    fn deref(&self) -> &Self::Target {
        assert_eq_size!(FdbKeyValue, fdb_sys::FDBKeyValue);
        assert_eq_align!(FdbKeyValue, u8);
        unsafe { &*self.keyvalue }
    }
}
impl AsRef<FdbKeyValue> for FdbValue {
    fn as_ref(&self) -> &FdbKeyValue {
        self.deref()
    }
}
impl PartialEq for FdbValue {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl Eq for FdbValue {}
impl fmt::Debug for FdbValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.deref().fmt(f)
    }
}

/// A keyvalue owned by a foundationDB future
///
/// Because the data it represent is owned by the future in FdbValues, you
/// can never own a FdbKeyValue directly, you can only have references to it.
/// This way, you can never obtain a lifetime greater than the lifetime of the
/// slice that gave you access to it.
#[repr(C, packed)]
pub struct FdbKeyValue(fdb_sys::FDBKeyValue);

impl FdbKeyValue {
    /// key
    pub fn key(&self) -> &[u8] {
        // This cast to `*const u8` isn't unnecessary in all configurations.
        #[allow(clippy::unnecessary_cast)]
        from_raw_fdb_slice(self.0.key as *const u8, self.0.key_length as usize)
    }

    /// value
    pub fn value(&self) -> &[u8] {
        // This cast to `*const u8` isn't unnecessary in all configurations.
        #[allow(clippy::unnecessary_cast)]
        from_raw_fdb_slice(self.0.value as *const u8, self.0.value_length as usize)
    }
}

impl PartialEq for FdbKeyValue {
    fn eq(&self, other: &Self) -> bool {
        (self.key(), self.value()) == (other.key(), other.value())
    }
}
impl Eq for FdbKeyValue {}
impl fmt::Debug for FdbKeyValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "({:?}, {:?})",
            crate::tuple::Bytes::from(self.key()),
            crate::tuple::Bytes::from(self.value())
        )
    }
}

impl TryFrom<FdbFutureHandle> for i64 {
    type Error = FdbError;

    fn try_from(f: FdbFutureHandle) -> FdbResult<Self> {
        let mut version: i64 = 0;
        error::eval(unsafe {
            if_cfg_api_versions!(min = 620 => {
                fdb_sys::fdb_future_get_int64(f.as_ptr(), &mut version)
            } else {
                fdb_sys::fdb_future_get_version(f.as_ptr(), &mut version)
            })
        })?;
        Ok(version)
    }
}

impl TryFrom<FdbFutureHandle> for () {
    type Error = FdbError;
    fn try_from(_f: FdbFutureHandle) -> FdbResult<Self> {
        Ok(())
    }
}
