// Copyright 2022 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Definitions of FDBKeys, used in api version 700 and more.

use crate::error;
use crate::from_raw_fdb_slice;
use crate::future::FdbFutureHandle;
use crate::{FdbError, FdbResult};
use foundationdb_sys as fdb_sys;
use std::fmt;
use std::ops::Deref;

/// An slice of keys owned by a FoundationDB future
pub struct FdbKeys {
    _f: FdbFutureHandle,
    keys: *const FdbKey,
    len: i32,
}
unsafe impl Sync for FdbKeys {}
unsafe impl Send for FdbKeys {}

impl TryFrom<FdbFutureHandle> for FdbKeys {
    type Error = FdbError;

    fn try_from(f: FdbFutureHandle) -> FdbResult<Self> {
        let mut keys = std::ptr::null();
        let mut len = 0;

        error::eval(unsafe { fdb_sys::fdb_future_get_key_array(f.as_ptr(), &mut keys, &mut len) })?;

        Ok(FdbKeys {
            _f: f,
            keys: keys as *const FdbKey,
            len,
        })
    }
}

impl Deref for FdbKeys {
    type Target = [FdbKey];
    fn deref(&self) -> &Self::Target {
        assert_eq_size!(FdbKey, fdb_sys::FDBKey);
        assert_eq_align!(FdbKey, u8);
        from_raw_fdb_slice(self.keys, self.len as usize)
    }
}

impl AsRef<[FdbKey]> for FdbKeys {
    fn as_ref(&self) -> &[FdbKey] {
        self.deref()
    }
}

impl<'a> IntoIterator for &'a FdbKeys {
    type Item = &'a FdbKey;
    type IntoIter = std::slice::Iter<'a, FdbKey>;

    fn into_iter(self) -> Self::IntoIter {
        self.deref().iter()
    }
}

/// An iterator of keyvalues owned by a foundationDB future
pub struct FdbKeysIter {
    f: std::rc::Rc<FdbFutureHandle>,
    keys: *const FdbKey,
    len: i32,
    pos: i32,
}

impl Iterator for FdbKeysIter {
    type Item = FdbRowKey;
    fn next(&mut self) -> Option<Self::Item> {
        #[allow(clippy::iter_nth_zero)]
        self.nth(0)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = (self.len - self.pos) as usize;
        (rem, Some(rem))
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let pos = (self.pos as usize).checked_add(n);
        match pos {
            Some(pos) if pos < self.len as usize => {
                // safe because pos < self.len
                let row_key = unsafe { self.keys.add(pos) };
                self.pos = pos as i32 + 1;

                Some(FdbRowKey {
                    _f: self.f.clone(),
                    row_key,
                })
            }
            _ => {
                self.pos = self.len;
                None
            }
        }
    }
}

impl IntoIterator for FdbKeys {
    type Item = FdbRowKey;
    type IntoIter = FdbKeysIter;

    fn into_iter(self) -> Self::IntoIter {
        FdbKeysIter {
            f: std::rc::Rc::new(self._f),
            keys: self.keys,
            len: self.len,
            pos: 0,
        }
    }
}
/// A row key you can own
///
/// Until dropped, this might prevent multiple key/values from beeing freed.
/// (i.e. the future that own the data is dropped once all data it provided is dropped)
pub struct FdbRowKey {
    _f: std::rc::Rc<FdbFutureHandle>,
    row_key: *const FdbKey,
}

impl Deref for FdbRowKey {
    type Target = FdbKey;
    fn deref(&self) -> &Self::Target {
        assert_eq_size!(FdbKey, fdb_sys::FDBKey);
        assert_eq_align!(FdbKey, u8);
        unsafe { &*(self.row_key) }
    }
}
impl AsRef<FdbKey> for FdbRowKey {
    fn as_ref(&self) -> &FdbKey {
        self.deref()
    }
}
impl PartialEq for FdbRowKey {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl Eq for FdbRowKey {}
impl fmt::Debug for FdbRowKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.deref().fmt(f)
    }
}

#[repr(C, packed)]
/// An FdbKey, owned by a FoundationDB Future
pub struct FdbKey(fdb_sys::FDBKey);

impl FdbKey {
    /// retrieves the associated key
    pub fn key(&self) -> &[u8] {
        from_raw_fdb_slice(self.0.key, self.0.key_length as usize)
    }
}

impl PartialEq for FdbKey {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for FdbKey {}

impl fmt::Debug for FdbKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({:?})", crate::tuple::Bytes::from(self.key()),)
    }
}
