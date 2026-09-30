// Copyright 2018 foundationdb-rs developers, https://github.com/Clikengo/foundationdb-rs/graphs/contributors
// Copyright 2013-2018 Apple, Inc and the FoundationDB project authors.
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! Implementations of the FDBCluster C API
//!
//! https://apple.github.io/foundationdb/api-c.html#cluster

use std::convert::TryFrom;
use std::future::Future;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::future::*;
use crate::{error, Database, FdbError, FdbResult};
use foundationdb_sys as fdb_sys;

/// An opaque type that represents a Cluster in the FoundationDB C API.
#[derive(Clone)]
pub struct Cluster {
    inner: Arc<ClusterHandle>,
}
struct ClusterHandle(NonNull<fdb_sys::FDBCluster>);
// The C API permits cross-thread use of cluster handles; Arc keeps the handle alive
// until the last Cluster clone is dropped.
unsafe impl Send for ClusterHandle {}
unsafe impl Sync for ClusterHandle {}
impl Drop for ClusterHandle {
    fn drop(&mut self) {
        unsafe {
            fdb_sys::fdb_cluster_destroy(self.0.as_ptr());
        }
    }
}

impl Cluster {
    pub fn new(
        path: Option<&str>,
    ) -> impl Future<Output = FdbResult<Cluster>> + Send + Sync + Unpin {
        let path_str = path.map(|path| std::ffi::CString::new(path).unwrap());
        let path_ptr = path_str
            .as_ref()
            .map(|path| path.as_ptr())
            .unwrap_or(std::ptr::null());
        let f = FdbFuture::new(unsafe { fdb_sys::fdb_create_cluster(path_ptr) });
        drop(path_str);
        f
    }

    /// Returns an FdbFuture which will be set to an FDBCluster object.
    ///
    /// # Arguments
    ///
    /// * `path` - A string giving a local path of a cluster file (often called ‘fdb.cluster’) which contains connection information for the FoundationDB cluster. See `foundationdb::default_config_path()`
    ///
    pub fn from_path(path: &str) -> impl Future<Output = FdbResult<Cluster>> + Send + Sync + Unpin {
        Self::new(Some(path))
    }

    pub fn default() -> impl Future<Output = FdbResult<Cluster>> {
        Self::new(None)
    }

    /// Returns an `FdbFuture` which will be set to an `Database` object.
    pub fn create_database(
        &self,
    ) -> impl Future<Output = FdbResult<Database>> + Send + Sync + Unpin {
        FdbFuture::new(unsafe {
            fdb_sys::fdb_cluster_create_database(self.inner.0.as_ptr(), b"DB" as *const _, 2)
        })
    }
}

impl TryFrom<FdbFutureHandle> for Cluster {
    type Error = FdbError;

    fn try_from(f: FdbFutureHandle) -> FdbResult<Self> {
        let mut v: *mut fdb_sys::FDBCluster = std::ptr::null_mut();
        error::eval(unsafe { fdb_sys::fdb_future_get_cluster(f.as_ptr(), &mut v) })?;

        Ok(Cluster {
            inner: Arc::new(ClusterHandle(NonNull::new(v).expect(
                "fdb_future_get_cluster to not return null if there is no error",
            ))),
        })
    }
}

impl TryFrom<FdbFutureHandle> for Database {
    type Error = FdbError;

    fn try_from(f: FdbFutureHandle) -> FdbResult<Self> {
        let mut v: *mut fdb_sys::FDBDatabase = std::ptr::null_mut();
        error::eval(unsafe { fdb_sys::fdb_future_get_database(f.as_ptr(), &mut v) })?;

        Ok(Database {
            inner: NonNull::new(v)
                .expect("fdb_future_get_database to not return null if there is no error"),
        })
    }
}
