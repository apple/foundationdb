use cxx::CxxString;
use std::ffi::CStr;
use std::pin::Pin;

#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("fdbclient/Notified.h");
        type NotifiedVersion;
        fn set(self: Pin<&mut NotifiedVersion>, val: &i64);
    }
    #[namespace = "fdbclient"]
    extern "Rust" {
       unsafe  fn set_version_to_42(notified_version: *mut NotifiedVersion);
    }
}

fn unchecked_pin<T>(t : *mut T) -> Pin<&'static mut T> {
    unsafe { Pin::new_unchecked(&mut *t) }
}

pub fn set_version_to_42(notified_version: *mut ffi::NotifiedVersion) {
    unchecked_pin(notified_version).set(&42);
}