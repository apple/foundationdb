use cxx::CxxString;
use std::ffi::CStr;
use std::pin::Pin;

#[cxx::bridge]
mod ffi {
    #[namespace = "shared"]
    struct Foo {
        i: u32,
    }

    #[derive(Debug)]
    struct UID {
        part: [u64; 2],
    }

    unsafe extern "C++" {
        include!("flow/IRandom.h");
        
        type UID;

        // fn toString(self: &UID) -> CxxString;
        // fn shortString(self: &UID) -> CxxString;
        fn isValid(self: &UID) -> bool;
        fn compare(self: &UID, r: &UID) -> i32;
        fn first(self: &UID) -> u64;
        fn second(self: &UID) -> u64;
        // Not sure how to represent static methods yet
        // fn fromString(&CxxString) -> UID;
        // fn fromStringThrowsOnFailure(&CxxString) -> UID;

        type IRandom;
        fn randomUniqueID(self: Pin<&mut IRandom>) -> UID;
        fn deterministicRandom() -> *mut IRandom;

        include!("flow/SourceVersion.h");
        fn getSourceVersion() -> *const c_char;

        // Breaks C++ build; fdbclient isn't available from the fdbrpc cmake subdirectory.
        // include!("fdbclient/Notified.h");
        // type NotifiedVersion;
        // fn set(self: Pin<&mut NotifiedVersion>, val: &i64);


    }
    #[namespace = "flow"]
    extern "Rust" {
        fn new_foo() -> Foo;
        fn print_foo(foo: &Foo);
    }
}

// TODO: Some sort of pin project, maybe?  (Go from *mut T -> Pin<*mut T>, then repeatedly to Pin<&mut T>?)
fn unchecked_pin<T>(t : *mut T) -> Pin<&'static mut T> {
    unsafe { Pin::new_unchecked(&mut *t) }
}

pub fn new_foo() -> ffi::Foo {
    ffi::Foo { i: 42 }
}

pub fn print_foo(foo: &ffi::Foo) {
    let ver_char_ptr = ffi::getSourceVersion();
    let ver_c_str = unsafe { CStr::from_ptr(ver_char_ptr) };
    let ver_str = ver_c_str.to_str().unwrap();

    println!("FDB git SHA: {}", ver_str);
    println!("Foo: {};", foo.i);

    let rand = ffi::deterministicRandom();
    let uid = unchecked_pin(rand).randomUniqueID();
    let uid2 = unchecked_pin(rand).randomUniqueID();
    // let uid = unsafe { Pin::new_unchecked(&mut *rand) }.randomUniqueID();

    println!("Got UID: {:x} {:x} ({:x?}) ({:x?})", uid.first(), uid.second(), uid, uid2);
}
