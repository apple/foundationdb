use std::ffi::CStr;

#[cxx::bridge]
mod ffi {
    #[namespace = "shared"]
    struct Foo {
        i: u32,
    }

    unsafe extern "C++" {
        include!("flow/SourceVersion.h");
        //const char* getSourceVersion()
        fn getSourceVersion() -> * const c_char;
    }
    #[namespace = "fdb_rust"]
    extern "Rust" {
        fn new_foo() -> Foo;
        fn print_foo(foo: &Foo);
    }   
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
}
