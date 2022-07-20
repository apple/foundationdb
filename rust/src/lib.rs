#[cxx::bridge]
mod ffi {
    #[namespace = "shared"]
    struct Foo {
        i: u32,
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
    println!("Foo: {}", foo.i);
}
