pub mod endpoints;
pub mod fdbserver;
pub mod flow;
pub mod services;

// *sigh* want this to be encapsulated down inside endpoints; such is life.
// #[allow(non_snake_case)]
#[allow(dead_code, unused_imports)]
#[path = "../target/flatbuffers/common_generated.rs"]
mod common_generated;

#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        include!("foundationdb/src/ffi.h");
        type RegisterWorkerRequest;

        fn hello_world(bytes: &[u8]) -> UniquePtr<RegisterWorkerRequest>;
    }


    // fn deserialize_client_worker_interface(bytes: &[u8]) -> UniquePtr<ClientWorkerInterface>;
}


#[test]
fn test_ffi_hello_world() {
    let v = ffi::hello_world("hello world".as_bytes());
}