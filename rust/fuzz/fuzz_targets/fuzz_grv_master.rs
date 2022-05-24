#![no_main]
use libfuzzer_sys::fuzz_target;

// use foundationdb::fdbserver::GrvMaster;

fuzz_target!(|data: &[u8]| {
    ()
});