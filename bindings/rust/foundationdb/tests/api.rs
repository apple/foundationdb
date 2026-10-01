use foundationdb::api::{self, FdbApiBuilder};
use foundationdb::options::NetworkOption;

// The whole lifecycle in a single #[test]: `stop_network` is terminal for the
// process, so a second test in this binary would race it.
#[test]
fn test_network_lifecycle() {
    // Integer options use the C API's full signed 64-bit domain. These values
    // must reach the client without truncating to a negative 32-bit integer.
    FdbApiBuilder::default()
        .build()
        .expect("failed to select fdb api")
        .set_option(NetworkOption::TraceRollSize(3 * 1024 * 1024 * 1024))
        .expect("trace roll size above i32::MAX must be accepted")
        .set_option(NetworkOption::TraceMaxLogsSize(i64::MAX))
        .expect("maximum signed 64-bit trace log size must be accepted");

    // boot is safe and idempotent
    let _guard1 = foundationdb::boot().expect("failed to boot fdb");
    let _guard2 = foundationdb::boot().expect("boot must be idempotent");

    // re-selecting the same api version is Ok, a different one fails with 2201
    let builder = FdbApiBuilder::default()
        .build()
        .expect("re-selecting the same api version must be Ok");
    let err = FdbApiBuilder::default()
        .set_runtime_version(300)
        .build()
        .map(drop)
        .expect_err("selecting another api version must fail");
    assert_eq!(err.code(), 2201);

    // network options are rejected once the network is running
    let err = builder
        .set_option(NetworkOption::TraceEnable(String::new()))
        .map(drop)
        .expect_err("setting a network option after boot must fail");
    assert_eq!(err.code(), 2009);

    // the network event loop reported no error
    assert!(api::network_run_error().is_none());

    // explicit stop: Ok, idempotent, terminal
    api::stop_network().expect("failed to stop the network");
    api::stop_network().expect("stop_network must be idempotent");
    let err = foundationdb::boot()
        .map(drop)
        .expect_err("boot after stop must fail");
    assert_eq!(err.code(), 2025);
    let err = futures::executor::block_on(foundationdb::Database::new_compat(None))
        .map(drop)
        .expect_err("creating a database after stop must fail");
    assert_eq!(err.code(), 2025);
    // the guards drop here: they are inert, and the atexit hook then finds the
    // network already stopped and does nothing
}
