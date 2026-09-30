use std::panic;

#[test]
fn test_run() {
    let old = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let mut db = None;
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        // Run the foundationdb client API
        let _drop_me = foundationdb::boot().expect("failed to initialize FoundationDB");
        db = Some(futures::executor::block_on(foundationdb::Database::new_compat(None)).unwrap());
        // Try to escape via unwind
        panic!("UNWIND!")
    }));
    assert!(result.is_err());
    // The boot guard is inert: the network survives the unwind and the database
    // is still usable (see issue #132).
    let trx = db.unwrap().create_trx().unwrap();
    futures::executor::block_on(trx.get_read_version())
        .expect("the network must survive an unwind past the boot guard");
    panic::set_hook(old);
}
