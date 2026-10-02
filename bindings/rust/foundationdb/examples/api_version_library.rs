//! Shared-library fixture for the API-selection integration test.

use foundationdb::{
    Database,
    api::FdbApiBuilder,
    options::{MutationType, TransactionOption},
    tuple::{Subspace, Versionstamp},
};
use foundationdb_sys::FDBDatabase;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;

#[unsafe(no_mangle)]
pub extern "C" fn test_select_api(version: i32) -> i32 {
    match FdbApiBuilder::default()
        .set_runtime_version(version)
        .build()
    {
        Ok(_) => 0,
        Err(error) => error.code(),
    }
}

/// Queue modern versionstamp mutations using this library's API-selection state.
///
/// # Safety
///
/// `database` must point to a live C API database with the network running. The
/// caller must keep it alive and must not access it until this call returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn test_versionstamped_mutations(database: *mut FDBDatabase) -> i32 {
    // The harness retains ownership. This wrapper is used exclusively for this
    // call, and all transactions are destroyed before returning the borrow.
    let database = ManuallyDrop::new(unsafe {
        Database::new_from_pointer(NonNull::new(database).expect("database pointer"))
    });
    let result = futures::executor::block_on(async {
        let trx = database.create_trx()?;
        trx.set_option(TransactionOption::Timeout(10_000))?;
        trx.get_read_version().await?;

        let subspace = Subspace::from("test-api-version-library");
        let key = subspace.pack_with_versionstamp(&("key", Versionstamp::incomplete(0)));
        let value = subspace.pack_with_versionstamp(&("value", Versionstamp::incomplete(1)));
        trx.atomic_op(&key, b"value", MutationType::SetVersionstampedKey);
        trx.atomic_op(
            &subspace.pack(&"value"),
            &value,
            MutationType::SetVersionstampedValue,
        );
        Ok::<_, foundationdb::FdbError>(())
    });
    match result {
        Ok(()) => 0,
        Err(error) => error.code(),
    }
}
