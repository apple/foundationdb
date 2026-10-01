use foundationdb::{
    Database,
    api::FdbApiBuilder,
    options::{MutationType, TransactionOption},
    tuple::{self, Subspace, Versionstamp},
};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::process::Command;

#[test]
fn versionstamped_mutations_follow_selected_runtime_api() {
    const CHILD_API: &str = "FDB_TEST_VERSIONSTAMP_RUNTIME_API";
    let Ok(version) = std::env::var(CHILD_API) else {
        // The C API selection is process-global and irreversible. Exercise old
        // runtime semantics even when this binary was built with current headers.
        for version in [510, 520, 740] {
            if version > foundationdb_sys::FDB_API_VERSION {
                continue;
            }
            let status = Command::new(std::env::current_exe().expect("test executable"))
                .args([
                    "--exact",
                    "versionstamped_mutations_follow_selected_runtime_api",
                    "--nocapture",
                ])
                .env(CHILD_API, version.to_string())
                .status()
                .expect("start test with independent API selection");
            assert!(status.success(), "runtime API {version} failed");
        }
        return;
    };
    let version = version.parse::<i32>().expect("integer runtime API");
    let _network = FdbApiBuilder::default()
        .set_runtime_version(version)
        .build()
        .expect("select runtime API")
        .boot()
        .expect("start network");
    futures::executor::block_on(check_versionstamped_mutations(version));
}

async fn check_versionstamped_mutations(version: i32) {
    let db = Database::new_compat(None).await.expect("open database");
    let subspace = Subspace::from(("test-versionstamp-runtime", version));
    let trx = db.create_trx().expect("create cleanup transaction");
    trx.set_option(TransactionOption::Timeout(10_000))
        .expect("set timeout");
    trx.clear_subspace_range(&subspace);
    trx.commit().await.expect("clear test keys");

    let stamped_key = subspace.pack_with_versionstamp(&("key", Versionstamp::incomplete(0)));
    let value_key = subspace.pack(&"value");
    let stamped_value = tuple::pack_with_versionstamp(&("value", Versionstamp::incomplete(1)));
    let counter_key = subspace.pack(&"counter");
    let trx = db.create_trx().expect("create mutation transaction");
    trx.set_option(TransactionOption::Timeout(10_000))
        .expect("set timeout");
    if version < 520 {
        for (key, value, mutation) in [
            (
                stamped_key.as_slice(),
                b"stored".as_slice(),
                MutationType::SetVersionstampedKey,
            ),
            (
                value_key.as_slice(),
                stamped_value.as_slice(),
                MutationType::SetVersionstampedValue,
            ),
        ] {
            let panic = catch_unwind(AssertUnwindSafe(|| trx.atomic_op(key, value, mutation)))
                .expect_err("pre-520 versionstamped mutations must be rejected");
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied());
            assert_eq!(
                message,
                Some("versionstamped mutations require runtime API 520 or later")
            );
        }
        assert_eq!(trx.attempt_usage().call_atomic_op, 0);
        assert_eq!(trx.attempt_usage().bytes_written, 0);
        // Unsupported versionstamps do not prevent API 510 atomic operations.
        trx.atomic_op(&counter_key, &1_i64.to_le_bytes(), MutationType::Add);
        trx.commit().await.expect("commit API 510 atomic add");
        let read = db.create_trx().expect("create read transaction");
        read.set_option(TransactionOption::Timeout(10_000))
            .expect("set timeout");
        assert_eq!(
            read.get(&counter_key, false)
                .await
                .expect("read counter")
                .as_deref(),
            Some(1_i64.to_le_bytes().as_slice())
        );
        assert!(
            read.get(&value_key, false)
                .await
                .expect("read rejected mutation")
                .is_none()
        );
        return;
    }

    trx.atomic_op(&stamped_key, b"stored", MutationType::SetVersionstampedKey);
    trx.atomic_op(
        &value_key,
        &stamped_value,
        MutationType::SetVersionstampedValue,
    );
    let committed_version = trx.get_versionstamp();
    trx.commit().await.expect("commit versionstamped mutations");
    let committed_version = committed_version.await.expect("get commit versionstamp");
    let transaction_version: [u8; 10] = (*committed_version).try_into().expect("ten-byte version");
    let expected_key = subspace.pack(&("key", Versionstamp::complete(transaction_version, 0)));
    let expected_value = tuple::pack(&("value", Versionstamp::complete(transaction_version, 1)));
    let read = db.create_trx().expect("create read transaction");
    read.set_option(TransactionOption::Timeout(10_000))
        .expect("set timeout");
    assert_eq!(
        read.get(&expected_key, false)
            .await
            .expect("read versionstamped key")
            .as_deref(),
        Some(b"stored".as_slice())
    );
    assert_eq!(
        read.get(&value_key, false)
            .await
            .expect("read versionstamped value")
            .as_deref(),
        Some(expected_value.as_slice())
    );
}
