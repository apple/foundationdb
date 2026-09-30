use foundationdb::{
    Database, FdbBindingError, FdbError, RangeOption, options,
    tuple::{Subspace, Versionstamp, pack, pack_with_versionstamp, unpack},
};
use futures::StreamExt;

#[tokio::main]
async fn main() {
    foundationdb::boot().expect("failed to initialize FoundationDB");
    // The network is stopped and joined automatically at process exit, which is
    // fine for tests and short-lived tools like this example. In a production
    // application, prefer a clean teardown: the network thread is the event loop
    // driving every transaction and you may still have on-going operations at
    // exit time. Finish or cancel your work, drop the Database handles, then
    // call `foundationdb::api::stop_network()` yourself (terminal: any
    // FoundationDB use afterwards fails with error 2025).

    run_versionstamp_key_example()
        .await
        .expect("failed to run versionstamp example");

    run_versionstamp_value_example()
        .await
        .expect("failed to run versionstamp example");
}

async fn run_versionstamp_key_example() -> Result<(), FdbBindingError> {
    println!("running example for setting versionstamped keys");
    // Using versionstamps in order to create a sequential path.
    let db = Database::default()?;
    db.set_option(options::DatabaseOption::TransactionTimeout(5000))?;
    db.set_option(options::DatabaseOption::TransactionRetryLimit(3))?;

    let subspace = Subspace::all().subspace(&"versionstamp_example");
    let (from, to) = subspace.range();
    let trx_clear = db.create_trx()?;
    trx_clear.clear_range(&from, &to);
    trx_clear.commit().await.map_err(FdbError::from)?;

    // Handles cloned from the runner transaction share one user-version allocator.
    // Explicit Versionstamp::incomplete(42) remains supported, but can collide.
    db.run(|trx, _| {
        let subspace = subspace.clone();
        async move {
            let component_a = trx.clone();
            let component_b = trx.clone();

            // Allocate inside the retry closure because retries restart allocation at zero.
            for (component, value) in [
                (&component_a, "component_a_1"),
                (&component_b, "component_b_1"),
                (&component_a, "component_a_2"),
            ] {
                let versionstamp = Versionstamp::incomplete(component.allocate_user_version()?);
                let key = subspace.pack_with_versionstamp(&("prefix", &versionstamp));
                component.atomic_op(
                    &key,
                    &pack(&value),
                    options::MutationType::SetVersionstampedKey,
                );
            }

            Ok::<_, FdbBindingError>(())
        }
    })
    .await?;

    // The three keys share one 10-byte transaction version and have user versions 0, 1, and 2.

    let trx = db.create_trx()?;
    let range = RangeOption::from((from, to));
    let mut kvs = trx.get_ranges_keyvalues(range, false);
    while let Some(kv) = kvs.next().await {
        let kv = kv?;
        let (_, v) = subspace
            .unpack::<(String, Versionstamp)>(kv.key())
            .expect("failed to unpack key");
        let value = unpack::<String>(kv.value()).expect("failed to unpack value");
        println!(
            "{:?} {}: {}",
            v.transaction_version(),
            v.user_version(),
            value
        );
    }
    Ok(())
}

async fn run_versionstamp_value_example() -> Result<(), FdbBindingError> {
    println!("running example for setting versionstamped values");
    // You can use versionstamps in values too, for example to point to a versionstamped key.
    let db = Database::default()?;
    db.set_option(options::DatabaseOption::TransactionTimeout(5000))?;
    db.set_option(options::DatabaseOption::TransactionRetryLimit(3))?;

    let subspace = Subspace::all().subspace(&"versionstamp_example");
    let (from, to) = subspace.range();
    let trx_clear = db.create_trx()?;
    trx_clear.clear_range(&from, &to);
    trx_clear.commit().await.map_err(FdbError::from)?;

    // In our transaction we will create a versionstamped key, and then reference it in another
    // known "index" key.
    let index_key = subspace.pack(&"index");
    db.run(|trx, _| {
        let subspace = subspace.clone();
        let index_key = index_key.clone();
        async move {
            // Reuse one allocated stamp for the data key and its index reference.
            let versionstamp = Versionstamp::incomplete(trx.allocate_user_version()?);
            let key_tuple = ("data", &versionstamp);
            let key = subspace.pack_with_versionstamp(&key_tuple);

            trx.atomic_op(
                &key,
                &pack(&"some value"),
                options::MutationType::SetVersionstampedKey,
            );
            trx.atomic_op(
                &index_key,
                &pack_with_versionstamp(&key_tuple),
                options::MutationType::SetVersionstampedValue,
            );

            Ok::<_, FdbBindingError>(())
        }
    })
    .await?;

    // Now we created our versionstamped key and a reference to it.
    // We can read the index key and get the versionstamped key back.
    let trx = db.create_trx()?;
    let index_kv = trx
        .get(&index_key, false)
        .await?
        .expect("didn't find index");
    let versionstamped_key =
        unpack::<(String, Versionstamp)>(&index_kv).expect("failed to unpack value");

    // notice that we don't pack with versionstamp here because the versionstamp we received is complete.
    let versionstamped_value = trx
        .get(&subspace.pack(&versionstamped_key), false)
        .await?
        .expect("didn't find reference");
    let value = unpack::<String>(&versionstamped_value).expect("failed to unpack value");
    println!("got back value {value}");
    Ok(())
}
