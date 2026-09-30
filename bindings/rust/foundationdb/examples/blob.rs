use foundationdb::tuple::Subspace;
use foundationdb::{Database, FdbBindingError, RangeOption};
use futures::TryStreamExt;
use rand::RngExt;
use rand::distr::Uniform;

/// The goal of this example is to store blob data as chunk of 1kB
/// Retrieve these data then verify correctness
async fn clear_subspace(db: &Database, subspace: &Subspace) {
    db.run(|trx, _maybe_committed| async move {
        trx.clear_subspace_range(subspace);
        Ok::<_, FdbBindingError>(())
    })
    .await
    .expect("Unable to commit transaction");
}

static CHUNK_SIZE: usize = 1024;
async fn write_blob(db: &Database, subspace: &Subspace, data: &[u8]) {
    db.run(|trx, _maybe_committed| async move {
        let mut chunk_number = 0;
        data.chunks(CHUNK_SIZE).for_each(|chunk| {
            let start = chunk_number * CHUNK_SIZE;
            let end = start + chunk.len();

            let key = subspace.pack(&(start));
            trx.set(&key, &data[start..end]);
            chunk_number += 1;
        });
        Ok::<_, FdbBindingError>(())
    })
    .await
    .expect("Unable to commit transaction");
}

async fn read_blob(db: &Database, subspace: &Subspace) -> Vec<u8> {
    db.run(|trx, _maybe_committed| async move {
        let range = RangeOption::from(subspace.range());

        let mut data: Vec<u8> = vec![];
        let mut stream = trx.get_ranges_keyvalues(range, false);
        while let Some(result) = stream.try_next().await? {
            data.extend(result.value().iter());
        }

        Ok::<_, FdbBindingError>(data)
    })
    .await
    .expect("Unable to read blob")
}

async fn test_blob_storing(db: &Database, subspace: &Subspace, iteration: usize) {
    // Generate random 10k bytes
    let data: Vec<u8> = rand::rng()
        .sample_iter(Uniform::new_inclusive(0, 255).expect("Unable to create random generator"))
        .map(|x| x as u8)
        .take(10000)
        .collect();

    // Create a subspace for iteration
    let subspace = subspace.subspace(&(iteration));

    // Write data as chunk of 1024
    write_blob(db, &subspace, &data).await;

    // Read data
    let result = read_blob(db, &subspace).await;

    // Check correctness
    assert_eq!(&data, &result);
}

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
    let db = Database::new_compat(None)
        .await
        .expect("Unable to create database");
    db.set_option(foundationdb::options::DatabaseOption::TransactionTimeout(
        5000,
    ))
    .expect("failed to set transaction timeout");
    db.set_option(foundationdb::options::DatabaseOption::TransactionRetryLimit(3))
        .expect("failed to set transaction retry limit");

    let subspace = Subspace::all().subspace(&("example-blob"));
    clear_subspace(&db, &subspace).await;

    for i in 1..=100 {
        println!("Iteration #{i}");
        test_blob_storing(&db, &subspace, i).await;
    }
}
