use byteorder::ByteOrder;
use foundationdb::api::FdbApiBuilder;
use foundationdb::options::NetworkOption;
use foundationdb::tuple::Subspace;
use foundationdb::{Database, FdbBindingError, options};

/// This example demonstrate usage of multi_version compatibility client.
///
/// While you still need to compile the crate with a specific FoundationDB library version,
/// it allows you to connect to a cluster with a different API version. Be aware that using
/// this feature might lead to divergent behaviors.
///
/// Ref: https://apple.github.io/foundationdb/api-general.html#multi-version-client-api
const NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY: &str =
    "FDB_NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY";

#[tokio::main]
async fn main() {
    let mut network_builder = FdbApiBuilder::default()
        .build()
        .expect("Failed to build API");
    // You can either use FoundationDB network option through environment variables
    // or through network options in code.
    // directory specified should contain at least one libfdb.so
    if std::env::var(NETWORK_OPTION_EXTERNAL_CLIENT_DIRECTORY).is_err() {
        network_builder = network_builder
            .set_option(NetworkOption::ExternalClientDirectory(
                "/usr/lib/foundationdb/".to_string(),
            ))
            .expect("Failed to add external library directory");
    }
    network_builder.boot().expect("failed to start the network");
    // The network is stopped and joined automatically at process exit, which is
    // fine for tests and short-lived tools like this example. In a production
    // application, prefer a clean teardown: the network thread is the event loop
    // driving every transaction and you may still have on-going operations at
    // exit time. Finish or cancel your work, drop the Database handles, then
    // call `foundationdb::api::stop_network()` yourself (terminal: any
    // FoundationDB use afterwards fails with error 2025).

    // You can replace `None` with an `Option` with the path to your `fdb.cluster` file
    let db = Database::new_compat(None)
        .await
        .expect("failed to get database");
    db.set_option(options::DatabaseOption::TransactionTimeout(5000))
        .expect("failed to set transaction timeout");
    db.set_option(options::DatabaseOption::TransactionRetryLimit(3))
        .expect("failed to set transaction retry limit");

    // used to catch the first cluster_version_changed error when using external clients
    // when using external clients, it will throw cluster_version_changed for the first time establish the connection to
    // the cluster. Thus, we catch it by doing a get version request to establish the connection
    // The 3000ms timeout is a guard to avoid waiting forever when the cli cannot talk to any coordinators
    let _ = db
        .run(|trx, _| async move {
            trx.set_option(options::TransactionOption::Timeout(3000))?;
            let maybe_version = trx.get_read_version().await;

            match maybe_version {
                Ok(_) => Ok(()),
                // 1039: cluster_version_changed
                Err(err) if err.code() == 1039 => Err(FdbBindingError::from(err)),
                Err(err) => Err(FdbBindingError::NonRetryableFdbError(err)),
            }
        })
        .await;

    let key = Subspace::all()
        .subspace(&"examples")
        .pack(&"multi_version_incr");

    // increment or create key with "1"
    db.run(|trx, _| {
        let key = key.clone();
        async move {
            let mut buf = [0u8; 8];
            byteorder::LE::write_i64(&mut buf, 1);
            trx.atomic_op(&key, &buf, options::MutationType::Add);
            Ok::<_, FdbBindingError>(())
        }
    })
    .await
    .expect("could not commit");

    // read counter value
    let raw_counter = db
        .run(|trx, _| {
            let key = key.clone();
            async move {
                let result = trx.get(&key, true).await?;
                Ok::<_, FdbBindingError>(result)
            }
        })
        .await
        .expect("could not read key")
        .expect("no value found");

    let counter = byteorder::LE::read_i64(raw_counter.as_ref());
    dbg!(counter);
    assert!(counter > 0);
}
