use foundationdb::tuple::{Subspace, TuplePack, unpack};
use foundationdb::{Database, FdbBindingError, FdbResult, RangeOption, Transaction};
use futures::TryStreamExt;
use std::fmt::{Display, Formatter};

#[derive(Default)]
struct User {
    id: String,
    name: String,
    zipcode: String,
}

impl Display for User {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "id => '{}', zipcode => '{}', name => '{}'",
            self.id, self.zipcode, self.name
        )
    }
}

/// Cleanup any data from previous run
async fn clear_subspaces(db: &Database, subspaces: &[Subspace]) {
    db.run(|trx, _maybe_committed| async move {
        // teardown, clear data from previous run
        for subspace in subspaces {
            trx.clear_subspace_range(subspace);
        }
        Ok::<_, FdbBindingError>(())
    })
    .await
    .expect("Unable to commit transaction");
}

async fn create_user(
    transaction: &Transaction,
    id: &str,
    zipcode: &str,
    name: &str,
    user_subspace: &Subspace,
    zipcode_index_subspace: &Subspace,
) {
    let key = user_subspace.pack(&(id, zipcode));
    let index = zipcode_index_subspace.pack(&(zipcode, id));

    transaction.set(&key, &name.pack_to_vec());
    transaction.set(&index, &[]);
}

async fn get_user(
    user_subspace: &Subspace,
    transaction: &Transaction,
    user_id: &str,
    zipcode: &str,
) -> FdbResult<User> {
    let key = user_subspace.pack(&(user_id, zipcode));

    let user = transaction
        .get(&key, false)
        .await?
        .expect("Could not found a row");

    let name: String = unpack(&user).expect("Could not unpack");
    Ok(User {
        id: user_id.to_string(),
        zipcode: zipcode.to_string(),
        name,
    })
}

async fn search_user_by_zipcode(
    db: &Database,
    user_subspace: &Subspace,
    zipcode_index: &Subspace,
    zipcode: &str,
) -> Result<Vec<User>, FdbBindingError> {
    db.run(|trx, _maybe_committed| async move {
        // Create scanning zipcode index range
        let begin = zipcode_index.pack(&(zipcode));

        let mut end = zipcode_index.pack(&(zipcode));
        end.pop();
        end.push(255_u8);

        let range = RangeOption::from((begin, end));

        let mut users = vec![];
        let mut stream = trx.get_ranges_keyvalues(range, false);
        while let Some(result) = stream.try_next().await? {
            let (zipcode, id): (String, String) = zipcode_index
                .unpack(result.key())
                .expect("Unable to unpack value from index");

            let result_user = get_user(user_subspace, &trx, &id, &zipcode).await;

            if let Ok(user) = result_user {
                users.push(user);
            }
        }

        Ok(users)
    })
    .await
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
        .expect("failed to get database");
    db.set_option(foundationdb::options::DatabaseOption::TransactionTimeout(
        5000,
    ))
    .expect("failed to set transaction timeout");
    db.set_option(foundationdb::options::DatabaseOption::TransactionRetryLimit(3))
        .expect("failed to set transaction retry limit");

    let user_subspace = Subspace::from_bytes("user");
    let zipcode_index_subspace = Subspace::from_bytes("zipcode_index");

    clear_subspaces(
        &db,
        &[user_subspace.clone(), zipcode_index_subspace.clone()],
    )
    .await;

    populate_users(&db, &user_subspace, &zipcode_index_subspace)
        .await
        .expect("Unable to populate database");

    let users = search_user_by_zipcode(&db, &user_subspace, &zipcode_index_subspace, "205").await;

    match users {
        Ok(users) => users.iter().for_each(|user| println!("{user}")),
        Err(e) => eprintln!("Error searching users: {e}"),
    }
}

async fn populate_users(
    db: &Database,
    user_subspace: &Subspace,
    zipcode_index_subspace: &Subspace,
) -> Result<(), FdbBindingError> {
    db.run(|trx, _maybe_committed| async move {
        create_user(
            &trx,
            "001",
            "20500",
            "Barack",
            user_subspace,
            zipcode_index_subspace,
        )
        .await;
        create_user(
            &trx,
            "002",
            "20500",
            "Michelle",
            user_subspace,
            zipcode_index_subspace,
        )
        .await;
        create_user(
            &trx,
            "003",
            "20500",
            "Sasha",
            user_subspace,
            zipcode_index_subspace,
        )
        .await;
        create_user(
            &trx,
            "004",
            "20500",
            "Malia",
            user_subspace,
            zipcode_index_subspace,
        )
        .await;
        create_user(
            &trx,
            "005",
            "20500",
            "Bo",
            user_subspace,
            zipcode_index_subspace,
        )
        .await;
        create_user(
            &trx,
            "101",
            "SW1A 1AA",
            "Elizabeth",
            user_subspace,
            zipcode_index_subspace,
        )
        .await;
        create_user(
            &trx,
            "102",
            "SW1A 1AA",
            "Philip",
            user_subspace,
            zipcode_index_subspace,
        )
        .await;
        Ok(())
    })
    .await
}
