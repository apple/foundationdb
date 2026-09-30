use core::fmt;
use std::error;

use foundationdb::{Database, FdbBindingError, RangeOption, future::FdbValue, tuple::Subspace};
use futures::StreamExt;
use rand::{Rng, rngs::SmallRng};

/// Clears subspaces of a database.
///
/// # Errors
///
/// If client failed to commit transaction.
async fn clear_subspace(db: &Database, subspace: &Subspace) -> Result<(), FdbBindingError> {
    db.run(|trx, _| async move {
        trx.clear_subspace_range(subspace);
        Ok(())
    })
    .await
}

/// Error returned on attempt to insert an item in a [`MicroQueue`]
/// which length is [`usize::MAX`].
#[derive(Debug)]
pub struct Overflow;

impl fmt::Display for Overflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("queue is too long")
    }
}

impl error::Error for Overflow {}

/// First-in-first-out (FIFO) queue of UTF-8 strings, implemented as a layer on
/// top of `FoundationDB`, after the [Java recipe](https://github.com/apple/foundationdb/blob/main/recipes/java-recipes/MicroQueue.java).
pub struct MicroQueue {
    db: Database,
    queue: Subspace,
    rng: SmallRng,
}

impl MicroQueue {
    /// Creates a new, empty, queue in `prefix` [`Subspace`].
    ///
    /// # Errors
    ///
    /// * If client failed to clear `prefix` [`Subspace`]
    pub async fn new(
        db: Database,
        prefix: impl Into<Vec<u8>> + Send + Sync + Unpin,
    ) -> Result<Self, FdbBindingError> {
        let queue = Subspace::from_bytes(prefix);
        clear_subspace(&db, &queue).await?;
        Ok(Self {
            db,
            queue,
            rng: rand::make_rng(),
        })
    }

    /// Get the last index in the queue.
    ///
    /// # Errors
    ///
    /// * If client failed to get [`FdbValues`](foundationdb::future::FdbValues)
    ///   stored under `prefix` [`Subspace`].
    /// * If client failed to commit transaction.
    async fn last_index(&self) -> Result<usize, FdbBindingError> {
        self.db
            .run(|trx, _maybe_committed| async move {
                Ok(trx
                    .get_ranges_keyvalues(self.queue.range().into(), true)
                    .count()
                    .await)
            })
            .await
    }

    /// Add an element to the queue.
    ///
    /// # Errors
    ///
    /// * If client failed to get [`FdbValues`](foundationdb::future::FdbValues)
    ///   stored under `prefix` [`Subspace`].
    /// * If client failed to commit transaction.
    /// * If the capacity of the queue is [`usize::MAX`].
    pub async fn enqueue(&mut self, value: &str) -> Result<(), FdbBindingError> {
        let index = self
            .last_index()
            .await?
            .checked_add(1)
            .ok_or_else(|| FdbBindingError::new_custom_error(Box::new(Overflow)))?;

        let rands = {
            // Create random seed to avoid conflicts.
            let mut rands = [0_u8; 20];
            self.rng.fill_bytes(&mut rands);
            rands
        };

        let key = &self.queue.subspace(&(index, rands.as_slice()));

        self.db
            .run(|trx, _maybe_committed| async move {
                trx.set(key.bytes(), value.as_bytes());
                Ok(())
            })
            .await
    }

    /// Get the top element of the queue.
    ///
    /// # Errors
    ///
    /// * Upon failure to collect stream.
    /// * If client failed to commit transaction.
    async fn first_item(&self) -> Result<Option<FdbValue>, FdbBindingError> {
        self.db
            .run(|trx, _maybe_committed| async move {
                trx.get_ranges_keyvalues(RangeOption::from(&self.queue).rev(), true)
                    .next()
                    .await
                    .transpose()
                    .map_err(Into::into)
            })
            .await
    }

    /// Remove the top element from the queue.
    ///
    /// # Errors
    ///
    /// * Upon failure to collect the stream of key values in `prefix` [`Subspace`].
    /// * If client failed to commit transaction.
    ///
    /// # Panics
    ///
    /// * If value is corrupted (invalid UTF-8).
    pub async fn dequeue(&mut self) -> Result<Option<String>, FdbBindingError> {
        match self.first_item().await? {
            None => Ok(None),
            Some(fdb_value) => {
                let key = fdb_value.key();
                let value = std::str::from_utf8(fdb_value.value()).expect("valid UTF-8");
                self.db
                    .run(|trx, _maybe_committed| async move {
                        trx.clear(key);
                        Ok(Some(value.to_owned()))
                    })
                    .await
            }
        }
    }
}

const LINE: [&str; 13] = [
    "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "George", "Harry", "Ian", "Jack", "Liz",
    "Mary", "Nathan",
];

#[tokio::main]
async fn main() -> Result<(), FdbBindingError> {
    // initialize FoundationDB Client API
    foundationdb::boot().expect("failed to initialize FoundationDB");
    // The network is stopped and joined automatically at process exit, which is
    // fine for tests and short-lived tools like this example. In a production
    // application, prefer a clean teardown: the network thread is the event loop
    // driving every transaction and you may still have on-going operations at
    // exit time. Finish or cancel your work, drop the Database handles, then
    // call `foundationdb::api::stop_network()` yourself (terminal: any
    // FoundationDB use afterwards fails with error 2025).

    // attempt connection to FoundationDB
    let db = Database::default()?;

    // create a micro queue in `Q` subspace
    let mut q = MicroQueue::new(db, "Q").await?;

    // push values at the back of the queue
    for value in LINE {
        q.enqueue(value).await?;
    }

    // pop values from the front of the queue
    while let Some(value) = q.dequeue().await? {
        println!("{value}");
    }

    Ok(())
}
