use bytesize::ByteSize;
use foundationdb::tuple::{PackError, Subspace, pack, unpack};
use foundationdb::{Database, FdbBindingError, RangeOption};
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom};
use uuid::Uuid;

///
/// This example is  concrete using of [blob example](./blob.rs)
///
/// The goal of this example is to create a simple blob storage system
///
/// As we can't predict the size of data, we'll create a system of data involving
/// a manifest and a bunch of chunk of data.
///
/// The genuine idea would be to create a simple key => value entry and to store every bytes
/// into it.
///
/// For example:
///  my_file => <data>
///
/// This way doesn't allow to append metadata.
///
/// Caution : This example can't handle file of more than 10 MB.
///
/// The idea is to split the key between a manifest and a data
///
/// A manifest can be represent as:
///     uuid              : the unique identifier through database
///     name              : the name of the data
///     size              : size of total data
///     chunk size        : max chunk size
///     number of chunks  : number of chunks of total data
///
/// As foundation DB can only store binary data, we have to pack our manifest.
///
/// This can be done through a tuple of data.
///
/// The foundation DB packing API is clever enough to keep the type of each field.
///
/// The "/" materialize the foundation subspace separator you can see it as "\x02"
///
/// This manifest will be stored as
///
/// <uuid>/_manifest => <manifest_data>
///
/// Then each chunk of data
///
/// <uuid>/_data/0 => <chunk_data_0>
/// <uuid>/_data/1 => <chunk_data_1>
/// <uuid>/_data/2 => <chunk_data_2>
/// <uuid>/_data/3 => <chunk_data_3>
/// ...
/// <uuid>/_data/n => <chunk_data_n>
///
/// To get data from database we have to gather all chunk of data associated to defined uuid,
/// we have to scan the database using the subspace of the uuid.
///
/// As all data is contained inside the <uuid>/_data subspace.
///
/// We can range between <uuid>/_data/\x00 and <uuid>/_data/\xff
///
/// \x00 is the minimal value that can be stored in subspace
/// \xff is the maximal value that can be stored in subspace
///
/// So
///     \x00 <= 0 < \xff
///     \x00 <= 1 < \xff
///     \x00 <= 2 < \xff
///     ...
///     \x00 <= n < \xff
///
///
/// Thus foundation DB will retrieve keys/values:
///
///     <uuid>/_data/0 => <chunk_data_0>
///     <uuid>/_data/1 => <chunk_data_1>
///     <uuid>/_data/2 => <chunk_data_2>
///     <uuid>/_data/3 => <chunk_data_3>
///     ...
///     <uuid>/_data/n => <chunk_data_n>
///
/// If we accumulate each chunk data, we gather the original data.
///
/// Finally, we have to check the correctness of our data.
///
/// As we have stored the manifest in the <uuid>/_manifest subspace
///
/// We can get this key/value.
///
/// Unpack the manifest_data to the type tuple and finally recreate our manifest containing
/// the hex digest.
///
/// We can compute the checksum of data from database and compare it to the manifest digest.
///
/// Workflow:
///
/// 0 - Create keyspace and cleanup previous one
/// 1 - Load bytes from image
/// 2 - Compute checksum of input data
/// 3 - Write bytes to database
/// 4 - Read bytes from database
/// 5 - Compute checksum of output data and verify correctness
async fn clear_subspace(db: &Database, subspaces: Vec<&Subspace>) {
    db.run(|transaction, _| {
        let subspaces = subspaces.clone();
        async move {
            for subspace in subspaces {
                transaction.clear_subspace_range(subspace)
            }
            Ok::<_, FdbBindingError>(())
        }
    })
    .await
    .expect("Unable to commit transaction");
}

fn sha256_hex_digest<R: Read>(mut reader: R) -> String {
    let mut context = Sha256::new();
    let mut buffer = [0_u8; 1024];

    loop {
        let count = reader.read(&mut buffer).expect("Unable to read buffer");

        if count == 0_usize {
            break;
        }
        context.update(&buffer[..count]);
    }

    let digest = context.finalize();

    data_encoding::HEXLOWER.encode(digest.as_ref())
}

fn load_bytes_from_file(path: &str) -> (Vec<u8>, String) {
    let mut file = File::open(path).expect("Unable to open path");
    let mut buffer = [0_u8; 1024];
    let mut data = vec![];

    loop {
        let count = file.read(&mut buffer).expect("Unable to read from file");
        if count == 0_usize {
            break;
        }
        data.extend(&buffer[..count].to_vec())
    }
    file.seek(SeekFrom::Start(0))
        .expect("Unable to rewind file cursor");

    let digest = sha256_hex_digest(&mut file);
    (data, digest)
}

static CHUNK_SIZE: usize = 1024;

/// Write slice of data to database using given subspace
async fn write_data(db: &Database, subspace: &Subspace, data: &Vec<u8>) {
    if data.is_empty() {
        return;
    }

    db.run(|transaction, _| async move {
        transaction.set(subspace.bytes(), data.as_slice());
        Ok::<_, FdbBindingError>(())
    })
    .await
    .expect("Unable to commit transaction");
}

/// Writes to database a slice of data chunked with to the given subspace
/// If chunk size isn't provided **CHUNK_SIZE**
async fn write_blob(
    db: &Database,
    subspace: &Subspace,
    data: &[u8],
    chunk_size: Option<usize>,
) -> usize {
    if data.is_empty() {
        return 0;
    }

    db.run(|transaction, _| async move {
        let chunk_size = chunk_size.unwrap_or(CHUNK_SIZE);

        let mut chunk_number = 0;
        data.chunks(chunk_size).for_each(|chunk| {
            let start = chunk_number * chunk_size;
            let end = start + chunk.len();

            let key = subspace.pack(&(chunk_number));
            transaction.set(&key, &data[start..end]);
            chunk_number += 1;
        });

        Ok::<_, FdbBindingError>(chunk_number)
    })
    .await
    .expect("Unable to commit transaction")
}

/// Read data from database
async fn read_data(db: &Database, subspace: &Subspace) -> Option<Vec<u8>> {
    db.run(|transaction, _| async move {
        let get_result = transaction.get(subspace.bytes(), false).await?;
        Ok::<_, FdbBindingError>(get_result.map(|data| data.to_vec()))
    })
    .await
    .ok()
    .flatten()
}

/// Gets data from database from the given subspace with specified key name.
async fn read_blob(db: &Database, subspace: &Subspace) -> Vec<u8> {
    db.run(|transaction, _| async move {
        let range = RangeOption::from(subspace.range());
        let mut data: Vec<u8> = vec![];
        let mut stream = transaction.get_ranges_keyvalues(range, false);
        while let Some(result) = stream.try_next().await? {
            data.extend(result.value().iter());
        }

        Ok::<_, FdbBindingError>(data)
    })
    .await
    .expect("Unable to read blob")
}

struct FileManifest {
    name: String,
    chunk_size: Option<usize>,
    nb_chunks: usize,
    digest: String,
    size: usize,
    uuid: Uuid,
}

impl FileManifest {
    pub fn serialize(&self) -> Vec<u8> {
        pack(&(
            &self.name,
            self.chunk_size,
            self.nb_chunks,
            &self.digest,
            &self.size,
            &self.uuid,
        ))
    }

    pub fn try_deserialize(value: &[u8]) -> Result<Self, PackError> {
        let (name, chunk_size, nb_chunks, digest, size, uuid): (
            String,
            Option<usize>,
            usize,
            String,
            usize,
            Uuid,
        ) = unpack(value)?;

        Ok(FileManifest {
            name,
            chunk_size,
            nb_chunks,
            digest,
            size,
            uuid,
        })
    }
}

impl Display for FileManifest {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Manifest of data {}\n\t\tname: {}\n\t\tchecksum: {}\n\t\tsize: {}\n\t\tblock size: {}\n\t\tnumber of chunks: {}\n",
            self.uuid,
            self.name,
            self.digest,
            ByteSize(self.size as u64).display().si(),
            ByteSize(self.chunk_size.unwrap_or(CHUNK_SIZE) as u64)
                .display()
                .si(),
            self.nb_chunks
        )
    }
}

async fn populate_data(
    db: &Database,
    subspace: &Subspace,
    file_names: &Vec<&str>,
    chunk_sizes: &Vec<usize>,
) -> Vec<Subspace> {
    let mut files_subspace: Vec<Subspace> = vec![];

    println!("--- Populate database");
    for file_name in file_names {
        println!("Writing file {file_name}");

        let (data, digest) = load_bytes_from_file(file_name);

        for chunk_size in chunk_sizes {
            let uuid = Uuid::new_v4();

            let subspace_file = subspace.subspace(&(uuid));
            let subspace_data = subspace_file.subspace(&("_data"));
            let subspace_manifest = subspace_file.subspace(&("_manifest"));

            println!(
                "\twith chunk of {}",
                ByteSize(*chunk_size as u64).display().si()
            );
            let nb_chunks = write_blob(db, &subspace_data, &data, Some(*chunk_size)).await;

            let manifest = FileManifest {
                name: file_name.to_string(),
                chunk_size: Some(*chunk_size),
                nb_chunks,
                digest: digest.clone(),
                size: data.len(),
                uuid,
            }
            .serialize();
            write_data(db, &subspace_manifest, &manifest).await;

            files_subspace.push(subspace_file);
        }

        let uuid = Uuid::new_v4();
        let subspace_file = subspace.subspace(&(uuid));
        let subspace_data = subspace_file.subspace(&("_data"));
        let subspace_manifest = subspace_file.subspace(&("_manifest"));

        let nb_chunks = write_blob(db, &subspace_data, &data, None).await;
        let manifest = FileManifest {
            name: file_name.to_string(),
            chunk_size: None,
            nb_chunks,
            digest: digest.clone(),
            size: data.len(),
            uuid,
        }
        .serialize();
        write_data(db, &subspace_manifest, &manifest).await;
    }

    files_subspace
}

async fn check_data_stored(db: &Database, files_subspaces: Vec<Subspace>) {
    println!("\n--- Checking data stored");
    for file_subspace in files_subspaces {
        let manifest_subspace = file_subspace.subspace(&("_manifest"));
        let data_subspace = file_subspace.subspace(&("_data"));
        let manifest_data = read_data(db, &manifest_subspace)
            .await
            .expect("Unable to get manifest");
        let manifest = FileManifest::try_deserialize(&manifest_data)
            .expect("Unable to deserialize manifest data");

        let data_from_database = read_blob(db, &data_subspace).await;

        let data_from_database = Cursor::new(data_from_database);
        let digest_data_from_database = sha256_hex_digest(data_from_database);

        println!("{manifest}");
        assert_eq!(digest_data_from_database, manifest.digest);
        println!(
            "\t Verify data integrity:\n\t\tfrom database hex digest : {}\n\t\treference hex digest     : {}\n",
            manifest.digest, digest_data_from_database
        );
    }
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

    // Create the subspace handling image data
    let images_subspace = Subspace::all().subspace(&("images"));

    clear_subspace(&db, vec![&images_subspace]).await;

    // define matrix of tests
    let file_names = vec!["./assets/logo-400x400.png"];
    let chunk_sizes = vec![500, 512, 1000, 2000, 4000, 10000, 20000];

    let files = populate_data(&db, &images_subspace, &file_names, &chunk_sizes).await;
    check_data_stored(&db, files).await;
}
