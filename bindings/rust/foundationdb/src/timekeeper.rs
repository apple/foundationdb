//! There is a key range called TimeKeeper in the system key space which stores a rolling history window
//! of time to version mappings, with one data point every 10 seconds.
//! It is not exposed via any user-facing API, though of course the data can be read by a user.
//! It is not an official database feature and should not be relied on for anything where accuracy
//! is critical as nothing prevents or detects system clock skew on the FDB process logging these data points.
//!
//! TimeKeeper is used by backup and restore to convert timestamps to approximate versions and versions
//! to approximate timestamps to make reasoning about backup data and restore operations easier.
//! Lookups work by finding the nearest value for the query version or timestamp, taking the equivalent other value,
//! and then adding an adjustment estimate based on 1 million versions per 1 second.
//! This logic accounts for arbitrary version advancement due to recovery, DR switch operations, or any other reason.
//!
//! [source](https://forums.foundationdb.org/t/versionstamp-as-absolute-time/2442/3)

use crate::future::FdbValue;
use crate::options::TransactionOption;
use crate::{FdbBindingError, FdbResult, KeySelector, RangeOption, Transaction};
use foundationdb_tuple::{pack, unpack};
use futures::StreamExt;

/// Timekeeper keys are stored in a special keyspace
/// Can be found in the [Java implementation](https://github.com/FoundationDB/fdb-record-layer/blob/main/fdb-extensions/src/main/java/com/apple/foundationdb/system/SystemKeyspace.java#L80)
const TIME_KEEPER_PREFIX: &[u8] = b"\xff\x02/timeKeeper/map/";

/// Flavor about the mode of scanning
pub enum HintMode {
    /// The read version is ensure to be before the timestamp
    BeforeTimestamp,
    /// The read version is ensured to be after the timestamp
    AfterTimestamp,
}

/// Try to get a version ID closer as possible as the asked timestamp
///
/// If no result are found, either your timestamp is in the future of the
/// Timekeeper or the data has been rolled by fresh ones.
///
/// The layout os follow:
///
/// TIME_KEEPER_PREFIX/timestamp1 => read_version1
/// TIME_KEEPER_PREFIX/timestamp2 => read_version2
/// TIME_KEEPER_PREFIX/timestamp3 => read_version3
///
/// Each key are associated to a pack read version on 8 bytes
/// compatible with an i64.
///
/// Timestamps are provided from unix time as seconds
pub async fn hint_version_from_timestamp(
    trx: &Transaction,
    unix_timestamp_as_seconds: u64,
    mode: HintMode,
    snapshot: bool,
) -> Result<Option<u64>, FdbBindingError> {
    // Timekeeper range keys are stored in /0x00/0x02 system namespace
    // to be able to read this range, the transaction must have
    // capabilities to read System Keys
    trx.set_option(TransactionOption::ReadSystemKeys)?;
    // The profiling should be used even when the Database has been locked
    trx.set_option(TransactionOption::ReadLockAware)?;

    // Timekeeper keys are defined has prefix/timestamp
    let mut start_key_bytes = TIME_KEEPER_PREFIX.to_vec();
    start_key_bytes.extend_from_slice(&pack(&unix_timestamp_as_seconds));
    // we get the first key greater than this value because timekeeper doesn't tick
    // each seconds but rather each 10 seconds but not each time
    let start_key = KeySelector::first_greater_or_equal(start_key_bytes.clone());

    // The end of the scan is the end of the timekeeper range
    // but we won't scan it the whole range
    let mut end_key_bytes = TIME_KEEPER_PREFIX.to_vec();
    end_key_bytes.extend_from_slice(b"\xff");
    let end_key = KeySelector::first_greater_than(end_key_bytes);

    let mut range = match mode {
        HintMode::AfterTimestamp => RangeOption::from((start_key, end_key)),
        HintMode::BeforeTimestamp => {
            let mut range = RangeOption::from((
                KeySelector::first_greater_than(TIME_KEEPER_PREFIX),
                start_key,
            ));
            range.reverse = true;
            range
        }
    };
    // No need to scan further than the next and previous key
    range.limit = Some(1);

    // We get the first key matching our start range bound
    let results = trx
        .get_ranges_keyvalues(range, snapshot)
        .take(1)
        .collect::<Vec<FdbResult<FdbValue>>>()
        .await;

    // If any result then the value found will be the read version ID
    if let Some(Ok(kv)) = results.first() {
        let version = unpack(kv.value()).map_err(FdbBindingError::PackError)?;
        return Ok(Some(version));
    }
    // otherwise timestamp too old or is future
    Ok(None)
}
