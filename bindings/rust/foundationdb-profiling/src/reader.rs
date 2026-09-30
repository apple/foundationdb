//! Paged, resumable reading of the client profiling keyspace.
//!
//! Every sampled transaction is stored as one or more chunks under
//! `\xff\x02/fdbClientInfo/client_latency/`. A chunk key is laid out as:
//!
//! ```text
//! PREFIX | versionstamp (10) | '/' | transaction id (16) | '/' | chunk (4, BE) | total (4, BE) | '/' | user id ...
//! ```
//!
//! [`ProfileScanner::read_page`] consumes the rows of [`ProfileScanner::range`], read by
//! the caller, reassembles the chunks of each transaction, decodes them with
//! [`decode_events`] and returns a [`Page`] with a [`Cursor`] to resume from.

use crate::decode::{DecodeError, decode_events};
use crate::event::{Event, ProtocolVersion};
use futures_core::Stream;
use std::collections::HashMap;
use std::future::poll_fn;
use std::pin::pin;
use tracing::instrument;

/// Prefix of the client profiling keyspace.
///
/// Defined by the [C++ client](https://github.com/apple/foundationdb/blob/main/contrib/transaction_profiling_analyzer/transaction_profiling_analyzer.py#L421).
pub const PROFILE_PREFIX: &[u8; 32] = b"\xff\x02/fdbClientInfo/client_latency/";

/// Exclusive end of the client profiling keyspace (`strinc(PROFILE_PREFIX)`).
const PROFILE_END: &[u8; 32] = b"\xff\x02/fdbClientInfo/client_latency0";

const VERSIONSTAMP_LEN: usize = 10;
const ID_LEN: usize = 16;
const VERSIONSTAMP_START: usize = PROFILE_PREFIX.len();
const VERSIONSTAMP_END: usize = VERSIONSTAMP_START + VERSIONSTAMP_LEN;
const ID_START: usize = VERSIONSTAMP_END + 1;
const ID_END: usize = ID_START + ID_LEN;
const CHUNK_START: usize = ID_END + 1;
const TOTAL_START: usize = CHUNK_START + 4;
/// Shortest chunk key the [`Assembler`] can parse: everything up to the total chunk
/// count.
const MIN_KEY_LEN: usize = TOTAL_START + 4;

/// Opaque, resumable and persistable position in the profiling keyspace: the key the next
/// page starts reading from.
///
/// Persist it with [`Cursor::as_bytes`] and restore it with [`Cursor::from_bytes`] to
/// resume reading later, for instance to tail the keyspace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Cursor {
    /// Inclusive begin key of the next read, starts with [`PROFILE_PREFIX`].
    key: Vec<u8>,
}

/// Bytes given to [`Cursor::from_bytes`] are not a serialized [`Cursor`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid profiling cursor")]
pub struct InvalidCursor;

impl Cursor {
    /// Cursor at the beginning of the profiling keyspace.
    #[instrument(level = "trace")]
    pub fn beginning() -> Self {
        Cursor {
            key: PROFILE_PREFIX.to_vec(),
        }
    }

    /// Cursor at the first record with a commit version greater than or equal to
    /// `version`.
    ///
    /// The version is the one at which the profiling record was written by the client,
    /// which is some time (up to the client's flush interval) after the profiled
    /// transaction ran. Negative versions are treated as 0.
    #[instrument(level = "trace")]
    pub fn at_version(version: i64) -> Self {
        Cursor {
            key: version_key(version),
        }
    }

    /// Serialized form of the cursor, to persist it.
    #[instrument(level = "trace", skip_all)]
    pub fn as_bytes(&self) -> &[u8] {
        &self.key
    }

    /// Restores a cursor persisted with [`Cursor::as_bytes`].
    ///
    /// # Errors
    ///
    /// Returns [`InvalidCursor`] when `bytes` is not a serialized cursor.
    #[instrument(level = "trace", skip_all, fields(len = bytes.len()))]
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, InvalidCursor> {
        if bytes.starts_with(PROFILE_PREFIX) {
            Ok(Cursor { key: bytes })
        } else {
            Err(InvalidCursor)
        }
    }

    /// Inclusive begin key of the next read.
    fn key(&self) -> &[u8] {
        &self.key
    }
}

/// `PROFILE_PREFIX | version (8, BE) | \x00\x00`: the first key of that version.
fn version_key(version: i64) -> Vec<u8> {
    let mut key = PROFILE_PREFIX.to_vec();
    key.extend_from_slice(&version.max(0).to_be_bytes());
    key.extend_from_slice(b"\x00\x00");
    key
}

/// The key right after `key`.
fn key_after(key: &[u8]) -> Vec<u8> {
    [key, b"\x00"].concat()
}

/// Builds pages of profiling data from the rows its caller reads.
///
/// Holds no connection and does no I/O: the caller reads [`range`](Self::range) with its
/// own FoundationDB client and hands the rows to [`read_page`](Self::read_page).
#[derive(Debug, Clone, Default)]
pub struct ProfileScanner {
    end_version: Option<i64>,
}

/// The key range the caller must read for one page, see [`ProfileScanner::range`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanRange {
    /// Inclusive begin key.
    pub begin: Vec<u8>,
    /// Exclusive end key.
    pub end: Vec<u8>,
}

/// Error of [`ProfileScanner::read_page`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ScanError<E> {
    /// The row stream failed. Map it back to the caller's error, for instance to the
    /// `FdbError` of a range read so that the caller's retry loop handles it.
    #[error(transparent)]
    Source(E),
    /// The stream yielded a row outside [`ProfileScanner::range`], or a key not strictly
    /// greater than the previous one: the caller did not read the range as required.
    #[error("profiling row {key:?} is outside the scanned range or not after the previous row")]
    InvalidRows {
        /// Key of the offending row.
        key: Vec<u8>,
    },
}

impl<E> ScanError<E> {
    /// Converts this error into the caller's error type: a stream error through `From`,
    /// any other error boxed and passed to `custom`.
    ///
    /// The boxed error does not depend on `E`: it is a [`ScanError<std::convert::Infallible>`]
    /// carrying the same variant data.
    ///
    /// With fdb-rs: `.map_err(|e| e.into_error(FdbBindingError::new_custom_error))?`, so a
    /// failed range read reaches `db.run` as the original `FdbError` and is retried.
    #[instrument(level = "trace", skip_all)]
    pub fn into_error<T: From<E>>(
        self,
        custom: impl FnOnce(Box<dyn std::error::Error + Send + Sync>) -> T,
    ) -> T {
        match self {
            ScanError::Source(err) => T::from(err),
            ScanError::InvalidRows { key } => {
                custom(Box::new(
                    ScanError::<std::convert::Infallible>::InvalidRows { key },
                ))
            }
        }
    }
}

impl ProfileScanner {
    /// A scanner with no [`end_version`](Self::end_version).
    #[must_use]
    #[instrument(level = "trace")]
    pub fn new() -> Self {
        Self::default()
    }

    /// Exclusive upper bound on the record commit version read by a page. Unset (the
    /// default) reads to the end of the keyspace.
    #[must_use]
    #[instrument(level = "trace", skip_all)]
    pub fn end_version(self, version: i64) -> Self {
        ProfileScanner {
            end_version: Some(version),
        }
    }

    /// The key range the caller must read for the page starting at `cursor`, and give to
    /// [`read_page`](Self::read_page): forward, with snapshot reads and no row limit,
    /// from `begin` (inclusive) to `end` (exclusive), both as `first_greater_or_equal`
    /// key selectors. The range is empty when `cursor` is at or past
    /// [`end_version`](Self::end_version).
    #[instrument(level = "trace", skip_all)]
    pub fn range(&self, cursor: &Cursor) -> ScanRange {
        let begin = cursor.key().to_vec();
        let end = match self.end_version {
            Some(version) => version_key(version),
            None => PROFILE_END.to_vec(),
        };
        // never an inverted range
        let end = end.max(begin.clone());
        ScanRange { begin, end }
    }
}

/// One sampled transaction, reassembled and decoded.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ProfiledTransaction {
    /// Commit version of the profiling record (the first 8 bytes of `versionstamp`).
    pub version: i64,
    /// Versionstamp of the first chunk of the profiling record. The client can write the
    /// chunks of one transaction in several commits, the later ones then carry greater
    /// versionstamps.
    pub versionstamp: [u8; 10],
    /// Transaction id chosen by the client that sampled the transaction.
    pub id: [u8; 16],
    /// Protocol version of the client that wrote the record.
    pub protocol_version: ProtocolVersion,
    /// Events of the transaction, in the order the client recorded them.
    pub events: Vec<Event>,
}

/// A transaction that could not be returned.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Skipped {
    /// Versionstamp of the first chunk read of the profiling record.
    pub versionstamp: [u8; 10],
    /// Transaction id chosen by the client that sampled the transaction.
    pub id: [u8; 16],
    /// Why it was skipped.
    pub reason: SkipReason,
}

/// Why a transaction was [`Skipped`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum SkipReason {
    /// All chunks were read but the reassembled blob did not decode.
    Decode(DecodeError),
    /// Chunks are missing, out of order or inconsistent, or the page was stopped before
    /// they were all read, so the blob could not be reassembled.
    BrokenChunks,
}

/// Result of [`ProfileScanner::read_page`].
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Page {
    /// Decoded transactions, in the order they completed (the key of their last chunk).
    pub transactions: Vec<ProfiledTransaction>,
    /// Transactions that could not be returned, in the order they were detected.
    pub skipped: Vec<Skipped>,
    /// Where to resume. Always valid, also to tail the keyspace once `exhausted`.
    pub next: Cursor,
    /// Whether the row stream ended, that is the end of the range (`end_version` or the
    /// end of the keyspace) was reached. `false` when the caller's stop check stopped
    /// the page.
    pub exhausted: bool,
}

impl ProfileScanner {
    /// Builds one page of profiled transactions from `rows`, the rows of
    /// [`range(cursor)`](Self::range) read by the caller, stopping when `should_stop`
    /// says so.
    ///
    /// `rows` must yield exactly the key-value pairs of [`range(cursor)`](Self::range),
    /// read forward, with snapshot reads and no row limit: every row inside the range,
    /// in strictly increasing key order. A row outside the range or out of order fails
    /// the page with [`ScanError::InvalidRows`]; an error of the stream fails it with
    /// [`ScanError::Source`]. The stream ending means the end of the range was reached,
    /// and the page is [`exhausted`](Page::exhausted).
    ///
    /// `should_stop` is where the caller bounds the page (a transaction budget, a row
    /// count, anything): it is called after every row, and the page stops as soon as it
    /// returns `true`, without polling `rows` again. Then:
    ///
    /// - With no transaction pending (all the chunks read so far belong to complete or
    ///   broken transactions), [`Page::next`] points right after the last row.
    /// - Otherwise [`Page::next`] points at the first chunk of the oldest pending
    ///   transaction, before every transaction the page completed after it, which the
    ///   page drops and the next one reads again. Such a transaction is returned whole
    ///   by a later page, even when its chunks were written in two commits with other
    ///   rows in between.
    /// - When that position is `cursor` itself (a single transaction cannot be read from
    ///   `cursor` before the caller says stop), the pending transactions are reported as
    ///   [`SkipReason::BrokenChunks`] and [`Page::next`] points right after the last
    ///   row, so every stopped page advances the cursor.
    ///
    /// Each transaction is returned once over a sequence of pages that each resume from
    /// the previous [`Page::next`]; one is lost only in the last case above. The client
    /// may write the chunks of one transaction in two consecutive commits (when a flush
    /// exceeds the transaction size limit), with rows of other transactions in between.
    /// When the range ends while such a transaction is still incomplete (its second
    /// commit is not written yet, or lies beyond [`end_version`](Self::end_version)),
    /// [`Page::next`] also points at its first chunk: a bounded read never returns such
    /// a transaction, an unbounded read from that cursor does once it is complete.
    ///
    /// A chunk whose earlier chunks are missing is reported as
    /// [`SkipReason::BrokenChunks`], once per transaction id and page, including chunks
    /// of a transaction that began before the cursor the scan started from
    /// ([`Cursor::beginning`], [`Cursor::at_version`]). A transaction that fails to
    /// decode is reported in [`Page::skipped`] and does not fail the page.
    ///
    /// # Errors
    ///
    /// [`ScanError::Source`] with the stream's error, [`ScanError::InvalidRows`] when the
    /// rows do not follow the contract above.
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            end_version = ?self.end_version,
            rows,
            transactions,
            skipped,
            exhausted,
        )
    )]
    pub async fn read_page<S, K, V, E>(
        &self,
        cursor: &Cursor,
        rows: S,
        mut should_stop: impl FnMut() -> bool,
    ) -> Result<Page, ScanError<E>>
    where
        S: Stream<Item = Result<(K, V), E>>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let range = self.range(cursor);
        let mut assembler = Assembler::new(cursor);
        let mut rows = pin!(rows);
        let mut read = 0usize;
        let page = loop {
            let Some(row) = poll_fn(|cx| rows.as_mut().poll_next(cx)).await else {
                break assembler.finish(false);
            };
            let (key, value) = row.map_err(ScanError::Source)?;
            let key = key.as_ref();
            let in_order = assembler.last_key.as_deref().is_none_or(|last| key > last);
            if key < range.begin.as_slice() || key >= range.end.as_slice() || !in_order {
                return Err(ScanError::InvalidRows { key: key.to_vec() });
            }
            assembler.push(key, value.as_ref());
            read += 1;
            if should_stop() {
                break assembler.finish(true);
            }
        };

        let span = tracing::Span::current();
        span.record("rows", read);
        span.record("transactions", page.transactions.len());
        span.record("skipped", page.skipped.len());
        span.record("exhausted", page.exhausted);
        Ok(page)
    }
}

fn version_of(versionstamp: &[u8; 10]) -> i64 {
    let mut version = [0u8; 8];
    version.copy_from_slice(&versionstamp[..8]);
    i64::from_be_bytes(version)
}

/// The fields of a chunk key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChunkKey {
    versionstamp: [u8; 10],
    id: [u8; 16],
    chunk: u32,
    total: u32,
}

impl ChunkKey {
    /// Parses a chunk key by offsets, ignoring what follows the total chunk count.
    /// Returns `None` for a key that is too short or has misplaced separators.
    fn parse(key: &[u8]) -> Option<Self> {
        if key.len() < MIN_KEY_LEN
            || !key.starts_with(PROFILE_PREFIX)
            || key[VERSIONSTAMP_END] != b'/'
            || key[ID_END] != b'/'
        {
            return None;
        }
        let mut versionstamp = [0u8; VERSIONSTAMP_LEN];
        versionstamp.copy_from_slice(&key[VERSIONSTAMP_START..VERSIONSTAMP_END]);
        let mut id = [0u8; ID_LEN];
        id.copy_from_slice(&key[ID_START..ID_END]);
        let be = |start: usize| {
            let mut n = [0u8; 4];
            n.copy_from_slice(&key[start..start + 4]);
            u32::from_be_bytes(n)
        };
        Some(ChunkKey {
            versionstamp,
            id,
            chunk: be(CHUNK_START),
            total: be(TOTAL_START),
        })
    }
}

/// A reassembly outcome of the [`Assembler`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Assembled {
    /// Every chunk of a transaction, concatenated.
    Complete {
        versionstamp: [u8; 10],
        id: [u8; 16],
        blob: Vec<u8>,
    },
    /// A transaction whose chunks could not be reassembled.
    Broken {
        versionstamp: [u8; 10],
        id: [u8; 16],
    },
}

/// An [`Assembled`] outcome with the keys of the rows it was made of.
#[derive(Debug)]
struct Output {
    /// Key of the first row of the transaction read by this page.
    first_key: Vec<u8>,
    /// Key of the last row of the transaction read by this page: the one that produced
    /// the outcome, or a later chunk of a broken transaction.
    last_key: Vec<u8>,
    assembled: Assembled,
}

/// A multi-chunk transaction being reassembled.
#[derive(Debug)]
struct Partial {
    /// Key of its first chunk.
    first_key: Vec<u8>,
    id: [u8; 16],
    /// Versionstamp of its first chunk.
    versionstamp: [u8; 10],
    /// Versionstamp of its last chunk so far.
    last_versionstamp: [u8; 10],
    total: u32,
    next_chunk: u32,
    /// Concatenated chunk values so far. Grows by the actual chunk lengths only, never by
    /// the untrusted total chunk count.
    blob: Vec<u8>,
}

/// Builds one [`Page`] from the rows of its range, fed in key order. Pure, no I/O:
/// [`ProfileScanner::read_page`] feeds it the caller's rows and tells it where it stopped.
///
/// A multi-chunk transaction starts at chunk 1 and its chunks come in order 1..=total,
/// with the same id and total and non-decreasing versionstamps: the client may write them
/// in two commits, so rows of other transactions can sit in between. Anything else
/// drops the partial buffer and reports the transaction as [`Assembled::Broken`], once
/// per id until a new chunk 1 of that id.
#[derive(Debug)]
struct Assembler {
    /// Where the page started.
    start: Cursor,
    /// Pending transactions by id.
    partials: HashMap<[u8; 16], Partial>,
    /// Ids reported as broken, with the index of their report in `outputs`, so that their
    /// remaining chunks are consumed silently. A new chunk 1 of the id removes it.
    broken: HashMap<[u8; 16], usize>,
    /// Outcomes, in the order they were produced.
    outputs: Vec<Output>,
    /// Last row fed.
    last_key: Option<Vec<u8>>,
}

impl Assembler {
    fn new(start: &Cursor) -> Self {
        Assembler {
            start: start.clone(),
            partials: HashMap::new(),
            broken: HashMap::new(),
            outputs: Vec::new(),
            last_key: None,
        }
    }

    /// Feeds one row.
    fn push(&mut self, key: &[u8], value: &[u8]) {
        self.last_key = Some(key.to_vec());
        match ChunkKey::parse(key) {
            Some(parsed) => self.process(parsed, key, value),
            None => tracing::warn!(key = ?key, "ignoring unparseable profiling key"),
        }
    }

    /// Processes a parsed row.
    fn process(&mut self, parsed: ChunkKey, key: &[u8], value: &[u8]) {
        if let Some(mut partial) = self.partials.remove(&parsed.id) {
            if parsed.total == partial.total
                && parsed.chunk == partial.next_chunk
                && parsed.versionstamp >= partial.last_versionstamp
            {
                partial.blob.extend_from_slice(value);
                if parsed.chunk == partial.total {
                    self.outputs.push(Output {
                        first_key: partial.first_key,
                        last_key: key.to_vec(),
                        assembled: Assembled::Complete {
                            versionstamp: partial.versionstamp,
                            id: parsed.id,
                            blob: partial.blob,
                        },
                    });
                    return;
                }
                // chunk < total, so this cannot overflow
                partial.next_chunk += 1;
                partial.last_versionstamp = parsed.versionstamp;
                self.partials.insert(parsed.id, partial);
                return;
            }
            self.report_broken(partial.first_key, partial.versionstamp, parsed.id, key);
        }

        if parsed.chunk == 1 && parsed.total >= 1 {
            // a new transaction under this id, its chunks are no longer those of a broken one
            self.broken.remove(&parsed.id);
        }
        if parsed.chunk == 1 && parsed.total == 1 {
            self.outputs.push(Output {
                first_key: key.to_vec(),
                last_key: key.to_vec(),
                assembled: Assembled::Complete {
                    versionstamp: parsed.versionstamp,
                    id: parsed.id,
                    blob: value.to_vec(),
                },
            });
            return;
        }
        if parsed.chunk == 1 && parsed.total > 1 {
            self.partials.insert(
                parsed.id,
                Partial {
                    first_key: key.to_vec(),
                    id: parsed.id,
                    versionstamp: parsed.versionstamp,
                    last_versionstamp: parsed.versionstamp,
                    total: parsed.total,
                    next_chunk: 2,
                    blob: value.to_vec(),
                },
            );
        } else {
            self.report_broken(key.to_vec(), parsed.versionstamp, parsed.id, key);
        }
    }

    /// Reports `id` as broken, unless it already is: then `key` becomes the last row of
    /// that report.
    fn report_broken(
        &mut self,
        first_key: Vec<u8>,
        versionstamp: [u8; 10],
        id: [u8; 16],
        key: &[u8],
    ) {
        if let Some(&index) = self.broken.get(&id) {
            if let Some(output) = self.outputs.get_mut(index) {
                output.last_key = key.to_vec();
            }
            return;
        }
        self.broken.insert(id, self.outputs.len());
        self.outputs.push(Output {
            first_key,
            last_key: key.to_vec(),
            assembled: Assembled::Broken { versionstamp, id },
        });
    }

    /// Where the next page resumes to read again the transaction whose first chunk is at
    /// `cut`: lowers `cut` to the first key of every outcome whose last row is at or after
    /// it, up to a fixed point. Returns the cut and the indexes of those outcomes, which
    /// the next page reports instead.
    fn cut(&self, mut cut: Vec<u8>) -> (Vec<u8>, Vec<usize>) {
        let mut by_last: Vec<usize> = (0..self.outputs.len()).collect();
        by_last.sort_by(|&a, &b| self.outputs[b].last_key.cmp(&self.outputs[a].last_key));
        let mut dropped = Vec::new();
        for index in by_last {
            let output = &self.outputs[index];
            if output.last_key < cut {
                break;
            }
            dropped.push(index);
            if output.first_key < cut {
                cut = output.first_key.clone();
            }
        }
        (cut, dropped)
    }

    /// Builds the page. `stopped` tells whether the caller's check stopped it right after
    /// its last row, rather than the end of its range.
    ///
    /// Pending transactions are left to the next page: it resumes at the
    /// [`cut`](Self::cut) before them and reads them again. When the page was stopped and
    /// that cut would not advance past the cursor, it resumes after the last row and
    /// breaks them instead. With nothing pending, it resumes after the last row.
    fn finish(mut self, stopped: bool) -> Page {
        let mut pending: Vec<Partial> = std::mem::take(&mut self.partials).into_values().collect();
        pending.sort_by(|a, b| a.first_key.cmp(&b.first_key));
        let after_last = match &self.last_key {
            Some(last) => key_after(last),
            None => self.start.key().to_vec(),
        };

        let mut kept = vec![true; self.outputs.len()];
        let cut = match pending.first() {
            None => None,
            Some(oldest) => {
                let (cut, dropped) = self.cut(oldest.first_key.clone());
                if stopped && cut.as_slice() <= self.start.key() {
                    // a cut at the cursor would not progress: break the pending ones
                    None
                } else {
                    for &index in &dropped {
                        kept[index] = false;
                    }
                    tracing::debug!(
                        pending = pending.len(),
                        left_to_next_page = dropped.len(),
                        stopped,
                        "page ended with incomplete transactions"
                    );
                    Some(cut)
                }
            }
        };
        let next = match cut {
            Some(cut) => cut,
            None => {
                // the page resumes after its last row: what is still pending cannot be
                // reassembled (with no cut, only a stopped page has pending ones)
                for partial in pending {
                    tracing::debug!("breaking incomplete transaction at a stop");
                    self.outputs.push(Output {
                        last_key: partial.first_key.clone(),
                        first_key: partial.first_key,
                        assembled: Assembled::Broken {
                            versionstamp: partial.versionstamp,
                            id: partial.id,
                        },
                    });
                    kept.push(true);
                }
                after_last
            }
        };

        let mut transactions = Vec::new();
        let mut skipped = Vec::new();
        for (output, kept) in self.outputs.into_iter().zip(kept) {
            if !kept {
                continue;
            }
            match output.assembled {
                Assembled::Complete {
                    versionstamp,
                    id,
                    blob,
                } => match decode_events(&blob) {
                    Ok((protocol_version, events)) => transactions.push(ProfiledTransaction {
                        version: version_of(&versionstamp),
                        versionstamp,
                        id,
                        protocol_version,
                        events,
                    }),
                    Err(err) => {
                        tracing::debug!(error = %err, "skipping undecodable transaction");
                        skipped.push(Skipped {
                            versionstamp,
                            id,
                            reason: SkipReason::Decode(err),
                        })
                    }
                },
                Assembled::Broken { versionstamp, id } => {
                    tracing::debug!("skipping transaction with broken chunks");
                    skipped.push(Skipped {
                        versionstamp,
                        id,
                        reason: SkipReason::BrokenChunks,
                    })
                }
            }
        }

        Page {
            transactions,
            skipped,
            next: Cursor { key: next },
            exhausted: !stopped,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{FutureExt, StreamExt, stream};
    use std::cell::Cell;
    use std::collections::BTreeSet;
    use std::convert::Infallible;
    use std::io;

    fn vs(version: u64, batch: u16) -> [u8; 10] {
        let mut out = [0u8; 10];
        out[..8].copy_from_slice(&version.to_be_bytes());
        out[8..].copy_from_slice(&batch.to_be_bytes());
        out
    }

    fn id(n: u8) -> [u8; 16] {
        [b'0' + n; 16]
    }

    fn key(versionstamp: [u8; 10], id: [u8; 16], chunk: u32, total: u32) -> Vec<u8> {
        let mut key = PROFILE_PREFIX.to_vec();
        key.extend_from_slice(&versionstamp);
        key.push(b'/');
        key.extend_from_slice(&id);
        key.push(b'/');
        key.extend_from_slice(&chunk.to_be_bytes());
        key.extend_from_slice(&total.to_be_bytes());
        key.extend_from_slice(b"/user-id");
        key
    }

    type Row = (Vec<u8>, Vec<u8>);
    type Rows = Vec<Row>;

    /// Feeds all rows from the beginning, returns the outcomes and the first key of the
    /// oldest pending transaction.
    fn reassemble(rows: &[(Vec<u8>, Vec<u8>)]) -> (Vec<Assembled>, Option<Vec<u8>>) {
        let mut assembler = Assembler::new(&Cursor::beginning());
        for (k, v) in rows {
            assembler.push(k, v);
        }
        let pending = assembler
            .partials
            .values()
            .map(|p| p.first_key.clone())
            .min();
        let out = assembler.outputs.into_iter().map(|o| o.assembled).collect();
        (out, pending)
    }

    fn complete(versionstamp: [u8; 10], id: [u8; 16], blob: &[u8]) -> Assembled {
        Assembled::Complete {
            versionstamp,
            id,
            blob: blob.to_vec(),
        }
    }

    fn broken(versionstamp: [u8; 10], id: [u8; 16]) -> Assembled {
        Assembled::Broken { versionstamp, id }
    }

    fn never() -> impl FnMut() -> bool {
        || false
    }

    fn always() -> impl FnMut() -> bool {
        || true
    }

    /// Stops at every `k`-th check.
    fn every(k: usize) -> impl FnMut() -> bool {
        let mut checks = 0;
        move || {
            checks += 1;
            checks % k == 0
        }
    }

    /// Stops after the `n`-th row of the page.
    fn after(n: usize) -> impl FnMut() -> bool {
        let mut checks = 0;
        move || {
            checks += 1;
            checks >= n
        }
    }

    /// Stops pseudo-randomly, about once every `m` checks, with a state shared by every
    /// page so that consecutive pages stop at different rows.
    fn random(state: &Cell<u64>, m: u64) -> impl FnMut() -> bool + '_ {
        move || {
            let next = state
                .get()
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state.set(next);
            (next >> 33) % m == 0
        }
    }

    /// Runs `read_page` over `rows`, which must be ready (no I/O in these tests).
    fn read<E>(
        scanner: &ProfileScanner,
        cursor: &Cursor,
        rows: Vec<Result<Row, E>>,
        should_stop: impl FnMut() -> bool,
    ) -> Result<Page, ScanError<E>> {
        scanner
            .read_page(cursor, stream::iter(rows), should_stop)
            .now_or_never()
            .expect("a ready stream completes the page in one poll")
    }

    /// Runs one page over the sorted `rows`, feeding the ones of its range like a caller
    /// honoring the contract of `read_page`.
    fn run_page(rows: &Rows, cursor: &Cursor, should_stop: impl FnMut() -> bool) -> Page {
        let scanner = ProfileScanner::new();
        let range = scanner.range(cursor);
        let rows = rows
            .iter()
            .filter(|(k, _)| *k >= range.begin && *k < range.end)
            .cloned()
            .map(Ok::<_, Infallible>)
            .collect();
        match read(&scanner, cursor, rows, should_stop) {
            Ok(page) => page,
            Err(err) => panic!("{err}"),
        }
    }

    /// Runs pages until exhausted, each with a fresh stop check, returns every reported
    /// transaction and skip.
    fn run_pages<C: FnMut() -> bool>(
        rows: &Rows,
        mut should_stop: impl FnMut() -> C,
    ) -> (Vec<ProfiledTransaction>, Vec<Skipped>) {
        let mut cursor = Cursor::beginning();
        let (mut txs, mut skipped) = (Vec::new(), Vec::new());
        for _ in 0..10_000 {
            let page = run_page(rows, &cursor, should_stop());
            txs.extend(page.transactions);
            skipped.extend(page.skipped);
            let next = Cursor::from_bytes(page.next.as_bytes().to_vec()).unwrap();
            assert_eq!(next, page.next);
            if page.exhausted {
                return (txs, skipped);
            }
            assert!(next.key() > cursor.key(), "no progress");
            cursor = next;
        }
        panic!("pages never exhausted");
    }

    /// A valid blob with no event, split in `n` chunks.
    fn blob_chunks(n: usize) -> Vec<Vec<u8>> {
        let blob = ProtocolVersion::V7_4.0.to_le_bytes();
        let size = blob.len().div_ceil(n);
        let mut chunks: Vec<Vec<u8>> = blob.chunks(size).map(<[u8]>::to_vec).collect();
        chunks.resize(n, Vec::new());
        chunks
    }

    /// Rows of a transaction whose chunks are spread over the given versionstamps.
    fn txn(id_n: u8, stamps: &[[u8; 10]]) -> Rows {
        let total = stamps.len() as u32;
        blob_chunks(stamps.len())
            .into_iter()
            .zip(stamps)
            .enumerate()
            .map(|(i, (chunk, stamp))| (key(*stamp, id(id_n), i as u32 + 1, total), chunk))
            .collect()
    }

    fn txn_ids(txs: &[ProfiledTransaction]) -> Vec<[u8; 16]> {
        txs.iter().map(|t| t.id).collect()
    }

    fn skipped_ids(skipped: &[Skipped]) -> Vec<[u8; 16]> {
        skipped.iter().map(|s| s.id).collect()
    }

    #[test]
    fn parses_real_key_layout() {
        let k = key(vs(7, 1), id(1), 2, 3);
        assert_eq!(
            ChunkKey::parse(&k),
            Some(ChunkKey {
                versionstamp: vs(7, 1),
                id: id(1),
                chunk: 2,
                total: 3,
            })
        );
        // the tail after the chunk fields is ignored, and may be absent
        assert!(ChunkKey::parse(&k[..MIN_KEY_LEN]).is_some());
        for len in 0..MIN_KEY_LEN {
            assert_eq!(ChunkKey::parse(&k[..len]), None, "len {len}");
        }
        let mut bad = k.clone();
        bad[VERSIONSTAMP_END] = b'x';
        assert_eq!(ChunkKey::parse(&bad), None);
    }

    #[test]
    fn single_and_multi_chunk() {
        let (out, pending) = reassemble(&[
            (key(vs(1, 0), id(1), 1, 1), b"one".to_vec()),
            (key(vs(1, 0), id(2), 1, 3), b"ab".to_vec()),
            (key(vs(1, 0), id(2), 2, 3), b"cd".to_vec()),
            (key(vs(1, 0), id(2), 3, 3), b"e".to_vec()),
        ]);
        assert_eq!(
            out,
            vec![
                complete(vs(1, 0), id(1), b"one"),
                complete(vs(1, 0), id(2), b"abcde")
            ]
        );
        assert_eq!(pending, None);
    }

    #[test]
    fn straddling_transaction_with_other_rows_in_between() {
        // id 1 has chunks 1-2 in the first commit and chunk 3 in the second one, with
        // rows of other transactions of both commits in between.
        let (out, pending) = reassemble(&[
            (key(vs(1, 0), id(1), 1, 3), b"a".to_vec()),
            (key(vs(1, 0), id(1), 2, 3), b"b".to_vec()),
            (key(vs(1, 0), id(2), 1, 1), b"B".to_vec()),
            (key(vs(2, 0), id(0), 1, 1), b"Z".to_vec()),
            (key(vs(2, 0), id(1), 3, 3), b"c".to_vec()),
            (key(vs(2, 0), id(3), 1, 1), b"D".to_vec()),
        ]);
        assert_eq!(
            out,
            vec![
                complete(vs(1, 0), id(2), b"B"),
                complete(vs(2, 0), id(0), b"Z"),
                complete(vs(1, 0), id(1), b"abc"),
                complete(vs(2, 0), id(3), b"D"),
            ]
        );
        assert_eq!(pending, None);
    }

    #[test]
    fn straddler_with_many_rows_in_between_completes() {
        let mut rows: Rows = vec![(key(vs(1, 0), id(1), 1, 2), b"a".to_vec())];
        for batch in 1..=5000 {
            rows.push((key(vs(1, batch), id(2), 1, 1), vec![0; 100]));
        }
        rows.push((key(vs(2, 0), id(1), 2, 2), b"b".to_vec()));
        let (out, pending) = reassemble(&rows);
        assert_eq!(out.len(), 5001);
        assert!(out.iter().all(|o| matches!(o, Assembled::Complete { .. })));
        assert_eq!(out.last(), Some(&complete(vs(1, 0), id(1), b"ab")));
        assert_eq!(pending, None);
    }

    #[test]
    fn interleaved_transactions_both_complete() {
        let (out, _) = reassemble(&[
            (key(vs(1, 0), id(1), 1, 2), b"a".to_vec()),
            (key(vs(1, 0), id(2), 1, 2), b"x".to_vec()),
            (key(vs(2, 0), id(1), 2, 2), b"b".to_vec()),
            (key(vs(2, 0), id(2), 2, 2), b"y".to_vec()),
        ]);
        assert_eq!(
            out,
            vec![
                complete(vs(1, 0), id(1), b"ab"),
                complete(vs(1, 0), id(2), b"xy")
            ]
        );
    }

    #[test]
    fn missing_chunk_is_broken_once() {
        let (out, pending) = reassemble(&[
            (key(vs(1, 0), id(1), 1, 4), b"a".to_vec()),
            (key(vs(1, 0), id(1), 2, 4), b"b".to_vec()),
            (key(vs(1, 0), id(1), 4, 4), b"d".to_vec()),
            (key(vs(1, 0), id(2), 1, 1), b"B".to_vec()),
        ]);
        assert_eq!(
            out,
            vec![broken(vs(1, 0), id(1)), complete(vs(1, 0), id(2), b"B")]
        );
        assert_eq!(pending, None);
    }

    #[test]
    fn orphan_chunks_are_broken_once() {
        let (out, _) = reassemble(&[
            (key(vs(1, 0), id(1), 2, 3), b"b".to_vec()),
            (key(vs(1, 0), id(1), 3, 3), b"c".to_vec()),
            (key(vs(1, 0), id(2), 1, 1), b"B".to_vec()),
        ]);
        assert_eq!(
            out,
            vec![broken(vs(1, 0), id(1)), complete(vs(1, 0), id(2), b"B")]
        );
    }

    #[test]
    fn out_of_order_or_inconsistent_chunks_are_broken() {
        let (out, _) = reassemble(&[
            (key(vs(1, 0), id(1), 1, 3), b"a".to_vec()),
            (key(vs(1, 0), id(1), 3, 3), b"c".to_vec()),
            (key(vs(1, 0), id(1), 2, 3), b"b".to_vec()),
        ]);
        assert_eq!(out, vec![broken(vs(1, 0), id(1))]);
        // other total
        let (out, _) = reassemble(&[
            (key(vs(1, 0), id(1), 1, 2), b"a".to_vec()),
            (key(vs(1, 0), id(1), 2, 3), b"b".to_vec()),
        ]);
        assert_eq!(out, vec![broken(vs(1, 0), id(1))]);
        // decreasing versionstamp (only possible with rows fed out of key order)
        let (out, _) = reassemble(&[
            (key(vs(2, 0), id(1), 1, 2), b"a".to_vec()),
            (key(vs(1, 0), id(1), 2, 2), b"b".to_vec()),
        ]);
        assert_eq!(out, vec![broken(vs(2, 0), id(1))]);
    }

    #[test]
    fn chunk_one_restarts_a_pending_transaction() {
        let (out, _) = reassemble(&[
            (key(vs(1, 0), id(1), 1, 2), b"a".to_vec()),
            (key(vs(2, 0), id(1), 1, 2), b"x".to_vec()),
            (key(vs(2, 0), id(1), 2, 2), b"y".to_vec()),
        ]);
        assert_eq!(
            out,
            vec![broken(vs(1, 0), id(1)), complete(vs(2, 0), id(1), b"xy")]
        );
    }

    #[test]
    fn nonsense_chunk_numbers_are_broken() {
        for (chunk, total) in [(0, 0), (0, 1), (1, 0), (2, 1), (u32::MAX, u32::MAX)] {
            let (out, _) = reassemble(&[(key(vs(1, 0), id(1), chunk, total), b"a".to_vec())]);
            assert_eq!(out, vec![broken(vs(1, 0), id(1))], "{chunk}/{total}");
        }
        // a huge total does not allocate for it
        let k = key(vs(1, 0), id(1), 1, u32::MAX);
        let (out, pending) = reassemble(&[(k.clone(), b"a".to_vec())]);
        assert_eq!(out, vec![]);
        assert_eq!(pending, Some(k));
    }

    #[test]
    fn reused_id_that_breaks_again_is_reported_again() {
        let (out, pending) = reassemble(&[
            (key(vs(1, 0), id(1), 1, 3), b"a".to_vec()),
            (key(vs(1, 0), id(1), 3, 3), b"c".to_vec()),
            (key(vs(2, 0), id(1), 1, 3), b"a".to_vec()),
            (key(vs(2, 0), id(1), 3, 3), b"c".to_vec()),
            (key(vs(3, 0), id(1), 1, 1), b"x".to_vec()),
            (key(vs(4, 0), id(1), 2, 2), b"y".to_vec()),
        ]);
        assert_eq!(
            out,
            vec![
                broken(vs(1, 0), id(1)),
                broken(vs(2, 0), id(1)),
                complete(vs(3, 0), id(1), b"x"),
                broken(vs(4, 0), id(1)),
            ]
        );
        assert_eq!(pending, None);
    }

    #[test]
    fn unparseable_keys_are_consumed_without_output() {
        let short = [PROFILE_PREFIX.as_slice(), b"short"].concat();
        let (out, pending) = reassemble(&[(short, b"x".to_vec())]);
        assert_eq!(out, vec![]);
        assert_eq!(pending, None);
    }

    #[test]
    fn should_stop_is_called_after_every_row_and_stops_reading() {
        let v1 = vs(1, 0);
        let mut rows: Rows = txn(1, &[v1, v1, v1]);
        rows.extend(txn(2, &[v1]));
        rows.sort();
        let mut checks = 0;
        let page = run_page(&rows, &Cursor::beginning(), || {
            checks += 1;
            false
        });
        assert_eq!(checks, rows.len());
        assert!(page.exhausted);

        // the page stops at the first `true`, without reading further
        let polled = Cell::new(0);
        let stream = stream::iter(rows.clone()).map(|row| {
            polled.set(polled.get() + 1);
            Ok::<_, Infallible>(row)
        });
        let page = ProfileScanner::new()
            .read_page(&Cursor::beginning(), stream, after(3))
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(polled.get(), 3);
        assert!(!page.exhausted);
        assert_eq!(txn_ids(&page.transactions), vec![id(1)]);
        assert_eq!(page.next.key(), key_after(&rows[2].0).as_slice());
    }

    #[test]
    fn stop_with_nothing_pending_resumes_after_the_row() {
        let mut rows: Rows = Vec::new();
        for n in 0..3u8 {
            rows.extend(txn(n, &[vs(1, 0)]));
        }
        rows.sort();
        let page = run_page(&rows, &Cursor::beginning(), after(2));
        assert!(!page.exhausted);
        assert_eq!(txn_ids(&page.transactions), vec![id(0), id(1)]);
        assert_eq!(page.next.key(), key_after(&rows[1].0).as_slice());
    }

    #[test]
    fn stop_while_straddler_is_pending_cuts_before_it() {
        let (v1, v2) = (vs(1, 0), vs(2, 0));
        let mut rows: Rows = txn(0, &[v1]);
        rows.extend(txn(1, &[v1, v2]));
        rows.extend(txn(2, &[v1]));
        rows.extend(txn(3, &[v2]));
        rows.sort();
        // id 0, id 1 chunk 1, id 2, id 1 chunk 2, id 3: stop right after id 2
        let page = run_page(&rows, &Cursor::beginning(), after(3));
        assert!(!page.exhausted);
        // id 2 completed after id 1's first chunk: left to the next page
        assert_eq!(txn_ids(&page.transactions), vec![id(0)]);
        assert!(page.skipped.is_empty());
        assert_eq!(page.next.key(), rows[1].0.as_slice());

        // the next page returns the straddler whole, and id 2 once
        let page = run_page(&rows, &page.next, never());
        assert!(page.exhausted);
        assert_eq!(txn_ids(&page.transactions), vec![id(2), id(1), id(3)]);
        assert!(page.skipped.is_empty());
    }

    #[test]
    fn stop_at_the_cursor_with_a_pending_record_breaks_it_and_progresses() {
        let (v1, v2) = (vs(1, 0), vs(2, 0));
        let mut rows: Rows = txn(1, &[v1, v2]);
        rows.extend(txn(2, &[v1]));
        rows.extend(txn(3, &[v2]));
        rows.sort();
        // id 1 chunk 1, id 2, id 1 chunk 2, id 3
        let cursor = Cursor::from_bytes(rows[0].0.clone()).unwrap();
        for stop in [1, 2] {
            let page = run_page(&rows, &cursor, after(stop));
            assert!(!page.exhausted);
            assert_eq!(txn_ids(&page.transactions), &[id(2)][..stop - 1]);
            assert_eq!(skipped_ids(&page.skipped), vec![id(1)]);
            assert_eq!(page.skipped[0].reason, SkipReason::BrokenChunks);
            assert_eq!(page.skipped[0].versionstamp, v1);
            assert_eq!(page.next.key(), key_after(&rows[stop - 1].0).as_slice());
        }

        // the rest of id 1 shows up again as broken on the next page
        let page = run_page(
            &rows,
            &Cursor::from_bytes(key_after(&rows[1].0)).unwrap(),
            never(),
        );
        assert!(page.exhausted);
        assert_eq!(txn_ids(&page.transactions), vec![id(3)]);
        assert_eq!(skipped_ids(&page.skipped), vec![id(1)]);
        assert_eq!(page.skipped[0].versionstamp, v2);
    }

    #[test]
    fn lost_chunk_is_read_again_then_broken_when_stopped_at_the_cursor() {
        let v1 = vs(1, 0);
        // id 1 never gets its third chunk
        let mut rows: Rows = txn(1, &[v1, v1, v1]);
        rows.truncate(2);
        rows.extend(txn(2, &[v1, v1, v1]));
        rows.sort();
        // the stop leaves id 1 pending: the page resumes at it to read it again
        let page = run_page(&rows, &Cursor::beginning(), after(2));
        assert!(!page.exhausted);
        assert!(page.transactions.is_empty());
        assert!(page.skipped.is_empty());
        assert_eq!(page.next.key(), rows[0].0.as_slice());

        // from there, the cut would not advance: id 1 is broken and the page moves on
        let page = run_page(&rows, &page.next, after(2));
        assert!(!page.exhausted);
        assert!(page.transactions.is_empty());
        assert_eq!(skipped_ids(&page.skipped), vec![id(1)]);
        assert_eq!(page.next.key(), key_after(&rows[1].0).as_slice());

        let page = run_page(&rows, &page.next, never());
        assert!(page.exhausted);
        assert_eq!(txn_ids(&page.transactions), vec![id(2)]);

        // at the end of the range, the page resumes at the pending transaction
        let lost: Rows = rows[..2].to_vec();
        let page = run_page(&lost, &Cursor::beginning(), never());
        assert!(page.exhausted);
        assert!(page.skipped.is_empty());
        assert_eq!(page.next.key(), lost[0].0.as_slice());
    }

    /// id 0 (one chunk), then id 1 (three contiguous chunks), then id 2 (one chunk).
    fn big_record_rows() -> Rows {
        let v1 = vs(1, 0);
        let mut rows: Rows = txn(0, &[v1]);
        rows.extend(txn(1, &[v1, v1, v1]));
        rows.extend(txn(2, &[v1]));
        rows.sort();
        rows
    }

    #[test]
    fn big_record_interrupted_by_a_stop_is_returned_whole_by_next_page() {
        let rows = big_record_rows();
        // stopped on id 1's first or second chunk
        for stop in [2, 3] {
            let page = run_page(&rows, &Cursor::beginning(), after(stop));
            assert!(!page.exhausted);
            assert_eq!(txn_ids(&page.transactions), vec![id(0)], "stop {stop}");
            assert!(page.skipped.is_empty(), "stop {stop}");
            assert_eq!(page.next.key(), rows[1].0.as_slice(), "stop {stop}");

            let page = run_page(&rows, &page.next, never());
            assert!(page.exhausted);
            assert_eq!(txn_ids(&page.transactions), vec![id(1), id(2)]);
            assert!(page.skipped.is_empty());
        }
    }

    #[test]
    fn stop_before_one_record_is_read_from_the_cursor_breaks_it_and_progresses() {
        let rows = big_record_rows();
        let cursor = Cursor::from_bytes(rows[1].0.clone()).unwrap();
        let page = run_page(&rows, &cursor, always());
        assert!(!page.exhausted);
        assert!(page.transactions.is_empty());
        assert_eq!(skipped_ids(&page.skipped), vec![id(1)]);
        assert_eq!(page.next.key(), key_after(&rows[1].0).as_slice());

        // its remaining chunks are reported again, by each page that reads them
        let page = run_page(&rows, &page.next, always());
        assert_eq!(page.next.key(), key_after(&rows[2].0).as_slice());
        assert_eq!(skipped_ids(&page.skipped), vec![id(1)]);
        let (txs, skipped) = run_pages(&rows, always);
        assert_eq!(txn_ids(&txs), vec![id(0), id(2)]);
        assert_eq!(skipped_ids(&skipped), vec![id(1); 3]);
    }

    #[test]
    fn end_of_range_with_pending_straddler_cuts_before_it() {
        let (v1, v2, v3) = (vs(1, 0), vs(2, 0), vs(3, 0));
        let mut rows: Rows = Vec::new();
        rows.extend(txn(0, &[v1])); // Z, before everything
        rows.extend(txn(1, &[v1, v2])); // T1, starts before P, completes after it
        rows.extend(txn(2, &[v1, v3])); // P, its second commit is not written yet
        rows.extend(txn(3, &[v1])); // A, between P's chunk 1 and T1's chunk 2
        rows.extend(txn(4, &[v2])); // C, after everything
        rows.sort();
        let t1_first = rows[1].0.clone();
        let written: Rows = rows
            .iter()
            .filter(|(k, _)| k[VERSIONSTAMP_START..] < v3[..])
            .cloned()
            .collect();

        let page = run_page(&written, &Cursor::beginning(), never());
        assert!(page.exhausted);
        // T1 completes after P's first chunk: dropped with everything after it, and the
        // cut goes back to T1's first chunk
        assert_eq!(txn_ids(&page.transactions), vec![id(0)]);
        assert!(page.skipped.is_empty());
        assert_eq!(page.next.key(), t1_first.as_slice());

        // once P's second commit is written, the next page returns the rest once
        let next = run_page(&rows, &page.next, never());
        assert!(next.exhausted);
        let mut got = txn_ids(&next.transactions);
        got.sort();
        assert_eq!(got, vec![id(1), id(2), id(3), id(4)]);
        assert!(next.skipped.is_empty());

        // a bounded read that never sees P's second commit keeps the same cursor
        let again = run_page(&written, &page.next, never());
        assert!(again.exhausted);
        assert!(again.transactions.is_empty());
        assert_eq!(again.next, page.next);

        // same through end_version, and with a stop on T1's second chunk
        let scanner = ProfileScanner::new().end_version(3);
        let bounded = |cursor: &Cursor, should_stop: &mut dyn FnMut() -> bool| {
            let range = scanner.range(cursor);
            let rows = rows
                .iter()
                .filter(|(k, _)| *k >= range.begin && *k < range.end)
                .cloned()
                .map(Ok::<_, Infallible>)
                .collect();
            read(&scanner, cursor, rows, should_stop).unwrap()
        };
        let page = bounded(&Cursor::beginning(), &mut never());
        assert_eq!(txn_ids(&page.transactions), vec![id(0)]);
        assert_eq!(page.next.key(), t1_first.as_slice());
        let stop_on_t1 = written
            .iter()
            .position(|(k, _)| k[ID_START..ID_END] == id(1) && k[VERSIONSTAMP_START..] >= v2[..])
            .unwrap()
            + 1;
        let page = bounded(&Cursor::beginning(), &mut after(stop_on_t1));
        assert!(!page.exhausted);
        assert_eq!(txn_ids(&page.transactions), vec![id(0)]);
        assert_eq!(page.next.key(), t1_first.as_slice());
    }

    #[test]
    fn end_of_range_cut_also_drops_broken_reports_read_again() {
        let (v1, v2) = (vs(1, 0), vs(2, 0));
        let mut rows: Rows = Vec::new();
        rows.extend(txn(1, &[v1, v2])); // pending at the end of the range
        rows.push((key(v1, id(2), 2, 3), Vec::new())); // orphan chunks of id 2
        rows.push((key(v2, id(2), 3, 3), Vec::new()));
        rows.sort();
        let written: Rows = rows
            .iter()
            .filter(|(k, _)| !(k[VERSIONSTAMP_START..] >= v2[..] && k[ID_START..ID_END] == id(1)))
            .cloned()
            .collect();
        let page = run_page(&written, &Cursor::beginning(), never());
        // id 2's report is left to the next page, which reads its chunks again
        assert!(page.skipped.is_empty());
        assert_eq!(page.next.key(), rows[0].0.as_slice());
        let page = run_page(&rows, &page.next, never());
        assert_eq!(txn_ids(&page.transactions), vec![id(1)]);
        assert_eq!(skipped_ids(&page.skipped), vec![id(2)]);
    }

    /// Checks that `txs` and `skipped` hold every id of `all` once: returned at most once,
    /// otherwise reported broken.
    fn assert_each_once(
        all: &BTreeSet<[u8; 16]>,
        (txs, skipped): (Vec<ProfiledTransaction>, Vec<Skipped>),
        context: &str,
    ) -> BTreeSet<[u8; 16]> {
        let returned: BTreeSet<_> = txn_ids(&txs).into_iter().collect();
        assert_eq!(returned.len(), txs.len(), "twice: {context}");
        assert!(
            skipped.iter().all(|s| s.reason == SkipReason::BrokenChunks),
            "{context}"
        );
        let broken: BTreeSet<_> = skipped_ids(&skipped).into_iter().collect();
        // lost only by being broken by a stop
        assert!(returned.is_disjoint(&broken), "{context}");
        assert_eq!(
            &returned.union(&broken).copied().collect::<BTreeSet<_>>(),
            all,
            "{context}"
        );
        broken
    }

    #[test]
    fn paging_returns_every_transaction_at_most_once() {
        let (v1, v2, v3) = (vs(1, 0), vs(2, 0), vs(3, 0));
        let mut rows: Rows = Vec::new();
        rows.extend(txn(1, &[v1, v1, v2]));
        rows.extend(txn(2, &[v1]));
        rows.extend(txn(3, &[v1, v2]));
        rows.extend(txn(4, &[v2]));
        rows.extend(txn(5, &[v2, v2]));
        rows.extend(txn(6, &[v2, v3, v3]));
        rows.extend(txn(7, &[v3]));
        rows.extend(txn(8, &[v3, v3, v3]));
        rows.extend(txn(9, &[v3]));
        rows.sort();
        let all: BTreeSet<[u8; 16]> = (1..=9).map(id).collect();

        let (txs, skipped) = run_pages(&rows, never);
        assert!(skipped.is_empty());
        assert_eq!(txs.len(), all.len());
        assert_eq!(txn_ids(&txs).into_iter().collect::<BTreeSet<_>>(), all);

        for k in 1..=rows.len() {
            assert_each_once(&all, run_pages(&rows, || every(k)), &format!("every {k}"));
            let broken =
                assert_each_once(&all, run_pages(&rows, || after(k)), &format!("after {k}"));
            if k == rows.len() {
                // a page long enough to read everything from any cursor loses nothing
                assert!(broken.is_empty());
            }
        }
        for m in 2..=6 {
            for seed in 0..50 {
                let state = Cell::new(seed);
                assert_each_once(
                    &all,
                    run_pages(&rows, || random(&state, m)),
                    &format!("random m {m} seed {seed}"),
                );
            }
        }
    }

    #[test]
    fn paging_single_chunk_transactions_returns_each_exactly_once() {
        let mut rows: Rows = Vec::new();
        for n in 0..9u8 {
            rows.extend(txn(n, &[vs(u64::from(n / 3), 0)]));
        }
        rows.sort();
        let expected: Vec<_> = (0..9).map(id).collect();
        for k in 1..=5 {
            let (txs, skipped) = run_pages(&rows, || every(k));
            assert!(skipped.is_empty(), "every {k}");
            let mut got = txn_ids(&txs);
            got.sort();
            assert_eq!(got, expected, "every {k}");
        }
        for seed in 0..50 {
            let state = Cell::new(seed);
            let (txs, skipped) = run_pages(&rows, || random(&state, 3));
            assert!(skipped.is_empty(), "seed {seed}");
            let mut got = txn_ids(&txs);
            got.sort();
            assert_eq!(got, expected, "seed {seed}");
        }
    }

    #[test]
    fn invalid_rows_are_rejected() {
        let scanner = ProfileScanner::new();
        let (a, b) = (key(vs(1, 0), id(1), 1, 1), key(vs(2, 0), id(2), 1, 1));
        let ok = |k: &Vec<u8>| Ok::<_, Infallible>((k.clone(), Vec::new()));
        let invalid = |result: Result<Page, ScanError<Infallible>>| match result {
            Err(ScanError::InvalidRows { key }) => key,
            other => panic!("{other:?}"),
        };
        // out of order, and a repeated key
        let rows = vec![ok(&b), ok(&a)];
        assert_eq!(
            invalid(read(&scanner, &Cursor::beginning(), rows, never())),
            a
        );
        let rows = vec![ok(&a), ok(&a)];
        assert_eq!(
            invalid(read(&scanner, &Cursor::beginning(), rows, never())),
            a
        );
        // before the cursor
        let rows = vec![ok(&a)];
        assert_eq!(
            invalid(read(&scanner, &Cursor::at_version(2), rows, never())),
            a
        );
        // at or past end_version, and outside the keyspace
        let rows = vec![ok(&a), ok(&b)];
        let bounded = scanner.clone().end_version(2);
        assert_eq!(
            invalid(read(&bounded, &Cursor::beginning(), rows, never())),
            b
        );
        let rows = vec![ok(&PROFILE_END.to_vec())];
        assert_eq!(
            invalid(read(&scanner, &Cursor::beginning(), rows, never())),
            PROFILE_END.to_vec()
        );
        let outside = b"\xff\x02/other".to_vec();
        let rows = vec![ok(&outside)];
        assert_eq!(
            invalid(read(&scanner, &Cursor::beginning(), rows, never())),
            outside
        );
    }

    #[test]
    fn source_error_is_propagated() {
        let rows = vec![
            Ok((key(vs(1, 0), id(1), 1, 1), Vec::new())),
            Err("boom"),
            Ok((key(vs(2, 0), id(2), 1, 1), Vec::new())),
        ];
        match read(&ProfileScanner::new(), &Cursor::beginning(), rows, never()) {
            Err(ScanError::Source("boom")) => {}
            other => panic!("{other:?}"),
        }
    }

    /// A caller's error type, the way `FdbBindingError` looks from `into_error`'s point of
    /// view: a `From` impl for the stream's error, and a catch-all for anything boxed.
    #[derive(Debug)]
    enum TargetError {
        Source(io::Error),
        Custom(Box<dyn std::error::Error + Send + Sync>),
    }

    impl From<io::Error> for TargetError {
        fn from(err: io::Error) -> Self {
            TargetError::Source(err)
        }
    }

    #[test]
    fn into_error_maps_source_through_from() {
        let err: ScanError<io::Error> = ScanError::Source(io::Error::other("boom"));
        match err.into_error(TargetError::Custom) {
            TargetError::Source(err) => assert_eq!(err.to_string(), "boom"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn into_error_boxes_invalid_rows_for_custom() {
        let key = b"row-key".to_vec();
        let err: ScanError<io::Error> = ScanError::InvalidRows { key: key.clone() };
        match err.into_error(TargetError::Custom) {
            TargetError::Custom(boxed) => {
                // the message is preserved, with the key readable in it
                assert_eq!(
                    boxed.to_string(),
                    format!(
                        "profiling row {key:?} is outside the scanned range or not after the previous row"
                    )
                );
                // and the original variant, still carrying the key, downcasts out of it
                match boxed
                    .downcast_ref::<ScanError<Infallible>>()
                    .expect("boxed error downcasts to ScanError<Infallible>")
                {
                    ScanError::InvalidRows { key: got } => assert_eq!(got, &key),
                    other => panic!("{other:?}"),
                }
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn range_is_the_cursor_to_end_version_and_never_inverted() {
        let scanner = ProfileScanner::new();
        let cursor = Cursor::at_version(5);
        assert_eq!(
            scanner.range(&cursor),
            ScanRange {
                begin: cursor.as_bytes().to_vec(),
                end: PROFILE_END.to_vec(),
            }
        );
        let bounded = scanner.end_version(7);
        assert_eq!(bounded.range(&cursor).end, Cursor::at_version(7).as_bytes());
        // at or past end_version, the range is empty and the page keeps the cursor
        for version in [7, 9] {
            let cursor = Cursor::at_version(version);
            let range = bounded.range(&cursor);
            assert_eq!(range.begin, cursor.as_bytes());
            assert_eq!(range.end, range.begin);
            let page = read(
                &bounded,
                &cursor,
                Vec::<Result<_, Infallible>>::new(),
                always(),
            )
            .unwrap();
            assert!(page.exhausted);
            assert_eq!(page.next, cursor);
        }
    }

    #[test]
    fn empty_range_keeps_the_cursor() {
        let cursor = Cursor::at_version(42);
        let page = run_page(&Vec::new(), &cursor, always());
        assert!(page.exhausted);
        assert_eq!(page.next, cursor);
    }

    #[test]
    fn page_decodes_and_reports_skips() {
        let valid = ProtocolVersion::V7_4.0.to_le_bytes().to_vec();
        let rows: Rows = vec![
            (key(vs(5, 0), id(1), 1, 1), valid),
            (key(vs(5, 0), id(2), 2, 2), Vec::new()),
            (key(vs(5, 0), id(3), 1, 1), b"garbage".to_vec()),
        ];
        let page = run_page(&rows, &Cursor::beginning(), never());
        assert_eq!(
            page.transactions,
            vec![ProfiledTransaction {
                version: 5,
                versionstamp: vs(5, 0),
                id: id(1),
                protocol_version: ProtocolVersion::V7_4,
                events: vec![],
            }]
        );
        assert_eq!(page.skipped.len(), 2);
        assert_eq!(page.skipped[0].reason, SkipReason::BrokenChunks);
        assert!(matches!(page.skipped[1].reason, SkipReason::Decode(_)));
        assert_eq!(page.next.key(), key_after(&rows[2].0).as_slice());
        assert!(page.exhausted);
    }

    #[test]
    fn cursor_round_trip_and_validation() {
        let a = key(vs(1, 0), id(1), 1, 2);
        for cursor in [
            Cursor::beginning(),
            Cursor::at_version(0),
            Cursor::at_version(123_456),
            Cursor::from_bytes(a.clone()).unwrap(),
        ] {
            assert_eq!(
                Cursor::from_bytes(cursor.as_bytes().to_vec()),
                Ok(cursor.clone())
            );
        }
        assert_eq!(Cursor::beginning().as_bytes(), PROFILE_PREFIX);
        assert_eq!(Cursor::at_version(-5), Cursor::at_version(0));
        let v7 = [PROFILE_PREFIX.as_slice(), &7i64.to_be_bytes(), b"\x00\x00"].concat();
        assert_eq!(Cursor::at_version(7).as_bytes(), v7.as_slice());

        let garbage: Vec<Vec<u8>> = vec![
            Vec::new(),
            b"nope".to_vec(),
            PROFILE_PREFIX[..PROFILE_PREFIX.len() - 1].to_vec(),
            b"\xff\xff".to_vec(),
            // the former two-key format
            [&[1u8, 0, 0, 0, 32][..], PROFILE_PREFIX, PROFILE_PREFIX].concat(),
        ];
        for bytes in garbage {
            assert_eq!(
                Cursor::from_bytes(bytes.clone()),
                Err(InvalidCursor),
                "{bytes:?}"
            );
        }
    }
}
