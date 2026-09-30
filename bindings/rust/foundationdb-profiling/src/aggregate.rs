//! Aggregated read/write counts over profiled transactions.
//!
//! [`Aggregator`] mirrors the Python `transaction_profiling_analyzer.py` `ReadCounter` /
//! `WriteCounter`: it counts which keys and ranges transactions read and wrote, so a
//! caller can find hot keys or split the keyspace into buckets of approximately equal
//! traffic. It does not resolve shard addresses (`ShardFinder` in the Python tool): that
//! is a separate, caller-side concern.

use crate::event::{Event, KeyRange, Mutation};
use crate::reader::ProfiledTransaction;
use std::collections::BTreeMap;
use tracing::instrument;

/// `MutationRef::Type` codes that write a single key (`param1`): the plain and atomic
/// mutations a client can send in a commit
/// (`fdbclient/include/fdbclient/CommitTransaction.h`). Deliberately an allowlist, not a
/// range check: `MutationRef::Type` also has codes a client commit never carries
/// (`DebugKeyRange`, `DebugKey`, `NoOp`, `AvailableForReuse`, the `Reserved_For_*` codes,
/// `Encrypted`), and `SetVersionstampedKey`, whose `param1` holds an unfilled versionstamp
/// placeholder rather than the real key, so it is left out on purpose.
const SINGLE_KEY_WRITE_TYPES: [u8; 14] = [
    Mutation::SET_VALUE,
    Mutation::ADD_VALUE,
    Mutation::AND,
    Mutation::OR,
    Mutation::XOR,
    Mutation::APPEND_IF_FITS,
    Mutation::MAX,
    Mutation::MIN,
    Mutation::SET_VERSIONSTAMPED_VALUE,
    Mutation::BYTE_MIN,
    Mutation::BYTE_MAX,
    Mutation::MIN_V2,
    Mutation::AND_V2,
    Mutation::COMPARE_AND_CLEAR,
];

/// Counts keys and ranges read and written by recorded transactions.
///
/// Only successful operations are counted: `Get`/`GetRange`/`Commit` events, never
/// `GetError`/`GetRangeError`/`CommitError`. This mirrors the Python `ReadCounter` and
/// `WriteCounter`, which only ever process `transaction_info.gets`, `.get_ranges` and
/// `.commit`, and never the `error_*` lists, since a failed operation never touched the
/// key or range it targeted. See [`Aggregator::writes`] for which mutation types count as
/// writes.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Aggregator {
    reads: KeyCounts,
    writes: KeyCounts,
}

impl Aggregator {
    /// Records every event of one profiled transaction.
    #[instrument(level = "debug", skip_all, fields(events = tx.events.len()))]
    pub fn record(&mut self, tx: &ProfiledTransaction) {
        for event in &tx.events {
            self.record_event(event);
        }
    }

    /// Records one event.
    ///
    /// See the [`Aggregator`] docs for which events and mutation types count.
    #[instrument(level = "trace", skip_all)]
    pub fn record_event(&mut self, event: &Event) {
        match event {
            Event::Get(get) => self.reads.insert_key(&get.key),
            Event::GetRange(get_range) => self.reads.insert_range(get_range.range.clone()),
            Event::Commit(commit) => {
                for mutation in &commit.request.mutations {
                    if mutation.mutation_type == Mutation::CLEAR_RANGE {
                        self.writes.insert_range(KeyRange {
                            begin: mutation.param1.clone(),
                            end: mutation.param2.clone(),
                        });
                    } else if SINGLE_KEY_WRITE_TYPES.contains(&mutation.mutation_type) {
                        self.writes.insert_key(&mutation.param1);
                    }
                }
            }
            Event::GetVersion(_)
            | Event::GetError(_)
            | Event::GetRangeError(_)
            | Event::CommitError(_) => {}
        }
    }

    /// Keys and ranges read by `Get` and `GetRange` events.
    #[instrument(level = "trace", skip_all)]
    pub fn reads(&self) -> &KeyCounts {
        &self.reads
    }

    /// Keys and ranges written by the mutations of successful `Commit` events.
    ///
    /// A `ClearRange` mutation counts as a range write, `param1` to `param2`. An allowlist
    /// of client write mutation types counts as a single-key write by `param1`:
    /// `SetValue`, `AddValue`, and the atomic ops `And`, `Or`, `Xor`, `AppendIfFits`,
    /// `Max`, `Min`, `SetVersionstampedValue`, `ByteMin`, `ByteMax`, `MinV2`, `AndV2` and
    /// `CompareAndClear`. Everything else is ignored: `SetVersionstampedKey` (its
    /// `param1` holds an unfilled versionstamp placeholder, not the real key), the
    /// server-only debug/reserved mutation types, and unknown codes.
    ///
    /// This is wider than the Python `WriteCounter`, which only counts `SetValue` and
    /// `AddValue` and drops every other mutation type, including `ClearRange`.
    #[instrument(level = "trace", skip_all)]
    pub fn writes(&self) -> &KeyCounts {
        &self.writes
    }
}

/// Per-key and per-range hit counts, as returned by [`Aggregator::reads`] and
/// [`Aggregator::writes`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct KeyCounts {
    keys: BTreeMap<Vec<u8>, u64>,
    ranges: BTreeMap<KeyRange, u64>,
}

impl KeyCounts {
    fn insert_key(&mut self, key: &[u8]) {
        *self.keys.entry(key.to_vec()).or_insert(0) += 1;
    }

    fn insert_range(&mut self, range: KeyRange) {
        *self.ranges.entry(range).or_insert(0) += 1;
    }

    /// Total number of recorded hits, single keys and ranges combined.
    #[instrument(level = "trace", skip(self))]
    pub fn total(&self) -> u64 {
        let key_total: u64 = self.keys.values().sum();
        let range_total: u64 = self.ranges.values().sum();
        key_total + range_total
    }

    /// The `n` most frequently hit single keys, descending by count, ties broken by key
    /// ascending.
    #[instrument(level = "trace", skip(self))]
    pub fn top_keys(&self, n: usize) -> Vec<(Vec<u8>, u64)> {
        let mut entries: Vec<(Vec<u8>, u64)> =
            self.keys.iter().map(|(k, v)| (k.clone(), *v)).collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        entries.truncate(n);
        entries
    }

    /// The `n` most frequently hit ranges, descending by count, ties broken by range
    /// ascending (begin, then end).
    #[instrument(level = "trace", skip(self))]
    pub fn top_ranges(&self, n: usize) -> Vec<(KeyRange, u64)> {
        let mut entries: Vec<(KeyRange, u64)> =
            self.ranges.iter().map(|(k, v)| (k.clone(), *v)).collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        entries.truncate(n);
        entries
    }

    /// Splits the keyspace into up to `n` buckets of approximately equal hit count, like
    /// `ReadCounter.get_range_boundaries` / `WriteCounter.get_range_boundaries` in the
    /// Python tool (minus its `ShardFinder` address resolution, and the "intersecting"
    /// count it additionally reports for ranges that span a bucket boundary, which is a
    /// display-only detail this crate does not expose).
    ///
    /// A bucket's range is `[bucket.start, next_bucket.start)`; the last bucket extends to
    /// the end of the keyspace. Returns fewer than `n` buckets when there are fewer than
    /// `n` distinct starting positions, and an empty vector for `n == 0` or an empty
    /// counter.
    #[instrument(level = "trace", skip(self))]
    pub fn buckets(&self, n: usize) -> Vec<Bucket> {
        if n == 0 {
            return Vec::new();
        }

        // Every recorded key or range contributes one hit at its starting position: a
        // single key at itself, a range at its begin. This is exactly the Python
        // `opened_this_range` counter, which is what decides where boundaries fall.
        let mut starts: BTreeMap<&[u8], u64> = BTreeMap::new();
        for (key, count) in &self.keys {
            *starts.entry(key.as_slice()).or_insert(0) += count;
        }
        for (range, count) in &self.ranges {
            *starts.entry(range.begin.as_slice()).or_insert(0) += count;
        }

        let total: u64 = starts.values().sum();
        if total == 0 {
            return Vec::new();
        }

        // Floored at 1: when `n` exceeds the amount of distinct traffic, the Python
        // tool's `range_size` (an integer division) truncates to 0, which then cuts a
        // boundary before any key is ever assigned to it. Flooring instead yields one
        // bucket per distinct starting position, the sensible reading of "n buckets" when
        // there is less traffic than that to split.
        let bucket_size = (total / n as u64).max(1);

        let mut out = Vec::new();
        let mut current_start: Option<Vec<u8>> = None;
        let mut current_count: u64 = 0;
        for (&start, &count) in &starts {
            // Once one more bucket would reach `n`, stop cutting: the last bucket absorbs
            // whatever remains, so `buckets` never returns more than `n` of them.
            if current_count >= bucket_size && out.len() + 1 < n {
                if let Some(start) = current_start.take() {
                    out.push(Bucket {
                        start,
                        count: current_count,
                    });
                }
                current_count = 0;
            }
            current_count += count;
            if current_start.is_none() {
                current_start = Some(start.to_vec());
            }
        }
        if current_count > 0 {
            if let Some(start) = current_start {
                out.push(Bucket {
                    start,
                    count: current_count,
                });
            }
        }
        out
    }
}

/// One boundary of an approximately equal-count keyspace split, from [`KeyCounts::buckets`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bucket {
    /// Inclusive start of the bucket. The bucket extends to the next bucket's `start`, or
    /// to the end of the keyspace for the last bucket.
    pub start: Vec<u8>,
    /// Number of read or write hits starting in this bucket.
    pub count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{
        Commit, CommitError, CommitRequest, EventHeader, Get, GetError, GetRange, GetRangeError,
        GetVersion, Mutation,
    };

    fn header() -> EventHeader {
        EventHeader {
            start_timestamp: 0.0,
            dc_id: Vec::new(),
            tenant: None,
        }
    }

    fn get(key: &[u8]) -> Event {
        Event::Get(Get {
            header: header(),
            latency: 0.0,
            value_size: 0,
            key: key.to_vec(),
        })
    }

    fn get_range(begin: &[u8], end: &[u8]) -> Event {
        Event::GetRange(GetRange {
            header: header(),
            latency: 0.0,
            range_size: 0,
            range: KeyRange {
                begin: begin.to_vec(),
                end: end.to_vec(),
            },
        })
    }

    fn get_error(key: &[u8]) -> Event {
        Event::GetError(GetError {
            header: header(),
            error_code: 1009,
            key: key.to_vec(),
        })
    }

    fn get_range_error(begin: &[u8], end: &[u8]) -> Event {
        Event::GetRangeError(GetRangeError {
            header: header(),
            error_code: 1009,
            range: KeyRange {
                begin: begin.to_vec(),
                end: end.to_vec(),
            },
        })
    }

    fn commit_request(mutations: Vec<Mutation>) -> CommitRequest {
        CommitRequest {
            read_conflict_ranges: Vec::new(),
            write_conflict_ranges: Vec::new(),
            mutations,
            read_snapshot: 0,
            report_conflicting_keys: false,
            lock_aware: false,
            span_context: None,
        }
    }

    fn commit(mutations: Vec<Mutation>) -> Event {
        Event::Commit(Commit {
            header: header(),
            latency: 0.0,
            num_mutations: mutations.len() as i32,
            commit_bytes: 0,
            commit_version: 0,
            request: commit_request(mutations),
        })
    }

    fn commit_error(mutations: Vec<Mutation>) -> Event {
        Event::CommitError(CommitError {
            header: header(),
            error_code: 1020,
            request: commit_request(mutations),
        })
    }

    fn mutation(mutation_type: u8, param1: &[u8], param2: &[u8]) -> Mutation {
        Mutation {
            mutation_type,
            param1: param1.to_vec(),
            param2: param2.to_vec(),
        }
    }

    /// A code in the `MutationRef::Type` range that is not a client write op: server-only
    /// debug mutation, not in the allowlist.
    const NO_OP: u8 = 5;
    /// Past `Encrypted` (23), the last type the C++ client defines.
    const UNKNOWN_MUTATION_TYPE: u8 = 250;

    #[test]
    fn reads_and_writes_are_split_and_only_successes_count() {
        let mut agg = Aggregator::default();
        for event in [
            get(b"a"),
            get_range(b"b", b"c"),
            get_error(b"ignored"),
            get_range_error(b"ig", b"nored"),
            Event::GetVersion(GetVersion {
                header: header(),
                latency: 0.0,
                priority: 0,
                read_version: 0,
            }),
            commit(vec![mutation(Mutation::SET_VALUE, b"w", b"v")]),
            commit_error(vec![mutation(Mutation::SET_VALUE, b"ignored-write", b"v")]),
        ] {
            agg.record_event(&event);
        }

        assert_eq!(agg.reads().total(), 2);
        assert_eq!(agg.reads().top_keys(10), vec![(b"a".to_vec(), 1)]);
        assert_eq!(
            agg.reads().top_ranges(10),
            vec![(
                KeyRange {
                    begin: b"b".to_vec(),
                    end: b"c".to_vec()
                },
                1
            )]
        );
        assert_eq!(agg.writes().total(), 1);
        assert_eq!(agg.writes().top_keys(10), vec![(b"w".to_vec(), 1)]);
        assert!(agg.writes().top_ranges(10).is_empty());
    }

    #[test]
    fn record_loops_record_event_over_transaction_events() {
        let mut agg = Aggregator::default();
        let tx = ProfiledTransaction {
            version: 1,
            versionstamp: [0u8; 10],
            id: [0u8; 16],
            protocol_version: crate::event::ProtocolVersion::V7_4,
            events: vec![get(b"a"), get(b"a"), commit(vec![])],
        };
        agg.record(&tx);
        assert_eq!(agg.reads().top_keys(10), vec![(b"a".to_vec(), 2)]);
        assert_eq!(agg.writes().total(), 0);
    }

    #[test]
    fn write_mutation_types() {
        let mut agg = Aggregator::default();
        agg.record_event(&commit(vec![
            mutation(Mutation::SET_VALUE, b"set", b"v"),
            mutation(Mutation::ADD_VALUE, b"add", b"1"),
            // Every other known atomic op counts too, not just SetValue/AddValue: wider
            // than the Python WriteCounter.
            mutation(Mutation::AND, b"anded", b"mask"),
            mutation(Mutation::SET_VERSIONSTAMPED_VALUE, b"svv", b"v"),
            mutation(Mutation::CLEAR_RANGE, b"c", b"c\x00"),
            // Placeholder key (unfilled versionstamp): never counted.
            mutation(Mutation::SET_VERSIONSTAMPED_KEY, b"placeholder", b"v"),
            // In the MutationRef::Type range but not a client write op: not in the
            // allowlist, ignored.
            mutation(NO_OP, b"noop", b"v"),
            // Past the last mutation type the C++ client defines: ignored.
            mutation(UNKNOWN_MUTATION_TYPE, b"future", b"v"),
        ]));

        let writes = agg.writes();
        assert_eq!(writes.total(), 5);
        assert_eq!(
            writes.top_keys(10),
            vec![
                (b"add".to_vec(), 1),
                (b"anded".to_vec(), 1),
                (b"set".to_vec(), 1),
                (b"svv".to_vec(), 1),
            ]
        );
        assert_eq!(
            writes.top_ranges(10),
            vec![(
                KeyRange {
                    begin: b"c".to_vec(),
                    end: b"c\x00".to_vec()
                },
                1
            )]
        );
    }

    #[test]
    fn top_keys_orders_by_count_then_key_ascending() {
        let mut counts = KeyCounts::default();
        for (key, hits) in [(b"z" as &[u8], 3), (b"a", 3), (b"m", 5), (b"b", 1)] {
            for _ in 0..hits {
                counts.insert_key(key);
            }
        }
        assert_eq!(
            counts.top_keys(10),
            vec![
                (b"m".to_vec(), 5),
                (b"a".to_vec(), 3),
                (b"z".to_vec(), 3),
                (b"b".to_vec(), 1),
            ]
        );
        assert_eq!(counts.top_keys(0), Vec::new());
        assert_eq!(counts.top_keys(2).len(), 2);
    }

    #[test]
    fn top_ranges_orders_by_count_then_range_ascending() {
        let mut counts = KeyCounts::default();
        let r = |b: &[u8], e: &[u8]| KeyRange {
            begin: b.to_vec(),
            end: e.to_vec(),
        };
        for (range, hits) in [(r(b"y", b"z"), 2), (r(b"a", b"b"), 2), (r(b"m", b"n"), 4)] {
            for _ in 0..hits {
                counts.insert_range(range.clone());
            }
        }
        assert_eq!(
            counts.top_ranges(10),
            vec![(r(b"m", b"n"), 4), (r(b"a", b"b"), 2), (r(b"y", b"z"), 2)]
        );
    }

    #[test]
    fn buckets_empty_counter_is_empty() {
        let counts = KeyCounts::default();
        assert_eq!(counts.buckets(4), Vec::new());
        assert_eq!(counts.buckets(0), Vec::new());
    }

    #[test]
    fn buckets_n_zero_is_empty_even_with_data() {
        let mut counts = KeyCounts::default();
        counts.insert_key(b"a");
        assert_eq!(counts.buckets(0), Vec::new());
    }

    #[test]
    fn buckets_split_approximately_evenly() {
        let mut counts = KeyCounts::default();
        // 8 hits spread over 4 distinct keys, split into 2 buckets of ~4.
        for key in [b"a" as &[u8], b"b", b"c", b"d"] {
            counts.insert_key(key);
            counts.insert_key(key);
        }
        let buckets = counts.buckets(2);
        assert_eq!(
            buckets,
            vec![
                Bucket {
                    start: b"a".to_vec(),
                    count: 4
                },
                Bucket {
                    start: b"c".to_vec(),
                    count: 4
                },
            ]
        );
        let total: u64 = buckets.iter().map(|b| b.count).sum();
        assert_eq!(total, counts.total());
    }

    #[test]
    fn buckets_n_larger_than_distinct_keys_yields_one_bucket_per_key() {
        let mut counts = KeyCounts::default();
        counts.insert_key(b"a");
        counts.insert_key(b"b");
        counts.insert_key(b"c");

        let buckets = counts.buckets(10);
        assert_eq!(
            buckets,
            vec![
                Bucket {
                    start: b"a".to_vec(),
                    count: 1
                },
                Bucket {
                    start: b"b".to_vec(),
                    count: 1
                },
                Bucket {
                    start: b"c".to_vec(),
                    count: 1
                },
            ]
        );
    }

    #[test]
    fn buckets_never_returns_more_than_n() {
        // 10 keys with 1 hit each: bucket_size = 10 / 3 = 3, which would cut a 4th bucket
        // (3, 3, 3, 1) if the last one didn't absorb the remainder instead.
        let mut counts = KeyCounts::default();
        for key in [
            b"a" as &[u8],
            b"b",
            b"c",
            b"d",
            b"e",
            b"f",
            b"g",
            b"h",
            b"i",
            b"j",
        ] {
            counts.insert_key(key);
        }
        let buckets = counts.buckets(3);
        assert_eq!(
            buckets,
            vec![
                Bucket {
                    start: b"a".to_vec(),
                    count: 3
                },
                Bucket {
                    start: b"d".to_vec(),
                    count: 3
                },
                Bucket {
                    start: b"g".to_vec(),
                    count: 4
                },
            ]
        );
        let total: u64 = buckets.iter().map(|b| b.count).sum();
        assert_eq!(total, counts.total());
    }

    #[test]
    fn buckets_range_contributes_at_its_begin() {
        let mut counts = KeyCounts::default();
        counts.insert_range(KeyRange {
            begin: b"a".to_vec(),
            end: b"z".to_vec(),
        });
        counts.insert_key(b"m");
        let buckets = counts.buckets(2);
        assert_eq!(
            buckets,
            vec![
                Bucket {
                    start: b"a".to_vec(),
                    count: 1
                },
                Bucket {
                    start: b"m".to_vec(),
                    count: 1
                },
            ]
        );
    }
}
