//! Decoding of reassembled profiling blobs into typed [`Event`]s.
//!
//! A blob is what the C++ client writes with `BinaryWriter(IncludeVersion())` for one sampled
//! transaction: a little-endian `u64` protocol version followed by events serialized back to
//! back. Each event starts with its `EventType` as a little-endian `i32`, then the shared
//! `Event` fields and the event-specific ones. Byte strings are a little-endian `u32` length
//! followed by the bytes, `Optional<T>` is a presence byte followed by `T`, vectors are a
//! little-endian `u32` count followed by the elements.
//!
//! Reference: `fdbclient/include/fdbclient/ClientLogEvents.h` and `CommitTransactionRef` /
//! `MutationRef::serialize` in `fdbclient/include/fdbclient/CommitTransaction.h`.

use crate::event::{
    Commit, CommitError, CommitRequest, Event, EventHeader, Get, GetError, GetRange, GetRangeError,
    GetVersion, KeyRange, Mutation, ProtocolVersion, SpanContext,
};
use tracing::instrument;

/// Errors returned by [`decode_events`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// The blob ended before a field could be read.
    #[error("unexpected end of blob at offset {offset}: needed {needed} bytes, {remaining} left")]
    UnexpectedEof {
        /// Offset in the blob where the read started.
        offset: usize,
        /// Number of bytes the read needed.
        needed: usize,
        /// Number of bytes left in the blob.
        remaining: usize,
    },
    /// The blob was written with a protocol version this crate does not decode.
    #[error("unsupported protocol version {0:#018x}")]
    UnsupportedProtocolVersion(u64),
    /// An event carries a type code unknown to this crate.
    #[error("unknown event type {0}")]
    UnknownEventType(i32),
    /// A conflict range uses the single-key encoding with a begin key not ending in `\x00`.
    #[error("malformed key range at offset {offset}")]
    MalformedKeyRange {
        /// Offset in the blob where the key range starts.
        offset: usize,
    },
    /// A mutation's checksum suffix could not be removed (`param2` shorter than the
    /// checksum, and accumulative checksum index when present).
    #[error("malformed mutation at offset {offset}")]
    MalformedMutation {
        /// Offset in the blob where the mutation starts.
        offset: usize,
    },
}

const EVENT_GET_VERSION: i32 = 0;
const EVENT_GET: i32 = 1;
const EVENT_GET_RANGE: i32 = 2;
const EVENT_COMMIT: i32 = 3;
const EVENT_ERROR_GET: i32 = 4;
const EVENT_ERROR_GET_RANGE: i32 = 5;
const EVENT_ERROR_COMMIT: i32 = 6;

const CHECKSUM_FLAG_MASK: u8 = 0x80;
const ACCUMULATIVE_CHECKSUM_INDEX_FLAG_MASK: u8 = 0x40;
const CHECKSUM_LEN: usize = 4;
const ACCUMULATIVE_CHECKSUM_INDEX_LEN: usize = 2;

/// Minimum encoded size of a key range (two empty byte strings).
const MIN_KEY_RANGE_LEN: usize = 8;
/// Minimum encoded size of a mutation (type byte plus two empty byte strings).
const MIN_MUTATION_LEN: usize = 9;

/// Decodes one reassembled profiling blob (all chunks of one sampled transaction,
/// concatenated) into its protocol version and events, in write order.
///
/// Only blobs written by 7.1 to 7.4 clients are supported. Never panics on malformed input,
/// and allocations are bounded by the blob length.
#[instrument(level = "trace", skip_all, fields(blob_len = blob.len()))]
pub fn decode_events(blob: &[u8]) -> Result<(ProtocolVersion, Vec<Event>), DecodeError> {
    let mut reader = Reader::new(blob);
    let version = ProtocolVersion(reader.u64()?);
    if !is_supported(version) {
        return Err(DecodeError::UnsupportedProtocolVersion(version.0));
    }
    let mut events = Vec::new();
    while !reader.is_empty() {
        events.push(reader.event(version)?);
    }
    Ok((version, events))
}

fn is_supported(version: ProtocolVersion) -> bool {
    matches!(
        version,
        ProtocolVersion::V7_1
            | ProtocolVersion::V7_2
            | ProtocolVersion::V7_3
            | ProtocolVersion::V7_4
    )
}

/// 7.2 switched the commit span context from `Optional<UID>` to `Optional<SpanContext>`
/// (`hasOTELSpanContext`).
fn has_otel_span_context(version: ProtocolVersion) -> bool {
    version.0 >= ProtocolVersion::V7_2.0
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        match self.buf.get(self.pos..).and_then(|rest| rest.get(..n)) {
            Some(bytes) => {
                self.pos += n;
                Ok(bytes)
            }
            None => Err(DecodeError::UnexpectedEof {
                offset: self.pos,
                needed: n,
                remaining: self.remaining(),
            }),
        }
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.array::<1>()?[0])
    }

    fn bool(&mut self) -> Result<bool, DecodeError> {
        Ok(self.u8()? != 0)
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn i32(&mut self) -> Result<i32, DecodeError> {
        Ok(i32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    fn f64(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from_le_bytes(self.array()?))
    }

    /// Length-prefixed byte string. The length is checked against the remaining bytes
    /// before anything is allocated.
    fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn key_range(&mut self) -> Result<KeyRange, DecodeError> {
        Ok(KeyRange {
            begin: self.bytes()?,
            end: self.bytes()?,
        })
    }

    /// Count-prefixed vector. The initial capacity is capped by what the remaining bytes can
    /// hold, so a garbage count cannot trigger a huge allocation.
    fn vec<T>(
        &mut self,
        min_elem_len: usize,
        mut elem: impl FnMut(&mut Self) -> Result<T, DecodeError>,
    ) -> Result<Vec<T>, DecodeError> {
        let count = self.u32()? as usize;
        let mut out = Vec::with_capacity(count.min(self.remaining() / min_elem_len));
        for _ in 0..count {
            out.push(elem(self)?);
        }
        Ok(out)
    }

    fn header(&mut self) -> Result<EventHeader, DecodeError> {
        let start_timestamp = self.f64()?;
        let dc_id = self.bytes()?;
        let tenant = if self.bool()? {
            Some(self.bytes()?)
        } else {
            None
        };
        Ok(EventHeader {
            start_timestamp,
            dc_id,
            tenant,
        })
    }

    fn event(&mut self, version: ProtocolVersion) -> Result<Event, DecodeError> {
        let event_type = self.i32()?;
        if !(EVENT_GET_VERSION..=EVENT_ERROR_COMMIT).contains(&event_type) {
            return Err(DecodeError::UnknownEventType(event_type));
        }
        let header = self.header()?;
        let event = match event_type {
            EVENT_GET_VERSION => Event::GetVersion(GetVersion {
                header,
                latency: self.f64()?,
                priority: self.u32()?,
                read_version: self.i64()?,
            }),
            EVENT_GET => Event::Get(Get {
                header,
                latency: self.f64()?,
                value_size: self.i32()?,
                key: self.bytes()?,
            }),
            EVENT_GET_RANGE => Event::GetRange(GetRange {
                header,
                latency: self.f64()?,
                range_size: self.i32()?,
                range: self.key_range()?,
            }),
            EVENT_COMMIT => Event::Commit(Commit {
                header,
                latency: self.f64()?,
                num_mutations: self.i32()?,
                commit_bytes: self.i32()?,
                commit_version: self.i64()?,
                request: self.commit_request(version)?,
            }),
            EVENT_ERROR_GET => Event::GetError(GetError {
                header,
                error_code: self.i32()?,
                key: self.bytes()?,
            }),
            EVENT_ERROR_GET_RANGE => Event::GetRangeError(GetRangeError {
                header,
                error_code: self.i32()?,
                range: self.key_range()?,
            }),
            EVENT_ERROR_COMMIT => Event::CommitError(CommitError {
                header,
                error_code: self.i32()?,
                request: self.commit_request(version)?,
            }),
            other => return Err(DecodeError::UnknownEventType(other)),
        };
        Ok(event)
    }

    /// A serialized `KeyRangeRef`. Mirrors its deserializer: the single-key range
    /// `[k, k\x00)` is written as `(k\x00, "")` and expanded back here.
    fn conflict_range(&mut self) -> Result<KeyRange, DecodeError> {
        let offset = self.pos;
        let mut range = self.key_range()?;
        if range.end.is_empty() && !range.begin.is_empty() {
            if range.begin.last() != Some(&0) {
                return Err(DecodeError::MalformedKeyRange { offset });
            }
            range.end = range.begin.clone();
            range.begin.pop();
        }
        Ok(range)
    }

    fn commit_request(&mut self, version: ProtocolVersion) -> Result<CommitRequest, DecodeError> {
        let read_conflict_ranges = self.vec(MIN_KEY_RANGE_LEN, Self::conflict_range)?;
        let write_conflict_ranges = self.vec(MIN_KEY_RANGE_LEN, Self::conflict_range)?;
        let mutations = self.vec(MIN_MUTATION_LEN, Self::mutation)?;
        let read_snapshot = self.i64()?;
        let report_conflicting_keys = self.bool()?;
        let lock_aware = self.bool()?;
        let span_context = if self.bool()? {
            Some(self.span_context(version)?)
        } else {
            None
        };
        Ok(CommitRequest {
            read_conflict_ranges,
            write_conflict_ranges,
            mutations,
            read_snapshot,
            report_conflicting_keys,
            lock_aware,
            span_context,
        })
    }

    fn span_context(&mut self, version: ProtocolVersion) -> Result<SpanContext, DecodeError> {
        let trace_id = [self.u64()?, self.u64()?];
        if has_otel_span_context(version) {
            Ok(SpanContext {
                trace_id,
                span_id: self.u64()?,
                flags: self.u8()?,
            })
        } else {
            Ok(SpanContext {
                trace_id,
                span_id: 0,
                flags: 0,
            })
        }
    }

    /// Mirrors the deserializing branch of `MutationRef::serialize`.
    fn mutation(&mut self) -> Result<Mutation, DecodeError> {
        let offset = self.pos;
        let mut mutation_type = self.u8()?;
        let mut param1 = self.bytes()?;
        let mut param2 = self.bytes()?;
        let malformed = DecodeError::MalformedMutation { offset };

        if mutation_type & CHECKSUM_FLAG_MASK != 0 {
            let mut suffix = CHECKSUM_LEN;
            if mutation_type & ACCUMULATIVE_CHECKSUM_INDEX_FLAG_MASK != 0 {
                suffix += ACCUMULATIVE_CHECKSUM_INDEX_LEN;
            }
            let len = param2.len().checked_sub(suffix).ok_or(malformed)?;
            param2.truncate(len);
            mutation_type &= !(CHECKSUM_FLAG_MASK | ACCUMULATIVE_CHECKSUM_INDEX_FLAG_MASK);
        }

        // A single-key clear `[k, k\x00)` is written as `(ClearRange, k\x00, "")`. Like the
        // C++ deserializer, a `param1` not ending in `\x00` is only traced as an error, not
        // treated as fatal: the mutation is still expanded, dropping its last byte.
        if mutation_type == Mutation::CLEAR_RANGE && param2.is_empty() && !param1.is_empty() {
            if param1.last() != Some(&0) {
                tracing::warn!(
                    offset,
                    param1 = ?param1,
                    "single-key clear range mutation with param1 not ending in \\x00"
                );
            }
            param2 = param1.clone();
            param1.pop();
        }

        Ok(Mutation {
            mutation_type,
            param1,
            param2,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const P: &[u8] = b"fdbrs_prof_fixture/";

    fn key(suffix: &[u8]) -> Vec<u8> {
        [P, suffix].concat()
    }

    /// Minimal writer following the C++ `BinaryWriter` layout, to build test blobs.
    #[derive(Default)]
    struct W(Vec<u8>);

    impl W {
        fn version(v: ProtocolVersion) -> Self {
            let mut w = W::default();
            w.u64(v.0);
            w
        }
        fn u8(&mut self, v: u8) -> &mut Self {
            self.0.push(v);
            self
        }
        fn u32(&mut self, v: u32) -> &mut Self {
            self.0.extend_from_slice(&v.to_le_bytes());
            self
        }
        fn i32(&mut self, v: i32) -> &mut Self {
            self.0.extend_from_slice(&v.to_le_bytes());
            self
        }
        fn u64(&mut self, v: u64) -> &mut Self {
            self.0.extend_from_slice(&v.to_le_bytes());
            self
        }
        fn i64(&mut self, v: i64) -> &mut Self {
            self.0.extend_from_slice(&v.to_le_bytes());
            self
        }
        fn f64(&mut self, v: f64) -> &mut Self {
            self.0.extend_from_slice(&v.to_le_bytes());
            self
        }
        fn bytes(&mut self, v: &[u8]) -> &mut Self {
            self.u32(v.len() as u32);
            self.0.extend_from_slice(v);
            self
        }
        fn header(
            &mut self,
            event_type: i32,
            ts: f64,
            dc: &[u8],
            tenant: Option<&[u8]>,
        ) -> &mut Self {
            self.i32(event_type).f64(ts).bytes(dc);
            match tenant {
                Some(t) => self.u8(1).bytes(t),
                None => self.u8(0),
            }
        }
        /// Commit request body with one read range, one write range, one set mutation.
        fn commit_request(&mut self, span: Option<&[u8]>) -> &mut Self {
            self.u32(1).bytes(&key(b"r")).bytes(&key(b"s"));
            self.u32(1).bytes(&key(b"w")).bytes(&key(b"x"));
            self.u32(1).u8(0).bytes(&key(b"k")).bytes(b"v");
            self.i64(42).u8(1).u8(0);
            match span {
                Some(s) => {
                    self.u8(1);
                    self.0.extend_from_slice(s);
                    self
                }
                None => self.u8(0),
            }
        }
    }

    fn header(ts: f64) -> EventHeader {
        EventHeader {
            start_timestamp: ts,
            dc_id: Vec::new(),
            tenant: None,
        }
    }

    fn expected_request(span_context: Option<SpanContext>) -> CommitRequest {
        CommitRequest {
            read_conflict_ranges: vec![KeyRange {
                begin: key(b"r"),
                end: key(b"s"),
            }],
            write_conflict_ranges: vec![KeyRange {
                begin: key(b"w"),
                end: key(b"x"),
            }],
            mutations: vec![Mutation {
                mutation_type: 0,
                param1: key(b"k"),
                param2: b"v".to_vec(),
            }],
            read_snapshot: 42,
            report_conflicting_keys: true,
            lock_aware: false,
            span_context,
        }
    }

    /// One blob with every event type, in type order.
    fn all_events_blob() -> Vec<u8> {
        let mut w = W::version(ProtocolVersion::V7_4);
        w.header(0, 1.0, b"dc1", Some(b"tenant"))
            .f64(0.5)
            .u32(2)
            .i64(100);
        w.header(1, 2.0, b"", None)
            .f64(0.25)
            .i32(7)
            .bytes(&key(b"a"));
        w.header(2, 3.0, b"", None)
            .f64(0.125)
            .i32(99)
            .bytes(&key(b"a"))
            .bytes(&key(b"z"));
        w.header(3, 4.0, b"", None)
            .f64(1.5)
            .i32(1)
            .i32(30)
            .i64(200)
            .commit_request(None);
        w.header(4, 5.0, b"", None).i32(1007).bytes(&key(b"g"));
        w.header(5, 6.0, b"", None)
            .i32(1009)
            .bytes(&key(b"b"))
            .bytes(&key(b"e"));
        w.header(6, 7.0, b"", None).i32(1020).commit_request(None);
        w.0
    }

    #[test]
    fn decodes_every_event_type() {
        let (version, events) = decode_events(&all_events_blob()).unwrap();
        assert_eq!(version, ProtocolVersion::V7_4);
        assert_eq!(
            events,
            vec![
                Event::GetVersion(GetVersion {
                    header: EventHeader {
                        start_timestamp: 1.0,
                        dc_id: b"dc1".to_vec(),
                        tenant: Some(b"tenant".to_vec()),
                    },
                    latency: 0.5,
                    priority: 2,
                    read_version: 100,
                }),
                Event::Get(Get {
                    header: header(2.0),
                    latency: 0.25,
                    value_size: 7,
                    key: key(b"a"),
                }),
                Event::GetRange(GetRange {
                    header: header(3.0),
                    latency: 0.125,
                    range_size: 99,
                    range: KeyRange {
                        begin: key(b"a"),
                        end: key(b"z"),
                    },
                }),
                Event::Commit(Commit {
                    header: header(4.0),
                    latency: 1.5,
                    num_mutations: 1,
                    commit_bytes: 30,
                    commit_version: 200,
                    request: expected_request(None),
                }),
                Event::GetError(GetError {
                    header: header(5.0),
                    error_code: 1007,
                    key: key(b"g"),
                }),
                Event::GetRangeError(GetRangeError {
                    header: header(6.0),
                    error_code: 1009,
                    range: KeyRange {
                        begin: key(b"b"),
                        end: key(b"e"),
                    },
                }),
                Event::CommitError(CommitError {
                    header: header(7.0),
                    error_code: 1020,
                    request: expected_request(None),
                }),
            ]
        );
        assert_eq!(events[3].header().start_timestamp, 4.0);
    }

    #[test]
    fn span_context_is_uid_in_7_1_and_otel_from_7_2() {
        let mut uid = Vec::new();
        uid.extend_from_slice(&1u64.to_le_bytes());
        uid.extend_from_slice(&2u64.to_le_bytes());
        let mut otel = uid.clone();
        otel.extend_from_slice(&3u64.to_le_bytes());
        otel.push(1);

        let cases = [
            (ProtocolVersion::V7_1, &uid, 0, 0),
            (ProtocolVersion::V7_2, &otel, 3, 1),
            (ProtocolVersion::V7_3, &otel, 3, 1),
            (ProtocolVersion::V7_4, &otel, 3, 1),
        ];
        for (version, span, span_id, flags) in cases {
            let mut w = W::version(version);
            w.header(6, 1.0, b"", None)
                .i32(1020)
                .commit_request(Some(span));
            let (_, events) = decode_events(&w.0).unwrap();
            let expected = Some(SpanContext {
                trace_id: [1, 2],
                span_id,
                flags,
            });
            assert_eq!(
                events,
                vec![Event::CommitError(CommitError {
                    header: header(1.0),
                    error_code: 1020,
                    request: expected_request(expected),
                })],
                "{version:?}"
            );
        }
    }

    /// Commit event whose request carries the given raw conflict range and mutation bytes.
    fn commit_with(range: &[u8], mutation: &[u8]) -> Vec<u8> {
        let mut w = W::version(ProtocolVersion::V7_4);
        w.header(3, 1.0, b"", None).f64(0.0).i32(1).i32(1).i64(1);
        w.u32(0).u32(1);
        w.0.extend_from_slice(range);
        w.u32(1);
        w.0.extend_from_slice(mutation);
        w.i64(0).u8(0).u8(0).u8(0);
        w.0
    }

    fn commit_request_of(blob: &[u8]) -> Result<CommitRequest, DecodeError> {
        match decode_events(blob)?.1.pop() {
            Some(Event::Commit(c)) => Ok(c.request),
            other => panic!("unexpected {other:?}"),
        }
    }

    fn range_bytes(begin: &[u8], end: &[u8]) -> Vec<u8> {
        let mut w = W::default();
        w.bytes(begin).bytes(end);
        w.0
    }

    fn mutation_bytes(t: u8, p1: &[u8], p2: &[u8]) -> Vec<u8> {
        let mut w = W::default();
        w.u8(t).bytes(p1).bytes(p2);
        w.0
    }

    #[test]
    fn expands_single_key_encodings() {
        let k0 = key(b"k\x00");
        let req = commit_request_of(&commit_with(
            &range_bytes(&k0, b""),
            &mutation_bytes(1, &k0, b""),
        ))
        .unwrap();
        let single = KeyRange {
            begin: key(b"k"),
            end: k0.clone(),
        };
        assert_eq!(req.write_conflict_ranges, vec![single]);
        assert_eq!(
            req.mutations,
            vec![Mutation {
                mutation_type: 1,
                param1: key(b"k"),
                param2: k0,
            }]
        );
    }

    #[test]
    fn strips_mutation_checksums() {
        let range = range_bytes(b"a", b"b");
        let expected = Mutation {
            mutation_type: 0,
            param1: key(b"k"),
            param2: b"value".to_vec(),
        };
        // checksum only
        let m = mutation_bytes(0x80, &key(b"k"), b"value\x01\x02\x03\x04");
        let req = commit_request_of(&commit_with(&range, &m)).unwrap();
        assert_eq!(req.mutations, vec![expected.clone()]);
        // checksum + accumulative checksum index
        let m = mutation_bytes(0xC0, &key(b"k"), b"value\x01\x02\x03\x04\x05\x06");
        let req = commit_request_of(&commit_with(&range, &m)).unwrap();
        assert_eq!(req.mutations, vec![expected]);
        // single-key clear with checksum: param2 is empty plus the checksum
        let k0 = key(b"k\x00");
        let m = mutation_bytes(0x81, &k0, b"\x01\x02\x03\x04");
        let req = commit_request_of(&commit_with(&range, &m)).unwrap();
        assert_eq!(
            req.mutations,
            vec![Mutation {
                mutation_type: 1,
                param1: key(b"k"),
                param2: k0,
            }]
        );
    }

    #[test]
    fn rejects_malformed_ranges_and_mutations() {
        let good_range = range_bytes(b"a", b"b");
        let err = commit_request_of(&commit_with(
            &range_bytes(b"k", b""),
            &mutation_bytes(0, b"k", b"v"),
        ));
        assert!(matches!(err, Err(DecodeError::MalformedKeyRange { .. })));
        let err = commit_request_of(&commit_with(
            &good_range,
            &mutation_bytes(0x80, b"k", b"abc"),
        ));
        assert!(matches!(err, Err(DecodeError::MalformedMutation { .. })));
        let err = commit_request_of(&commit_with(
            &good_range,
            &mutation_bytes(0xC0, b"k", b"abcde"),
        ));
        assert!(matches!(err, Err(DecodeError::MalformedMutation { .. })));
    }

    /// Mirrors `MutationRef::serialize`: a single-key clear whose `param1` does not end in
    /// `\x00` is only traced as an error server-side, not treated as fatal. The mutation is
    /// still expanded, same as a well-formed single-key clear.
    #[test]
    fn expands_single_key_clear_with_bad_param1_instead_of_erroring() {
        let good_range = range_bytes(b"a", b"b");
        let req =
            commit_request_of(&commit_with(&good_range, &mutation_bytes(1, b"k", b""))).unwrap();
        assert_eq!(
            req.mutations,
            vec![Mutation {
                mutation_type: 1,
                param1: Vec::new(),
                param2: b"k".to_vec(),
            }]
        );
    }

    #[test]
    fn rejects_unsupported_versions_and_unknown_events() {
        for v in [
            0x0FDB_00B0_6301_0001u64, // 6.3
            0x0FDB_00B0_7001_0001,    // 7.0
            0x0FDB_00B0_7500_0000,    // unknown future
            0,
        ] {
            assert_eq!(
                decode_events(&v.to_le_bytes()),
                Err(DecodeError::UnsupportedProtocolVersion(v))
            );
        }
        let mut w = W::version(ProtocolVersion::V7_4);
        w.i32(7);
        assert_eq!(decode_events(&w.0), Err(DecodeError::UnknownEventType(7)));
        let mut w = W::version(ProtocolVersion::V7_4);
        w.i32(-1);
        assert_eq!(decode_events(&w.0), Err(DecodeError::UnknownEventType(-1)));
    }

    #[test]
    fn empty_event_list_is_ok() {
        let blob = ProtocolVersion::V7_1.0.to_le_bytes();
        assert_eq!(
            decode_events(&blob),
            Ok((ProtocolVersion::V7_1, Vec::new()))
        );
    }

    #[test]
    fn every_truncation_errors_without_panic() {
        let blob = all_events_blob();
        let (_, full) = decode_events(&blob).unwrap();
        for len in 0..blob.len() {
            match decode_events(&blob[..len]) {
                Ok((_, events)) => assert!(events.len() < full.len(), "len {len}"),
                Err(DecodeError::UnexpectedEof {
                    offset,
                    needed,
                    remaining,
                }) => {
                    assert!(offset <= len && remaining < needed, "len {len}");
                }
                Err(e) => panic!("len {len}: unexpected {e:?}"),
            }
        }
    }

    #[test]
    fn huge_lengths_and_counts_do_not_allocate() {
        // a key length of u32::MAX
        let mut w = W::version(ProtocolVersion::V7_4);
        w.header(1, 1.0, b"", None).f64(0.0).i32(0).u32(u32::MAX);
        assert!(matches!(
            decode_events(&w.0),
            Err(DecodeError::UnexpectedEof { needed, .. }) if needed == u32::MAX as usize
        ));
        // conflict range and mutation counts of u32::MAX
        for prefix_vectors in 0..3 {
            let mut w = W::version(ProtocolVersion::V7_4);
            w.header(6, 1.0, b"", None).i32(1020);
            for _ in 0..prefix_vectors {
                w.u32(0);
            }
            w.u32(u32::MAX).bytes(b"a").bytes(b"b");
            assert!(matches!(
                decode_events(&w.0),
                Err(DecodeError::UnexpectedEof { .. })
            ));
        }
    }

    #[test]
    fn random_garbage_never_panics() {
        // xorshift64, deterministic
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let valid = all_events_blob();
        for i in 0..20_000 {
            let blob = if i % 2 == 0 {
                // pure garbage after a valid version
                let mut blob = ProtocolVersion::V7_4.0.to_le_bytes().to_vec();
                let len = (next() % 256) as usize;
                blob.extend((0..len).map(|_| next() as u8));
                blob
            } else {
                // a valid blob with a few corrupted bytes
                let mut blob = valid.clone();
                for _ in 0..(1 + next() % 4) {
                    let pos = 8 + (next() as usize) % (blob.len() - 8);
                    blob[pos] = next() as u8;
                }
                blob
            };
            let _ = decode_events(&blob);
        }
    }

    const COMMIT_FIXTURE: &[u8] = include_bytes!("../tests/fixtures/commit_7_4.bin");
    const COMMIT_ERROR_FIXTURE: &[u8] = include_bytes!("../tests/fixtures/commit_error_7_4.bin");

    /// Captured from a 7.4.6 cluster: get, get_range, then set/clear/clear_range/atomic add
    /// in one committed transaction.
    #[test]
    fn decodes_real_commit_fixture() {
        let (version, events) = decode_events(COMMIT_FIXTURE).unwrap();
        assert_eq!(version, ProtocolVersion::V7_4);
        assert_eq!(events.len(), 4);
        for e in &events {
            assert!(e.header().dc_id.is_empty());
            assert_eq!(e.header().tenant, None);
            assert!(e.header().start_timestamp > 1.7e9);
        }
        let Event::GetVersion(grv) = &events[0] else {
            panic!("{:?}", events[0])
        };
        assert_eq!((grv.priority, grv.read_version), (0, 228403251085));
        let Event::Get(get) = &events[1] else {
            panic!("{:?}", events[1])
        };
        assert_eq!(
            (get.key.as_slice(), get.value_size),
            (key(b"a").as_slice(), 0)
        );
        let Event::GetRange(range) = &events[2] else {
            panic!("{:?}", events[2])
        };
        assert_eq!(
            range.range,
            KeyRange {
                begin: key(b"a\x00"),
                end: key(b"z"),
            }
        );
        let Event::Commit(commit) = &events[3] else {
            panic!("{:?}", events[3])
        };
        assert_eq!(commit.num_mutations, 5);
        assert_eq!(commit.commit_bytes, 415);
        assert_eq!(commit.commit_version, 228403870263);
        let req = &commit.request;
        assert_eq!(req.read_snapshot, 228403251085);
        assert!(!req.report_conflicting_keys && !req.lock_aware);
        assert_eq!(req.span_context, None);
        let r = |b: &[u8], e: &[u8]| KeyRange {
            begin: key(b),
            end: key(e),
        };
        assert_eq!(req.read_conflict_ranges, vec![r(b"a", b"z")]);
        assert_eq!(
            req.write_conflict_ranges,
            vec![r(b"a", b"a\x00"), r(b"b", b"b\x00"), r(b"c", b"d")]
        );
        let m = |t: u8, p1: Vec<u8>, p2: Vec<u8>| Mutation {
            mutation_type: t,
            param1: p1,
            param2: p2,
        };
        assert_eq!(
            req.mutations,
            vec![
                m(1, key(b"b"), key(b"b\x00")),
                m(1, key(b"c"), key(b"counter")),
                m(1, key(b"counter\x00"), key(b"d")),
                m(0, key(b"a"), b"value-a".to_vec()),
                // the add on a key cleared in the same transaction is folded into a set
                m(0, key(b"counter"), 1i64.to_le_bytes().to_vec()),
            ]
        );
    }

    /// Captured from a 7.4.6 cluster: a transaction that read a key another transaction
    /// then wrote, failing its commit with `not_committed`.
    #[test]
    fn decodes_real_commit_error_fixture() {
        let (version, events) = decode_events(COMMIT_ERROR_FIXTURE).unwrap();
        assert_eq!(version, ProtocolVersion::V7_4);
        assert_eq!(events.len(), 3);
        assert!(matches!(events[0], Event::GetVersion(_)));
        let Event::Get(get) = &events[1] else {
            panic!("{:?}", events[1])
        };
        assert_eq!(get.key, key(b"conflict"));
        let Event::CommitError(err) = &events[2] else {
            panic!("{:?}", events[2])
        };
        assert_eq!(err.error_code, 1020);
        let req = &err.request;
        assert_eq!(
            req.read_conflict_ranges[0],
            KeyRange {
                begin: key(b"conflict"),
                end: key(b"conflict\x00"),
            }
        );
        assert!(req.write_conflict_ranges.contains(&KeyRange {
            begin: key(b"other"),
            end: key(b"other\x00"),
        }));
        assert_eq!(
            req.mutations,
            vec![Mutation {
                mutation_type: 0,
                param1: key(b"other"),
                param2: b"y".to_vec(),
            }]
        );
    }
}
