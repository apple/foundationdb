//! Typed client profiling events.
//!
//! These types mirror the `FdbClientLogEvents` structures written by the FoundationDB
//! C++ client (`fdbclient/include/fdbclient/ClientLogEvents.h`) for protocol versions
//! 7.1 and later. They are produced by [`crate::decode_events`].

/// FoundationDB protocol version stored at the start of every profiling blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProtocolVersion(pub u64);

impl ProtocolVersion {
    /// Protocol version written by 7.1 clients.
    pub const V7_1: ProtocolVersion = ProtocolVersion(0x0FDB_00B0_7101_0000);
    /// Protocol version written by 7.2 clients.
    pub const V7_2: ProtocolVersion = ProtocolVersion(0x0FDB_00B0_7200_0000);
    /// Protocol version written by 7.3 clients.
    pub const V7_3: ProtocolVersion = ProtocolVersion(0x0FDB_00B0_7300_0000);
    /// Protocol version written by 7.4 clients.
    pub const V7_4: ProtocolVersion = ProtocolVersion(0x0FDB_00B0_7400_0000);
}

/// A key range `[begin, end)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyRange {
    /// Inclusive start key.
    pub begin: Vec<u8>,
    /// Exclusive end key.
    pub end: Vec<u8>,
}

/// Fields shared by every event (`FdbClientLogEvents::Event`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct EventHeader {
    /// Client-side start time of the operation, in seconds since the unix epoch.
    pub start_timestamp: f64,
    /// Datacenter id of the client, empty when unset.
    pub dc_id: Vec<u8>,
    /// Tenant name, when the operation ran in a tenant.
    pub tenant: Option<Vec<u8>>,
}

/// One profiling event of a sampled transaction.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Event {
    /// Read version acquisition (`GET_VERSION_LATENCY`).
    GetVersion(GetVersion),
    /// Point read (`GET_LATENCY`).
    Get(Get),
    /// Range read (`GET_RANGE_LATENCY`).
    GetRange(GetRange),
    /// Successful commit (`COMMIT_LATENCY`).
    Commit(Commit),
    /// Failed point read (`ERROR_GET`).
    GetError(GetError),
    /// Failed range read (`ERROR_GET_RANGE`).
    GetRangeError(GetRangeError),
    /// Failed commit (`ERROR_COMMIT`).
    CommitError(CommitError),
}

impl Event {
    /// Returns the header shared by all event kinds.
    #[tracing::instrument(level = "trace", skip_all)]
    pub fn header(&self) -> &EventHeader {
        match self {
            Event::GetVersion(e) => &e.header,
            Event::Get(e) => &e.header,
            Event::GetRange(e) => &e.header,
            Event::Commit(e) => &e.header,
            Event::GetError(e) => &e.header,
            Event::GetRangeError(e) => &e.header,
            Event::CommitError(e) => &e.header,
        }
    }
}

/// Read version acquisition (`EventGetVersion_V3`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct GetVersion {
    /// Common event fields.
    pub header: EventHeader,
    /// Latency in seconds.
    pub latency: f64,
    /// `TransactionPriorityType`: 0 default, 1 batch, 2 immediate.
    pub priority: u32,
    /// The read version obtained.
    pub read_version: i64,
}

/// Point read (`EventGet`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Get {
    /// Common event fields.
    pub header: EventHeader,
    /// Latency in seconds.
    pub latency: f64,
    /// Size in bytes of the returned value.
    pub value_size: i32,
    /// The key read.
    pub key: Vec<u8>,
}

/// Range read (`EventGetRange`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct GetRange {
    /// Common event fields.
    pub header: EventHeader,
    /// Latency in seconds.
    pub latency: f64,
    /// Size in bytes of the returned key-values.
    pub range_size: i32,
    /// The range read.
    pub range: KeyRange,
}

/// Successful commit (`EventCommit_V2`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Commit {
    /// Common event fields.
    pub header: EventHeader,
    /// Latency in seconds.
    pub latency: f64,
    /// Number of mutations.
    pub num_mutations: i32,
    /// Size in bytes of the commit.
    pub commit_bytes: i32,
    /// Version at which the transaction committed.
    pub commit_version: i64,
    /// The commit request sent to the proxies.
    pub request: CommitRequest,
}

/// Failed point read (`EventGetError`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct GetError {
    /// Common event fields.
    pub header: EventHeader,
    /// FoundationDB error code.
    pub error_code: i32,
    /// The key read.
    pub key: Vec<u8>,
}

/// Failed range read (`EventGetRangeError`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct GetRangeError {
    /// Common event fields.
    pub header: EventHeader,
    /// FoundationDB error code.
    pub error_code: i32,
    /// The range read.
    pub range: KeyRange,
}

/// Failed commit (`EventCommitError`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct CommitError {
    /// Common event fields.
    pub header: EventHeader,
    /// FoundationDB error code (e.g. 1020 `not_committed` on conflict).
    pub error_code: i32,
    /// The commit request sent to the proxies.
    pub request: CommitRequest,
}

/// The serialized part of a `CommitTransactionRequest` (its `CommitTransactionRef`).
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct CommitRequest {
    /// Read conflict ranges.
    pub read_conflict_ranges: Vec<KeyRange>,
    /// Write conflict ranges.
    pub write_conflict_ranges: Vec<KeyRange>,
    /// Mutations, in commit order.
    pub mutations: Vec<Mutation>,
    /// Read version the transaction used for conflict detection.
    pub read_snapshot: i64,
    /// Whether the client asked for conflicting keys to be reported.
    pub report_conflicting_keys: bool,
    /// Whether the transaction was lock aware.
    pub lock_aware: bool,
    /// Tracing span context attached to the commit, if any.
    pub span_context: Option<SpanContext>,
}

/// A mutation (`MutationRef`), normalized like the C++ deserializer does.
///
/// The optional mutation checksum and accumulative checksum index (7.4+, only when the
/// client enables them) are stripped from `param2` and their flag bits cleared from
/// `mutation_type`. A single-key clear, which the wire encodes compactly, is expanded back
/// to `ClearRange(key, key + \x00)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Mutation {
    /// `MutationRef::Type` code (0 SetValue, 1 ClearRange, 2 AddValue, ...).
    pub mutation_type: u8,
    /// Key, or range begin for ClearRange.
    pub param1: Vec<u8>,
    /// Value/operand, or range end for ClearRange.
    pub param2: Vec<u8>,
}

impl Mutation {
    /// `MutationRef::Type::SetValue`.
    pub const SET_VALUE: u8 = 0;
    /// `MutationRef::Type::ClearRange`.
    pub const CLEAR_RANGE: u8 = 1;
    /// `MutationRef::Type::AddValue`.
    pub const ADD_VALUE: u8 = 2;
    /// `MutationRef::Type::And`.
    pub const AND: u8 = 6;
    /// `MutationRef::Type::Or`.
    pub const OR: u8 = 7;
    /// `MutationRef::Type::Xor`.
    pub const XOR: u8 = 8;
    /// `MutationRef::Type::AppendIfFits`.
    pub const APPEND_IF_FITS: u8 = 9;
    /// `MutationRef::Type::Max`.
    pub const MAX: u8 = 12;
    /// `MutationRef::Type::Min`.
    pub const MIN: u8 = 13;
    /// `MutationRef::Type::SetVersionstampedKey`.
    pub const SET_VERSIONSTAMPED_KEY: u8 = 14;
    /// `MutationRef::Type::SetVersionstampedValue`.
    pub const SET_VERSIONSTAMPED_VALUE: u8 = 15;
    /// `MutationRef::Type::ByteMin`.
    pub const BYTE_MIN: u8 = 16;
    /// `MutationRef::Type::ByteMax`.
    pub const BYTE_MAX: u8 = 17;
    /// `MutationRef::Type::MinV2`.
    pub const MIN_V2: u8 = 18;
    /// `MutationRef::Type::AndV2`.
    pub const AND_V2: u8 = 19;
    /// `MutationRef::Type::CompareAndClear`.
    pub const COMPARE_AND_CLEAR: u8 = 20;
}

/// Tracing span context attached to a commit (`SpanContext`).
///
/// Protocol 7.1 only carries a 16 byte trace id (`Optional<UID>`): `span_id` and `flags`
/// are then 0, as in the C++ deserializer. 7.2+ carries all three fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpanContext {
    /// Trace id, as the two 64-bit halves of a `UID` (`first`, `second`).
    pub trace_id: [u64; 2],
    /// Span id.
    pub span_id: u64,
    /// `TraceFlags` (bit 0: sampled).
    pub flags: u8,
}
