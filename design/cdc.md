# Native Change Data Capture (CDC)

## Status and scope

Native Change Data Capture (CDC) provides a FoundationDB-native mechanism for
reading committed mutations for a registered key range. A client registers a
named stream, creates a cursor for that name, consumes batches of mutations,
and acknowledges processed versions. The implementation persists enough state
to retain unread TLog data and to resume stream service after CDC proxy failure
or transaction-system recovery.

This design describes the native C++ interface and its server implementation.
The feature is disabled by default behind `ENABLE_NATIVE_CDC`; the native CDC
workloads explicitly enable it, and simulation may randomly enable it. The
initial interface is native-only: it does not expose bindings or an external
protocol compatibility guarantee.

The implementation uses the following terms:

* A **stream** is a durable named registration for a fixed user key range.
* A **cursor** identifies one stream and the version through which a consumer
  has read.
* A **CDC tag** is a TLog tag with locality `tagLocalityCDC`. Commit proxies
  append these tags to mutations covered by registered streams.
* A **CDC proxy** reads tagged TLog mutation streams, filters mutations to a
  registered range, serves consumers, and coordinates acknowledgement-driven
  log popping.

CDC is not implemented as a storage server change feed. It captures mutations
in the transaction logging path, which lets an acknowledged consumer retain
and release its own log history without changing user data storage.

## Goals

Native CDC is intended to provide:

* Durable, named registrations for key ranges in normal user key space.
* A cursor-based API in which a consumer only needs a stream name after
  registration, rather than repeating its registered range on every read.
* Ordered mutation batches identified by FoundationDB commit versions.
* Durable acknowledgements that determine how much CDC-tagged TLog history may
  be popped.
* Correct retention when several streams share a CDC tag, including streams
  whose ranges overlap or whose consumers advance at different rates.
* Replacement and recovery of CDC proxies without losing active stream
  ownership or prematurely releasing log data.
* Finite cleanup when streams are removed, so an old stream does not require
  CDC infrastructure forever.

The first implementation does not attempt to provide:

* Exactly-once side effects in the consumer. A consumer must make its output
  and its acknowledgement consistent if it needs exactly-once processing.
* Dynamic stream range changes. A name is registered for one range; changing a
  range requires removing and registering a stream.
* Throughput-aware assignment of streams across CDC proxies.
* Throughput-aware movement of streams between CDC tags.
* Client bindings beyond the native API.

## Client interface

The client-facing declarations are in `fdbclient/NativeCdc.h`; cursor and wire
request types are in `fdbclient/CDCProxyInterface.h`.

```cpp
Future<CDCStreamId> registerNativeCdcStreamClient(Database cx, Key name, KeyRange keys);
Future<Void> removeNativeCdcStreamClient(Database cx, Key name);
Future<std::vector<NativeCdcStreamInfo>> listNativeCdcStreamsClient(Database cx);

Future<CDCCursor> createNativeCdcCursor(Database cx, Key name);
Future<CDCConsumeReply> consumeNativeCdcStream(Database cx, CDCCursor cursor);
Future<Void> acknowledgeNativeCdcStreamClient(Database cx, CDCCursor cursor);
```

A stream registration contains:

```cpp
struct NativeCdcStreamInfo {
	Key name;
	CDCStreamId streamId;
	KeyRange keys;
	Version minVersion;
};
```

The durable identity of a stream is its `CDCStreamId`, not its name. Names are
used to create and manage streams. A cursor resolves the current stream ID
once, so removing a name and later registering the same name does not silently
redirect an existing consumer to a different stream.

```cpp
struct CDCCursor {
	CDCStreamId streamId;
	Version lastConsumedVersion;
};
```

`lastConsumedVersion` is initialized to `invalidVersion`. A consume response
returns both mutations and a new `lastConsumedVersion`:

```cpp
struct VersionedMutationsRef {
	Version version;
	VectorRef<MutationRef> mutations;
};

struct CDCConsumeReply {
	VectorRef<VersionedMutationsRef> mutations;
	Version lastConsumedVersion;
};
```

A typical consumer loop is:

```cpp
co_await registerNativeCdcStreamClient(db, "orders"_sr, KeyRangeRef("order/"_sr, "order0"_sr));
state CDCCursor cursor = co_await createNativeCdcCursor(db, "orders"_sr);

loop {
	CDCConsumeReply reply = co_await consumeNativeCdcStream(db, cursor);
	for (auto const& versionedMutations : reply.mutations) {
		// Apply all mutations for versionedMutations.version.
	}

	cursor.lastConsumedVersion = reply.lastConsumedVersion;
	co_await acknowledgeNativeCdcStreamClient(db, cursor);
}
```

The acknowledgement means that the consumer no longer requires CDC mutations
through `cursor.lastConsumedVersion`. Internally, acknowledgement advances the
stream's persisted minimum required version to `lastConsumedVersion + 1`.
Therefore the consumer must not acknowledge a returned cursor position before
it has durably processed all mutations represented through that position.
The server rejects an acknowledgement beyond its current read version, so a
consumer cannot pre-pop future mutations on a tag that may later be assigned
to another stream.

### Registration and removal semantics

`registerNativeCdcStreamClient()` accepts a non-empty stream name and a
non-empty range entirely within normal user keys. Registration of an existing
name with the same range is idempotent. Registering an existing name with a
different range is rejected.

Registration establishes an initial minimum version using the registration
transaction's commit version. Mutations committed after the registration has
become visible are routed to the stream's CDC tag. The initial minimum version
also supplies the first retention watermark for its TLog history.

`removeNativeCdcStreamClient()` removes the named stream and schedules final
release of tagged log history that was protected by the removed stream.
Removal explicitly relinquishes any unread history for that stream while still
respecting the retention needs of other streams sharing its tags. Stream
removal is terminal for existing cursors. Stale consume or acknowledgement
operations return an error instead of waiting indefinitely for an owner that
will never be assigned again.

### Consumption and expiration

Consumption is ordered by commit version. Mutations from a clear range are
intersected with the stream's registered range before being returned; a
single-key mutation is returned only if its key is within that range.

For an active stream, unacknowledged CDC mutations are retained by its durable
minimum version: TLogs must not pop tagged data that the stream may still
consume. A slow consumer therefore retains its unread history rather than
expiring solely because of age.

Consumption returns `transaction_too_old` when the caller supplies a cursor
older than the stream's already acknowledged durable watermark. The proxy also
treats discovery that an active stream's required tagged data has nevertheless
already been popped as `transaction_too_old`; that condition indicates a
retention invariant violation rather than a supported expiration policy.

The native client methods retry transient endpoint and routing failures such as
a CDC proxy replacement. Invalid stream operations, already-acknowledged
cursor positions, and retention invariant violations are terminal errors for
that request.

## Architecture

The data path is:

```text
                            durable stream and acknowledgement state
                                      +-------------------------+
                                      | transaction state store |
                                      | and system key storage  |
                                      +------------+------------+
                                                   |
                                                   v
Client <---- consume / acknowledge ----> CDC Proxy <---- peek / pop ---- TLogs
                                             ^                            ^
                                             |                            |
                                             | assigned streams           | ordinary and
                                             |                            | CDC tags
                                      Cluster Controller            Commit Proxies
                                                                        ^
                                                                        |
                                                                  user commits
```

Commit proxies keep a routing table derived from durable CDC metadata. For
each committed user mutation, the commit proxy determines which registered CDC
ranges include that mutation and appends the corresponding CDC tags to the
mutation sent to TLogs. A transaction continues to follow its ordinary
replication tags as well; CDC tags are additional log destinations used by CDC
consumers.

CDC proxies do not participate in committing user transactions. They consume
the extra tagged log streams, buffer readable results, filter shared tagged
data back to each stream's registered range, and pop data after durable
acknowledgement permits it.

The cluster controller recruits CDC proxies, publishes their interfaces, and
keeps durable stream-to-proxy ownership consistent with current endpoints.

## Durable state

CDC uses two categories of system data. Routing and recovery-critical metadata
is stored in the transaction state store, where commit proxies and recovery
can reconstruct it in transaction order. Acknowledgement and final-pop
watermarks are stored as regular storage-server-backed system keys, because
they are durable progress values rather than commit routing configuration.

### Transaction state metadata

These keys are in the metadata portion of system key space and are represented
in transaction state:

| Key | Value | Purpose |
| --- | --- | --- |
| `\xff/cdc/name/<name>` | `CDCStreamId` | Resolves a user-visible name to its durable stream identity. |
| `\xff/cdc/maxStreamId` | `CDCStreamId` | Allocates monotonic stream identifiers. |
| `\xff/cdc/keys/<streamId>` | `KeyRange` | Stores the immutable registered range for an active stream. |
| `\xff/cdc/tagHistory/<streamId>/<version>/<tag>` | empty | Records the CDC tag assignment history used for routing and historical reads. |
| `\xff/cdc/proxies/<streamId>/<proxyId>` | empty | Stores the CDC proxy assigned to an active stream. |
| `\xff/cdc/proxyAssignmentChange` | version/change signal | Wakes ownership monitoring when durable assignments change. |
| `\xff/cdc/retiredTagPop/<tag>` | empty | Retains recovery-visible pending final-pop work after removal. |

Tag history is versioned so the data model can support a stream moving between
tags without forgetting which old log streams may still contain unread
mutations. The initial implementation writes the initial assignment and reads
the history; dynamic throughput-driven reassignment is future work.

### Storage-backed system data

These keys are in the storage-server-backed `\xff\x02` system key range rather
than transaction state:

| Key | Value | Purpose |
| --- | --- | --- |
| `\xff\x02/cdc/minVersion/<streamId>` | `Version` | Earliest version that an active stream may still require. |
| `\xff\x02/cdc/retiredTagPopVersion/<tag>` | `Version` | Final pop watermark required after a stream using a tag is removed. |

The initial `minVersion` is written with a versionstamp at stream
registration. When a cursor acknowledges processing through version `V`, the
stored value advances monotonically to `V + 1`. A CDC proxy may pop tagged
mutations before this watermark only when doing so is safe for every live
stream sharing that tag.

The retired-tag marker and watermark are deliberately split between transaction
state and regular system storage. The marker tells recovery that a CDC proxy
must still be recruited to finish durable cleanup; the watermark bounds the
actual final pop to perform.

## Stream creation and assignment

Registration runs as a durable metadata transaction:

1. It validates the feature knob, the stream name, and the registered normal
   key range.
2. It checks whether the name is already registered and applies the idempotent
   same-name/same-range rule.
3. It allocates a new monotonically increasing `CDCStreamId`.
4. It selects a CDC tag using current active stream counts. The allocator uses
   the least populated tag among `NATIVE_CDC_TAG_COUNT` tags, choosing the
   lowest tag ID on a tie.
5. It records the stream name, range, initial tag history entry, and
   versionstamped initial minimum version.
6. It records an available CDC proxy owner and signals assignment monitoring.

The tag allocator bounds the number of distinct CDC log streams while allowing
many user streams. Several streams may therefore share one tag intentionally.
This makes filtering and acknowledgement coordination required correctness
properties rather than exceptional cases.

The current proxy assignment at registration uses an available CDC proxy; it
does not yet balance by stream traffic, memory use, or consumer lag.

## Commit routing

Each commit proxy has a `CDCRoutingTable`, reconstructed from active stream
ranges and tag history in transaction state. Changes to CDC stream metadata
are applied in commit order along with other transaction state mutations, so
the routing decision for later mutations observes committed registration and
removal changes.

For a single-key mutation, the routing table returns CDC tags for all active
stream ranges containing that key. For a clear-range mutation, it returns tags
for all active stream ranges intersecting the cleared interval. These CDC tags
are appended in both the tag-determination and log-writing portions of commit
proxy processing.

A shared CDC tag is a multiplexed log stream. A mutation routed because of
stream A may be read by the proxy serving stream B if both share the tag.
Consequently, the CDC proxy filters every read mutation against B's registered
range before returning it to B's consumer. Filtering also clips clear ranges
to the stream range.

Routing at commit time has two important implications:

* CDC observes mutations once their commits enter the transaction logging
  path, without scanning storage server data.
* Registering CDC does not change normal durability for the user mutation; it
  adds tagged log data whose retention is controlled separately by consumers.

## CDC proxy read path

A CDC proxy owns a set of active stream IDs. For each owned stream it loads:

* The registered key range.
* The durable minimum required version.
* Its current CDC tag and versioned tag history.

The proxy reads data from TLogs through `LogSystemConsumer::peekSingle()`.
When a stream has historical assignments, the proxy uses the history to select
the tag appropriate for the version interval it is reading. It filters
mutations to the registered range and stores versioned mutation batches in a
per-stream in-memory buffer.

All stream buffers owned by one CDC proxy share a `CDC_PROXY_BUFFER_BYTES`
budget. Before requesting more TLog data, a stream reserves a bounded peek
window from that budget, then converts the reservation to the actual filtered
mutation bytes retained in its buffer. Acknowledgement or stream removal
releases the retained reservation. This applies backpressure before ordinary
peek batches arrive, rather than allowing each stream or each received batch
to independently overshoot the proxy limit. A slow consumer does not require
the proxy to buffer its entire retained history in memory: durable
acknowledgement state and tagged TLog retention are the source of resumability,
while the proxy buffer is a delivery optimization.

A consume operation supplies a cursor. The proxy returns buffered or newly
peeked data after the cursor position and a position through which the
consumer may acknowledge after processing. If a stream is removed while a
consume is blocked, reconciliation wakes the request and it fails rather than
waiting on a data-change trigger for an inactive stream.

## Acknowledgement and tag popping

Acknowledgement is per stream, while TLog popping is per CDC tag. This
distinction is the core retention rule.

For every active stream `S`, `minVersion(S)` is the first version its consumer
may still need. For a tag `T`, the safe pop watermark is:

```text
safePop(T) = min(minVersion(S)) for every live stream S whose history uses T
```

Popping tag `T` through `safePop(T)` discards versions older than the minimum
required by any live stream using that tag. A fast consumer therefore cannot
pop mutations still needed by a slower stream sharing its tag.

The proxy recomputes these minima from durable active stream metadata and
acknowledgement rows. It does not rely solely on its in-memory owned-stream
set, because shared tags and replacement proxies must preserve the same global
retention decision.

### Removing a stream

Removing a stream eliminates its active name, range, tag history, minimum
version, and ownership rows. Removal must not unconditionally pop each tag in
the removed history: a different live stream may share a tag and still need
older data.

Before deleting active state, removal writes pending final-pop state for each
historical tag:

* A transaction-state retired-tag marker, which survives recovery and makes
  outstanding cleanup discoverable.
* A versionstamped storage-backed retired-tag watermark, which identifies the
  upper bound of tagged history protected by the removed stream.

The CDC proxy processes retired work together with active acknowledgement
watermarks. If a retired tag is also used by a live stream, its attempted pop
is capped at that live stream's `safePop` value. Only when it is possible to
pop through the complete retired watermark is the retired operation eligible
for completion.

### Completing retired work

Retired final-pop state is durable work, not permanent stream state. After
issuing a complete retired pop, a CDC proxy waits for every targeted current
TLog to report that the tag has been popped through the required watermark.
It then transactionally clears both the retired marker and its stored
watermark.

The cleanup transaction rereads the watermark before clearing it. If a newer
stream removal has advanced the retired watermark for the same tag while the
earlier pop was in progress, the newer work is retained rather than erased by
the older completion.

This establishes the lifecycle invariant:

```text
no live streams and no pending retired final pops
    implies no durable CDC proxy requirement
```

Without this completion protocol, removing any stream would leave a retired
marker forever, causing later recoveries to recruit CDC proxies indefinitely
and repeatedly replay already-completed final pops.

## Failure handling and recovery

### CDC proxy failure

CDC proxies are consumers of durable state and tagged logs, not authorities
for committed application data. If a CDC proxy fails, the cluster controller
can recruit a replacement, publish the replacement interface, and durably
reassign affected streams. The replacement reloads stream state and resumes
reading at durable acknowledgement watermarks.

The cluster controller monitors durable proxy assignment rows and repairs
stale owner identifiers when endpoints are replaced or the controller itself
is reconstructed. Clients obtain the currently published owner for their
stream ID and retry transient proxy/routing failures.

### Transaction-system recovery

During full recovery, active stream ranges, tag history, and pending retired
markers are present in transaction state. Active history tags are added to the
set of log tags that recovery must preserve; otherwise TLog generations could
discard CDC data that an active consumer has not yet acknowledged.

CDC proxy recruitment is required during recovery when either:

* Native CDC is enabled and streams may be served, or
* Durable CDC state remains, including retired final-pop work that must be
  completed even after admission of new CDC activity is disabled.

When the knob is disabled and recovery finds neither active streams nor
retired work, no CDC proxies are published. This makes stream removal and
retired-pop completion observable as a finite drain rather than a permanent
cluster role.

## Feature gating

`ENABLE_NATIVE_CDC` defaults to false. In simulation it may be randomly enabled
under buggification; workloads that depend on CDC set it explicitly.

The feature knob gates client admission to native CDC operations. Internal
cleanup and recovery paths remain capable of handling durable CDC state that
was created while the feature was enabled. This is necessary because disabling
new use of a feature cannot safely abandon log-retention obligations for
already registered or recently removed streams.

`NATIVE_CDC_TAG_COUNT` controls the bounded tag pool used for new stream
allocation. Normal operation defaults to a larger tag pool; simulation may
reduce it so shared-tag behavior is exercised frequently.

## Correctness properties

The implementation is structured around the following properties:

* **Registration identity:** a cursor binds to a stream ID, so reuse of a
  removed stream name cannot cause an existing consumer to read a new stream.
* **Range correctness:** CDC proxies return only mutations within a stream's
  registered range, even when its tag is shared with other streams.
* **Acknowledgement monotonicity:** durable minimum required versions advance
  only forward.
* **Shared-tag retention:** tagged data is popped no farther than the minimum
  durable watermark of all active streams that may require it.
* **Removal safety:** deleting one stream cannot pop unread data required by
  another stream using the same tag.
* **Finite retired cleanup:** removal retains enough state to finish final
  pops through failure and recovery, then removes that state after completion.
* **No age-based expiration:** an active stream's unread tagged mutations
  remain protected until acknowledgement or explicit stream removal.
  `transaction_too_old` for required active history indicates either a stale
  already-acknowledged cursor or a violated retention invariant.
* **Failure visibility:** a removed stream fails consume and acknowledgement
  requests rather than leaving them blocked indefinitely.
* **Recovery retention:** active CDC tag history is included in recovery's
  required log data, and pending cleanup retains CDC proxy availability until
  it has been completed.

## Current limitations and future work

The design records tag history and proxy ownership in forms that support more
complete load balancing, but the first implementation intentionally keeps
policy simple.

* Tag selection is based on active stream counts, not observed byte or mutation
  throughput. Data distribution could make equally counted tags very
  different in cost.
* Registration selects an available CDC proxy without balancing aggregate
  proxy throughput, buffer memory, lag, or number of active readers.
* There is no background process that changes a live stream's CDC tag in
  response to load. A future implementation can use versioned tag history to
  make such changes without losing the ability to read earlier tagged data.
* The native interface does not yet provide external binding support,
  administrative tooling, or a higher-level consumer checkpoint abstraction.

These improvements must preserve the acknowledgement and retired-pop
invariants above. In particular, moving a stream between tags cannot forget an
old tag until all data protected by that assignment has either been
acknowledged and popped or retained as finite final-pop work.

## Validation

The implementation includes codec and metadata tests for CDC system keys and
simulation workloads for the end-to-end behavior.

The basic native CDC workload covers:

* Registering, listing, consuming, acknowledging, and removing streams.
* Name-based cursor creation and correct filtering of returned mutations.
* Rejection of incompatible same-name registrations.
* CDC proxy replacement and recovery of stream service.
* Errors for stale consume and acknowledgement requests after removal.
* Creation and eventual collection of retired final-pop state.
* Recovery with native CDC disabled after the last stream and final-pop work
  have drained, verifying that no CDC proxy remains required.

The shared-tag workload forces streams to share routing tags and verifies both
range filtering and acknowledgement coordination. In particular, removing one
stream while another shared-tag stream is behind must not pop the unread
mutations needed by the remaining consumer.

The simulation configurations enable CDC explicitly when testing these
behaviors, while the default-disabled knob and randomized simulation admission
exercise the requirement that clusters without active or pending CDC work do
not carry CDC service overhead.
