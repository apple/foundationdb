# RangeDigest: a storage-server-side content fingerprint for backup/restore validation

## Objective

Provide a way to prove that a FoundationDB backup and restore preserved the user
data *exactly* — every key-value present, none added, dropped, or corrupted —
without shipping the entire dataset to an external verifier. The mechanism is a
256-bit **content fingerprint** ("RangeDigest") of the key-value multiset, computed
in parallel by the storage servers that already hold the data, such that a single
root taken **before backup** equals the root taken **after restore** if and only if
the restored data is identical as a set, *regardless of how the two clusters shard
the keyspace*.

## Background

Validating a restore at scale is hard precisely where it matters most. A backup of
a multi-terabyte database is only trustworthy if we can cheaply and rigorously
confirm the restore reproduced it. The obvious approach — have a client read the
whole keyspace and hash it — does not scale: at 10 TB it means moving 10 TB across
the network through one process, taking many hours and stressing exactly the read
path we are trying to trust.

FDB already distributes data across storage servers and moves it continuously
(shard splits, merges, rebalancing, exclusions). A restore in particular lands the
same logical data under a completely different shard layout than the original. So
any integrity check must be **independent of the physical partition** — two clusters
holding the identical key-value set must produce the identical fingerprint even
though no shard boundary lines up.

An external Phase-0 prototype (`fdbfingerprint`) established the per-key-value leaf
encoding and validated the idea end-to-end from a client, with matching before/after
roots at 1B and 3B. It was abandoned on measured cost: fingerprinting the 3B dataset
took **9h11m at ~79 MB/s aggregate**, and it put that entire read load on the very
path under validation. At 10B and beyond, a pre- and post-restore pair of those runs
is not a check anyone will wait for.

Two honest qualifications. First, that 79 MB/s was **not a hard client ceiling**: it
worked out to ~4 MB/s per disk across 20 disks that sat ~98% idle, so the prototype
was under-parallelized — 8 pods, each a single-network-thread client that could not
hide cold-read latency — and a tuned client would have gone considerably faster.
Second, the server-side digest measured ~2770 MB/s at 10B, but against that untuned
client, so the ~35× gap is not a like-for-like comparison.

The architectural argument does not rest on those numbers. An external fingerprint
scales with whatever bandwidth one client pool is given and consumes read capacity
that production traffic needs, whereas hashing on the servers that already hold the
data scales with storage-server count and moves only 32-byte digests. RangeDigest
keeps the Phase-0 leaf encoding byte-for-byte (so the two agree) but makes the
fingerprint an `AuditType` computed in parallel by the storage servers.

## Requirements

1. **Set-exactness.** Two datasets produce equal roots iff they are equal as
   multisets of key-value pairs. Any missing, extra, or modified key-value changes
   the root. (Keys in FDB user space are unique, so "multiset" and "set" coincide.)
2. **Partition independence.** The root is a function of the key-value set only —
   not of shard boundaries, storage-server assignment, or the order data is read.
   This is mandatory for before-backup vs after-restore comparison.
3. **Scales with the cluster, not a client.** Per-server data is hashed locally on
   the server that owns it; only fixed-size digests cross the network. No user data
   is moved to compute the root.
4. **Localizable mismatch.** When two roots differ, the divergent key range can be
   narrowed down without re-reading everything.
5. **Bounded, tunable footprint.** The audit must not starve the foreground
   workload; it runs rate-limited and in batches.
6. **Sufficient collision resistance for a non-adversarial integrity check.**
   Accidental collisions between distinct honest datasets must be astronomically
   unlikely at realistic scale (billions of keys).

Explicit non-goal: defense against an adversary who deliberately crafts colliding
datasets (see Alternatives / security posture).

## Design Overview

Defined terms:

- **Leaf** — the SHA-256 hash of one canonically-encoded key-value pair.
- **RangeDigest** — a 256-bit accumulator (`std::array<uint8_t,32>`, big-endian)
  holding the sum, modulo 2²⁵⁶, of the leaves folded into it. Zero is the identity
  and the digest of the empty set.
- **combine** — addition of two RangeDigests modulo 2²⁵⁶.
- **Root** — the RangeDigest of the entire user keyspace: the combine of every
  storage server's per-range digest.

The construction is an incremental **multiset hash** ("AdHash"): hash each element,
sum the hashes in a large modular group. Because the group operation (addition mod
2²⁵⁶) is associative and commutative, the sum is independent of grouping and order —
which is exactly the partition-independence requirement.

## Detailed Design

### Leaf encoding and accumulation

Implemented in `fdbclient/RangeDigest.{h,cpp}`. For each key-value pair:

```
leaf = SHA-256( u32be len(key) | key | u32be len(value) | value )
```

The explicit big-endian length prefixes make the encoding unambiguous (no
key/value boundary confusion), and it is identical to the `fdbfingerprint` Phase-0
tool so client-side and server-side computations agree.

`addKeyValue` folds a leaf into the accumulator as a 256-bit big-endian add with
carry from the least-significant byte (index 31). `combine` adds two accumulators
the same way. `root = ( Σ over all kv of leaf(kv) ) mod 2²⁵⁶`.

### Why 256 bits

The accumulator width matches the SHA-256 leaf so leaves add with no truncation.
256 bits gives a birthday bound of ~2¹²⁸ against *accidental* collision between
distinct honest datasets — dwarfing any real dataset (10 billion keys ≈ 2³³), at a
trivial 32 bytes on the wire per range. A wider accumulator would only add cost;
the width is driven by collision margin, not by network size.

### Server-side computation and audit integration

RangeDigest is exposed as `AuditType::RangeDigest`, dispatched to
`auditRangeDigestQ` on the storage server (`serveAuditStorageRequests`). Each
storage server folds the key-values it owns into a per-range RangeDigest locally,
rate-limited and batched so the audit yields to foreground traffic. Per-range
digests are persisted and combined up to a single cluster root. Only 32-byte
digests — never user data — cross the network, so throughput scales with the
number of storage servers and their disks.

### Precondition: quiescence

The digest must be taken over a **quiescent** cluster: writes stopped and data
distribution settled (`moving_data → 0`). Two independent invariants require this:

1. **Single version.** Each server folds its owned key-values at *its own* current
   read version; there is no single cluster-wide pinned version. Concurrent writes
   would let servers hash at inconsistent versions, so the root would not
   correspond to any one snapshot.
2. **Exactly-once.** The additive combine assumes every key-value is folded exactly
   once cluster-wide. During in-flight shard movement a key can be counted by both
   the losing and the gaining server (double-counted) or by neither at the instant
   it is read (missed) — either yields a wrong root. This was observed empirically
   as a 100M-scale root mismatch traced to reading during data-distribution churn.

Callers therefore wait for quiescence before reading, on *both* the before and
after sides of a comparison. For backup/restore this is natural: both states are
settled datasets.

**Future work (see TODO in `RangeDigest.h`):** quiescence is an implementation
choice, not inherent. Folding every server against a single pinned cluster read
version, with an ownership snapshot consistent with that version, would make the
digest correct **online** by re-establishing the single-version and exactly-once
invariants MVCC-style. It is unnecessary for the backup/restore use case (which
compares two settled states) but would generalize the primitive to a live cluster.

### Mismatch localization

Two roots differing tells you the datasets differ, not where. Localization works
from the per-range digests, which each storage server emits as it finishes a task:

    SSAuditRangeDigestComplete  AuditRange=<range> Digest=<hex> KVCount=<n> Bytes=<n>

Diffing those events between the two runs identifies the ranges whose digests
disagree. This is the primary mechanism, and it is how a 935,560 key-value
shortfall was localized to a single bulkload task at 10B scale. Two consequences
follow from it living in the trace logs: localization depends on log retention, and
it needs the events from *both* runs, so the earlier run's logs must be kept as long
as its root is still something you might compare against.

While an audit is `Running` the same per-range digests are also queryable directly:

    get_audit_status range_digest progress <id>

That view disappears at `Complete`, when the progress metadata is cleared.

Bisection by re-audit is the fallback: because the root is additive over any
partition, a digest over a narrower range is directly comparable, so the divergent
region can be found by halving. It costs about one extra full digest in total
(`N/2 + N/4 + … ≈ N`) rather than a few cheap probes, and — the real limitation — it
requires *both* datasets to still exist. That does not hold for the backup/restore
case this feature was built for: the source keyspace is cleared before the restore,
so only the restored side can be re-audited and the original survives solely as a
32-byte root. Re-audit is therefore available when fingerprinting a live cluster
that is still around, and the trace events are what remain otherwise.

### Failure handling

The digest is a read-only audit; a partial or interrupted audit yields no root and
is simply retried. A wrong-length or empty persisted state parses as the zero
digest (no contribution), so a missing per-range digest cannot silently corrupt a
root — it makes the combine visibly incomplete rather than plausibly wrong.

## Alternatives Considered

- **External client reads and hashes everything** (the Phase-0 `fdbfingerprint`
  approach). Correct, simple, and actually built — it produced matching roots at 1B
  and 3B. Rejected as the mechanism on measured cost: **9h11m at ~79 MB/s for the 3B
  dataset**, with all of that read load falling on the path under validation, and a
  comparison needs two such runs. As noted in Background, that rate reflected an
  under-parallelized client (~4 MB/s across 20 mostly-idle disks) rather than a hard
  client limit, so the objection is not "a client cannot go fast" but that a client
  scales with the bandwidth you hand it while consuming capacity production needs. Its
  leaf encoding is retained as the reference and an independent cross-check.
- **Merkle tree / hash over a canonical shard list.** Order- and partition-*dependent*
  by construction: a restore's different shard boundaries would change the root even
  for identical data. Fails requirement 2. Additive hashing is chosen specifically
  to be partition-independent.
- **List of per-shard hashes compared pairwise.** Same problem — shard boundaries
  differ across backup/restore, so there is no shard-to-shard correspondence to
  compare, and it cannot produce a single stable root.
- **Keyed/cryptographic MAC (adversarial collision resistance).** Additive multiset
  hashes are not collision-resistant against an adversary who chooses inputs
  (subset-sum / lattice structure). We deliberately do not defend against that: the
  data is FDB's own, on a trusted cluster, and the threat is accidental corruption,
  not a crafted collision. The 256-bit accidental-collision margin is what matters,
  and a MAC would add key management and cost for a threat outside scope.
- **Narrower digest (64/128-bit).** Cheaper on the wire but erodes the accidental-
  collision margin; 32 bytes per range is already negligible, so there is no reason
  to shrink it.

## Testing Considerations

- **Simulation** (`tests/fast/RangeDigestValidation.toml`, workload
  `fdbserver/workloads/RangeDigestValidation.cpp`): under Sim2 with fault injection, the workload
  writes a known key-value set, then (a) cross-checks the audit's combined root against an
  **independent client-side computation** of the same additive digest — scanning every key-value and
  applying the canonical leaf encoding, which validates the storage-server fold *and* the combine —
  and (b) runs a **second** audit and asserts the root is identical. Data distribution may have
  moved shards in between, but nothing forces it to, so treat this as a determinism check that is
  additionally partition-independent on the seeds where movement did occur. The workload waits for
  the data to split into shards, but it does **not** wait for `moving_data -> 0`, so the precondition
  is not established in simulation — a root mismatch caused by movement would surface as a hard
  failure. The full backup → clear → restore → recompute cycle is exercised by the Kubernetes test
  below, not in simulation.
- **Skips are failures.** `runOneDigest` retries the transient audit errors itself; if the digest
  still never reaches `Complete` the workload records `RangeDigestValidationDidNotComplete` and
  `check()` fails, so a permanently broken digest cannot pass as a skip. `RangeDigest` is excluded
  from the DD-restart fault injection in `launchAudit` for the same reason. The content assertions
  deliberately sit *outside* the `try` so a genuine mismatch can never be swallowed either.
- **Shard coverage caveat.** The committed configuration is deliberately modest
  (`nodeCount=10000`) to stay cheap across the Joshua matrix, so some seeds keep the data in a
  single shard and do not exercise the cross-server combine. `strictShardCheck=true` turns
  multi-shard coverage into a hard requirement for dedicated runs.
- **Scale validation on Kubernetes** (`test_backup_restore_rangedigest`): the full
  before/backup/clear/restore/after/compare cycle at 100M, 1B, 3B, and 10B. The 10B
  dataset (~9.8 billion key-values, ~9.26 TB) was fingerprinted with
  `root_before = fa1bee7a…` and retained as a reusable bulkdump baseline; a restore
  passes iff `root_after` reproduces it.
- **Measured throughput at 10B (2026-08-13).** Digesting 9,257,887,466,305 bytes over
  9,808,347,148 key-values took **53m07s, ~2770 MB/s aggregate**, on a 30-machine cluster with
  4 storage disks per machine. For scale, the external Phase-0 client fingerprinted the smaller
  3B dataset in 9h11m at ~79 MB/s — but that client was under-parallelized (see Background), so
  read the two as "server-side reaches cluster-scale rates" rather than as a tuned 35× speedup.
- **Reproducibility across time.** The same 10B dataset re-digested to an identical root on a
  later run against the same cluster, with no restore involved — so a matching root is not an
  artifact of digesting twice in quick succession.
- **Cross-implementation agreement:** the server-side leaf hash must match the
  Phase-0 `fdbfingerprint` tool byte-for-byte.

## Observability/Supportability Considerations

- Per-range digests are progress metadata and are cleared when the audit completes; a mismatch is
  localized by re-auditing narrower ranges rather than by inspecting them.
- The validation workload emits `RangeDigestValidationSuccess` with the root, key-value count and
  byte count on a successful comparison, and fails `check()` if the comparison never ran.
- Because the digest reads at cluster scale, its throughput (MB/s) and duration are
  worth tracking as a proxy for storage-server read health, especially since a low
  block-cache-to-data ratio can make the read-heavy audit disk-bound.

## Rollout/Migration Considerations

RangeDigest is additive and opt-in: a new `AuditType` invoked on demand, with no
on-disk format change to existing data and no effect on clusters that never request
it. There is nothing to migrate. Rollback is simply not issuing the audit. The
online (pinned-version) generalization in Future Work can land later without
changing the leaf encoding or the root of a quiesced dataset, so fingerprints taken
today remain comparable.
