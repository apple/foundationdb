/*
 * storageserver.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cinttypes>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <type_traits>
#include <unordered_map>

#include "fdbclient/BlobCipher.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BulkLoading.h"
#include "fdbclient/Knobs.h"
#include "fdbrpc/TenantInfo.h"
#include "flow/ApiVersion.h"
#include "flow/Buggify.h"
#include "flow/Platform.h"
#include "flow/network.h"
#include "fmt/format.h"
#include "fdbclient/Audit.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/LoadBalance.h"
#include "fdbserver/OTELSpanContextMessage.h"
#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/Hash3.h"
#include "flow/Histogram.h"
#include "flow/PriorityMultiLock.actor.h"
#include "flow/IRandom.h"
#include "flow/IndexedSet.h"
#include "flow/SystemMonitor.h"
#include "flow/Trace.h"
#include "fdbclient/Tracing.h"
#include "flow/Util.h"
#include "fdbclient/Atomic.h"
#include "fdbclient/AuditUtils.actor.h"
#include "fdbclient/BlobConnectionProvider.h"
#include "fdbclient/BlobGranuleReader.actor.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Notified.h"
#include "fdbclient/StatusClient.h"
#include "fdbclient/StorageServerShard.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/TransactionLineage.h"
#include "fdbclient/Tuple.h"
#include "fdbclient/VersionedMap.h"
#include "fdbrpc/sim_validation.h"
#include "fdbrpc/Smoother.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/AccumulativeChecksumUtil.h"
#include "fdbserver/BulkDumpUtil.actor.h"
#include "fdbserver/BulkLoadUtil.actor.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbclient/GetEncryptCipherKeys.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LatencyBandConfig.h"
#include "fdbserver/LogProtocolMessage.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/MutationTracking.h"
#include "fdbserver/OTELSpanContextMessage.h"
#include "fdbserver/Ratekeeper.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "fdbserver/ServerCheckpoint.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/SpanContextMessage.h"
#include "fdbserver/StorageMetrics.actor.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/TransactionTagCounter.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/StorageCorruptionBug.h"
#include "fdbserver/StorageServerUtils.h"
#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/Hash3.h"
#include "flow/Histogram.h"
#include "flow/IRandom.h"
#include "flow/IndexedSet.h"
#include "flow/SystemMonitor.h"
#include "flow/TDMetric.actor.h"
#include "flow/Trace.h"
#include "flow/Util.h"
#include "flow/genericactors.actor.h"
#include "fdbserver/FDBRocksDBVersion.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#ifndef __INTEL_COMPILER
#pragma region Data Structures
#endif

#define SHORT_CIRCUT_ACTUAL_STORAGE 0

namespace {
enum ChangeServerKeysContext { CSK_UPDATE, CSK_RESTORE, CSK_ASSIGN_EMPTY, CSK_FALL_BACK };

std::string changeServerKeysContextName(const ChangeServerKeysContext& context) {
	switch (context) {
	case CSK_UPDATE:
		return "Update";
	case CSK_RESTORE:
		return "Restore";
	case CSK_ASSIGN_EMPTY:
		return "AssignEmpty";
	case CSK_FALL_BACK:
		return "FallBackToFetchKeys";
	default:
		ASSERT(false);
	}
	return "UnknownContext";
}

bool canReplyWith(Error e) {
	switch (e.code()) {
	case error_code_transaction_too_old:
	case error_code_future_version:
	case error_code_wrong_shard_server:
	case error_code_process_behind:
	case error_code_watch_cancelled:
	case error_code_unknown_change_feed:
	case error_code_server_overloaded:
	case error_code_change_feed_popped:
	case error_code_tenant_name_required:
	case error_code_tenant_removed:
	case error_code_tenant_not_found:
	case error_code_tenant_locked:
	// getMappedRange related exceptions that are not retriable:
	case error_code_mapper_bad_index:
	case error_code_mapper_no_such_key:
	case error_code_mapper_bad_range_decriptor:
	case error_code_quick_get_key_values_has_more:
	case error_code_quick_get_value_miss:
	case error_code_quick_get_key_values_miss:
	case error_code_get_mapped_key_values_has_more:
	case error_code_key_not_tuple:
	case error_code_value_not_tuple:
	case error_code_mapper_not_tuple:
		// case error_code_all_alternatives_failed:
		return true;
	default:
		return false;
	}
}

} // namespace

#define PERSIST_PREFIX "\xff\xff"

FDB_BOOLEAN_PARAM(UnlimitedCommitBytes);
FDB_BOOLEAN_PARAM(MoveInFailed);
FDB_BOOLEAN_PARAM(MoveInUpdatesSpilled);

// Immutable
static const KeyValueRef persistFormat(PERSIST_PREFIX "Format"_sr, "FoundationDB/StorageServer/1/4"_sr);
static const KeyValueRef persistShardAwareFormat(PERSIST_PREFIX "Format"_sr, "FoundationDB/StorageServer/1/5"_sr);
static const KeyRangeRef persistFormatReadableRange("FoundationDB/StorageServer/1/2"_sr,
                                                    "FoundationDB/StorageServer/1/6"_sr);
static const KeyRef persistID = PERSIST_PREFIX "ID"_sr;
static const KeyRef persistTssPairID = PERSIST_PREFIX "tssPairID"_sr;
static const KeyRef persistSSPairID = PERSIST_PREFIX "ssWithTSSPairID"_sr;
static const KeyRef persistTssQuarantine = PERSIST_PREFIX "tssQ"_sr;

// (Potentially) change with the durable version or when fetchKeys completes
static const KeyRef persistVersion = PERSIST_PREFIX "Version"_sr;
static const KeyRangeRef persistShardAssignedKeys =
    KeyRangeRef(PERSIST_PREFIX "ShardAssigned/"_sr, PERSIST_PREFIX "ShardAssigned0"_sr);
static const KeyRangeRef persistShardAvailableKeys =
    KeyRangeRef(PERSIST_PREFIX "ShardAvailable/"_sr, PERSIST_PREFIX "ShardAvailable0"_sr);
static const KeyRangeRef persistByteSampleKeys = KeyRangeRef(PERSIST_PREFIX "BS/"_sr, PERSIST_PREFIX "BS0"_sr);
static const KeyRangeRef persistByteSampleSampleKeys =
    KeyRangeRef(PERSIST_PREFIX "BS/"_sr PERSIST_PREFIX "BS/"_sr, PERSIST_PREFIX "BS/"_sr PERSIST_PREFIX "BS0"_sr);
static const KeyRef persistLogProtocol = PERSIST_PREFIX "LogProtocol"_sr;
static const KeyRef persistPrimaryLocality = PERSIST_PREFIX "PrimaryLocality"_sr;
static const KeyRangeRef persistChangeFeedKeys = KeyRangeRef(PERSIST_PREFIX "CF/"_sr, PERSIST_PREFIX "CF0"_sr);
static const KeyRangeRef persistTenantMapKeys = KeyRangeRef(PERSIST_PREFIX "TM/"_sr, PERSIST_PREFIX "TM0"_sr);
// data keys are unmangled (but never start with PERSIST_PREFIX because they are always in allKeys)

static const KeyRangeRef persistStorageServerShardKeys =
    KeyRangeRef(PERSIST_PREFIX "StorageServerShard/"_sr, PERSIST_PREFIX "StorageServerShard0"_sr);

// Checkpoint related prefixes.
static const KeyRangeRef persistCheckpointKeys =
    KeyRangeRef(PERSIST_PREFIX "Checkpoint/"_sr, PERSIST_PREFIX "Checkpoint0"_sr);
static const KeyRangeRef persistPendingCheckpointKeys =
    KeyRangeRef(PERSIST_PREFIX "PendingCheckpoint/"_sr, PERSIST_PREFIX "PendingCheckpoint0"_sr);
static const std::string serverCheckpointFolder = "serverCheckpoints";
static const std::string checkpointBytesSampleTempFolder = "/metadata_temp";
static const std::string fetchedCheckpointFolder = "fetchedCheckpoints";
static const std::string serverBulkDumpFolder = "bulkDumpFiles";
static const std::string serverBulkLoadFolder = "bulkLoadFiles";

static const KeyRangeRef persistBulkLoadTaskKeys =
    KeyRangeRef(PERSIST_PREFIX "BulkLoadTask/"_sr, PERSIST_PREFIX "BulkLoadTask0"_sr);

// Accumulative checksum related prefix
static const KeyRangeRef persistAccumulativeChecksumKeys =
    KeyRangeRef(PERSIST_PREFIX "AccumulativeChecksum/"_sr, PERSIST_PREFIX "AccumulativeChecksum0"_sr);

inline Key encodePersistAccumulativeChecksumKey(uint16_t acsIndex) {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(persistAccumulativeChecksumKeys.begin);
	wr << bigEndian16(acsIndex);
	return wr.toValue();
}

inline uint16_t decodePersistAccumulativeChecksumKey(const Key& key) {
	uint16_t acsIndex;
	BinaryReader rd(key.removePrefix(persistAccumulativeChecksumKeys.begin), Unversioned());
	rd >> acsIndex;
	return bigEndian16(acsIndex);
}

// MoveInUpdates caches new updates of a move-in shard, before that shard is ready to accept writes.
struct MoveInUpdates {
	MoveInUpdates() : spilled(MoveInUpdatesSpilled::False) {}
	MoveInUpdates(UID id,
	              Version version,
	              struct StorageServer* data,
	              IKeyValueStore* store,
	              MoveInUpdatesSpilled spilled);

	void addMutation(Version version,
	                 bool fromFetch,
	                 MutationRef const& mutation,
	                 MutationRefAndCipherKeys const& encryptedMutation,
	                 bool allowSpill);

	bool hasNext() const;

	std::vector<Standalone<VerUpdateRef>> next(const int byteLimit);
	const std::deque<Standalone<VerUpdateRef>>& getUpdatesQueue() const { return this->updates; }

	UID id;
	Version lastRepliedVersion;
	std::deque<Standalone<VerUpdateRef>> updates;
	std::vector<Standalone<VerUpdateRef>> spillBuffer;
	struct StorageServer* data;
	IKeyValueStore* store;
	KeyRange range;
	bool fail;
	MoveInUpdatesSpilled spilled;
	size_t size;
	Future<Void> loadFuture;
	Severity logSev;

private:
	ACTOR static Future<Void> loadUpdates(MoveInUpdates* self, Version begin, Version end);

	Key getPersistKey(const Version version, const int idx) const;
};

ACTOR Future<Void> MoveInUpdates::loadUpdates(MoveInUpdates* self, Version begin, Version end) {
	ASSERT(self->spilled);
	if (begin >= end) {
		self->spilled = MoveInUpdatesSpilled::False;
		return Void();
	}

	const Key beginKey = persistUpdatesKey(self->id, begin), endKey = persistUpdatesKey(self->id, end);
	TraceEvent(self->logSev, "MoveInUpdatesLoadBegin", self->id)
	    .detail("BeginVersion", begin)
	    .detail("EndVersion", end)
	    .detail("BeginKey", beginKey)
	    .detail("EndKey", endKey);
	ASSERT(beginKey < endKey);
	RangeResult res = wait(self->store->readRange(KeyRangeRef(beginKey, endKey),
	                                              SERVER_KNOBS->FETCH_SHARD_UPDATES_BYTE_LIMIT,
	                                              SERVER_KNOBS->FETCH_SHARD_UPDATES_BYTE_LIMIT));
	std::vector<Standalone<VerUpdateRef>> restored;
	for (int i = 0; i < res.size(); ++i) {
		const Version version = decodePersistUpdateVersion(res[i].key.removePrefix(self->range.begin));
		Standalone<VerUpdateRef> vur =
		    BinaryReader::fromStringRef<Standalone<VerUpdateRef>>(res[i].value, IncludeVersion());
		ASSERT(version == vur.version);
		TraceEvent(self->logSev, "MoveInUpdatesLoadedMutations", self->id)
		    .detail("Version", version)
		    .detail("Mutations", vur.mutations.size());
		restored.push_back(std::move(vur));
	}

	if (!res.more) {
		for (int i = restored.size() - 1; i >= 0; --i) {
			if (self->updates.empty() || restored[i].version < self->updates.front().version) {
				self->updates.push_front(std::move(restored[i]));
			}
		}
		self->spilled = MoveInUpdatesSpilled::False;
	} else {
		ASSERT(self->spillBuffer.empty());
		std::swap(self->spillBuffer, restored);
	}

	self->loadFuture = Future<Void>();
	TraceEvent(self->logSev, "MoveInUpdatesLoadEnd", self->id)
	    .detail("MinVersion", restored.empty() ? invalidVersion : restored.front().version)
	    .detail("MaxVersion", restored.empty() ? invalidVersion : restored.back().version)
	    .detail("VersionCount", restored.size())
	    .detail("LastBatch", !res.more);

	return Void();
}

bool MoveInUpdates::hasNext() const {
	return this->spilled || (!this->updates.empty() && this->updates.back().version > this->lastRepliedVersion);
}

// MoveInShard corresponds to a move-in physical shard, a class representation of MoveInShardMetaData.
struct MoveInShard {
	std::shared_ptr<MoveInShardMetaData> meta;
	struct StorageServer* server;
	std::shared_ptr<MoveInUpdates> updates;
	bool isRestored;
	Version transferredVersion;
	ConductBulkLoad conductBulkLoad = ConductBulkLoad::False;

	Future<Void> fetchClient; // holds FetchShard() actor
	Promise<Void> fetchComplete;
	Promise<Void> readWrite;

	Severity logSev = static_cast<Severity>(SERVER_KNOBS->PHYSICAL_SHARD_MOVE_LOG_SEVERITY);

	MoveInShard() = default;
	MoveInShard(StorageServer* server,
	            const UID& id,
	            const UID& dataMoveId,
	            const Version version,
	            const ConductBulkLoad conductBulkLoad,
	            MoveInPhase phase);
	MoveInShard(StorageServer* server,
	            const UID& id,
	            const UID& dataMoveId,
	            const Version version,
	            const ConductBulkLoad conductBulkLoad);
	MoveInShard(StorageServer* server, MoveInShardMetaData meta);
	~MoveInShard();

	UID id() const { return this->meta->id; }
	UID dataMoveId() const { return this->meta->dataMoveId; }
	void setPhase(const MoveInPhase& phase) { this->meta->setPhase(phase); }
	MoveInPhase getPhase() const { return this->meta->getPhase(); }
	const std::vector<KeyRange>& ranges() const { return this->meta->ranges; }
	const std::vector<CheckpointMetaData>& checkpoints() const { return this->meta->checkpoints; }
	std::string destShardIdString() const { return this->meta->destShardIdString(); }
	void addRange(const KeyRangeRef range);
	void removeRange(const KeyRangeRef range);
	void cancel(const MoveInFailed failed = MoveInFailed::False);
	bool isDataTransferred() const { return meta->getPhase() >= MoveInPhase::ApplyingUpdates; }
	bool isDataAndCFTransferred() const { throw not_implemented(); }
	bool failed() const { return this->getPhase() == MoveInPhase::Cancel || this->getPhase() == MoveInPhase::Error; }
	void setHighWatermark(const Version version) { this->meta->highWatermark = version; }
	Version getHighWatermark() const { return this->meta->highWatermark; }

	void addMutation(Version version,
	                 bool fromFetch,
	                 MutationRef const& mutation,
	                 MutationRefAndCipherKeys const& encryptedMutation);

	KeyRangeRef getAffectedRange(const MutationRef& mutation) const;

	std::string toString() const { return meta != nullptr ? meta->toString() : "Empty"; }
};

struct AddingShard : NonCopyable {
	KeyRange keys;
	Future<Void> fetchClient; // holds FetchKeys() actor
	Promise<Void> fetchComplete;
	Promise<Void> readWrite;
	DataMovementReason reason;
	SSBulkLoadMetadata ssBulkLoadMetadata;

	// During the Fetching phase, it saves newer mutations whose version is greater or equal to fetchClient's
	// fetchVersion, while the shard is still busy catching up with fetchClient. It applies these updates after fetching
	// completes.
	std::deque<Standalone<VerUpdateRef>> updates;

	struct StorageServer* server;
	Version transferredVersion;
	Version fetchVersion;

	// To learn more details of the phase transitions, see function fetchKeys(). The phases below are sorted in
	// chronological order and do not go back.
	enum Phase {
		WaitPrevious,
		// During Fetching phase, it fetches data before fetchVersion and write it to storage, then let updater know it
		// is ready to update the deferred updates` (see the comment of member variable `updates` above).
		Fetching,
		// During the FetchingCF phase, the shard data is transferred but the remaining change feed data is still being
		// transferred. This is equivalent to the waiting phase for non-changefeed data.
		FetchingCF,
		// During Waiting phase, it sends updater the deferred updates, and wait until they are durable.
		Waiting
		// The shard's state is changed from adding to readWrite then.
	};

	Phase phase;

	AddingShard(StorageServer* server,
	            KeyRangeRef const& keys,
	            DataMovementReason reason,
	            const SSBulkLoadMetadata& ssBulkLoadMetadata);

	// When fetchKeys "partially completes" (splits an adding shard in two), this is used to construct the left half
	AddingShard(AddingShard* prev, KeyRange const& keys)
	  : keys(keys), fetchClient(prev->fetchClient), server(prev->server), transferredVersion(prev->transferredVersion),
	    fetchVersion(prev->fetchVersion), phase(prev->phase), reason(prev->reason),
	    ssBulkLoadMetadata(prev->ssBulkLoadMetadata) {}
	~AddingShard() {
		if (!fetchComplete.isSet())
			fetchComplete.send(Void());
		if (!readWrite.isSet())
			readWrite.send(Void());
	}

	void addMutation(Version version,
	                 bool fromFetch,
	                 MutationRef const& mutation,
	                 MutationRefAndCipherKeys const& encryptedMutation);

	bool isDataTransferred() const { return phase >= FetchingCF; }
	bool isDataAndCFTransferred() const { return phase >= Waiting; }

	SSBulkLoadMetadata getSSBulkLoadMetadata() const { return ssBulkLoadMetadata; }
};

class ShardInfo : public ReferenceCounted<ShardInfo>, NonCopyable {
	ShardInfo(KeyRange keys, std::unique_ptr<AddingShard>&& adding, StorageServer* readWrite)
	  : adding(std::move(adding)), readWrite(readWrite), keys(keys), shardId(0LL), desiredShardId(0LL), version(0) {}
	ShardInfo(KeyRange keys, std::shared_ptr<MoveInShard> moveInShard)
	  : adding(nullptr), readWrite(nullptr), moveInShard(moveInShard), keys(keys),
	    shardId(moveInShard->meta->destShardId()), desiredShardId(moveInShard->meta->destShardId()),
	    version(moveInShard->meta->createVersion) {}

public:
	// A shard has 4 mutual exclusive states: adding, moveInShard, readWrite and notAssigned.
	std::unique_ptr<AddingShard> adding;
	struct StorageServer* readWrite;
	std::shared_ptr<MoveInShard> moveInShard; // The shard is being moved in via physical-shard-move.
	KeyRange keys;
	uint64_t changeCounter;
	uint64_t shardId;
	uint64_t desiredShardId;
	Version version;

	static ShardInfo* newNotAssigned(KeyRange keys) { return new ShardInfo(keys, nullptr, nullptr); }
	static ShardInfo* newReadWrite(KeyRange keys, StorageServer* data) { return new ShardInfo(keys, nullptr, data); }
	static ShardInfo* newAdding(StorageServer* data,
	                            KeyRange keys,
	                            DataMovementReason reason,
	                            const SSBulkLoadMetadata& ssBulkLoadMetadata) {
		return new ShardInfo(keys, std::make_unique<AddingShard>(data, keys, reason, ssBulkLoadMetadata), nullptr);
	}
	static ShardInfo* addingSplitLeft(KeyRange keys, AddingShard* oldShard) {
		return new ShardInfo(keys, std::make_unique<AddingShard>(oldShard, keys), nullptr);
	}

	static ShardInfo* newShard(StorageServer* data, const StorageServerShard& shard);

	static bool canMerge(const ShardInfo* l, const ShardInfo* r) {
		if (l == nullptr || r == nullptr || l->keys.end != r->keys.begin || l->version == invalidVersion ||
		    r->version == invalidVersion) {
			return false;
		}
		if (l->shardId != r->shardId || l->desiredShardId != r->desiredShardId) {
			return false;
		}
		return (l->isReadable() && r->isReadable()) || (!l->assigned() && !r->assigned());
	}

	StorageServerShard toStorageServerShard() const {
		StorageServerShard::ShardState st = StorageServerShard::NotAssigned;
		Optional<UID> moveInShardId;
		if (this->isReadable()) {
			st = StorageServerShard::ReadWrite;
		} else if (!this->assigned()) {
			st = StorageServerShard::NotAssigned;
		} else if (this->adding) {
			st = this->adding->phase == AddingShard::Waiting ? StorageServerShard::ReadWritePending
			                                                 : StorageServerShard::Adding;
		} else {
			ASSERT(this->moveInShard);
			const MoveInPhase phase = this->moveInShard->getPhase();
			if (phase < MoveInPhase::ReadWritePending) {
				st = StorageServerShard::MovingIn;
			} else if (phase == MoveInPhase::ReadWritePending) {
				st = StorageServerShard::ReadWritePending;
			} else if (phase == MoveInPhase::Complete) {
				st = StorageServerShard::ReadWrite;
			} else {
				st = StorageServerShard::MovingIn;
			}
			// Clear moveInShardId if the data move is complete.
			if (phase != MoveInPhase::ReadWritePending && phase != MoveInPhase::Complete) {
				moveInShardId = this->moveInShard->id();
			}
		}
		return StorageServerShard(this->keys, this->version, this->shardId, this->desiredShardId, st, moveInShardId);
	}

	// Copies necessary information from `shard`.
	void populateShard(const StorageServerShard& shard) {
		this->version = shard.version;
		this->shardId = shard.id;
		this->desiredShardId = shard.desiredId;
	}

	// Returns true if the current shard is merged with `other`.
	bool mergeWith(const ShardInfo* other) {
		if (!canMerge(this, other)) {
			return false;
		}
		this->keys = KeyRangeRef(this->keys.begin, other->keys.end);
		this->version = std::max(this->version, other->version);
		return true;
	}

	void validate() const {
		// TODO: Complete this.
	}

	SSBulkLoadMetadata getSSBulkLoadMetadata() const {
		if (adding) {
			return adding->getSSBulkLoadMetadata();
		} else {
			return SSBulkLoadMetadata();
		}
	}

	bool isReadable() const { return readWrite != nullptr; }
	bool notAssigned() const { return !readWrite && !adding && !moveInShard; }
	bool assigned() const { return readWrite || adding || moveInShard; }
	bool isInVersionedData() const {
		return readWrite || (adding && adding->isDataTransferred()) ||
		       (moveInShard && moveInShard->isDataTransferred());
	}
	bool isCFInVersionedData() const { return readWrite || (adding && adding->isDataAndCFTransferred()); }
	bool isReadWritePending() const {
		return isCFInVersionedData() || (moveInShard && (moveInShard->getPhase() == MoveInPhase::ReadWritePending ||
		                                                 moveInShard->getPhase() == MoveInPhase::Complete));
	}
	void addMutation(Version version,
	                 bool fromFetch,
	                 MutationRef const& mutation,
	                 MutationRefAndCipherKeys const& encryptedMutation);
	bool isFetched() const {
		return readWrite || (adding && adding->fetchComplete.isSet()) ||
		       (moveInShard && moveInShard->fetchComplete.isSet());
	}

	std::string debugDescribeState() const {
		if (notAssigned()) {
			return "NotAssigned";
		} else if (adding && !adding->isDataAndCFTransferred()) {
			return "AddingFetchingCF";
		} else if (adding && !adding->isDataTransferred()) {
			return "AddingFetching";
		} else if (adding) {
			return "AddingTransferred";
		} else if (moveInShard) {
			return moveInShard->meta->toString();
		} else {
			return "ReadWrite";
		}
	}
};

struct StorageServerDisk {
	explicit StorageServerDisk(struct StorageServer* data, IKeyValueStore* storage) : data(data), storage(storage) {}

	IKeyValueStore* getKeyValueStore() const { return this->storage; }

	void makeNewStorageServerDurable(const bool shardAware);
	bool makeVersionMutationsDurable(Version& prevStorageVersion,
	                                 Version newStorageVersion,
	                                 int64_t& bytesLeft,
	                                 UnlimitedCommitBytes unlimitedCommitBytes);
	void makeVersionDurable(Version version);
	void makeAccumulativeChecksumDurable(const AccumulativeChecksumState& acsState);
	void clearAccumulativeChecksumState(const AccumulativeChecksumState& acsState);
	void makeTssQuarantineDurable();
	Future<bool> restoreDurableState();

	void changeLogProtocol(Version version, ProtocolVersion protocol);

	void writeMutation(MutationRef mutation);
	void writeKeyValue(KeyValueRef kv);
	void clearRange(KeyRangeRef keys);

	Future<Void> addRange(KeyRangeRef range, std::string id) {
		return storage->addRange(range, id, !SERVER_KNOBS->SHARDED_ROCKSDB_DELAY_COMPACTION_FOR_DATA_MOVE);
	}

	std::vector<std::string> removeRange(KeyRangeRef range) { return storage->removeRange(range); }

	void markRangeAsActive(KeyRangeRef range) { storage->markRangeAsActive(range); }

	Future<Void> replaceRange(KeyRange range, Standalone<VectorRef<KeyValueRef>> data) {
		return storage->replaceRange(range, data);
	}

	void persistRangeMapping(KeyRangeRef range, bool isAdd) { storage->persistRangeMapping(range, isAdd); }

	CoalescedKeyRangeMap<std::string> getExistingRanges() { return storage->getExistingRanges(); }

	Future<Void> getError() { return storage->getError(); }
	Future<Void> init() { return storage->init(); }
	Future<Void> canCommit() { return storage->canCommit(); }
	Future<Void> commit() { return storage->commit(); }

	void logRecentRocksDBBackgroundWorkStats(UID ssId, std::string logReason) {
		return storage->logRecentRocksDBBackgroundWorkStats(ssId, logReason);
	}

	// SOMEDAY: Put readNextKeyInclusive in IKeyValueStore
	// Read the key that is equal or greater then 'key' from the storage engine.
	// For example, readNextKeyInclusive("a") should return:
	//  - "a", if key "a" exist
	//  - "b", if key "a" doesn't exist, and "b" is the next existing key in total order
	//  - allKeys.end, if keyrange [a, allKeys.end) is empty
	Future<Key> readNextKeyInclusive(KeyRef key, Optional<ReadOptions> options = Optional<ReadOptions>()) {
		++(*kvScans);
		return readFirstKey(storage, KeyRangeRef(key, allKeys.end), options);
	}
	Future<Optional<Value>> readValue(KeyRef key, Optional<ReadOptions> options = Optional<ReadOptions>()) {
		++(*kvGets);
		return storage->readValue(key, options);
	}
	Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                        int maxLength,
	                                        Optional<ReadOptions> options = Optional<ReadOptions>()) {
		++(*kvGets);
		return storage->readValuePrefix(key, maxLength, options);
	}
	Future<RangeResult> readRange(KeyRangeRef keys,
	                              int rowLimit = 1 << 30,
	                              int byteLimit = 1 << 30,
	                              Optional<ReadOptions> options = Optional<ReadOptions>()) {
		++(*kvScans);
		return storage->readRange(keys, rowLimit, byteLimit, options);
	}

	Future<CheckpointMetaData> checkpoint(const CheckpointRequest& request) { return storage->checkpoint(request); }

	Future<Void> restore(const std::vector<CheckpointMetaData>& checkpoints) { return storage->restore(checkpoints); }

	Future<Void> restore(const std::string& shardId,
	                     const std::vector<KeyRange>& ranges,
	                     const std::vector<CheckpointMetaData>& checkpoints) {
		return storage->restore(shardId, ranges, checkpoints);
	}

	Future<Void> deleteCheckpoint(const CheckpointMetaData& checkpoint) {
		return storage->deleteCheckpoint(checkpoint);
	}

	KeyValueStoreType getKeyValueStoreType() const { return storage->getType(); }
	StorageBytes getStorageBytes() const { return storage->getStorageBytes(); }
	std::tuple<size_t, size_t, size_t> getSize() const { return storage->getSize(); }

	Future<EncryptionAtRestMode> encryptionMode() { return storage->encryptionMode(); }

	// The following are pointers to the Counters in StorageServer::counters of the same names.
	Counter* kvCommitLogicalBytes;
	Counter* kvClearRanges;
	Counter* kvClearSingleKey;
	Counter* kvGets;
	Counter* kvScans;
	Counter* kvCommits;

private:
	struct StorageServer* data;
	IKeyValueStore* storage;
	void writeMutations(const VectorRef<MutationRef>& mutations, Version debugVersion, const char* debugContext);
	void writeMutationsBuggy(const VectorRef<MutationRef>& mutations, Version debugVersion, const char* debugContext);

	ACTOR static Future<Key> readFirstKey(IKeyValueStore* storage, KeyRangeRef range, Optional<ReadOptions> options) {
		RangeResult r = wait(storage->readRange(range, 1, 1 << 30, options));
		if (r.size())
			return r[0].key;
		else
			return range.end;
	}
};

struct UpdateEagerReadInfo {
	std::vector<KeyRef> keyBegin;
	std::vector<Key> keyEnd; // these are for ClearRange

	std::vector<std::pair<KeyRef, int>> keys;
	std::vector<Optional<Value>> value;

	Arena arena;
	bool enableClearRangeEagerReads;

	UpdateEagerReadInfo(bool enableClearRangeEagerReads) : enableClearRangeEagerReads(enableClearRangeEagerReads) {}

	void addMutations(VectorRef<MutationRef> const& mutations) {
		for (auto& m : mutations)
			addMutation(m);
	}

	void addMutation(MutationRef const& m) {
		// SOMEDAY: Theoretically we can avoid a read if there is an earlier overlapping ClearRange
		if (m.type == MutationRef::ClearRange && !m.param2.startsWith(systemKeys.end) && enableClearRangeEagerReads)
			keyBegin.push_back(m.param2);
		else if (m.type == MutationRef::CompareAndClear) {
			if (enableClearRangeEagerReads)
				keyBegin.push_back(keyAfter(m.param1, arena));
			if (keys.size() > 0 && keys.back().first == m.param1) {
				// Don't issue a second read, if the last read was equal to the current key.
				// CompareAndClear is likely to be used after another atomic operation on same key.
				keys.back().second = std::max(keys.back().second, m.param2.size() + 1);
			} else {
				keys.emplace_back(m.param1, m.param2.size() + 1);
			}
		} else if ((m.type == MutationRef::AppendIfFits) || (m.type == MutationRef::ByteMin) ||
		           (m.type == MutationRef::ByteMax))
			keys.emplace_back(m.param1, CLIENT_KNOBS->VALUE_SIZE_LIMIT);
		else if (isAtomicOp((MutationRef::Type)m.type))
			keys.emplace_back(m.param1, m.param2.size());
	}

	void finishKeyBegin() {
		if (enableClearRangeEagerReads) {
			std::sort(keyBegin.begin(), keyBegin.end());
			keyBegin.resize(std::unique(keyBegin.begin(), keyBegin.end()) - keyBegin.begin());
		}
		std::sort(keys.begin(), keys.end(), [](const std::pair<KeyRef, int>& lhs, const std::pair<KeyRef, int>& rhs) {
			return (lhs.first < rhs.first) || (lhs.first == rhs.first && lhs.second > rhs.second);
		});
		keys.resize(std::unique(keys.begin(),
		                        keys.end(),
		                        [](const std::pair<KeyRef, int>& lhs, const std::pair<KeyRef, int>& rhs) {
			                        return lhs.first == rhs.first;
		                        }) -
		            keys.begin());
		// value gets populated in doEagerReads
	}

	Optional<Value>& getValue(KeyRef key) {
		int i = std::lower_bound(keys.begin(),
		                         keys.end(),
		                         std::pair<KeyRef, int>(key, 0),
		                         [](const std::pair<KeyRef, int>& lhs, const std::pair<KeyRef, int>& rhs) {
			                         return lhs.first < rhs.first;
		                         }) -
		        keys.begin();
		ASSERT(i < keys.size() && keys[i].first == key);
		return value[i];
	}

	KeyRef getKeyEnd(KeyRef key) {
		int i = std::lower_bound(keyBegin.begin(), keyBegin.end(), key) - keyBegin.begin();
		ASSERT(i < keyBegin.size() && keyBegin[i] == key);
		return keyEnd[i];
	}
};

const int VERSION_OVERHEAD =
    64 + sizeof(Version) + sizeof(Standalone<VerUpdateRef>) + // mutationLog, 64b overhead for map
    2 * (64 + sizeof(Version) +
         sizeof(Reference<VersionedMap<KeyRef, ValueOrClearToRef>::PTreeT>)); // versioned map [ x2 for
                                                                              // createNewVersion(version+1) ], 64b
                                                                              // overhead for map

static int mvccStorageBytes(MutationRef const& m) {
	return mvccStorageBytes(m.param1.size() + m.param2.size());
}

struct FetchInjectionInfo {
	Arena arena;
	std::vector<VerUpdateRef> changes;
};

struct ChangeFeedInfo : ReferenceCounted<ChangeFeedInfo> {
	std::deque<Standalone<EncryptedMutationsAndVersionRef>> mutations;
	Version fetchVersion = invalidVersion; // The version that commits from a fetch have been written to storage, but
	                                       // have not yet been committed as part of updateStorage.
	Version storageVersion = invalidVersion; // The version between the storage version and the durable version are
	                                         // being written to disk as part of the current commit in updateStorage.
	Version durableVersion = invalidVersion; // All versions before the durable version are durable on disk
	Version metadataVersion = invalidVersion; // Last update to the change feed metadata. Used for reasoning about
	                                          // fetched metadata vs local metadata
	Version emptyVersion = 0; // The change feed does not have any mutations before emptyVersion
	KeyRange range;
	Key id;
	AsyncTrigger newMutations;
	NotifiedVersion durableFetchVersion;
	// A stopped change feed no longer adds new mutations, but is still queryable.
	// stopVersion = MAX_VERSION means the feed has not been stopped
	Version stopVersion = MAX_VERSION;

	// We need to track the version the change feed metadata was created by private mutation, so that if it is rolled
	// back, we can avoid notifying other SS of change feeds that don't durably exist
	Version metadataCreateVersion = invalidVersion;

	FlowLock fetchLock = FlowLock(1);

	bool removing = false;
	bool destroyed = false;

	KeyRangeMap<std::unordered_map<UID, Promise<Void>>> moveTriggers;

	void triggerOnMove(KeyRange range, UID streamUID, Promise<Void> p) {
		auto toInsert = moveTriggers.modify(range);
		for (auto triggerRange = toInsert.begin(); triggerRange != toInsert.end(); ++triggerRange) {
			triggerRange->value().insert({ streamUID, p });
		}
	}

	void moved(KeyRange range) {
		auto toTrigger = moveTriggers.intersectingRanges(range);
		for (auto& triggerRange : toTrigger) {
			for (auto& triggerStream : triggerRange.cvalue()) {
				if (triggerStream.second.canBeSet()) {
					triggerStream.second.send(Void());
				}
			}
		}
		// coalesce doesn't work with promises
		moveTriggers.insert(range, std::unordered_map<UID, Promise<Void>>());
	}

	void removeOnMoveTrigger(KeyRange range, UID streamUID) {
		auto toRemove = moveTriggers.modify(range);
		for (auto triggerRange = toRemove.begin(); triggerRange != toRemove.end(); ++triggerRange) {
			auto streamToRemove = triggerRange->value().find(streamUID);
			if (streamToRemove == triggerRange->cvalue().end()) {
				ASSERT(destroyed);
			} else {
				triggerRange->value().erase(streamToRemove);
			}
		}
		// TODO: may be more cleanup possible here
	}

	void destroy(Version destroyVersion) {
		updateMetadataVersion(destroyVersion);
		removing = true;
		destroyed = true;
		moved(range);
		newMutations.trigger();
	}

	bool updateMetadataVersion(Version version) {
		// don't update metadata version if removing, so that metadata version remains the moved away version
		if (!removing && version > metadataVersion) {
			metadataVersion = version;
			return true;
		}
		return false;
	}
};

class ServerWatchMetadata : public ReferenceCounted<ServerWatchMetadata> {
public:
	Key key;
	Optional<Value> value;
	Version version;
	Future<Version> watch_impl;
	Promise<Version> versionPromise;
	Optional<TagSet> tags;
	Optional<UID> debugID;
	int64_t tenantId;

	ServerWatchMetadata(Key key,
	                    Optional<Value> value,
	                    Version version,
	                    Optional<TagSet> tags,
	                    Optional<UID> debugID,
	                    int64_t tenantId)
	  : key(key), value(value), version(version), tags(tags), debugID(debugID), tenantId(tenantId) {}
};

struct BusiestWriteTagContext {
	const std::string busiestWriteTagTrackingKey;
	UID ratekeeperID;
	Reference<EventCacheHolder> busiestWriteTagEventHolder;
	double lastUpdateTime;

	BusiestWriteTagContext(const UID& thisServerID)
	  : busiestWriteTagTrackingKey(thisServerID.toString() + "/BusiestWriteTag"), ratekeeperID(UID()),
	    busiestWriteTagEventHolder(makeReference<EventCacheHolder>(busiestWriteTagTrackingKey)), lastUpdateTime(-1) {}
};

// A SSPhysicalShard represents a physical shard, it contains a list of keyranges.
class SSPhysicalShard {
public:
	SSPhysicalShard(const int64_t id) : id(id) {}

	void addRange(Reference<ShardInfo> shard);

	// Remove the shard if a shard to the same pointer (ShardInfo*) exists.
	void removeRange(Reference<ShardInfo> shard);

	// Clear all shards overlapping with `range`.
	void removeRange(KeyRangeRef range);

	bool supportCheckpoint() const;

	bool hasRange(Reference<ShardInfo> shard) const;

	int size() const { return ranges.size(); }
	// Public function to iterate over the ranges
	std::vector<Reference<ShardInfo>>::const_iterator begin() const { return ranges.begin(); }

	std::vector<Reference<ShardInfo>>::const_iterator end() const { return ranges.end(); }

private:
	const int64_t id;
	std::vector<Reference<ShardInfo>> ranges;
};

void SSPhysicalShard::addRange(Reference<ShardInfo> shard) {
	TraceEvent(SevVerbose, "SSPhysicalShardAddShard")
	    .detail("ShardID", format("%016llx", this->id))
	    .detail("Assigned", !shard->notAssigned())
	    .detail("Range", shard->keys);
	ASSERT(!shard->notAssigned());

	removeRange(shard->keys);

	ranges.push_back(shard);
}

void SSPhysicalShard::removeRange(Reference<ShardInfo> shard) {
	TraceEvent(SevVerbose, "SSPhysicalShardRemoveShard")
	    .detail("ShardID", format("%016llx", this->id))
	    .detail("Assigned", !shard->notAssigned())
	    .detail("Range", shard->keys);

	for (int i = 0; i < this->ranges.size(); ++i) {
		const auto& r = this->ranges[i];
		if (r.getPtr() == shard.getPtr()) {
			this->ranges[i] = this->ranges.back();
			this->ranges.pop_back();
			return;
		}
	}
}

void SSPhysicalShard::removeRange(KeyRangeRef range) {
	TraceEvent(SevVerbose, "SSPhysicalShardRemoveRange")
	    .detail("ShardID", format("%016llx", this->id))
	    .detail("Range", range);
	for (int i = 0; i < this->ranges.size();) {
		const auto& r = this->ranges[i];
		if (r->keys.intersects(range)) {
			this->ranges[i] = this->ranges.back();
			this->ranges.pop_back();
		} else {
			++i;
		}
	}
}

bool SSPhysicalShard::supportCheckpoint() const {
	for (const auto& r : this->ranges) {
		ASSERT(r->desiredShardId == this->id);
		if (r->shardId != this->id) {
			return false;
		}
	}
	return true;
}

bool SSPhysicalShard::hasRange(Reference<ShardInfo> shard) const {
	for (int i = 0; i < this->ranges.size(); ++i) {
		if (this->ranges[i].getPtr() == shard.getPtr()) {
			return true;
		}
	}

	return false;
}

struct TenantSSInfo {
	constexpr static FileIdentifier file_identifier = 3253114;
	TenantAPI::TenantLockState lockState;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, lockState);
	}
};

struct StorageServer : public IStorageMetricsService {
	typedef VersionedMap<KeyRef, ValueOrClearToRef> VersionedData;

private:
	// versionedData contains sets and clears.

	// * Nonoverlapping: No clear overlaps a set or another clear, or adjoins another clear.
	// ~ Clears are maximal: If versionedData.at(v) contains a clear [b,e) then
	//      there is a key data[e]@v, or e==allKeys.end, or a shard boundary or former boundary at e

	// * Reads are possible: When k is in a readable shard, for any v in [storageVersion, version.get()],
	//      storage[k] + versionedData.at(v)[k] = database[k] @ v    (storage[k] might be @ any version in
	//      [durableVersion, storageVersion])

	// * Transferred shards are partially readable: When k is in an adding, transferred shard, for any v in
	// [transferredVersion, version.get()],
	//      storage[k] + versionedData.at(v)[k] = database[k] @ v

	// * versionedData contains versions [storageVersion(), version.get()].  It might also contain version
	// (version.get()+1), in which changeDurableVersion may be deleting ghosts, and/or it might
	//      contain later versions if applyUpdate is on the stack.

	// * Old shards are erased: versionedData.atLatest() has entries (sets or intersecting clears) only for keys in
	// readable or adding,transferred shards.
	//   Earlier versions may have extra entries for shards that *were* readable or adding,transferred when those
	//   versions were the latest, but they eventually are forgotten.

	// * Old mutations are erased: All items in versionedData.atLatest() have insertVersion() > durableVersion(), but
	// views
	//   at older versions may contain older items which are also in storage (this is OK because of idempotency)

	VersionedData versionedData;
	std::map<Version, Standalone<VerUpdateRef>> mutationLog; // versions (durableVersion, version]

	using WatchMapKey = std::pair<int64_t, Key>;
	using WatchMapKeyHasher = boost::hash<WatchMapKey>;
	using WatchMapValue = Reference<ServerWatchMetadata>;
	using WatchMap_t = std::unordered_map<WatchMapKey, WatchMapValue, WatchMapKeyHasher>;
	WatchMap_t watchMap; // keep track of server watches

public:
	struct PendingNewShard {
		PendingNewShard(uint64_t shardId, KeyRangeRef range) : shardId(format("%016llx", shardId)), range(range) {}

		std::string toString() const {
			return fmt::format("PendingNewShard: [ShardID]: {} [Range]: {}",
			                   this->shardId,
			                   Traceable<KeyRangeRef>::toString(this->range));
		}

		std::string shardId;
		KeyRange range;
	};

	std::map<Version, std::vector<CheckpointMetaData>> pendingCheckpoints; // Pending checkpoint requests
	std::unordered_map<UID, CheckpointMetaData> checkpoints; // Existing and deleting checkpoints
	std::unordered_map<UID, ICheckpointReader*> liveCheckpointReaders; // Active checkpoint readers
	VersionedMap<int64_t, TenantSSInfo> tenantMap;
	std::map<Version, std::vector<PendingNewShard>>
	    pendingAddRanges; // Pending requests to add ranges to physical shards
	std::map<Version, std::vector<KeyRange>>
	    pendingRemoveRanges; // Pending requests to remove ranges from physical shards
	std::deque<std::pair<Standalone<StringRef>, Standalone<StringRef>>> constructedData;

	bool shardAware; // True if the storage server is aware of the physical shards.

	// Histograms
	struct FetchKeysHistograms {
		const Reference<Histogram> latency;
		const Reference<Histogram> bytes;
		const Reference<Histogram> bandwidth;
		const Reference<Histogram> bytesPerCommit;

		FetchKeysHistograms()
		  : latency(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
		                                    FETCH_KEYS_LATENCY_HISTOGRAM,
		                                    Histogram::Unit::milliseconds)),
		    bytes(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
		                                  FETCH_KEYS_BYTES_HISTOGRAM,
		                                  Histogram::Unit::bytes)),
		    bandwidth(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
		                                      FETCH_KEYS_BYTES_PER_SECOND_HISTOGRAM,
		                                      Histogram::Unit::bytes_per_second)),
		    bytesPerCommit(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
		                                           FETCH_KEYS_BYTES_PER_COMMIT_HISTOGRAM,
		                                           Histogram::Unit::bytes)) {}
	} fetchKeysHistograms;

	Reference<Histogram> tlogCursorReadsLatencyHistogram;
	Reference<Histogram> ssVersionLockLatencyHistogram;
	Reference<Histogram> eagerReadsLatencyHistogram;
	Reference<Histogram> fetchKeysPTreeUpdatesLatencyHistogram;
	Reference<Histogram> tLogMsgsPTreeUpdatesLatencyHistogram;
	Reference<Histogram> storageUpdatesDurableLatencyHistogram;
	Reference<Histogram> storageCommitLatencyHistogram;
	Reference<Histogram> ssDurableVersionUpdateLatencyHistogram;
	// Histograms of requests sent to KVS.
	Reference<Histogram> readRangeBytesReturnedHistogram;
	Reference<Histogram> readRangeBytesLimitHistogram;
	Reference<Histogram> readRangeKVPairsReturnedHistogram;

	// watch map operations
	Reference<ServerWatchMetadata> getWatchMetadata(KeyRef key, int64_t tenantId) const;
	KeyRef setWatchMetadata(Reference<ServerWatchMetadata> metadata);
	void deleteWatchMetadata(KeyRef key, int64_t tenantId);
	void clearWatchMetadata();

	// tenant map operations
	void insertTenant(TenantMapEntry const& tenant, Version version, bool persist);
	void clearTenants(StringRef startTenant, StringRef endTenant, Version version);

	void checkTenantEntry(Version version, TenantInfo tenant, bool lockAware);

	std::vector<StorageServerShard> getStorageServerShards(KeyRangeRef range);
	std::shared_ptr<MoveInShard> getMoveInShard(const UID& dataMoveId,
	                                            const Version version,
	                                            const ConductBulkLoad conductBulkLoad);

	class CurrentRunningFetchKeys {
		std::unordered_map<UID, double> startTimeMap;
		std::unordered_map<UID, KeyRange> keyRangeMap;

		static const StringRef emptyString;
		static const KeyRangeRef emptyKeyRange;

	public:
		void recordStart(const UID id, const KeyRange& keyRange) {
			startTimeMap[id] = now();
			keyRangeMap[id] = keyRange;
		}

		void recordFinish(const UID id) {
			startTimeMap.erase(id);
			keyRangeMap.erase(id);
		}

		std::pair<double, KeyRange> longestTime() const {
			if (numRunning() == 0) {
				return { -1, emptyKeyRange };
			}

			const double currentTime = now();
			double longest = 0;
			UID UIDofLongest;
			for (const auto& kv : startTimeMap) {
				const double currentRunningTime = currentTime - kv.second;
				if (longest <= currentRunningTime) {
					longest = currentRunningTime;
					UIDofLongest = kv.first;
				}
			}
			if (BUGGIFY) {
				UIDofLongest = deterministicRandom()->randomUniqueID();
			}
			auto it = keyRangeMap.find(UIDofLongest);
			if (it != keyRangeMap.end()) {
				return { longest, it->second };
			}
			return { -1, emptyKeyRange };
		}

		int numRunning() const { return startTimeMap.size(); }
	} currentRunningFetchKeys;

	Tag tag;
	std::vector<std::pair<Version, Tag>> history;
	std::vector<std::pair<Version, Tag>> allHistory;
	Version poppedAllAfter;
	std::map<Version, Arena>
	    freeable; // for each version, an Arena that must be held until that version is < oldestVersion
	Arena lastArena;
	double cpuUsage;
	double diskUsage;

	std::map<Version, Standalone<VerUpdateRef>> const& getMutationLog() const { return mutationLog; }
	std::map<Version, Standalone<VerUpdateRef>>& getMutableMutationLog() { return mutationLog; }
	VersionedData const& data() const { return versionedData; }
	VersionedData& mutableData() { return versionedData; }

	mutable double old_rate = 1.0;
	double currentRate() const {
		auto versionLag = version.get() - durableVersion.get();
		double res;
		if (versionLag >= SERVER_KNOBS->STORAGE_DURABILITY_LAG_HARD_MAX) {
			res = 0.0;
		} else if (versionLag > SERVER_KNOBS->STORAGE_DURABILITY_LAG_SOFT_MAX) {
			res =
			    1.0 -
			    (double(versionLag - SERVER_KNOBS->STORAGE_DURABILITY_LAG_SOFT_MAX) /
			     double(SERVER_KNOBS->STORAGE_DURABILITY_LAG_HARD_MAX - SERVER_KNOBS->STORAGE_DURABILITY_LAG_SOFT_MAX));
		} else {
			res = 1.0;
		}
		if (res != old_rate) {
			TraceEvent(SevDebug, "LocalRatekeeperChange", thisServerID)
			    .detail("Old", old_rate)
			    .detail("New", res)
			    .detail("NonDurableVersions", versionLag);
			old_rate = res;
		}
		return res;
	}

	void addMutationToMutationLogOrStorage(
	    Version ver,
	    MutationRef m); // Appends m to mutationLog@ver, or to storage if ver==invalidVersion

	// Update the byteSample, and write the updates to the mutation log@ver, or to storage if ver==invalidVersion
	void byteSampleApplyMutation(MutationRef const& m, Version ver);
	void byteSampleApplySet(KeyValueRef kv, Version ver);
	void byteSampleApplyClear(KeyRangeRef range, Version ver);

	void popVersion(Version v, bool popAllTags = false) {
		if (logSystem && !isTss()) {
			if (v > poppedAllAfter) {
				popAllTags = true;
				poppedAllAfter = std::numeric_limits<Version>::max();
			}

			std::vector<std::pair<Version, Tag>>* hist = &history;
			std::vector<std::pair<Version, Tag>> allHistoryCopy;
			if (popAllTags) {
				allHistoryCopy = allHistory;
				hist = &allHistoryCopy;
			}

			while (hist->size() && v > hist->back().first) {
				logSystem->pop(v, hist->back().second);
				hist->pop_back();
			}
			if (hist->size()) {
				logSystem->pop(v, hist->back().second);
			} else {
				logSystem->pop(v, tag);
			}
		}
	}

	Standalone<VerUpdateRef>& addVersionToMutationLog(Version v) {
		// return existing version...
		auto m = mutationLog.find(v);
		if (m != mutationLog.end())
			return m->second;

		// ...or create a new one
		auto& u = mutationLog[v];
		u.version = v;
		if (lastArena.getSize() >= 65536)
			lastArena = Arena(4096);
		u.arena() = lastArena;
		counters.bytesInput += VERSION_OVERHEAD;
		return u;
	}

	MutationRef addMutationToMutationLog(Standalone<VerUpdateRef>& mLV, MutationRef const& m) {
		byteSampleApplyMutation(m, mLV.version);
		counters.bytesInput += mvccStorageBytes(m);
		return mLV.push_back_deep(mLV.arena(), m);
	}

	void setTssPair(UID pairId) {
		tssPairID = Optional<UID>(pairId);

		// Set up tss fault injection here, only if we are in simulated mode and with fault injection.
		// With fault injection enabled, the tss will start acting normal for a bit, then after the specified delay
		// start behaving incorrectly.
		if (g_network->isSimulated() && !g_simulator->speedUpSimulation &&
		    g_simulator->tssMode >= ISimulator::TSSMode::EnabledAddDelay) {
			tssFaultInjectTime = now() + deterministicRandom()->randomInt(60, 300);
			TraceEvent(SevWarnAlways, "TSSInjectFaultEnabled", thisServerID)
			    .detail("Mode", g_simulator->tssMode)
			    .detail("At", tssFaultInjectTime.get());
		}
	}

	// If a TSS is "in quarantine", it means it has incorrect data. It is effectively in a "zombie" state where it
	// rejects all read requests and ignores all non-private mutations and data movements, but otherwise is still part
	// of the cluster. The purpose of this state is to "freeze" the TSS state after a mismatch so a human operator can
	// investigate, but preventing a new storage process from replacing the TSS on the worker. It will still get removed
	// from the cluster if it falls behind on the mutation stream, or if its tss pair gets removed and its tag is no
	// longer valid.
	bool isTSSInQuarantine() const { return tssPairID.present() && tssInQuarantine; }

	void startTssQuarantine() {
		if (!tssInQuarantine) {
			// persist quarantine so it's still quarantined if rebooted
			storage.makeTssQuarantineDurable();
		}
		tssInQuarantine = true;
	}

	StorageServerDisk storage;

	KeyRangeMap<Reference<ShardInfo>> shards;
	std::unordered_map<int64_t, SSPhysicalShard> physicalShards;
	uint64_t shardChangeCounter; // max( shards->changecounter )

	KeyRangeMap<bool> cachedRangeMap; // indicates if a key-range is being cached

	KeyRangeMap<std::vector<Reference<ChangeFeedInfo>>> keyChangeFeed;
	std::unordered_map<Key, Reference<ChangeFeedInfo>> uidChangeFeed;
	Deque<std::pair<std::vector<Key>, Version>> changeFeedVersions;
	std::map<UID, PromiseStream<Key>> changeFeedDestroys;
	std::set<Key> currentChangeFeeds;
	std::set<Key> fetchingChangeFeeds;
	std::unordered_map<NetworkAddress, std::unordered_map<UID, Version>> changeFeedClientVersions;
	std::unordered_map<Key, Version> changeFeedCleanupDurable;
	int64_t activeFeedQueries = 0;
	int64_t changeFeedMemoryBytes = 0;
	std::deque<std::pair<Version, int64_t>> feedMemoryBytesByVersion;

	// newestAvailableVersion[k]
	//   == invalidVersion -> k is unavailable at all versions
	//   <= storageVersion -> k is unavailable at all versions (but might be read anyway from storage if we are in the
	//   process of committing makeShardDurable)
	//   == v              -> k is readable (from storage+versionedData) @ [storageVersion,v], and not being updated
	//   when version increases
	//   == latestVersion  -> k is readable (from storage+versionedData) @ [storageVersion,version.get()], and thus
	//   stays available when version increases
	CoalescedKeyRangeMap<Version> newestAvailableVersion;

	CoalescedKeyRangeMap<Version> newestDirtyVersion; // Similar to newestAvailableVersion, but includes (only) keys
	                                                  // that were only partly available (due to cancelled fetchKeys)

	// The following are in rough order from newest to oldest
	Version lastTLogVersion, lastVersionWithData, restoredVersion, prevVersion;
	NotifiedVersion version;
	NotifiedVersion desiredOldestVersion; // We can increase oldestVersion (and then durableVersion) to this version
	                                      // when the disk permits
	NotifiedVersion oldestVersion; // See also storageVersion()
	NotifiedVersion durableVersion; // At least this version will be readable from storage after a power failure
	// In the event of the disk corruption, sqlite and redwood will either not recover, recover to durableVersion
	// but be unable to read some data, or they could lose the last commit. If we lose the last commit, the storage
	// might not be able to peek from the tlog (depending on when it sent the last pop). So this version just keeps
	// track of the version we committed to the storage engine before we did commit durableVersion.
	Version storageMinRecoverVersion = 0;
	Version rebootAfterDurableVersion;
	int8_t primaryLocality;
	NotifiedVersion knownCommittedVersion;

	Deque<std::pair<Version, Version>> recoveryVersionSkips;
	int64_t versionLag; // An estimate for how many versions it takes for the data to move from the logs to this storage
	                    // server

	Optional<UID> sourceTLogID; // the tLog from which the latest batch of versions were fetched

	ProtocolVersion logProtocol;

	Reference<ILogSystem> logSystem;
	Reference<ILogSystem::IPeekCursor> logCursor;

	// The version the cluster starts on. This value is not persisted and may
	// not be valid after a recovery.
	Version initialClusterVersion = 1;
	UID thisServerID;
	Optional<UID> tssPairID; // if this server is a tss, this is the id of its (ss) pair
	Optional<UID> ssPairID; // if this server is an ss, this is the id of its (tss) pair
	Optional<double> tssFaultInjectTime;
	bool tssInQuarantine;

	Key sk;
	Reference<AsyncVar<ServerDBInfo> const> db;
	Database cx;
	ActorCollection actors;

	CoalescedKeyRangeMap<bool, int64_t, KeyBytesMetric<int64_t>> byteSampleClears;
	AsyncVar<bool> byteSampleClearsTooLarge;
	Future<Void> byteSampleRecovery;
	Future<Void> durableInProgress;

	AsyncMap<Key, bool> watches;
	AsyncMap<int64_t, bool> tenantWatches;
	int64_t watchBytes;
	int64_t numWatches;
	AsyncVar<bool> noRecentUpdates;
	double lastUpdate;

	std::string folder;
	std::string checkpointFolder;
	std::string fetchedCheckpointFolder;
	std::string bulkDumpFolder;
	std::string bulkLoadFolder;

	// defined only during splitMutations()/addMutation()
	UpdateEagerReadInfo* updateEagerReads;

	FlowLock durableVersionLock;
	FlowLock fetchKeysParallelismLock;
	// Extra lock that prevents too much post-initial-fetch work from building up, such as mutation applying and change
	// feed tail fetching
	FlowLock fetchKeysParallelismChangeFeedLock;
	int64_t fetchKeysBytesBudget;
	AsyncVar<bool> fetchKeysBudgetUsed;
	int64_t fetchKeysTotalCommitBytes;
	std::vector<Promise<FetchInjectionInfo*>> readyFetchKeys;

	ThroughputLimiter fetchKeysLimiter;

	FlowLock serveFetchCheckpointParallelismLock;

	std::unordered_map<UID, std::shared_ptr<MoveInShard>> moveInShards;

	Reference<PriorityMultiLock> ssLock;
	std::vector<int> readPriorityRanks;

	Future<PriorityMultiLock::Lock> getReadLock(const Optional<ReadOptions>& options) {
		int readType = (int)(options.present() ? options.get().type : ReadType::NORMAL);
		readType = std::clamp<int>(readType, 0, readPriorityRanks.size() - 1);
		return ssLock->lock(readPriorityRanks[readType]);
	}

	FlowLock serveAuditStorageParallelismLock;

	FlowLock serveBulkDumpParallelismLock;

	int64_t instanceID;

	Promise<Void> otherError;
	Promise<Void> coreStarted;
	bool shuttingDown;

	Promise<Void> registerInterfaceAcceptingRequests;
	Future<Void> interfaceRegistered;

	bool behind;
	bool versionBehind;

	bool debug_inApplyUpdate;
	double debug_lastValidateTime;

	int64_t lastBytesInputEBrake;
	Version lastDurableVersionEBrake;

	int maxQueryQueue;
	int getAndResetMaxQueryQueueSize() {
		int val = maxQueryQueue;
		maxQueryQueue = 0;
		return val;
	}

	TransactionTagCounter transactionTagCounter;
	BusiestWriteTagContext busiestWriteTagContext;

	Optional<LatencyBandConfig> latencyBandConfig;

	Optional<EncryptionAtRestMode> encryptionMode;
	Reference<GetEncryptCipherKeysMonitor> getEncryptCipherKeysMonitor;

	struct Counters : CommonStorageCounters {

		Counter allQueries, systemKeyQueries, getKeyQueries, getValueQueries, getRangeQueries, getRangeSystemKeyQueries,
		    getRangeStreamQueries, lowPriorityQueries, rowsQueried, watchQueries, emptyQueries, feedRowsQueried,
		    feedBytesQueried, feedStreamQueries, rejectedFeedStreamQueries, feedVersionQueries;

		// counters related to getMappedRange queries
		Counter getMappedRangeBytesQueried, finishedGetMappedRangeSecondaryQueries, getMappedRangeQueries,
		    finishedGetMappedRangeQueries;

		// Bytes pulled from TLogs, it counts the size of the key value pairs, e.g., key-value pair ("a", "b") is
		// counted as 2 Bytes.
		Counter logicalBytesInput;
		// Bytes pulled from TLogs for moving-in shards, it counts the mutations sent to the moving-in shard during
		// Fetching and Waiting phases.
		Counter logicalBytesMoveInOverhead;
		// Bytes committed to the underlying storage engine by SS, it counts the size of key value pairs.
		Counter kvCommitLogicalBytes;
		// Count of all clearRange operations to the storage engine.
		Counter kvClearRanges;
		// Count of all clearRange operations on a singlekeyRange(key delete) to the storage engine.
		Counter kvClearSingleKey;
		// ClearRange operations issued by FDB, instead of from users, e.g., ClearRange operations to remove a shard
		// from a storage server, as in removeDataRange().
		Counter kvSystemClearRanges;
		// Bytes of the mutations that have been removed from memory because they durable. The counting is same as
		// bytesInput, instead of the actual bytes taken in the storages, so that (bytesInput - bytesDurable) can
		// reflect the current memory footprint of MVCC.
		Counter bytesDurable;

		// Bytes fetched by fetchChangeFeed for data movements.
		Counter feedBytesFetched;

		Counter sampledBytesCleared;
		Counter atomicMutations, changeFeedMutations, changeFeedMutationsDurable;
		Counter updateBatches, updateVersions;
		Counter loops;
		Counter fetchWaitingMS, fetchWaitingCount, fetchExecutingMS, fetchExecutingCount;
		Counter readsRejected;
		Counter wrongShardServer;
		Counter fetchedVersions;
		Counter fetchesFromLogs;
		// The following counters measure how many of lookups in the getMappedRangeQueries are effective. "Miss"
		// means fallback if fallback is enabled, otherwise means failure (so that another layer could implement
		// fallback).
		Counter quickGetValueHit, quickGetValueMiss, quickGetKeyValuesHit, quickGetKeyValuesMiss;

		// The number of logical bytes returned from storage engine, in response to readRange operations.
		Counter kvScanBytes;
		// The number of logical bytes returned from storage engine, in response to readValue operations.
		Counter kvGetBytes;
		// The number of keys read from storage engine by eagerReads.
		Counter eagerReadsKeys;
		// The count of readValue operation to the storage engine.
		Counter kvGets;
		// The count of readValue operation to the storage engine.
		Counter kvScans;
		// The count of commit operation to the storage engine.
		Counter kvCommits;
		// The count of change feed reads that hit disk
		Counter changeFeedDiskReads;
		// The count of ChangeServerKeys actions.
		Counter changeServerKeysAssigned;
		Counter changeServerKeysUnassigned;

		// The count of 'set' inserted to pTree. The actual ptree.insert() number could be higher, because of the range
		// clear split, see metric pTreeClearSplits.
		Counter pTreeSets;
		// The count of clear range inserted to pTree
		Counter pTreeClears;
		// If set is within a range of clear, the clear is split. It's tracking the number of splits, the split could be
		// expensive.
		Counter pTreeClearSplits;

		LatencySample readLatencySample;
		LatencySample readKeyLatencySample;
		LatencySample readValueLatencySample;
		LatencySample readRangeLatencySample;
		LatencySample readVersionWaitSample;
		LatencySample readQueueWaitSample;
		LatencySample kvReadRangeLatencySample;
		LatencySample updateLatencySample;
		LatencySample updateEncryptionLatencySample;

		LatencyBands readLatencyBands;
		std::unique_ptr<LatencySample> mappedRangeSample; // Samples getMappedRange latency
		std::unique_ptr<LatencySample> mappedRangeRemoteSample; // Samples getMappedRange remote subquery latency
		std::unique_ptr<LatencySample> mappedRangeLocalSample; // Samples getMappedRange local subquery latency

		explicit Counters(StorageServer* self)
		  : CommonStorageCounters("StorageServer", self->thisServerID.toString(), &self->metrics),
		    allQueries("QueryQueue", cc), systemKeyQueries("SystemKeyQueries", cc), getKeyQueries("GetKeyQueries", cc),
		    getValueQueries("GetValueQueries", cc), getRangeQueries("GetRangeQueries", cc),
		    getRangeSystemKeyQueries("GetRangeSystemKeyQueries", cc),
		    getMappedRangeQueries("GetMappedRangeQueries", cc), getRangeStreamQueries("GetRangeStreamQueries", cc),
		    lowPriorityQueries("LowPriorityQueries", cc), rowsQueried("RowsQueried", cc),
		    watchQueries("WatchQueries", cc), emptyQueries("EmptyQueries", cc), feedRowsQueried("FeedRowsQueried", cc),
		    feedBytesQueried("FeedBytesQueried", cc), feedStreamQueries("FeedStreamQueries", cc),
		    rejectedFeedStreamQueries("RejectedFeedStreamQueries", cc), feedVersionQueries("FeedVersionQueries", cc),
		    logicalBytesInput("LogicalBytesInput", cc), logicalBytesMoveInOverhead("LogicalBytesMoveInOverhead", cc),
		    kvCommitLogicalBytes("KVCommitLogicalBytes", cc), kvClearRanges("KVClearRanges", cc),
		    kvClearSingleKey("KVClearSingleKey", cc), kvSystemClearRanges("KVSystemClearRanges", cc),
		    bytesDurable("BytesDurable", cc), feedBytesFetched("FeedBytesFetched", cc),
		    sampledBytesCleared("SampledBytesCleared", cc), atomicMutations("AtomicMutations", cc),
		    changeFeedMutations("ChangeFeedMutations", cc),
		    changeFeedMutationsDurable("ChangeFeedMutationsDurable", cc), updateBatches("UpdateBatches", cc),
		    updateVersions("UpdateVersions", cc), loops("Loops", cc), fetchWaitingMS("FetchWaitingMS", cc),
		    fetchWaitingCount("FetchWaitingCount", cc), fetchExecutingMS("FetchExecutingMS", cc),
		    fetchExecutingCount("FetchExecutingCount", cc), readsRejected("ReadsRejected", cc),
		    wrongShardServer("WrongShardServer", cc), fetchedVersions("FetchedVersions", cc),
		    fetchesFromLogs("FetchesFromLogs", cc), quickGetValueHit("QuickGetValueHit", cc),
		    quickGetValueMiss("QuickGetValueMiss", cc), quickGetKeyValuesHit("QuickGetKeyValuesHit", cc),
		    quickGetKeyValuesMiss("QuickGetKeyValuesMiss", cc), kvScanBytes("KVScanBytes", cc),
		    kvGetBytes("KVGetBytes", cc), eagerReadsKeys("EagerReadsKeys", cc), kvGets("KVGets", cc),
		    kvScans("KVScans", cc), kvCommits("KVCommits", cc), changeFeedDiskReads("ChangeFeedDiskReads", cc),
		    getMappedRangeBytesQueried("GetMappedRangeBytesQueried", cc),
		    finishedGetMappedRangeQueries("FinishedGetMappedRangeQueries", cc),
		    finishedGetMappedRangeSecondaryQueries("FinishedGetMappedRangeSecondaryQueries", cc),
		    pTreeSets("PTreeSets", cc), pTreeClears("PTreeClears", cc), pTreeClearSplits("PTreeClearSplits", cc),
		    changeServerKeysAssigned("ChangeServerKeysAssigned", cc),
		    changeServerKeysUnassigned("ChangeServerKeysUnassigned", cc),
		    readLatencySample("ReadLatencyMetrics",
		                      self->thisServerID,
		                      SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                      SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
		    readKeyLatencySample("GetKeyMetrics",
		                         self->thisServerID,
		                         SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                         SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
		    readValueLatencySample("GetValueMetrics",
		                           self->thisServerID,
		                           SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                           SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
		    readRangeLatencySample("GetRangeMetrics",
		                           self->thisServerID,
		                           SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                           SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
		    readVersionWaitSample("ReadVersionWaitMetrics",
		                          self->thisServerID,
		                          SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                          SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
		    readQueueWaitSample("ReadQueueWaitMetrics",
		                        self->thisServerID,
		                        SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                        SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
		    kvReadRangeLatencySample("KVGetRangeMetrics",
		                             self->thisServerID,
		                             SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                             SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
		    updateLatencySample("UpdateLatencyMetrics",
		                        self->thisServerID,
		                        SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                        SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
		    updateEncryptionLatencySample("UpdateEncryptionLatencyMetrics",
		                                  self->thisServerID,
		                                  SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                                  SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
		    readLatencyBands("ReadLatencyBands", self->thisServerID, SERVER_KNOBS->STORAGE_LOGGING_DELAY),
		    mappedRangeSample(std::make_unique<LatencySample>("GetMappedRangeMetrics",
		                                                      self->thisServerID,
		                                                      SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                                                      SERVER_KNOBS->LATENCY_SKETCH_ACCURACY)),
		    mappedRangeRemoteSample(std::make_unique<LatencySample>("GetMappedRangeRemoteMetrics",
		                                                            self->thisServerID,
		                                                            SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                                                            SERVER_KNOBS->LATENCY_SKETCH_ACCURACY)),
		    mappedRangeLocalSample(std::make_unique<LatencySample>("GetMappedRangeLocalMetrics",
		                                                           self->thisServerID,
		                                                           SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                                                           SERVER_KNOBS->LATENCY_SKETCH_ACCURACY)) {
			specialCounter(cc, "LastTLogVersion", [self]() { return self->lastTLogVersion; });
			specialCounter(cc, "Version", [self]() { return self->version.get(); });
			specialCounter(cc, "StorageVersion", [self]() { return self->storageVersion(); });
			specialCounter(cc, "DurableVersion", [self]() { return self->durableVersion.get(); });
			specialCounter(cc, "DesiredOldestVersion", [self]() { return self->desiredOldestVersion.get(); });
			specialCounter(cc, "VersionLag", [self]() { return self->versionLag; });
			specialCounter(cc, "LocalRate", [self] { return int64_t(self->currentRate() * 100); });

			specialCounter(
			    cc, "FetchKeysFetchActive", [self]() { return self->fetchKeysParallelismLock.activePermits(); });
			specialCounter(cc, "FetchKeysWaiting", [self]() { return self->fetchKeysParallelismLock.waiters(); });
			specialCounter(cc, "FetchKeysChangeFeedFetchActive", [self]() {
				return self->fetchKeysParallelismChangeFeedLock.activePermits();
			});
			specialCounter(cc, "FetchKeysFullFetchWaiting", [self]() {
				return self->fetchKeysParallelismChangeFeedLock.waiters();
			});
			specialCounter(cc, "ServeFetchCheckpointActive", [self]() {
				return self->serveFetchCheckpointParallelismLock.activePermits();
			});
			specialCounter(cc, "ServeFetchCheckpointWaiting", [self]() {
				return self->serveFetchCheckpointParallelismLock.waiters();
			});
			specialCounter(cc, "ServeValidateStorageActive", [self]() {
				return self->serveAuditStorageParallelismLock.activePermits();
			});
			specialCounter(cc, "ServeValidateStorageWaiting", [self]() {
				return self->serveAuditStorageParallelismLock.waiters();
			});
			specialCounter(
			    cc, "ServerBulkDumpActive", [self]() { return self->serveBulkDumpParallelismLock.activePermits(); });
			specialCounter(
			    cc, "ServerBulkDumpWaiting", [self]() { return self->serveBulkDumpParallelismLock.waiters(); });
			specialCounter(cc, "QueryQueueMax", [self]() { return self->getAndResetMaxQueryQueueSize(); });
			specialCounter(cc, "ActiveWatches", [self]() { return self->numWatches; });
			specialCounter(cc, "WatchBytes", [self]() { return self->watchBytes; });
			specialCounter(cc, "KvstoreSizeTotal", [self]() { return std::get<0>(self->storage.getSize()); });
			specialCounter(cc, "KvstoreNodeTotal", [self]() { return std::get<1>(self->storage.getSize()); });
			specialCounter(cc, "KvstoreInlineKey", [self]() { return std::get<2>(self->storage.getSize()); });
			specialCounter(cc, "ActiveChangeFeeds", [self]() { return self->uidChangeFeed.size(); });
			specialCounter(cc, "ActiveChangeFeedQueries", [self]() { return self->activeFeedQueries; });
			specialCounter(cc, "ChangeFeedMemoryBytes", [self]() { return self->changeFeedMemoryBytes; });
		}
	} counters;

	// Bytes read from storage engine when a storage server starts.
	int64_t bytesRestored = 0;

	Reference<EventCacheHolder> storageServerSourceTLogIDEventHolder;

	// Tenant metadata to manage connection to blob store for fetchKeys()
	BGTenantMap tenantData;

	std::shared_ptr<AccumulativeChecksumValidator> acsValidator = nullptr;

	StorageServer(IKeyValueStore* storage,
	              Reference<AsyncVar<ServerDBInfo> const> const& db,
	              StorageServerInterface const& ssi,
	              Reference<GetEncryptCipherKeysMonitor> encryptionMonitor)
	  : shardAware(false), tlogCursorReadsLatencyHistogram(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
	                                                                               TLOG_CURSOR_READS_LATENCY_HISTOGRAM,
	                                                                               Histogram::Unit::milliseconds)),
	    ssVersionLockLatencyHistogram(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
	                                                          SS_VERSION_LOCK_LATENCY_HISTOGRAM,
	                                                          Histogram::Unit::milliseconds)),
	    eagerReadsLatencyHistogram(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
	                                                       EAGER_READS_LATENCY_HISTOGRAM,
	                                                       Histogram::Unit::milliseconds)),
	    fetchKeysPTreeUpdatesLatencyHistogram(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
	                                                                  FETCH_KEYS_PTREE_UPDATES_LATENCY_HISTOGRAM,
	                                                                  Histogram::Unit::milliseconds)),
	    tLogMsgsPTreeUpdatesLatencyHistogram(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
	                                                                 TLOG_MSGS_PTREE_UPDATES_LATENCY_HISTOGRAM,
	                                                                 Histogram::Unit::milliseconds)),
	    storageUpdatesDurableLatencyHistogram(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
	                                                                  STORAGE_UPDATES_DURABLE_LATENCY_HISTOGRAM,
	                                                                  Histogram::Unit::milliseconds)),
	    storageCommitLatencyHistogram(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
	                                                          STORAGE_COMMIT_LATENCY_HISTOGRAM,
	                                                          Histogram::Unit::milliseconds)),
	    ssDurableVersionUpdateLatencyHistogram(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
	                                                                   SS_DURABLE_VERSION_UPDATE_LATENCY_HISTOGRAM,
	                                                                   Histogram::Unit::milliseconds)),
	    readRangeBytesReturnedHistogram(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
	                                                            SS_READ_RANGE_BYTES_RETURNED_HISTOGRAM,
	                                                            Histogram::Unit::bytes)),
	    readRangeBytesLimitHistogram(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
	                                                         SS_READ_RANGE_BYTES_LIMIT_HISTOGRAM,
	                                                         Histogram::Unit::bytes)),
	    readRangeKVPairsReturnedHistogram(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
	                                                              SS_READ_RANGE_KV_PAIRS_RETURNED_HISTOGRAM,
	                                                              Histogram::Unit::bytes)),
	    tag(invalidTag), poppedAllAfter(std::numeric_limits<Version>::max()), cpuUsage(0.0), diskUsage(0.0),
	    storage(this, storage), shardChangeCounter(0), lastTLogVersion(0), lastVersionWithData(0), restoredVersion(0),
	    prevVersion(0), rebootAfterDurableVersion(std::numeric_limits<Version>::max()),
	    primaryLocality(tagLocalityInvalid), knownCommittedVersion(0), versionLag(0), logProtocol(0),
	    thisServerID(ssi.id()), tssInQuarantine(false), db(db), actors(false),
	    trackShardAssignmentMinVersion(invalidVersion), byteSampleClears(false, "\xff\xff\xff"_sr),
	    durableInProgress(Void()), watchBytes(0), numWatches(0), noRecentUpdates(false), lastUpdate(now()),
	    updateEagerReads(nullptr), fetchKeysParallelismLock(SERVER_KNOBS->FETCH_KEYS_PARALLELISM),
	    fetchKeysParallelismChangeFeedLock(SERVER_KNOBS->FETCH_KEYS_PARALLELISM_CHANGE_FEED),
	    fetchKeysBytesBudget(SERVER_KNOBS->STORAGE_FETCH_BYTES), fetchKeysBudgetUsed(false),
	    fetchKeysTotalCommitBytes(0), fetchKeysLimiter(SERVER_KNOBS->STORAGE_FETCH_KEYS_RATE_LIMIT),
	    serveFetchCheckpointParallelismLock(SERVER_KNOBS->SERVE_FETCH_CHECKPOINT_PARALLELISM),
	    ssLock(makeReference<PriorityMultiLock>(SERVER_KNOBS->STORAGE_SERVER_READ_CONCURRENCY,
	                                            SERVER_KNOBS->STORAGESERVER_READ_PRIORITIES)),
	    serveAuditStorageParallelismLock(SERVER_KNOBS->SERVE_AUDIT_STORAGE_PARALLELISM),
	    serveBulkDumpParallelismLock(SERVER_KNOBS->SS_SERVE_BULKDUMP_PARALLELISM),
	    instanceID(deterministicRandom()->randomUniqueID().first()), shuttingDown(false), behind(false),
	    versionBehind(false), debug_inApplyUpdate(false), debug_lastValidateTime(0), lastBytesInputEBrake(0),
	    lastDurableVersionEBrake(0), maxQueryQueue(0),
	    transactionTagCounter(ssi.id(),
	                          /*maxTagsTracked=*/SERVER_KNOBS->SS_THROTTLE_TAGS_TRACKED,
	                          /*minRateTracked=*/SERVER_KNOBS->MIN_TAG_READ_PAGES_RATE *
	                              CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE),
	    busiestWriteTagContext(ssi.id()), getEncryptCipherKeysMonitor(encryptionMonitor), counters(this),
	    storageServerSourceTLogIDEventHolder(
	        makeReference<EventCacheHolder>(ssi.id().toString() + "/StorageServerSourceTLogID")),
	    tenantData(db),
	    acsValidator(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM &&
	                         !SERVER_KNOBS->ENABLE_VERSION_VECTOR && !SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST
	                     ? std::make_shared<AccumulativeChecksumValidator>()
	                     : nullptr) {
		readPriorityRanks = parseStringToVector<int>(SERVER_KNOBS->STORAGESERVER_READTYPE_PRIORITY_MAP, ',');
		ASSERT(readPriorityRanks.size() > (int)ReadType::MAX);
		version.initMetric("StorageServer.Version"_sr, counters.cc.getId());
		oldestVersion.initMetric("StorageServer.OldestVersion"_sr, counters.cc.getId());
		durableVersion.initMetric("StorageServer.DurableVersion"_sr, counters.cc.getId());
		desiredOldestVersion.initMetric("StorageServer.DesiredOldestVersion"_sr, counters.cc.getId());

		newestAvailableVersion.insert(allKeys, invalidVersion);
		newestDirtyVersion.insert(allKeys, invalidVersion);
		if (storage->shardAware()) {
			addShard(ShardInfo::newShard(this, StorageServerShard::notAssigned(allKeys)));
		} else {
			addShard(ShardInfo::newNotAssigned(allKeys));
		}

		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, LockAware::True);

		this->storage.kvCommitLogicalBytes = &counters.kvCommitLogicalBytes;
		this->storage.kvClearRanges = &counters.kvClearRanges;
		this->storage.kvClearSingleKey = &counters.kvClearSingleKey;
		this->storage.kvGets = &counters.kvGets;
		this->storage.kvScans = &counters.kvScans;
		this->storage.kvCommits = &counters.kvCommits;
	}

	//~StorageServer() { fclose(log); }
	void addRangeToPhysicalShard(Reference<ShardInfo> newRange) {
		if (!shardAware || newRange->notAssigned()) {
			return;
		}

		auto [it, ignored] =
		    physicalShards.insert(std::make_pair(newRange->desiredShardId, SSPhysicalShard(newRange->desiredShardId)));
		it->second.addRange(newRange);
	}

	void removeRangeFromPhysicalShard(Reference<ShardInfo> range) {
		if (!range.isValid() || !shardAware || range->notAssigned()) {
			return;
		}

		auto it = physicalShards.find(range->desiredShardId);
		ASSERT(it != physicalShards.end());
		it->second.removeRange(range);
	}

	// Puts the given shard into shards.  The caller is responsible for adding shards
	//   for all ranges in shards.getAffectedRangesAfterInsertion(newShard->keys)), because these
	//   shards are invalidated by the call.
	void addShard(ShardInfo* newShard) {
		ASSERT(!newShard->keys.empty());
		newShard->changeCounter = ++shardChangeCounter;
		// TraceEvent("AddShard", this->thisServerID).detail("KeyBegin", newShard->keys.begin).detail("KeyEnd", newShard->keys.end).detail("State",newShard->isReadable() ? "Readable" : newShard->notAssigned() ? "NotAssigned" : "Adding").detail("Version", this->version.get());
		/*auto affected = shards.getAffectedRangesAfterInsertion( newShard->keys, Reference<ShardInfo>() );
		for(auto i = affected.begin(); i != affected.end(); ++i)
		    shards.insert( *i, Reference<ShardInfo>() );*/

		if (shardAware && newShard->notAssigned()) {
			auto sh = shards.intersectingRanges(newShard->keys);
			for (auto it = sh.begin(); it != sh.end(); ++it) {
				if (it->value().isValid() && !it->value()->notAssigned()) {
					TraceEvent(SevVerbose, "StorageServerAddShardClear")
					    .detail("NewShardRange", newShard->keys)
					    .detail("Range", it->value()->keys)
					    .detail("ShardID", format("%016llx", it->value()->desiredShardId))
					    .detail("NewShardID", format("%016llx", newShard->desiredShardId))
					    .detail("NewShardActualID", format("%016llx", newShard->shardId));
					removeRangeFromPhysicalShard(it->value());
				}
			}
		}

		Reference<ShardInfo> rShard(newShard);
		shards.insert(newShard->keys, rShard);
		addRangeToPhysicalShard(rShard);
	}
	void addMutation(Version version,
	                 bool fromFetch,
	                 MutationRef const& mutation,
	                 MutationRefAndCipherKeys const& encryptedMutation,
	                 KeyRangeRef const& shard,
	                 UpdateEagerReadInfo* eagerReads);
	void setInitialVersion(Version ver) {
		version = ver;
		desiredOldestVersion = ver;
		oldestVersion = ver;
		durableVersion = ver;
		storageMinRecoverVersion = ver;
		lastVersionWithData = ver;
		restoredVersion = ver;

		mutableData().createNewVersion(ver);
		mutableData().forgetVersionsBefore(ver);
	}

	bool isTss() const { return tssPairID.present(); }

	bool isSSWithTSSPair() const { return ssPairID.present(); }

	void setSSWithTssPair(UID idOfTSS) { ssPairID = Optional<UID>(idOfTSS); }

	void clearSSWithTssPair() { ssPairID = Optional<UID>(); }

	// This is the maximum version that might be read from storage (the minimum version is durableVersion)
	Version storageVersion() const { return oldestVersion.get(); }

	bool isReadable(KeyRangeRef const& keys) const override {
		auto sh = shards.intersectingRanges(keys);
		for (auto i = sh.begin(); i != sh.end(); ++i)
			if (!i->value()->isReadable())
				return false;
		return true;
	}

	void checkChangeCounter(uint64_t oldShardChangeCounter, KeyRef const& key) {
		if (oldShardChangeCounter != shardChangeCounter && shards[key]->changeCounter > oldShardChangeCounter) {
			CODE_PROBE(true, "shard change during getValueQ");
			throw wrong_shard_server();
		}
	}

	void checkChangeCounter(uint64_t oldShardChangeCounter, KeyRangeRef const& keys) {
		if (oldShardChangeCounter != shardChangeCounter) {
			auto sh = shards.intersectingRanges(keys);
			for (auto i = sh.begin(); i != sh.end(); ++i)
				if (i->value()->changeCounter > oldShardChangeCounter) {
					CODE_PROBE(true, "shard change during range operation");
					throw wrong_shard_server();
				}
		}
	}

	Counter::Value queueSize() const { return counters.bytesInput.getValue() - counters.bytesDurable.getValue(); }

	// penalty used by loadBalance() to balance requests among SSes. We prefer SS with less write queue size.
	double getPenalty() const override {
		return std::max(std::max(1.0,
		                         (queueSize() - (SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER -
		                                         2.0 * SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER)) /
		                             SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER),
		                (currentRate() < 1e-6 ? 1e6 : 1.0 / currentRate()));
	}

	// Normally the storage server prefers to serve read requests over making mutations
	// durable to disk. However, when the storage server falls to far behind on
	// making mutations durable, this function will change the priority to prefer writes.
	Future<Void> getQueryDelay() {
		if ((version.get() - durableVersion.get() > SERVER_KNOBS->LOW_PRIORITY_DURABILITY_LAG) ||
		    (queueSize() > SERVER_KNOBS->LOW_PRIORITY_STORAGE_QUEUE_BYTES)) {
			++counters.lowPriorityQueries;
			return delay(0, TaskPriority::LowPriorityRead);
		}
		return delay(0, TaskPriority::DefaultEndpoint);
	}

	template <class Reply>
	using isLoadBalancedReply = std::is_base_of<LoadBalancedReply, Reply>;

	template <class Reply>
	typename std::enable_if<isLoadBalancedReply<Reply>::value, void>::type
	sendErrorWithPenalty(const ReplyPromise<Reply>& promise, const Error& err, double penalty) {
		if (err.code() == error_code_wrong_shard_server) {
			++counters.wrongShardServer;
		}
		Reply reply;
		reply.error = err;
		reply.penalty = penalty;
		promise.send(reply);
	}

	template <class Reply>
	typename std::enable_if<!isLoadBalancedReply<Reply>::value, void>::type
	sendErrorWithPenalty(const ReplyPromise<Reply>& promise, const Error& err, double) {
		if (err.code() == error_code_wrong_shard_server) {
			++counters.wrongShardServer;
		}
		promise.sendError(err);
	}

	template <class Request>
	bool shouldRead(const Request& request) {
		auto rate = currentRate();
		if (isTSSInQuarantine() || (rate < SERVER_KNOBS->STORAGE_DURABILITY_LAG_REJECT_THRESHOLD &&
		                            deterministicRandom()->random01() >
		                                std::max(SERVER_KNOBS->STORAGE_DURABILITY_LAG_MIN_RATE,
		                                         rate / SERVER_KNOBS->STORAGE_DURABILITY_LAG_REJECT_THRESHOLD))) {
			sendErrorWithPenalty(request.reply, server_overloaded(), getPenalty());
			++counters.readsRejected;
			return false;
		}
		return true;
	}

	template <class Request, class HandleFunction>
	Future<Void> readGuard(const Request& request, const HandleFunction& fun) {
		bool read = shouldRead(request);
		if (!read) {
			return Void();
		}
		return fun(this, request);
	}

	Version minFeedVersionForAddress(const NetworkAddress& addr) {
		auto& clientVersions = changeFeedClientVersions[addr];
		Version minVersion = version.get();
		for (auto& it : clientVersions) {
			/*fmt::print("SS {0} Blocked client {1} @ {2}\n",
			        thisServerID.toString().substr(0, 4),
			        it.first.toString().substr(0, 8),
			        it.second);*/
			minVersion = std::min(minVersion, it.second);
		}
		return minVersion;
	}

	// count in-memory change feed bytes towards storage queue size, for the purposes of memory management and
	// throttling
	void addFeedBytesAtVersion(int64_t bytes, Version version) {
		if (feedMemoryBytesByVersion.empty() || version != feedMemoryBytesByVersion.back().first) {
			ASSERT(feedMemoryBytesByVersion.empty() || version >= feedMemoryBytesByVersion.back().first);
			feedMemoryBytesByVersion.push_back({ version, 0 });
		}
		feedMemoryBytesByVersion.back().second += bytes;
		changeFeedMemoryBytes += bytes;
		if (SERVER_KNOBS->STORAGE_INCLUDE_FEED_STORAGE_QUEUE) {
			counters.bytesInput += bytes;
		}
	}

	void getSplitPoints(SplitRangeRequest const& req) override {
		try {
			checkTenantEntry(version.get(), req.tenantInfo, true);
			metrics.getSplitPoints(req, req.tenantInfo.prefix);
		} catch (Error& e) {
			req.reply.sendError(e);
		}
	}

	void maybeInjectTargetedRestart(Version v) {
		// inject an SS restart at most once per test
		if (g_network->isSimulated() && !g_simulator->speedUpSimulation &&
		    now() > g_simulator->injectTargetedSSRestartTime &&
		    rebootAfterDurableVersion == std::numeric_limits<Version>::max()) {
			CODE_PROBE(true, "Injecting SS targeted restart");
			TraceEvent("SimSSInjectTargetedRestart", thisServerID).detail("Version", v);
			rebootAfterDurableVersion = v;
			g_simulator->injectTargetedSSRestartTime = std::numeric_limits<double>::max();
		}
	}

	bool maybeInjectDelay() {
		if (g_network->isSimulated() && !g_simulator->speedUpSimulation && now() > g_simulator->injectSSDelayTime) {
			CODE_PROBE(true, "Injecting SS targeted delay");
			TraceEvent("SimSSInjectDelay", thisServerID).log();
			g_simulator->injectSSDelayTime = std::numeric_limits<double>::max();
			return true;
		}
		return false;
	}

	Future<Void> waitMetricsTenantAware(const WaitMetricsRequest& req) override;

	void addActor(Future<Void> future) override { actors.add(future); }

	void getStorageMetrics(const GetStorageMetricsRequest& req) override {
		StorageBytes sb = storage.getStorageBytes();
		metrics.getStorageMetrics(req,
		                          sb,
		                          counters.bytesInput.getRate(),
		                          versionLag,
		                          lastUpdate,
		                          counters.bytesDurable.getValue(),
		                          counters.bytesInput.getValue());
	}

	void getSplitMetrics(const SplitMetricsRequest& req) override { this->metrics.splitMetrics(req); }

	void getHotRangeMetrics(const ReadHotSubRangeRequest& req) override { this->metrics.getReadHotRanges(req); }

	int64_t getHotShardsMetrics(const KeyRange& range) override { return this->metrics.getHotShards(range); }

	// Used for recording shard assignment history for auditStorage
	std::vector<std::pair<Version, KeyRange>> shardAssignmentHistory;
	Version trackShardAssignmentMinVersion; // == invalidVersion means tracking stopped

	std::string printShardAssignmentHistory() {
		std::string toPrint = "";
		for (const auto& [version, range] : shardAssignmentHistory) {
			toPrint = toPrint + std::to_string(version) + " ";
		}
		return toPrint;
	}

	void startTrackShardAssignment(Version startVersion) {
		ASSERT(startVersion != invalidVersion);
		ASSERT(trackShardAssignmentMinVersion == invalidVersion);
		trackShardAssignmentMinVersion = startVersion;
		return;
	}

	void stopTrackShardAssignment() { trackShardAssignmentMinVersion = invalidVersion; }

	std::vector<std::pair<Version, KeyRangeRef>> getShardAssignmentHistory(Version early, Version later) {
		std::vector<std::pair<Version, KeyRangeRef>> res;
		for (const auto& shardAssignment : shardAssignmentHistory) {
			if (shardAssignment.first >= early && shardAssignment.first <= later) {
				TraceEvent(SevVerbose, "ShardAssignmentHistoryGetOne", thisServerID)
				    .detail("Keys", shardAssignment.second)
				    .detail("Version", shardAssignment.first);
				res.push_back(shardAssignment);
			} else {
				TraceEvent(SevVerbose, "ShardAssignmentHistoryGetSkip", thisServerID)
				    .detail("Keys", shardAssignment.second)
				    .detail("Version", shardAssignment.first)
				    .detail("EarlyVersion", early)
				    .detail("LaterVersion", later);
			}
		}
		TraceEvent(SevVerbose, "ShardAssignmentHistoryGetDone", thisServerID)
		    .detail("EarlyVersion", early)
		    .detail("LaterVersion", later)
		    .detail("HistoryTotalSize", shardAssignmentHistory.size())
		    .detail("HistoryTotal", printShardAssignmentHistory());
		return res;
	}
};

const StringRef StorageServer::CurrentRunningFetchKeys::emptyString = ""_sr;
const KeyRangeRef StorageServer::CurrentRunningFetchKeys::emptyKeyRange =
    KeyRangeRef(StorageServer::CurrentRunningFetchKeys::emptyString,
                StorageServer::CurrentRunningFetchKeys::emptyString);

// If and only if key:=value is in (storage+versionedData),    // NOT ACTUALLY: and key < allKeys.end,
//   and H(key) < |key+value|/bytesPerSample,
//     let sampledSize = max(|key+value|,bytesPerSample)
//     persistByteSampleKeys.begin()+key := sampledSize is in storage
//     (key,sampledSize) is in byteSample

// So P(key is sampled) * sampledSize == |key+value|

void StorageServer::byteSampleApplyMutation(MutationRef const& m, Version ver) {
	if (m.type == MutationRef::ClearRange)
		byteSampleApplyClear(KeyRangeRef(m.param1, m.param2), ver);
	else if (m.type == MutationRef::SetValue)
		byteSampleApplySet(KeyValueRef(m.param1, m.param2), ver);
	else
		ASSERT(false); // Mutation of unknown type modifying byte sample
}

// watchMap Operations
Reference<ServerWatchMetadata> StorageServer::getWatchMetadata(KeyRef key, int64_t tenantId) const {
	const WatchMapKey mapKey(tenantId, key);
	const auto it = watchMap.find(mapKey);
	if (it == watchMap.end())
		return Reference<ServerWatchMetadata>();
	return it->second;
}

KeyRef StorageServer::setWatchMetadata(Reference<ServerWatchMetadata> metadata) {
	KeyRef keyRef = metadata->key.contents();
	int64_t tenantId = metadata->tenantId;
	const WatchMapKey mapKey(tenantId, keyRef);

	watchMap[mapKey] = metadata;
	return keyRef;
}

void StorageServer::deleteWatchMetadata(KeyRef key, int64_t tenantId) {
	const WatchMapKey mapKey(tenantId, key);
	watchMap.erase(mapKey);
}

void StorageServer::clearWatchMetadata() {
	watchMap.clear();
}

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

/////////////////////////////////// Validation ///////////////////////////////////////
#ifndef __INTEL_COMPILER
#pragma region Validation
#endif
bool validateRange(StorageServer::VersionedData::ViewAtVersion const& view,
                   KeyRangeRef range,
                   Version version,
                   UID id,
                   Version minInsertVersion) {
	// * Nonoverlapping: No clear overlaps a set or another clear, or adjoins another clear.
	// * Old mutations are erased: All items in versionedData.atLatest() have insertVersion() > durableVersion()

	//TraceEvent("ValidateRange", id).detail("KeyBegin", range.begin).detail("KeyEnd", range.end).detail("Version", version);
	KeyRef k;
	bool ok = true;
	bool kIsClear = false;
	auto i = view.lower_bound(range.begin);
	if (i != view.begin())
		--i;
	for (; i != view.end() && i.key() < range.end; ++i) {
		ASSERT(i.insertVersion() > minInsertVersion);
		if (kIsClear && i->isClearTo() ? i.key() <= k : i.key() < k) {
			TraceEvent(SevError, "InvalidRange", id)
			    .detail("Key1", k)
			    .detail("Key2", i.key())
			    .detail("Version", version);
			ok = false;
		}
		// ASSERT( i.key() >= k );
		kIsClear = i->isClearTo();
		k = kIsClear ? i->getEndKey() : i.key();
	}
	return ok;
}

void validate(StorageServer* data, bool force = false) {
	try {
		if (!data->shuttingDown && (force || (EXPENSIVE_VALIDATION))) {
			data->newestAvailableVersion.validateCoalesced();
			data->newestDirtyVersion.validateCoalesced();

			for (auto s = data->shards.ranges().begin(); s != data->shards.ranges().end(); ++s) {
				TraceEvent(SevVerbose, "ValidateShard", data->thisServerID)
				    .detail("Range", s->range())
				    .detail("ShardID", format("%016llx", s->value()->shardId))
				    .detail("DesiredShardID", format("%016llx", s->value()->desiredShardId))
				    .detail("ShardRange", s->value()->keys)
				    .detail("ShardState", s->value()->debugDescribeState())
				    .log();
				ASSERT(s->value()->keys == s->range());
				ASSERT(!s->value()->keys.empty());
				if (data->shardAware) {
					s->value()->validate();
					if (!s->value()->notAssigned()) {
						auto it = data->physicalShards.find(s->value()->desiredShardId);
						ASSERT(it != data->physicalShards.end());
						ASSERT(it->second.hasRange(s->value()));
					}
				}
			}

			for (auto s = data->shards.ranges().begin(); s != data->shards.ranges().end(); ++s) {
				if (s->value()->isReadable()) {
					auto ar = data->newestAvailableVersion.intersectingRanges(s->range());
					for (auto a = ar.begin(); a != ar.end(); ++a) {
						TraceEvent(SevVerbose, "ValidateShardReadable", data->thisServerID)
						    .detail("Range", s->range())
						    .detail("ShardRange", s->value()->keys)
						    .detail("ShardState", s->value()->debugDescribeState())
						    .detail("AvailableRange", a->range())
						    .detail("AvailableVersion", a->value())
						    .log();
						ASSERT(a->value() == latestVersion);
					}
				}
			}

			// * versionedData contains versions [storageVersion(), version.get()].  It might also contain version
			// (version.get()+1), in which changeDurableVersion may be deleting ghosts, and/or it might
			//      contain later versions if applyUpdate is on the stack.
			ASSERT(data->data().getOldestVersion() == data->storageVersion());
			ASSERT(data->data().getLatestVersion() == data->version.get() ||
			       data->data().getLatestVersion() == data->version.get() + 1 ||
			       (data->debug_inApplyUpdate && data->data().getLatestVersion() > data->version.get()));

			auto latest = data->data().atLatest();

			// * Old shards are erased: versionedData.atLatest() has entries (sets or clear *begins*) only for keys in
			// readable or adding,transferred shards.
			for (auto s = data->shards.ranges().begin(); s != data->shards.ranges().end(); ++s) {
				ShardInfo* shard = s->value().getPtr();
				if (!shard->isInVersionedData()) {
					auto beginNext = latest.lower_bound(s->begin());
					auto endNext = latest.lower_bound(s->end());
					if (beginNext != endNext) {
						TraceEvent(SevError, "VF", data->thisServerID)
						    .detail("LastValidTime", data->debug_lastValidateTime)
						    .detail("KeyBegin", s->begin())
						    .detail("KeyEnd", s->end())
						    .detail("DbgState", shard->debugDescribeState())
						    .detail("FirstKey", beginNext.key())
						    .detail("LastKey", endNext != latest.end() ? endNext.key() : "End"_sr)
						    .detail("FirstInsertV", beginNext.insertVersion())
						    .detail("LastInsertV", endNext != latest.end() ? endNext.insertVersion() : invalidVersion);
					}
					ASSERT(beginNext == endNext);
				}

				if (shard->assigned() && data->shardAware) {
					TraceEvent(SevVerbose, "ValidateAssignedShard", data->thisServerID)
					    .detail("Range", shard->keys)
					    .detailf("ShardID", "%016llx", shard->shardId)
					    .detailf("DesiredShardID", "%016llx", shard->desiredShardId)
					    .detail("State", shard->debugDescribeState());
					ASSERT(shard->shardId != 0UL && shard->desiredShardId != 0UL);
				}
			}

			// FIXME: do some change feed validation?

			latest.validate();
			validateRange(latest, allKeys, data->version.get(), data->thisServerID, data->durableVersion.get());

			data->debug_lastValidateTime = now();
		}
	} catch (...) {
		TraceEvent(SevError, "ValidationFailure", data->thisServerID)
		    .detail("LastValidTime", data->debug_lastValidateTime);
		throw;
	}
}
#ifndef __INTEL_COMPILER
#pragma endregion
#endif

void updateProcessStats(StorageServer* self) {
	if (g_network->isSimulated()) {
		// diskUsage and cpuUsage are not relevant in the simulator,
		// and relying on the actual values could break seed determinism
		self->cpuUsage = 100.0;
		self->diskUsage = 100.0;
		return;
	}

	SystemStatistics sysStats = getSystemStatistics();
	if (sysStats.initialized) {
		self->cpuUsage = 100 * sysStats.processCPUSeconds / sysStats.elapsed;
		self->diskUsage = 100 * std::max(0.0, (sysStats.elapsed - sysStats.processDiskIdleSeconds) / sysStats.elapsed);
	}
}

///////////////////////////////////// Queries /////////////////////////////////
#ifndef __INTEL_COMPILER
#pragma region Queries
#endif

ACTOR Future<Version> waitForVersionActor(StorageServer* data, Version version, SpanContext spanContext) {
	state Span span("SS:WaitForVersion"_loc, spanContext);
	choose {
		when(wait(data->version.whenAtLeast(version))) {
			// FIXME: A bunch of these can block with or without the following delay 0.
			// wait( delay(0) );  // don't do a whole bunch of these at once
			if (version < data->oldestVersion.get()) {
				throw transaction_too_old(); // just in case
			}
			return version;
		}
		when(wait(delay(SERVER_KNOBS->FUTURE_VERSION_DELAY))) {
			if (deterministicRandom()->random01() < 0.001)
				TraceEvent(SevWarn, "ShardServerFutureVersion1000x", data->thisServerID)
				    .detail("Version", version)
				    .detail("MyVersion", data->version.get())
				    .detail("ServerID", data->thisServerID);
			throw future_version();
		}
	}
}

// If the latest commit version that mutated the shard(s) being served by the specified storage
// server is below the client specified read version then do a read at the latest commit version
// of the storage server.
Version getRealReadVersion(VersionVector& ssLatestCommitVersions, Tag& tag, Version specifiedReadVersion) {
	Version realReadVersion =
	    ssLatestCommitVersions.hasVersion(tag) ? ssLatestCommitVersions.getVersion(tag) : specifiedReadVersion;
	ASSERT(realReadVersion <= specifiedReadVersion);
	return realReadVersion;
}

// Find the latest commit version of the given tag.
Version getLatestCommitVersion(VersionVector& ssLatestCommitVersions, Tag& tag) {
	Version commitVersion =
	    ssLatestCommitVersions.hasVersion(tag) ? ssLatestCommitVersions.getVersion(tag) : invalidVersion;
	return commitVersion;
}

Future<Version> waitForVersion(StorageServer* data, Version version, SpanContext spanContext) {
	if (version == latestVersion) {
		version = std::max(Version(1), data->version.get());
	}

	if (version < data->oldestVersion.get() || version <= 0) {
		// TraceEvent(SevDebug, "WFVThrow", data->thisServerID).detail("Version", version).detail("OldestVersion", data->oldestVersion.get());
		return transaction_too_old();
	} else if (version <= data->version.get()) {
		return version;
	}

	if ((data->behind || data->versionBehind) && version > data->version.get()) {
		return process_behind();
	}

	if (deterministicRandom()->random01() < 0.001) {
		TraceEvent("WaitForVersion1000x").log();
	}
	return waitForVersionActor(data, version, spanContext);
}

Future<Version> waitForVersion(StorageServer* data,
                               Version commitVersion,
                               Version readVersion,
                               SpanContext spanContext) {
	ASSERT(commitVersion == invalidVersion || commitVersion < readVersion);

	if (commitVersion == invalidVersion) {
		return waitForVersion(data, readVersion, spanContext);
	}

	if (readVersion == latestVersion) {
		readVersion = std::max(Version(1), data->version.get());
	}

	if (readVersion < data->oldestVersion.get() || readVersion <= 0) {
		return transaction_too_old();
	} else {
		// It is correct to read any version between [commitVersion, readVersion],
		// because version vector guarantees no mutations between them.
		if (commitVersion < data->oldestVersion.get()) {
			if (data->version.get() < readVersion) {
				// Majority of the case, try using higher version to avoid
				// transaction_too_old error when oldestVersion advances.
				// BTW, any version in the range [oldestVersion, data->version.get()] is valid in this case.
				return data->version.get();
			} else {
				ASSERT(readVersion >= data->oldestVersion.get());
				return readVersion;
			}
		} else if (commitVersion <= data->version.get()) {
			return commitVersion;
		}
	}

	if ((data->behind || data->versionBehind) && commitVersion > data->version.get()) {
		return process_behind();
	}

	if (deterministicRandom()->random01() < 0.001) {
		TraceEvent("WaitForVersion1000x");
	}
	return waitForVersionActor(data, std::max(commitVersion, data->oldestVersion.get()), spanContext);
}

ACTOR Future<Version> waitForVersionNoTooOld(StorageServer* data, Version version) {
	// This could become an Actor transparently, but for now it just does the lookup
	if (version == latestVersion)
		version = std::max(Version(1), data->version.get());
	if (version <= data->version.get())
		return version;
	choose {
		when(wait(data->version.whenAtLeast(version))) {
			return version;
		}
		when(wait(delay(SERVER_KNOBS->FUTURE_VERSION_DELAY))) {
			if (deterministicRandom()->random01() < 0.001)
				TraceEvent(SevWarn, "ShardServerFutureVersion1000x", data->thisServerID)
				    .detail("Version", version)
				    .detail("MyVersion", data->version.get())
				    .detail("ServerID", data->thisServerID);
			throw future_version();
		}
	}
}

ACTOR Future<Version> waitForMinVersion(StorageServer* data, Version version) {
	// This could become an Actor transparently, but for now it just does the lookup
	if (version == latestVersion)
		version = std::max(Version(1), data->version.get());
	if (version < data->oldestVersion.get() || version <= 0) {
		return data->oldestVersion.get();
	} else if (version <= data->version.get()) {
		return version;
	}
	choose {
		when(wait(data->version.whenAtLeast(version))) {
			return version;
		}
		when(wait(delay(SERVER_KNOBS->FUTURE_VERSION_DELAY))) {
			if (deterministicRandom()->random01() < 0.001)
				TraceEvent(SevWarn, "ShardServerFutureVersion1000x", data->thisServerID)
				    .detail("Version", version)
				    .detail("MyVersion", data->version.get())
				    .detail("ServerID", data->thisServerID);
			throw future_version();
		}
	}
}

void StorageServer::checkTenantEntry(Version version, TenantInfo tenantInfo, bool lockAware) {
	if (tenantInfo.hasTenant()) {
		ASSERT(version == latestVersion || (version >= tenantMap.oldestVersion && version <= this->version.get()));
		auto view = tenantMap.at(version);
		auto itr = view.find(tenantInfo.tenantId);
		if (itr == view.end()) {
			TraceEvent(SevWarn, "StorageTenantNotFound", thisServerID)
			    .detail("Tenant", tenantInfo.tenantId)
			    .backtrace();
			CODE_PROBE(true, "Storage server tenant not found");
			throw tenant_not_found();
		} else if (!lockAware && itr->lockState == TenantAPI::TenantLockState::LOCKED) {
			CODE_PROBE(true, "Storage server access locked tenant without lock awareness");
			throw tenant_locked();
		}
	}
}

std::vector<StorageServerShard> StorageServer::getStorageServerShards(KeyRangeRef range) {
	std::vector<StorageServerShard> res;
	for (auto t : this->shards.intersectingRanges(range)) {
		res.push_back(t.value()->toStorageServerShard());
	}
	return res;
}

std::shared_ptr<MoveInShard> StorageServer::getMoveInShard(const UID& dataMoveId,
                                                           const Version version,
                                                           const ConductBulkLoad conductBulkLoad) {
	for (auto& [id, moveInShard] : this->moveInShards) {
		if (moveInShard->dataMoveId() == dataMoveId && moveInShard->meta->createVersion == version) {
			return moveInShard;
		}
	}

	const UID id = deterministicRandom()->randomUniqueID();
	std::shared_ptr<MoveInShard> shard = std::make_shared<MoveInShard>(this, id, dataMoveId, version, conductBulkLoad);
	auto [it, inserted] = this->moveInShards.emplace(id, shard);
	ASSERT(inserted);
	TraceEvent(SevDebug, "SSNewMoveInShard", this->thisServerID)
	    .detail("MoveInShard", shard->toString())
	    .detail("ConductBulkLoad", conductBulkLoad);
	return shard;
}

ACTOR Future<Void> getValueQ(StorageServer* data, GetValueRequest req) {
	state int64_t resultSize = 0;
	Span span("SS:getValue"_loc, req.spanContext);
	// Temporarily disabled -- this path is hit a lot
	// getCurrentLineage()->modify(&TransactionLineage::txID) = req.spanContext.first();

	try {
		++data->counters.getValueQueries;
		++data->counters.allQueries;
		if (req.key.startsWith(systemKeys.begin)) {
			++data->counters.systemKeyQueries;
		}
		data->maxQueryQueue = std::max<int>(
		    data->maxQueryQueue, data->counters.allQueries.getValue() - data->counters.finishedQueries.getValue());

		// Active load balancing runs at a very high priority (to obtain accurate queue lengths)
		// so we need to downgrade here
		wait(data->getQueryDelay());
		state PriorityMultiLock::Lock readLock = wait(data->getReadLock(req.options));

		// Track time from requestTime through now as read queueing wait time
		state double queueWaitEnd = g_network->timer();
		data->counters.readQueueWaitSample.addMeasurement(queueWaitEnd - req.requestTime());

		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent("GetValueDebug",
			                      req.options.get().debugID.get().first(),
			                      "getValueQ.DoRead"); //.detail("TaskID", g_network->getCurrentTask());

		state Optional<Value> v;
		Version commitVersion = getLatestCommitVersion(req.ssLatestCommitVersions, data->tag);
		state Version version = wait(waitForVersion(data, commitVersion, req.version, req.spanContext));
		data->counters.readVersionWaitSample.addMeasurement(g_network->timer() - queueWaitEnd);

		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent("GetValueDebug",
			                      req.options.get().debugID.get().first(),
			                      "getValueQ.AfterVersion"); //.detail("TaskID", g_network->getCurrentTask());

		data->checkTenantEntry(version, req.tenantInfo, req.options.present() ? req.options.get().lockAware : false);
		if (req.tenantInfo.hasTenant()) {
			req.key = req.key.withPrefix(req.tenantInfo.prefix.get());
		}
		state uint64_t changeCounter = data->shardChangeCounter;

		if (!data->shards[req.key]->isReadable()) {
			//TraceEvent("WrongShardServer", data->thisServerID).detail("Key", req.key).detail("Version", version).detail("In", "getValueQ");
			throw wrong_shard_server();
		}

		state int path = 0;
		auto i = data->data().at(version).lastLessOrEqual(req.key);
		if (i && i->isValue() && i.key() == req.key) {
			v = (Value)i->getValue();
			path = 1;
		} else if (!i || !i->isClearTo() || i->getEndKey() <= req.key) {
			path = 2;
			Optional<Value> vv = wait(data->storage.readValue(req.key, req.options));
			data->counters.kvGetBytes += vv.expectedSize();
			// Validate that while we were reading the data we didn't lose the version or shard
			if (version < data->storageVersion()) {
				CODE_PROBE(true, "transaction_too_old after readValue");
				throw transaction_too_old();
			}
			data->checkChangeCounter(changeCounter, req.key);
			v = vv;
		}

		DEBUG_MUTATION("ShardGetValue",
		               version,
		               MutationRef(MutationRef::DebugKey, req.key, v.present() ? v.get() : "<null>"_sr),
		               data->thisServerID);
		DEBUG_MUTATION("ShardGetPath",
		               version,
		               MutationRef(MutationRef::DebugKey,
		                           req.key,
		                           path == 0   ? "0"_sr
		                           : path == 1 ? "1"_sr
		                                       : "2"_sr),
		               data->thisServerID);

		/*
		StorageMetrics m;
		m.bytesWrittenPerKSecond = req.key.size() + (v.present() ? v.get().size() : 0);
		m.iosPerKSecond = 1;
		data->metrics.notify(req.key, m);
		*/

		if (v.present()) {
			++data->counters.rowsQueried;
			resultSize = v.get().size();
			data->counters.bytesQueried += resultSize;
		} else {
			++data->counters.emptyQueries;
		}

		if (SERVER_KNOBS->READ_SAMPLING_ENABLED) {
			// If the read yields no value, randomly sample the empty read.
			int64_t bytesReadPerKSecond =
			    v.present() ? std::max((int64_t)(req.key.size() + v.get().size()), SERVER_KNOBS->EMPTY_READ_PENALTY)
			                : SERVER_KNOBS->EMPTY_READ_PENALTY;
			data->metrics.notifyBytesReadPerKSecond(req.key, bytesReadPerKSecond);
		}

		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent("GetValueDebug",
			                      req.options.get().debugID.get().first(),
			                      "getValueQ.AfterRead"); //.detail("TaskID", g_network->getCurrentTask());

		// Check if the desired key might be cached
		auto cached = data->cachedRangeMap[req.key];
		// if (cached)
		//	TraceEvent(SevDebug, "SSGetValueCached").detail("Key", req.key);

		GetValueReply reply(v, cached);
		reply.penalty = data->getPenalty();
		req.reply.send(reply);
	} catch (Error& e) {
		if (!canReplyWith(e))
			throw;
		data->sendErrorWithPenalty(req.reply, e, data->getPenalty());
	}

	// Key size is not included in "BytesQueried", but still contributes to cost,
	// so it must be accounted for here.
	data->transactionTagCounter.addRequest(req.tags, req.key.size() + resultSize);

	++data->counters.finishedQueries;

	double duration = g_network->timer() - req.requestTime();
	data->counters.readLatencySample.addMeasurement(duration);
	data->counters.readValueLatencySample.addMeasurement(duration);
	if (data->latencyBandConfig.present()) {
		int maxReadBytes =
		    data->latencyBandConfig.get().readConfig.maxReadBytes.orDefault(std::numeric_limits<int>::max());
		data->counters.readLatencyBands.addMeasurement(duration, 1, Filtered(resultSize > maxReadBytes));
	}

	return Void();
}

// Pessimistic estimate the number of overhead bytes used by each
// watch. Watch key references are stored in an AsyncMap<Key,bool>, and actors
// must be kept alive until the watch is finished.
extern size_t WATCH_OVERHEAD_WATCHQ, WATCH_OVERHEAD_WATCHIMPL;

ACTOR Future<Version> watchWaitForValueChange(StorageServer* data, SpanContext parent, KeyRef key, int64_t tenantId) {
	state Location spanLocation = "SS:watchWaitForValueChange"_loc;
	state Span span(spanLocation, parent);
	state Reference<ServerWatchMetadata> metadata = data->getWatchMetadata(key, tenantId);
	if (metadata->debugID.present())
		g_traceBatch.addEvent("WatchValueDebug",
		                      metadata->debugID.get().first(),
		                      "watchValueSendReply.Before"); //.detail("TaskID", g_network->getCurrentTask());

	state Version originalMetadataVersion = metadata->version;
	wait(success(waitForVersionNoTooOld(data, metadata->version)));
	if (metadata->debugID.present())
		g_traceBatch.addEvent("WatchValueDebug",
		                      metadata->debugID.get().first(),
		                      "watchValueSendReply.AfterVersion"); //.detail("TaskID", g_network->getCurrentTask());

	state Version minVersion = data->data().latestVersion;
	state Future<Void> watchFuture = data->watches.onChange(metadata->key);
	if (tenantId != TenantInfo::INVALID_TENANT) {
		watchFuture = watchFuture || data->tenantWatches.onChange(tenantId);
	}
	state ReadOptions options;
	loop {
		try {
			if (tenantId != TenantInfo::INVALID_TENANT) {
				auto view = data->tenantMap.at(latestVersion);
				if (view.find(tenantId) == view.end()) {
					CODE_PROBE(true, "Watched tenant removed");
					throw tenant_removed();
				}
			}
			metadata = data->getWatchMetadata(key, tenantId);
			state Version latest = data->version.get();
			options.debugID = metadata->debugID;

			CODE_PROBE(latest >= minVersion && latest < data->data().latestVersion,
			           "Starting watch loop with latestVersion > data->version",
			           probe::decoration::rare);
			GetValueRequest getReq(
			    span.context, TenantInfo(), metadata->key, latest, metadata->tags, options, VersionVector());
			state Future<Void> getValue = getValueQ(
			    data, getReq); // we are relying on the delay zero at the top of getValueQ, if removed we need one here
			GetValueReply reply = wait(getReq.reply.getFuture());
			span = Span(spanLocation, parent);

			if (reply.error.present()) {
				ASSERT(reply.error.get().code() != error_code_future_version);
				throw reply.error.get();
			}
			if (BUGGIFY) {
				throw transaction_too_old();
			}

			DEBUG_MUTATION("ShardWatchValue",
			               latest,
			               MutationRef(MutationRef::DebugKey,
			                           metadata->key,
			                           reply.value.present() ? StringRef(reply.value.get()) : "<null>"_sr),
			               data->thisServerID);

			if (metadata->debugID.present())
				g_traceBatch.addEvent(
				    "WatchValueDebug",
				    metadata->debugID.get().first(),
				    "watchValueSendReply.AfterRead"); //.detail("TaskID", g_network->getCurrentTask());

			// If the version we read is less than minVersion, then we may fail to be notified of any changes that occur
			// up to or including minVersion. To prevent that, we'll check the key again once the version reaches our
			// minVersion.
			Version waitVersion = minVersion;
			if (reply.value != metadata->value) {
				if (latest >= metadata->version) {
					return latest; // fire watch
				} else if (metadata->version > originalMetadataVersion) {
					// another watch came in and raced in case 2 and updated the version. simply just wait and read
					// again at the higher version to confirm
					CODE_PROBE(true, "racing watches for same value at different versions", probe::decoration::rare);
					if (metadata->version > waitVersion) {
						waitVersion = metadata->version;
					}
				}
			}

			if (data->watchBytes > SERVER_KNOBS->MAX_STORAGE_SERVER_WATCH_BYTES) {
				CODE_PROBE(true, "Too many watches, reverting to polling");
				throw watch_cancelled();
			}

			state int64_t watchBytes =
			    (metadata->key.expectedSize() + metadata->value.expectedSize() + key.expectedSize() +
			     sizeof(Reference<ServerWatchMetadata>) + sizeof(ServerWatchMetadata) + WATCH_OVERHEAD_WATCHIMPL);

			data->watchBytes += watchBytes;
			try {
				if (latest < waitVersion) {
					// if we need to wait for a higher version because of a race, wait for that version
					watchFuture = watchFuture || data->version.whenAtLeast(waitVersion);
				}
				if (BUGGIFY) {
					// Simulate a trigger on the watch that results in the loop going around without the value changing
					watchFuture = watchFuture || delay(deterministicRandom()->random01());
				}

				if (metadata->debugID.present())
					g_traceBatch.addEvent(
					    "WatchValueDebug", metadata->debugID.get().first(), "watchValueSendReply.WaitChange");
				wait(watchFuture);
				data->watchBytes -= watchBytes;
			} catch (Error& e) {
				data->watchBytes -= watchBytes;
				throw;
			}
		} catch (Error& e) {
			if (e.code() != error_code_transaction_too_old) {
				throw e;
			}

			CODE_PROBE(true, "Reading a watched key failed with transaction_too_old");
		}

		watchFuture = data->watches.onChange(metadata->key);
		if (tenantId != TenantInfo::INVALID_TENANT) {
			watchFuture = watchFuture || data->tenantWatches.onChange(tenantId);
		}

		wait(data->version.whenAtLeast(data->data().latestVersion));
	}
}

void checkCancelWatchImpl(StorageServer* data, WatchValueRequest req) {
	Reference<ServerWatchMetadata> metadata = data->getWatchMetadata(req.key.contents(), req.tenantInfo.tenantId);
	if (metadata.isValid() && metadata->versionPromise.getFutureReferenceCount() == 1) {
		// last watch timed out so cancel watch_impl and delete key from the map
		data->deleteWatchMetadata(req.key.contents(), req.tenantInfo.tenantId);
		metadata->watch_impl.cancel();
	}
}

ACTOR Future<Void> watchValueSendReply(StorageServer* data,
                                       WatchValueRequest req,
                                       Future<Version> resp,
                                       SpanContext spanContext) {
	state Span span("SS:watchValue"_loc, spanContext);
	state double startTime = now();
	++data->counters.watchQueries;
	++data->numWatches;
	data->watchBytes += WATCH_OVERHEAD_WATCHQ;

	loop {
		double timeoutDelay = -1;
		if (data->noRecentUpdates.get()) {
			timeoutDelay = std::max(CLIENT_KNOBS->FAST_WATCH_TIMEOUT - (now() - startTime), 0.0);
		} else if (!BUGGIFY) {
			timeoutDelay = std::max(CLIENT_KNOBS->WATCH_TIMEOUT - (now() - startTime), 0.0);
		}

		try {
			choose {
				when(Version ver = wait(resp)) {
					// fire watch
					req.reply.send(WatchValueReply{ ver });
					checkCancelWatchImpl(data, req);
					--data->numWatches;
					data->watchBytes -= WATCH_OVERHEAD_WATCHQ;
					return Void();
				}
				when(wait(timeoutDelay < 0 ? Never() : delay(timeoutDelay))) {
					// watch timed out
					data->sendErrorWithPenalty(req.reply, timed_out(), data->getPenalty());
					checkCancelWatchImpl(data, req);
					--data->numWatches;
					data->watchBytes -= WATCH_OVERHEAD_WATCHQ;
					return Void();
				}
				when(wait(data->noRecentUpdates.onChange())) {}
			}
		} catch (Error& e) {
			data->watchBytes -= WATCH_OVERHEAD_WATCHQ;
			checkCancelWatchImpl(data, req);
			--data->numWatches;

			if (!canReplyWith(e))
				throw e;
			data->sendErrorWithPenalty(req.reply, e, data->getPenalty());
			return Void();
		}
	}
}

// Finds a checkpoint.
ACTOR Future<Void> getCheckpointQ(StorageServer* self, GetCheckpointRequest req) {
	// Wait until the desired version is durable.
	wait(self->durableVersion.whenAtLeast(req.version + 1));

	TraceEvent(SevDebug, "ServeGetCheckpointVersionSatisfied", self->thisServerID)
	    .detail("Version", req.version)
	    .detail("Ranges", describe(req.ranges))
	    .detail("Format", static_cast<int>(req.format));
	ASSERT(req.ranges.size() == 1);
	for (const auto& range : req.ranges) {
		if (!self->isReadable(range)) {
			req.reply.sendError(wrong_shard_server());
			return Void();
		}
	}

	try {
		std::unordered_map<UID, CheckpointMetaData>::iterator it = self->checkpoints.begin();
		for (; it != self->checkpoints.end(); ++it) {
			const CheckpointMetaData& md = it->second;
			if (md.version == req.version && md.format == req.format && req.actionId == md.actionId &&
			    md.hasRanges(req.ranges) && md.getState() == CheckpointMetaData::Complete) {
				req.reply.send(md);
				TraceEvent(SevDebug, "ServeGetCheckpointEnd", self->thisServerID).detail("Checkpoint", md.toString());
				break;
			}
		}

		if (it == self->checkpoints.end()) {
			req.reply.sendError(checkpoint_not_found());
		}
	} catch (Error& e) {
		if (!canReplyWith(e)) {
			throw;
		}
		req.reply.sendError(e);
	}
	return Void();
}

// Delete the checkpoint from disk, as well as all related persisted meta data.
ACTOR Future<Void> deleteCheckpointQ(StorageServer* self, Version version, CheckpointMetaData checkpoint) {
	wait(delay(0, TaskPriority::Low));

	wait(self->durableVersion.whenAtLeast(version));

	TraceEvent(SevInfo, "DeleteCheckpointBegin", self->thisServerID).detail("Checkpoint", checkpoint.toString());

	self->checkpoints.erase(checkpoint.checkpointID);

	try {
		wait(deleteCheckpoint(checkpoint));
	} catch (Error& e) {
		// TODO: Handle errors more gracefully.
		throw;
	}

	state Key persistCheckpointKey(persistCheckpointKeys.begin.toString() + checkpoint.checkpointID.toString());
	state Key pendingCheckpointKey(persistPendingCheckpointKeys.begin.toString() + checkpoint.checkpointID.toString());
	auto& mLV = self->addVersionToMutationLog(self->data().getLatestVersion());
	self->addMutationToMutationLog(
	    mLV, MutationRef(MutationRef::ClearRange, pendingCheckpointKey, keyAfter(pendingCheckpointKey)));
	self->addMutationToMutationLog(
	    mLV, MutationRef(MutationRef::ClearRange, persistCheckpointKey, keyAfter(persistCheckpointKey)));
	TraceEvent(SevInfo, "DeleteCheckpointEnd", self->thisServerID).detail("Checkpoint", checkpoint.toString());

	return Void();
}

// Serves FetchCheckpointRequests.
ACTOR Future<Void> fetchCheckpointQ(StorageServer* self, FetchCheckpointRequest req) {
	TraceEvent("ServeFetchCheckpointBegin", self->thisServerID)
	    .detail("CheckpointID", req.checkpointID)
	    .detail("Token", req.token);

	state ICheckpointReader* reader = nullptr;
	state int64_t totalSize = 0;

	req.reply.setByteLimit(SERVER_KNOBS->CHECKPOINT_TRANSFER_BLOCK_BYTES);

	// Returns error is the checkpoint cannot be found.
	const auto it = self->checkpoints.find(req.checkpointID);
	if (it == self->checkpoints.end()) {
		req.reply.sendError(checkpoint_not_found());
		TraceEvent("ServeFetchCheckpointNotFound", self->thisServerID).detail("CheckpointID", req.checkpointID);
		return Void();
	}

	try {
		reader = newCheckpointReader(it->second, CheckpointAsKeyValues::False, deterministicRandom()->randomUniqueID());
		wait(reader->init(req.token));

		loop {
			state Standalone<StringRef> data = wait(reader->nextChunk(CLIENT_KNOBS->REPLY_BYTE_LIMIT));
			wait(req.reply.onReady());
			FetchCheckpointReply reply(req.token);
			reply.data = data;
			req.reply.send(reply);
			totalSize += data.size();
		}
	} catch (Error& e) {
		if (e.code() == error_code_end_of_stream || e.code() == error_code_checkpoint_not_found) {
			req.reply.sendError(e);
			TraceEvent("ServeFetchCheckpointEnd", self->thisServerID)
			    .error(e)
			    .detail("CheckpointID", req.checkpointID)
			    .detail("TotalSize", totalSize)
			    .detail("Token", req.token);
		} else if (e.code() != error_code_operation_obsolete) {
			TraceEvent(SevWarnAlways, "ServerFetchCheckpointFailure")
			    .errorUnsuppressed(e)
			    .detail("CheckpointID", req.checkpointID)
			    .detail("Token", req.token);
			if (canReplyWith(e)) {
				req.reply.sendError(e);
			}
			state Error err = e;
			if (reader != nullptr) {
				wait(reader->close());
			}
			throw err;
		}
	}

	wait(reader->close());
	return Void();
}

// Serves FetchCheckpointKeyValuesRequest, reads local checkpoint and sends it to the client over wire.
ACTOR Future<Void> fetchCheckpointKeyValuesQ(StorageServer* self, FetchCheckpointKeyValuesRequest req) {
	wait(self->serveFetchCheckpointParallelismLock.take(TaskPriority::DefaultYield));
	state FlowLock::Releaser holder(self->serveFetchCheckpointParallelismLock);

	TraceEvent("ServeFetchCheckpointKeyValuesBegin", self->thisServerID)
	    .detail("CheckpointID", req.checkpointID)
	    .detail("Range", req.range);

	req.reply.setByteLimit(SERVER_KNOBS->CHECKPOINT_TRANSFER_BLOCK_BYTES);

	// Returns error if the checkpoint cannot be found.
	const auto it = self->checkpoints.find(req.checkpointID);
	if (it == self->checkpoints.end()) {
		req.reply.sendError(checkpoint_not_found());
		TraceEvent("ServeFetchCheckpointNotFound", self->thisServerID).detail("CheckpointID", req.checkpointID);
		return Void();
	}

	state ICheckpointReader* reader = nullptr;
	auto crIt = self->liveCheckpointReaders.find(req.checkpointID);
	if (crIt != self->liveCheckpointReaders.end()) {
		reader = crIt->second;
	} else {
		reader = newCheckpointReader(it->second, CheckpointAsKeyValues::True, deterministicRandom()->randomUniqueID());
		self->liveCheckpointReaders[req.checkpointID] = reader;
	}

	state std::unique_ptr<ICheckpointIterator> iter;
	try {
		wait(reader->init(BinaryWriter::toValue(req.range, IncludeVersion())));
		iter = reader->getIterator(req.range);

		loop {
			state RangeResult res =
			    wait(iter->nextBatch(CLIENT_KNOBS->REPLY_BYTE_LIMIT, CLIENT_KNOBS->REPLY_BYTE_LIMIT));
			if (!res.empty()) {
				TraceEvent(SevDebug, "FetchCheckpontKeyValuesReadRange", self->thisServerID)
				    .detail("CheckpointID", req.checkpointID)
				    .detail("FirstReturnedKey", res.front().key)
				    .detail("LastReturnedKey", res.back().key)
				    .detail("Size", res.size());
			} else {
				TraceEvent(SevInfo, "FetchCheckpontKeyValuesEmptyRange", self->thisServerID)
				    .detail("CheckpointID", req.checkpointID);
			}

			wait(req.reply.onReady());
			FetchCheckpointKeyValuesStreamReply reply;
			reply.arena.dependsOn(res.arena());
			for (int i = 0; i < res.size(); ++i) {
				reply.data.push_back(reply.arena, res[i]);
			}

			req.reply.send(reply);
		}
	} catch (Error& e) {
		if (e.code() == error_code_end_of_stream || e.code() == error_code_checkpoint_not_found) {
			req.reply.sendError(e);
			TraceEvent(SevInfo, "ServeFetchCheckpointKeyValuesEnd", self->thisServerID)
			    .error(e)
			    .detail("CheckpointID", req.checkpointID)
			    .detail("Range", req.range);
		} else {
			TraceEvent(SevWarnAlways, "ServerFetchCheckpointKeyValuesFailure")
			    .errorUnsuppressed(e)
			    .detail("CheckpointID", req.checkpointID)
			    .detail("Range", req.range);
			if (canReplyWith(e)) {
				req.reply.sendError(e);
			}
		}
	}

	iter.reset();
	if (!reader->inUse()) {
		self->liveCheckpointReaders.erase(req.checkpointID);
		wait(reader->close());
	}
	return Void();
}

ACTOR Future<Void> overlappingChangeFeedsQ(StorageServer* data, OverlappingChangeFeedsRequest req) {
	wait(delay(0));
	try {
		wait(success(waitForVersionNoTooOld(data, req.minVersion)));
	} catch (Error& e) {
		if (!canReplyWith(e))
			throw;
		req.reply.sendError(e);
		return Void();
	}

	if (!data->isReadable(req.range)) {
		req.reply.sendError(wrong_shard_server());
		return Void();
	}

	Version metadataWaitVersion = invalidVersion;

	auto ranges = data->keyChangeFeed.intersectingRanges(req.range);
	std::map<Key, std::tuple<KeyRange, Version, Version, Version>> rangeIds;
	for (auto r : ranges) {
		for (auto& it : r.value()) {
			if (!it->removing) {
				// Can't tell other SS about a change feed create or stopVersion that may get rolled back, and we only
				// need to tell it about the metadata if req.minVersion > metadataVersion, since it will get the
				// information from its own private mutations if it hasn't processed up that version yet
				metadataWaitVersion = std::max(metadataWaitVersion, it->metadataCreateVersion);

				// don't wait for all it->metadataVersion updates, if metadata was fetched from elsewhere it's already
				// durable, and some updates are unnecessary to wait for
				Version stopVersion;
				if (it->stopVersion != MAX_VERSION && req.minVersion > it->stopVersion) {
					stopVersion = it->stopVersion;
					metadataWaitVersion = std::max(metadataWaitVersion, stopVersion);
				} else {
					stopVersion = MAX_VERSION;
				}

				rangeIds[it->id] = std::tuple(it->range, it->emptyVersion, stopVersion, it->metadataVersion);
			} else if (it->destroyed && it->metadataVersion > metadataWaitVersion) {
				// if we communicate the lack of a change feed because it's destroying, ensure the feed destroy isn't
				// rolled back first
				CODE_PROBE(true, "Overlapping Change Feeds ensuring destroy isn't rolled back");
				metadataWaitVersion = it->metadataVersion;
			}
		}
	}
	state OverlappingChangeFeedsReply reply;
	reply.feedMetadataVersion = data->version.get();
	for (auto& it : rangeIds) {
		reply.feeds.push_back_deep(reply.arena,
		                           OverlappingChangeFeedEntry(it.first,
		                                                      std::get<0>(it.second),
		                                                      std::get<1>(it.second),
		                                                      std::get<2>(it.second),
		                                                      std::get<3>(it.second)));
		TraceEvent(SevDebug, "OverlappingChangeFeedEntry", data->thisServerID)
		    .detail("MinVersion", req.minVersion)
		    .detail("FeedID", it.first)
		    .detail("Range", std::get<0>(it.second))
		    .detail("EmptyVersion", std::get<1>(it.second))
		    .detail("StopVersion", std::get<2>(it.second))
		    .detail("FeedMetadataVersion", std::get<3>(it.second));
	}

	// Make sure all of the metadata we are sending won't get rolled back
	if (metadataWaitVersion != invalidVersion && metadataWaitVersion > data->desiredOldestVersion.get()) {
		CODE_PROBE(true, "overlapping change feeds waiting for metadata version to be safe from rollback");
		wait(data->desiredOldestVersion.whenAtLeast(metadataWaitVersion));
	}
	req.reply.send(reply);
	return Void();
}

MutationsAndVersionRef filterMutations(Arena& arena,
                                       EncryptedMutationsAndVersionRef const& m,
                                       KeyRange const& range,
                                       bool encrypted,
                                       int commonPrefixLength) {
	if (m.mutations.size() == 1 && m.mutations.back().param1 == lastEpochEndPrivateKey) {
		return MutationsAndVersionRef(m.mutations, m.version, m.knownCommittedVersion);
	}

	Optional<VectorRef<MutationRef>> modifiedMutations;
	for (int i = 0; i < m.mutations.size(); i++) {
		if (m.mutations[i].type == MutationRef::SetValue) {
			bool inRange = range.begin.compareSuffix(m.mutations[i].param1, commonPrefixLength) <= 0 &&
			               m.mutations[i].param1.compareSuffix(range.end, commonPrefixLength) < 0;
			if (modifiedMutations.present() && inRange) {
				modifiedMutations.get().push_back(
				    arena, encrypted && m.encrypted.present() ? m.encrypted.get()[i] : m.mutations[i]);
			}
			if (!modifiedMutations.present() && !inRange) {
				if (encrypted && m.encrypted.present()) {
					modifiedMutations = m.encrypted.get().slice(0, i);
				} else {
					modifiedMutations = m.mutations.slice(0, i);
				}
				arena.dependsOn(range.arena());
			}
		} else {
			ASSERT(m.mutations[i].type == MutationRef::ClearRange);
			// param1 < range.begin || param2 > range.end
			if (!modifiedMutations.present() &&
			    (m.mutations[i].param1.compareSuffix(range.begin, commonPrefixLength) < 0 ||
			     m.mutations[i].param2.compareSuffix(range.end, commonPrefixLength) > 0)) {
				if (encrypted && m.encrypted.present()) {
					modifiedMutations = m.encrypted.get().slice(0, i);
				} else {
					modifiedMutations = m.mutations.slice(0, i);
				}
				arena.dependsOn(range.arena());
			}
			if (modifiedMutations.present()) {
				// param1 < range.end && range.begin < param2
				if (m.mutations[i].param1.compareSuffix(range.end, commonPrefixLength) < 0 &&
				    range.begin.compareSuffix(m.mutations[i].param2, commonPrefixLength) < 0) {
					StringRef clearBegin = m.mutations[i].param1;
					StringRef clearEnd = m.mutations[i].param2;
					bool modified = false;
					if (clearBegin.compareSuffix(range.begin, commonPrefixLength) < 0) {
						clearBegin = range.begin;
						modified = true;
					}
					if (range.end.compareSuffix(clearEnd, commonPrefixLength) < 0) {
						clearEnd = range.end;
						modified = true;
					}
					if (modified) {
						MutationRef clearMutation = MutationRef(MutationRef::ClearRange, clearBegin, clearEnd);
						if (encrypted && m.encrypted.present() && m.encrypted.get()[i].isEncrypted()) {
							clearMutation = clearMutation.encrypt(m.cipherKeys[i], arena, BlobCipherMetrics::TLOG);
						}
						modifiedMutations.get().push_back(arena, clearMutation);
					} else {
						modifiedMutations.get().push_back(
						    arena, encrypted && m.encrypted.present() ? m.encrypted.get()[i] : m.mutations[i]);
					}
				}
			}
		}
	}
	if (modifiedMutations.present()) {
		return MutationsAndVersionRef(modifiedMutations.get(), m.version, m.knownCommittedVersion);
	}
	if (!encrypted || !m.encrypted.present()) {
		return MutationsAndVersionRef(m.mutations, m.version, m.knownCommittedVersion);
	}
	return MutationsAndVersionRef(m.encrypted.get(), m.version, m.knownCommittedVersion);
}

// set this for VERY verbose logs on change feed SS reads
#define DEBUG_CF_TRACE false

// To easily find if a change feed read missed data. Set the CF to the feedId, the key to the missing key, and the
// version to the version the mutation is missing at.
#define DO_DEBUG_CF_MISSING false
#define DEBUG_CF_MISSING_CF ""_sr
#define DEBUG_CF_MISSING_KEY ""_sr
#define DEBUG_CF_MISSING_VERSION invalidVersion
#define DEBUG_CF_MISSING(cfId, keyRange, beginVersion, lastVersion)                                                    \
	DO_DEBUG_CF_MISSING&& cfId.printable().substr(0, 6) ==                                                             \
	        DEBUG_CF_MISSING_CF&& keyRange.contains(DEBUG_CF_MISSING_KEY) &&                                           \
	    beginVersion <= DEBUG_CF_MISSING_VERSION&& lastVersion >= DEBUG_CF_MISSING_VERSION

// efficiently searches for the change feed mutation start point at begin version
static std::deque<Standalone<EncryptedMutationsAndVersionRef>>::const_iterator searchChangeFeedStart(
    std::deque<Standalone<EncryptedMutationsAndVersionRef>> const& mutations,
    Version beginVersion,
    bool atLatest) {

	if (mutations.empty() || beginVersion > mutations.back().version) {
		return mutations.end();
	} else if (beginVersion <= mutations.front().version) {
		return mutations.begin();
	}

	EncryptedMutationsAndVersionRef searchKey;
	searchKey.version = beginVersion;
	if (atLatest) {
		int jump = 1;
		// exponential search backwards, because atLatest means the new mutations are likely only at the very end
		auto lastEnd = mutations.end();
		auto currentEnd = mutations.end() - 1;
		while (currentEnd > mutations.begin()) {
			if (beginVersion >= currentEnd->version) {
				break;
			}
			lastEnd = currentEnd + 1;
			jump = std::min((int)(currentEnd - mutations.begin()), jump);
			currentEnd -= jump;
			jump <<= 1;
		}
		auto ret = std::lower_bound(currentEnd, lastEnd, searchKey, EncryptedMutationsAndVersionRef::OrderByVersion());
		// TODO REMOVE: for validation
		if (ret != mutations.end()) {
			if (ret->version < beginVersion) {
				fmt::print("ERROR: {0}) {1} < {2}\n", ret - mutations.begin(), ret->version, beginVersion);
			}
			ASSERT(ret->version >= beginVersion);
		}
		if (ret != mutations.begin()) {
			if ((ret - 1)->version >= beginVersion) {
				fmt::print("ERROR: {0}) {1} >= {2}\n", (ret - mutations.begin()) - 1, (ret - 1)->version, beginVersion);
			}
			ASSERT((ret - 1)->version < beginVersion);
		}
		return ret;
	} else {
		// binary search
		return std::lower_bound(
		    mutations.begin(), mutations.end(), searchKey, EncryptedMutationsAndVersionRef::OrderByVersion());
	}
}

// The normal read case for a change feed stream query is that it will first read the disk portion, which is at a lower
// version than the memory portion, and then will effectively switch to reading only the memory portion. The complexity
// lies in the fact that the feed does not know the switchover point ahead of time before reading from disk, and the
// switchover point is constantly changing as the SS persists the in-memory data to disk. As a result, the
// implementation first reads from memory, then reads from disk if necessary, then merges the result and potentially
// discards the in-memory read data if the disk data is large and behind the in-memory data. The goal of
// FeedDiskReadState is that we want to skip doing the full memory read if we still have a lot of disk reads to catch up
// on. In the DISK_CATCHUP phase, the feed query will read only the first row from memory, to
// determine if it's hit the switchover point, instead of reading (potentially) both in the normal phase.  We also want
// to default to the normal behavior at the start in case there is not a lot of disk data. This guarantees that if we
// somehow incorrectly went into DISK_CATCHUP when there wasn't much more data on disk, we only have one cycle of
// getChangeFeedMutations in the incorrect mode that returns a smaller result before switching to NORMAL mode.
//
// Put another way, the state transitions are:
//
// STARTING ->
//   DISK_CATCHUP (if after the first read, there is more disk data to read before the first memory data)
//   NORMAL (otherwise)
// DISK_CATCHUP ->
//   still DISK_CATCHUP (if there is still more disk data to read before the first memory data)
//   NORMAL (otherwise)
// NORMAL -> NORMAL (always)
enum FeedDiskReadState { STARTING, NORMAL, DISK_CATCHUP };

ACTOR Future<std::pair<ChangeFeedStreamReply, bool>> getChangeFeedMutations(StorageServer* data,
                                                                            Reference<ChangeFeedInfo> feedInfo,
                                                                            ChangeFeedStreamRequest req,
                                                                            bool atLatest,
                                                                            bool doFilterMutations,
                                                                            int commonFeedPrefixLength,
                                                                            FeedDiskReadState* feedDiskReadState) {
	state ChangeFeedStreamReply reply;
	state ChangeFeedStreamReply memoryReply;
	state int remainingLimitBytes = CLIENT_KNOBS->REPLY_BYTE_LIMIT;
	state int remainingDurableBytes = CLIENT_KNOBS->REPLY_BYTE_LIMIT;
	state Version startVersion = data->version.get();

	if (DEBUG_CF_TRACE) {
		TraceEvent(SevDebug, "TraceChangeFeedMutationsBegin", data->thisServerID)
		    .detail("FeedID", req.rangeID)
		    .detail("StreamUID", req.id)
		    .detail("Range", req.range)
		    .detail("Begin", req.begin)
		    .detail("End", req.end)
		    .detail("PeerAddr", req.reply.getEndpoint().getPrimaryAddress())
		    .detail("PeerAddress", req.reply.getEndpoint().getPrimaryAddress());
	}

	if (data->version.get() < req.begin) {
		wait(data->version.whenAtLeast(req.begin));
		// we must delay here to ensure that any up-to-date change feeds that are waiting on the
		// mutation trigger run BEFORE any blocked change feeds run, in order to preserve the
		// correct minStreamVersion ordering
		wait(delay(0));
	}

	state uint64_t changeCounter = data->shardChangeCounter;
	if (!data->isReadable(req.range)) {
		throw wrong_shard_server();
	}

	if (feedInfo->removing) {
		throw unknown_change_feed();
	}

	// We must copy the mutationDeque when fetching the durable bytes in case mutations are popped from memory while
	// waiting for the results
	state Version dequeVersion = data->version.get();
	state Version dequeKnownCommit = data->knownCommittedVersion.get();
	state Version emptyVersion = feedInfo->emptyVersion;
	state Version durableValidationVersion = std::min(data->durableVersion.get(), feedInfo->durableFetchVersion.get());
	state Version lastMemoryVersion = invalidVersion;
	state Version lastMemoryKnownCommitted = invalidVersion;
	Version fetchStorageVersion = std::max(feedInfo->fetchVersion, feedInfo->durableFetchVersion.get());
	state bool doValidation = EXPENSIVE_VALIDATION;

	if (DEBUG_CF_TRACE) {
		TraceEvent(SevDebug, "TraceChangeFeedMutationsDetails", data->thisServerID)
		    .detail("FeedID", req.rangeID)
		    .detail("StreamUID", req.id)
		    .detail("Range", req.range)
		    .detail("Begin", req.begin)
		    .detail("End", req.end)
		    .detail("AtLatest", atLatest)
		    .detail("DequeVersion", dequeVersion)
		    .detail("EmptyVersion", feedInfo->emptyVersion)
		    .detail("StorageVersion", feedInfo->storageVersion)
		    .detail("DurableVersion", feedInfo->durableVersion)
		    .detail("FetchStorageVersion", fetchStorageVersion)
		    .detail("FetchVersion", feedInfo->fetchVersion)
		    .detail("DurableFetchVersion", feedInfo->durableFetchVersion.get())
		    .detail("DurableValidationVersion", durableValidationVersion)
		    .detail("PeerAddr", req.reply.getEndpoint().getPrimaryAddress())
		    .detail("PeerAddress", req.reply.getEndpoint().getPrimaryAddress());
	}

	if (req.end > emptyVersion + 1) {
		auto it = searchChangeFeedStart(feedInfo->mutations, req.begin, atLatest);
		while (it != feedInfo->mutations.end()) {
			// If DISK_CATCHUP, only read 1 mutation from the memory queue
			if (it->version >= req.end || it->version > dequeVersion || remainingLimitBytes <= 0) {
				break;
			}
			if ((*feedDiskReadState) == FeedDiskReadState::DISK_CATCHUP && !memoryReply.mutations.empty()) {
				// so we don't add an empty mutation at the end
				remainingLimitBytes = -1;
				break;
			}

			// subtract size BEFORE filter, to avoid huge cpu loop and processing if very selective filter applied
			remainingLimitBytes -= sizeof(MutationsAndVersionRef) + it->expectedSize();

			MutationsAndVersionRef m;
			if (doFilterMutations) {
				m = filterMutations(memoryReply.arena, *it, req.range, req.encrypted, commonFeedPrefixLength);
			} else {
				m = MutationsAndVersionRef(req.encrypted && it->encrypted.present() ? it->encrypted.get()
				                                                                    : it->mutations,
				                           it->version,
				                           it->knownCommittedVersion);
			}
			if (m.mutations.size()) {
				memoryReply.arena.dependsOn(it->arena());
				memoryReply.mutations.push_back(memoryReply.arena, m);
			}

			lastMemoryVersion = m.version;
			lastMemoryKnownCommitted = m.knownCommittedVersion;
			it++;
		}
	}

	state bool readDurable = feedInfo->durableVersion != invalidVersion && req.begin <= feedInfo->durableVersion;
	state bool readFetched = req.begin <= fetchStorageVersion && !atLatest;
	state bool waitFetched = false;
	if (req.end > emptyVersion + 1 && (readDurable || readFetched)) {
		if (readFetched && req.begin <= feedInfo->fetchVersion) {
			waitFetched = true;
			// Request needs data that has been written to storage by a change feed fetch, but not committed yet
			// To not block fetchKeys making normal SS data readable on making change feed data written to storage, we
			// wait in here instead for all fetched data to become readable from the storage engine.
			ASSERT(req.begin <= feedInfo->fetchVersion);
			CODE_PROBE(true, "getChangeFeedMutations before fetched data durable");

			// Wait for next commit to write pending feed data to storage
			wait(feedInfo->durableFetchVersion.whenAtLeast(feedInfo->fetchVersion));
			// To let update storage finish
			wait(delay(0));
		}

		state PriorityMultiLock::Lock ssReadLock = wait(data->getReadLock(req.options));
		// The assumption of feed ordering is that all atLatest feeds will get processed before the !atLatest ones.
		// Without this delay(0), there is a case where that can happen:
		//  - a read request (eg getValueQ) has the read lock and is waiting on ss->version.whenAtLeast(V)
		//  - a getChangeFeedMutations is blocked on that read lock (which is necessarily not atLatest because it's
		//  reading from disk)
		//  - the ss calls version->set(V'), which triggers the read requests, which does not wait by reading only from
		//  the p-tree, and releases this lock.
		//  - the following storage read does not require waiting, and returns immediately to changeFeedStreamQ,
		//  triggering its reply with an incorrectly large minStreamVersion
		//  - the ss calls feed->triggerMutations(), triggering the atLatest feeds to reply with their minStreamVersion
		//  at the correct version
		// The delay(0) prevents this, blocking the rest of this execution until all feed triggers finish and the
		// storage update loop actor completes, making the sequence of minStreamVersions returned to the client valid.
		// The delay(0) is technically only necessary if we did not immediately acquire the lock, but isn't a big deal
		// to do always
		wait(delay(0));
		state RangeResult res = wait(
		    data->storage.readRange(KeyRangeRef(changeFeedDurableKey(req.rangeID, std::max(req.begin, emptyVersion)),
		                                        changeFeedDurableKey(req.rangeID, req.end)),
		                            1 << 30,
		                            remainingDurableBytes,
		                            req.options));
		ssReadLock.release();
		data->counters.kvScanBytes += res.logicalSize();
		++data->counters.changeFeedDiskReads;

		if (!req.range.empty()) {
			data->checkChangeCounter(changeCounter, req.range);
		}

		state std::vector<std::pair<Standalone<VectorRef<MutationRef>>, Version>> decodedMutations;
		std::unordered_set<BlobCipherDetails> cipherDetails;
		decodedMutations.reserve(res.size());
		for (auto& kv : res) {
			decodedMutations.push_back(decodeChangeFeedDurableValue(kv.value));
			if (doFilterMutations || !req.encrypted) {
				for (auto& m : decodedMutations.back().first) {
					ASSERT(data->encryptionMode.present());
					ASSERT(!data->encryptionMode.get().isEncryptionEnabled() || m.isEncrypted() ||
					       isBackupLogMutation(m) || mutationForKey(m, lastEpochEndPrivateKey));
					if (m.isEncrypted()) {
						m.updateEncryptCipherDetails(cipherDetails);
					}
				}
			}
		}

		state std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> cipherMap;
		if (cipherDetails.size()) {
			std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> getCipherKeysResult =
			    wait(GetEncryptCipherKeys<ServerDBInfo>::getEncryptCipherKeys(
			        data->db, cipherDetails, BlobCipherMetrics::TLOG));
			cipherMap = getCipherKeysResult;
		}

		int memoryVerifyIdx = 0;

		Version lastVersion = req.begin - 1;
		Version lastKnownCommitted = invalidVersion;
		for (int i = 0; i < res.size(); i++) {
			Key id;
			Version version, knownCommittedVersion;
			Standalone<VectorRef<MutationRef>> mutations;
			Standalone<VectorRef<MutationRef>> encryptedMutations;
			std::vector<TextAndHeaderCipherKeys> cipherKeys;
			std::tie(id, version) = decodeChangeFeedDurableKey(res[i].key);
			std::tie(encryptedMutations, knownCommittedVersion) = decodedMutations[i];
			cipherKeys.resize(encryptedMutations.size());

			if (doFilterMutations || !req.encrypted) {
				mutations.resize(mutations.arena(), encryptedMutations.size());
				for (int j = 0; j < encryptedMutations.size(); j++) {
					ASSERT(data->encryptionMode.present());
					ASSERT(!data->encryptionMode.get().isEncryptionEnabled() || encryptedMutations[j].isEncrypted() ||
					       isBackupLogMutation(encryptedMutations[j]) ||
					       mutationForKey(encryptedMutations[j], lastEpochEndPrivateKey));
					if (encryptedMutations[j].isEncrypted()) {
						cipherKeys[j] = encryptedMutations[j].getCipherKeys(cipherMap);
						mutations[j] =
						    encryptedMutations[j].decrypt(cipherKeys[j], mutations.arena(), BlobCipherMetrics::TLOG);
					} else {
						mutations[j] = encryptedMutations[j];
					}
				}
			} else {
				mutations = encryptedMutations;
			}

			// gap validation
			while (doValidation && memoryVerifyIdx < memoryReply.mutations.size() &&
			       version > memoryReply.mutations[memoryVerifyIdx].version) {
				if (req.canReadPopped) {
					// There are weird cases where SS fetching mixed with SS durability and popping can mean there are
					// gaps before the popped version temporarily
					memoryVerifyIdx++;
					continue;
				}

				// There is a case where this can happen - if we wait on a fetching change feed, and the feed is
				// popped while we wait, we could have copied the memory mutations into memoryReply before the
				// pop, but they may or may not have been skipped writing to disk
				if (waitFetched && feedInfo->emptyVersion > emptyVersion &&
				    memoryReply.mutations[memoryVerifyIdx].version <= feedInfo->emptyVersion) {
					memoryVerifyIdx++;
					continue;
				} else {
					fmt::print("ERROR: SS {0} CF {1} SQ {2} has mutation at {3} in memory but not on disk (next disk "
					           "is {4}) (emptyVersion={5}, emptyBefore={6})!\n",
					           data->thisServerID.toString().substr(0, 4),
					           req.rangeID.printable().substr(0, 6),
					           req.id.toString().substr(0, 8),
					           memoryReply.mutations[memoryVerifyIdx].version,
					           version,
					           feedInfo->emptyVersion,
					           emptyVersion);

					fmt::print("  Memory: ({})\n", memoryReply.mutations[memoryVerifyIdx].mutations.size());
					for (auto& it : memoryReply.mutations[memoryVerifyIdx].mutations) {
						if (it.type == MutationRef::SetValue) {
							fmt::print("    {}=\n", it.param1.printable());
						} else {
							fmt::print("    {} - {}\n", it.param1.printable(), it.param2.printable());
						}
					}
					ASSERT(false);
				}
			}

			MutationsAndVersionRef m;
			if (doFilterMutations) {
				m = filterMutations(reply.arena,
				                    EncryptedMutationsAndVersionRef(
				                        mutations, encryptedMutations, cipherKeys, version, knownCommittedVersion),
				                    req.range,
				                    req.encrypted,
				                    commonFeedPrefixLength);
			} else {
				m = MutationsAndVersionRef(
				    req.encrypted ? encryptedMutations : mutations, version, knownCommittedVersion);
			}
			if (m.mutations.size()) {
				reply.arena.dependsOn(mutations.arena());
				reply.arena.dependsOn(encryptedMutations.arena());
				reply.mutations.push_back(reply.arena, m);

				if (doValidation && memoryVerifyIdx < memoryReply.mutations.size() &&
				    version == memoryReply.mutations[memoryVerifyIdx].version) {
					// We could do validation of mutations here too, but it's complicated because clears can get split
					// and stuff
					memoryVerifyIdx++;
				}
			} else if (doValidation && memoryVerifyIdx < memoryReply.mutations.size() &&
			           version == memoryReply.mutations[memoryVerifyIdx].version) {
				if (version > durableValidationVersion) {
					// Another validation case - feed was popped, data was fetched, fetched data was persisted but pop
					// wasn't yet, then SS restarted. Now SS has the data without the popped version. This looks wrong
					// here but is fine.
					memoryVerifyIdx++;
				} else {
					fmt::print("ERROR: SS {0} CF {1} SQ {2} has mutation at {3} in memory but all filtered out on "
					           "disk! (durable validation = {4})\n",
					           data->thisServerID.toString().substr(0, 4),
					           req.rangeID.printable().substr(0, 6),
					           req.id.toString().substr(0, 8),
					           version,
					           durableValidationVersion);

					fmt::print("  Memory: ({})\n", memoryReply.mutations[memoryVerifyIdx].mutations.size());
					for (auto& it : memoryReply.mutations[memoryVerifyIdx].mutations) {
						if (it.type == MutationRef::SetValue) {
							fmt::print("    {}=\n", it.param1.printable().c_str());
						} else {
							fmt::print("    {} - {}\n", it.param1.printable().c_str(), it.param2.printable().c_str());
						}
					}
					fmt::print("  Disk(pre-filter): ({})\n", mutations.size());
					for (auto& it : mutations) {
						if (it.type == MutationRef::SetValue) {
							fmt::print("    {}=\n", it.param1.printable().c_str());
						} else {
							fmt::print("    {} - {}\n", it.param1.printable().c_str(), it.param2.printable().c_str());
						}
					}
					ASSERT_WE_THINK(false);
				}
			}
			remainingDurableBytes -=
			    sizeof(KeyValueRef) + res[i].expectedSize(); // This is tracking the size on disk rather than the reply
			                                                 // size because we cannot add mutations from memory if
			                                                 // there are potentially more on disk
			lastVersion = version;
			lastKnownCommitted = knownCommittedVersion;
		}

		if ((*feedDiskReadState) == FeedDiskReadState::STARTING ||
		    (*feedDiskReadState) == FeedDiskReadState::DISK_CATCHUP) {
			if (!memoryReply.mutations.empty() && !reply.mutations.empty() &&
			    reply.mutations.back().version < memoryReply.mutations.front().version && remainingDurableBytes <= 0) {
				// if we read a full batch from disk and the entire disk read was still less than the first memory
				// mutation, switch to disk_catchup mode
				*feedDiskReadState = FeedDiskReadState::DISK_CATCHUP;
				CODE_PROBE(true, "Feed switching to disk_catchup mode");
			} else {
				// for testing
				if ((*feedDiskReadState) == FeedDiskReadState::STARTING && BUGGIFY_WITH_PROB(0.001)) {
					*feedDiskReadState = FeedDiskReadState::DISK_CATCHUP;
					CODE_PROBE(true, "Feed forcing disk_catchup mode");
				} else {
					// else switch to normal mode
					CODE_PROBE(true, "Feed switching to normal mode");
					*feedDiskReadState = FeedDiskReadState::NORMAL;
				}
			}
		}

		if (remainingDurableBytes > 0) {
			reply.arena.dependsOn(memoryReply.arena);
			auto it = memoryReply.mutations.begin();
			int totalCount = memoryReply.mutations.size();
			while (it != memoryReply.mutations.end() && it->version <= lastVersion) {
				++it;
				--totalCount;
			}
			reply.mutations.append(reply.arena, it, totalCount);
			// If still empty, that means disk results were filtered out, but skipped all memory results. Add an empty,
			// either the last version from disk
			if (reply.mutations.empty()) {
				if (res.size() || (lastMemoryVersion != invalidVersion && remainingLimitBytes <= 0)) {
					CODE_PROBE(true, "Change feed adding empty version after disk + memory filtered");
					if (res.empty()) {
						lastVersion = lastMemoryVersion;
						lastKnownCommitted = lastMemoryKnownCommitted;
					}
					reply.mutations.push_back(reply.arena, MutationsAndVersionRef(lastVersion, lastKnownCommitted));
				}
			}
		} else if (reply.mutations.empty() || reply.mutations.back().version < lastVersion) {
			CODE_PROBE(true, "Change feed adding empty version after disk filtered");
			reply.mutations.push_back(reply.arena, MutationsAndVersionRef(lastVersion, lastKnownCommitted));
		}
	} else {
		reply = memoryReply;
		*feedDiskReadState = FeedDiskReadState::NORMAL;

		// if we processed memory results that got entirely or mostly filtered, but we're not caught up, add an empty at
		// the end
		if ((reply.mutations.empty() || reply.mutations.back().version < lastMemoryVersion) &&
		    remainingLimitBytes <= 0) {
			CODE_PROBE(true, "Memory feed adding empty version after memory filtered", probe::decoration::rare);
			reply.mutations.push_back(reply.arena, MutationsAndVersionRef(lastMemoryVersion, lastMemoryKnownCommitted));
		}
	}

	bool gotAll = remainingLimitBytes > 0 && remainingDurableBytes > 0 && data->version.get() == startVersion;
	Version finalVersion = std::min(req.end - 1, dequeVersion);
	if ((reply.mutations.empty() || reply.mutations.back().version < finalVersion) && remainingLimitBytes > 0 &&
	    remainingDurableBytes > 0) {
		CODE_PROBE(true, "Change feed adding empty version after empty results");
		reply.mutations.push_back(
		    reply.arena, MutationsAndVersionRef(finalVersion, finalVersion == dequeVersion ? dequeKnownCommit : 0));
		// if we add empty mutation after the last thing in memory, and didn't read from disk, gotAll is true
		if (data->version.get() == startVersion) {
			gotAll = true;
		}
	}

	// FIXME: clean all of this up, and just rely on client-side check
	// This check is done just before returning, after all waits in this function
	// Check if pop happened concurrently
	if (!req.canReadPopped && req.begin <= feedInfo->emptyVersion) {
		// This can happen under normal circumstances if this part of a change feed got no updates, but then the feed
		// was popped. We can check by confirming that the client was sent empty versions as part of another feed's
		// response's minStorageVersion, or a ChangeFeedUpdateRequest. If this was the case, we know no updates could
		// have happened between req.begin and minVersion.
		Version minVersion = data->minFeedVersionForAddress(req.reply.getEndpoint().getPrimaryAddress());
		bool ok = atLatest && minVersion > feedInfo->emptyVersion;
		CODE_PROBE(ok, "feed popped while valid read waiting");
		CODE_PROBE(!ok, "feed popped while invalid read waiting");
		if (!ok) {
			TraceEvent("ChangeFeedMutationsPopped", data->thisServerID)
			    .detail("FeedID", req.rangeID)
			    .detail("StreamUID", req.id)
			    .detail("Range", req.range)
			    .detail("Begin", req.begin)
			    .detail("End", req.end)
			    .detail("EmptyVersion", feedInfo->emptyVersion)
			    .detail("AtLatest", atLatest)
			    .detail("MinVersionSent", minVersion);
			// Disabling this check because it returns false positives when forcing a delta file flush at an empty
			// version that was not a mutation version throw change_feed_popped();
		}
	}

	if (MUTATION_TRACKING_ENABLED) {
		for (auto& mutations : reply.mutations) {
			for (auto& m : mutations.mutations) {
				DEBUG_MUTATION("ChangeFeedSSRead", mutations.version, m, data->thisServerID)
				    .detail("ChangeFeedID", req.rangeID)
				    .detail("StreamUID", req.id)
				    .detail("ReqBegin", req.begin)
				    .detail("ReqEnd", req.end)
				    .detail("ReqRange", req.range);
			}
		}
	}

	if (DEBUG_CF_MISSING(req.rangeID, req.range, req.begin, reply.mutations.back().version) && !req.canReadPopped) {
		bool foundVersion = false;
		bool foundKey = false;
		for (auto& it : reply.mutations) {
			if (it.version == DEBUG_CF_MISSING_VERSION) {
				foundVersion = true;
				for (auto& m : it.mutations) {
					if (m.type == MutationRef::SetValue && m.param1 == DEBUG_CF_MISSING_KEY) {
						foundKey = true;
						break;
					}
				}
				break;
			}
		}
		if (!foundVersion || !foundKey) {
			fmt::print("ERROR: SS {0} CF {1} SQ {2} missing {3} @ {4} from request for [{5} - {6}) {7} - {8}\n",
			           data->thisServerID.toString().substr(0, 4),
			           req.rangeID.printable().substr(0, 6),
			           req.id.toString().substr(0, 8),
			           foundVersion ? "key" : "version",
			           static_cast<int64_t>(DEBUG_CF_MISSING_VERSION),
			           req.range.begin.printable(),
			           req.range.end.printable(),
			           req.begin,
			           req.end);
			fmt::print("ERROR: {0} versions in response {1} - {2}:\n",
			           reply.mutations.size(),
			           reply.mutations.front().version,
			           reply.mutations.back().version);
			for (auto& it : reply.mutations) {
				fmt::print("ERROR:    {0} ({1}){2}\n",
				           it.version,
				           it.mutations.size(),
				           it.version == DEBUG_CF_MISSING_VERSION ? "<-------" : "");
			}
		} else {
			fmt::print("DBG: SS {0} CF {1} SQ {2} correct @ {3} from request for [{4} - {5}) {6} - {7}\n",
			           data->thisServerID.toString().substr(0, 4),
			           req.rangeID.printable().substr(0, 6),
			           req.id.toString().substr(0, 8),
			           static_cast<int64_t>(DEBUG_CF_MISSING_VERSION),
			           req.range.begin.printable(),
			           req.range.end.printable(),
			           req.begin,
			           req.end);
		}
	}

	reply.popVersion = feedInfo->emptyVersion + 1;

	if (DEBUG_CF_TRACE) {
		TraceEvent(SevDebug, "ChangeFeedMutationsDone", data->thisServerID)
		    .detail("FeedID", req.rangeID)
		    .detail("StreamUID", req.id)
		    .detail("Range", req.range)
		    .detail("Begin", req.begin)
		    .detail("End", req.end)
		    .detail("FirstVersion", reply.mutations.empty() ? invalidVersion : reply.mutations.front().version)
		    .detail("LastVersion", reply.mutations.empty() ? invalidVersion : reply.mutations.back().version)
		    .detail("PopVersion", reply.popVersion)
		    .detail("Count", reply.mutations.size())
		    .detail("GotAll", gotAll)
		    .detail("PeerAddr", req.reply.getEndpoint().getPrimaryAddress())
		    .detail("PeerAddress", req.reply.getEndpoint().getPrimaryAddress());
	}

	// If the SS's version advanced at all during any of the waits, the read from memory may have missed some
	// mutations, so gotAll can only be true if data->version didn't change over the course of this actor
	return std::make_pair(reply, gotAll);
}

// Change feed stream must be sent an error as soon as it is moved away, or change feed can get incorrect results
ACTOR Future<Void> stopChangeFeedOnMove(StorageServer* data, ChangeFeedStreamRequest req) {
	auto feed = data->uidChangeFeed.find(req.rangeID);
	if (feed == data->uidChangeFeed.end() || feed->second->removing) {
		req.reply.sendError(unknown_change_feed());
		return Void();
	}
	state Promise<Void> moved;
	feed->second->triggerOnMove(req.range, req.id, moved);
	try {
		wait(moved.getFuture());
	} catch (Error& e) {
		ASSERT(e.code() == error_code_operation_cancelled);
		// remove from tracking

		auto feed = data->uidChangeFeed.find(req.rangeID);
		if (feed != data->uidChangeFeed.end()) {
			feed->second->removeOnMoveTrigger(req.range, req.id);
		}
		return Void();
	}
	CODE_PROBE(true, "Change feed moved away cancelling queries");
	// DO NOT call req.reply.onReady before sending - we need to propagate this error through regardless of how far
	// behind client is
	req.reply.sendError(wrong_shard_server());
	return Void();
}

ACTOR Future<Void> changeFeedStreamQ(StorageServer* data, ChangeFeedStreamRequest req) {
	state Span span("SS:getChangeFeedStream"_loc, req.spanContext);
	state bool atLatest = false;
	state bool removeUID = false;
	state FeedDiskReadState feedDiskReadState = STARTING;
	state Optional<Version> blockedVersion;
	state Reference<ChangeFeedInfo> feedInfo;
	state Future<Void> streamEndReached;
	state bool doFilterMutations;
	state int commonFeedPrefixLength;

	try {
		++data->counters.feedStreamQueries;

		// FIXME: do something more sophisticated here besides hard limit
		// Allow other storage servers fetching feeds to go above this limit. currently, req.canReadPopped == read is a
		// fetch from another ss
		if (!req.canReadPopped && (data->activeFeedQueries >= SERVER_KNOBS->STORAGE_FEED_QUERY_HARD_LIMIT ||
		                           (g_network->isSimulated() && BUGGIFY_WITH_PROB(0.005)))) {
			req.reply.sendError(storage_too_many_feed_streams());
			++data->counters.rejectedFeedStreamQueries;
			return Void();
		}

		data->activeFeedQueries++;

		if (req.replyBufferSize <= 0) {
			req.reply.setByteLimit(SERVER_KNOBS->CHANGEFEEDSTREAM_LIMIT_BYTES);
		} else {
			req.reply.setByteLimit(std::min((int64_t)req.replyBufferSize, SERVER_KNOBS->CHANGEFEEDSTREAM_LIMIT_BYTES));
		}

		// Change feeds that are not atLatest must have a lower priority than UpdateStorage to not starve it out, and
		// change feed disk reads generally only happen on blob worker recovery or data movement, so they should be
		// lower priority. AtLatest change feeds are triggered directly from the SS update loop with no waits, so they
		// will still be low latency
		wait(delay(0, TaskPriority::SSSpilledChangeFeedReply));

		if (DEBUG_CF_TRACE) {
			TraceEvent(SevDebug, "TraceChangeFeedStreamStart", data->thisServerID)
			    .detail("FeedID", req.rangeID)
			    .detail("StreamUID", req.id)
			    .detail("Range", req.range)
			    .detail("Begin", req.begin)
			    .detail("End", req.end)
			    .detail("CanReadPopped", req.canReadPopped)
			    .detail("PeerAddr", req.reply.getEndpoint().getPrimaryAddress())
			    .detail("PeerAddress", req.reply.getEndpoint().getPrimaryAddress());
		}

		Version checkTooOldVersion = (!req.canReadPopped || req.end == MAX_VERSION) ? req.begin : req.end;
		wait(success(waitForVersionNoTooOld(data, checkTooOldVersion)));

		// set persistent references to map data structures to not have to re-look them up every loop
		auto feed = data->uidChangeFeed.find(req.rangeID);
		if (feed == data->uidChangeFeed.end() || feed->second->removing) {
			req.reply.sendError(unknown_change_feed());
			// throw to delete from changeFeedClientVersions if present
			throw unknown_change_feed();
		}
		feedInfo = feed->second;

		streamEndReached =
		    (req.end == std::numeric_limits<Version>::max()) ? Never() : data->version.whenAtLeast(req.end);

		doFilterMutations = !req.range.contains(feedInfo->range);
		commonFeedPrefixLength = 0;
		if (doFilterMutations) {
			commonFeedPrefixLength = commonPrefixLength(feedInfo->range.begin, feedInfo->range.end);
		}

		// send an empty version at begin - 1 to establish the stream quickly
		ChangeFeedStreamReply emptyInitialReply;
		MutationsAndVersionRef emptyInitialVersion;
		emptyInitialVersion.version = req.begin - 1;
		emptyInitialReply.mutations.push_back_deep(emptyInitialReply.arena, emptyInitialVersion);
		ASSERT(emptyInitialReply.atLatestVersion == false);
		ASSERT(emptyInitialReply.minStreamVersion == invalidVersion);
		req.reply.send(emptyInitialReply);

		if (DEBUG_CF_TRACE) {
			TraceEvent(SevDebug, "TraceChangeFeedStreamSentInitialEmpty", data->thisServerID)
			    .detail("FeedID", req.rangeID)
			    .detail("StreamUID", req.id)
			    .detail("Range", req.range)
			    .detail("Begin", req.begin)
			    .detail("End", req.end)
			    .detail("CanReadPopped", req.canReadPopped)
			    .detail("Version", req.begin - 1)
			    .detail("PeerAddr", req.reply.getEndpoint().getPrimaryAddress())
			    .detail("PeerAddress", req.reply.getEndpoint().getPrimaryAddress());
		}

		loop {
			Future<Void> onReady = req.reply.onReady();
			if (atLatest && !onReady.isReady() && !removeUID) {
				data->changeFeedClientVersions[req.reply.getEndpoint().getPrimaryAddress()][req.id] =
				    blockedVersion.present() ? blockedVersion.get() : data->prevVersion;
				if (DEBUG_CF_TRACE) {
					TraceEvent(SevDebug, "TraceChangeFeedStreamBlockedOnReady", data->thisServerID)
					    .detail("FeedID", req.rangeID)
					    .detail("StreamUID", req.id)
					    .detail("Range", req.range)
					    .detail("Begin", req.begin)
					    .detail("End", req.end)
					    .detail("CanReadPopped", req.canReadPopped)
					    .detail("Version", blockedVersion.present() ? blockedVersion.get() : data->prevVersion)
					    .detail("PeerAddr", req.reply.getEndpoint().getPrimaryAddress())
					    .detail("PeerAddress", req.reply.getEndpoint().getPrimaryAddress());
				}
				removeUID = true;
			}
			wait(onReady);

			// keep this as not state variable so it is freed after sending to reduce memory
			Future<std::pair<ChangeFeedStreamReply, bool>> feedReplyFuture = getChangeFeedMutations(
			    data, feedInfo, req, atLatest, doFilterMutations, commonFeedPrefixLength, &feedDiskReadState);
			if (atLatest && !removeUID && !feedReplyFuture.isReady()) {
				data->changeFeedClientVersions[req.reply.getEndpoint().getPrimaryAddress()][req.id] =
				    blockedVersion.present() ? blockedVersion.get() : data->prevVersion;
				removeUID = true;
				if (DEBUG_CF_TRACE) {
					TraceEvent(SevDebug, "TraceChangeFeedStreamBlockedMutations", data->thisServerID)
					    .detail("FeedID", req.rangeID)
					    .detail("StreamUID", req.id)
					    .detail("Range", req.range)
					    .detail("Begin", req.begin)
					    .detail("End", req.end)
					    .detail("CanReadPopped", req.canReadPopped)
					    .detail("Version", blockedVersion.present() ? blockedVersion.get() : data->prevVersion)
					    .detail("PeerAddr", req.reply.getEndpoint().getPrimaryAddress())
					    .detail("PeerAddress", req.reply.getEndpoint().getPrimaryAddress());
				}
			}
			std::pair<ChangeFeedStreamReply, bool> _feedReply = wait(feedReplyFuture);
			ChangeFeedStreamReply feedReply = _feedReply.first;
			bool gotAll = _feedReply.second;

			ASSERT(feedReply.mutations.size() > 0);
			req.begin = feedReply.mutations.back().version + 1;
			if (!atLatest && gotAll) {
				atLatest = true;
			}

			auto& clientVersions = data->changeFeedClientVersions[req.reply.getEndpoint().getPrimaryAddress()];
			// If removeUID is not set, that means that this loop was never blocked and executed synchronously as part
			// of the new mutations trigger in the storage update loop. In that case, since there are potentially still
			// other feeds triggering that this would race with, the largest version we can reply with is the storage's
			// version just before the feed triggers. Otherwise, removeUID is set, and we were blocked at some point
			// after the trigger. This means all feed triggers are done, and we can safely reply with the storage's
			// version.
			Version minVersion = removeUID ? data->version.get() : data->prevVersion;
			if (removeUID) {
				if (gotAll || req.begin == req.end) {
					clientVersions.erase(req.id);
					removeUID = false;
				} else {
					clientVersions[req.id] = feedReply.mutations.back().version;
				}
			}

			for (auto& it : clientVersions) {
				minVersion = std::min(minVersion, it.second);
			}
			feedReply.atLatestVersion = atLatest;
			feedReply.minStreamVersion = minVersion;

			data->counters.feedRowsQueried += feedReply.mutations.size();
			data->counters.feedBytesQueried += feedReply.mutations.expectedSize();

			req.reply.send(feedReply);
			if (req.begin == req.end) {
				data->activeFeedQueries--;
				req.reply.sendError(end_of_stream());
				return Void();
			}
			if (gotAll) {
				blockedVersion = Optional<Version>();
				if (feedInfo->removing) {
					req.reply.sendError(unknown_change_feed());
					// throw to delete from changeFeedClientVersions if present
					throw unknown_change_feed();
				}
				choose {
					when(wait(feedInfo->newMutations.onTrigger())) {}
					when(wait(streamEndReached)) {}
				}
				if (feedInfo->removing) {
					req.reply.sendError(unknown_change_feed());
					// throw to delete from changeFeedClientVersions if present
					throw unknown_change_feed();
				}
			} else {
				blockedVersion = feedReply.mutations.back().version;
			}
		}
	} catch (Error& e) {
		data->activeFeedQueries--;
		auto it = data->changeFeedClientVersions.find(req.reply.getEndpoint().getPrimaryAddress());
		if (it != data->changeFeedClientVersions.end()) {
			if (removeUID) {
				it->second.erase(req.id);
			}
			if (it->second.empty()) {
				data->changeFeedClientVersions.erase(it);
			}
		}
		if (e.code() != error_code_operation_obsolete) {
			if (!canReplyWith(e))
				throw;
			req.reply.sendError(e);
		}
	}
	return Void();
}

ACTOR Future<Void> changeFeedVersionUpdateQ(StorageServer* data, ChangeFeedVersionUpdateRequest req) {
	++data->counters.feedVersionQueries;
	wait(data->version.whenAtLeast(req.minVersion));
	wait(delay(0));
	Version minVersion = data->minFeedVersionForAddress(req.reply.getEndpoint().getPrimaryAddress());
	req.reply.send(ChangeFeedVersionUpdateReply(minVersion));
	return Void();
}

#ifdef NO_INTELLISENSE
size_t WATCH_OVERHEAD_WATCHQ =
    sizeof(WatchValueSendReplyActorState<WatchValueSendReplyActor>) + sizeof(WatchValueSendReplyActor);
size_t WATCH_OVERHEAD_WATCHIMPL =
    sizeof(WatchWaitForValueChangeActorState<WatchWaitForValueChangeActor>) + sizeof(WatchWaitForValueChangeActor);
#else
size_t WATCH_OVERHEAD_WATCHQ = 0; // only used in IDE so value is irrelevant
size_t WATCH_OVERHEAD_WATCHIMPL = 0;
#endif

ACTOR Future<Void> getShardState_impl(StorageServer* data, GetShardStateRequest req) {
	ASSERT(req.mode != GetShardStateRequest::NO_WAIT);

	loop {
		std::vector<Future<Void>> onChange;

		for (auto t : data->shards.intersectingRanges(req.keys)) {
			if (!t.value()->assigned()) {
				onChange.push_back(delay(SERVER_KNOBS->SHARD_READY_DELAY));
				break;
			}

			if (req.mode == GetShardStateRequest::READABLE && !t.value()->isReadable()) {
				if (t.value()->adding) {
					onChange.push_back(t.value()->adding->readWrite.getFuture());
				} else {
					ASSERT(t.value()->moveInShard);
					onChange.push_back(t.value()->moveInShard->readWrite.getFuture());
				}
			}

			if (req.mode == GetShardStateRequest::FETCHING && !t.value()->isFetched()) {
				if (t.value()->adding) {
					onChange.push_back(t.value()->adding->fetchComplete.getFuture());
				} else {
					ASSERT(t.value()->moveInShard);
					onChange.push_back(t.value()->moveInShard->fetchComplete.getFuture());
				}
			}
		}

		if (!onChange.size()) {
			GetShardStateReply rep(data->version.get(), data->durableVersion.get());
			if (req.includePhysicalShard) {
				rep.shards = data->getStorageServerShards(req.keys);
			}
			req.reply.send(rep);
			return Void();
		}

		wait(waitForAll(onChange));
		wait(delay(0)); // onChange could have been triggered by cancellation, let things settle before rechecking
	}
}

ACTOR Future<Void> getShardStateQ(StorageServer* data, GetShardStateRequest req) {
	choose {
		when(wait(getShardState_impl(data, req))) {}
		when(wait(delay(g_network->isSimulated() ? 10 : 60))) {
			data->sendErrorWithPenalty(req.reply, timed_out(), data->getPenalty());
		}
	}
	return Void();
}

KeyRef addPrefix(KeyRef const& key, Optional<KeyRef> prefix, Arena& arena) {
	if (prefix.present()) {
		return key.withPrefix(prefix.get(), arena);
	} else {
		return key;
	}
}

KeyValueRef removePrefix(KeyValueRef const& src, Optional<KeyRef> prefix) {
	if (prefix.present()) {
		return KeyValueRef(src.key.removePrefix(prefix.get()), src.value);
	} else {
		return src;
	}
}

void merge(Arena& arena,
           VectorRef<KeyValueRef, VecSerStrategy::String>& output,
           VectorRef<KeyValueRef> const& vm_output,
           RangeResult const& base,
           int& vCount,
           int limit,
           bool stopAtEndOfBase,
           int& pos,
           int limitBytes,
           Optional<KeyRef> tenantPrefix)
// Combines data from base (at an older version) with sets from newer versions in [start, end) and appends the first (up
// to) |limit| rows to output If limit<0, base and output are in descending order, and start->key()>end->key(), but
// start is still inclusive and end is exclusive
{
	ASSERT(limit != 0);
	// Add a dependency of the new arena on the result from the KVS so that we don't have to copy any of the KVS
	// results.
	arena.dependsOn(base.arena());

	bool forward = limit > 0;
	if (!forward)
		limit = -limit;
	int adjustedLimit = limit + output.size();
	int accumulatedBytes = 0;
	KeyValueRef const* baseStart = base.begin();
	KeyValueRef const* baseEnd = base.end();
	while (baseStart != baseEnd && vCount > 0 && output.size() < adjustedLimit && accumulatedBytes < limitBytes) {
		if (forward ? baseStart->key < vm_output[pos].key : baseStart->key > vm_output[pos].key) {
			output.push_back(arena, removePrefix(*baseStart++, tenantPrefix));
		} else {
			output.push_back_deep(arena, removePrefix(vm_output[pos], tenantPrefix));
			if (baseStart->key == vm_output[pos].key)
				++baseStart;
			++pos;
			vCount--;
		}
		accumulatedBytes += sizeof(KeyValueRef) + output.end()[-1].expectedSize();
	}
	while (baseStart != baseEnd && output.size() < adjustedLimit && accumulatedBytes < limitBytes) {
		output.push_back(arena, removePrefix(*baseStart++, tenantPrefix));
		accumulatedBytes += sizeof(KeyValueRef) + output.end()[-1].expectedSize();
	}
	if (!stopAtEndOfBase) {
		while (vCount > 0 && output.size() < adjustedLimit && accumulatedBytes < limitBytes) {
			output.push_back_deep(arena, removePrefix(vm_output[pos], tenantPrefix));
			accumulatedBytes += sizeof(KeyValueRef) + output.end()[-1].expectedSize();
			++pos;
			vCount--;
		}
	}
}

static inline void copyOptionalValue(Arena* a,
                                     GetValueReqAndResultRef& getValue,
                                     const Optional<Value>& optionalValue) {
	getValue.result = optionalValue.castTo<ValueRef>();
	if (optionalValue.present()) {
		a->dependsOn(optionalValue.get().arena());
	}
}
ACTOR Future<GetValueReqAndResultRef> quickGetValue(StorageServer* data,
                                                    StringRef key,
                                                    Version version,
                                                    Arena* a,
                                                    // To provide span context, tags, debug ID to underlying lookups.
                                                    GetMappedKeyValuesRequest* pOriginalReq) {
	state GetValueReqAndResultRef getValue;
	state double getValueStart = g_network->timer();
	getValue.key = key;

	if (data->shards[key]->isReadable()) {
		try {
			// TODO: Use a lower level API may be better? Or tweak priorities?
			GetValueRequest req(pOriginalReq->spanContext,
			                    pOriginalReq->tenantInfo,
			                    key,
			                    version,
			                    pOriginalReq->tags,
			                    pOriginalReq->options,
			                    VersionVector());
			// Note that it does not use readGuard to avoid server being overloaded here. Throttling is enforced at the
			// original request level, rather than individual underlying lookups. The reason is that throttle any
			// individual underlying lookup will fail the original request, which is not productive.
			data->actors.add(getValueQ(data, req));
			GetValueReply reply = wait(req.reply.getFuture());
			if (!reply.error.present()) {
				++data->counters.quickGetValueHit;
				copyOptionalValue(a, getValue, reply.value);
				const double duration = g_network->timer() - getValueStart;
				data->counters.mappedRangeLocalSample->addMeasurement(duration);
				return getValue;
			}
			// Otherwise fallback.
		} catch (Error& e) {
			// Fallback.
		}
	}
	// Otherwise fallback.

	++data->counters.quickGetValueMiss;
	if (SERVER_KNOBS->QUICK_GET_VALUE_FALLBACK) {
		Optional<Reference<Tenant>> tenant = pOriginalReq->tenantInfo.hasTenant()
		                                         ? makeReference<Tenant>(pOriginalReq->tenantInfo.tenantId)
		                                         : Optional<Reference<Tenant>>();
		state Transaction tr(data->cx, tenant);
		tr.setVersion(version);
		// TODO: is DefaultPromiseEndpoint the best priority for this?
		tr.trState->taskID = TaskPriority::DefaultPromiseEndpoint;
		Future<Optional<Value>> valueFuture = tr.get(key, Snapshot::True);
		// TODO: async in case it needs to read from other servers.
		Optional<Value> valueOption = wait(valueFuture);
		copyOptionalValue(a, getValue, valueOption);
		double duration = g_network->timer() - getValueStart;
		data->counters.mappedRangeRemoteSample->addMeasurement(duration);
		return getValue;
	} else {
		throw quick_get_value_miss();
	}
}

// If limit>=0, it returns the first rows in the range (sorted ascending), otherwise the last rows (sorted descending).
// readRange has O(|result|) + O(log |data|) cost
ACTOR Future<GetKeyValuesReply> readRange(StorageServer* data,
                                          Version version,
                                          KeyRange range,
                                          int limit,
                                          int* pLimitBytes,
                                          SpanContext parentSpan,
                                          Optional<ReadOptions> options,
                                          Optional<KeyRef> tenantPrefix) {
	state GetKeyValuesReply result;
	state StorageServer::VersionedData::ViewAtVersion view = data->data().at(version);
	state StorageServer::VersionedData::iterator vCurrent = view.end();
	state KeyRef readBegin;
	state KeyRef readEnd;
	state Key readBeginTemp;
	state int vCount = 0;
	state Span span("SS:readRange"_loc, parentSpan);
	state int resultLogicalSize = 0;
	state int logicalSize = 0;

	// for caching the storage queue results during the first PTree traversal
	state VectorRef<KeyValueRef> resultCache;

	// for remembering the position in the resultCache
	state int pos = 0;

	// Check if the desired key-range is cached
	auto containingRange = data->cachedRangeMap.rangeContaining(range.begin);
	if (containingRange.value() && containingRange->range().end >= range.end) {
		//TraceEvent(SevDebug, "SSReadRangeCached").detail("Size",data->cachedRangeMap.size()).detail("ContainingRangeBegin",containingRange->range().begin).detail("ContainingRangeEnd",containingRange->range().end).
		//	detail("Begin", range.begin).detail("End",range.end);
		result.cached = true;
	} else
		result.cached = false;

	// if (limit >= 0) we are reading forward, else backward
	if (limit >= 0) {
		// We might care about a clear beginning before start that
		//  runs into range
		vCurrent = view.lastLessOrEqual(range.begin);
		if (vCurrent && vCurrent->isClearTo() && vCurrent->getEndKey() > range.begin)
			readBegin = vCurrent->getEndKey();
		else
			readBegin = range.begin;

		// We can get lower_bound from the result of lastLessOrEqual
		if (vCurrent) {
			if (vCurrent.key() != readBegin) {
				++vCurrent;
			}
		} else {
			// There's nothing less than or equal to readBegin in view, so
			// begin() is the first thing greater than readBegin, or end().
			// Either way that's the correct result for lower_bound.
			vCurrent = view.begin();
		}
		if (EXPENSIVE_VALIDATION) {
			ASSERT(vCurrent == view.lower_bound(readBegin));
		}

		while (limit > 0 && *pLimitBytes > 0 && readBegin < range.end) {
			ASSERT(!vCurrent || vCurrent.key() >= readBegin);
			ASSERT(data->storageVersion() <= version);

			/* Traverse the PTree further, if there are no unconsumed resultCache items */
			if (pos == resultCache.size()) {
				if (vCurrent) {
					auto b = vCurrent;
					--b;
					ASSERT(!b || b.key() < readBegin);
				}

				// Read up to limit items from the view, stopping at the next clear (or the end of the range)
				int vSize = 0;
				while (vCurrent && vCurrent.key() < range.end && !vCurrent->isClearTo() && vCount < limit &&
				       vSize < *pLimitBytes) {
					// Store the versionedData results in resultCache
					resultCache.emplace_back(result.arena, vCurrent.key(), vCurrent->getValue());
					vSize += sizeof(KeyValueRef) + resultCache.cback().expectedSize() -
					         (tenantPrefix.present() ? tenantPrefix.get().size() : 0);
					++vCount;
					++vCurrent;
				}
			}

			// Read the data on disk up to vCurrent (or the end of the range)
			readEnd = vCurrent ? std::min(vCurrent.key(), range.end) : range.end;
			RangeResult atStorageVersion =
			    wait(data->storage.readRange(KeyRangeRef(readBegin, readEnd), limit, *pLimitBytes, options));
			logicalSize = atStorageVersion.logicalSize();
			data->counters.kvScanBytes += logicalSize;
			resultLogicalSize += logicalSize;
			data->readRangeBytesLimitHistogram->sample(*pLimitBytes);

			ASSERT(atStorageVersion.size() <= limit);
			if (data->storageVersion() > version) {
				DisabledTraceEvent("SS_TTO", data->thisServerID)
				    .detail("StorageVersion", data->storageVersion())
				    .detail("Oldest", data->oldestVersion.get())
				    .detail("Version", version)
				    .detail("Range", range);
				throw transaction_too_old();
			}

			// merge the sets in resultCache with the sets on disk, stopping at the last key from disk if there is
			// 'more'
			int prevSize = result.data.size();
			merge(result.arena,
			      result.data,
			      resultCache,
			      atStorageVersion,
			      vCount,
			      limit,
			      atStorageVersion.more,
			      pos,
			      *pLimitBytes,
			      tenantPrefix);
			limit -= result.data.size() - prevSize;

			for (auto i = result.data.begin() + prevSize; i != result.data.end(); i++) {
				*pLimitBytes -= sizeof(KeyValueRef) + i->expectedSize();
			}

			if (limit <= 0 || *pLimitBytes <= 0) {
				break;
			}

			// Setup for the next iteration
			// If we hit our limits reading from disk but then combining with MVCC gave us back more room

			// if there might be more data, begin reading right after what we already found to find out
			if (atStorageVersion.more) {
				ASSERT(atStorageVersion.end()[-1].key.size() ==
				           result.data.end()[-1].key.size() + tenantPrefix.orDefault(""_sr).size() &&
				       atStorageVersion.end()[-1].key.endsWith(result.data.end()[-1].key) &&
				       atStorageVersion.end()[-1].key.startsWith(tenantPrefix.orDefault(""_sr)));

				readBegin = readBeginTemp = keyAfter(atStorageVersion.end()[-1].key);
			}

			// if vCurrent is a clear, skip it.
			else if (vCurrent && vCurrent->isClearTo()) {
				ASSERT(vCurrent->getEndKey() > readBegin);
				// next disk read should start at the end of the clear
				readBegin = vCurrent->getEndKey();
				++vCurrent;
			} else {
				ASSERT(readEnd == range.end);
				break;
			}
		}
	} else {
		vCurrent = view.lastLess(range.end);

		// A clear might extend all the way to range.end
		if (vCurrent && vCurrent->isClearTo() && vCurrent->getEndKey() >= range.end) {
			readEnd = vCurrent.key();
			--vCurrent;
		} else {
			readEnd = range.end;
		}

		while (limit < 0 && *pLimitBytes > 0 && readEnd > range.begin) {
			ASSERT(!vCurrent || vCurrent.key() < readEnd);
			ASSERT(data->storageVersion() <= version);

			/* Traverse the PTree further, if there are no unconsumed resultCache items */
			if (pos == resultCache.size()) {
				if (vCurrent) {
					auto b = vCurrent;
					++b;
					ASSERT(!b || b.key() >= readEnd);
				}

				vCount = 0;
				int vSize = 0;
				while (vCurrent && vCurrent.key() >= range.begin && !vCurrent->isClearTo() && vCount < -limit &&
				       vSize < *pLimitBytes) {
					// Store the versionedData results in resultCache
					resultCache.emplace_back(result.arena, vCurrent.key(), vCurrent->getValue());
					vSize += sizeof(KeyValueRef) + resultCache.cback().expectedSize() -
					         (tenantPrefix.present() ? tenantPrefix.get().size() : 0);
					++vCount;
					--vCurrent;
				}
			}

			readBegin = vCurrent ? std::max(vCurrent->isClearTo() ? vCurrent->getEndKey() : vCurrent.key(), range.begin)
			                     : range.begin;
			RangeResult atStorageVersion =
			    wait(data->storage.readRange(KeyRangeRef(readBegin, readEnd), limit, *pLimitBytes, options));
			logicalSize = atStorageVersion.logicalSize();
			data->counters.kvScanBytes += logicalSize;
			resultLogicalSize += logicalSize;
			data->readRangeBytesLimitHistogram->sample(*pLimitBytes);

			ASSERT(atStorageVersion.size() <= -limit);
			if (data->storageVersion() > version) {
				DisabledTraceEvent("SS_TTO", data->thisServerID)
				    .detail("StorageVersion", data->storageVersion())
				    .detail("Oldest", data->oldestVersion.get())
				    .detail("Version", version)
				    .detail("Range", range);
				throw transaction_too_old();
			}

			int prevSize = result.data.size();
			merge(result.arena,
			      result.data,
			      resultCache,
			      atStorageVersion,
			      vCount,
			      limit,
			      atStorageVersion.more,
			      pos,
			      *pLimitBytes,
			      tenantPrefix);
			limit += result.data.size() - prevSize;

			for (auto i = result.data.begin() + prevSize; i != result.data.end(); i++) {
				*pLimitBytes -= sizeof(KeyValueRef) + i->expectedSize();
			}

			if (limit >= 0 || *pLimitBytes <= 0) {
				break;
			}

			if (atStorageVersion.more) {
				ASSERT(atStorageVersion.end()[-1].key.size() ==
				           result.data.end()[-1].key.size() + tenantPrefix.orDefault(""_sr).size() &&
				       atStorageVersion.end()[-1].key.endsWith(result.data.end()[-1].key) &&
				       atStorageVersion.end()[-1].key.startsWith(tenantPrefix.orDefault(""_sr)));

				readEnd = atStorageVersion.end()[-1].key;
			} else if (vCurrent && vCurrent->isClearTo()) {
				ASSERT(vCurrent.key() < readEnd);
				readEnd = vCurrent.key();
				--vCurrent;
			} else {
				ASSERT(readBegin == range.begin);
				break;
			}
		}
	}
	data->readRangeBytesReturnedHistogram->sample(resultLogicalSize);
	data->readRangeKVPairsReturnedHistogram->sample(result.data.size());

	// all but the last item are less than *pLimitBytes
	ASSERT(result.data.size() == 0 || *pLimitBytes + result.data.end()[-1].expectedSize() + sizeof(KeyValueRef) > 0);
	result.more = limit == 0 || *pLimitBytes <= 0; // FIXME: Does this have to be exact?
	result.version = version;
	return result;
}

ACTOR Future<Key> findKey(StorageServer* data,
                          KeySelectorRef sel,
                          Version version,
                          KeyRange range,
                          int* pOffset,
                          SpanContext parentSpan,
                          Optional<ReadOptions> options)
// Attempts to find the key indicated by sel in the data at version, within range.
// Precondition: selectorInRange(sel, range)
// If it is found, offset is set to 0 and a key is returned which falls inside range.
// If the search would depend on any key outside range OR if the key selector offset is too large (range read returns
// too many bytes), it returns either
//   a negative offset and a key in [range.begin, sel.getKey()], indicating the key is (the first key <= returned key) +
//   offset, or a positive offset and a key in (sel.getKey(), range.end], indicating the key is (the first key >=
//   returned key) + offset-1
// The range passed in to this function should specify a shard.  If range.begin is repeatedly not the beginning of a
// shard, then it is possible to get stuck looping here
{
	ASSERT(version != latestVersion);
	ASSERT(selectorInRange(sel, range) && version >= data->oldestVersion.get());

	// Count forward or backward distance items, skipping the first one if it == key and skipEqualKey
	state bool forward = sel.offset > 0; // If forward, result >= sel.getKey(); else result <= sel.getKey()
	state int sign = forward ? +1 : -1;
	state bool skipEqualKey = sel.orEqual == forward;
	state int distance = forward ? sel.offset : 1 - sel.offset;
	state Span span("SS.findKey"_loc, parentSpan);

	// Don't limit the number of bytes if this is a trivial key selector (there will be at most two items returned from
	// the read range in this case)
	state int maxBytes;
	if (sel.offset <= 1 && sel.offset >= 0)
		maxBytes = std::numeric_limits<int>::max();
	else
		maxBytes = (g_network->isSimulated() && g_simulator->tssMode == ISimulator::TSSMode::Disabled && BUGGIFY)
		               ? SERVER_KNOBS->BUGGIFY_LIMIT_BYTES
		               : SERVER_KNOBS->STORAGE_LIMIT_BYTES;

	state GetKeyValuesReply rep = wait(
	    readRange(data,
	              version,
	              forward ? KeyRangeRef(sel.getKey(), range.end) : KeyRangeRef(range.begin, keyAfter(sel.getKey())),
	              (distance + skipEqualKey) * sign,
	              &maxBytes,
	              span.context,
	              options,
	              {}));
	state bool more = rep.more && rep.data.size() != distance + skipEqualKey;

	// If we get only one result in the reverse direction as a result of the data being too large, we could get stuck in
	// a loop
	if (more && !forward && rep.data.size() == 1) {
		CODE_PROBE(true, "Reverse key selector returned only one result in range read");
		maxBytes = std::numeric_limits<int>::max();
		GetKeyValuesReply rep2 = wait(readRange(
		    data, version, KeyRangeRef(range.begin, keyAfter(sel.getKey())), -2, &maxBytes, span.context, options, {}));
		rep = rep2;
		more = rep.more && rep.data.size() != distance + skipEqualKey;
		ASSERT(rep.data.size() == 2 || !more);
	}

	int index = distance - 1;
	if (skipEqualKey && rep.data.size() && rep.data[0].key == sel.getKey())
		++index;

	if (index < rep.data.size()) {
		*pOffset = 0;

		if (SERVER_KNOBS->READ_SAMPLING_ENABLED) {
			int64_t bytesReadPerKSecond =
			    std::max((int64_t)rep.data[index].key.size(), SERVER_KNOBS->EMPTY_READ_PENALTY);
			data->metrics.notifyBytesReadPerKSecond(sel.getKey(), bytesReadPerKSecond);
		}

		return rep.data[index].key;
	} else {
		if (SERVER_KNOBS->READ_SAMPLING_ENABLED) {
			int64_t bytesReadPerKSecond = SERVER_KNOBS->EMPTY_READ_PENALTY;
			data->metrics.notifyBytesReadPerKSecond(sel.getKey(), bytesReadPerKSecond);
		}

		// FIXME: If range.begin=="" && !forward, return success?
		*pOffset = index - rep.data.size() + 1;
		if (!forward)
			*pOffset = -*pOffset;

		if (more) {
			CODE_PROBE(true, "Key selector read range had more results");

			ASSERT(rep.data.size());
			Key returnKey = forward ? keyAfter(rep.data.back().key) : rep.data.back().key;

			// This is possible if key/value pairs are very large and only one result is returned on a last less than
			// query SOMEDAY: graceful handling of exceptionally sized values
			ASSERT(returnKey != sel.getKey());
			return returnKey;
		} else {
			return forward ? range.end : range.begin;
		}
	}
}

KeyRange getShardKeyRange(StorageServer* data, const KeySelectorRef& sel)
// Returns largest range such that the shard state isReadable and selectorInRange(sel, range) or wrong_shard_server if
// no such range exists
{
	auto i = sel.isBackward() ? data->shards.rangeContainingKeyBefore(sel.getKey())
	                          : data->shards.rangeContaining(sel.getKey());
	auto fullRange = data->shards.ranges();
	if (!i->value()->isReadable())
		throw wrong_shard_server();
	ASSERT(selectorInRange(sel, i->range()));
	Key begin, end;
	if (sel.isBackward()) {
		end = i->range().end;
		while (i != fullRange.begin() && i.value()->isReadable()) {
			begin = i->range().begin;
			--i;
		}
		if (i.value()->isReadable()) {
			begin = i->range().begin;
		}
	} else {
		begin = i->range().begin;
		while (i != fullRange.end() && i.value()->isReadable()) {
			end = i->range().end;
			++i;
		}
	}
	return KeyRangeRef(begin, end);
}

void maybeInjectConsistencyScanCorruption(UID thisServerID, GetKeyValuesRequest const& req, GetKeyValuesReply& reply) {
	if (g_simulator->consistencyScanState != ISimulator::SimConsistencyScanState::Enabled_InjectCorruption ||
	    !req.options.present() || !req.options.get().consistencyCheckStartVersion.present() ||
	    !g_simulator->consistencyScanCorruptRequestKey.present()) {
		return;
	}

	UID destination = req.reply.getEndpoint().token;

	ASSERT(g_simulator->consistencyScanInjectedCorruptionType.present() ==
	       g_simulator->consistencyScanInjectedCorruptionDestination.present());
	// if we already injected a corruption, reinject it if this request was a retransmit of the same one we corrupted
	// could also check that this storage sent the corruption but the reply endpoints should be globally unique so this
	// covers it
	if (g_simulator->consistencyScanInjectedCorruptionDestination.present() &&
	    (g_simulator->consistencyScanInjectedCorruptionDestination.get() != destination)) {
		return;
	}

	CODE_PROBE(true, "consistency check injecting corruption");
	CODE_PROBE(g_simulator->consistencyScanInjectedCorruptionDestination.present() &&
	               g_simulator->consistencyScanInjectedCorruptionDestination.get() == destination,
	           "consistency check re-injecting corruption after retransmit",
	           probe::decoration::rare);

	g_simulator->consistencyScanInjectedCorruptionDestination = destination;
	// FIXME: reinject same type of corruption once we enable other types

	// FIXME: code probe for each type?

	if (true /*deterministicRandom()->random01() < 0.3*/) {
		// flip more flag
		reply.more = !reply.more;
		g_simulator->consistencyScanInjectedCorruptionType = ISimulator::SimConsistencyScanCorruptionType::FlipMoreFlag;
	} else {
		// FIXME: weird memory issues when messing with actual response data, enable and figure out later
		ASSERT(false);
		// make deep copy of request, since some of the underlying memory can reference storage engine data directly
		GetKeyValuesReply copy = reply;
		reply = GetKeyValuesReply();
		reply.more = copy.more;
		reply.cached = copy.cached;
		reply.version = copy.version;
		reply.data.append_deep(reply.arena, copy.data.begin(), copy.data.size());

		if (reply.data.empty()) {
			// add row to empty response
			g_simulator->consistencyScanInjectedCorruptionType =
			    ISimulator::SimConsistencyScanCorruptionType::AddToEmpty;
			reply.data.push_back_deep(
			    reply.arena,
			    KeyValueRef(g_simulator->consistencyScanCorruptRequestKey.get(), "consistencyCheckCorruptValue"_sr));
		} else if (deterministicRandom()->coinflip() || reply.data.back().value.empty()) {
			// change value in non-empty response
			g_simulator->consistencyScanInjectedCorruptionType =
			    ISimulator::SimConsistencyScanCorruptionType::RemoveLastRow;
			reply.data.pop_back();
		} else {
			// chop off last byte of first value
			g_simulator->consistencyScanInjectedCorruptionType =
			    ISimulator::SimConsistencyScanCorruptionType::ChangeFirstValue;

			reply.data[0].value = reply.data[0].value.substr(0, reply.data[0].value.size() - 1);
		}
	}

	TraceEvent(SevWarnAlways, "InjectedConsistencyScanCorruption", thisServerID)
	    .detail("CorruptionType", g_simulator->consistencyScanInjectedCorruptionType.get())
	    .detail("Version", req.version)
	    .detail("Count", reply.data.size());
}

ACTOR Future<Void> getKeyValuesQ(StorageServer* data, GetKeyValuesRequest req)
// Throws a wrong_shard_server if the keys in the request or result depend on data outside this server OR if a large
// selector offset prevents all data from being read in one range read
{
	state Span span("SS:getKeyValues"_loc, req.spanContext);
	state int64_t resultSize = 0;

	getCurrentLineage()->modify(&TransactionLineage::txID) = req.spanContext.traceID;

	++data->counters.getRangeQueries;
	++data->counters.allQueries;
	if (req.begin.getKey().startsWith(systemKeys.begin)) {
		++data->counters.systemKeyQueries;
		++data->counters.getRangeSystemKeyQueries;
	}
	data->maxQueryQueue = std::max<int>(
	    data->maxQueryQueue, data->counters.allQueries.getValue() - data->counters.finishedQueries.getValue());

	// Active load balancing runs at a very high priority (to obtain accurate queue lengths)
	// so we need to downgrade here
	wait(data->getQueryDelay());
	state PriorityMultiLock::Lock readLock = wait(data->getReadLock(req.options));

	// Track time from requestTime through now as read queueing wait time
	state double queueWaitEnd = g_network->timer();
	data->counters.readQueueWaitSample.addMeasurement(queueWaitEnd - req.requestTime());

	try {
		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent(
			    "TransactionDebug", req.options.get().debugID.get().first(), "storageserver.getKeyValues.Before");

		Version commitVersion = getLatestCommitVersion(req.ssLatestCommitVersions, data->tag);
		state Version version = wait(waitForVersion(data, commitVersion, req.version, span.context));
		DisabledTraceEvent("VVV", data->thisServerID)
		    .detail("Version", version)
		    .detail("ReqVersion", req.version)
		    .detail("Oldest", data->oldestVersion.get())
		    .detail("VV", req.ssLatestCommitVersions.toString())
		    .detail("DebugID",
		            req.options.present() && req.options.get().debugID.present() ? req.options.get().debugID.get()
		                                                                         : UID());
		data->counters.readVersionWaitSample.addMeasurement(g_network->timer() - queueWaitEnd);

		data->checkTenantEntry(version, req.tenantInfo, req.options.present() ? req.options.get().lockAware : false);
		if (req.tenantInfo.hasTenant()) {
			req.begin.setKeyUnlimited(req.begin.getKey().withPrefix(req.tenantInfo.prefix.get(), req.arena));
			req.end.setKeyUnlimited(req.end.getKey().withPrefix(req.tenantInfo.prefix.get(), req.arena));
		}

		state uint64_t changeCounter = data->shardChangeCounter;
		//		try {
		state KeyRange shard = getShardKeyRange(data, req.begin);

		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent(
			    "TransactionDebug", req.options.get().debugID.get().first(), "storageserver.getKeyValues.AfterVersion");
		//.detail("ShardBegin", shard.begin).detail("ShardEnd", shard.end);
		//} catch (Error& e) { TraceEvent("WrongShardServer", data->thisServerID).detail("Begin",
		// req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("Shard",
		//"None").detail("In", "getKeyValues>getShardKeyRange"); throw e; }

		if (!selectorInRange(req.end, shard) && !(req.end.isFirstGreaterOrEqual() && req.end.getKey() == shard.end)) {
			//			TraceEvent("WrongShardServer1", data->thisServerID).detail("Begin",
			// req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("ShardBegin",
			// shard.begin).detail("ShardEnd", shard.end).detail("In", "getKeyValues>checkShardExtents");
			throw wrong_shard_server();
		}

		KeyRangeRef searchRange = TenantAPI::clampRangeToTenant(shard, req.tenantInfo, req.arena);

		state int offset1 = 0;
		state int offset2;
		state Future<Key> fBegin =
		    req.begin.isFirstGreaterOrEqual()
		        ? Future<Key>(req.begin.getKey())
		        : findKey(data, req.begin, version, searchRange, &offset1, span.context, req.options);
		state Future<Key> fEnd =
		    req.end.isFirstGreaterOrEqual()
		        ? Future<Key>(req.end.getKey())
		        : findKey(data, req.end, version, searchRange, &offset2, span.context, req.options);
		state Key begin = wait(fBegin);
		state Key end = wait(fEnd);

		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent(
			    "TransactionDebug", req.options.get().debugID.get().first(), "storageserver.getKeyValues.AfterKeys");
		//.detail("Off1",offset1).detail("Off2",offset2).detail("ReqBegin",req.begin.getKey()).detail("ReqEnd",req.end.getKey());

		// Offsets of zero indicate begin/end keys in this shard, which obviously means we can answer the query
		// An end offset of 1 is also OK because the end key is exclusive, so if the first key of the next shard is the
		// end the last actual key returned must be from this shard. A begin offset of 1 is also OK because then either
		// begin is past end or equal to end (so the result is definitely empty)
		if ((offset1 && offset1 != 1) || (offset2 && offset2 != 1)) {
			CODE_PROBE(true, "wrong_shard_server due to offset");
			// We could detect when offset1 takes us off the beginning of the database or offset2 takes us off the end,
			// and return a clipped range rather than an error (since that is what the NativeAPI.getRange will do anyway
			// via its "slow path"), but we would have to add some flags to the response to encode whether we went off
			// the beginning and the end, since it needs that information.
			//TraceEvent("WrongShardServer2", data->thisServerID).detail("Begin", req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("ShardBegin", shard.begin).detail("ShardEnd", shard.end).detail("In", "getKeyValues>checkOffsets").detail("BeginKey", begin).detail("EndKey", end).detail("BeginOffset", offset1).detail("EndOffset", offset2);
			throw wrong_shard_server();
		}

		if (begin >= end) {
			if (req.options.present() && req.options.get().debugID.present())
				g_traceBatch.addEvent(
				    "TransactionDebug", req.options.get().debugID.get().first(), "storageserver.getKeyValues.Send");
			//.detail("Begin",begin).detail("End",end);

			GetKeyValuesReply none;
			none.version = version;
			none.more = false;
			none.penalty = data->getPenalty();

			data->checkChangeCounter(changeCounter,
			                         KeyRangeRef(std::min<KeyRef>(req.begin.getKey(), req.end.getKey()),
			                                     std::max<KeyRef>(req.begin.getKey(), req.end.getKey())));

			if (g_network->isSimulated()) {
				maybeInjectConsistencyScanCorruption(data->thisServerID, req, none);
			}
			req.reply.send(none);
		} else {
			state int remainingLimitBytes = req.limitBytes;

			state double kvReadRange = g_network->timer();
			GetKeyValuesReply _r = wait(readRange(data,
			                                      version,
			                                      KeyRangeRef(begin, end),
			                                      req.limit,
			                                      &remainingLimitBytes,
			                                      span.context,
			                                      req.options,
			                                      req.tenantInfo.prefix));
			const double duration = g_network->timer() - kvReadRange;
			data->counters.kvReadRangeLatencySample.addMeasurement(duration);
			GetKeyValuesReply r = _r;

			if (req.options.present() && req.options.get().debugID.present())
				g_traceBatch.addEvent("TransactionDebug",
				                      req.options.get().debugID.get().first(),
				                      "storageserver.getKeyValues.AfterReadRange");
			//.detail("Begin",begin).detail("End",end).detail("SizeOf",r.data.size());
			data->checkChangeCounter(
			    changeCounter,
			    KeyRangeRef(std::min<KeyRef>(begin, std::min<KeyRef>(req.begin.getKey(), req.end.getKey())),
			                std::max<KeyRef>(end, std::max<KeyRef>(req.begin.getKey(), req.end.getKey()))));
			if (EXPENSIVE_VALIDATION) {
				for (int i = 0; i < r.data.size(); i++) {
					if (req.tenantInfo.prefix.present()) {
						ASSERT(r.data[i].key >= begin.removePrefix(req.tenantInfo.prefix.get()) &&
						       r.data[i].key < end.removePrefix(req.tenantInfo.prefix.get()));
					} else {
						ASSERT(r.data[i].key >= begin && r.data[i].key < end);
					}
				}
				ASSERT(r.data.size() <= std::abs(req.limit));
			}

			// For performance concerns, the cost of a range read is billed to the start key and end key of the range.
			int64_t totalByteSize = 0;
			for (int i = 0; i < r.data.size(); i++) {
				totalByteSize += r.data[i].expectedSize();
			}
			if (totalByteSize > 0 && SERVER_KNOBS->READ_SAMPLING_ENABLED) {
				int64_t bytesReadPerKSecond = std::max(totalByteSize, SERVER_KNOBS->EMPTY_READ_PENALTY) / 2;
				data->metrics.notifyBytesReadPerKSecond(addPrefix(r.data[0].key, req.tenantInfo.prefix, req.arena),
				                                        bytesReadPerKSecond);
				data->metrics.notifyBytesReadPerKSecond(
				    addPrefix(r.data[r.data.size() - 1].key, req.tenantInfo.prefix, req.arena), bytesReadPerKSecond);
			}

			r.penalty = data->getPenalty();
			if (g_network->isSimulated()) {
				maybeInjectConsistencyScanCorruption(data->thisServerID, req, r);
			}
			req.reply.send(r);

			resultSize = req.limitBytes - remainingLimitBytes;
			data->counters.bytesQueried += resultSize;
			data->counters.rowsQueried += r.data.size();
			if (r.data.size() == 0) {
				++data->counters.emptyQueries;
			}
		}
	} catch (Error& e) {
		if (!canReplyWith(e))
			throw;
		data->sendErrorWithPenalty(req.reply, e, data->getPenalty());
	}

	data->transactionTagCounter.addRequest(req.tags, resultSize);
	++data->counters.finishedQueries;

	double duration = g_network->timer() - req.requestTime();
	data->counters.readLatencySample.addMeasurement(duration);
	data->counters.readRangeLatencySample.addMeasurement(duration);
	if (data->latencyBandConfig.present()) {
		int maxReadBytes =
		    data->latencyBandConfig.get().readConfig.maxReadBytes.orDefault(std::numeric_limits<int>::max());
		int maxSelectorOffset =
		    data->latencyBandConfig.get().readConfig.maxKeySelectorOffset.orDefault(std::numeric_limits<int>::max());
		data->counters.readLatencyBands.addMeasurement(duration,
		                                               1,
		                                               Filtered(resultSize > maxReadBytes ||
		                                                        abs(req.begin.offset) > maxSelectorOffset ||
		                                                        abs(req.end.offset) > maxSelectorOffset));
	}

	return Void();
}

ACTOR Future<GetRangeReqAndResultRef> quickGetKeyValues(
    StorageServer* data,
    StringRef prefix,
    Version version,
    Arena* a,
    // To provide span context, tags, debug ID to underlying lookups.
    GetMappedKeyValuesRequest* pOriginalReq) {
	state GetRangeReqAndResultRef getRange;
	state double getValuesStart = g_network->timer();
	getRange.begin = firstGreaterOrEqual(KeyRef(*a, prefix));
	getRange.end = firstGreaterOrEqual(strinc(prefix, *a));
	if (pOriginalReq->options.present() && pOriginalReq->options.get().debugID.present())
		g_traceBatch.addEvent("TransactionDebug",
		                      pOriginalReq->options.get().debugID.get().first(),
		                      "storageserver.quickGetKeyValues.Before");
	try {
		// TODO: Use a lower level API may be better?
		GetKeyValuesRequest req;
		req.spanContext = pOriginalReq->spanContext;
		req.options = pOriginalReq->options;
		req.arena = *a;
		req.begin = getRange.begin;
		req.end = getRange.end;
		req.version = version;
		req.tenantInfo = pOriginalReq->tenantInfo;
		// TODO: Validate when the underlying range query exceeds the limit.
		// TODO: Use remainingLimit, remainingLimitBytes rather than separate knobs.
		req.limit = SERVER_KNOBS->QUICK_GET_KEY_VALUES_LIMIT;
		req.limitBytes = SERVER_KNOBS->QUICK_GET_KEY_VALUES_LIMIT_BYTES;
		req.options = pOriginalReq->options;
		// TODO: tweak priorities in req.options.get().type?
		req.tags = pOriginalReq->tags;
		req.ssLatestCommitVersions = VersionVector();

		// Note that it does not use readGuard to avoid server being overloaded here. Throttling is enforced at the
		// original request level, rather than individual underlying lookups. The reason is that throttle any individual
		// underlying lookup will fail the original request, which is not productive.
		data->actors.add(getKeyValuesQ(data, req));
		GetKeyValuesReply reply = wait(req.reply.getFuture());
		if (!reply.error.present()) {
			++data->counters.quickGetKeyValuesHit;
			// Convert GetKeyValuesReply to RangeResult.
			a->dependsOn(reply.arena);
			getRange.result = RangeResultRef(reply.data, reply.more);
			const double duration = g_network->timer() - getValuesStart;
			data->counters.mappedRangeLocalSample->addMeasurement(duration);
			if (pOriginalReq->options.present() && pOriginalReq->options.get().debugID.present())
				g_traceBatch.addEvent("TransactionDebug",
				                      pOriginalReq->options.get().debugID.get().first(),
				                      "storageserver.quickGetKeyValues.AfterLocalFetch");
			return getRange;
		}
		// Otherwise fallback.
	} catch (Error& e) {
		// Fallback.
	}

	++data->counters.quickGetKeyValuesMiss;
	if (SERVER_KNOBS->QUICK_GET_KEY_VALUES_FALLBACK) {
		Optional<Reference<Tenant>> tenant = pOriginalReq->tenantInfo.hasTenant()
		                                         ? makeReference<Tenant>(pOriginalReq->tenantInfo.tenantId)
		                                         : Optional<Reference<Tenant>>();
		state Transaction tr(data->cx, tenant);
		tr.setVersion(version);
		if (pOriginalReq->options.present() && pOriginalReq->options.get().debugID.present()) {
			tr.debugTransaction(pOriginalReq->options.get().debugID.get());
		}
		// TODO: is DefaultPromiseEndpoint the best priority for this?
		tr.trState->taskID = TaskPriority::DefaultPromiseEndpoint;
		Future<RangeResult> rangeResultFuture =
		    tr.getRange(prefixRange(prefix), GetRangeLimits::ROW_LIMIT_UNLIMITED, Snapshot::True);
		// TODO: async in case it needs to read from other servers.
		RangeResult rangeResult = wait(rangeResultFuture);
		a->dependsOn(rangeResult.arena());
		getRange.result = rangeResult;
		const double duration = g_network->timer() - getValuesStart;
		data->counters.mappedRangeRemoteSample->addMeasurement(duration);
		if (pOriginalReq->options.present() && pOriginalReq->options.get().debugID.present())
			g_traceBatch.addEvent("TransactionDebug",
			                      pOriginalReq->options.get().debugID.get().first(),
			                      "storageserver.quickGetKeyValues.AfterRemoteFetch");
		return getRange;
	} else {
		throw quick_get_key_values_miss();
	}
}

void unpackKeyTuple(Tuple** referenceTuple, Optional<Tuple>& keyTuple, KeyValueRef* keyValue) {
	if (!keyTuple.present()) {
		// May throw exception if the key is not parsable as a tuple.
		try {
			keyTuple = Tuple::unpack(keyValue->key);
		} catch (Error& e) {
			TraceEvent("KeyNotTuple").error(e).detail("Key", keyValue->key.printable());
			throw key_not_tuple();
		}
	}
	*referenceTuple = &keyTuple.get();
}

void unpackValueTuple(Tuple** referenceTuple, Optional<Tuple>& valueTuple, KeyValueRef* keyValue) {
	if (!valueTuple.present()) {
		// May throw exception if the value is not parsable as a tuple.
		try {
			valueTuple = Tuple::unpack(keyValue->value);
		} catch (Error& e) {
			TraceEvent("ValueNotTuple").error(e).detail("Value", keyValue->value.printable());
			throw value_not_tuple();
		}
	}
	*referenceTuple = &valueTuple.get();
}

bool unescapeLiterals(std::string& s, std::string before, std::string after) {
	bool escaped = false;
	size_t p = 0;
	while (true) {
		size_t found = s.find(before, p);
		if (found == std::string::npos) {
			break;
		}
		s.replace(found, before.length(), after);
		p = found + after.length();
		escaped = true;
	}
	return escaped;
}

bool singleKeyOrValue(const std::string& s, size_t sz) {
	// format would be {K[??]} or {V[??]}
	return sz > 5 && s[0] == '{' && (s[1] == 'K' || s[1] == 'V') && s[2] == '[' && s[sz - 2] == ']' && s[sz - 1] == '}';
}

bool rangeQuery(const std::string& s) {
	return s == "{...}";
}

// create a vector of Optional<Tuple>
// in case of a singleKeyOrValue, insert an empty Tuple to vector as placeholder
// in case of a rangeQuery, insert Optional.empty as placeholder
// in other cases, insert the correct Tuple to be used.
void preprocessMappedKey(Tuple& mappedKeyFormatTuple, std::vector<Optional<Tuple>>& vt, bool& isRangeQuery) {
	vt.reserve(mappedKeyFormatTuple.size());

	for (int i = 0; i < mappedKeyFormatTuple.size(); i++) {
		Tuple::ElementType type = mappedKeyFormatTuple.getType(i);
		if (type == Tuple::BYTES || type == Tuple::UTF8) {
			std::string s = mappedKeyFormatTuple.getString(i).toString();
			auto sz = s.size();
			bool escaped = unescapeLiterals(s, "{{", "{");
			escaped = unescapeLiterals(s, "}}", "}") || escaped;
			if (escaped) {
				vt.emplace_back(Tuple::makeTuple(s));
			} else if (singleKeyOrValue(s, sz)) {
				// when it is SingleKeyOrValue, insert an empty Tuple to vector as placeholder
				vt.emplace_back(Tuple());
			} else if (rangeQuery(s)) {
				if (i != mappedKeyFormatTuple.size() - 1) {
					// It must be the last element of the mapper tuple
					throw mapper_bad_range_decriptor();
				}
				// when it is rangeQuery, insert Optional.empty as placeholder
				vt.emplace_back(Optional<Tuple>());
				isRangeQuery = true;
			} else {
				Tuple t;
				t.appendRaw(mappedKeyFormatTuple.subTupleRawString(i));
				vt.emplace_back(t);
			}
		} else {
			Tuple t;
			t.appendRaw(mappedKeyFormatTuple.subTupleRawString(i));
			vt.emplace_back(t);
		}
	}
}

Key constructMappedKey(KeyValueRef* keyValue, std::vector<Optional<Tuple>>& vec, Tuple& mappedKeyFormatTuple) {
	// Lazily parse key and/or value to tuple because they may not need to be a tuple if not used.
	Optional<Tuple> keyTuple;
	Optional<Tuple> valueTuple;
	Tuple mappedKeyTuple;

	mappedKeyTuple.reserve(vec.size());

	for (int i = 0; i < vec.size(); i++) {
		if (!vec[i].present()) {
			// rangeQuery
			continue;
		}
		if (vec[i].get().size()) {
			mappedKeyTuple.append(vec[i].get());
		} else {
			// singleKeyOrValue is true
			std::string s = mappedKeyFormatTuple.getString(i).toString();
			auto sz = s.size();
			int idx;
			Tuple* referenceTuple;
			try {
				idx = std::stoi(s.substr(3, sz - 5));
			} catch (std::exception& e) {
				throw mapper_bad_index();
			}
			if (s[1] == 'K') {
				unpackKeyTuple(&referenceTuple, keyTuple, keyValue);
			} else if (s[1] == 'V') {
				unpackValueTuple(&referenceTuple, valueTuple, keyValue);
			} else {
				ASSERT(false);
				throw internal_error();
			}
			if (idx < 0 || idx >= referenceTuple->size()) {
				throw mapper_bad_index();
			}
			mappedKeyTuple.appendRaw(referenceTuple->subTupleRawString(idx));
		}
	}

	return mappedKeyTuple.pack();
}

struct AuditGetShardInfoRes {
	Version readAtVersion;
	UID serverId;
	std::vector<KeyRange> ownRanges;
	AuditGetShardInfoRes() = default;
	AuditGetShardInfoRes(Version readAtVersion, UID serverId, std::vector<KeyRange> ownRanges)
	  : readAtVersion(readAtVersion), serverId(serverId), ownRanges(ownRanges) {}
};

// Given an input server, get ranges with in the input range
// from the perspective of SS->shardInfo
// Input: (1) SS ID; (2) within range
// Return AuditGetShardInfoRes including: (1) version of the read; (2) ranges of the SS
AuditGetShardInfoRes getThisServerShardInfo(StorageServer* data, KeyRange range) {
	std::vector<KeyRange> ownRange;
	for (auto& t : data->shards.intersectingRanges(range)) {
		KeyRange alignedRange = t.value()->keys & range;
		if (alignedRange.empty()) {
			TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
			           "SSAuditStorageReadShardInfoEmptyAlignedRange",
			           data->thisServerID)
			    .detail("Range", range);
			throw audit_storage_cancelled();
		}
		TraceEvent(SevVerbose, "SSAuditStorageGetThisServerShardInfo", data->thisServerID)
		    .detail("AlignedRange", alignedRange)
		    .detail("Range", t.value()->keys)
		    .detail("AtVersion", data->version.get())
		    .detail("AuditServer", data->thisServerID)
		    .detail("ReadWrite", t.value()->readWrite ? "True" : "False")
		    .detail("Adding", t.value()->adding ? "True" : "False");
		if (t.value()->assigned()) {
			ownRange.push_back(alignedRange);
		}
	}
	return AuditGetShardInfoRes(data->version.get(), data->thisServerID, ownRange);
}

// Check consistency between StorageServer->shardInfo and ServerKeys system key space
ACTOR Future<Void> auditStorageServerShardQ(StorageServer* data, AuditStorageRequest req) {
	ASSERT(req.getType() == AuditType::ValidateStorageServerShard);
	wait(data->serveAuditStorageParallelismLock.take(TaskPriority::DefaultYield));
	// The trackShardAssignment is correct when at most 1 auditStorageServerShardQ runs
	// at a time. Currently, this is guaranteed by setting serveAuditStorageParallelismLock == 1
	// If serveAuditStorageParallelismLock > 1, we need to check trackShardAssignmentMinVersion
	// to make sure no onging auditStorageServerShardQ is running
	if (data->trackShardAssignmentMinVersion != invalidVersion) {
		// Another auditStorageServerShardQ is running
		req.reply.sendError(audit_storage_cancelled());
		TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
		           "ExistStorageServerShardAuditExit") // unexpected
		    .detail("NewAuditId", req.id)
		    .detail("NewAuditType", req.getType());
		return Void();
	}
	state FlowLock::Releaser holder(data->serveAuditStorageParallelismLock);
	TraceEvent(SevInfo, "SSAuditStorageSsShardBegin", data->thisServerID)
	    .detail("AuditId", req.id)
	    .detail("AuditRange", req.range);
	state AuditStorageState res(req.id, data->thisServerID, req.getType());
	state std::vector<std::string> errors;
	state std::vector<Future<Void>> fs;
	state Transaction tr(data->cx);
	state AuditGetServerKeysRes serverKeyRes;
	state Version serverKeyReadAtVersion;
	state KeyRange serverKeyCompleteRange;
	state AuditGetKeyServersRes keyServerRes;
	state Version keyServerReadAtVersion;
	state KeyRange keyServerCompleteRange;
	state AuditGetShardInfoRes ownRangesLocalViewRes;
	state Version localShardInfoReadAtVersion;
	// We want to find out any mismatch between ownRangesSeenByServerKey and ownRangesLocalView and
	// ownRangesSeenByKeyServerMap
	state std::unordered_map<UID, std::vector<KeyRange>> ownRangesSeenByKeyServerMap;
	state std::vector<KeyRange> ownRangesSeenByServerKey;
	state std::vector<KeyRange> ownRangesSeenByKeyServer;
	state std::vector<KeyRange> ownRangesLocalView;
	state std::string failureReason;
	// Note that since krmReadRange may not return the value of the entire range at a time
	// Given req.range, a part of the range is returned, thus, only a part of the range is
	// able to be compared --- claimRange
	// Given claimRange, rangeToRead is decided for reading the remaining range
	// At beginning, rangeToRead is req.range
	state KeyRange claimRange;
	state Key rangeToReadBegin = req.range.begin;
	state KeyRangeRef rangeToRead;
	state int retryCount = 0;
	state int64_t cumulatedValidatedLocalShardsNum = 0;
	state int64_t cumulatedValidatedServerKeysNum = 0;
	state Reference<IRateControl> rateLimiter =
	    Reference<IRateControl>(new SpeedLimit(SERVER_KNOBS->AUDIT_STORAGE_RATE_PER_SERVER_MAX, 1));
	state int64_t remoteReadBytes = 0;
	state double startTime = now();
	state double lastRateLimiterWaitTime = 0;
	state double rateLimiterBeforeWaitTime = 0;
	state double rateLimiterTotalWaitTime = 0;

	try {
		loop {
			try {
				if (data->version.get() == 0) {
					failureReason = "SS version is 0";
					throw audit_storage_failed();
				}

				// Read serverKeys and shardInfo
				errors.clear();
				// We do not reset retryCount for each partial read range
				ownRangesLocalView.clear();
				ownRangesSeenByServerKey.clear();
				ownRangesSeenByKeyServer.clear();
				ownRangesSeenByKeyServerMap.clear();
				rangeToRead = KeyRangeRef(rangeToReadBegin, req.range.end);

				// At this point, shard assignment history guarantees to contain assignments
				// from localShardInfoReadAtVersion
				ownRangesLocalViewRes = getThisServerShardInfo(data, rangeToRead);
				localShardInfoReadAtVersion = ownRangesLocalViewRes.readAtVersion;
				if (localShardInfoReadAtVersion != data->version.get()) {
					TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
					           "SSAuditStorageSsShardGRVMismatchError",
					           data->thisServerID);
					throw audit_storage_cancelled();
				}

				// Request to record shard assignment history at least localShardInfoReadAtVersion
				data->startTrackShardAssignment(localShardInfoReadAtVersion);
				TraceEvent(SevVerbose, "SSShardAssignmentHistoryRecordStart", data->thisServerID)
				    .detail("AuditID", req.id);

				// Transactional read of serverKeys
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				fs.clear();
				fs.push_back(
				    store(serverKeyRes, getThisServerKeysFromServerKeys(data->thisServerID, &tr, rangeToRead)));
				fs.push_back(store(keyServerRes, getShardMapFromKeyServers(data->thisServerID, &tr, rangeToRead)));
				wait(waitForAll(fs));
				// Get serverKeys result
				serverKeyCompleteRange = serverKeyRes.completeRange;
				serverKeyReadAtVersion = serverKeyRes.readAtVersion;
				// Get keyServers result
				keyServerCompleteRange = keyServerRes.completeRange;
				keyServerReadAtVersion = keyServerRes.readAtVersion;
				// Get bytes read
				remoteReadBytes = keyServerRes.readBytes + serverKeyRes.readBytes;
				// We want to do transactional read at a version newer than data->version
				while (serverKeyReadAtVersion < localShardInfoReadAtVersion) {
					if (retryCount >= SERVER_KNOBS->AUDIT_RETRY_COUNT_MAX) {
						failureReason = "Read serverKeys retry count exceeds the max";
						throw audit_storage_failed();
					}
					wait(rateLimiter->getAllowance(remoteReadBytes)); // RateKeeping
					retryCount++;
					wait(delay(0.5));
					tr.reset();
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::LOCK_AWARE);
					fs.clear();
					fs.push_back(
					    store(serverKeyRes, getThisServerKeysFromServerKeys(data->thisServerID, &tr, rangeToRead)));
					fs.push_back(store(keyServerRes, getShardMapFromKeyServers(data->thisServerID, &tr, rangeToRead)));
					wait(waitForAll(fs));
					// Get serverKeys result
					serverKeyCompleteRange = serverKeyRes.completeRange;
					serverKeyReadAtVersion = serverKeyRes.readAtVersion;
					// Get keyServers result
					keyServerCompleteRange = keyServerRes.completeRange;
					keyServerReadAtVersion = keyServerRes.readAtVersion;
					// Get bytes read
					remoteReadBytes = keyServerRes.readBytes + serverKeyRes.readBytes;
				} // retry until serverKeyReadAtVersion is as larger as localShardInfoReadAtVersion
				// Check versions
				if (serverKeyReadAtVersion < localShardInfoReadAtVersion) {
					TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
					           "SSAuditStorageSsShardComparedVersionError",
					           data->thisServerID);
					throw audit_storage_cancelled();
				}
				if (keyServerReadAtVersion != serverKeyReadAtVersion) {
					TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
					           "SSAuditStorageSsShardKSVersionMismatchError",
					           data->thisServerID);
					throw audit_storage_cancelled();
				}
				try {
					wait(timeoutError(data->version.whenAtLeast(serverKeyReadAtVersion), 30));
				} catch (Error& e) {
					TraceEvent(SevWarn, "SSAuditStorageSsShardWaitSSVersionTooLong", data->thisServerID)
					    .detail("ServerKeyReadAtVersion", serverKeyReadAtVersion)
					    .detail("SSVersion", data->version.get());
					failureReason = "SS version takes long time to catch up with serverKeyReadAtVersion";
					throw audit_storage_failed();
				}
				// At this point, shard assignment history guarantees to contain assignments
				// upto serverKeyReadAtVersion

				// Stop requesting to record shard assignment history
				data->stopTrackShardAssignment();
				TraceEvent(SevVerbose, "ShardAssignmentHistoryRecordStop", data->thisServerID)
				    .detail("AuditID", req.id);

				// check any serverKey update between localShardInfoReadAtVersion and serverKeyReadAtVersion
				std::vector<std::pair<Version, KeyRangeRef>> shardAssignments =
				    data->getShardAssignmentHistory(localShardInfoReadAtVersion, serverKeyReadAtVersion);
				TraceEvent(SevInfo, "SSAuditStorageSsShardGetHistory", data->thisServerID)
				    .detail("AuditId", req.id)
				    .detail("AuditRange", req.range)
				    .detail("ServerKeyAtVersion", serverKeyReadAtVersion)
				    .detail("LocalShardInfoAtVersion", localShardInfoReadAtVersion)
				    .detail("ShardAssignmentsCount", shardAssignments.size());
				// Ideally, we revert ownRangesLocalView changes by shard assignment history
				// Currently, we give up if any update collected
				if (!shardAssignments.empty()) {
					failureReason = "Shard assignment history is not empty";
					throw audit_storage_failed();
				}
				// Get claim range
				KeyRange claimRange = rangeToRead;
				claimRange = claimRange & serverKeyCompleteRange;
				if (claimRange.empty()) {
					TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
					           "SSAuditStorageSsShardOverlapRangeEmpty",
					           data->thisServerID);
					throw audit_storage_cancelled();
				}
				claimRange = claimRange & keyServerCompleteRange;
				if (claimRange.empty()) {
					TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
					           "SSAuditStorageSsShardOverlapRangeEmpty",
					           data->thisServerID);
					throw audit_storage_cancelled();
				}
				// We only compare within claimRange
				// Get ownRangesLocalView within claimRange
				for (auto& range : ownRangesLocalViewRes.ownRanges) {
					KeyRange overlappingRange = range & claimRange;
					if (overlappingRange.empty()) {
						continue;
					}
					ownRangesLocalView.push_back(overlappingRange);
				}
				// Get ownRangesSeenByServerKey within claimRange
				for (auto& range : serverKeyRes.ownRanges) {
					KeyRange overlappingRange = range & claimRange;
					if (overlappingRange.empty()) {
						continue;
					}
					ownRangesSeenByServerKey.push_back(overlappingRange);
				}
				// Get ownRangesSeenByKeyServer within claimRange
				if (keyServerRes.rangeOwnershipMap.contains(data->thisServerID)) {
					std::vector mergedRanges = coalesceRangeList(keyServerRes.rangeOwnershipMap[data->thisServerID]);
					for (auto& range : mergedRanges) {
						KeyRange overlappingRange = range & claimRange;
						if (overlappingRange.empty()) {
							continue;
						}
						ownRangesSeenByKeyServer.push_back(overlappingRange);
					}
				}
				TraceEvent(SevInfo, "SSAuditStorageSsShardReadDone", data->thisServerID)
				    .detail("AuditId", req.id)
				    .detail("AuditRange", req.range)
				    .detail("ClaimRange", claimRange)
				    .detail("ServerKeyAtVersion", serverKeyReadAtVersion)
				    .detail("ShardInfoAtVersion", data->version.get());

				// Log statistic
				cumulatedValidatedLocalShardsNum = cumulatedValidatedLocalShardsNum + ownRangesLocalView.size();
				cumulatedValidatedServerKeysNum = cumulatedValidatedServerKeysNum + ownRangesSeenByServerKey.size();
				TraceEvent(SevInfo, "SSAuditStorageStatisticShardInfo", data->thisServerID)
				    .suppressFor(30.0)
				    .detail("AuditType", req.getType())
				    .detail("AuditId", req.id)
				    .detail("AuditRange", req.range)
				    .detail("CurrentValidatedLocalShardsNum", ownRangesLocalView.size())
				    .detail("CurrentValidatedServerKeysNum", ownRangesSeenByServerKey.size())
				    .detail("CurrentValidatedInclusiveRange", claimRange)
				    .detail("CumulatedValidatedLocalShardsNum", cumulatedValidatedLocalShardsNum)
				    .detail("CumulatedValidatedServerKeysNum", cumulatedValidatedServerKeysNum)
				    .detail("CumulatedValidatedInclusiveRange", KeyRangeRef(req.range.begin, claimRange.end));

				// Compare
				// Compare keyServers and serverKeys
				if (ownRangesSeenByKeyServer.empty()) {
					if (!ownRangesSeenByServerKey.empty()) {
						std::string error =
						    format("ServerKeys shows %zu ranges that not appear on keyServers for Server(%s): ",
						           ownRangesSeenByServerKey.size(),
						           data->thisServerID.toString().c_str(),
						           describe(ownRangesSeenByServerKey).c_str());
						TraceEvent(SevError, "SSAuditStorageSsShardError", data->thisServerID)
						    .setMaxFieldLength(-1)
						    .setMaxEventLength(-1)
						    .detail("AuditId", req.id)
						    .detail("AuditRange", req.range)
						    .detail("ClaimRange", claimRange)
						    .detail("ErrorMessage", error)
						    .detail("MismatchedRangeByLocalView", describe(ownRangesSeenByServerKey))
						    .detail("AuditServer", data->thisServerID);
					}
				} else {
					Optional<std::pair<KeyRange, KeyRange>> anyMismatch =
					    rangesSame(ownRangesSeenByServerKey, ownRangesSeenByKeyServer);
					if (anyMismatch.present()) { // mismatch detected
						KeyRange mismatchedRangeByServerKey = anyMismatch.get().first;
						KeyRange mismatchedRangeByKeyServer = anyMismatch.get().second;
						std::string error =
						    format("KeyServers and serverKeys mismatch on Server(%s): ServerKey: %s; KeyServer: %s",
						           data->thisServerID.toString().c_str(),
						           mismatchedRangeByServerKey.toString().c_str(),
						           mismatchedRangeByKeyServer.toString().c_str());
						TraceEvent(SevError, "SSAuditStorageSsShardError", data->thisServerID)
						    .setMaxFieldLength(-1)
						    .setMaxEventLength(-1)
						    .detail("AuditId", req.id)
						    .detail("AuditRange", req.range)
						    .detail("ClaimRange", claimRange)
						    .detail("ErrorMessage", error)
						    .detail("MismatchedRangeByKeyServer", mismatchedRangeByKeyServer)
						    .detail("MismatchedRangeByServerKey", mismatchedRangeByServerKey)
						    .detail("AuditServer", data->thisServerID);
						errors.push_back(error);
					}
				}

				// Compare SS shard info and serverKeys
				Optional<std::pair<KeyRange, KeyRange>> anyMismatch =
				    rangesSame(ownRangesSeenByServerKey, ownRangesLocalView);
				if (anyMismatch.present()) { // mismatch detected
					KeyRange mismatchedRangeByServerKey = anyMismatch.get().first;
					KeyRange mismatchedRangeByLocalView = anyMismatch.get().second;
					std::string error =
					    format("Storage server shard info mismatch on Server(%s): ServerKey: %s; ServerShardInfo: %s",
					           data->thisServerID.toString().c_str(),
					           mismatchedRangeByServerKey.toString().c_str(),
					           mismatchedRangeByLocalView.toString().c_str());
					TraceEvent(SevError, "SSAuditStorageSsShardError", data->thisServerID)
					    .setMaxFieldLength(-1)
					    .setMaxEventLength(-1)
					    .detail("AuditId", req.id)
					    .detail("AuditRange", req.range)
					    .detail("ClaimRange", claimRange)
					    .detail("ErrorMessage", error)
					    .detail("MismatchedRangeByLocalView", mismatchedRangeByLocalView)
					    .detail("MismatchedRangeByServerKey", mismatchedRangeByServerKey)
					    .detail("AuditServer", data->thisServerID);
					errors.push_back(error);
				}

				// Return result
				if (!errors.empty()) {
					TraceEvent(SevVerbose, "SSAuditStorageSsShardErrorEnd", data->thisServerID)
					    .detail("AuditId", req.id)
					    .detail("AuditRange", req.range)
					    .detail("AuditServer", data->thisServerID);
					res.range = claimRange;
					res.setPhase(AuditPhase::Error);
					if (!req.ddId.isValid()) {
						TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
						           "SSAuditStorageSsShardDDIdInvalid",
						           data->thisServerID);
						throw audit_storage_cancelled();
					}
					res.ddId = req.ddId; // used to compare req.ddId with existing persisted ddId
					wait(persistAuditStateByServer(data->cx, res));
					req.reply.sendError(audit_storage_error());
					break;
				} else {
					// Expand persisted complete range
					res.range = Standalone(KeyRangeRef(req.range.begin, claimRange.end));
					res.setPhase(AuditPhase::Complete);
					if (!req.ddId.isValid()) {
						TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
						           "SSAuditStorageSsShardDDIdInvalid",
						           data->thisServerID);
						throw audit_storage_cancelled();
					}
					res.ddId = req.ddId; // used to compare req.ddId with existing persisted ddId
					wait(persistAuditStateByServer(data->cx, res));
					if (res.range.end < req.range.end) {
						TraceEvent(SevInfo, "SSAuditStorageSsShardPartialDone", data->thisServerID)
						    .suppressFor(10.0)
						    .detail("AuditId", req.id)
						    .detail("AuditRange", req.range)
						    .detail("AuditServer", data->thisServerID)
						    .detail("CompleteRange", res.range)
						    .detail("ClaimRange", claimRange)
						    .detail("RangeToReadEnd", req.range.end)
						    .detail("LastRateLimiterWaitTime", lastRateLimiterWaitTime)
						    .detail("RateLimiterTotalWaitTime", rateLimiterTotalWaitTime);
						rangeToReadBegin = res.range.end;
					} else { // complete
						req.reply.send(res);
						TraceEvent(SevInfo, "SSAuditStorageSsShardComplete", data->thisServerID)
						    .detail("AuditId", req.id)
						    .detail("AuditRange", req.range)
						    .detail("AuditServer", data->thisServerID)
						    .detail("ClaimRange", claimRange)
						    .detail("CompleteRange", res.range)
						    .detail("NumValidatedLocalShards", cumulatedValidatedLocalShardsNum)
						    .detail("NumValidatedServerKeys", cumulatedValidatedServerKeysNum)
						    .detail("RateLimiterTotalWaitTime", rateLimiterTotalWaitTime)
						    .detail("TotalTime", now() - startTime);
						break;
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					// In this case, we need not stop tracking shard assignment
					// The shard history will not get unboundedly large for this case
					// When this actor gets cancelled, data will be eventually destroyed
					// Therefore, the shard history will be destroyed
					throw e;
				}
				data->stopTrackShardAssignment();
				wait(tr.onError(e));
			}

			rateLimiterBeforeWaitTime = now();
			wait(rateLimiter->getAllowance(remoteReadBytes)); // RateKeeping
			lastRateLimiterWaitTime = now() - rateLimiterBeforeWaitTime;
			rateLimiterTotalWaitTime = rateLimiterTotalWaitTime + lastRateLimiterWaitTime;
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			return Void(); // silently exit
		}
		TraceEvent(SevInfo, "SSAuditStorageSsShardFailed", data->thisServerID)
		    .errorUnsuppressed(e)
		    .detail("AuditId", req.id)
		    .detail("AuditRange", req.range)
		    .detail("AuditServer", data->thisServerID)
		    .detail("Reason", failureReason)
		    .detail("RateLimiterTotalWaitTime", rateLimiterTotalWaitTime)
		    .detail("TotalTime", now() - startTime);
		// Make sure the history collection is not open due to this audit
		data->stopTrackShardAssignment();
		TraceEvent(SevVerbose, "SSShardAssignmentHistoryRecordStopWhenError", data->thisServerID)
		    .detail("AuditID", req.id);

		if (e.code() == error_code_audit_storage_cancelled) {
			req.reply.sendError(audit_storage_cancelled());
		} else if (e.code() == error_code_audit_storage_task_outdated) {
			req.reply.sendError(audit_storage_task_outdated());
		} else {
			req.reply.sendError(audit_storage_failed());
		}
	}

	// Make sure the history collection is not open due to this audit
	data->stopTrackShardAssignment();
	TraceEvent(SevVerbose, "SSShardAssignmentHistoryRecordStopWhenExit", data->thisServerID).detail("AuditID", req.id);

	return Void();
}

ACTOR Future<Void> auditStorageShardReplicaQ(StorageServer* data, AuditStorageRequest req) {
	ASSERT(req.getType() == AuditType::ValidateHA || req.getType() == AuditType::ValidateReplica);
	wait(data->serveAuditStorageParallelismLock.take(TaskPriority::DefaultYield));
	state FlowLock::Releaser holder(data->serveAuditStorageParallelismLock);

	TraceEvent(SevInfo, "SSAuditStorageShardReplicaBegin", data->thisServerID)
	    .detail("AuditID", req.id)
	    .detail("AuditRange", req.range)
	    .detail("AuditType", req.type)
	    .detail("TargetServers", describe(req.targetServers));

	state AuditStorageState res(req.id, req.getType()); // we will set range of audit later
	state std::vector<Optional<Value>> serverListValues;
	state std::vector<Future<ErrorOr<GetKeyValuesReply>>> fs;
	state std::vector<std::string> errors;
	state Version version;
	state Transaction tr(data->cx);
	state KeyRange rangeToRead = req.range;
	state Key rangeToReadBegin = req.range.begin;
	state KeyRange claimRange;
	state int limit = 1e4;
	state int limitBytes = CLIENT_KNOBS->REPLY_BYTE_LIMIT;
	state int64_t readBytes = 0;
	state int64_t numValidatedKeys = 0;
	state int64_t validatedBytes = 0;
	state bool complete = false;
	state int64_t checkTimes = 0;
	state double startTime = now();
	state double lastRateLimiterWaitTime = 0;
	state double rateLimiterBeforeWaitTime = 0;
	state double rateLimiterTotalWaitTime = 0;
	state Reference<IRateControl> rateLimiter =
	    Reference<IRateControl>(new SpeedLimit(SERVER_KNOBS->AUDIT_STORAGE_RATE_PER_SERVER_MAX, 1));

	try {
		loop {
			try {
				readBytes = 0;
				rangeToRead = KeyRangeRef(rangeToReadBegin, req.range.end);
				TraceEvent(SevDebug, "SSAuditStorageShardReplicaNewRoundBegin", data->thisServerID)
				    .suppressFor(10.0)
				    .detail("AuditID", req.id)
				    .detail("AuditRange", req.range)
				    .detail("AuditType", req.type)
				    .detail("ReadRangeBegin", rangeToReadBegin)
				    .detail("ReadRangeEnd", req.range.end);
				serverListValues.clear();
				errors.clear();
				fs.clear();
				tr.reset();
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

				// Get SS interfaces
				std::vector<Future<Optional<Value>>> serverListEntries;
				for (const UID& id : req.targetServers) {
					if (id != data->thisServerID) {
						serverListEntries.push_back(tr.get(serverListKeyFor(id)));
					}
				}
				wait(store(serverListValues, getAll(serverListEntries)));

				// Decide version to compare
				wait(store(version, tr.getReadVersion()));

				// Read remote servers
				for (const auto& v : serverListValues) {
					if (!v.present()) {
						TraceEvent(SevWarn, "SSAuditStorageShardReplicaRemoteServerNotFound", data->thisServerID)
						    .detail("AuditID", req.id)
						    .detail("AuditRange", req.range)
						    .detail("AuditType", req.type);
						throw audit_storage_failed();
					}
					StorageServerInterface remoteServer = decodeServerListValue(v.get());

					GetKeyValuesRequest req;
					req.begin = firstGreaterOrEqual(rangeToRead.begin);
					req.end = firstGreaterOrEqual(rangeToRead.end);
					req.limit = limit;
					req.limitBytes = limitBytes;
					req.version = version;
					req.tags = TagSet();
					fs.push_back(remoteServer.getKeyValues.getReplyUnlessFailedFor(req, 2, 0));
				}

				// Read local server
				GetKeyValuesRequest localReq;
				localReq.begin = firstGreaterOrEqual(rangeToRead.begin);
				localReq.end = firstGreaterOrEqual(rangeToRead.end);
				localReq.limit = limit;
				localReq.limitBytes = limitBytes;
				localReq.version = version;
				localReq.tags = TagSet();
				data->actors.add(getKeyValuesQ(data, localReq));
				fs.push_back(errorOr(localReq.reply.getFuture()));
				std::vector<ErrorOr<GetKeyValuesReply>> reps = wait(getAll(fs));
				// Note: getAll() must keep the order of fs

				// Check read result
				for (int i = 0; i < reps.size(); ++i) {
					if (reps[i].isError()) {
						TraceEvent(SevWarn, "SSAuditStorageShardReplicaGetKeyValuesError", data->thisServerID)
						    .errorUnsuppressed(reps[i].getError())
						    .detail("AuditID", req.id)
						    .detail("AuditRange", req.range)
						    .detail("AuditType", req.type)
						    .detail("ReplyIndex", i)
						    .detail("RangeRead", rangeToRead);
						throw reps[i].getError();
					}
					if (reps[i].get().error.present()) {
						TraceEvent(SevWarn, "SSAuditStorageShardReplicaGetKeyValuesError", data->thisServerID)
						    .errorUnsuppressed(reps[i].get().error.get())
						    .detail("AuditID", req.id)
						    .detail("AuditRange", req.range)
						    .detail("AuditType", req.type)
						    .detail("ReplyIndex", i)
						    .detail("RangeRead", rangeToRead);
						throw reps[i].get().error.get();
					}
					readBytes = readBytes + reps[i].get().data.expectedSize();
					validatedBytes = validatedBytes + reps[i].get().data.expectedSize();
					// If any of reps finishes read, we think we complete
					// Even some rep does not finish read, this unfinished rep has more key than
					// the complete rep, which will lead to missKey inconsistency in
					// this round of check
					if (!reps[i].get().more) {
						complete = true;
					}
				}

				// Validation
				claimRange = rangeToRead;
				const GetKeyValuesReply& local = reps.back().get();
				if (serverListValues.size() != reps.size() - 1) {
					TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
					           "SSAuditStorageShardReplicaRepsLengthWrong",
					           data->thisServerID)
					    .detail("ServerListValuesSize", serverListValues.size())
					    .detail("RepsSize", reps.size());
					throw audit_storage_cancelled();
				}
				if (reps.size() == 1) {
					// if no other server to compare
					TraceEvent(SevWarn, "SSAuditStorageShardReplicaNothingToCompare", data->thisServerID)
					    .detail("AuditID", req.id)
					    .detail("AuditRange", req.range)
					    .detail("AuditType", req.type)
					    .detail("TargetServers", describe(req.targetServers));
					complete = true;
				}
				// Compare local and each remote one by one
				// The last one of reps is local, so skip it
				for (int repIdx = 0; repIdx < reps.size() - 1; repIdx++) {
					const GetKeyValuesReply& remote = reps[repIdx].get();
					// serverListValues and reps should be same order
					if (!serverListValues[repIdx].present()) { // if not, already throw audit_storage_failed
						TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
						           "SSAuditStorageShardReplicaRepIdxNotPresent",
						           data->thisServerID)
						    .detail("RepIdx", repIdx);
						throw audit_storage_cancelled();
					}
					const StorageServerInterface& remoteServer = decodeServerListValue(serverListValues[repIdx].get());
					Key lastKey = rangeToRead.begin;
					const int end = std::min(local.data.size(), remote.data.size());
					bool missingKey = local.data.size() != remote.data.size();
					// Compare each key one by one
					std::string error;
					int i = 0;
					for (; i < end; ++i) {
						KeyValueRef remoteKV = remote.data[i];
						KeyValueRef localKV = local.data[i];
						if (!req.range.contains(remoteKV.key) || !req.range.contains(localKV.key)) {
							TraceEvent(SevWarn, "SSAuditStorageShardReplicaKeyOutOfRange", data->thisServerID)
							    .detail("AuditRange", req.range)
							    .detail("RemoteServer", remoteServer.toString())
							    .detail("LocalKey", localKV.key)
							    .detail("RemoteKey", remoteKV.key);
							throw wrong_shard_server();
						}
						// Check if mismatch
						if (remoteKV.key != localKV.key) {
							error = format("Key Mismatch: local server (%016llx): %s, remote server(%016llx) %s",
							               data->thisServerID.first(),
							               Traceable<StringRef>::toString(localKV.key).c_str(),
							               remoteServer.uniqueID.first(),
							               Traceable<StringRef>::toString(remoteKV.key).c_str());
							TraceEvent(SevError, "SSAuditStorageShardReplicaError", data->thisServerID)
							    .setMaxFieldLength(-1)
							    .setMaxEventLength(-1)
							    .detail("AuditId", req.id)
							    .detail("AuditRange", req.range)
							    .detail("ErrorMessage", error)
							    .detail("Version", version)
							    .detail("ClaimRange", claimRange);
							errors.push_back(error);
							break;
						} else if (remoteKV.value != localKV.value) {
							error = format(
							    "Value Mismatch for Key %s: local server (%016llx): %s, remote server(%016llx) %s",
							    Traceable<StringRef>::toString(localKV.key).c_str(),
							    data->thisServerID.first(),
							    Traceable<StringRef>::toString(localKV.value).c_str(),
							    remoteServer.uniqueID.first(),
							    Traceable<StringRef>::toString(remoteKV.value).c_str());
							TraceEvent(SevError, "SSAuditStorageShardReplicaError", data->thisServerID)
							    .setMaxFieldLength(-1)
							    .setMaxEventLength(-1)
							    .detail("AuditId", req.id)
							    .detail("AuditRange", req.range)
							    .detail("ErrorMessage", error)
							    .detail("Version", version)
							    .detail("ClaimRange", claimRange);
							errors.push_back(error);
							break;
						} else {
							TraceEvent(SevVerbose, "SSAuditStorageShardReplicaValidatedKey", data->thisServerID)
							    .detail("Key", localKV.key);
						}
						++numValidatedKeys;
						lastKey = localKV.key;
					}
					KeyRange completeRange = Standalone(KeyRangeRef(rangeToRead.begin, keyAfter(lastKey)));
					if (completeRange.empty() || claimRange.begin != completeRange.begin) {
						TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
						           "SSAuditStorageShardReplicaCompleteRangeUnexpected",
						           data->thisServerID)
						    .detail("ClaimRange", claimRange)
						    .detail("CompleteRange", completeRange);
						throw audit_storage_cancelled();
					}
					claimRange = claimRange & completeRange;
					if (!error.empty()) { // if key or value mismatch detected
						continue; // check next remote server
					}
					if (!local.more && !remote.more && local.data.size() == remote.data.size()) {
						continue; // check next remote server
					} else if (i >= local.data.size() && !local.more && i < remote.data.size()) {
						if (!missingKey) {
							TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
							           "SSAuditStorageShardReplicaMissingKeyUnexpected",
							           data->thisServerID);
						}
						std::string error =
						    format("Missing key(s) form local server (%lld), next key: %s, remote server(%016llx) ",
						           data->thisServerID.first(),
						           Traceable<StringRef>::toString(remote.data[i].key).c_str(),
						           remoteServer.uniqueID.first());
						TraceEvent(SevError, "SSAuditStorageShardReplicaError", data->thisServerID)
						    .setMaxFieldLength(-1)
						    .setMaxEventLength(-1)
						    .detail("AuditId", req.id)
						    .detail("AuditRange", req.range)
						    .detail("ErrorMessage", error)
						    .detail("Version", version)
						    .detail("ClaimRange", claimRange);
						errors.push_back(error);
						continue; // check next remote server
					} else if (i >= remote.data.size() && !remote.more && i < local.data.size()) {
						if (!missingKey) {
							TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
							           "SSAuditStorageShardReplicaMissingKeyUnexpected",
							           data->thisServerID);
						}
						std::string error =
						    format("Missing key(s) form remote server (%lld), next local server(%016llx) key: %s",
						           remoteServer.uniqueID.first(),
						           data->thisServerID.first(),
						           Traceable<StringRef>::toString(local.data[i].key).c_str());
						TraceEvent(SevError, "SSAuditStorageShardReplicaError", data->thisServerID)
						    .setMaxFieldLength(-1)
						    .setMaxEventLength(-1)
						    .detail("AuditId", req.id)
						    .detail("AuditRange", req.range)
						    .detail("ErrorMessage", error)
						    .detail("Version", version)
						    .detail("ClaimRange", claimRange);
						errors.push_back(error);
						continue; // check next remote server
					}
				}

				TraceEvent(SevInfo, "SSAuditStorageStatisticValidateReplica", data->thisServerID)
				    .suppressFor(30.0)
				    .detail("AuditID", req.id)
				    .detail("AuditRange", req.range)
				    .detail("AuditType", req.type)
				    .detail("AuditServer", data->thisServerID)
				    .detail("ReplicaServers", req.targetServers)
				    .detail("CheckTimes", checkTimes)
				    .detail("NumValidatedKeys", numValidatedKeys)
				    .detail("CurrentValidatedInclusiveRange", claimRange)
				    .detail("CumulatedValidatedInclusiveRange", KeyRangeRef(req.range.begin, claimRange.end));

				// Return result
				if (!errors.empty()) {
					TraceEvent(SevError, "SSAuditStorageShardReplicaError", data->thisServerID)
					    .setMaxFieldLength(-1)
					    .setMaxEventLength(-1)
					    .detail("AuditId", req.id)
					    .detail("AuditRange", req.range)
					    .detail("ErrorCount", errors.size())
					    .detail("Version", version)
					    .detail("ClaimRange", claimRange);
					res.range = claimRange;
					res.setPhase(AuditPhase::Error);
					if (!req.ddId.isValid()) {
						TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
						           "SSAuditStorageShardReplicaDDIdInvalid",
						           data->thisServerID);
						throw audit_storage_cancelled();
					}
					res.ddId = req.ddId; // used to compare req.ddId with existing persisted ddId
					wait(persistAuditStateByRange(data->cx, res));
					req.reply.sendError(audit_storage_error());
					break;
				} else {
					if (complete || checkTimes % 100 == 0) {
						if (complete) {
							res.range = req.range;
						} else {
							res.range = Standalone(KeyRangeRef(req.range.begin, claimRange.end));
						}
						res.setPhase(AuditPhase::Complete);
						if (!req.ddId.isValid()) {
							TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
							           "SSAuditStorageShardReplicaDDIdInvalid",
							           data->thisServerID);
							throw audit_storage_cancelled();
						}
						res.ddId = req.ddId; // used to compare req.ddId with existing persisted ddId
						TraceEvent(SevInfo, "SSAuditStorageShardReplicaProgressPersist", data->thisServerID)
						    .suppressFor(10.0)
						    .detail("AuditId", req.id)
						    .detail("AuditRange", req.range)
						    .detail("AuditServer", data->thisServerID)
						    .detail("Progress", res.toString());
						wait(persistAuditStateByRange(data->cx, res));
					}
					// Expand persisted complete range
					if (complete) {
						req.reply.send(res);
						TraceEvent(SevInfo, "SSAuditStorageShardReplicaComplete", data->thisServerID)
						    .detail("AuditId", req.id)
						    .detail("AuditRange", req.range)
						    .detail("AuditServer", data->thisServerID)
						    .detail("ReplicaServers", req.targetServers)
						    .detail("ClaimRange", claimRange)
						    .detail("CompleteRange", res.range)
						    .detail("CheckTimes", checkTimes)
						    .detail("NumValidatedKeys", numValidatedKeys)
						    .detail("ValidatedBytes", validatedBytes)
						    .detail("RateLimiterTotalWaitTime", rateLimiterTotalWaitTime)
						    .detail("TotalTime", now() - startTime);
						break;
					} else {
						TraceEvent(SevInfo, "SSAuditStorageShardReplicaPartialDone", data->thisServerID)
						    .suppressFor(10.0)
						    .detail("AuditId", req.id)
						    .detail("AuditRange", req.range)
						    .detail("AuditServer", data->thisServerID)
						    .detail("ReplicaServers", req.targetServers)
						    .detail("ClaimRange", claimRange)
						    .detail("CompleteRange", res.range)
						    .detail("LastRateLimiterWaitTime", lastRateLimiterWaitTime)
						    .detail("RateLimiterTotalWaitTime", rateLimiterTotalWaitTime);
						rangeToReadBegin = claimRange.end;
					}
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}

			rateLimiterBeforeWaitTime = now();
			wait(rateLimiter->getAllowance(readBytes)); // RateKeeping
			lastRateLimiterWaitTime = now() - rateLimiterBeforeWaitTime;
			rateLimiterTotalWaitTime = rateLimiterTotalWaitTime + lastRateLimiterWaitTime;
			++checkTimes;
		}

	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			return Void(); // silently exit
		}
		TraceEvent(SevInfo, "SSAuditStorageShardReplicaFailed", data->thisServerID)
		    .errorUnsuppressed(e)
		    .detail("AuditId", req.id)
		    .detail("AuditRange", req.range)
		    .detail("AuditServer", data->thisServerID)
		    .detail("RateLimiterTotalWaitTime", rateLimiterTotalWaitTime)
		    .detail("TotalTime", now() - startTime);
		if (e.code() == error_code_audit_storage_cancelled) {
			req.reply.sendError(audit_storage_cancelled());
		} else if (e.code() == error_code_audit_storage_task_outdated) {
			req.reply.sendError(audit_storage_task_outdated());
		} else {
			req.reply.sendError(audit_storage_failed());
		}
	}

	return Void();
}

struct RangeDumpData {
	std::map<Key, Value> kvs;
	std::map<Key, Value> sampled;
	Key lastKey;
	int64_t kvsBytes;
	RangeDumpData() = default;
	RangeDumpData(const std::map<Key, Value>& kvs,
	              const std::map<Key, Value>& sampled,
	              const Key& lastKey,
	              int64_t kvsBytes)
	  : kvs(kvs), sampled(sampled), lastKey(lastKey), kvsBytes(kvsBytes) {}
};

ACTOR Future<RangeDumpData> getRangeDataToDump(StorageServer* data, KeyRange range, Version version) {
	state std::map<Key, Value> kvsToDump;
	state std::map<Key, Value> sample;
	state int64_t currentExpectedBytes = 0;
	state Key beginKey = range.begin;
	state Key lastKey = range.begin;
	state bool immediateError = true;
	// Accumulate data read from local storage to kvsToDump and make sampling until any error presents
	loop {
		// Read data and stop for any error
		state ErrorOr<GetKeyValuesReply> rep;
		try {
			state GetKeyValuesRequest localReq;
			localReq.begin = firstGreaterOrEqual(beginKey);
			localReq.end = firstGreaterOrEqual(range.end);
			localReq.version = version;
			localReq.limit = SERVER_KNOBS->MOVE_SHARD_KRM_ROW_LIMIT;
			localReq.limitBytes = SERVER_KNOBS->MOVE_SHARD_KRM_BYTE_LIMIT;
			localReq.tags = TagSet();
			data->actors.add(getKeyValuesQ(data, localReq));
			wait(store(rep, errorOr(localReq.reply.getFuture())));
			if (rep.isError()) {
				throw rep.getError();
			}
			if (rep.get().error.present()) {
				throw rep.get().error.get();
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			break;
		}

		// immediateError is used in case the read is failed at the first range
		immediateError = false;

		// Given the data, create KVS and sample. Stop if the accumulated data size is too large.
		for (const auto& kv : rep.get().data) { // TODO(BulkDump): directly read from special key space.
			lastKey = kv.key;
			auto res = kvsToDump.insert({ kv.key, kv.value });
			ASSERT(res.second);
			ByteSampleInfo sampleInfo = isKeyValueInSample(KeyValueRef(kv.key, kv.value));
			if (sampleInfo.inSample) {
				auto resSample =
				    sample.insert({ kv.key, BinaryWriter::toValue(sampleInfo.sampledSize, Unversioned()) });
				ASSERT(resSample.second);
			}
			currentExpectedBytes = currentExpectedBytes + kv.expectedSize();
			if (currentExpectedBytes >= SERVER_KNOBS->SS_BULKDUMP_BATCH_BYTES) {
				break;
			}
		}

		// Stop if no more data or having too large bytes
		if (currentExpectedBytes >= SERVER_KNOBS->SS_BULKDUMP_BATCH_BYTES) {
			break;
		} else if (!rep.get().more) {
			lastKey = range.end; // Use the range end as the lastKey
			break;
		}

		// Yield and go to the next round
		wait(delay(0.1));
		beginKey = keyAfter(lastKey);
	}

	if (immediateError) {
		throw retry();
	}

	return RangeDumpData(kvsToDump, sample, lastKey, currentExpectedBytes);
}

// The SS actor handling bulk dump task sent from DD.
// The SS partitions the task range into batches and make progress on each batch one by one.
// Each batch is a subrange of the task range sent from DD.
// When SS completes one batch, SS persists the metadata indicating this batch range completed.
// If the SS fails on dumping a batch data, the SS will send an error to DD and the leftover files
// is cleaned up when this actor returns.
// In the case of SS crashes, the leftover files will be cleared at the init step when the SS restores.
// If the SS uploads any file with succeed but the blob store is actually stored, this inconsistency will
// be captured by DD and DD will retry to dump the problematic range with a new task.
// DD will retry later if it receives any error from SS.
// Upload the data for the range with the following path organization:
//  <rootRemote>/<JobId>/<TaskId>/<batchNum>/<dumpVersion>-manifest.sst
//	<rootRemote>/<JobId>/<TaskId>/<batchNum>/<dumpVersion>-data.sst
//	<rootRemote>/<JobId>/<TaskId>/<batchNum>/<dumpVersion>-sample.sst
// where rootRemote = req.bulkDumpState.remoteRoot, jobId = req.bulkDumpState.jobId, taskId = req.bulkDumpState.taskId,
// batchNum and dumpVersion are dynamically generated.
// Each task must have one manifest file.
// If the task's range is empty, data file and sample file do not exist
// If the task's data size is too small, the sample file may omitted
ACTOR Future<Void> bulkDumpQ(StorageServer* data, BulkDumpRequest req) {
	wait(data->serveBulkDumpParallelismLock.take(TaskPriority::DefaultYield));
	state FlowLock::Releaser holder(data->serveBulkDumpParallelismLock); // A SS can handle one bulkDump task at a time
	state Key rangeBegin = req.bulkDumpState.getRange().begin;
	state Key rangeEnd = req.bulkDumpState.getRange().end;
	state BulkLoadTransportMethod transportMethod = req.bulkDumpState.getTransportMethod();
	state BulkLoadType dumpType = req.bulkDumpState.getType();
	state int64_t readBytes = 0;
	state int retryCount = 0;
	state uint64_t batchNum = 0;
	state Version versionToDump;
	state RangeDumpData rangeDumpData;
	state UID jobId = req.bulkDumpState.getJobId();
	state std::string rootFolderLocal = data->bulkDumpFolder;
	state std::string rootFolderRemote = req.bulkDumpState.getJobRoot();
	// Use jobId and taskId as the folder to store the data of the task range
	ASSERT(req.bulkDumpState.getTaskId().present());
	state std::string taskFolder = getBulkDumpJobTaskFolder(jobId, req.bulkDumpState.getTaskId().get());
	state BulkLoadFileSet destinationFileSets;
	state Transaction tr(data->cx);

	loop {
		try {
			// Clear local files
			clearFileFolder(abspath(joinPath(rootFolderLocal, taskFolder)));

			// Dump data of rangeToDump in a relativeFolder
			state KeyRange rangeToDump = Standalone(KeyRangeRef(rangeBegin, rangeEnd));

			// relativeFolder = <JobId>/<TaskId>/<batchNum>
			// relativeFolder remains consistent between local path and remote path
			state std::string relativeFolder = joinPath(taskFolder, std::to_string(batchNum));

			// Get version to dump
			tr.reset();
			wait(store(versionToDump, tr.getReadVersion()));

			// Read data
			// TODO(BulkDump): Read data from other servers at the versionToDump as much as possible
			wait(store(rangeDumpData, getRangeDataToDump(data, rangeToDump, versionToDump)));

			// Generate local file paths and remote file paths
			// The data in KVStore is dumped to the local folder at first and then
			// the local files are uploaded to the remote folder
			// Local files and remotes files have the same relative path but different root
			state std::pair<BulkLoadFileSet, BulkLoadFileSet> resFileSets =
			    getLocalRemoteFileSetSetting(versionToDump,
			                                 relativeFolder,
			                                 /*rootLocal=*/rootFolderLocal,
			                                 /*rootRemote=*/rootFolderRemote);

			// The remote file path:
			state BulkLoadFileSet localFileSetSetting = resFileSets.first;
			state BulkLoadFileSet remoteFileSetSetting = resFileSets.second;

			// Generate byte sampling setting
			BulkLoadByteSampleSetting byteSampleSetting(0,
			                                            "hashlittle2", // use function name to represent the method
			                                            SERVER_KNOBS->BYTE_SAMPLING_FACTOR,
			                                            SERVER_KNOBS->BYTE_SAMPLING_OVERHEAD,
			                                            SERVER_KNOBS->MIN_BYTE_SAMPLING_PROBABILITY);

			// Write to SST file
			state KeyRange dataRange = rangeToDump & KeyRangeRef(rangeBegin, keyAfter(rangeDumpData.lastKey));
			state BulkLoadManifest manifest =
			    wait(dumpDataFileToLocalDirectory(data->thisServerID,
			                                      rangeDumpData.kvs,
			                                      rangeDumpData.sampled,
			                                      localFileSetSetting,
			                                      remoteFileSetSetting,
			                                      byteSampleSetting,
			                                      versionToDump,
			                                      dataRange, // the actual range of the rangeDumpData.kvs
			                                      rangeDumpData.kvsBytes,
			                                      rangeDumpData.kvs.size(),
			                                      dumpType,
			                                      transportMethod));
			readBytes = readBytes + rangeDumpData.kvsBytes;
			TraceEvent(SevInfo, "SSBulkDumpDataFileGenerated", data->thisServerID)
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Task", req.bulkDumpState.toString())
			    .detail("ChecksumServers", describe(req.checksumServers))
			    .detail("RangeToDump", rangeToDump)
			    .detail("DataRange", dataRange)
			    .detail("RootFolderLocal", rootFolderLocal)
			    .detail("RelativeFolder", relativeFolder)
			    .detail("DataKeyCount", rangeDumpData.kvs.size())
			    .detail("DataBytes", rangeDumpData.kvsBytes)
			    .detail("RemoteFileSet", manifest.getFileSet().toString())
			    .detail("BatchNum", batchNum);

			// Upload Files
			state BulkLoadFileSet localFileSet = localFileSetSetting;
			if (!manifest.hasDataFile()) {
				localFileSet.removeDataFile();
			}
			if (!manifest.hasByteSampleFile()) {
				localFileSet.removeByteSampleFile();
			}
			wait(uploadBulkDumpFileSet(
			    req.bulkDumpState.getTransportMethod(), localFileSet, manifest.getFileSet(), data->thisServerID));

			// Progressively set metadata of the data range as complete phase
			// Persist remoteFilePaths to the corresponding range
			if (!dataRange.empty()) {
				// The persisting range (dataRange) must be exactly same as the range presented in the manifest file
				ASSERT(dataRange == manifest.getRange());
				wait(persistCompleteBulkDumpRange(data->cx,
				                                  req.bulkDumpState.generateBulkDumpMetadataToPersist(manifest)));
			}

			// Move to the next range
			rangeBegin = keyAfter(rangeDumpData.lastKey);
			if (rangeBegin >= rangeEnd) {
				req.reply.send(req.bulkDumpState);
				break;
			}
			batchNum++;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevInfo, "SSBulkDumpError", data->thisServerID)
			    .errorUnsuppressed(e)
			    .detail("Task", req.bulkDumpState.toString())
			    .detail("RetryCount", retryCount)
			    .detail("BatchNum", batchNum);
			if (e.code() == error_code_bulkdump_task_outdated) {
				req.reply.sendError(bulkdump_task_outdated()); // give up
				break; // silently exit
			}
			if (e.code() == error_code_wrong_shard_server || e.code() == error_code_platform_error ||
			    e.code() == error_code_io_error || retryCount >= 50) {
				req.reply.sendError(bulkdump_task_failed()); // give up
				break; // silently exit
			}
			retryCount++;
		}
		wait(delay(1.0));
	}

	// Do best effort cleanup
	clearFileFolder(abspath(joinPath(rootFolderLocal, taskFolder)), data->thisServerID, /*ignoreError=*/true);

	return Void();
}

TEST_CASE("/fdbserver/storageserver/constructMappedKey") {
	Key key = Tuple::makeTuple("key-0"_sr, "key-1"_sr, "key-2"_sr).getDataAsStandalone();
	Value value = Tuple::makeTuple("value-0"_sr, "value-1"_sr, "value-2"_sr).getDataAsStandalone();
	state KeyValueRef kvr(key, value);
	{
		Tuple mappedKeyFormatTuple =
		    Tuple::makeTuple("normal"_sr, "{{escaped}}"_sr, "{K[2]}"_sr, "{V[0]}"_sr, "{...}"_sr);

		std::vector<Optional<Tuple>> vt;
		bool isRangeQuery = false;
		preprocessMappedKey(mappedKeyFormatTuple, vt, isRangeQuery);

		Key mappedKey = constructMappedKey(&kvr, vt, mappedKeyFormatTuple);

		Key expectedMappedKey =
		    Tuple::makeTuple("normal"_sr, "{escaped}"_sr, "key-2"_sr, "value-0"_sr).getDataAsStandalone();
		//		std::cout << printable(mappedKey) << " == " << printable(expectedMappedKey) << std::endl;
		ASSERT(mappedKey.compare(expectedMappedKey) == 0);
		ASSERT(isRangeQuery == true);
	}

	{
		Tuple mappedKeyFormatTuple = Tuple::makeTuple("{{{{}}"_sr, "}}"_sr);

		std::vector<Optional<Tuple>> vt;
		bool isRangeQuery = false;
		preprocessMappedKey(mappedKeyFormatTuple, vt, isRangeQuery);
		Key mappedKey = constructMappedKey(&kvr, vt, mappedKeyFormatTuple);

		Key expectedMappedKey = Tuple::makeTuple("{{}"_sr, "}"_sr).getDataAsStandalone();
		//		std::cout << printable(mappedKey) << " == " << printable(expectedMappedKey) << std::endl;
		ASSERT(mappedKey.compare(expectedMappedKey) == 0);
		ASSERT(isRangeQuery == false);
	}
	{
		Tuple mappedKeyFormatTuple = Tuple::makeTuple("{{{{}}"_sr, "}}"_sr);

		std::vector<Optional<Tuple>> vt;
		bool isRangeQuery = false;
		preprocessMappedKey(mappedKeyFormatTuple, vt, isRangeQuery);
		Key mappedKey = constructMappedKey(&kvr, vt, mappedKeyFormatTuple);

		Key expectedMappedKey = Tuple::makeTuple("{{}"_sr, "}"_sr).getDataAsStandalone();
		//		std::cout << printable(mappedKey) << " == " << printable(expectedMappedKey) << std::endl;
		ASSERT(mappedKey.compare(expectedMappedKey) == 0);
		ASSERT(isRangeQuery == false);
	}
	{
		Tuple mappedKeyFormatTuple = Tuple::makeTuple("{K[100]}"_sr);
		state bool throwException = false;
		try {
			std::vector<Optional<Tuple>> vt;
			bool isRangeQuery = false;
			preprocessMappedKey(mappedKeyFormatTuple, vt, isRangeQuery);

			Key mappedKey = constructMappedKey(&kvr, vt, mappedKeyFormatTuple);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_mapper_bad_index);
			throwException = true;
		}
		ASSERT(throwException);
	}
	{
		Tuple mappedKeyFormatTuple = Tuple::makeTuple("{...}"_sr, "last-element"_sr);
		state bool throwException2 = false;
		try {
			std::vector<Optional<Tuple>> vt;
			bool isRangeQuery = false;
			preprocessMappedKey(mappedKeyFormatTuple, vt, isRangeQuery);

			Key mappedKey = constructMappedKey(&kvr, vt, mappedKeyFormatTuple);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_mapper_bad_range_decriptor);
			throwException2 = true;
		}
		ASSERT(throwException2);
	}
	{
		Tuple mappedKeyFormatTuple = Tuple::makeTuple("{K[not-a-number]}"_sr);
		state bool throwException3 = false;
		try {
			std::vector<Optional<Tuple>> vt;
			bool isRangeQuery = false;
			preprocessMappedKey(mappedKeyFormatTuple, vt, isRangeQuery);

			Key mappedKey = constructMappedKey(&kvr, vt, mappedKeyFormatTuple);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_mapper_bad_index);
			throwException3 = true;
		}
		ASSERT(throwException3);
	}
	return Void();
}

// Issues a secondary query (either range and point read) and fills results into "kvm".
ACTOR Future<Void> mapSubquery(StorageServer* data,
                               Version version,
                               GetMappedKeyValuesRequest* pOriginalReq,
                               Arena* pArena,
                               bool isRangeQuery,
                               KeyValueRef* it,
                               MappedKeyValueRef* kvm,
                               Key mappedKey) {
	if (isRangeQuery) {
		// Use the mappedKey as the prefix of the range query.
		GetRangeReqAndResultRef getRange = wait(quickGetKeyValues(data, mappedKey, version, pArena, pOriginalReq));
		kvm->key = it->key;
		kvm->value = it->value;
		kvm->reqAndResult = getRange;
	} else {
		GetValueReqAndResultRef getValue = wait(quickGetValue(data, mappedKey, version, pArena, pOriginalReq));
		kvm->reqAndResult = getValue;
	}
	return Void();
}

int getMappedKeyValueSize(MappedKeyValueRef mappedKeyValue) {
	auto& reqAndResult = mappedKeyValue.reqAndResult;
	int bytes = 0;
	if (std::holds_alternative<GetValueReqAndResultRef>(reqAndResult)) {
		const auto& getValue = std::get<GetValueReqAndResultRef>(reqAndResult);
		bytes = getValue.expectedSize();
	} else if (std::holds_alternative<GetRangeReqAndResultRef>(reqAndResult)) {
		const auto& getRange = std::get<GetRangeReqAndResultRef>(reqAndResult);
		bytes = getRange.result.expectedSize();
	} else {
		throw internal_error();
	}
	return bytes;
}

ACTOR Future<GetMappedKeyValuesReply> mapKeyValues(StorageServer* data,
                                                   GetKeyValuesReply input,
                                                   StringRef mapper,
                                                   // To provide span context, tags, debug ID to underlying lookups.
                                                   GetMappedKeyValuesRequest* pOriginalReq,
                                                   int* remainingLimitBytes) {
	state GetMappedKeyValuesReply result;
	result.version = input.version;
	result.cached = input.cached;
	result.arena.dependsOn(input.arena);

	result.data.reserve(result.arena, input.data.size());
	if (pOriginalReq->options.present() && pOriginalReq->options.get().debugID.present())
		g_traceBatch.addEvent(
		    "TransactionDebug", pOriginalReq->options.get().debugID.get().first(), "storageserver.mapKeyValues.Start");
	state Tuple mappedKeyFormatTuple;

	try {
		mappedKeyFormatTuple = Tuple::unpack(mapper);
	} catch (Error& e) {
		TraceEvent("MapperNotTuple").error(e).detail("Mapper", mapper);
		throw mapper_not_tuple();
	}
	state std::vector<Optional<Tuple>> vt;
	state bool isRangeQuery = false;
	preprocessMappedKey(mappedKeyFormatTuple, vt, isRangeQuery);

	state int sz = input.data.size();
	const int k = std::min(sz, SERVER_KNOBS->MAX_PARALLEL_QUICK_GET_VALUE);
	state std::vector<MappedKeyValueRef> kvms(k);
	state std::vector<Future<Void>> subqueries;
	state int offset = 0;
	if (pOriginalReq->options.present() && pOriginalReq->options.get().debugID.present())
		g_traceBatch.addEvent("TransactionDebug",
		                      pOriginalReq->options.get().debugID.get().first(),
		                      "storageserver.mapKeyValues.BeforeLoop");

	for (; (offset < sz) && (*remainingLimitBytes > 0); offset += SERVER_KNOBS->MAX_PARALLEL_QUICK_GET_VALUE) {
		// Divide into batches of MAX_PARALLEL_QUICK_GET_VALUE subqueries
		for (int i = 0; i + offset < sz && i < SERVER_KNOBS->MAX_PARALLEL_QUICK_GET_VALUE; i++) {
			KeyValueRef* it = &input.data[i + offset];
			MappedKeyValueRef* kvm = &kvms[i];
			// Clear key value to the default.
			kvm->key = ""_sr;
			kvm->value = ""_sr;
			Key mappedKey = constructMappedKey(it, vt, mappedKeyFormatTuple);
			// Make sure the mappedKey is always available, so that it's good even we want to get key asynchronously.
			result.arena.dependsOn(mappedKey.arena());

			// std::cout << "key:" << printable(kvm->key) << ", value:" << printable(kvm->value)
			//          << ", mappedKey:" << printable(mappedKey) << std::endl;

			subqueries.push_back(
			    mapSubquery(data, input.version, pOriginalReq, &result.arena, isRangeQuery, it, kvm, mappedKey));
		}
		wait(waitForAll(subqueries));
		if (pOriginalReq->options.present() && pOriginalReq->options.get().debugID.present())
			g_traceBatch.addEvent("TransactionDebug",
			                      pOriginalReq->options.get().debugID.get().first(),
			                      "storageserver.mapKeyValues.AfterBatch");
		subqueries.clear();
		for (int i = 0; i + offset < sz && i < SERVER_KNOBS->MAX_PARALLEL_QUICK_GET_VALUE; i++) {
			// since we always read the index, so always consider the index size
			int indexSize = sizeof(KeyValueRef) + input.data[i + offset].expectedSize();
			int size = indexSize + getMappedKeyValueSize(kvms[i]);
			*remainingLimitBytes -= size;
			result.data.push_back(result.arena, kvms[i]);
			if (SERVER_KNOBS->STRICTLY_ENFORCE_BYTE_LIMIT && *remainingLimitBytes <= 0) {
				break;
			}
		}
	}

	int resultSize = result.data.size();
	if (resultSize > 0) {
		// keep index for boundary index entries, so that caller can use it as a continuation.
		result.data[0].key = input.data[0].key;
		result.data[0].value = input.data[0].value;

		result.data.back().key = input.data[resultSize - 1].key;
		result.data.back().value = input.data[resultSize - 1].value;
	}
	result.more = input.more || resultSize < sz;
	if (pOriginalReq->options.present() && pOriginalReq->options.get().debugID.present())
		g_traceBatch.addEvent("TransactionDebug",
		                      pOriginalReq->options.get().debugID.get().first(),
		                      "storageserver.mapKeyValues.AfterAll");
	return result;
}

bool rangeIntersectsAnyTenant(VersionedMap<int64_t, TenantSSInfo>& tenantMap, KeyRangeRef range, Version ver) {
	auto view = tenantMap.at(ver);

	// There are no tenants, so we don't need to do any work
	if (view.begin() == view.end()) {
		return false;
	}

	if (range.begin >= "\x80"_sr) {
		return false;
	}

	int64_t beginId;
	int64_t endId;

	if (range.begin.size() >= 8) {
		beginId = TenantAPI::prefixToId(range.begin.substr(0, 8));
	} else {
		Key prefix = makeString(8);
		uint8_t* bytes = mutateString(prefix);
		range.begin.copyTo(bytes);
		memset(bytes + range.begin.size(), 0, 8 - range.begin.size());
		beginId = TenantAPI::prefixToId(prefix);
	}

	if (range.end >= "\x80"_sr) {
		endId = std::numeric_limits<int64_t>::max();
	} else if (range.end.size() >= 8) {
		endId = TenantAPI::prefixToId(range.end.substr(0, 8));
		if (range.end.size() == 8) {
			// Don't include the end prefix in the tenant search if our range doesn't extend into that prefix
			--endId;
		}
	} else {
		Key prefix = makeString(8);
		uint8_t* bytes = mutateString(prefix);
		range.end.copyTo(bytes);
		memset(bytes + range.end.size(), 0, 8 - range.end.size());
		endId = TenantAPI::prefixToId(prefix) - 1;
	}

	auto beginItr = view.lower_bound(beginId);

	// If beginItr has a value less than or equal to endId, then it points to a tenant intersecting the range
	return beginItr != view.end() && beginItr.key() <= endId;
}

TEST_CASE("/fdbserver/storageserver/rangeIntersectsAnyTenant") {
	std::set<int64_t> entries = { 0, 2, 3, 4, 6 };

	VersionedMap<int64_t, TenantSSInfo> tenantMap;
	tenantMap.createNewVersion(1);
	for (auto entry : entries) {
		tenantMap.insert(entry, TenantSSInfo{ TenantAPI::TenantLockState::UNLOCKED });
	}

	// Before all tenants
	ASSERT(!rangeIntersectsAnyTenant(tenantMap, KeyRangeRef(""_sr, "\x00"_sr), tenantMap.getLatestVersion()));

	// After all tenants
	ASSERT(!rangeIntersectsAnyTenant(tenantMap, KeyRangeRef("\xfe"_sr, "\xff"_sr), tenantMap.getLatestVersion()));

	// In between tenants
	ASSERT(
	    !rangeIntersectsAnyTenant(tenantMap,
	                              KeyRangeRef(TenantAPI::idToPrefix(1), TenantAPI::idToPrefix(1).withSuffix("\xff"_sr)),
	                              tenantMap.getLatestVersion()));

	// In between tenants with end intersecting tenant start
	ASSERT(!rangeIntersectsAnyTenant(
	    tenantMap, KeyRangeRef(TenantAPI::idToPrefix(5), TenantAPI::idToPrefix(6)), tenantMap.getLatestVersion()));

	// Entire tenants
	ASSERT(rangeIntersectsAnyTenant(
	    tenantMap, KeyRangeRef(TenantAPI::idToPrefix(0), TenantAPI::idToPrefix(1)), tenantMap.getLatestVersion()));
	ASSERT(rangeIntersectsAnyTenant(
	    tenantMap, KeyRangeRef(TenantAPI::idToPrefix(2), TenantAPI::idToPrefix(3)), tenantMap.getLatestVersion()));

	// Partial tenants
	ASSERT(
	    rangeIntersectsAnyTenant(tenantMap,
	                             KeyRangeRef(TenantAPI::idToPrefix(0), TenantAPI::idToPrefix(0).withSuffix("foo"_sr)),
	                             tenantMap.getLatestVersion()));
	ASSERT(
	    rangeIntersectsAnyTenant(tenantMap,
	                             KeyRangeRef(TenantAPI::idToPrefix(3).withSuffix("foo"_sr), TenantAPI::idToPrefix(4)),
	                             tenantMap.getLatestVersion()));
	ASSERT(rangeIntersectsAnyTenant(
	    tenantMap,
	    KeyRangeRef(TenantAPI::idToPrefix(4).withSuffix("bar"_sr), TenantAPI::idToPrefix(4).withSuffix("foo"_sr)),
	    tenantMap.getLatestVersion()));

	// Begin outside, end inside tenant
	ASSERT(
	    rangeIntersectsAnyTenant(tenantMap,
	                             KeyRangeRef(TenantAPI::idToPrefix(1), TenantAPI::idToPrefix(2).withSuffix("foo"_sr)),
	                             tenantMap.getLatestVersion()));
	ASSERT(
	    rangeIntersectsAnyTenant(tenantMap,
	                             KeyRangeRef(TenantAPI::idToPrefix(1), TenantAPI::idToPrefix(3).withSuffix("foo"_sr)),
	                             tenantMap.getLatestVersion()));

	// Begin inside, end outside tenant
	ASSERT(
	    rangeIntersectsAnyTenant(tenantMap,
	                             KeyRangeRef(TenantAPI::idToPrefix(3).withSuffix("foo"_sr), TenantAPI::idToPrefix(5)),
	                             tenantMap.getLatestVersion()));
	ASSERT(
	    rangeIntersectsAnyTenant(tenantMap,
	                             KeyRangeRef(TenantAPI::idToPrefix(4).withSuffix("foo"_sr), TenantAPI::idToPrefix(5)),
	                             tenantMap.getLatestVersion()));

	// Both inside different tenants
	ASSERT(rangeIntersectsAnyTenant(
	    tenantMap,
	    KeyRangeRef(TenantAPI::idToPrefix(0).withSuffix("foo"_sr), TenantAPI::idToPrefix(2).withSuffix("foo"_sr)),
	    tenantMap.getLatestVersion()));
	ASSERT(rangeIntersectsAnyTenant(
	    tenantMap,
	    KeyRangeRef(TenantAPI::idToPrefix(0).withSuffix("foo"_sr), TenantAPI::idToPrefix(3).withSuffix("foo"_sr)),
	    tenantMap.getLatestVersion()));
	ASSERT(rangeIntersectsAnyTenant(
	    tenantMap,
	    KeyRangeRef(TenantAPI::idToPrefix(2).withSuffix("foo"_sr), TenantAPI::idToPrefix(6).withSuffix("foo"_sr)),
	    tenantMap.getLatestVersion()));

	// Both outside tenants with tenant in the middle
	ASSERT(rangeIntersectsAnyTenant(
	    tenantMap, KeyRangeRef(""_sr, TenantAPI::idToPrefix(1).withSuffix("foo"_sr)), tenantMap.getLatestVersion()));
	ASSERT(rangeIntersectsAnyTenant(tenantMap, KeyRangeRef(""_sr, "\xff"_sr), tenantMap.getLatestVersion()));
	ASSERT(rangeIntersectsAnyTenant(tenantMap,
	                                KeyRangeRef(TenantAPI::idToPrefix(5).withSuffix("foo"_sr), "\xff"_sr),
	                                tenantMap.getLatestVersion()));

	return Void();
}

TEST_CASE("/fdbserver/storageserver/randomRangeIntersectsAnyTenant") {
	VersionedMap<int64_t, TenantSSInfo> tenantMap;
	std::set<Key> tenantPrefixes;
	tenantMap.createNewVersion(1);
	int numEntries = deterministicRandom()->randomInt(0, 20);
	for (int i = 0; i < numEntries; ++i) {
		int64_t tenantId = deterministicRandom()->randomInt64(0, std::numeric_limits<int64_t>::max());
		tenantMap.insert(tenantId, TenantSSInfo{ TenantAPI::TenantLockState::UNLOCKED });
		tenantPrefixes.insert(TenantAPI::idToPrefix(tenantId));
	}

	for (int i = 0; i < 1000; ++i) {
		Standalone<StringRef> startBytes = makeString(deterministicRandom()->randomInt(0, 16));
		Standalone<StringRef> endBytes = makeString(deterministicRandom()->randomInt(0, 16));

		uint8_t* buf = mutateString(startBytes);
		deterministicRandom()->randomBytes(buf, startBytes.size());

		buf = mutateString(endBytes);
		deterministicRandom()->randomBytes(buf, endBytes.size());

		if (startBytes > endBytes) {
			std::swap(startBytes, endBytes);
		}

		bool hasIntersection =
		    rangeIntersectsAnyTenant(tenantMap, KeyRangeRef(startBytes, endBytes), tenantMap.getLatestVersion());

		auto startItr = tenantPrefixes.lower_bound(startBytes);
		auto endItr = tenantPrefixes.upper_bound(endBytes);

		// If the iterators are the same, then that means there were no intersecting prefixes
		ASSERT((startItr == endItr) != hasIntersection);
	}

	return Void();
}

// Most of the actor is copied from getKeyValuesQ. I tried to use templates but things become nearly impossible after
// combining actor shenanigans with template shenanigans.
ACTOR Future<Void> getMappedKeyValuesQ(StorageServer* data, GetMappedKeyValuesRequest req)
// Throws a wrong_shard_server if the keys in the request or result depend on data outside this server OR if a large
// selector offset prevents all data from being read in one range read
{
	state Span span("SS:getMappedKeyValues"_loc, req.spanContext);
	state int64_t resultSize = 0;

	getCurrentLineage()->modify(&TransactionLineage::txID) = req.spanContext.traceID;

	++data->counters.getMappedRangeQueries;
	++data->counters.allQueries;
	if (req.begin.getKey().startsWith(systemKeys.begin)) {
		++data->counters.systemKeyQueries;
	}
	data->maxQueryQueue = std::max<int>(
	    data->maxQueryQueue, data->counters.allQueries.getValue() - data->counters.finishedQueries.getValue());

	// Active load balancing runs at a very high priority (to obtain accurate queue lengths)
	// so we need to downgrade here
	wait(data->getQueryDelay());
	state PriorityMultiLock::Lock readLock = wait(data->getReadLock(req.options));

	// Track time from requestTime through now as read queueing wait time
	state double queueWaitEnd = g_network->timer();
	data->counters.readQueueWaitSample.addMeasurement(queueWaitEnd - req.requestTime());

	try {
		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent(
			    "TransactionDebug", req.options.get().debugID.get().first(), "storageserver.getMappedKeyValues.Before");
		// VERSION_VECTOR change
		Version commitVersion = getLatestCommitVersion(req.ssLatestCommitVersions, data->tag);
		state Version version = wait(waitForVersion(data, commitVersion, req.version, span.context));
		data->counters.readVersionWaitSample.addMeasurement(g_network->timer() - queueWaitEnd);

		data->checkTenantEntry(version, req.tenantInfo, req.options.present() ? req.options.get().lockAware : false);
		if (req.tenantInfo.hasTenant()) {
			req.begin.setKeyUnlimited(req.begin.getKey().withPrefix(req.tenantInfo.prefix.get(), req.arena));
			req.end.setKeyUnlimited(req.end.getKey().withPrefix(req.tenantInfo.prefix.get(), req.arena));
		}

		state uint64_t changeCounter = data->shardChangeCounter;
		//		try {
		state KeyRange shard = getShardKeyRange(data, req.begin);

		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent("TransactionDebug",
			                      req.options.get().debugID.get().first(),
			                      "storageserver.getMappedKeyValues.AfterVersion");
		//.detail("ShardBegin", shard.begin).detail("ShardEnd", shard.end);
		//} catch (Error& e) { TraceEvent("WrongShardServer", data->thisServerID).detail("Begin",
		// req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("Shard",
		//"None").detail("In", "getMappedKeyValues>getShardKeyRange"); throw e; }

		if (!selectorInRange(req.end, shard) && !(req.end.isFirstGreaterOrEqual() && req.end.getKey() == shard.end)) {
			//			TraceEvent("WrongShardServer1", data->thisServerID).detail("Begin",
			// req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("ShardBegin",
			// shard.begin).detail("ShardEnd", shard.end).detail("In", "getMappedKeyValues>checkShardExtents");
			throw wrong_shard_server();
		}

		KeyRangeRef searchRange = TenantAPI::clampRangeToTenant(shard, req.tenantInfo, req.arena);

		state int offset1 = 0;
		state int offset2;
		state Future<Key> fBegin =
		    req.begin.isFirstGreaterOrEqual()
		        ? Future<Key>(req.begin.getKey())
		        : findKey(data, req.begin, version, searchRange, &offset1, span.context, req.options);
		state Future<Key> fEnd =
		    req.end.isFirstGreaterOrEqual()
		        ? Future<Key>(req.end.getKey())
		        : findKey(data, req.end, version, searchRange, &offset2, span.context, req.options);
		state Key begin = wait(fBegin);
		state Key end = wait(fEnd);

		if (!req.tenantInfo.hasTenant()) {
			// We do not support raw flat map requests when using tenants.
			// When tenants are required, we disable raw flat map requests entirely.
			// If tenants are optional, we check whether the range intersects a tenant and fail if it does.
			if (data->db->get().client.tenantMode == TenantMode::REQUIRED) {
				throw tenant_name_required();
			}

			if (rangeIntersectsAnyTenant(data->tenantMap, KeyRangeRef(begin, end), req.version)) {
				throw tenant_name_required();
			}
		}

		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent("TransactionDebug",
			                      req.options.get().debugID.get().first(),
			                      "storageserver.getMappedKeyValues.AfterKeys");
		//.detail("Off1",offset1).detail("Off2",offset2).detail("ReqBegin",req.begin.getKey()).detail("ReqEnd",req.end.getKey());

		// Offsets of zero indicate begin/end keys in this shard, which obviously means we can answer the query
		// An end offset of 1 is also OK because the end key is exclusive, so if the first key of the next shard is the
		// end the last actual key returned must be from this shard. A begin offset of 1 is also OK because then either
		// begin is past end or equal to end (so the result is definitely empty)
		if ((offset1 && offset1 != 1) || (offset2 && offset2 != 1)) {
			CODE_PROBE(true, "wrong_shard_server due to offset in getMappedKeyValuesQ", probe::decoration::rare);
			// We could detect when offset1 takes us off the beginning of the database or offset2 takes us off the end,
			// and return a clipped range rather than an error (since that is what the NativeAPI.getRange will do anyway
			// via its "slow path"), but we would have to add some flags to the response to encode whether we went off
			// the beginning and the end, since it needs that information.
			//TraceEvent("WrongShardServer2", data->thisServerID).detail("Begin", req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("ShardBegin", shard.begin).detail("ShardEnd", shard.end).detail("In", "getMappedKeyValues>checkOffsets").detail("BeginKey", begin).detail("EndKey", end).detail("BeginOffset", offset1).detail("EndOffset", offset2);
			throw wrong_shard_server();
		}

		if (begin >= end) {
			if (req.options.present() && req.options.get().debugID.present())
				g_traceBatch.addEvent("TransactionDebug",
				                      req.options.get().debugID.get().first(),
				                      "storageserver.getMappedKeyValues.Send");
			//.detail("Begin",begin).detail("End",end);

			GetMappedKeyValuesReply none;
			none.version = version;
			none.more = false;
			none.penalty = data->getPenalty();

			data->checkChangeCounter(changeCounter,
			                         KeyRangeRef(std::min<KeyRef>(req.begin.getKey(), req.end.getKey()),
			                                     std::max<KeyRef>(req.begin.getKey(), req.end.getKey())));
			req.reply.send(none);
		} else {
			state int remainingLimitBytes = req.limitBytes;
			// create a temporary byte limit for index fetching ONLY, this should be excessive
			// because readRange is cheap when reading additional bytes
			state int bytesForIndex =
			    std::min(req.limitBytes, (int)(req.limitBytes * SERVER_KNOBS->FRACTION_INDEX_BYTELIMIT_PREFETCH));
			GetKeyValuesReply getKeyValuesReply = wait(readRange(data,
			                                                     version,
			                                                     KeyRangeRef(begin, end),
			                                                     req.limit,
			                                                     &bytesForIndex,
			                                                     span.context,
			                                                     req.options,
			                                                     req.tenantInfo.prefix));

			// Unlock read lock before the subqueries because each
			// subquery will route back to getValueQ or getKeyValuesQ with a new request having the same
			// read options which will each acquire the ssLock.
			readLock.release();

			state GetMappedKeyValuesReply r;
			try {
				// Map the scanned range to another list of keys and look up.
				GetMappedKeyValuesReply _r =
				    wait(mapKeyValues(data, getKeyValuesReply, req.mapper, &req, &remainingLimitBytes));
				r = _r;
			} catch (Error& e) {
				// catch txn_too_old here if prefetch runs for too long, and returns it back to client
				TraceEvent("MapError").error(e);
				throw;
			}

			if (req.options.present() && req.options.get().debugID.present())
				g_traceBatch.addEvent("TransactionDebug",
				                      req.options.get().debugID.get().first(),
				                      "storageserver.getMappedKeyValues.AfterReadRange");
			//.detail("Begin",begin).detail("End",end).detail("SizeOf",r.data.size());
			data->checkChangeCounter(
			    changeCounter,
			    KeyRangeRef(std::min<KeyRef>(begin, std::min<KeyRef>(req.begin.getKey(), req.end.getKey())),
			                std::max<KeyRef>(end, std::max<KeyRef>(req.begin.getKey(), req.end.getKey()))));
			if (EXPENSIVE_VALIDATION) {
				// TODO: GetMappedKeyValuesRequest doesn't respect limit yet.
				//                ASSERT(r.data.size() <= std::abs(req.limit));
			}

			r.penalty = data->getPenalty();
			req.reply.send(r);

			resultSize = req.limitBytes - remainingLimitBytes;
			data->counters.getMappedRangeBytesQueried += resultSize;
			data->counters.finishedGetMappedRangeSecondaryQueries += r.data.size();
			if (r.data.size() == 0) {
				++data->counters.emptyQueries;
			}
		}
	} catch (Error& e) {
		if (!canReplyWith(e))
			throw;
		data->sendErrorWithPenalty(req.reply, e, data->getPenalty());
	}

	data->transactionTagCounter.addRequest(req.tags, resultSize);
	++data->counters.finishedQueries;
	++data->counters.finishedGetMappedRangeQueries;

	double duration = g_network->timer() - req.requestTime();
	data->counters.readLatencySample.addMeasurement(duration);
	data->counters.mappedRangeSample->addMeasurement(duration);
	if (data->latencyBandConfig.present()) {
		int maxReadBytes =
		    data->latencyBandConfig.get().readConfig.maxReadBytes.orDefault(std::numeric_limits<int>::max());
		int maxSelectorOffset =
		    data->latencyBandConfig.get().readConfig.maxKeySelectorOffset.orDefault(std::numeric_limits<int>::max());
		data->counters.readLatencyBands.addMeasurement(duration,
		                                               1,
		                                               Filtered(resultSize > maxReadBytes ||
		                                                        abs(req.begin.offset) > maxSelectorOffset ||
		                                                        abs(req.end.offset) > maxSelectorOffset));
	}

	return Void();
}

ACTOR Future<Void> getKeyValuesStreamQ(StorageServer* data, GetKeyValuesStreamRequest req)
// Throws a wrong_shard_server if the keys in the request or result depend on data outside this server OR if a large
// selector offset prevents all data from being read in one range read
{
	state Span span("SS:getKeyValuesStream"_loc, req.spanContext);
	state int64_t resultSize = 0;

	req.reply.setByteLimit(SERVER_KNOBS->RANGESTREAM_LIMIT_BYTES);
	++data->counters.getRangeStreamQueries;
	++data->counters.allQueries;
	if (req.begin.getKey().startsWith(systemKeys.begin)) {
		++data->counters.systemKeyQueries;
	}
	data->maxQueryQueue = std::max<int>(
	    data->maxQueryQueue, data->counters.allQueries.getValue() - data->counters.finishedQueries.getValue());

	// Active load balancing runs at a very high priority (to obtain accurate queue lengths)
	// so we need to downgrade here
	wait(delay(0, TaskPriority::DefaultEndpoint));

	try {
		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent(
			    "TransactionDebug", req.options.get().debugID.get().first(), "storageserver.getKeyValuesStream.Before");

		Version commitVersion = getLatestCommitVersion(req.ssLatestCommitVersions, data->tag);
		state Version version = wait(waitForVersion(data, commitVersion, req.version, span.context));

		data->checkTenantEntry(version, req.tenantInfo, req.options.present() ? req.options.get().lockAware : false);
		if (req.tenantInfo.hasTenant()) {
			req.begin.setKeyUnlimited(req.begin.getKey().withPrefix(req.tenantInfo.prefix.get(), req.arena));
			req.end.setKeyUnlimited(req.end.getKey().withPrefix(req.tenantInfo.prefix.get(), req.arena));
		}

		state uint64_t changeCounter = data->shardChangeCounter;
		//		try {
		state KeyRange shard = getShardKeyRange(data, req.begin);

		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent("TransactionDebug",
			                      req.options.get().debugID.get().first(),
			                      "storageserver.getKeyValuesStream.AfterVersion");
		//.detail("ShardBegin", shard.begin).detail("ShardEnd", shard.end);
		//} catch (Error& e) { TraceEvent("WrongShardServer", data->thisServerID).detail("Begin",
		// req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("Shard",
		//"None").detail("In", "getKeyValues>getShardKeyRange"); throw e; }

		if (!selectorInRange(req.end, shard) && !(req.end.isFirstGreaterOrEqual() && req.end.getKey() == shard.end)) {
			//			TraceEvent("WrongShardServer1", data->thisServerID).detail("Begin",
			// req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("ShardBegin",
			// shard.begin).detail("ShardEnd", shard.end).detail("In", "getKeyValues>checkShardExtents");
			throw wrong_shard_server();
		}

		KeyRangeRef searchRange = TenantAPI::clampRangeToTenant(shard, req.tenantInfo, req.arena);

		state int offset1 = 0;
		state int offset2;
		state Future<Key> fBegin =
		    req.begin.isFirstGreaterOrEqual()
		        ? Future<Key>(req.begin.getKey())
		        : findKey(data, req.begin, version, searchRange, &offset1, span.context, req.options);
		state Future<Key> fEnd =
		    req.end.isFirstGreaterOrEqual()
		        ? Future<Key>(req.end.getKey())
		        : findKey(data, req.end, version, searchRange, &offset2, span.context, req.options);
		state Key begin = wait(fBegin);
		state Key end = wait(fEnd);
		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent("TransactionDebug",
			                      req.options.get().debugID.get().first(),
			                      "storageserver.getKeyValuesStream.AfterKeys");
		//.detail("Off1",offset1).detail("Off2",offset2).detail("ReqBegin",req.begin.getKey()).detail("ReqEnd",req.end.getKey());

		// Offsets of zero indicate begin/end keys in this shard, which obviously means we can answer the query
		// An end offset of 1 is also OK because the end key is exclusive, so if the first key of the next shard is the
		// end the last actual key returned must be from this shard. A begin offset of 1 is also OK because then either
		// begin is past end or equal to end (so the result is definitely empty)
		if ((offset1 && offset1 != 1) || (offset2 && offset2 != 1)) {
			CODE_PROBE(true, "wrong_shard_server due to offset in rangeStream", probe::decoration::rare);
			// We could detect when offset1 takes us off the beginning of the database or offset2 takes us off the end,
			// and return a clipped range rather than an error (since that is what the NativeAPI.getRange will do anyway
			// via its "slow path"), but we would have to add some flags to the response to encode whether we went off
			// the beginning and the end, since it needs that information.
			//TraceEvent("WrongShardServer2", data->thisServerID).detail("Begin", req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("ShardBegin", shard.begin).detail("ShardEnd", shard.end).detail("In", "getKeyValues>checkOffsets").detail("BeginKey", begin).detail("EndKey", end).detail("BeginOffset", offset1).detail("EndOffset", offset2);
			throw wrong_shard_server();
		}

		if (begin >= end) {
			if (req.options.present() && req.options.get().debugID.present())
				g_traceBatch.addEvent("TransactionDebug",
				                      req.options.get().debugID.get().first(),
				                      "storageserver.getKeyValuesStream.Send");
			//.detail("Begin",begin).detail("End",end);

			GetKeyValuesStreamReply none;
			none.version = version;
			none.more = false;

			data->checkChangeCounter(changeCounter,
			                         KeyRangeRef(std::min<KeyRef>(req.begin.getKey(), req.end.getKey()),
			                                     std::max<KeyRef>(req.begin.getKey(), req.end.getKey())));
			req.reply.send(none);
			req.reply.sendError(end_of_stream());
		} else {
			loop {
				wait(req.reply.onReady());
				state PriorityMultiLock::Lock readLock = wait(data->getReadLock(req.options));

				if (version < data->oldestVersion.get()) {
					throw transaction_too_old();
				}

				// Even if TSS mode is Disabled, this may be the second test in a restarting test where the first run
				// had it enabled.
				state int byteLimit =
				    (BUGGIFY && g_network->isSimulated() && g_simulator->tssMode == ISimulator::TSSMode::Disabled &&
				     !data->isTss() && !data->isSSWithTSSPair())
				        ? 1
				        : CLIENT_KNOBS->REPLY_BYTE_LIMIT;
				TraceEvent(SevDebug, "SSGetKeyValueStreamLimits")
				    .detail("ByteLimit", byteLimit)
				    .detail("ReqLimit", req.limit)
				    .detail("Begin", begin.printable())
				    .detail("End", end.printable());

				GetKeyValuesReply _r = wait(readRange(data,
				                                      version,
				                                      KeyRangeRef(begin, end),
				                                      req.limit,
				                                      &byteLimit,
				                                      span.context,
				                                      req.options,
				                                      req.tenantInfo.prefix));
				readLock.release();
				GetKeyValuesStreamReply r(_r);

				if (req.options.present() && req.options.get().debugID.present())
					g_traceBatch.addEvent("TransactionDebug",
					                      req.options.get().debugID.get().first(),
					                      "storageserver.getKeyValuesStream.AfterReadRange");
				//.detail("Begin",begin).detail("End",end).detail("SizeOf",r.data.size());
				data->checkChangeCounter(
				    changeCounter,
				    KeyRangeRef(std::min<KeyRef>(begin, std::min<KeyRef>(req.begin.getKey(), req.end.getKey())),
				                std::max<KeyRef>(end, std::max<KeyRef>(req.begin.getKey(), req.end.getKey()))));
				if (EXPENSIVE_VALIDATION) {
					for (int i = 0; i < r.data.size(); i++) {
						if (req.tenantInfo.hasTenant()) {
							ASSERT(r.data[i].key >= begin.removePrefix(req.tenantInfo.prefix.get()) &&
							       r.data[i].key < end.removePrefix(req.tenantInfo.prefix.get()));
						} else {
							ASSERT(r.data[i].key >= begin && r.data[i].key < end);
						}
					}
					ASSERT(r.data.size() <= std::abs(req.limit));
				}

				// For performance concerns, the cost of a range read is billed to the start key and end key of the
				// range.
				int64_t totalByteSize = 0;
				for (int i = 0; i < r.data.size(); i++) {
					totalByteSize += r.data[i].expectedSize();
				}

				KeyRef lastKey;
				if (!r.data.empty()) {
					lastKey = addPrefix(r.data.back().key, req.tenantInfo.prefix, req.arena);
				}
				if (totalByteSize > 0 && SERVER_KNOBS->READ_SAMPLING_ENABLED) {
					int64_t bytesReadPerKSecond = std::max(totalByteSize, SERVER_KNOBS->EMPTY_READ_PENALTY) / 2;
					KeyRef firstKey = addPrefix(r.data[0].key, req.tenantInfo.prefix, req.arena);
					data->metrics.notifyBytesReadPerKSecond(firstKey, bytesReadPerKSecond);
					data->metrics.notifyBytesReadPerKSecond(lastKey, bytesReadPerKSecond);
				}

				req.reply.send(r);

				data->counters.rowsQueried += r.data.size();
				if (r.data.size() == 0) {
					++data->counters.emptyQueries;
				}
				if (!r.more) {
					req.reply.sendError(end_of_stream());
					break;
				}
				ASSERT(r.data.size());

				if (req.limit >= 0) {
					begin = keyAfter(lastKey);
				} else {
					end = lastKey;
				}

				data->transactionTagCounter.addRequest(req.tags, resultSize);
				// lock.release();
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_operation_obsolete) {
			if (!canReplyWith(e))
				throw;
			req.reply.sendError(e);
		}
	}

	data->transactionTagCounter.addRequest(req.tags, resultSize);
	++data->counters.finishedQueries;

	return Void();
}

ACTOR Future<Void> getKeyQ(StorageServer* data, GetKeyRequest req) {
	state Span span("SS:getKey"_loc, req.spanContext);
	state int64_t resultSize = 0;

	getCurrentLineage()->modify(&TransactionLineage::txID) = req.spanContext.traceID;

	++data->counters.getKeyQueries;
	++data->counters.allQueries;
	data->maxQueryQueue = std::max<int>(
	    data->maxQueryQueue, data->counters.allQueries.getValue() - data->counters.finishedQueries.getValue());

	// Active load balancing runs at a very high priority (to obtain accurate queue lengths)
	// so we need to downgrade here
	wait(data->getQueryDelay());
	state PriorityMultiLock::Lock readLock = wait(data->getReadLock(req.options));

	// Track time from requestTime through now as read queueing wait time
	state double queueWaitEnd = g_network->timer();
	data->counters.readQueueWaitSample.addMeasurement(queueWaitEnd - req.requestTime());

	try {
		Version commitVersion = getLatestCommitVersion(req.ssLatestCommitVersions, data->tag);
		state Version version = wait(waitForVersion(data, commitVersion, req.version, req.spanContext));
		data->counters.readVersionWaitSample.addMeasurement(g_network->timer() - queueWaitEnd);

		data->checkTenantEntry(version, req.tenantInfo, req.options.map(&ReadOptions::lockAware).orDefault(false));
		if (req.tenantInfo.hasTenant()) {
			req.sel.setKeyUnlimited(req.sel.getKey().withPrefix(req.tenantInfo.prefix.get(), req.arena));
		}
		state uint64_t changeCounter = data->shardChangeCounter;

		KeyRange shard = getShardKeyRange(data, req.sel);
		KeyRangeRef searchRange = TenantAPI::clampRangeToTenant(shard, req.tenantInfo, req.arena);

		state int offset;
		Key absoluteKey = wait(findKey(data, req.sel, version, searchRange, &offset, req.spanContext, req.options));

		data->checkChangeCounter(changeCounter,
		                         KeyRangeRef(std::min<KeyRef>(req.sel.getKey(), absoluteKey),
		                                     std::max<KeyRef>(req.sel.getKey(), absoluteKey)));

		KeyRef k = absoluteKey;
		if (req.tenantInfo.hasTenant()) {
			k = k.removePrefix(req.tenantInfo.prefix.get());
		}

		KeySelector updated;
		if (offset < 0)
			updated = firstGreaterOrEqual(k) +
			          offset; // first thing on this shard OR (large offset case) smallest key retrieved in range read
		else if (offset > 0)
			updated =
			    firstGreaterOrEqual(k) + offset -
			    1; // first thing on next shard OR (large offset case) keyAfter largest key retrieved in range read
		else
			updated = KeySelectorRef(k, true, 0); // found

		resultSize = k.size();
		data->counters.bytesQueried += resultSize;
		++data->counters.rowsQueried;

		// Check if the desired key might be cached
		auto cached = data->cachedRangeMap[absoluteKey];
		// if (cached)
		//	TraceEvent(SevDebug, "SSGetKeyCached").detail("Key", k).detail("Begin",
		// shard.begin).detail("End", shard.end);

		GetKeyReply reply(updated, cached);
		reply.penalty = data->getPenalty();

		req.reply.send(reply);
	} catch (Error& e) {
		// if (e.code() == error_code_wrong_shard_server) TraceEvent("WrongShardServer").detail("In","getKey");
		if (!canReplyWith(e))
			throw;
		data->sendErrorWithPenalty(req.reply, e, data->getPenalty());
	}

	// SOMEDAY: The size reported here is an undercount of the bytes read due to the fact that we have to scan for the
	// key It would be more accurate to count all the read bytes, but it's not critical because this function is only
	// used if read-your-writes is disabled
	data->transactionTagCounter.addRequest(req.tags, resultSize);

	++data->counters.finishedQueries;

	double duration = g_network->timer() - req.requestTime();
	data->counters.readLatencySample.addMeasurement(duration);
	data->counters.readKeyLatencySample.addMeasurement(duration);

	if (data->latencyBandConfig.present()) {
		int maxReadBytes =
		    data->latencyBandConfig.get().readConfig.maxReadBytes.orDefault(std::numeric_limits<int>::max());
		int maxSelectorOffset =
		    data->latencyBandConfig.get().readConfig.maxKeySelectorOffset.orDefault(std::numeric_limits<int>::max());
		data->counters.readLatencyBands.addMeasurement(
		    duration, 1, Filtered(resultSize > maxReadBytes || abs(req.sel.offset) > maxSelectorOffset));
	}

	return Void();
}

void getQueuingMetrics(StorageServer* self, StorageQueuingMetricsRequest const& req) {
	StorageQueuingMetricsReply reply;
	reply.localTime = now();
	reply.instanceID = self->instanceID;
	reply.bytesInput = self->counters.bytesInput.getValue();
	reply.bytesDurable = self->counters.bytesDurable.getValue();

	reply.storageBytes = self->storage.getStorageBytes();
	reply.localRateLimit = self->currentRate();

	reply.version = self->version.get();
	reply.cpuUsage = self->cpuUsage;
	reply.diskUsage = self->diskUsage;
	reply.durableVersion = self->durableVersion.get();

	reply.busiestTags = self->transactionTagCounter.getBusiestTags();

	req.reply.send(reply);
}

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

/////////////////////////// Updates ////////////////////////////////
#ifndef __INTEL_COMPILER
#pragma region Updates
#endif

ACTOR Future<Void> doEagerReads(StorageServer* data, UpdateEagerReadInfo* eager) {
	eager->finishKeyBegin();
	state ReadOptions options;
	options.type = ReadType::EAGER;
	if (eager->enableClearRangeEagerReads) {
		std::vector<Future<Key>> keyEnd(eager->keyBegin.size());
		for (int i = 0; i < keyEnd.size(); i++)
			keyEnd[i] = data->storage.readNextKeyInclusive(eager->keyBegin[i], options);
		data->counters.eagerReadsKeys += keyEnd.size();

		state Future<std::vector<Key>> futureKeyEnds = getAll(keyEnd);
		state std::vector<Key> keyEndVal = wait(futureKeyEnds);
		for (const auto& key : keyEndVal) {
			data->counters.kvScanBytes += key.expectedSize();
		}
		eager->keyEnd = keyEndVal;
	}

	std::vector<Future<Optional<Value>>> value(eager->keys.size());
	for (int i = 0; i < value.size(); i++)
		value[i] = data->storage.readValuePrefix(eager->keys[i].first, eager->keys[i].second, options);

	state Future<std::vector<Optional<Value>>> futureValues = getAll(value);
	std::vector<Optional<Value>> optionalValues = wait(futureValues);
	for (const auto& value : optionalValues) {
		if (value.present()) {
			data->counters.kvGetBytes += value.expectedSize();
		}
	}
	data->counters.eagerReadsKeys += eager->keys.size();
	eager->value = optionalValues;

	return Void();
}

bool changeDurableVersion(StorageServer* data, Version desiredDurableVersion) {
	// Remove entries from the latest version of data->versionedData that haven't changed since they were inserted
	//   before or at desiredDurableVersion, to maintain the invariants for versionedData.
	// Such entries remain in older versions of versionedData until they are forgotten, because it is expensive to dig
	// them out. We also remove everything up to and including newDurableVersion from mutationLog, and everything
	//   up to but excluding desiredDurableVersion from freeable
	// May return false if only part of the work has been done, in which case the caller must call again with the same
	// parameters

	auto& verData = data->mutableData();
	ASSERT(verData.getLatestVersion() == data->version.get() || verData.getLatestVersion() == data->version.get() + 1);

	Version nextDurableVersion = desiredDurableVersion;

	auto mlv = data->getMutationLog().begin();
	if (mlv != data->getMutationLog().end() && mlv->second.version <= desiredDurableVersion) {
		auto& v = mlv->second;
		nextDurableVersion = v.version;
		data->freeable[data->version.get()].dependsOn(v.arena());

		if (verData.getLatestVersion() <= data->version.get())
			verData.createNewVersion(data->version.get() + 1);

		int64_t bytesDurable = VERSION_OVERHEAD;
		for (const auto& m : v.mutations) {
			bytesDurable += mvccStorageBytes(m);
			auto i = verData.atLatest().find(m.param1);
			if (i) {
				ASSERT(i.key() == m.param1);
				ASSERT(i.insertVersion() >= nextDurableVersion);
				if (i.insertVersion() == nextDurableVersion)
					verData.erase(i);
			}
			if (m.type == MutationRef::SetValue) {
				// A set can split a clear, so there might be another entry immediately after this one that should also
				// be cleaned up
				i = verData.atLatest().upper_bound(m.param1);
				if (i) {
					ASSERT(i.insertVersion() >= nextDurableVersion);
					if (i.insertVersion() == nextDurableVersion)
						verData.erase(i);
				}
			}
		}
		data->counters.bytesDurable += bytesDurable;
	}

	int64_t feedBytesDurable = 0;
	while (!data->feedMemoryBytesByVersion.empty() &&
	       data->feedMemoryBytesByVersion.front().first <= desiredDurableVersion) {
		feedBytesDurable += data->feedMemoryBytesByVersion.front().second;
		data->feedMemoryBytesByVersion.pop_front();
	}
	data->changeFeedMemoryBytes -= feedBytesDurable;
	if (SERVER_KNOBS->STORAGE_INCLUDE_FEED_STORAGE_QUEUE) {
		data->counters.bytesDurable += feedBytesDurable;
	}

	if (EXPENSIVE_VALIDATION) {
		// Check that the above loop did its job
		auto view = data->data().atLatest();
		for (auto i = view.begin(); i != view.end(); ++i)
			ASSERT(i.insertVersion() > nextDurableVersion);
	}
	data->getMutableMutationLog().erase(data->getMutationLog().begin(),
	                                    data->getMutationLog().upper_bound(nextDurableVersion));
	data->freeable.erase(data->freeable.begin(), data->freeable.lower_bound(nextDurableVersion));

	Future<Void> checkFatalError = data->otherError.getFuture();
	data->storageMinRecoverVersion = data->durableVersion.get();
	data->durableVersion.set(nextDurableVersion);
	setDataDurableVersion(data->thisServerID, data->durableVersion.get());
	if (checkFatalError.isReady())
		checkFatalError.get();

	// TraceEvent("ForgotVersionsBefore", data->thisServerID).detail("Version", nextDurableVersion);
	validate(data);

	return nextDurableVersion == desiredDurableVersion;
}

Optional<MutationRef> clipMutation(MutationRef const& m, KeyRangeRef range) {
	if (isSingleKeyMutation((MutationRef::Type)m.type)) {
		if (range.contains(m.param1))
			return m;
	} else if (m.type == MutationRef::ClearRange) {
		KeyRangeRef i = range & KeyRangeRef(m.param1, m.param2);
		if (!i.empty())
			return MutationRef((MutationRef::Type)m.type, i.begin, i.end);
	} else
		ASSERT(false);
	return Optional<MutationRef>();
}

bool convertAtomicOp(MutationRef& m, StorageServer::VersionedData const& data, UpdateEagerReadInfo* eager, Arena& ar) {
	// After this function call, m should be copied into an arena immediately (before modifying data, shards, or eager)
	if (m.type != MutationRef::ClearRange && m.type != MutationRef::SetValue) {
		Optional<StringRef> oldVal;
		auto it = data.atLatest().lastLessOrEqual(m.param1);
		if (it != data.atLatest().end() && it->isValue() && it.key() == m.param1)
			oldVal = it->getValue();
		else if (it != data.atLatest().end() && it->isClearTo() && it->getEndKey() > m.param1) {
			CODE_PROBE(true, "Atomic op right after a clear.");
		} else {
			Optional<Value>& oldThing = eager->getValue(m.param1);
			if (oldThing.present())
				oldVal = oldThing.get();
		}

		switch (m.type) {
		case MutationRef::AddValue:
			m.param2 = doLittleEndianAdd(oldVal, m.param2, ar);
			break;
		case MutationRef::And:
			m.param2 = doAnd(oldVal, m.param2, ar);
			break;
		case MutationRef::Or:
			m.param2 = doOr(oldVal, m.param2, ar);
			break;
		case MutationRef::Xor:
			m.param2 = doXor(oldVal, m.param2, ar);
			break;
		case MutationRef::AppendIfFits:
			m.param2 = doAppendIfFits(oldVal, m.param2, ar);
			break;
		case MutationRef::Max:
			m.param2 = doMax(oldVal, m.param2, ar);
			break;
		case MutationRef::Min:
			m.param2 = doMin(oldVal, m.param2, ar);
			break;
		case MutationRef::ByteMin:
			m.param2 = doByteMin(oldVal, m.param2, ar);
			break;
		case MutationRef::ByteMax:
			m.param2 = doByteMax(oldVal, m.param2, ar);
			break;
		case MutationRef::MinV2:
			m.param2 = doMinV2(oldVal, m.param2, ar);
			break;
		case MutationRef::AndV2:
			m.param2 = doAndV2(oldVal, m.param2, ar);
			break;
		case MutationRef::CompareAndClear:
			if (oldVal.present() && m.param2 == oldVal.get()) {
				m.type = MutationRef::ClearRange;
				m.param2 = keyAfter(m.param1, ar);
				return true;
			}
			return false;
		}
		m.type = MutationRef::SetValue;
	}
	return true;
}

void expandClear(MutationRef& m,
                 StorageServer::VersionedData const& data,
                 UpdateEagerReadInfo* eager,
                 KeyRef eagerTrustedEnd) {
	// After this function call, m should be copied into an arena immediately (before modifying data, shards, or eager)
	ASSERT(m.type == MutationRef::ClearRange);
	// Expand the clear
	const auto& d = data.atLatest();

	// If another clear overlaps the beginning of this one, engulf it
	auto i = d.lastLess(m.param1);
	if (i && i->isClearTo() && i->getEndKey() >= m.param1)
		m.param1 = i.key();

	// If another clear overlaps the end of this one, engulf it; otherwise expand
	i = d.lastLessOrEqual(m.param2);
	if (i && i->isClearTo() && i->getEndKey() >= m.param2) {
		m.param2 = i->getEndKey();
	} else if (eager->enableClearRangeEagerReads) {
		// Expand to the next set or clear (from storage or latestVersion), and if it
		// is a clear, engulf it as well

		// We can get lower_bound from the result of lastLessOrEqual
		if (i) {
			if (i.key() != m.param2) {
				++i;
			}
		} else {
			// There's nothing less than or equal to m.param2 in view, so
			// begin() is the first thing greater than m.param2, or end().
			// Either way that's the correct result for lower_bound.
			i = d.begin();
		}
		if (EXPENSIVE_VALIDATION) {
			ASSERT(i == d.lower_bound(m.param2));
		}

		KeyRef endKeyAtStorageVersion =
		    m.param2 == eagerTrustedEnd ? eagerTrustedEnd : std::min(eager->getKeyEnd(m.param2), eagerTrustedEnd);
		if (!i || endKeyAtStorageVersion < i.key())
			m.param2 = endKeyAtStorageVersion;
		else if (i->isClearTo())
			m.param2 = i->getEndKey();
		else
			m.param2 = i.key();
	}
}

void applyMutation(StorageServer* self,
                   MutationRef const& m,
                   Arena& arena,
                   StorageServer::VersionedData& data,
                   Version version) {
	// m is expected to be in arena already
	// Clear split keys are added to arena
	StorageMetrics metrics;
	// FIXME: remove the / 2 and double the related knobs.
	metrics.bytesWrittenPerKSecond = mvccStorageBytes(m) / 2; // comparable to counter.bytesInput / 2
	metrics.iosPerKSecond = 1;
	self->metrics.notify(m.param1, metrics);

	if (m.type == MutationRef::SetValue) {
		// VersionedMap (data) is bookkeeping all empty ranges. If the key to be set is new, it is supposed to be in a
		// range what was empty. Break the empty range into halves.
		auto prev = data.atLatest().lastLessOrEqual(m.param1);
		if (prev && prev->isClearTo() && prev->getEndKey() > m.param1) {
			ASSERT(prev.key() <= m.param1);
			KeyRef end = prev->getEndKey();
			// the insert version of the previous clear is preserved for the "left half", because in
			// changeDurableVersion() the previous clear is still responsible for removing it insert() invalidates prev,
			// so prev.key() is not safe to pass to it by reference
			data.insert(KeyRef(prev.key()),
			            ValueOrClearToRef::clearTo(m.param1),
			            prev.insertVersion()); // overwritten by below insert if empty
			KeyRef nextKey = keyAfter(m.param1, arena);
			if (end != nextKey) {
				ASSERT(end > nextKey);
				// the insert version of the "right half" is not preserved, because in changeDurableVersion() this set
				// is responsible for removing it
				// FIXME: This copy is technically an asymptotic problem, definitely a waste of memory (copy of keyAfter
				// is a waste, but not asymptotic)
				data.insert(nextKey, ValueOrClearToRef::clearTo(KeyRef(arena, end)));
			}
			++self->counters.pTreeClearSplits;
		}
		data.insert(m.param1, ValueOrClearToRef::value(m.param2));
		self->watches.trigger(m.param1);
		++self->counters.pTreeSets;
	} else if (m.type == MutationRef::ClearRange) {
		data.erase(m.param1, m.param2);
		ASSERT(m.param2 > m.param1);
		if (EXPENSIVE_VALIDATION) {
			ASSERT(!data.isClearContaining(data.atLatest(), m.param1));
		}
		data.insert(m.param1, ValueOrClearToRef::clearTo(m.param2));
		self->watches.triggerRange(m.param1, m.param2);
		++self->counters.pTreeClears;
	}
}

void applyChangeFeedMutation(StorageServer* self,
                             MutationRef const& m,
                             MutationRefAndCipherKeys const& encryptedMutation,
                             Version version,
                             KeyRangeRef const& shard) {
	ASSERT(self->encryptionMode.present());
	ASSERT(!self->encryptionMode.get().isEncryptionEnabled() || encryptedMutation.mutation.isEncrypted() ||
	       isBackupLogMutation(m) || mutationForKey(m, lastEpochEndPrivateKey));
	if (m.type == MutationRef::SetValue) {
		for (auto& it : self->keyChangeFeed[m.param1]) {
			if (version < it->stopVersion && !it->removing && version > it->emptyVersion) {
				if (it->mutations.empty() || it->mutations.back().version != version) {
					it->mutations.push_back(
					    EncryptedMutationsAndVersionRef(version, self->knownCommittedVersion.get()));
				}
				if (encryptedMutation.mutation.isValid()) {
					if (!it->mutations.back().encrypted.present()) {
						it->mutations.back().encrypted = it->mutations.back().mutations;
						it->mutations.back().cipherKeys.resize(it->mutations.back().mutations.size());
					}
					it->mutations.back().encrypted.get().push_back_deep(it->mutations.back().arena(),
					                                                    encryptedMutation.mutation);
					it->mutations.back().cipherKeys.push_back(encryptedMutation.cipherKeys);
				} else if (it->mutations.back().encrypted.present()) {
					it->mutations.back().encrypted.get().push_back_deep(it->mutations.back().arena(), m);
					it->mutations.back().cipherKeys.push_back(TextAndHeaderCipherKeys());
				}
				it->mutations.back().mutations.push_back_deep(it->mutations.back().arena(), m);

				self->currentChangeFeeds.insert(it->id);
				self->addFeedBytesAtVersion(m.totalSize(), version);

				DEBUG_MUTATION("ChangeFeedWriteSet", version, m, self->thisServerID)
				    .detail("Range", it->range)
				    .detail("ChangeFeedID", it->id);

				++self->counters.changeFeedMutations;
			} else {
				CODE_PROBE(version <= it->emptyVersion, "Skip CF write because version <= emptyVersion");
				CODE_PROBE(it->removing, "Skip CF write because removing");
				CODE_PROBE(version >= it->stopVersion, "Skip CF write because stopped");
				DEBUG_MUTATION("ChangeFeedWriteSetIgnore", version, m, self->thisServerID)
				    .detail("Range", it->range)
				    .detail("ChangeFeedID", it->id)
				    .detail("StopVersion", it->stopVersion)
				    .detail("EmptyVersion", it->emptyVersion)
				    .detail("Removing", it->removing);
			}
		}
	} else if (m.type == MutationRef::ClearRange) {
		KeyRangeRef mutationClearRange(m.param1, m.param2);
		// FIXME: this might double-insert clears if the same feed appears in multiple sub-ranges
		auto ranges = self->keyChangeFeed.intersectingRanges(mutationClearRange);
		for (auto& r : ranges) {
			for (auto& it : r.value()) {
				if (version < it->stopVersion && !it->removing && version > it->emptyVersion) {
					// clamp feed mutation to change feed range
					MutationRef clearMutation = m;
					bool modified = false;
					if (clearMutation.param1 < it->range.begin) {
						clearMutation.param1 = it->range.begin;
						modified = true;
					}
					if (clearMutation.param2 > it->range.end) {
						clearMutation.param2 = it->range.end;
						modified = true;
					}
					if (!modified && (clearMutation.param1 == shard.begin || clearMutation.param2 == shard.end)) {
						modified = true;
					}
					if (it->mutations.empty() || it->mutations.back().version != version) {
						it->mutations.push_back(
						    EncryptedMutationsAndVersionRef(version, self->knownCommittedVersion.get()));
					}
					if (encryptedMutation.mutation.isEncrypted()) {
						if (!it->mutations.back().encrypted.present()) {
							it->mutations.back().encrypted = it->mutations.back().mutations;
							it->mutations.back().cipherKeys.resize(it->mutations.back().mutations.size());
						}
						if (modified) {
							it->mutations.back().encrypted.get().push_back_deep(
							    it->mutations.back().arena(),
							    clearMutation.encrypt(encryptedMutation.cipherKeys,
							                          it->mutations.back().arena(),
							                          BlobCipherMetrics::TLOG));
						} else {
							it->mutations.back().encrypted.get().push_back_deep(it->mutations.back().arena(),
							                                                    encryptedMutation.mutation);
						}
						it->mutations.back().cipherKeys.push_back(encryptedMutation.cipherKeys);
					} else if (it->mutations.back().encrypted.present()) {
						it->mutations.back().encrypted.get().push_back_deep(it->mutations.back().arena(), m);
						it->mutations.back().cipherKeys.push_back(TextAndHeaderCipherKeys());
					}

					it->mutations.back().mutations.push_back_deep(it->mutations.back().arena(), clearMutation);
					self->currentChangeFeeds.insert(it->id);
					self->addFeedBytesAtVersion(m.totalSize(), version);

					DEBUG_MUTATION("ChangeFeedWriteClear", version, m, self->thisServerID)
					    .detail("Range", it->range)
					    .detail("ChangeFeedID", it->id);
					++self->counters.changeFeedMutations;
				} else {
					CODE_PROBE(version <= it->emptyVersion, "Skip CF clear because version <= emptyVersion");
					CODE_PROBE(it->removing, "Skip CF clear because removing");
					CODE_PROBE(version >= it->stopVersion, "Skip CF clear because stopped");
					DEBUG_MUTATION("ChangeFeedWriteClearIgnore", version, m, self->thisServerID)
					    .detail("Range", it->range)
					    .detail("ChangeFeedID", it->id)
					    .detail("StopVersion", it->stopVersion)
					    .detail("EmptyVersion", it->emptyVersion)
					    .detail("Removing", it->removing);
				}
			}
		}
	}
}

void removeDataRange(StorageServer* ss,
                     Standalone<VerUpdateRef>& mLV,
                     KeyRangeMap<Reference<ShardInfo>>& shards,
                     KeyRangeRef range) {
	// modify the latest version of data to remove all sets and trim all clears to exclude range.
	// Add a clear to mLV (mutationLog[data.getLatestVersion()]) that ensures all keys in range are removed from the
	// disk when this latest version becomes durable mLV is also modified if necessary to ensure that split clears can
	// be forgotten

	MutationRef clearRange(MutationRef::ClearRange, range.begin, range.end);
	clearRange = ss->addMutationToMutationLog(mLV, clearRange);

	auto& data = ss->mutableData();

	// Expand the range to the right to include other shards not in versionedData
	for (auto r = shards.rangeContaining(range.end); r != shards.ranges().end() && !r->value()->isInVersionedData();
	     ++r)
		range = KeyRangeRef(range.begin, r->end());

	auto endClear = data.atLatest().lastLess(range.end);
	if (endClear && endClear->isClearTo() && endClear->getEndKey() > range.end) {
		// This clear has been bumped up to insertVersion==data.getLatestVersion and needs a corresponding mutation log
		// entry to forget
		MutationRef m(MutationRef::ClearRange, range.end, endClear->getEndKey());
		m = ss->addMutationToMutationLog(mLV, m);
		data.insert(m.param1, ValueOrClearToRef::clearTo(m.param2));
		++ss->counters.kvSystemClearRanges;
	}

	auto beginClear = data.atLatest().lastLess(range.begin);
	if (beginClear && beginClear->isClearTo() && beginClear->getEndKey() > range.begin) {
		// We don't need any special mutationLog entry - because the begin key and insert version are unchanged the
		// original clear
		//   mutation works to forget this one - but we need range.begin in the right arena
		KeyRef rb(mLV.arena(), range.begin);
		// insert() invalidates beginClear, so beginClear.key() is not safe to pass to it by reference
		data.insert(KeyRef(beginClear.key()), ValueOrClearToRef::clearTo(rb), beginClear.insertVersion());
	}

	data.erase(range.begin, range.end);
}

void setAvailableStatus(StorageServer* self, KeyRangeRef keys, bool available);
void setAssignedStatus(StorageServer* self, KeyRangeRef keys, bool nowAssigned);
void updateStorageShard(StorageServer* self, StorageServerShard shard);
void setRangeBasedBulkLoadStatus(StorageServer* self, KeyRangeRef keys, const SSBulkLoadMetadata& ssBulkLoadMetadata);

void coalescePhysicalShards(StorageServer* data, KeyRangeRef keys) {
	auto shardRanges = data->shards.intersectingRanges(keys);
	auto fullRange = data->shards.ranges();

	auto iter = shardRanges.begin();
	if (iter != fullRange.begin()) {
		--iter;
	}
	auto iterEnd = shardRanges.end();
	if (iterEnd != fullRange.end()) {
		++iterEnd;
	}

	KeyRangeMap<Reference<ShardInfo>>::iterator lastShard = iter;
	++iter;

	for (; iter != iterEnd; ++iter) {
		if (ShardInfo::canMerge(lastShard.value().getPtr(), iter->value().getPtr())) {
			ShardInfo* newShard = lastShard.value().extractPtr();
			ASSERT(newShard->mergeWith(iter->value().getPtr()));
			data->addShard(newShard);
			iter = data->shards.rangeContaining(newShard->keys.begin);
		}
		lastShard = iter;
	}
}

void coalesceShards(StorageServer* data, KeyRangeRef keys) {
	auto shardRanges = data->shards.intersectingRanges(keys);
	auto fullRange = data->shards.ranges();

	auto iter = shardRanges.begin();
	if (iter != fullRange.begin())
		--iter;
	auto iterEnd = shardRanges.end();
	if (iterEnd != fullRange.end())
		++iterEnd;

	bool lastReadable = false;
	bool lastNotAssigned = false;
	KeyRangeMap<Reference<ShardInfo>>::iterator lastRange;

	for (; iter != iterEnd; ++iter) {
		if (lastReadable && iter->value()->isReadable()) {
			KeyRange range = KeyRangeRef(lastRange->begin(), iter->end());
			data->addShard(ShardInfo::newReadWrite(range, data));
			iter = data->shards.rangeContaining(range.begin);
		} else if (lastNotAssigned && iter->value()->notAssigned()) {
			KeyRange range = KeyRangeRef(lastRange->begin(), iter->end());
			data->addShard(ShardInfo::newNotAssigned(range));
			iter = data->shards.rangeContaining(range.begin);
		}

		lastReadable = iter->value()->isReadable();
		lastNotAssigned = iter->value()->notAssigned();
		lastRange = iter;
	}
}

template <class T>
void addMutation(T& target,
                 Version version,
                 bool fromFetch,
                 MutationRef const& mutation,
                 MutationRefAndCipherKeys const& encryptedMutation) {
	target.addMutation(version, fromFetch, mutation, encryptedMutation);
}

template <class T>
void addMutation(Reference<T>& target,
                 Version version,
                 bool fromFetch,
                 MutationRef const& mutation,
                 MutationRefAndCipherKeys const& encryptedMutation) {
	addMutation(*target, version, fromFetch, mutation, encryptedMutation);
}

template <class T>
void splitMutations(StorageServer* data, KeyRangeMap<T>& map, VerUpdateRef const& update) {
	for (int i = 0; i < update.mutations.size(); i++) {
		splitMutation(data, map, update.mutations[i], MutationRefAndCipherKeys(), update.version, update.version);
	}
}

template <class T>
void splitMutation(StorageServer* data,
                   KeyRangeMap<T>& map,
                   MutationRef const& m,
                   MutationRefAndCipherKeys const& encryptedMutation,
                   Version ver,
                   bool fromFetch) {
	if (isSingleKeyMutation((MutationRef::Type)m.type)) {
		if (!SHORT_CIRCUT_ACTUAL_STORAGE || !normalKeys.contains(m.param1))
			addMutation(map.rangeContaining(m.param1)->value(), ver, fromFetch, m, encryptedMutation);
	} else if (m.type == MutationRef::ClearRange) {
		KeyRangeRef mKeys(m.param1, m.param2);
		if (!SHORT_CIRCUT_ACTUAL_STORAGE || !normalKeys.contains(mKeys)) {
			auto r = map.intersectingRanges(mKeys);
			for (auto i = r.begin(); i != r.end(); ++i) {
				KeyRangeRef k = mKeys & i->range();
				addMutation(i->value(),
				            ver,
				            fromFetch,
				            MutationRef((MutationRef::Type)m.type, k.begin, k.end),
				            encryptedMutation);
			}
		}
	} else
		ASSERT(false); // Unknown mutation type in splitMutations
}

ACTOR Future<Void> logFetchKeysWarning(AddingShard* shard) {
	state double startTime = now();
	loop {
		state double waitSeconds = BUGGIFY ? 5.0 : 600.0;
		wait(delay(waitSeconds));

		const auto traceEventLevel =
		    waitSeconds > SERVER_KNOBS->FETCH_KEYS_TOO_LONG_TIME_CRITERIA ? SevWarnAlways : SevInfo;
		TraceEvent(traceEventLevel, "FetchKeysTooLong")
		    .detail("Duration", now() - startTime)
		    .detail("Phase", shard->phase)
		    .detail("Begin", shard->keys.begin)
		    .detail("End", shard->keys.end);
	}
}

class FetchKeysMetricReporter {
	const UID uid;
	const double startTime;
	int fetchedBytes;
	StorageServer::FetchKeysHistograms& histograms;
	StorageServer::CurrentRunningFetchKeys& currentRunning;
	Counter& bytesFetchedCounter;
	Counter& kvFetchedCounter;

public:
	FetchKeysMetricReporter(const UID& uid_,
	                        const double startTime_,
	                        const KeyRange& keyRange,
	                        StorageServer::FetchKeysHistograms& histograms_,
	                        StorageServer::CurrentRunningFetchKeys& currentRunning_,
	                        Counter& bytesFetchedCounter,
	                        Counter& kvFetchedCounter)
	  : uid(uid_), startTime(startTime_), fetchedBytes(0), histograms(histograms_), currentRunning(currentRunning_),
	    bytesFetchedCounter(bytesFetchedCounter), kvFetchedCounter(kvFetchedCounter) {

		currentRunning.recordStart(uid, keyRange);
	}

	void addFetchedBytes(const int bytes, const int kvCount) {
		fetchedBytes += bytes;
		bytesFetchedCounter += bytes;
		kvFetchedCounter += kvCount;
	}

	~FetchKeysMetricReporter() {
		double latency = now() - startTime;

		// If fetchKeys is *NOT* run, i.e. returning immediately, still report a record.
		if (latency == 0)
			latency = 1e6;

		const uint32_t bandwidth = fetchedBytes / latency;

		histograms.latency->sampleSeconds(latency);
		histograms.bytes->sample(fetchedBytes);
		histograms.bandwidth->sample(bandwidth);

		currentRunning.recordFinish(uid);
	}
};

ACTOR Future<Void> tryGetRange(PromiseStream<RangeResult> results, Transaction* tr, KeyRange keys) {
	if (SERVER_KNOBS->FETCH_USING_STREAMING) {
		wait(tr->getRangeStream(results, keys, GetRangeLimits(), Snapshot::True));
		return Void();
	}

	state KeySelectorRef begin = firstGreaterOrEqual(keys.begin);
	state KeySelectorRef end = firstGreaterOrEqual(keys.end);

	try {
		loop {
			GetRangeLimits limits(GetRangeLimits::ROW_LIMIT_UNLIMITED, SERVER_KNOBS->FETCH_BLOCK_BYTES);
			limits.minRows = 0;
			state RangeResult rep = wait(tr->getRange(begin, end, limits, Snapshot::True));
			results.send(rep);

			if (!rep.more) {
				results.sendError(end_of_stream());
				return Void();
			}

			begin = rep.nextBeginKeySelector();
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		results.sendError(e);
		throw;
	}
}

// Read blob granules metadata. It keeps retrying until reaching maxRetryCount.
// The key range should not cross tenant boundary.
ACTOR Future<Standalone<VectorRef<BlobGranuleChunkRef>>> tryReadBlobGranuleChunks(Transaction* tr,
                                                                                  KeyRange keys,
                                                                                  Version fetchVersion) {
	state Version readVersion = fetchVersion;
	loop {
		try {
			Standalone<VectorRef<BlobGranuleChunkRef>> chunks = wait(tr->readBlobGranules(keys, 0, readVersion));
			TraceEvent(SevDebug, "ReadBlobGranules")
			    .detail("Keys", keys)
			    .detail("Chunks", chunks.size())
			    .detail("FetchVersion", fetchVersion);
			return chunks;
		} catch (Error& e) {
			if (e.code() == error_code_blob_granule_transaction_too_old) {
				if (SERVER_KNOBS->BLOB_RESTORE_SKIP_EMPTY_RANGES) {
					CODE_PROBE(true, "Skip blob ranges for restore", probe::decoration::rare);
					TraceEvent(SevWarn, "SkipBlobGranuleForRestore").error(e).detail("Keys", keys);
					Standalone<VectorRef<BlobGranuleChunkRef>> empty;
					return empty;
				} else {
					TraceEvent(SevWarn, "NotRestorableBlobGranule").error(e).detail("Keys", keys);
				}
			}
			wait(tr->onError(e));
		}
	}
}

// Read blob granules metadata. The key range can cross tenant bundary.
ACTOR Future<Standalone<VectorRef<BlobGranuleChunkRef>>> readBlobGranuleChunks(Transaction* tr,
                                                                               Database cx,
                                                                               KeyRangeRef keys,
                                                                               Version fetchVersion) {
	state Standalone<VectorRef<BlobGranuleChunkRef>> results;
	state Standalone<VectorRef<KeyRangeRef>> ranges = wait(cx->listBlobbifiedRanges(keys, CLIENT_KNOBS->TOO_MANY));
	for (auto& range : ranges) {
		KeyRangeRef intersectedRange(std::max(keys.begin, range.begin), std::min(keys.end, range.end));
		Standalone<VectorRef<BlobGranuleChunkRef>> chunks =
		    wait(tryReadBlobGranuleChunks(tr, intersectedRange, fetchVersion));
		results.append(results.arena(), chunks.begin(), chunks.size());
		results.arena().dependsOn(chunks.arena());
	}
	return results;
}

// Read keys from blob storage
ACTOR Future<Void> tryGetRangeFromBlob(PromiseStream<RangeResult> results,
                                       Transaction* tr,
                                       Database cx,
                                       KeyRange keys,
                                       Version fetchVersion,
                                       BGTenantMap* tenantData) {
	try {
		state Standalone<VectorRef<BlobGranuleChunkRef>> chunks =
		    wait(readBlobGranuleChunks(tr, cx, keys, fetchVersion));
		TraceEvent(SevDebug, "ReadBlobGranuleChunks").detail("Keys", keys).detail("Chunks", chunks.size());

		state int i;
		for (i = 0; i < chunks.size(); ++i) {
			state KeyRangeRef chunkRange = chunks[i].keyRange;
			// Chunk is empty if no snapshot file. Skip it
			if (!chunks[i].snapshotFile.present()) {
				TraceEvent("SkipEmptyBlobChunkForRestore")
				    .detail("Chunk", chunks[i].keyRange)
				    .detail("Version", chunks[i].includedVersion);
				RangeResult rows;
				if (i == chunks.size() - 1) {
					rows.more = false;
				} else {
					rows.more = true;
					rows.readThrough = KeyRef(rows.arena(), std::min(chunkRange.end, keys.end));
				}
				results.send(rows);
				continue;
			}
			try {
				state Reference<BlobConnectionProvider> blobConn = wait(loadBStoreForTenant(tenantData, chunkRange));
				state RangeResult rows = wait(readBlobGranule(chunks[i], keys, 0, fetchVersion, blobConn));

				TraceEvent(SevDebug, "ReadBlobData")
				    .detail("Rows", rows.size())
				    .detail("ChunkRange", chunkRange)
				    .detail("FetchVersion", fetchVersion);
				// It should read all the data from that chunk
				ASSERT(!rows.more);
				if (i == chunks.size() - 1) {
					// set more to false when it's the last chunk
					rows.more = false;
				} else {
					rows.more = true;
					rows.readThrough = KeyRef(rows.arena(), std::min(chunkRange.end, keys.end));
				}
				results.send(rows);
			} catch (Error& err) {
				if (err.code() == error_code_file_not_found ||
				    err.code() == error_code_blob_granule_transaction_too_old) {
					if (SERVER_KNOBS->BLOB_RESTORE_SKIP_EMPTY_RANGES) {
						// skip no data ranges and restore as much data as we can
						TraceEvent(SevWarn, "SkipBlobChunkForRestore").error(err).detail("ChunkRange", chunkRange);
						RangeResult rows;
						results.send(rows);
						CODE_PROBE(true, "Skip blob chunks for restore", probe::decoration::rare);
					} else {
						TraceEvent(SevWarn, "NotRestorableBlobChunk").error(err).detail("ChunkRange", chunkRange);
						throw;
					}
				} else {
					throw;
				}
			}
		}

		if (chunks.size() == 0) {
			RangeResult rows;
			results.send(rows);
		}

		results.sendError(end_of_stream()); // end of range read
	} catch (Error& e) {
		TraceEvent(SevWarn, "ReadBlobDataFailure")
		    .suppressFor(5.0)
		    .detail("Keys", keys)
		    .detail("FetchVersion", fetchVersion)
		    .detail("Error", e.what());
		tr->reset();
		tr->setVersion(fetchVersion);
		results.sendError(e);
	}
	return Void();
}

// We have to store the version the change feed was stopped at in the SS instead of just the stopped status
// In addition to simplifying stopping logic, it enables communicating stopped status when fetching change feeds
// from other SS correctly
const Value changeFeedSSValue(KeyRangeRef const& range,
                              Version popVersion,
                              Version stopVersion,
                              Version metadataVersion) {
	BinaryWriter wr(IncludeVersion(ProtocolVersion::withChangeFeed()));
	wr << range;
	wr << popVersion;
	wr << stopVersion;
	wr << metadataVersion;
	return wr.toValue();
}

std::tuple<KeyRange, Version, Version, Version> decodeChangeFeedSSValue(ValueRef const& value) {
	KeyRange range;
	Version popVersion, stopVersion, metadataVersion;
	BinaryReader reader(value, IncludeVersion());
	reader >> range;
	reader >> popVersion;
	reader >> stopVersion;
	reader >> metadataVersion;
	return std::make_tuple(range, popVersion, stopVersion, metadataVersion);
}

ACTOR Future<Void> changeFeedPopQ(StorageServer* self, ChangeFeedPopRequest req) {
	// if a SS restarted and is way behind, wait for it to at least have caught up through the pop version
	wait(self->version.whenAtLeast(req.version));
	wait(delay(0));

	if (!self->isReadable(req.range)) {
		req.reply.sendError(wrong_shard_server());
		return Void();
	}
	auto feed = self->uidChangeFeed.find(req.rangeID);
	if (feed == self->uidChangeFeed.end()) {
		req.reply.sendError(unknown_change_feed());
		return Void();
	}

	TraceEvent(SevDebug, "ChangeFeedPopQuery", self->thisServerID)
	    .detail("FeedID", req.rangeID)
	    .detail("Version", req.version)
	    .detail("SSVersion", self->version.get())
	    .detail("Range", req.range);

	if (req.version - 1 > feed->second->emptyVersion) {
		feed->second->emptyVersion = req.version - 1;
		while (!feed->second->mutations.empty() && feed->second->mutations.front().version < req.version) {
			feed->second->mutations.pop_front();
		}
		if (!feed->second->destroyed) {
			Version durableVersion = self->data().getLatestVersion();
			auto& mLV = self->addVersionToMutationLog(durableVersion);
			self->addMutationToMutationLog(
			    mLV,
			    MutationRef(MutationRef::SetValue,
			                persistChangeFeedKeys.begin.toString() + feed->second->id.toString(),
			                changeFeedSSValue(feed->second->range,
			                                  feed->second->emptyVersion + 1,
			                                  feed->second->stopVersion,
			                                  feed->second->metadataVersion)));
			if (feed->second->storageVersion != invalidVersion) {
				++self->counters.kvSystemClearRanges;
				self->addMutationToMutationLog(mLV,
				                               MutationRef(MutationRef::ClearRange,
				                                           changeFeedDurableKey(feed->second->id, 0),
				                                           changeFeedDurableKey(feed->second->id, req.version)));
				if (req.version > feed->second->storageVersion) {
					feed->second->storageVersion = invalidVersion;
					feed->second->durableVersion = invalidVersion;
				}
			}
			wait(self->durableVersion.whenAtLeast(durableVersion));
		}
	}
	req.reply.send(Void());
	return Void();
}

// FIXME: there's a decent amount of duplicated code around fetching and popping change feeds
// Returns max version fetched
ACTOR Future<Version> fetchChangeFeedApplier(StorageServer* data,
                                             Reference<ChangeFeedInfo> changeFeedInfo,
                                             Key rangeId,
                                             KeyRange range,
                                             Version emptyVersion,
                                             Version beginVersion,
                                             Version endVersion,
                                             ReadOptions readOptions) {
	state FlowLock::Releaser feedFetchReleaser;

	// avoid fetching the same version range of the same change feed multiple times.
	choose {
		when(wait(changeFeedInfo->fetchLock.take())) {
			feedFetchReleaser = FlowLock::Releaser(changeFeedInfo->fetchLock);
		}
		when(wait(changeFeedInfo->durableFetchVersion.whenAtLeast(endVersion))) {
			return invalidVersion;
		}
	}

	state Version startVersion = beginVersion;
	startVersion = std::max(startVersion, emptyVersion + 1);
	startVersion = std::max(startVersion, changeFeedInfo->fetchVersion + 1);
	startVersion = std::max(startVersion, changeFeedInfo->durableFetchVersion.get() + 1);

	ASSERT(startVersion >= 0);

	if (startVersion >= endVersion || (changeFeedInfo->removing)) {
		CODE_PROBE(true, "Change Feed popped before fetch");
		TraceEvent(SevDebug, "FetchChangeFeedNoOp", data->thisServerID)
		    .detail("FeedID", rangeId)
		    .detail("Range", range)
		    .detail("StartVersion", startVersion)
		    .detail("EndVersion", endVersion)
		    .detail("Removing", changeFeedInfo->removing);
		return invalidVersion;
	}

	// FIXME: if this feed range is not wholly contained within the shard, set cache to true on reading
	state Reference<ChangeFeedData> feedResults = makeReference<ChangeFeedData>();
	state Future<Void> feed = data->cx->getChangeFeedStream(feedResults,
	                                                        rangeId,
	                                                        startVersion,
	                                                        endVersion,
	                                                        range,
	                                                        SERVER_KNOBS->CHANGEFEEDSTREAM_LIMIT_BYTES,
	                                                        true,
	                                                        readOptions,
	                                                        true);

	state Version firstVersion = invalidVersion;
	state Version lastVersion = invalidVersion;
	state int64_t versionsFetched = 0;

	// ensure SS is at least caught up to begin version, to maintain behavior with old fetch
	wait(data->version.whenAtLeast(startVersion));

	try {
		loop {
			while (data->fetchKeysBudgetUsed.get()) {
				wait(data->fetchKeysBudgetUsed.onChange());
			}

			state Standalone<VectorRef<MutationsAndVersionRef>> remoteResult =
			    waitNext(feedResults->mutations.getFuture());
			state int remoteLoc = 0;
			// ensure SS is at least caught up to begin version, to maintain behavior with old fetch
			if (!remoteResult.empty()) {
				wait(data->version.whenAtLeast(remoteResult.back().version));
			}

			while (remoteLoc < remoteResult.size()) {
				if (feedResults->popVersion - 1 > changeFeedInfo->emptyVersion) {
					CODE_PROBE(true, "CF fetched updated popped version from src SS");
					changeFeedInfo->emptyVersion = feedResults->popVersion - 1;
					// pop mutations
					while (!changeFeedInfo->mutations.empty() &&
					       changeFeedInfo->mutations.front().version <= changeFeedInfo->emptyVersion) {
						changeFeedInfo->mutations.pop_front();
					}
					auto& mLV = data->addVersionToMutationLog(data->data().getLatestVersion());
					data->addMutationToMutationLog(
					    mLV,
					    MutationRef(MutationRef::SetValue,
					                persistChangeFeedKeys.begin.toString() + changeFeedInfo->id.toString(),
					                changeFeedSSValue(changeFeedInfo->range,
					                                  changeFeedInfo->emptyVersion + 1,
					                                  changeFeedInfo->stopVersion,
					                                  changeFeedInfo->metadataVersion)));
					data->addMutationToMutationLog(
					    mLV,
					    MutationRef(MutationRef::ClearRange,
					                changeFeedDurableKey(changeFeedInfo->id, 0),
					                changeFeedDurableKey(changeFeedInfo->id, feedResults->popVersion)));
					++data->counters.kvSystemClearRanges;
				}

				Version remoteVersion = remoteResult[remoteLoc].version;
				// ensure SS is at least caught up to this version, to maintain behavior with old fetch
				ASSERT(remoteVersion <= data->version.get());
				if (remoteVersion > changeFeedInfo->emptyVersion) {
					if (MUTATION_TRACKING_ENABLED) {
						for (auto& m : remoteResult[remoteLoc].mutations) {
							DEBUG_MUTATION("ChangeFeedWriteMove", remoteVersion, m, data->thisServerID)
							    .detail("Range", range)
							    .detail("ChangeFeedID", rangeId);
						}
					}
					data->storage.writeKeyValue(
					    KeyValueRef(changeFeedDurableKey(rangeId, remoteVersion),
					                changeFeedDurableValue(remoteResult[remoteLoc].mutations,
					                                       remoteResult[remoteLoc].knownCommittedVersion)));
					++data->counters.kvSystemClearRanges;
					changeFeedInfo->fetchVersion = std::max(changeFeedInfo->fetchVersion, remoteVersion);

					if (firstVersion == invalidVersion) {
						firstVersion = remoteVersion;
					}
					lastVersion = remoteVersion;
					versionsFetched++;
				} else {
					CODE_PROBE(true, "Change feed ignoring write on move because it was popped concurrently");
					if (MUTATION_TRACKING_ENABLED) {
						for (auto& m : remoteResult[remoteLoc].mutations) {
							DEBUG_MUTATION("ChangeFeedWriteMoveIgnore", remoteVersion, m, data->thisServerID)
							    .detail("Range", range)
							    .detail("ChangeFeedID", rangeId)
							    .detail("EmptyVersion", changeFeedInfo->emptyVersion);
						}
					}
					if (versionsFetched > 0) {
						ASSERT(firstVersion != invalidVersion);
						ASSERT(lastVersion != invalidVersion);
						data->storage.clearRange(
						    KeyRangeRef(changeFeedDurableKey(changeFeedInfo->id, firstVersion),
						                changeFeedDurableKey(changeFeedInfo->id, lastVersion + 1)));
						++data->counters.kvSystemClearRanges;
						firstVersion = invalidVersion;
						lastVersion = invalidVersion;
						versionsFetched = 0;
					}
				}
				remoteLoc++;
			}
			// Do this once per wait instead of once per version for efficiency
			data->fetchingChangeFeeds.insert(changeFeedInfo->id);

			data->counters.feedBytesFetched += remoteResult.expectedSize();
			data->fetchKeysBytesBudget -= remoteResult.expectedSize();
			data->fetchKeysBudgetUsed.set(data->fetchKeysBytesBudget <= 0);
			wait(yield());
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream) {
			TraceEvent(SevDebug, "FetchChangeFeedError", data->thisServerID)
			    .errorUnsuppressed(e)
			    .detail("FeedID", rangeId)
			    .detail("Range", range)
			    .detail("EndVersion", endVersion)
			    .detail("Removing", changeFeedInfo->removing)
			    .detail("Destroyed", changeFeedInfo->destroyed);
			throw;
		}
	}

	if (feedResults->popVersion - 1 > changeFeedInfo->emptyVersion) {
		CODE_PROBE(true, "CF fetched updated popped version from src SS at end");
		changeFeedInfo->emptyVersion = feedResults->popVersion - 1;
		while (!changeFeedInfo->mutations.empty() &&
		       changeFeedInfo->mutations.front().version <= changeFeedInfo->emptyVersion) {
			changeFeedInfo->mutations.pop_front();
		}
		auto& mLV = data->addVersionToMutationLog(data->data().getLatestVersion());
		data->addMutationToMutationLog(
		    mLV,
		    MutationRef(MutationRef::SetValue,
		                persistChangeFeedKeys.begin.toString() + changeFeedInfo->id.toString(),
		                changeFeedSSValue(changeFeedInfo->range,
		                                  changeFeedInfo->emptyVersion + 1,
		                                  changeFeedInfo->stopVersion,
		                                  changeFeedInfo->metadataVersion)));
		data->addMutationToMutationLog(mLV,
		                               MutationRef(MutationRef::ClearRange,
		                                           changeFeedDurableKey(changeFeedInfo->id, 0),
		                                           changeFeedDurableKey(changeFeedInfo->id, feedResults->popVersion)));
		++data->counters.kvSystemClearRanges;
	}

	// if we were popped or removed while fetching but it didn't pass the fetch version while writing, clean up here
	if (versionsFetched > 0 && startVersion < changeFeedInfo->emptyVersion) {
		CODE_PROBE(true, "Change feed cleaning up popped data after move");
		ASSERT(firstVersion != invalidVersion);
		ASSERT(lastVersion != invalidVersion);
		Version endClear = std::min(lastVersion + 1, changeFeedInfo->emptyVersion);
		if (endClear > firstVersion) {
			auto& mLV2 = data->addVersionToMutationLog(data->data().getLatestVersion());
			data->addMutationToMutationLog(mLV2,
			                               MutationRef(MutationRef::ClearRange,
			                                           changeFeedDurableKey(changeFeedInfo->id, firstVersion),
			                                           changeFeedDurableKey(changeFeedInfo->id, endClear)));
			++data->counters.kvSystemClearRanges;
		}
	}

	TraceEvent(SevDebug, "FetchChangeFeedDone", data->thisServerID)
	    .detail("FeedID", rangeId)
	    .detail("Range", range)
	    .detail("StartVersion", startVersion)
	    .detail("EndVersion", endVersion)
	    .detail("EmptyVersion", changeFeedInfo->emptyVersion)
	    .detail("FirstFetchedVersion", firstVersion)
	    .detail("LastFetchedVersion", lastVersion)
	    .detail("VersionsFetched", versionsFetched)
	    .detail("Removed", changeFeedInfo->removing);
	return lastVersion;
}

// returns largest version fetched
ACTOR Future<Version> fetchChangeFeed(StorageServer* data,
                                      Reference<ChangeFeedInfo> changeFeedInfo,
                                      Version beginVersion,
                                      Version endVersion,
                                      ReadOptions readOptions) {
	wait(delay(0)); // allow this actor to be cancelled by removals

	TraceEvent(SevDebug, "FetchChangeFeed", data->thisServerID)
	    .detail("FeedID", changeFeedInfo->id)
	    .detail("Range", changeFeedInfo->range)
	    .detail("BeginVersion", beginVersion)
	    .detail("EndVersion", endVersion);

	auto cleanupPending = data->changeFeedCleanupDurable.find(changeFeedInfo->id);
	if (cleanupPending != data->changeFeedCleanupDurable.end()) {
		CODE_PROBE(true, "Change feed waiting for dirty previous move to finish");
		TraceEvent(SevDebug, "FetchChangeFeedWaitCleanup", data->thisServerID)
		    .detail("FeedID", changeFeedInfo->id)
		    .detail("Range", changeFeedInfo->range)
		    .detail("CleanupVersion", cleanupPending->second)
		    .detail("EmptyVersion", changeFeedInfo->emptyVersion)
		    .detail("BeginVersion", beginVersion)
		    .detail("EndVersion", endVersion);
		wait(data->durableVersion.whenAtLeast(cleanupPending->second + 1));
		wait(delay(0));
		// shard might have gotten moved away (again) while we were waiting
		auto cleanupPendingAfter = data->changeFeedCleanupDurable.find(changeFeedInfo->id);
		if (cleanupPendingAfter != data->changeFeedCleanupDurable.end()) {
			ASSERT(cleanupPendingAfter->second >= endVersion);
			TraceEvent(SevDebug, "FetchChangeFeedCancelledByCleanup", data->thisServerID)
			    .detail("FeedID", changeFeedInfo->id)
			    .detail("Range", changeFeedInfo->range)
			    .detail("BeginVersion", beginVersion)
			    .detail("EndVersion", endVersion);
			return invalidVersion;
		}
	}

	state bool seenNotRegistered = false;
	loop {
		try {
			Version maxFetched = wait(fetchChangeFeedApplier(data,
			                                                 changeFeedInfo,
			                                                 changeFeedInfo->id,
			                                                 changeFeedInfo->range,
			                                                 changeFeedInfo->emptyVersion,
			                                                 beginVersion,
			                                                 endVersion,
			                                                 readOptions));
			data->fetchingChangeFeeds.insert(changeFeedInfo->id);
			return maxFetched;
		} catch (Error& e) {
			if (e.code() != error_code_change_feed_not_registered) {
				throw;
			}
		}

		// There are two reasons for change_feed_not_registered:
		//   1. The feed was just created, but the ss mutation stream is ahead of the GRV that
		//   fetchChangeFeedApplier uses to read the change feed data from the database. In this case we need to
		//   wait and retry
		//   2. The feed was destroyed, but we missed a metadata update telling us this. In this case we need to
		//   destroy the feed
		// endVersion >= the metadata create version, so we can safely use it as a proxy
		if (beginVersion != 0 || seenNotRegistered || endVersion <= data->desiredOldestVersion.get()) {
			// If any of these are true, the feed must be destroyed.
			Version cleanupVersion = data->data().getLatestVersion();

			TraceEvent(SevDebug, "DestroyingChangeFeedFromFetch", data->thisServerID)
			    .detail("FeedID", changeFeedInfo->id)
			    .detail("Range", changeFeedInfo->range)
			    .detail("Version", cleanupVersion);

			if (g_network->isSimulated() && !g_simulator->restarted) {
				// verify that the feed was actually destroyed and it's not an error in this inference logic.
				// Restarting tests produce false positives because the validation state isn't kept across tests
				ASSERT(g_simulator->validationData.allDestroyedChangeFeedIDs.contains(changeFeedInfo->id.toString()));
			}

			Key beginClearKey = changeFeedInfo->id.withPrefix(persistChangeFeedKeys.begin);

			auto& mLV = data->addVersionToMutationLog(cleanupVersion);
			data->addMutationToMutationLog(
			    mLV, MutationRef(MutationRef::ClearRange, beginClearKey, keyAfter(beginClearKey)));
			++data->counters.kvSystemClearRanges;
			data->addMutationToMutationLog(mLV,
			                               MutationRef(MutationRef::ClearRange,
			                                           changeFeedDurableKey(changeFeedInfo->id, 0),
			                                           changeFeedDurableKey(changeFeedInfo->id, cleanupVersion)));
			++data->counters.kvSystemClearRanges;

			changeFeedInfo->destroy(cleanupVersion);

			if (data->uidChangeFeed.contains(changeFeedInfo->id)) {
				// only register range for cleanup if it has not been already cleaned up
				data->changeFeedCleanupDurable[changeFeedInfo->id] = cleanupVersion;
			}

			for (auto& it : data->changeFeedDestroys) {
				it.second.send(changeFeedInfo->id);
			}

			return invalidVersion;
		}

		// otherwise assume the feed just hasn't been created on the SS we tried to read it from yet, wait for it to
		// definitely be committed and retry
		seenNotRegistered = true;
		wait(data->desiredOldestVersion.whenAtLeast(endVersion));
	}
}

ACTOR Future<std::vector<Key>> fetchChangeFeedMetadata(StorageServer* data,
                                                       KeyRange keys,
                                                       PromiseStream<Key> destroyedFeeds,
                                                       UID fetchKeysID) {

	// Wait for current TLog batch to finish to ensure that we're fetching metadata at a version >= the version of
	// the ChangeServerKeys mutation. This guarantees we don't miss any metadata between the previous batch's
	// version (data->version) and the mutation version.
	wait(data->version.whenAtLeast(data->version.get() + 1));
	state Version fetchVersion = data->version.get();

	TraceEvent(SevDebug, "FetchChangeFeedMetadata", data->thisServerID)
	    .detail("Range", keys)
	    .detail("FetchVersion", fetchVersion)
	    .detail("FKID", fetchKeysID);

	state OverlappingChangeFeedsInfo feedMetadata = wait(data->cx->getOverlappingChangeFeeds(keys, fetchVersion));
	// rest of this actor needs to happen without waits that might yield to scheduler, to avoid races in feed
	// metadata.

	// Find set of feeds we currently have that were not present in fetch, to infer that they may have been
	// destroyed.
	state std::unordered_map<Key, Version> missingFeeds;
	auto ranges = data->keyChangeFeed.intersectingRanges(keys);
	for (auto& r : ranges) {
		for (auto& cfInfo : r.value()) {
			if (cfInfo->removing && !cfInfo->destroyed) {
				missingFeeds.insert({ cfInfo->id, cfInfo->metadataVersion });
			}
		}
	}

	// handle change feeds destroyed while fetching overlapping info
	while (destroyedFeeds.getFuture().isReady()) {
		Key destroyed = waitNext(destroyedFeeds.getFuture());
		for (int i = 0; i < feedMetadata.feeds.size(); i++) {
			if (feedMetadata.feeds[i].feedId == destroyed) {
				missingFeeds.erase(destroyed); // feed definitely destroyed, no need to infer
				swapAndPop(&feedMetadata.feeds, i--);
			}
		}
	}
	// FIXME: might want to inject delay here sometimes in simulation, so that races that would only happen when a
	// feed destroy causes a wait are more prominent?

	std::vector<Key> feedIds;
	feedIds.reserve(feedMetadata.feeds.size());
	// create change feed metadata if it does not exist
	for (auto& cfEntry : feedMetadata.feeds) {
		auto cleanupEntry = data->changeFeedCleanupDurable.find(cfEntry.feedId);
		bool cleanupPending = cleanupEntry != data->changeFeedCleanupDurable.end();
		auto existingEntry = data->uidChangeFeed.find(cfEntry.feedId);
		bool existing = existingEntry != data->uidChangeFeed.end();

		TraceEvent(SevDebug, "FetchedChangeFeedInfo", data->thisServerID)
		    .detail("FeedID", cfEntry.feedId)
		    .detail("Range", cfEntry.range)
		    .detail("FetchVersion", fetchVersion)
		    .detail("EmptyVersion", cfEntry.emptyVersion)
		    .detail("StopVersion", cfEntry.stopVersion)
		    .detail("FeedMetadataVersion", cfEntry.feedMetadataVersion)
		    .detail("Existing", existing)
		    .detail("ExistingMetadataVersion", existing ? existingEntry->second->metadataVersion : invalidVersion)
		    .detail("CleanupPendingVersion", cleanupPending ? cleanupEntry->second : invalidVersion)
		    .detail("FKID", fetchKeysID);

		bool addMutationToLog = false;
		Reference<ChangeFeedInfo> changeFeedInfo;

		if (!existing) {
			CODE_PROBE(cleanupPending,
			           "Fetch change feed which is cleanup pending. This means there was a move away and a move back, "
			           "this will remake the metadata",
			           probe::decoration::rare);

			changeFeedInfo = Reference<ChangeFeedInfo>(new ChangeFeedInfo());
			changeFeedInfo->range = cfEntry.range;
			changeFeedInfo->id = cfEntry.feedId;

			changeFeedInfo->emptyVersion = cfEntry.emptyVersion;
			changeFeedInfo->stopVersion = cfEntry.stopVersion;
			data->uidChangeFeed[cfEntry.feedId] = changeFeedInfo;
			auto rs = data->keyChangeFeed.modify(cfEntry.range);
			for (auto r = rs.begin(); r != rs.end(); ++r) {
				r->value().push_back(changeFeedInfo);
			}
			data->keyChangeFeed.coalesce(cfEntry.range);

			addMutationToLog = true;
		} else {
			changeFeedInfo = existingEntry->second;

			CODE_PROBE(cfEntry.feedMetadataVersion > data->version.get(),
			           "Change Feed fetched future metadata version");

			auto fid = missingFeeds.find(cfEntry.feedId);
			if (fid != missingFeeds.end()) {
				missingFeeds.erase(fid);
				ASSERT(!changeFeedInfo->destroyed);
				// could possibly be not removing because it was reset  while
				// waiting on destroyedFeeds by a private mutation or another fetch
				if (changeFeedInfo->removing) {
					TraceEvent(SevDebug, "ResetChangeFeedInfoFromFetch", data->thisServerID)
					    .detail("FeedID", changeFeedInfo->id.printable())
					    .detail("Range", changeFeedInfo->range)
					    .detail("FetchVersion", fetchVersion)
					    .detail("EmptyVersion", changeFeedInfo->emptyVersion)
					    .detail("StopVersion", changeFeedInfo->stopVersion)
					    .detail("PreviousMetadataVersion", changeFeedInfo->metadataVersion)
					    .detail("NewMetadataVersion", cfEntry.feedMetadataVersion)
					    .detail("FKID", fetchKeysID);

					CODE_PROBE(true, "re-fetching feed scheduled for deletion! Un-mark it as removing");

					// TODO only reset data if feed is still removing
					changeFeedInfo->removing = false;
					// reset fetch versions because everything previously fetched was cleaned up
					changeFeedInfo->fetchVersion = invalidVersion;
					changeFeedInfo->durableFetchVersion = NotifiedVersion();
					addMutationToLog = true;
				}
			}

			if (changeFeedInfo->destroyed) {
				CODE_PROBE(true,
				           "Change feed fetched and destroyed by other fetch while fetching metadata",
				           probe::decoration::rare);
				continue;
			}

			// we checked all feeds we already owned in this range at the start to reset them if they were removing,
			// and this actor would have been cancelled if a later remove happened
			ASSERT(!changeFeedInfo->removing);
			if (cfEntry.stopVersion < changeFeedInfo->stopVersion) {
				CODE_PROBE(true, "Change feed updated stop version from fetch metadata");
				changeFeedInfo->stopVersion = cfEntry.stopVersion;
				addMutationToLog = true;
			}

			// don't update empty version past SS version if SS is behind, it can cause issues
			if (cfEntry.emptyVersion < data->version.get() && cfEntry.emptyVersion > changeFeedInfo->emptyVersion) {
				CODE_PROBE(true, "Change feed updated empty version from fetch metadata");
				changeFeedInfo->emptyVersion = cfEntry.emptyVersion;
				addMutationToLog = true;
			}
		}
		feedIds.push_back(cfEntry.feedId);
		addMutationToLog |= changeFeedInfo->updateMetadataVersion(cfEntry.feedMetadataVersion);
		if (addMutationToLog) {
			ASSERT(changeFeedInfo.isValid());
			Version logV = data->data().getLatestVersion();
			auto& mLV = data->addVersionToMutationLog(logV);
			data->addMutationToMutationLog(
			    mLV,
			    MutationRef(MutationRef::SetValue,
			                persistChangeFeedKeys.begin.toString() + cfEntry.feedId.toString(),
			                changeFeedSSValue(cfEntry.range,
			                                  changeFeedInfo->emptyVersion + 1,
			                                  changeFeedInfo->stopVersion,
			                                  changeFeedInfo->metadataVersion)));
			// if we updated pop version, remove mutations
			while (!changeFeedInfo->mutations.empty() &&
			       changeFeedInfo->mutations.front().version <= changeFeedInfo->emptyVersion) {
				changeFeedInfo->mutations.pop_front();
			}
			if (BUGGIFY) {
				data->maybeInjectTargetedRestart(logV);
			}
		}
	}

	for (auto& feed : missingFeeds) {
		auto existingEntry = data->uidChangeFeed.find(feed.first);
		ASSERT(existingEntry != data->uidChangeFeed.end());
		ASSERT(existingEntry->second->removing);
		ASSERT(!existingEntry->second->destroyed);

		Version fetchedMetadataVersion = feedMetadata.getFeedMetadataVersion(existingEntry->second->range);
		Version lastMetadataVersion = feed.second;
		// Look for case where feed's range was moved away, feed was destroyed, and then feed's range was moved
		// back. This happens where feed is removing, the fetch metadata is higher than the moved away version, and
		// the feed isn't in the fetched response. In that case, the feed must have been destroyed between
		// lastMetadataVersion and fetchedMetadataVersion
		if (lastMetadataVersion >= fetchedMetadataVersion) {
			CODE_PROBE(true, "Change Feed fetched higher metadata version before moved away", probe::decoration::rare);
			continue;
		}

		Version cleanupVersion = data->data().getLatestVersion();

		CODE_PROBE(true, "Destroying change feed from fetch metadata"); //
		TraceEvent(SevDebug, "DestroyingChangeFeedFromFetchMetadata", data->thisServerID)
		    .detail("FeedID", feed.first)
		    .detail("Range", existingEntry->second->range)
		    .detail("Version", cleanupVersion)
		    .detail("FKID", fetchKeysID);

		if (g_network->isSimulated() && !g_simulator->restarted) {
			// verify that the feed was actually destroyed and it's not an error in this inference logic. Restarting
			// tests produce false positives because the validation state isn't kept across tests
			ASSERT(g_simulator->validationData.allDestroyedChangeFeedIDs.contains(feed.first.toString()));
		}

		Key beginClearKey = feed.first.withPrefix(persistChangeFeedKeys.begin);

		auto& mLV = data->addVersionToMutationLog(cleanupVersion);
		data->addMutationToMutationLog(mLV,
		                               MutationRef(MutationRef::ClearRange, beginClearKey, keyAfter(beginClearKey)));
		++data->counters.kvSystemClearRanges;
		data->addMutationToMutationLog(mLV,
		                               MutationRef(MutationRef::ClearRange,
		                                           changeFeedDurableKey(feed.first, 0),
		                                           changeFeedDurableKey(feed.first, cleanupVersion)));
		++data->counters.kvSystemClearRanges;

		existingEntry->second->destroy(cleanupVersion);
		data->changeFeedCleanupDurable[feed.first] = cleanupVersion;

		for (auto& it : data->changeFeedDestroys) {
			it.second.send(feed.first);
		}
		if (BUGGIFY) {
			data->maybeInjectTargetedRestart(cleanupVersion);
		}
	}
	return feedIds;
}

ReadOptions readOptionsForFeedFetch(const ReadOptions& options, const KeyRangeRef& keys, const KeyRangeRef& feedRange) {
	if (!feedRange.contains(keys)) {
		return options;
	}
	// If feed range wholly contains shard range, cache on fetch because other shards will likely also fetch it
	ReadOptions newOptions = options;
	newOptions.cacheResult = true;
	return newOptions;
}

// returns max version fetched for each feed
// newFeedIds is used for the second fetch to get data for new feeds that weren't there for the first fetch
ACTOR Future<std::unordered_map<Key, Version>> dispatchChangeFeeds(StorageServer* data,
                                                                   UID fetchKeysID,
                                                                   KeyRange keys,
                                                                   Version beginVersion,
                                                                   Version endVersion,
                                                                   PromiseStream<Key> destroyedFeeds,
                                                                   std::vector<Key>* feedIds,
                                                                   std::unordered_set<Key> newFeedIds,
                                                                   ReadOptions readOptions) {
	state std::unordered_map<Key, Version> feedMaxFetched;
	if (feedIds->empty() && newFeedIds.empty()) {
		return feedMaxFetched;
	}

	wait(data->fetchKeysParallelismChangeFeedLock.take(TaskPriority::DefaultYield));
	state FlowLock::Releaser holdingFKPL(data->fetchKeysParallelismChangeFeedLock);

	// find overlapping range feeds
	state std::map<Key, Future<Version>> feedFetches;

	try {
		for (auto& feedId : *feedIds) {
			auto feedIt = data->uidChangeFeed.find(feedId);
			// feed may have been moved away or deleted after move was scheduled, do nothing in that case
			if (feedIt != data->uidChangeFeed.end() && !feedIt->second->removing) {
				ReadOptions fetchReadOptions = readOptionsForFeedFetch(readOptions, keys, feedIt->second->range);
				feedFetches[feedIt->second->id] =
				    fetchChangeFeed(data, feedIt->second, beginVersion, endVersion, fetchReadOptions);
			}
		}
		for (auto& feedId : newFeedIds) {
			auto feedIt = data->uidChangeFeed.find(feedId);
			// feed may have been moved away or deleted while we took the feed lock, do nothing in that case
			if (feedIt != data->uidChangeFeed.end() && !feedIt->second->removing) {
				ReadOptions fetchReadOptions = readOptionsForFeedFetch(readOptions, keys, feedIt->second->range);
				feedFetches[feedIt->second->id] =
				    fetchChangeFeed(data, feedIt->second, 0, endVersion, fetchReadOptions);
			}
		}

		loop {
			Future<Version> nextFeed = Never();
			if (!destroyedFeeds.getFuture().isReady()) {
				bool done = true;
				while (!feedFetches.empty()) {
					if (feedFetches.begin()->second.isReady()) {
						Version maxFetched = feedFetches.begin()->second.get();
						if (maxFetched != invalidVersion) {
							feedFetches[feedFetches.begin()->first] = maxFetched;
						}
						feedFetches.erase(feedFetches.begin());
					} else {
						nextFeed = feedFetches.begin()->second;
						done = false;
						break;
					}
				}
				if (done) {
					return feedMaxFetched;
				}
			}
			choose {
				when(state Key destroyed = waitNext(destroyedFeeds.getFuture())) {
					wait(delay(0));
					feedFetches.erase(destroyed);
					for (int i = 0; i < feedIds->size(); i++) {
						if ((*feedIds)[i] == destroyed) {
							swapAndPop(feedIds, i--);
						}
					}
				}
				when(wait(success(nextFeed))) {}
			}
		}

	} catch (Error& e) {
		if (!data->shuttingDown) {
			data->changeFeedDestroys.erase(fetchKeysID);
		}
		throw;
	}
}

bool fetchKeyCanRetry(const Error& e) {
	switch (e.code()) {
	case error_code_end_of_stream:
	case error_code_connection_failed:
	case error_code_transaction_too_old:
	case error_code_future_version:
	case error_code_process_behind:
	case error_code_server_overloaded:
	case error_code_blob_granule_request_failed:
	case error_code_blob_granule_transaction_too_old:
	case error_code_grv_proxy_memory_limit_exceeded:
	case error_code_commit_proxy_memory_limit_exceeded:
	case error_code_storage_replica_comparison_error:
	case error_code_unreachable_storage_replica:
	case error_code_bulkload_task_failed: // for fetchKey based bulkload
		return true;
	default:
		return false;
	}
}

ACTOR Future<BulkLoadFileSet> bulkLoadFetchKeyValueFileToLoad(StorageServer* data,
                                                              std::string dir,
                                                              BulkLoadTaskState bulkLoadTaskState) {
	ASSERT(bulkLoadTaskState.getLoadType() == BulkLoadType::SST);
	TraceEvent(SevInfo, "SSBulkLoadTaskFetchSSTFile", data->thisServerID)
	    .setMaxEventLength(-1)
	    .setMaxFieldLength(-1)
	    .detail("BulkLoadTask", bulkLoadTaskState.toString())
	    .detail("Dir", abspath(dir));
	state double fetchStartTime = now();
	// Download data file from fromRemoteFileSet to toLocalFileSet
	state BulkLoadFileSet fromRemoteFileSet = bulkLoadTaskState.getFileSet();
	state BulkLoadFileSet toLocalFileSet = wait(bulkLoadDownloadTaskFileSet(
	    bulkLoadTaskState.getTransportMethod(), fromRemoteFileSet, dir, data->thisServerID));
	// Do not need byte sampling locally in fetchKeys
	const double duration = now() - fetchStartTime;
	const int64_t totalBytes = bulkLoadTaskState.getTotalBytes();
	TraceEvent(SevInfo, "SSBulkLoadTaskFetchSSTFileFetched", data->thisServerID)
	    .setMaxEventLength(-1)
	    .setMaxFieldLength(-1)
	    .detail("BulkLoadTask", bulkLoadTaskState.toString())
	    .detail("Dir", abspath(dir))
	    .detail("LocalFileSet", toLocalFileSet.toString())
	    .detail("Duration", duration)
	    .detail("TotalBytes", totalBytes)
	    .detail("Rate", duration == 0 ? -1.0 : (double)totalBytes / duration);
	return toLocalFileSet;
}

ACTOR Future<Void> tryGetRangeForBulkLoad(PromiseStream<RangeResult> results, KeyRange keys, std::string dataPath) {
	try {
		// TODO(BulkLoad): what if the data file is empty but the totalKeyCount is not zero
		state Key beginKey = keys.begin;
		state Key endKey = keys.end;
		state std::unique_ptr<IRocksDBSstFileReader> reader = newRocksDBSstFileReader(
		    keys, SERVER_KNOBS->SS_BULKLOAD_GETRANGE_BATCH_SIZE, SERVER_KNOBS->FETCH_BLOCK_BYTES);
		// TODO(BulkLoad): this can be a slow task. We will make this as async call.
		reader->open(abspath(dataPath));
		loop {
			// TODO(BulkLoad): this is a blocking call. We will make this as async call.
			RangeResult rep = reader->getRange(KeyRangeRef(beginKey, endKey));
			results.send(rep);
			if (!rep.more) {
				results.sendError(end_of_stream());
				return Void();
			}
			beginKey = keyAfter(rep.back().key);
			wait(delay(0.1)); // context switch to avoid busy loop
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		results.sendError(bulkload_task_failed());
		throw;
	}
}

ACTOR Future<Void> fetchKeys(StorageServer* data, AddingShard* shard) {
	state const UID fetchKeysID = deterministicRandom()->randomUniqueID();
	state TraceInterval interval("FetchKeys");
	state KeyRange keys = shard->keys;
	state Future<Void> warningLogger = logFetchKeysWarning(shard);
	state const double startTime = now();
	state Version fetchVersion = invalidVersion;
	state int64_t totalBytes = 0;
	state int priority = dataMovementPriority(shard->reason);
	state UID dataMoveId = shard->getSSBulkLoadMetadata().getDataMoveId();
	state ConductBulkLoad conductBulkLoad = ConductBulkLoad(shard->getSSBulkLoadMetadata().getConductBulkLoad());
	state std::string bulkLoadLocalDir =
	    joinPath(joinPath(data->bulkLoadFolder, dataMoveId.toString()), fetchKeysID.toString());
	// Since the fetchKey can split, so multiple fetchzkeys can have the same data move id. We want each fetchkey
	// downloads its file without conflict, so we add fetchKeysID to the bulkLoadLocalDir.
	state PromiseStream<Key> destroyedFeeds;
	state FetchKeysMetricReporter metricReporter(fetchKeysID,
	                                             startTime,
	                                             keys,
	                                             data->fetchKeysHistograms,
	                                             data->currentRunningFetchKeys,
	                                             data->counters.bytesFetched,
	                                             data->counters.kvFetched);

	// Set read options to use non-caching reads and set Fetch type unless low priority data fetching is disabled by
	// a knob
	state ReadOptions readOptions = ReadOptions(
	    {}, SERVER_KNOBS->FETCH_KEYS_LOWER_PRIORITY ? ReadType::FETCH : ReadType::NORMAL, CacheResult::False);

	if (conductBulkLoad) {
		TraceEvent(SevInfo, "SSBulkLoadTaskFetchKey", data->thisServerID)
		    .detail("DataMoveId", dataMoveId.toString())
		    .detail("Range", keys)
		    .detail("Phase", "Begin");
	}

	// need to set this at the very start of the fetch, to handle any private change feed destroy mutations we get
	// for this key range, that apply to change feeds we don't know about yet because their metadata hasn't been
	// fetched yet
	data->changeFeedDestroys[fetchKeysID] = destroyedFeeds;

	// delay(0) to force a return to the run loop before the work of fetchKeys is started.
	//  This allows adding->start() to be called inline with CSK.
	try {
		wait(data->coreStarted.getFuture() && delay(0));

		// On SS Reboot, durableVersion == latestVersion, so any mutations we add to the mutation log would be
		// skipped if added before latest version advances. To ensure this doesn't happen, we wait for version to
		// increase by one if this fetchKeys was initiated by a changeServerKeys from restoreDurableState
		if (data->version.get() == data->durableVersion.get()) {
			wait(data->version.whenAtLeast(data->version.get() + 1));
			wait(delay(0));
		}
	} catch (Error& e) {
		if (!data->shuttingDown) {
			data->changeFeedDestroys.erase(fetchKeysID);
		}
		throw e;
	}

	try {
		DEBUG_KEY_RANGE("fetchKeysBegin", data->version.get(), shard->keys, data->thisServerID);

		TraceEvent(SevDebug, interval.begin(), data->thisServerID)
		    .detail("KeyBegin", shard->keys.begin)
		    .detail("KeyEnd", shard->keys.end)
		    .detail("Version", data->version.get())
		    .detail("FKID", fetchKeysID)
		    .detail("DataMoveId", dataMoveId)
		    .detail("ConductBulkLoad", conductBulkLoad);

		state Future<std::vector<Key>> fetchCFMetadata =
		    fetchChangeFeedMetadata(data, keys, destroyedFeeds, fetchKeysID);

		validate(data);

		// Wait (if necessary) for the latest version at which any key in keys was previously available (+1) to be
		// durable
		auto navr = data->newestAvailableVersion.intersectingRanges(keys);
		Version lastAvailable = invalidVersion;
		for (auto r = navr.begin(); r != navr.end(); ++r) {
			ASSERT(r->value() != latestVersion);
			lastAvailable = std::max(lastAvailable, r->value());
		}
		auto ndvr = data->newestDirtyVersion.intersectingRanges(keys);
		for (auto r = ndvr.begin(); r != ndvr.end(); ++r)
			lastAvailable = std::max(lastAvailable, r->value());

		if (lastAvailable != invalidVersion && lastAvailable >= data->durableVersion.get()) {
			CODE_PROBE(true, "FetchKeys waits for previous available version to be durable");
			wait(data->durableVersion.whenAtLeast(lastAvailable + 1));
		}

		TraceEvent(SevDebug, "FetchKeysVersionSatisfied", data->thisServerID)
		    .detail("FKID", interval.pairID)
		    .detail("DataMoveId", dataMoveId)
		    .detail("ConductBulkLoad", conductBulkLoad);

		wait(data->fetchKeysParallelismLock.take(TaskPriority::DefaultYield));
		state FlowLock::Releaser holdingFKPL(data->fetchKeysParallelismLock);

		state double executeStart = now();
		++data->counters.fetchWaitingCount;
		data->counters.fetchWaitingMS += 1000 * (executeStart - startTime);

		// Fetch keys gets called while the update actor is processing mutations. data->version will not be updated
		// until all mutations for a version have been processed. We need to take the durableVersionLock to ensure
		// data->version is greater than the version of the mutation which caused the fetch to be initiated.

		// We must also ensure we have fetched all change feed metadata BEFORE changing the phase to fetching to
		// ensure change feed mutations get applied correctly
		state std::vector<Key> changeFeedsToFetch;
		state Reference<BlobRestoreController> restoreController = makeReference<BlobRestoreController>(data->cx, keys);
		state bool isFullRestore = wait(BlobRestoreController::isRestoring(restoreController));
		if (!isFullRestore) {
			std::vector<Key> _cfToFetch = wait(fetchCFMetadata);
			changeFeedsToFetch = _cfToFetch;
		}
		wait(data->durableVersionLock.take());

		shard->phase = AddingShard::Fetching;

		data->durableVersionLock.release();

		wait(delay(0));

		// Get the history
		state int debug_getRangeRetries = 0;
		state int debug_nextRetryToLog = 1;
		state Error lastError;

		// FIXME: The client cache does not notice when servers are added to a team. To read from a local storage
		// server we must refresh the cache manually.
		data->cx->invalidateCache(Key(), keys);

		loop {
			state Transaction tr(data->cx);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			if (!isFullRestore && SERVER_KNOBS->ENABLE_REPLICA_CONSISTENCY_CHECK_ON_DATA_MOVEMENT) {
				tr.setOption(FDBTransactionOptions::ENABLE_REPLICA_CONSISTENCY_CHECK);
				int64_t requiredReplicas = SERVER_KNOBS->CONSISTENCY_CHECK_REQUIRED_REPLICAS;
				tr.setOption(FDBTransactionOptions::CONSISTENCY_CHECK_REQUIRED_REPLICAS,
				             StringRef((uint8_t*)&requiredReplicas, sizeof(int64_t)));
			}
			tr.trState->readOptions = readOptions;
			tr.trState->taskID = TaskPriority::FetchKeys;

			// fetchVersion = data->version.get();
			// A quick fix:
			// By default, we use data->version as the fetchVersion.
			// In the case where dest SS falls far behind src SS, we use GRV as the fetchVersion instead of
			// data->version, and then the dest SS waits for catching up the fetchVersion outside the
			// fetchKeysParallelismLock.
			// For example, consider dest SS falls far behind src SS.
			// At iteration 0, dest SS selects its version as fetchVersion,
			// but cannot read src SS and result in error_code_transaction_too_old.
			// Due to error_code_transaction_too_old, dest SS starts iteration 1.
			// At iteration 1, dest SS selects GRV as fetchVersion and (suppose) can read the data from src SS.
			// Then dest SS waits its version catch up with this GRV version and write the data to disk.
			// Note that dest SS waits outside the fetchKeysParallelismLock.
			fetchVersion = std::max(shard->fetchVersion, data->version.get());
			if (g_network->isSimulated() && BUGGIFY_WITH_PROB(0.01)) {
				// Test using GRV version for fetchKey.
				lastError = transaction_too_old();
			}
			if (lastError.code() == error_code_transaction_too_old) {
				try {
					Version grvVersion = wait(tr.getRawReadVersion());
					if (g_network->isSimulated() && BUGGIFY_WITH_PROB(0.01)) {
						// Test failed GRV request.
						throw grv_proxy_memory_limit_exceeded();
					}
					fetchVersion = std::max(grvVersion, fetchVersion);
				} catch (Error& e) {
					if (e.code() == error_code_actor_cancelled) {
						throw e;
					}

					// Note that error in getting GRV doesn't affect any storage server state. Therefore, we catch
					// all errors here without failing the storage server. When error happens, fetchVersion fall
					// back to the above computed fetchVersion.
					TraceEvent(SevWarn, "FetchKeyGRVError", data->thisServerID).error(e);
					lastError = e;
				}
			}
			ASSERT(fetchVersion >= shard->fetchVersion); // at this point, shard->fetchVersion is the last fetchVersion
			shard->fetchVersion = fetchVersion;
			TraceEvent(SevVerbose, "FetchKeysUnblocked", data->thisServerID)
			    .detail("FKID", interval.pairID)
			    .detail("Version", fetchVersion);

			while (!shard->updates.empty() && shard->updates[0].version <= fetchVersion)
				shard->updates.pop_front();
			tr.setVersion(fetchVersion);

			state PromiseStream<RangeResult> results;
			state Future<Void> hold;
			state KeyRef rangeEnd;
			if (isFullRestore) {
				state BlobRestorePhase phase = wait(BlobRestoreController::currentPhase(restoreController));
				// Read from blob only when it's copying data for full restore. Otherwise it may cause data
				// corruptions e.g we don't want to copy from blob any more when it's applying mutation
				// logs(APPLYING_MLOGS)
				if (phase == BlobRestorePhase::COPYING_DATA || phase == BlobRestorePhase::ERROR) {
					wait(loadBGTenantMap(&data->tenantData, &tr));
					// only copy the range that intersects with full restore range
					state KeyRangeRef range(std::max(keys.begin, normalKeys.begin), std::min(keys.end, normalKeys.end));
					Version version = wait(BlobRestoreController::getTargetVersion(restoreController, fetchVersion));
					hold = tryGetRangeFromBlob(results, &tr, data->cx, range, version, &data->tenantData);
					rangeEnd = range.end;
				} else {
					hold = tryGetRange(results, &tr, keys);
					rangeEnd = keys.end;
				}
			} else if (conductBulkLoad) {
				TraceEvent(SevInfo, "SSBulkLoadTaskFetchKey", data->thisServerID)
				    .detail("DataMoveId", dataMoveId.toString())
				    .detail("Range", keys)
				    .detail("Phase", "Read task metadata");
				ASSERT(dataMoveIdIsValidForBulkLoad(dataMoveId)); // TODO(BulkLoad): remove dangerous assert
				// Get the bulkload task metadata from the data move metadata. Note that a SS can receive a data move
				// mutation before the bulkload task metadata is persisted. In this case, the SS will not be able to
				// read the bulkload task. SS will wait at this point until the bulkload task metadata is persisted.
				// Moreover, the bulkload task metadata is persist at a verison at least the version when this SS
				// receives the datamove mutation. Therefore, the SS should read the bulkload task metadata at a version
				// at least this SS version.
				// Note that it is possible that this SS can never get the bulkload metadata because the bulkload data
				// move is cancelled or replaced by another data move. In this case, while
				// getBulkLoadTaskStateFromDataMove get stuck, this fetchKeys is guaranteed to be cancelled.
				BulkLoadTaskState bulkLoadTaskState = wait(getBulkLoadTaskStateFromDataMove(
				    data->cx, dataMoveId, /*atLeastVersion=*/data->version.get(), data->thisServerID));
				TraceEvent(SevInfo, "SSBulkLoadTaskFetchKey", data->thisServerID)
				    .detail("DataMoveId", dataMoveId.toString())
				    .detail("Range", keys)
				    .detail("Phase", "Got task metadata");
				// Check the correctness: bulkLoadTaskMetadata stored in dataMoveMetadata must have the same
				// dataMoveId.
				ASSERT(bulkLoadTaskState.getDataMoveId() == dataMoveId);
				// We download the data file to local disk and pass the data file path to read in the next step.
				BulkLoadFileSet localFileSet =
				    wait(bulkLoadFetchKeyValueFileToLoad(data, bulkLoadLocalDir, bulkLoadTaskState));
				hold = tryGetRangeForBulkLoad(results, keys, localFileSet.getDataFileFullPath());
				rangeEnd = keys.end;
			} else {
				hold = tryGetRange(results, &tr, keys);
				rangeEnd = keys.end;
			}

			state Key blockBegin = keys.begin;

			try {
				loop {
					CODE_PROBE(true, "Fetching keys for transferred shard");
					while (data->fetchKeysBudgetUsed.get()) {
						std::vector<Future<Void>> delays;
						if (SERVER_KNOBS->STORAGE_FETCH_KEYS_DELAY > 0) {
							delays.push_back(delayJittered(SERVER_KNOBS->STORAGE_FETCH_KEYS_DELAY));
						}
						delays.push_back(data->fetchKeysBudgetUsed.onChange());
						wait(waitForAll(delays));
					}
					state RangeResult this_block = waitNext(results.getFuture());

					state int expectedBlockSize =
					    (int)this_block.expectedSize() + (8 - (int)sizeof(KeyValueRef)) * this_block.size();

					TraceEvent(SevDebug, "FetchKeysBlock", data->thisServerID)
					    .detail("FKID", interval.pairID)
					    .detail("BlockRows", this_block.size())
					    .detail("BlockBytes", expectedBlockSize)
					    .detail("KeyBegin", keys.begin)
					    .detail("KeyEnd", keys.end)
					    .detail("Last", this_block.size() ? this_block.end()[-1].key : std::string())
					    .detail("Version", fetchVersion)
					    .detail("More", this_block.more)
					    .detail("DataMoveId", dataMoveId.toString())
					    .detail("ConductBulkLoad", conductBulkLoad);

					DEBUG_KEY_RANGE("fetchRange", fetchVersion, keys, data->thisServerID);
					if (MUTATION_TRACKING_ENABLED) {
						for (auto k = this_block.begin(); k != this_block.end(); ++k) {
							DEBUG_MUTATION("fetch",
							               fetchVersion,
							               MutationRef(MutationRef::SetValue, k->key, k->value),
							               data->thisServerID);
						}
					}
					metricReporter.addFetchedBytes(expectedBlockSize, this_block.size());
					totalBytes += expectedBlockSize;

					if (shard->reason != DataMovementReason::INVALID &&
					    priority < SERVER_KNOBS->FETCH_KEYS_THROTTLE_PRIORITY_THRESHOLD &&
					    !data->fetchKeysLimiter.ready().isReady()) {
						TraceEvent(SevDebug, "FetchKeysThrottling", data->thisServerID);
						state double ts = now();
						wait(data->fetchKeysLimiter.ready());
						TraceEvent(SevDebug, "FetchKeysThrottled", data->thisServerID)
						    .detail("Priority", priority)
						    .detail("KeyRange", shard->keys)
						    .detail("Delay", now() - ts);
					}

					// Write this_block to storage
					state Standalone<VectorRef<KeyValueRef>> blockData(this_block, this_block.arena());
					state Key blockEnd =
					    this_block.size() > 0 && this_block.more ? keyAfter(this_block.back().key) : keys.end;
					state KeyRange blockRange(KeyRangeRef(blockBegin, blockEnd));
					wait(data->storage.replaceRange(blockRange, blockData));

					if (conductBulkLoad) {
						TraceEvent(SevInfo, "SSBulkLoadTaskFetchKey", data->thisServerID)
						    .detail("DataMoveId", dataMoveId.toString())
						    .detail("Range", keys)
						    .detail("BlockRange", blockRange)
						    .detail("Phase", "Replaced range");
					}

					data->fetchKeysLimiter.addBytes(expectedBlockSize);

					state KeyValueRef* kvItr = this_block.begin();
					for (; kvItr != this_block.end(); ++kvItr) {
						data->byteSampleApplySet(*kvItr, invalidVersion);
					}
					if (this_block.more) {
						blockBegin = this_block.getReadThrough();
					} else {
						ASSERT(!this_block.readThrough.present());
						blockBegin = rangeEnd;
					}
					this_block = RangeResult();

					data->fetchKeysTotalCommitBytes += expectedBlockSize;
					data->fetchKeysBytesBudget -= expectedBlockSize;
					data->fetchKeysBudgetUsed.set(data->fetchKeysBytesBudget <= 0);
				}
			} catch (Error& e) {
				if (!fetchKeyCanRetry(e)) {
					throw e;
				}
				lastError = e;
				if (lastError.code() == error_code_storage_replica_comparison_error) {
					// The inconsistency could be because of the inclusion of a rolled back
					// transaction(s)/version(s) in the returned results. Retry.
					wait(data->knownCommittedVersion.whenAtLeast(fetchVersion));
				}
				if (blockBegin == keys.begin) {
					TraceEvent("FKBlockFail", data->thisServerID)
					    .errorUnsuppressed(lastError)
					    .suppressFor(1.0)
					    .detail("FKID", interval.pairID);

					debug_getRangeRetries++;
					if (debug_nextRetryToLog == debug_getRangeRetries) {
						debug_nextRetryToLog += std::min(debug_nextRetryToLog, 1024);
						TraceEvent(SevWarn, "FetchPast", data->thisServerID)
						    .detail("TotalAttempts", debug_getRangeRetries)
						    .detail("FKID", interval.pairID)
						    .detail("N", fetchVersion)
						    .detail("E", data->version.get());
					}
					wait(delayJittered(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
					continue;
				}
				if (blockBegin < keys.end) {
					std::deque<Standalone<VerUpdateRef>> updatesToSplit = std::move(shard->updates);

					// This actor finishes committing the keys [keys.begin,nfk) that we already fetched.
					// The remaining unfetched keys [nfk,keys.end) will become a separate AddingShard with its own
					// fetchKeys.
					if (data->shardAware) {
						StorageServerShard rightShard = data->shards[keys.begin]->toStorageServerShard();
						rightShard.range = KeyRangeRef(blockBegin, keys.end);
						auto* leftShard = ShardInfo::addingSplitLeft(KeyRangeRef(keys.begin, blockBegin), shard);
						leftShard->populateShard(rightShard);
						shard->server->addShard(leftShard);
						shard->server->addShard(ShardInfo::newShard(data, rightShard));
					} else {
						shard->server->addShard(ShardInfo::addingSplitLeft(KeyRangeRef(keys.begin, blockBegin), shard));
						shard->server->addShard(ShardInfo::newAdding(
						    data, KeyRangeRef(blockBegin, keys.end), shard->reason, shard->getSSBulkLoadMetadata()));
						if (conductBulkLoad) {
							TraceEvent(SevInfo, "SSBulkLoadTaskFetchKey", data->thisServerID)
							    .detail("DataMoveId", dataMoveId.toString())
							    .detail("Range", keys)
							    .detail("NewSplitBeginKey", blockBegin)
							    .detail("Phase", "Split range");
						}
					}
					shard = data->shards.rangeContaining(keys.begin).value()->adding.get();
					warningLogger = logFetchKeysWarning(shard);
					AddingShard* otherShard = data->shards.rangeContaining(blockBegin).value()->adding.get();
					keys = shard->keys;

					// Split our prior updates.  The ones that apply to our new, restricted key range will go back
					// into shard->updates, and the ones delivered to the new shard will be discarded because it is
					// in WaitPrevious phase (hasn't chosen a fetchVersion yet). What we are doing here is expensive
					// and could get more expensive if we started having many more blocks per shard. May need
					// optimization in the future.
					std::deque<Standalone<VerUpdateRef>>::iterator u = updatesToSplit.begin();
					for (; u != updatesToSplit.end(); ++u) {
						splitMutations(data, data->shards, *u);
					}

					CODE_PROBE(true, "fetchkeys has more");
					CODE_PROBE(shard->updates.size(), "Shard has updates");
					ASSERT(otherShard->updates.empty());
				}
				break;
			}
		}

		// We have completed the fetch and write of the data, now we wait for MVCC window to pass.
		//  As we have finished this work, we will allow more work to start...
		shard->fetchComplete.send(Void());
		if (SERVER_KNOBS->SHARDED_ROCKSDB_DELAY_COMPACTION_FOR_DATA_MOVE) {
			data->storage.markRangeAsActive(keys);
		}
		const double duration = now() - startTime;
		TraceEvent(SevInfo, "FetchKeysStats", data->thisServerID)
		    .detail("TotalBytes", totalBytes)
		    .detail("Duration", duration)
		    .detail("Rate", static_cast<double>(totalBytes) / duration);

		TraceEvent(SevDebug, "FKBeforeFinalCommit", data->thisServerID)
		    .detail("FKID", interval.pairID)
		    .detail("SV", data->storageVersion())
		    .detail("DV", data->durableVersion.get());
		// Directly commit()ing the IKVS would interfere with updateStorage, possibly resulting in an incomplete
		// version being recovered. Instead we wait for the updateStorage loop to commit something (and consequently
		// also what we have written)

		state Future<std::unordered_map<Key, Version>> feedFetchMain = dispatchChangeFeeds(data,
		                                                                                   fetchKeysID,
		                                                                                   keys,
		                                                                                   0,
		                                                                                   fetchVersion + 1,
		                                                                                   destroyedFeeds,
		                                                                                   &changeFeedsToFetch,
		                                                                                   std::unordered_set<Key>(),
		                                                                                   readOptions);

		state Future<Void> fetchDurable = data->durableVersion.whenAtLeast(data->storageVersion() + 1);
		state Future<Void> dataArrive = data->version.whenAtLeast(fetchVersion);

		holdingFKPL.release();
		wait(dataArrive && fetchDurable);

		state std::unordered_map<Key, Version> feedFetchedVersions = wait(feedFetchMain);

		TraceEvent(SevDebug, "FKAfterFinalCommit", data->thisServerID)
		    .detail("FKID", interval.pairID)
		    .detail("SV", data->storageVersion())
		    .detail("DV", data->durableVersion.get());

		// Wait to run during update(), after a new batch of versions is received from the tlog but before eager
		// reads take place.
		Promise<FetchInjectionInfo*> p;
		data->readyFetchKeys.push_back(p);

		// After we add to the promise readyFetchKeys, update() would provide a pointer to FetchInjectionInfo that
		// we can put mutation in.
		FetchInjectionInfo* batch = wait(p.getFuture());
		TraceEvent(SevDebug, "FKUpdateBatch", data->thisServerID).detail("FKID", interval.pairID);

		shard->phase = AddingShard::FetchingCF;
		ASSERT(data->version.get() >= fetchVersion);
		// Choose a transferredVersion.  This choice and timing ensure that
		//   * The transferredVersion can be mutated in versionedData
		//   * The transferredVersion isn't yet committed to storage (so we can write the availability status
		//   change)
		//   * The transferredVersion is <= the version of any of the updates in batch, and if there is an equal
		//   version
		//     its mutations haven't been processed yet
		shard->transferredVersion = data->version.get() + 1;
		// shard->transferredVersion = batch->changes[0].version;  //< FIXME: This obeys the documented properties,
		// and seems "safer" because it never introduces extra versions into the data structure, but violates some
		// ASSERTs currently
		data->mutableData().createNewVersion(shard->transferredVersion);
		ASSERT(shard->transferredVersion > data->storageVersion());
		ASSERT(shard->transferredVersion == data->data().getLatestVersion());

		// find new change feeds for this range that didn't exist when we started the fetch
		auto ranges = data->keyChangeFeed.intersectingRanges(keys);
		std::unordered_set<Key> newChangeFeeds;
		for (auto& r : ranges) {
			for (auto& cfInfo : r.value()) {
				CODE_PROBE(true, "SS fetching new change feed that didn't exist when fetch started");
				if (!cfInfo->removing) {
					newChangeFeeds.insert(cfInfo->id);
				}
			}
		}
		for (auto& cfId : changeFeedsToFetch) {
			newChangeFeeds.erase(cfId);
		}
		// This is split into two fetches to reduce tail. Fetch [0 - fetchVersion+1)
		// once fetchVersion is finalized, and [fetchVersion+1, transferredVersion) here once transferredVersion is
		// finalized. Also fetch new change feeds alongside it
		state Future<std::unordered_map<Key, Version>> feedFetchTransferred =
		    dispatchChangeFeeds(data,
		                        fetchKeysID,
		                        keys,
		                        fetchVersion + 1,
		                        shard->transferredVersion,
		                        destroyedFeeds,
		                        &changeFeedsToFetch,
		                        newChangeFeeds,
		                        readOptions);

		TraceEvent(SevDebug, "FetchKeysHaveData", data->thisServerID)
		    .detail("FKID", interval.pairID)
		    .detail("Version", shard->transferredVersion)
		    .detail("StorageVersion", data->storageVersion());
		validate(data);

		// the minimal version in updates must be larger than fetchVersion
		ASSERT(shard->updates.empty() || shard->updates[0].version > fetchVersion);

		// Put the updates that were collected during the FinalCommit phase into the batch at the
		// transferredVersion. Eager reads will be done for them by update(), and the mutations will come back
		// through AddingShard::addMutations and be applied to versionedMap and mutationLog as normal. The lie about
		// their version is acceptable because this shard will never be read at versions < transferredVersion

		for (auto i = shard->updates.begin(); i != shard->updates.end(); ++i) {
			i->version = shard->transferredVersion;
			batch->arena.dependsOn(i->arena());
		}

		int startSize = batch->changes.size();
		CODE_PROBE(startSize, "Adding fetch data to a batch which already has changes");
		batch->changes.resize(batch->changes.size() + shard->updates.size());

		// FIXME: pass the deque back rather than copy the data
		std::copy(shard->updates.begin(), shard->updates.end(), batch->changes.begin() + startSize);
		Version checkv = shard->transferredVersion;

		for (auto b = batch->changes.begin() + startSize; b != batch->changes.end(); ++b) {
			ASSERT(b->version >= checkv);
			checkv = b->version;
			if (MUTATION_TRACKING_ENABLED) {
				for (auto& m : b->mutations) {
					DEBUG_MUTATION("fetchKeysFinalCommitInject", batch->changes[0].version, m, data->thisServerID);
				}
			}
		}

		shard->updates.clear();

		// wait on change feed fetch to complete writing to storage before marking data as available
		std::unordered_map<Key, Version> feedFetchedVersions2 = wait(feedFetchTransferred);
		for (auto& newFetch : feedFetchedVersions2) {
			auto prevFetch = feedFetchedVersions.find(newFetch.first);
			if (prevFetch != feedFetchedVersions.end()) {
				prevFetch->second = std::max(prevFetch->second, newFetch.second);
			} else {
				feedFetchedVersions[newFetch.first] = newFetch.second;
			}
		}

		data->changeFeedDestroys.erase(fetchKeysID);

		shard->phase = AddingShard::Waiting;

		// Similar to transferred version, but wait for all feed data and
		Version feedTransferredVersion = data->version.get() + 1;

		TraceEvent(SevDebug, "FetchKeysHaveFeedData", data->thisServerID)
		    .detail("FKID", interval.pairID)
		    .detail("Version", feedTransferredVersion)
		    .detail("StorageVersion", data->storageVersion());

		state StorageServerShard newShard;
		if (data->shardAware) {
			newShard = data->shards[keys.begin]->toStorageServerShard();
			ASSERT(newShard.range == keys);
			ASSERT(newShard.getShardState() == StorageServerShard::ReadWritePending);
			newShard.setShardState(StorageServerShard::ReadWrite);
			updateStorageShard(data, newShard);
		}
		setAvailableStatus(data,
		                   keys,
		                   true); // keys will be available when getLatestVersion()==transferredVersion is durable

		// Note that since it receives a pointer to FetchInjectionInfo, the thread does not leave this actor until
		// this point.

		// Wait for the transferred version (and therefore the shard data) to be committed and durable.
		wait(data->durableVersion.whenAtLeast(feedTransferredVersion));

		ASSERT(data->shards[shard->keys.begin]->assigned() &&
		       data->shards[shard->keys.begin]->keys ==
		           shard->keys); // We aren't changing whether the shard is assigned
		data->newestAvailableVersion.insert(shard->keys, latestVersion);
		shard->readWrite.send(Void());
		if (data->shardAware) {
			data->addShard(ShardInfo::newShard(data, newShard)); // invalidates shard!
			coalescePhysicalShards(data, keys);
		} else {
			data->addShard(ShardInfo::newReadWrite(shard->keys, data)); // invalidates shard!
			coalesceShards(data, keys);
		}

		validate(data);

		++data->counters.fetchExecutingCount;
		data->counters.fetchExecutingMS += 1000 * (now() - executeStart);

		TraceEvent(SevDebug, interval.end(), data->thisServerID);
		if (conductBulkLoad) {
			// Do best effort cleanup
			clearFileFolder(bulkLoadLocalDir, data->thisServerID, /*ignoreError=*/true);
		}

	} catch (Error& e) {
		TraceEvent(SevDebug, interval.end(), data->thisServerID)
		    .errorUnsuppressed(e)
		    .detail("Version", data->version.get());
		if (!data->shuttingDown) {
			data->changeFeedDestroys.erase(fetchKeysID);
		}
		if (e.code() == error_code_actor_cancelled && !data->shuttingDown && shard->phase >= AddingShard::Fetching) {
			if (shard->phase < AddingShard::FetchingCF) {
				data->storage.clearRange(keys);
				++data->counters.kvSystemClearRanges;
				data->byteSampleApplyClear(keys, invalidVersion);
			} else {
				ASSERT(data->data().getLatestVersion() > data->version.get());
				removeDataRange(
				    data, data->addVersionToMutationLog(data->data().getLatestVersion()), data->shards, keys);
				setAvailableStatus(data, keys, false);
				// Prevent another, overlapping fetchKeys from entering the Fetching phase until
				// data->data().getLatestVersion() is durable
				data->newestDirtyVersion.insert(keys, data->data().getLatestVersion());
			}
		}
		TraceEvent(SevError, "FetchKeysError", data->thisServerID)
		    .error(e)
		    .detail("Elapsed", now() - startTime)
		    .detail("KeyBegin", keys.begin)
		    .detail("KeyEnd", keys.end)
		    .detail("FetchVersion", fetchVersion)
		    .detail("KnownCommittedVersion", data->knownCommittedVersion.get());
		if (e.code() != error_code_actor_cancelled)
			data->otherError.sendError(e); // Kill the storage server.  Are there any recoverable errors?
		if (conductBulkLoad) {
			// Do best effort cleanup
			clearFileFolder(bulkLoadLocalDir, data->thisServerID, /*ignoreError=*/true);
		}
		throw; // goes nowhere
	}

	return Void();
}

AddingShard::AddingShard(StorageServer* server,
                         KeyRangeRef const& keys,
                         DataMovementReason reason,
                         const SSBulkLoadMetadata& ssBulkLoadMetadata)
  : keys(keys), server(server), transferredVersion(invalidVersion), fetchVersion(invalidVersion), phase(WaitPrevious),
    reason(reason), ssBulkLoadMetadata(ssBulkLoadMetadata) {
	fetchClient = fetchKeys(server, this);
}

void AddingShard::addMutation(Version version,
                              bool fromFetch,
                              MutationRef const& mutation,
                              MutationRefAndCipherKeys const& encryptedMutation) {
	if (version <= fetchVersion) {
		return;
	}

	server->counters.logicalBytesMoveInOverhead += mutation.expectedSize();
	if (mutation.type == mutation.ClearRange) {
		ASSERT(keys.begin <= mutation.param1 && mutation.param2 <= keys.end);
	} else if (isSingleKeyMutation((MutationRef::Type)mutation.type)) {
		ASSERT(keys.contains(mutation.param1));
	}

	if (phase == WaitPrevious) {
		// Updates can be discarded
	} else if (phase == Fetching) {
		// Save incoming mutations (See the comments of member variable `updates`).

		// Create a new VerUpdateRef in updates queue if it is a new version.
		if (!updates.size() || version > updates.end()[-1].version) {
			VerUpdateRef v;
			v.version = version;
			v.isPrivateData = false;
			updates.push_back(v);
		} else {
			ASSERT(version == updates.end()[-1].version);
		}
		// Add the mutation to the version.
		updates.back().mutations.push_back_deep(updates.back().arena(), mutation);
	} else if (phase == FetchingCF || phase == Waiting) {
		server->addMutation(version, fromFetch, mutation, encryptedMutation, keys, server->updateEagerReads);
	} else
		ASSERT(false);
}

void updateMoveInShardMetaData(StorageServer* data, MoveInShard* shard) {
	data->storage.writeKeyValue(KeyValueRef(persistMoveInShardKey(shard->id()), moveInShardValue(*shard->meta)));
	TraceEvent(shard->logSev, "UpdatedMoveInShardMetaData", data->thisServerID)
	    .detail("Shard", shard->toString())
	    .detail("ShardKey", persistMoveInShardKey(shard->id()))
	    .detail("DurableVersion", data->durableVersion.get());
}

void changeServerKeysWithPhysicalShards(StorageServer* data,
                                        const KeyRangeRef& keys,
                                        const UID& dataMoveId,
                                        bool nowAssigned,
                                        Version version,
                                        ChangeServerKeysContext context,
                                        EnablePhysicalShardMove enablePSM,
                                        ConductBulkLoad conductBulkLoad);

ACTOR Future<Void> fallBackToAddingShard(StorageServer* data, MoveInShard* moveInShard) {
	if (moveInShard->getPhase() != MoveInPhase::Fetching && moveInShard->getPhase() != MoveInPhase::Ingesting) {
		TraceEvent(SevError, "FallBackToAddingShardError", data->thisServerID)
		    .detail("MoveInShard", moveInShard->meta->toString());
		throw internal_error();
	}
	if (moveInShard->failed()) {
		return Void();
	}
	auto& mLV = data->addVersionToMutationLog(data->data().getLatestVersion());
	TraceEvent(SevInfo, "FallBackToAddingShardBegin", data->thisServerID)
	    .detail("Version", mLV.version)
	    .detail("MoveInShard", moveInShard->meta->toString());
	moveInShard->cancel();
	for (const auto& range : moveInShard->meta->ranges) {
		const Reference<ShardInfo>& currentShard = data->shards[range.begin];
		if (currentShard->moveInShard && currentShard->moveInShard->id() == moveInShard->id()) {
			ASSERT(range == currentShard->keys);
			changeServerKeysWithPhysicalShards(data,
			                                   range,
			                                   moveInShard->dataMoveId(),
			                                   true,
			                                   mLV.version - 1,
			                                   CSK_FALL_BACK,
			                                   EnablePhysicalShardMove::False,
			                                   ConductBulkLoad::False);
		} else {
			TraceEvent(SevWarn, "ShardAlreadyChanged", data->thisServerID)
			    .detail("ShardRange", currentShard->keys)
			    .detail("ShardState", currentShard->debugDescribeState());
		}
	}

	wait(data->durableVersion.whenAtLeast(mLV.version + 1));

	return Void();
}

ACTOR Future<Void> bulkLoadFetchShardFileToLoad(StorageServer* data,
                                                MoveInShard* moveInShard,
                                                std::string localRoot,
                                                BulkLoadTaskState bulkLoadTaskState) {
	ASSERT(bulkLoadTaskState.getLoadType() == BulkLoadType::SST);
	TraceEvent(SevInfo, "SSBulkLoadTaskFetchShardFile", data->thisServerID)
	    .setMaxEventLength(-1)
	    .setMaxFieldLength(-1)
	    .detail("BulkLoadTask", bulkLoadTaskState.toString())
	    .detail("MoveInShard", moveInShard->toString())
	    .detail("LocalRoot", abspath(localRoot));

	state double fetchStartTime = now();

	// Step 1: Download files to localRoot
	state BulkLoadFileSet fromRemoteFileSet = bulkLoadTaskState.getFileSet();
	BulkLoadByteSampleSetting currentClusterByteSampleSetting(
	    0,
	    "hashlittle2", // use function name to represent the method
	    SERVER_KNOBS->BYTE_SAMPLING_FACTOR,
	    SERVER_KNOBS->BYTE_SAMPLING_OVERHEAD,
	    SERVER_KNOBS->MIN_BYTE_SAMPLING_PROBABILITY);
	if (currentClusterByteSampleSetting != bulkLoadTaskState.getByteSampleSetting()) {
		// If the byte sampling setting mismatches between the data to load and the current cluster setting,
		// we need to redo the byte sampling.
		// Setting byteSampleFileName to empty string triggers redo byte sampling in the step 2.
		fromRemoteFileSet.removeByteSampleFile();
	}
	// Download data file and byte sample file from fromRemoteFileSet to toLocalFileSet
	state BulkLoadFileSet toLocalFileSet = wait(bulkLoadDownloadTaskFileSet(
	    bulkLoadTaskState.getTransportMethod(), fromRemoteFileSet, localRoot, data->thisServerID));
	TraceEvent(SevInfo, "SSBulkLoadTaskFetchShardSSTFileFetched", data->thisServerID)
	    .setMaxEventLength(-1)
	    .setMaxFieldLength(-1)
	    .detail("BulkLoadTask", bulkLoadTaskState.toString())
	    .detail("MoveInShard", moveInShard->toString())
	    .detail("RemoteFileSet", fromRemoteFileSet.toString())
	    .detail("LocalFileSet", toLocalFileSet.toString());

	// Step 2: Do byte sampling locally if the remote byte sampling file is not valid nor existing
	if (!toLocalFileSet.hasByteSampleFile()) {
		TraceEvent(SevInfo, "SSBulkLoadTaskFetchShardSSTFileValidByteSampleNotFound", data->thisServerID)
		    .setMaxEventLength(-1)
		    .setMaxFieldLength(-1)
		    .detail("BulkLoadTaskState", bulkLoadTaskState.toString())
		    .detail("LocalFileSet", toLocalFileSet.toString());
		state std::string byteSampleFileName =
		    generateBulkLoadBytesSampleFileNameFromDataFileName(toLocalFileSet.getDataFileName());
		std::string byteSampleFilePathLocal = abspath(joinPath(toLocalFileSet.getFolder(), byteSampleFileName));
		bool bytesSampleFileGenerated = wait(doBytesSamplingOnDataFile(
		    toLocalFileSet.getDataFileFullPath(), byteSampleFilePathLocal, data->thisServerID));
		if (bytesSampleFileGenerated) {
			toLocalFileSet.setByteSampleFileName(byteSampleFileName);
		}
	}
	TraceEvent(SevInfo, "SSBulkLoadTaskFetchShardByteSampled", data->thisServerID)
	    .setMaxEventLength(-1)
	    .setMaxFieldLength(-1)
	    .detail("BulkLoadTask", bulkLoadTaskState.toString())
	    .detail("MoveInShard", moveInShard->toString())
	    .detail("RemoteFileSet", fromRemoteFileSet.toString())
	    .detail("LocalFileSet", toLocalFileSet.toString());

	// Step 3: Build LocalRecord used by ShardedRocksDB KVStore when injecting data
	state CheckpointMetaData localRecord;
	localRecord.checkpointID = UID();
	localRecord.dir = abspath(toLocalFileSet.getFolder());
	for (const auto& range : moveInShard->ranges()) {
		ASSERT(bulkLoadTaskState.getRange().contains(range));
	}
	RocksDBCheckpointKeyValues rcp({ bulkLoadTaskState.getRange() });
	std::vector<KeyRange> coalesceRanges = coalesceRangeList(moveInShard->ranges());
	if (coalesceRanges.size() != 1) {
		TraceEvent(SevError, "SSBulkLoadTaskFetchShardSSTFileError", data->thisServerID)
		    .detail("Reason", "MoveInShard ranges unexpected, resulting in partially injecting data")
		    .setMaxEventLength(-1)
		    .setMaxFieldLength(-1)
		    .detail("BulkLoadTaskState", bulkLoadTaskState.toString())
		    .detail("MoveInShard", moveInShard->toString())
		    .detail("LocalFileSet", toLocalFileSet.toString());
	}
	localRecord.ranges = coalesceRanges;
	rcp.fetchedFiles.emplace_back(
	    abspath(toLocalFileSet.getDataFileFullPath()), coalesceRanges[0], bulkLoadTaskState.getTotalBytes());
	localRecord.serializedCheckpoint = ObjectWriter::toValue(rcp, IncludeVersion());
	localRecord.version = moveInShard->meta->createVersion;
	if (toLocalFileSet.hasByteSampleFile()) {
		ASSERT(fileExists(abspath(toLocalFileSet.getBytesSampleFileFullPath())));
		localRecord.bytesSampleFile = abspath(toLocalFileSet.getBytesSampleFileFullPath());
	}
	localRecord.setFormat(CheckpointFormat::RocksDBKeyValues);
	localRecord.setState(CheckpointMetaData::Complete);
	moveInShard->meta->checkpoints.push_back(localRecord);

	const double duration = now() - fetchStartTime;
	const int64_t totalBytes = getTotalFetchedBytes(moveInShard->meta->checkpoints);
	TraceEvent(SevInfo, "SSBulkLoadTaskFetchShardSSTFileBuildMetadata", data->thisServerID)
	    .setMaxEventLength(-1)
	    .setMaxFieldLength(-1)
	    .detail("BulkLoadTask", bulkLoadTaskState.toString())
	    .detail("MoveInShard", moveInShard->toString())
	    .detail("LocalRoot", abspath(localRoot))
	    .detail("LocalFileSet", toLocalFileSet.toString())
	    .detail("Duration", duration)
	    .detail("TotalBytes", totalBytes)
	    .detail("Rate", duration == 0 ? -1.0 : (double)totalBytes / duration);

	// Step 4: Update the moveInShard phase
	moveInShard->setPhase(MoveInPhase::Ingesting);
	return Void();
}

ACTOR Future<Void> fetchShardCheckpoint(StorageServer* data, MoveInShard* moveInShard, std::string dir) {
	TraceEvent(SevInfo, "FetchShardCheckpointMetaDataBegin", data->thisServerID)
	    .detail("MoveInShard", moveInShard->toString());
	ASSERT(moveInShard->getPhase() == MoveInPhase::Fetching);

	state std::vector<std::pair<KeyRange, CheckpointMetaData>> records;
	state std::vector<CheckpointMetaData> localRecords;
	state int attempt = 0; // TODO(heliu): use shard->meta->checkpoints to continue the fetch.
	state double fetchStartTime = now();

	loop {
		wait(delay(0, TaskPriority::FetchKeys));
		if (moveInShard->failed()) {
			return Void();
		}
		++attempt;
		records.clear();
		for (const auto& range : moveInShard->ranges()) {
			data->cx->invalidateCache(/*tenantPrefix=*/Key(), range);
		}
		try {
			wait(store(records,
			           getCheckpointMetaData(data->cx,
			                                 moveInShard->ranges(),
			                                 moveInShard->meta->createVersion,
			                                 DataMoveRocksCF,
			                                 moveInShard->dataMoveId())));
			if (moveInShard->failed()) {
				return Void();
			}

			if (g_network->isSimulated()) {
				for (const auto& [range, record] : records) {
					TraceEvent(moveInShard->logSev, "FetchShardCheckpointMetaData", data->thisServerID)
					    .detail("MoveInShardID", moveInShard->id())
					    .detail("Range", range)
					    .detail("CheckpointMetaData", record.toString());
				}
			}

			platform::eraseDirectoryRecursive(dir);
			ASSERT(platform::createDirectory(dir));

			TraceEvent(SevInfo, "FetchShardFetchCheckpointsBegin", data->thisServerID)
			    .detail("MoveInShardID", moveInShard->id());
			std::vector<Future<CheckpointMetaData>> fFetchCheckpoint;
			for (const auto& [range, record] : records) {
				const std::string checkpointDir = joinPath(dir, deterministicRandom()->randomAlphaNumeric(8));
				if (!platform::createDirectory(checkpointDir)) {
					throw retry();
				}
				fFetchCheckpoint.push_back(fetchCheckpointRanges(data->cx, record, checkpointDir, { range }));
			}
			wait(store(localRecords, getAll(fFetchCheckpoint)));
			if (moveInShard->failed()) {
				return Void();
			}
			break;
		} catch (Error& err) {
			TraceEvent(SevWarn, "FetchShardCheckpointsError", data->thisServerID)
			    .errorUnsuppressed(err)
			    .detail("Attempt", attempt)
			    .detail("MoveInShard", moveInShard->toString());
			state Error e = err;
			if (e.code() == error_code_actor_cancelled || moveInShard->getPhase() != MoveInPhase::Fetching) {
				throw e;
			} else if (attempt > 10 || e.code() == error_code_checkpoint_not_found) {
				wait(fallBackToAddingShard(data, moveInShard));
				return Void();
			} else {
				wait(delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, TaskPriority::FetchKeys));
			}
		}
	}

	const double duration = now() - fetchStartTime;
	const int64_t totalBytes = getTotalFetchedBytes(localRecords);
	TraceEvent(SevInfo, "FetchCheckpointsStats", data->thisServerID)
	    .detail("MoveInShard", moveInShard->toString())
	    .detail("Checkpoint", describe(localRecords))
	    .detail("Duration", duration)
	    .detail("TotalBytes", totalBytes)
	    .detail("Rate", (double)totalBytes / duration);

	moveInShard->meta->checkpoints = std::move(localRecords);
	moveInShard->setPhase(MoveInPhase::Ingesting);

	updateMoveInShardMetaData(data, moveInShard);

	TraceEvent(SevInfo, "FetchShardCheckpointsEnd", data->thisServerID).detail("MoveInShard", moveInShard->toString());
	return Void();
}

ACTOR Future<Void> fetchShardIngestCheckpoint(StorageServer* data, MoveInShard* moveInShard) {
	TraceEvent(SevInfo, "FetchShardIngestCheckpointBegin", data->thisServerID)
	    .detail("Checkpoints", describe(moveInShard->checkpoints()));
	ASSERT(moveInShard->getPhase() == MoveInPhase::Ingesting);
	state double startTime = now();

	try {
		wait(
		    data->storage.restore(moveInShard->destShardIdString(), moveInShard->ranges(), moveInShard->checkpoints()));
	} catch (Error& e) {
		state Error err = e;
		TraceEvent(SevWarn, "FetchShardIngestedCheckpointError", data->thisServerID)
		    .errorUnsuppressed(e)
		    .detail("MoveInShard", moveInShard->toString())
		    .detail("Checkpoints", describe(moveInShard->checkpoints()));
		if (e.code() == error_code_failed_to_restore_checkpoint && !moveInShard->failed()) {
			moveInShard->setPhase(MoveInPhase::Fetching);
			updateMoveInShardMetaData(data, moveInShard);
			return Void();
		}
		throw err;
	}

	TraceEvent(SevInfo, "FetchShardIngestedCheckpoint", data->thisServerID)
	    .detail("MoveInShard", moveInShard->toString())
	    .detail("Checkpoints", describe(moveInShard->checkpoints()));

	if (moveInShard->failed()) {
		return Void();
	}

	for (const auto& range : moveInShard->ranges()) {
		data->storage.persistRangeMapping(range, true);
	}

	for (const auto& checkpoint : moveInShard->checkpoints()) {
		if (!checkpoint.bytesSampleFile.present()) {
			continue;
		}
		std::unique_ptr<ICheckpointByteSampleReader> reader = newCheckpointByteSampleReader(checkpoint);
		while (reader->hasNext()) {
			KeyValue kv = reader->next();
			int64_t size = BinaryReader::fromStringRef<int64_t>(kv.value, Unversioned());
			Key key = kv.key;
			if (key.startsWith(persistByteSampleKeys.begin)) {
				key = key.removePrefix(persistByteSampleKeys.begin);
			}
			if (!checkpoint.containsKey(key)) {
				TraceEvent(moveInShard->logSev, "StorageRestoreCheckpointKeySampleNotInRange", data->thisServerID)
				    .detail("Checkpoint", checkpoint.toString())
				    .detail("SampleKey", key)
				    .detail("Size", size);
				continue;
			}
			TraceEvent(moveInShard->logSev, "StorageRestoreCheckpointKeySample", data->thisServerID)
			    .detail("Checkpoint", checkpoint.checkpointID.toString())
			    .detail("SampleKey", key)
			    .detail("Size", size);
			data->metrics.byteSample.sample.insert(key, size);
			data->metrics.notifyBytes(key, size);
			data->addMutationToMutationLogOrStorage(
			    invalidVersion,
			    MutationRef(MutationRef::SetValue, key.withPrefix(persistByteSampleKeys.begin), kv.value));
		}
	}

	moveInShard->setPhase(MoveInPhase::ApplyingUpdates);
	updateMoveInShardMetaData(data, moveInShard);

	moveInShard->fetchComplete.send(Void());

	const int64_t totalBytes = getTotalFetchedBytes(moveInShard->checkpoints());
	const double duration = now() - startTime;
	TraceEvent(SevInfo, "FetchShardIngestCheckpointEnd", data->thisServerID)
	    .detail("Checkpoints", describe(moveInShard->checkpoints()))
	    .detail("Bytes", totalBytes)
	    .detail("Duration", duration)
	    .detail("Rate", static_cast<double>(totalBytes) / duration);

	return Void();
}

ACTOR Future<Void> fetchShardApplyUpdates(StorageServer* data,
                                          MoveInShard* moveInShard,
                                          std::shared_ptr<MoveInUpdates> moveInUpdates) {
	TraceEvent(SevInfo, "FetchShardApplyUpdatesBegin", data->thisServerID)
	    .detail("MoveInShard", moveInShard->toString());
	ASSERT(moveInShard->getPhase() == MoveInPhase::ApplyingUpdates);
	state double startTime = now();

	try {
		if (moveInShard->failed()) {
			return Void();
		}

		loop {
			Promise<FetchInjectionInfo*> p;
			data->readyFetchKeys.push_back(p);
			FetchInjectionInfo* batch = wait(p.getFuture());
			if (moveInShard->failed()) {
				return Void();
			}

			state Version version = data->version.get() + 1;
			data->mutableData().createNewVersion(version);
			ASSERT(version == data->data().getLatestVersion());

			Version highWatermark = moveInShard->getHighWatermark();
			std::vector<Standalone<VerUpdateRef>> updates =
			    moveInUpdates->next(SERVER_KNOBS->FETCH_SHARD_UPDATES_BYTE_LIMIT);
			if (!updates.empty()) {
				TraceEvent(moveInShard->logSev, "FetchShardApplyingUpdates", data->thisServerID)
				    .detail("MoveInShard", moveInShard->toString())
				    .detail("MinVerion", updates.front().version)
				    .detail("MaxVerion", updates.back().version)
				    .detail("TargetVersion", version)
				    .detail("HighWatermark", highWatermark)
				    .detail("Size", updates.size());
			}
			for (auto i = updates.begin(); i != updates.end(); ++i) {
				ASSERT(i->version <= version);
				ASSERT(i->version > highWatermark);
				TraceEvent(SevDebug, "MoveInUpdatesInject", moveInShard->id())
				    .detail("Version", i->version)
				    .detail("Size", i->mutations.size());
				highWatermark = i->version;
				i->version = version;
				batch->arena.dependsOn(i->arena());
				if (MUTATION_TRACKING_ENABLED) {
					for (auto& m : i->mutations) {
						DEBUG_MUTATION("fetchShardMutationInject", i->version, m, data->thisServerID);
					}
				}
			}

			const int startSize = batch->changes.size();
			batch->changes.resize(startSize + updates.size());

			// FIXME: pass the deque back rather than copy the data
			std::copy(updates.begin(), updates.end(), batch->changes.begin() + startSize);

			moveInShard->setHighWatermark(highWatermark);
			auto& mLV = data->addVersionToMutationLog(data->data().getLatestVersion());

			if (!moveInUpdates->hasNext()) {
				moveInShard->setPhase(MoveInPhase::ReadWritePending);
				MoveInShardMetaData newMoveInShard(*moveInShard->meta);
				newMoveInShard.setPhase(MoveInPhase::Complete);
				data->addMutationToMutationLog(mLV,
				                               MutationRef(MutationRef::SetValue,
				                                           persistMoveInShardKey(moveInShard->id()),
				                                           moveInShardValue(newMoveInShard)));
				std::vector<KeyRange> ranges = moveInShard->ranges();
				std::sort(ranges.begin(), ranges.end(), KeyRangeRef::ArbitraryOrder());
				for (const auto& range : ranges) {
					TraceEvent(moveInShard->logSev, "PersistShardReadWriteStatus").detail("Range", range);
					ASSERT(data->shards[range.begin]->keys == range);
					StorageServerShard newShard = data->shards[range.begin]->toStorageServerShard();
					ASSERT(newShard.range == range);
					ASSERT(newShard.getShardState() == StorageServerShard::ReadWritePending);
					newShard.setShardState(StorageServerShard::ReadWrite);
					updateStorageShard(data, newShard);
					setAvailableStatus(data, range, true);
				}

				// Wait for the transferredVersion (and therefore the shard data) to be committed and durable.
				wait(data->durableVersion.whenAtLeast(mLV.version + 1));
				if (moveInShard->failed()) {
					return Void();
				}
				break;
			} else {
				data->addMutationToMutationLog(mLV,
				                               MutationRef(MutationRef::SetValue,
				                                           persistMoveInShardKey(moveInShard->id()),
				                                           moveInShardValue(*moveInShard->meta)));
				TraceEvent(moveInShard->logSev, "MultipleApplyUpdates").detail("MoveInShard", moveInShard->toString());
			}

			wait(data->version.whenAtLeast(version + 1));
		}

		moveInShard->setPhase(MoveInPhase::Complete);
		TraceEvent(moveInShard->logSev, "FetchShardApplyUpdatesSuccess", data->thisServerID)
		    .detail("MoveInShard", moveInShard->toString());

		double duration = now() - startTime;
		const int64_t totalBytes = getTotalFetchedBytes(moveInShard->checkpoints());
		TraceEvent(moveInShard->logSev, "FetchShardApplyUpdatesStats", data->thisServerID)
		    .detail("MoveInShard", moveInShard->toString())
		    .detail("Duration", duration)
		    .detail("TotalBytes", totalBytes)
		    .detail("Rate", (double)totalBytes / duration);
		duration = now() - moveInShard->meta->startTime;
		TraceEvent(SevInfo, "FetchShardStats", data->thisServerID)
		    .detail("MoveInShard", moveInShard->toString())
		    .detail("Duration", duration)
		    .detail("TotalBytes", totalBytes)
		    .detail("Rate", (double)totalBytes / duration);

		for (const auto& range : moveInShard->ranges()) {
			const Reference<ShardInfo>& currentShard = data->shards[range.begin];
			if (!currentShard->moveInShard || currentShard->moveInShard->id() != moveInShard->id()) {
				TraceEvent(SevWarn, "MoveInShardChanged", data->thisServerID)
				    .detail("CurrentShard", currentShard->debugDescribeState())
				    .detail("MoveInShard", moveInShard->toString());
				throw operation_cancelled();
			}
			StorageServerShard newShard = currentShard->toStorageServerShard();
			ASSERT(newShard.range == range);
			newShard.setShardState(StorageServerShard::ReadWrite);
			TraceEvent(SevInfo, "MoveInShardReadWrite", data->thisServerID)
			    .detail("Version", data->version.get())
			    .detail("MoveInShard", moveInShard->toString());
			data->addShard(ShardInfo::newShard(data, newShard));
			data->newestAvailableVersion.insert(range, latestVersion);
			coalescePhysicalShards(data, range);
		}
		validate(data);
		moveInShard->readWrite.send(Void());
	} catch (Error& e) {
		// TODO(heliu): In case of unrecoverable errors, fail the data move.
		TraceEvent(SevWarn, "FetchShardApplyUpdatesError", data->thisServerID)
		    .errorUnsuppressed(e)
		    .detail("MoveInShard", moveInShard->toString());
		throw e;
	}

	return Void();
}

ACTOR Future<Void> cleanUpMoveInShard(StorageServer* data, Version version, MoveInShard* moveInShard) {
	TraceEvent(moveInShard->logSev, "CleanUpMoveInShardBegin", data->thisServerID)
	    .detail("MoveInShard", moveInShard->meta->toString())
	    .detail("Version", version);
	wait(data->durableVersion.whenAtLeast(version));

	platform::eraseDirectoryRecursive(fetchedCheckpointDir(data->folder, moveInShard->id()));

	auto& mLV = data->addVersionToMutationLog(data->data().getLatestVersion());
	KeyRange persistUpdatesRange = persistUpdatesKeyRange(moveInShard->id());
	data->addMutationToMutationLog(
	    mLV, MutationRef(MutationRef::ClearRange, persistUpdatesRange.begin, persistUpdatesRange.end));

	state bool clearRecord = true;
	if (moveInShard->failed()) {
		for (const auto& mir : moveInShard->ranges()) {
			auto existingShards = data->shards.intersectingRanges(mir);
			for (auto it = existingShards.begin(); it != existingShards.end(); ++it) {
				if (it->value()->moveInShard && it->value()->moveInShard->id() == moveInShard->id()) {
					clearRecord = false;
					break;
				}
			}
		}
	}
	if (clearRecord) {
		const Key persistKey = persistMoveInShardKey(moveInShard->id());
		data->addMutationToMutationLog(mLV, MutationRef(MutationRef::ClearRange, persistKey, keyAfter(persistKey)));
	}
	wait(data->durableVersion.whenAtLeast(mLV.version + 1));

	if (clearRecord) {
		data->moveInShards.erase(moveInShard->id());
	}

	return Void();
}

// It works in the following sequences:
// 1. Look up the corresponding checkpoints, based on key ranges and version.
// 2. Fetch the checkpoints from the source storage servers
// 3. Restore the checkpoints.
// 4. Apply any new updates accumulated since the checkpoint version.
// 5. Mark the new shard as read-write
// 6. Clean up all the checkpoint files etc.
ACTOR Future<Void> fetchShard(StorageServer* data, MoveInShard* moveInShard) {
	TraceEvent(SevInfo, "FetchShardBegin", data->thisServerID).detail("MoveInShard", moveInShard->toString());
	state std::shared_ptr<MoveInUpdates> moveInUpdates = moveInShard->updates;
	state std::string dir = fetchedCheckpointDir(data->folder, moveInShard->id());
	state MoveInPhase phase;

	ASSERT(moveInShard->getPhase() != MoveInPhase::Pending);

	wait(data->coreStarted.getFuture() && data->durableVersion.whenAtLeast(moveInShard->meta->createVersion + 1));
	wait(data->fetchKeysParallelismLock.take(TaskPriority::DefaultYield));
	state FlowLock::Releaser holdingFKPL(data->fetchKeysParallelismLock);

	state BulkLoadTaskState bulkLoadTaskState;
	state bool conductBulkLoad = moveInShard->meta->conductBulkLoad;
	if (conductBulkLoad) {
		// Get the bulkload task metadata from the data move metadata. For details, see the comments in the fetchKeys.
		wait(store(bulkLoadTaskState,
		           getBulkLoadTaskStateFromDataMove(
		               data->cx, moveInShard->dataMoveId(), data->version.get(), data->thisServerID)));
	}

	loop {
		phase = moveInShard->getPhase();
		TraceEvent(moveInShard->logSev, "FetchShardLoop", data->thisServerID)
		    .detail("MoveInShard", moveInShard->toString());
		try {
			// Pending = 0, Fetching = 1, Ingesting = 2, ApplyingUpdates = 3, Complete = 4, Deleting = 4, Fail = 6,
			if (phase == MoveInPhase::Fetching) {
				if (conductBulkLoad) {
					// Check the correctness: bulkLoadTaskMetadata stored in dataMoveMetadata must have the same
					// dataMoveId.
					ASSERT(bulkLoadTaskState.getDataMoveId() != moveInShard->dataMoveId());
					wait(bulkLoadFetchShardFileToLoad(data, moveInShard, dir, bulkLoadTaskState));
				} else {
					wait(fetchShardCheckpoint(data, moveInShard, dir));
					TraceEvent(SevWarn, "SSBulkLoadTaskFetchShardFailedForNoMetadata", data->thisServerID)
					    .detail("DataMoveId", moveInShard->dataMoveId());
				}
			} else if (phase == MoveInPhase::Ingesting) {
				wait(fetchShardIngestCheckpoint(data, moveInShard));
			} else if (phase == MoveInPhase::ApplyingUpdates) {
				wait(fetchShardApplyUpdates(data, moveInShard, moveInUpdates));
			} else if (phase == MoveInPhase::Complete) {
				data->actors.add(cleanUpMoveInShard(data, data->data().getLatestVersion(), moveInShard));
				break;
			} else if (phase == MoveInPhase::Error || phase == MoveInPhase::Cancel) {
				data->actors.add(cleanUpMoveInShard(data, data->data().getLatestVersion(), moveInShard));
				break;
			}
		} catch (Error& e) {
			TraceEvent(SevWarn, "FetchShardError", data->thisServerID)
			    .errorUnsuppressed(e)
			    .detail("MoveInShardID", moveInShard->id())
			    .detail("MoveInShard", moveInShard->toString());
			throw e;
		}
		wait(delay(1, TaskPriority::FetchKeys));
	}

	validate(data);

	TraceEvent(SevInfo, "FetchShardEnd", data->thisServerID).detail("MoveInShard", moveInShard->toString());

	return Void();
}

MoveInUpdates::MoveInUpdates(UID id,
                             Version version,
                             struct StorageServer* data,
                             IKeyValueStore* store,
                             MoveInUpdatesSpilled spilled)
  : id(id), lastRepliedVersion(version), data(data), store(store), range(persistUpdatesKeyRange(id)), fail(false),
    spilled(spilled), size(0), logSev(static_cast<Severity>(SERVER_KNOBS->PHYSICAL_SHARD_MOVE_LOG_SEVERITY)) {
	if (spilled) {
		this->loadFuture = loadUpdates(this, lastRepliedVersion + 1, data->version.get() + 1);
	}
}

std::vector<Standalone<VerUpdateRef>> MoveInUpdates::next(const int byteLimit) {
	std::vector<Standalone<VerUpdateRef>> res;
	if (this->fail) {
		return res;
	}
	if (this->spilled) {
		std::swap(res, this->spillBuffer);
		if (!res.empty()) {
			this->lastRepliedVersion = res.back().version;
		}
		if (!this->loadFuture.isValid()) {
			const Version begin = this->lastRepliedVersion + 1;
			Version end = this->data->version.get() + 1;
			if (!this->updates.empty() && this->updates.front().version < end) {
				ASSERT(this->lastRepliedVersion < this->updates.front().version);
				end = this->updates.front().version;
			}
			this->loadFuture = loadUpdates(this, begin, end);
		} else if (this->loadFuture.isError()) {
			throw operation_cancelled();
		}
	} else {
		int size = 0;
		for (auto it = updates.begin(); it != updates.end();) {
			if (it->version > this->lastRepliedVersion) {
				res.push_back(*it);
				size += it->mutations.expectedSize();
			}
			if (it->version <= this->data->durableVersion.get()) {
				it = updates.erase(it);
			} else {
				++it;
			}
			if (size > byteLimit) {
				break;
			}
		}
	}

	if (!res.empty()) {
		this->lastRepliedVersion = res.back().version;
	}

	return res;
}

Key MoveInUpdates::getPersistKey(const Version version, const int idx) const {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(range.begin);
	wr << bigEndian64(static_cast<uint64_t>(version));
	wr.serializeBytes("/"_sr);
	wr << bigEndian64(static_cast<uint64_t>(idx));
	return wr.toValue();
}

void MoveInUpdates::addMutation(Version version,
                                bool fromFetch,
                                MutationRef const& mutation,
                                MutationRefAndCipherKeys const& encryptedMutation,
                                bool allowSpill) {
	if (version <= lastRepliedVersion || this->fail) {
		return;
	}

	if (updates.empty() || version > updates.back().version) {
		Standalone<VerUpdateRef> v;
		v.version = version;
		v.isPrivateData = false;
		updates.push_back(v);
	}

	// Add the mutation to the version.
	updates.back().mutations.push_back_deep(updates.back().arena(), mutation);
	this->size += sizeof(MutationRef) + mutation.expectedSize();
	TraceEvent(SevDebug, "MoveInUpdatesAddMutation", id).detail("Version", version).detail("Mutation", mutation);

	if (allowSpill) {
		while (!updates.empty() && this->size > SERVER_KNOBS->FETCH_SHARD_BUFFER_BYTE_LIMIT &&
		       updates.front().version <= data->durableVersion.get()) {
			TraceEvent(SevInfo, "MoveInUpdatesSpill", id)
			    .detail("CurrentSize", this->size)
			    .detail("SpillSize", updates.front().expectedSize())
			    .detail("SpillVersion", updates.front().version)
			    .detail("MutationCount", updates.front().mutations.size());
			this->size -= updates.front().expectedSize();
			spilled = MoveInUpdatesSpilled::True;
			updates.pop_front();
		}
	}
}

MoveInShard::MoveInShard(StorageServer* server,
                         const UID& id,
                         const UID& dataMoveId,
                         const Version version,
                         const ConductBulkLoad conductBulkLoad,
                         MoveInPhase phase)
  : meta(std::make_shared<MoveInShardMetaData>(id,
                                               dataMoveId,
                                               std::vector<KeyRange>(),
                                               version,
                                               phase,
                                               conductBulkLoad)),
    server(server), updates(std::make_shared<MoveInUpdates>(id,
                                                            version,
                                                            server,
                                                            server->storage.getKeyValueStore(),
                                                            MoveInUpdatesSpilled::False)),
    isRestored(true) {
	if (phase != MoveInPhase::Pending) {
		fetchClient = fetchShard(server, this);
	} else {
		fetchClient = Void();
	}
}

MoveInShard::MoveInShard(StorageServer* server,
                         const UID& id,
                         const UID& dataMoveId,
                         const Version version,
                         const ConductBulkLoad conductBulkLoad)
  : MoveInShard(server, id, dataMoveId, version, conductBulkLoad, MoveInPhase::Fetching) {}

MoveInShard::MoveInShard(StorageServer* server, MoveInShardMetaData meta)
  : meta(std::make_shared<MoveInShardMetaData>(meta)), server(server),
    updates(std::make_shared<MoveInUpdates>(meta.id,
                                            meta.highWatermark,
                                            server,
                                            server->storage.getKeyValueStore(),
                                            MoveInUpdatesSpilled::True)),
    isRestored(true) {
	if (getPhase() != MoveInPhase::Pending) {
		fetchClient = fetchShard(server, this);
	} else {
		fetchClient = Void();
	}
}

MoveInShard::~MoveInShard() {
	// Note even if the MoveInShard is cancelled, the following are used as signals of changes, not
	// necessarily fetch complete.
	if (!fetchComplete.isSet()) {
		fetchComplete.send(Void());
	}
	if (!readWrite.isSet()) {
		readWrite.send(Void());
	}
}

void MoveInShard::addRange(const KeyRangeRef range) {
	for (const auto& kr : this->meta->ranges) {
		if (kr.intersects(range)) {
			ASSERT(kr == range);
			return;
		}
	}
	this->meta->ranges.push_back(range);
	std::sort(this->meta->ranges.begin(), this->meta->ranges.end(), KeyRangeRef::ArbitraryOrder());
}

void MoveInShard::removeRange(const KeyRangeRef range) {
	std::vector<KeyRange> newRanges;
	for (const auto& kr : this->meta->ranges) {
		const std::vector<KeyRangeRef> rs = kr - range;
		newRanges.insert(newRanges.end(), rs.begin(), rs.end());
	}
	this->meta->ranges.swap(newRanges);
	std::sort(this->meta->ranges.begin(), this->meta->ranges.end(), KeyRangeRef::ArbitraryOrder());
}

void MoveInShard::cancel(const MoveInFailed failed) {
	const Version version = this->server->data().getLatestVersion();
	TraceEvent(SevDebug, "MoveInCancelled", this->server->thisServerID)
	    .detail("MoveInShard", this->meta->toString())
	    .detail("Version", version);
	if (this->getPhase() == MoveInPhase::Error || this->getPhase() == MoveInPhase::Cancel) {
		return;
	}
	if (failed) {
		this->setPhase(MoveInPhase::Error);
		this->meta->error = "Error";
	} else {
		this->setPhase(MoveInPhase::Cancel);
		this->meta->error = "Cancelled";
	}
	this->updates->fail = true;
	auto& mLV = this->server->addVersionToMutationLog(version);
	this->server->addMutationToMutationLog(
	    mLV, MutationRef(MutationRef::SetValue, persistMoveInShardKey(this->id()), moveInShardValue(*this->meta)));
}

void MoveInShard::addMutation(Version version,
                              bool fromFetch,
                              MutationRef const& mutation,
                              MutationRefAndCipherKeys const& encryptedMutation) {
	DEBUG_MUTATION("MoveInShardAddMutation", version, mutation, this->id());
	server->counters.logicalBytesMoveInOverhead += mutation.expectedSize();
	const KeyRangeRef range = this->getAffectedRange(mutation);
	if (mutation.type == mutation.ClearRange) {
		ASSERT(range.begin <= mutation.param1 && mutation.param2 <= range.end);
	} else if (isSingleKeyMutation((MutationRef::Type)mutation.type)) {
		ASSERT(range.contains(mutation.param1));
	}

	const MoveInPhase phase = this->getPhase();
	if (phase < MoveInPhase::ReadWritePending && !fromFetch) {
		updates->addMutation(version, fromFetch, mutation, encryptedMutation, phase < MoveInPhase::ApplyingUpdates);
	} else if (phase == MoveInPhase::ReadWritePending || phase == MoveInPhase::Complete || fromFetch) {
		server->addMutation(version, fromFetch, mutation, encryptedMutation, range, server->updateEagerReads);
	}
}

KeyRangeRef MoveInShard::getAffectedRange(const MutationRef& mutation) const {
	ASSERT(meta != nullptr);
	KeyRangeRef res;
	if (mutation.type == mutation.ClearRange) {
		const KeyRangeRef mr(mutation.param1, mutation.param2);
		for (const auto& range : meta->ranges) {
			if (range.intersects(mr)) {
				ASSERT(range.contains(mr));
				res = range;
				break;
			}
		}
	} else if (isSingleKeyMutation((MutationRef::Type)mutation.type)) {
		for (const auto& range : meta->ranges) {
			if (range.contains(mutation.param1)) {
				res = range;
				break;
			}
		}
	}
	return res;
}

// static
ShardInfo* ShardInfo::newShard(StorageServer* data, const StorageServerShard& shard) {
	TraceEvent(SevDebug, "NewShard", data->thisServerID).detail("StorageServerShard", shard.toString());
	ShardInfo* res = nullptr;
	switch (shard.getShardState()) {
	case StorageServerShard::NotAssigned:
		res = newNotAssigned(shard.range);
		break;
	case StorageServerShard::Adding:
		// This handles two cases: (1) old data moves when encode_shard_location_metadata is off; (2) fallback data
		// moves. For case 1, the bulkload is available only if the encode_shard_location_metadata is on. Therefore, the
		// old data moves is never for bulkload. For case 2, fallback happens only if fetchCheckpoint fails which is not
		// a case for bulkload which does not do fetchCheckpoint.
		res = newAdding(data, shard.range, DataMovementReason::INVALID, SSBulkLoadMetadata());
		break;
	case StorageServerShard::ReadWritePending:
		TraceEvent(SevWarnAlways, "CancellingAlmostReadyMoveInShard").detail("StorageServerShard", shard.toString());
		ASSERT(!shard.moveInShardId.present());
		// TODO(BulkLoad): current bulkload with ShardedRocksDB and PhysicalSharMove cannot handle this fallback case.
		res = newAdding(data, shard.range, DataMovementReason::INVALID, SSBulkLoadMetadata());
		break;
	case StorageServerShard::MovingIn: {
		ASSERT(shard.moveInShardId.present());
		const auto it = data->moveInShards.find(shard.moveInShardId.get());
		ASSERT(it != data->moveInShards.end());
		res = new ShardInfo(shard.range, it->second);
		break;
	}
	case StorageServerShard::ReadWrite:
		res = newReadWrite(shard.range, data);
		break;
	default:
		TraceEvent(SevError, "UnknownShardState").detail("StorageServerShard", shard.toString());
	}
	res->populateShard(shard);
	return res;
}

void ShardInfo::addMutation(Version version,
                            bool fromFetch,
                            MutationRef const& mutation,
                            MutationRefAndCipherKeys const& encryptedMutation) {
	ASSERT((void*)this);
	ASSERT(keys.contains(mutation.param1));
	if (adding) {
		adding->addMutation(version, fromFetch, mutation, encryptedMutation);
	} else if (moveInShard) {
		moveInShard->addMutation(version, fromFetch, mutation, encryptedMutation);
	} else if (readWrite) {
		readWrite->addMutation(
		    version, fromFetch, mutation, encryptedMutation, this->keys, readWrite->updateEagerReads);
	} else if (mutation.type != MutationRef::ClearRange) {
		TraceEvent(SevError, "DeliveredToNotAssigned").detail("Version", version).detail("Mutation", mutation);
		ASSERT(false); // Mutation delivered to notAssigned shard!
	}
}

ACTOR Future<Void> restoreShards(StorageServer* data,
                                 Version version,
                                 RangeResult storageShards,
                                 RangeResult moveInShards,
                                 RangeResult assignedShards,
                                 RangeResult availableShards) {
	TraceEvent(SevInfo, "StorageServerRestoreShardsBegin", data->thisServerID)
	    .detail("StorageShard", storageShards.size())
	    .detail("Version", version);

	state int moveInLoc;
	for (moveInLoc = 0; moveInLoc < moveInShards.size(); ++moveInLoc) {
		MoveInShardMetaData meta = decodeMoveInShardValue(moveInShards[moveInLoc].value);
		TraceEvent(SevInfo, "RestoreMoveInShard", data->thisServerID).detail("MoveInShard", meta.toString());
		data->moveInShards.emplace(meta.id, std::make_shared<MoveInShard>(data, meta));
		wait(yield());
	}

	state int shardLoc;
	for (shardLoc = 0; shardLoc < storageShards.size(); ++shardLoc) {
		const KeyRangeRef shardRange(
		    storageShards[shardLoc].key.removePrefix(persistStorageServerShardKeys.begin),
		    shardLoc + 1 == storageShards.size()
		        ? allKeys.end
		        : storageShards[shardLoc + 1].key.removePrefix(persistStorageServerShardKeys.begin));
		StorageServerShard shard =
		    ObjectReader::fromStringRef<StorageServerShard>(storageShards[shardLoc].value, IncludeVersion());
		shard.range = shardRange;
		TraceEvent(SevVerbose, "RestoreShardsStorageShard", data->thisServerID)
		    .detail("Range", shardRange)
		    .detail("StorageShard", shard.toString());

		const StorageServerShard::ShardState shardState = shard.getShardState();
		auto existingShards = data->shards.intersectingRanges(shardRange);
		for (auto it = existingShards.begin(); it != existingShards.end(); ++it) {
			TraceEvent(SevVerbose, "RestoreShardsIntersectingRange", data->thisServerID)
			    .detail("StorageShard", shard.toString())
			    .detail("IntersectingShardRange", it->value()->keys)
			    .detail("IntersectingShardState", it->value()->debugDescribeState())
			    .log();
			ASSERT(it->value()->notAssigned());
		}

		if (shardState == StorageServerShard::NotAssigned) {
			ASSERT(data->newestAvailableVersion.allEqual(shardRange, invalidVersion));
			continue;
		}

		auto ranges = data->shards.getAffectedRangesAfterInsertion(shard.range, Reference<ShardInfo>());
		for (int i = 0; i < ranges.size(); i++) {
			KeyRangeRef& range = static_cast<KeyRangeRef&>(ranges[i]);
			TraceEvent(SevVerbose, "RestoreShardsAddShard", data->thisServerID)
			    .detail("Shard", shard.toString())
			    .detail("Range", range);
			if (range == shard.range) {
				data->addShard(ShardInfo::newShard(data, shard));
			} else {
				StorageServerShard rightShard = ranges[i].value->toStorageServerShard();
				rightShard.range = range;
				data->addShard(ShardInfo::newShard(data, rightShard));
			}
		}

		const bool nowAvailable = shard.getShardState() == StorageServerShard::ReadWrite;
		if (nowAvailable) {
			auto r = data->newestAvailableVersion.intersectingRanges(shardRange);
			for (auto i = r.begin(); i != r.end(); ++i) {
				TraceEvent(SevDebug, "CheckAvailableRange", data->thisServerID).detail("Range", i.range());
				ASSERT(i.value() == latestVersion);
			}
		}

		if (shardState == StorageServerShard::Adding) {
			data->storage.clearRange(shardRange);
			++data->counters.kvSystemClearRanges;
			data->byteSampleApplyClear(shardRange, invalidVersion);
			data->newestDirtyVersion.insert(shardRange, version);
		}

		wait(yield());
	}

	state int availableLoc;
	for (availableLoc = 0; availableLoc < availableShards.size(); availableLoc++) {
		KeyRangeRef shardRange(
		    availableShards[availableLoc].key.removePrefix(persistShardAvailableKeys.begin),
		    availableLoc + 1 == availableShards.size()
		        ? allKeys.end
		        : availableShards[availableLoc + 1].key.removePrefix(persistShardAvailableKeys.begin));
		ASSERT(!shardRange.empty());

		const bool nowAvailable = availableShards[availableLoc].value != "0"_sr;
		auto existingShards = data->shards.intersectingRanges(shardRange);
		for (auto it = existingShards.begin(); it != existingShards.end(); ++it) {
			TraceEvent(SevVerbose, "RestoreShardsValidateAvailable", data->thisServerID)
			    .detail("Range", shardRange)
			    .detail("Available", nowAvailable)
			    .detail("IntersectingShardRange", it->value()->keys)
			    .detail("IntersectingShardState", it->value()->debugDescribeState())
			    .log();
			if (nowAvailable) {
				ASSERT(it->value()->isReadable());
				ASSERT(data->newestAvailableVersion.allEqual(shardRange, latestVersion));
			}
		}

		wait(yield());
	}

	state int assignedLoc;
	for (assignedLoc = 0; assignedLoc < assignedShards.size(); ++assignedLoc) {
		KeyRangeRef shardRange(assignedShards[assignedLoc].key.removePrefix(persistShardAssignedKeys.begin),
		                       assignedLoc + 1 == assignedShards.size()
		                           ? allKeys.end
		                           : assignedShards[assignedLoc + 1].key.removePrefix(persistShardAssignedKeys.begin));
		ASSERT(!shardRange.empty());
		const bool nowAssigned = assignedShards[assignedLoc].value != "0"_sr;

		auto existingShards = data->shards.intersectingRanges(shardRange);
		for (auto it = existingShards.begin(); it != existingShards.end(); ++it) {
			TraceEvent(SevVerbose, "RestoreShardsValidateAssigned", data->thisServerID)
			    .detail("Range", shardRange)
			    .detail("Assigned", nowAssigned)
			    .detail("IntersectingShardRange", it->value()->keys)
			    .detail("IntersectingShardState", it->value()->debugDescribeState())
			    .log();

			ASSERT_EQ(it->value()->assigned(), nowAssigned);
			if (!nowAssigned) {
				ASSERT(data->newestAvailableVersion.allEqual(shardRange, invalidVersion));
			}
		}

		wait(yield());
	}

	coalescePhysicalShards(data, allKeys);
	validate(data, /*force=*/true);
	TraceEvent(SevInfo, "StorageServerRestoreShardsEnd", data->thisServerID).detail("Version", version);

	return Void();
}

// Finds any change feeds that no longer have shards on this server, and clean them up
void cleanUpChangeFeeds(StorageServer* data, const KeyRangeRef& keys, Version version) {
	std::map<Key, KeyRange> candidateFeeds;
	auto ranges = data->keyChangeFeed.intersectingRanges(keys);
	for (auto r : ranges) {
		for (auto feed : r.value()) {
			candidateFeeds[feed->id] = feed->range;
		}
	}
	for (auto f : candidateFeeds) {
		bool foundAssigned = false;
		auto shards = data->shards.intersectingRanges(f.second);
		for (auto shard : shards) {
			if (shard->value()->assigned()) {
				foundAssigned = true;
				break;
			}
		}

		if (!foundAssigned) {
			Version durableVersion = data->data().getLatestVersion();
			TraceEvent(SevDebug, "ChangeFeedCleanup", data->thisServerID)
			    .detail("FeedID", f.first)
			    .detail("Version", version)
			    .detail("DurableVersion", durableVersion);

			data->changeFeedCleanupDurable[f.first] = durableVersion;

			Key beginClearKey = f.first.withPrefix(persistChangeFeedKeys.begin);
			auto& mLV = data->addVersionToMutationLog(durableVersion);
			data->addMutationToMutationLog(
			    mLV, MutationRef(MutationRef::ClearRange, beginClearKey, keyAfter(beginClearKey)));
			++data->counters.kvSystemClearRanges;
			data->addMutationToMutationLog(mLV,
			                               MutationRef(MutationRef::ClearRange,
			                                           changeFeedDurableKey(f.first, 0),
			                                           changeFeedDurableKey(f.first, version)));

			// We can't actually remove this change feed fully until the mutations clearing its data become durable.
			// If the SS restarted at version R before the clearing mutations became durable at version D (R < D),
			// then the restarted SS would restore the change feed clients would be able to read data and would miss
			// mutations from versions [R, D), up until we got the private mutation triggering the cleanup again.

			auto feed = data->uidChangeFeed.find(f.first);
			if (feed != data->uidChangeFeed.end()) {
				feed->second->updateMetadataVersion(version);
				feed->second->removing = true;
				feed->second->moved(feed->second->range);
				feed->second->newMutations.trigger();
			}

			if (BUGGIFY) {
				data->maybeInjectTargetedRestart(durableVersion);
			}
		} else {
			// if just part of feed's range is moved away
			auto feed = data->uidChangeFeed.find(f.first);
			if (feed != data->uidChangeFeed.end()) {
				feed->second->moved(keys);
			}
		}
	}
}

void changeServerKeys(StorageServer* data,
                      const KeyRangeRef& keys,
                      bool nowAssigned,
                      Version version,
                      ChangeServerKeysContext context,
                      DataMovementReason dataMoveReason,
                      const SSBulkLoadMetadata& bulkLoadInfoForAddingShard) {
	ASSERT(!keys.empty());
	// TraceEvent("ChangeServerKeys", data->thisServerID)
	//     .detail("KeyBegin", keys.begin)
	//     .detail("KeyEnd", keys.end)
	//     .detail("NowAssigned", nowAssigned)
	//     .detail("Version", version)
	//     .detail("Context", changeServerKeysContextName(context));
	validate(data);
	// TODO(alexmiller): Figure out how to selectively enable spammy data distribution events.
	DEBUG_KEY_RANGE(nowAssigned ? "KeysAssigned" : "KeysUnassigned", version, keys, data->thisServerID);

	bool isDifferent = false;
	auto existingShards = data->shards.intersectingRanges(keys);
	for (auto it = existingShards.begin(); it != existingShards.end(); ++it) {
		if (nowAssigned != it->value()->assigned()) {
			isDifferent = true;
			TraceEvent("CSKRangeDifferent", data->thisServerID)
			    .detail("KeyBegin", it->range().begin)
			    .detail("KeyEnd", it->range().end);
			break;
		}
	}
	if (!isDifferent) {
		TraceEvent(SevDebug, "CSKShortCircuit", data->thisServerID)
		    .detail("KeyBegin", keys.begin)
		    .detail("KeyEnd", keys.end);
		return;
	}

	if (nowAssigned) {
		++data->counters.changeServerKeysAssigned;
	} else {
		++data->counters.changeServerKeysUnassigned;
	}

	// Save a backup of the ShardInfo references before we start messing with shards, in order to defer fetchKeys
	// cancellation (and its potential call to removeDataRange()) until shards is again valid
	std::vector<Reference<ShardInfo>> oldShards;
	auto os = data->shards.intersectingRanges(keys);
	for (auto r = os.begin(); r != os.end(); ++r)
		oldShards.push_back(r->value());

	// As addShard (called below)'s documentation requires, reinitialize any overlapping range(s)
	auto ranges = data->shards.getAffectedRangesAfterInsertion(
	    keys, Reference<ShardInfo>()); // null reference indicates the range being changed
	for (int i = 0; i < ranges.size(); i++) {
		if (!ranges[i].value) {
			ASSERT((KeyRangeRef&)ranges[i] == keys); // there shouldn't be any nulls except for the range being inserted
		} else if (ranges[i].value->notAssigned())
			data->addShard(ShardInfo::newNotAssigned(ranges[i]));
		else if (ranges[i].value->isReadable())
			data->addShard(ShardInfo::newReadWrite(ranges[i], data));
		else {
			ASSERT(ranges[i].value->adding);
			data->addShard(ShardInfo::newAdding(
			    data, ranges[i], ranges[i].value->adding->reason, ranges[i].value->getSSBulkLoadMetadata()));
			CODE_PROBE(true, "ChangeServerKeys reFetchKeys");
		}
	}

	// Shard state depends on nowAssigned and whether the data is available (actually assigned in memory or on the
	// disk) up to the given version.  The latter depends on data->newestAvailableVersion, so loop over the ranges
	// of that. SOMEDAY: Could this just use shards?  Then we could explicitly do the removeDataRange here when an
	// adding/transferred shard is cancelled
	auto vr = data->newestAvailableVersion.intersectingRanges(keys);
	std::vector<std::pair<KeyRange, Version>> changeNewestAvailable;
	std::vector<KeyRange> removeRanges;
	std::vector<KeyRange> newEmptyRanges;
	for (auto r = vr.begin(); r != vr.end(); ++r) {
		KeyRangeRef range = keys & r->range();
		bool dataAvailable = r->value() == latestVersion || r->value() >= version;
		// TraceEvent(SevDebug, "CSKRange", data->thisServerID)
		//     .detail("KeyBegin", range.begin)
		//     .detail("KeyEnd", range.end)
		//     .detail("Available", dataAvailable)
		//     .detail("NowAssigned", nowAssigned)
		//     .detail("NewestAvailable", r->value())
		//     .detail("ShardState0", data->shards[range.begin]->debugDescribeState())
		//     .detail("SSBulkLoadMetaData", bulkLoadInfoForAddingShard.toString())
		//     .detail("Context", context);
		if (context == CSK_ASSIGN_EMPTY && !dataAvailable) {
			ASSERT(nowAssigned);
			TraceEvent("ChangeServerKeysAddEmptyRange", data->thisServerID)
			    .detail("Begin", range.begin)
			    .detail("End", range.end);
			newEmptyRanges.push_back(range);
			data->addShard(ShardInfo::newReadWrite(range, data));
		} else if (!nowAssigned) {
			if (dataAvailable) {
				ASSERT(r->value() ==
				       latestVersion); // Not that we care, but this used to be checked instead of dataAvailable
				ASSERT(data->mutableData().getLatestVersion() > version || context == CSK_RESTORE);
				changeNewestAvailable.emplace_back(range, version);
				removeRanges.push_back(range);
			}
			data->addShard(ShardInfo::newNotAssigned(range));
			data->watches.triggerRange(range.begin, range.end);
		} else if (!dataAvailable) {
			// SOMEDAY: Avoid restarting adding/transferred shards
			// bypass fetchkeys; shard is known empty at initial cluster version
			if (version == data->initialClusterVersion - 1) {
				TraceEvent("ChangeServerKeysInitialRange", data->thisServerID)
				    .detail("Begin", range.begin)
				    .detail("End", range.end);
				changeNewestAvailable.emplace_back(range, latestVersion);
				data->addShard(ShardInfo::newReadWrite(range, data));
				setAvailableStatus(data, range, true);
			} else {
				auto& shard = data->shards[range.begin];
				if (!shard->assigned() || shard->keys != range)
					data->addShard(ShardInfo::newAdding(data, range, dataMoveReason, bulkLoadInfoForAddingShard));
			}
		} else {
			changeNewestAvailable.emplace_back(range, latestVersion);
			data->addShard(ShardInfo::newReadWrite(range, data));
		}
	}
	// Update newestAvailableVersion when a shard becomes (un)available (in a separate loop to avoid invalidating vr
	// above)
	for (auto r = changeNewestAvailable.begin(); r != changeNewestAvailable.end(); ++r)
		data->newestAvailableVersion.insert(r->first, r->second);

	if (!nowAssigned)
		data->metrics.notifyNotReadable(keys);

	coalesceShards(data, KeyRangeRef(ranges[0].begin, ranges[ranges.size() - 1].end));

	// Now it is OK to do removeDataRanges, directly and through fetchKeys cancellation (and we have to do so before
	// validate())
	oldShards.clear();
	ranges.clear();
	for (auto r = removeRanges.begin(); r != removeRanges.end(); ++r) {
		removeDataRange(data, data->addVersionToMutationLog(data->data().getLatestVersion()), data->shards, *r);
		setAvailableStatus(data, *r, false);
	}

	// Clear the moving-in empty range, and set it available at the latestVersion.
	for (const auto& range : newEmptyRanges) {
		MutationRef clearRange(MutationRef::ClearRange, range.begin, range.end);
		data->addMutation(data->data().getLatestVersion(),
		                  true,
		                  clearRange,
		                  MutationRefAndCipherKeys(),
		                  range,
		                  data->updateEagerReads);
		data->newestAvailableVersion.insert(range, latestVersion);
		setAvailableStatus(data, range, true);
		++data->counters.kvSystemClearRanges;
	}
	validate(data);

	if (!nowAssigned) {
		cleanUpChangeFeeds(data, keys, version);
	}

	if (data->trackShardAssignmentMinVersion != invalidVersion && version >= data->trackShardAssignmentMinVersion) {
		// data->trackShardAssignmentMinVersion==invalidVersion means trackAssignment stops
		data->shardAssignmentHistory.push_back(std::make_pair(version, keys));
		TraceEvent(SevVerbose, "ShardAssignmentHistoryAdd", data->thisServerID)
		    .detail("Version", version)
		    .detail("Keys", keys)
		    .detail("SSVersion", data->version.get());
	} else {
		data->shardAssignmentHistory.clear();
		TraceEvent(SevVerbose, "ShardAssignmentHistoryClear", data->thisServerID);
	}
}

void changeServerKeysWithPhysicalShards(StorageServer* data,
                                        const KeyRangeRef& keys,
                                        const UID& dataMoveId,
                                        bool nowAssigned,
                                        Version version,
                                        ChangeServerKeysContext context,
                                        EnablePhysicalShardMove enablePSM,
                                        ConductBulkLoad conductBulkLoad) {
	ASSERT(!keys.empty());
	const Severity sevDm = static_cast<Severity>(SERVER_KNOBS->PHYSICAL_SHARD_MOVE_LOG_SEVERITY);
	TraceEvent(SevInfo, "ChangeServerKeysWithPhysicalShards", data->thisServerID)
	    .detail("DataMoveID", dataMoveId)
	    .detail("Range", keys)
	    .detail("NowAssigned", nowAssigned)
	    .detail("Version", version)
	    .detail("PhysicalShardMove", static_cast<bool>(enablePSM))
	    .detail("BulkLoading", static_cast<bool>(conductBulkLoad))
	    .detail("IsTSS", data->isTss())
	    .detail("Context", changeServerKeysContextName(context));

	validate(data);

	DEBUG_KEY_RANGE(nowAssigned ? "KeysAssigned" : "KeysUnassigned", version, keys, data->thisServerID)
	    .detail("DataMoveID", dataMoveId);

	if (nowAssigned) {
		++data->counters.changeServerKeysAssigned;
	} else {
		++data->counters.changeServerKeysUnassigned;
	}

	const uint64_t desiredId = dataMoveId.first();
	const Version cVer = version + 1;
	ASSERT(data->data().getLatestVersion() == cVer);

	// Save a backup of the ShardInfo references before we start messing with shards, in order to defer fetchKeys
	// cancellation (and its potential call to removeDataRange()) until shards is again valid
	std::vector<Reference<ShardInfo>> oldShards;
	auto os = data->shards.intersectingRanges(keys);
	for (auto r = os.begin(); r != os.end(); ++r) {
		oldShards.push_back(r->value());
	}

	auto ranges = data->shards.getAffectedRangesAfterInsertion(
	    keys,
	    Reference<ShardInfo>()); // null reference indicates the range being changed
	std::unordered_map<UID, std::shared_ptr<MoveInShard>> updatedMoveInShards;

	// When TSS is lagging behind, it could see data move conflicts. The conflicting TSS will not recover from error and
	// needs to be removed.
	Severity sev = data->isTss() ? SevWarnAlways : SevError;
	for (int i = 0; i < ranges.size(); i++) {
		const Reference<ShardInfo> currentShard = ranges[i].value;
		const KeyRangeRef currentRange = static_cast<KeyRangeRef>(ranges[i]);
		if (currentShard.isValid()) {
			TraceEvent(sevDm, "OverlappingPhysicalShard", data->thisServerID)
			    .detail("PhysicalShard", currentShard->toStorageServerShard().toString());
		}
		if (!currentShard.isValid()) {
			if (currentRange != keys) {
				TraceEvent(sev, "PhysicalShardStateError")
				    .detail("SubError", "RangeDifferent")
				    .detail("CurrentRange", currentRange)
				    .detail("ModifiedRange", keys)
				    .detail("Assigned", nowAssigned)
				    .detail("DataMoveId", dataMoveId)
				    .detail("Version", version)
				    .detail("InitialVersion", currentShard->version);
				throw data_move_conflict();
			}
		} else if (currentShard->notAssigned()) {
			if (!nowAssigned) {
				TraceEvent(sev, "PhysicalShardStateError")
				    .detail("SubError", "UnassignEmptyRange")
				    .detail("Assigned", nowAssigned)
				    .detail("ModifiedRange", keys)
				    .detail("DataMoveId", dataMoveId)
				    .detail("Version", version)
				    .detail("ConflictingShard", currentShard->shardId)
				    .detail("DesiredShardId", currentShard->desiredShardId)
				    .detail("InitialVersion", currentShard->version);
				throw data_move_conflict();
			}
			StorageServerShard newShard = currentShard->toStorageServerShard();
			newShard.range = currentRange;
			data->addShard(ShardInfo::newShard(data, newShard));
			TraceEvent(sevDm, "SSSplitShardNotAssigned", data->thisServerID)
			    .detail("Range", keys)
			    .detail("NowAssigned", nowAssigned)
			    .detail("Version", cVer)
			    .detail("ResultingShard", newShard.toString());
		} else if (currentShard->isReadable()) {
			StorageServerShard newShard = currentShard->toStorageServerShard();
			newShard.range = currentRange;
			data->addShard(ShardInfo::newShard(data, newShard));
			TraceEvent(sevDm, "SSSplitShardReadable", data->thisServerID)
			    .detail("Range", keys)
			    .detail("NowAssigned", nowAssigned)
			    .detail("Version", cVer)
			    .detail("ResultingShard", newShard.toString());
		} else if (currentShard->adding) {
			if (nowAssigned) {
				TraceEvent(sev, "PhysicalShardStateError")
				    .detail("SubError", "UpdateAddingShard")
				    .detail("Assigned", nowAssigned)
				    .detail("ModifiedRange", keys)
				    .detail("DataMoveId", dataMoveId)
				    .detail("Version", version)
				    .detail("ConflictingShard", currentShard->shardId)
				    .detail("DesiredShardId", currentShard->desiredShardId)
				    .detail("InitialVersion", currentShard->version);
				throw data_move_conflict();
			}
			StorageServerShard newShard = currentShard->toStorageServerShard();
			newShard.range = currentRange;
			data->addShard(ShardInfo::newShard(data, newShard));
			TraceEvent(sevDm, "SSSplitShardAdding", data->thisServerID)
			    .detail("Range", keys)
			    .detail("NowAssigned", nowAssigned)
			    .detail("Version", cVer)
			    .detail("ResultingShard", newShard.toString());
		} else if (currentShard->moveInShard) {
			if (nowAssigned) {
				TraceEvent(sev, "PhysicalShardStateError")
				    .detail("SubError", "UpdateMoveInShard")
				    .detail("Assigned", nowAssigned)
				    .detail("ModifiedRange", keys)
				    .detail("DataMoveId", dataMoveId)
				    .detail("Version", version)
				    .detail("ConflictingShard", currentShard->shardId)
				    .detail("DesiredShardId", currentShard->desiredShardId)
				    .detail("InitialVersion", currentShard->version);
				throw data_move_conflict();
			}
			currentShard->moveInShard->cancel();
			updatedMoveInShards.emplace(currentShard->moveInShard->id(), currentShard->moveInShard);
			StorageServerShard newShard = currentShard->toStorageServerShard();
			newShard.range = currentRange;
			data->addShard(ShardInfo::newShard(data, newShard));
			TraceEvent(SevVerbose, "SSCancelMoveInShard", data->thisServerID)
			    .detail("Range", keys)
			    .detail("NowAssigned", nowAssigned)
			    .detail("Version", cVer)
			    .detail("ResultingShard", newShard.toString());
		} else {
			ASSERT(false);
		}
	}

	auto vr = data->shards.intersectingRanges(keys);
	std::vector<std::pair<KeyRange, Version>> changeNewestAvailable;
	std::vector<KeyRange> removeRanges;
	std::vector<KeyRange> newEmptyRanges;
	std::vector<StorageServerShard> updatedShards;
	int totalAssignedAtVer = 0;
	for (auto r = vr.begin(); r != vr.end(); ++r) {
		KeyRangeRef range = keys & r->range();
		const bool dataAvailable = r->value()->isReadable();
		TraceEvent(sevDm, "CSKPhysicalShard", data->thisServerID)
		    .detail("Range", range)
		    .detail("ExistingShardRange", r->range())
		    .detail("Available", dataAvailable)
		    .detail("NowAssigned", nowAssigned)
		    .detail("ShardState", r->value()->debugDescribeState());
		ASSERT(keys.contains(r->range()));
		if (context == CSK_ASSIGN_EMPTY && !dataAvailable) {
			ASSERT(nowAssigned);
			TraceEvent(sevDm, "ChangeServerKeysAddEmptyRange", data->thisServerID)
			    .detail("Range", range)
			    .detail("Version", cVer);
			newEmptyRanges.push_back(range);
			updatedShards.emplace_back(range, cVer, desiredId, desiredId, StorageServerShard::ReadWrite);
			if (data->physicalShards.find(desiredId) == data->physicalShards.end()) {
				data->pendingAddRanges[cVer].emplace_back(desiredId, range);
			}
		} else if (!nowAssigned) {
			if (dataAvailable) {
				ASSERT(data->newestAvailableVersion[range.begin] ==
				       latestVersion); // Not that we care, but this used to be checked instead of dataAvailable
				ASSERT(data->mutableData().getLatestVersion() > version || context == CSK_RESTORE);
				changeNewestAvailable.emplace_back(range, version);
				removeRanges.push_back(range);
			}
			if (r->value()->moveInShard) {
				r->value()->moveInShard->cancel();
				// This is an overkill, and is necessary only when psm has written data to `range`; Also we don't need
				// to clean up the PTree.
				removeRanges.push_back(range);
			}
			updatedShards.push_back(StorageServerShard::notAssigned(range, cVer));
			data->pendingRemoveRanges[cVer].push_back(range);
			data->watches.triggerRange(range.begin, range.end);
			TraceEvent(sevDm, "SSUnassignShard", data->thisServerID)
			    .detail("Range", range)
			    .detail("NowAssigned", nowAssigned)
			    .detail("Version", cVer)
			    .detail("NewShard", updatedShards.back().toString());
		} else if (!dataAvailable) {
			if (version == data->initialClusterVersion - 1) {
				TraceEvent(sevDm, "CSKWithPhysicalShardsSeedRange", data->thisServerID)
				    .detail("ShardID", desiredId)
				    .detail("Range", range);
				changeNewestAvailable.emplace_back(range, latestVersion);
				updatedShards.push_back(
				    StorageServerShard(range, version, desiredId, desiredId, StorageServerShard::ReadWrite));
				setAvailableStatus(data, range, true);
				// Note: The initial range is available, however, the shard won't be created in the storage engine
				// until version is committed.
				data->pendingAddRanges[cVer].emplace_back(desiredId, range);
				TraceEvent(sevDm, "SSInitialShard", data->thisServerID)
				    .detail("Range", range)
				    .detail("NowAssigned", nowAssigned)
				    .detail("Version", cVer)
				    .detail("NewShard", updatedShards.back().toString());
			} else {
				auto& shard = data->shards[range.begin];
				if (!shard->assigned()) {
					if (enablePSM) {
						std::shared_ptr<MoveInShard> moveInShard =
						    data->getMoveInShard(dataMoveId, cVer, conductBulkLoad);
						moveInShard->addRange(range);
						updatedMoveInShards.emplace(moveInShard->id(), moveInShard);
						updatedShards.push_back(StorageServerShard(
						    range, cVer, desiredId, desiredId, StorageServerShard::MovingIn, moveInShard->id()));
					} else {
						updatedShards.push_back(
						    StorageServerShard(range, cVer, desiredId, desiredId, StorageServerShard::Adding));
						data->pendingAddRanges[cVer].emplace_back(desiredId, range);
					}
					data->newestDirtyVersion.insert(range, cVer);
					TraceEvent(sevDm, "SSAssignShard", data->thisServerID)
					    .detail("Range", range)
					    .detail("NowAssigned", nowAssigned)
					    .detail("Version", cVer)
					    .detail("TotalAssignedAtVer", ++totalAssignedAtVer)
					    .detail("ConductBulkLoad", conductBulkLoad)
					    .detail("NewShard", updatedShards.back().toString());
				} else {
					ASSERT(shard->adding != nullptr || shard->moveInShard != nullptr);
					if (shard->desiredShardId != desiredId) {
						TraceEvent(SevWarnAlways, "CSKConflictingMoveInShards", data->thisServerID)
						    .detail("DataMoveID", dataMoveId)
						    .detail("Range", range)
						    .detailf("TargetShard", "%016llx", desiredId)
						    .detailf("CurrentShard", "%016llx", shard->desiredShardId)
						    .detail("IsTSS", data->isTss())
						    .detail("Version", cVer);
						if (data->isTss() && g_network->isSimulated()) {
							// Tss data move conflicts are expected in simulation, and can be safely ignored
							// by restarting the server.
							throw please_reboot();
						} else {
							throw data_move_conflict();
						}
					} else {
						TraceEvent(SevInfo, "CSKMoveInToSameShard", data->thisServerID)
						    .detail("DataMoveID", dataMoveId)
						    .detailf("TargetShard", "%016llx", desiredId)
						    .detail("MoveRange", keys)
						    .detail("Range", range)
						    .detail("ExistingShardRange", shard->keys)
						    .detail("ShardDebugString", shard->debugDescribeState())
						    .detail("Version", cVer);
						if (context == CSK_FALL_BACK) {
							updatedShards.push_back(
							    StorageServerShard(range, cVer, desiredId, desiredId, StorageServerShard::Adding));
							// Physical shard move fall back happens if and only if the data move is failed to get the
							// checkpoint. However, this case never happens the bulkload. So, the bulkload does not
							// support fall back.
							ASSERT(!conductBulkLoad); // TODO(BulkLoad): remove this assert
							data->pendingAddRanges[cVer].emplace_back(desiredId, range);
							data->newestDirtyVersion.insert(range, cVer);
							// TODO: removeDataRange if the moveInShard has written to the kvs.
						}
					}
				}
			}
		} else {
			updatedShards.push_back(StorageServerShard(
			    range, cVer, data->shards[range.begin]->shardId, desiredId, StorageServerShard::ReadWrite));
			changeNewestAvailable.emplace_back(range, latestVersion);
			TraceEvent(sevDm, "SSAssignShardAlreadyAvailable", data->thisServerID)
			    .detail("Range", range)
			    .detail("NowAssigned", nowAssigned)
			    .detail("Version", cVer)
			    .detail("NewShard", updatedShards.back().toString());
		}
	}

	for (const auto& shard : updatedShards) {
		data->addShard(ShardInfo::newShard(data, shard));
		updateStorageShard(data, shard);
	}
	auto& mLV = data->addVersionToMutationLog(data->data().getLatestVersion());
	for (const auto& [id, shard] : updatedMoveInShards) {
		data->addMutationToMutationLog(
		    mLV, MutationRef(MutationRef::SetValue, persistMoveInShardKey(id), moveInShardValue(*shard->meta)));
	}

	// Update newestAvailableVersion when a shard becomes (un)available (in a separate loop to avoid invalidating vr
	// above)
	for (auto r = changeNewestAvailable.begin(); r != changeNewestAvailable.end(); ++r) {
		data->newestAvailableVersion.insert(r->first, r->second);
	}

	if (!nowAssigned) {
		data->metrics.notifyNotReadable(keys);
	}

	coalescePhysicalShards(data, KeyRangeRef(ranges[0].begin, ranges[ranges.size() - 1].end));

	// Now it is OK to do removeDataRanges, directly and through fetchKeys cancellation (and we have to do so before
	// validate())
	oldShards.clear();
	ranges.clear();
	for (auto r = removeRanges.begin(); r != removeRanges.end(); ++r) {
		removeDataRange(data, data->addVersionToMutationLog(data->data().getLatestVersion()), data->shards, *r);
		setAvailableStatus(data, *r, false);
	}

	// Clear the moving-in empty range, and set it available at the latestVersion.
	for (const auto& range : newEmptyRanges) {
		MutationRef clearRange(MutationRef::ClearRange, range.begin, range.end);
		data->addMutation(data->data().getLatestVersion(),
		                  true,
		                  clearRange,
		                  MutationRefAndCipherKeys(),
		                  range,
		                  data->updateEagerReads);
		data->newestAvailableVersion.insert(range, latestVersion);
		setAvailableStatus(data, range, true);
		++data->counters.kvSystemClearRanges;
	}
	validate(data);

	// find any change feeds that no longer have shards on this server, and clean them up
	if (!nowAssigned) {
		cleanUpChangeFeeds(data, keys, version);
	}

	if (data->trackShardAssignmentMinVersion != invalidVersion && version >= data->trackShardAssignmentMinVersion) {
		// data->trackShardAssignmentMinVersion==invalidVersion means trackAssignment stops
		data->shardAssignmentHistory.push_back(std::make_pair(version, keys));
		TraceEvent(SevVerbose, "ShardAssignmentHistoryAdd", data->thisServerID)
		    .detail("Version", version)
		    .detail("Keys", keys)
		    .detail("SSVersion", data->version.get());
	} else {
		data->shardAssignmentHistory.clear();
		TraceEvent(SevVerbose, "ShardAssignmentHistoryClear", data->thisServerID);
	}
}

void rollback(StorageServer* data, Version rollbackVersion, Version nextVersion) {
	CODE_PROBE(true, "call to shard rollback");
	DEBUG_KEY_RANGE("Rollback", rollbackVersion, allKeys, data->thisServerID);

	// We used to do a complicated dance to roll back in MVCC history.  It's much simpler, and more testable,
	// to simply restart the storage server actor and restore from the persistent disk state, and then roll
	// forward from the TLog's history.  It's not quite as efficient, but we rarely have to do this in practice.

	// FIXME: This code is relying for liveness on an undocumented property of the log system implementation: that
	// after a rollback the rolled back versions will eventually be missing from the peeked log.  A more
	// sophisticated approach would be to make the rollback range durable and, after reboot, skip over those
	// versions if they appear in peek results.

	throw please_reboot();
}

void StorageServer::addMutation(Version version,
                                bool fromFetch,
                                MutationRef const& mutation,
                                MutationRefAndCipherKeys const& encryptedMutation,
                                KeyRangeRef const& shard,
                                UpdateEagerReadInfo* eagerReads) {
	MutationRef expanded = mutation;
	MutationRef
	    nonExpanded; // need to keep non-expanded but atomic converted version of clear mutations for change feeds
	auto& mLog = addVersionToMutationLog(version);

	if (!convertAtomicOp(expanded, data(), eagerReads, mLog.arena())) {
		return;
	}
	if (expanded.type == MutationRef::ClearRange) {
		nonExpanded = expanded;
		expandClear(expanded, data(), eagerReads, shard.end);
	}
	expanded = addMutationToMutationLog(mLog, expanded);
	DEBUG_MUTATION("applyMutation", version, expanded, thisServerID)
	    .detail("ShardBegin", shard.begin)
	    .detail("ShardEnd", shard.end);

	if (!fromFetch) {
		// have to do change feed before applyMutation because nonExpanded wasn't copied into the mutation log
		// arena, and thus would go out of scope if it wasn't copied into the change feed arena

		MutationRefAndCipherKeys encrypt = encryptedMutation;
		if (encrypt.mutation.isEncrypted() && mutation.type != MutationRef::SetValue &&
		    mutation.type != MutationRef::ClearRange) {
			encrypt.mutation = expanded.encrypt(encrypt.cipherKeys, mLog.arena(), BlobCipherMetrics::TLOG);
		}

		applyChangeFeedMutation(
		    this, expanded.type == MutationRef::ClearRange ? nonExpanded : expanded, encrypt, version, shard);
	}
	applyMutation(this, expanded, mLog.arena(), mutableData(), version);

	// printf("\nSSUpdate: Printing versioned tree after applying mutation\n");
	// mutableData().printTree(version);
}

struct OrderByVersion {
	bool operator()(const VerUpdateRef& a, const VerUpdateRef& b) {
		if (a.version != b.version)
			return a.version < b.version;
		if (a.isPrivateData != b.isPrivateData)
			return a.isPrivateData;
		return false;
	}
};

class StorageUpdater {
public:
	StorageUpdater()
	  : currentVersion(invalidVersion), fromVersion(invalidVersion), restoredVersion(invalidVersion),
	    processedStartKey(false), processedCacheStartKey(false) {}
	StorageUpdater(Version fromVersion, Version restoredVersion)
	  : currentVersion(fromVersion), fromVersion(fromVersion), restoredVersion(restoredVersion),
	    processedStartKey(false), processedCacheStartKey(false) {}

	void applyMutation(StorageServer* data,
	                   MutationRef const& m,
	                   MutationRefAndCipherKeys const& encryptedMutation,
	                   Version ver,
	                   bool fromFetch) {
		//TraceEvent("SSNewVersion", data->thisServerID).detail("VerWas", data->mutableData().latestVersion).detail("ChVer", ver);

		if (currentVersion != ver) {
			fromVersion = currentVersion;
			currentVersion = ver;
			data->mutableData().createNewVersion(ver);
		}

		if (m.param1.startsWith(systemKeys.end)) {
			if ((m.type == MutationRef::SetValue) && m.param1.substr(1).startsWith(storageCachePrefix)) {
				applyPrivateCacheData(data, m);
			} else if ((m.type == MutationRef::SetValue) && m.param1.substr(1).startsWith(checkpointPrefix)) {
				handleCheckpointPrivateMutation(data, m, ver);
			} else {
				applyPrivateData(data, ver, m);
			}
		} else {
			if (MUTATION_TRACKING_ENABLED) {
				DEBUG_MUTATION("SSUpdateMutation", ver, m, data->thisServerID).detail("FromFetch", fromFetch);
			}
			splitMutation(data, data->shards, m, encryptedMutation, ver, fromFetch);
		}

		if (data->otherError.getFuture().isReady())
			data->otherError.getFuture().get();
	}

	Version currentVersion;

private:
	Version fromVersion;
	Version restoredVersion;

	KeyRef startKey;
	bool nowAssigned;
	bool emptyRange;
	EnablePhysicalShardMove enablePSM = EnablePhysicalShardMove::False;
	DataMovementReason dataMoveReason = DataMovementReason::INVALID;
	UID dataMoveId;
	bool processedStartKey;
	ConductBulkLoad conductBulkLoad = ConductBulkLoad::False;

	KeyRef cacheStartKey;
	bool processedCacheStartKey;

	void applyPrivateData(StorageServer* data, Version ver, MutationRef const& m) {
		TraceEvent(SevDebug, "SSPrivateMutation", data->thisServerID).detail("Mutation", m).detail("Version", ver);

		if (processedStartKey) {
			// Because of the implementation of the krm* functions, we expect changes in pairs, [begin,end)
			// We can also ignore clearRanges, because they are always accompanied by such a pair of sets with the
			// same keys
			ASSERT(m.type == MutationRef::SetValue && m.param1.startsWith(data->sk));
			KeyRangeRef keys(startKey.removePrefix(data->sk), m.param1.removePrefix(data->sk));

			// ignore data movements for tss in quarantine
			if (!data->isTSSInQuarantine()) {
				const ChangeServerKeysContext context = emptyRange ? CSK_ASSIGN_EMPTY : CSK_UPDATE;
				TraceEvent(SevDebug, "SSSetAssignedStatus", data->thisServerID)
				    .detail("SSShardAware", data->shardAware)
				    .detail("Range", keys)
				    .detail("NowAssigned", nowAssigned)
				    .detail("Version", ver)
				    .detail("EnablePSM", enablePSM)
				    .detail("DataMoveId", dataMoveId.toString())
				    .detail("ConductBulkLoad", conductBulkLoad);
				if (data->shardAware) {
					setAssignedStatus(data, keys, nowAssigned);
					changeServerKeysWithPhysicalShards(
					    data, keys, dataMoveId, nowAssigned, currentVersion - 1, context, enablePSM, conductBulkLoad);
				} else {
					// add changes in shard assignment to the mutation log
					setAssignedStatus(data, keys, nowAssigned);
					if (conductBulkLoad) {
						ASSERT(!emptyRange && dataMoveIdIsValidForBulkLoad(dataMoveId));
						ASSERT(nowAssigned);
					}
					if (!nowAssigned) {
						ASSERT(!conductBulkLoad);
					}
					SSBulkLoadMetadata bulkLoadMetadata(dataMoveId, conductBulkLoad);
					setRangeBasedBulkLoadStatus(data, keys, bulkLoadMetadata);

					// The changes for version have already been received (and are being processed now).  We need to
					// fetch the data for change.version-1 (changes from versions < change.version) If emptyRange,
					// treat the shard as empty, see removeKeysFromFailedServer() for more details about this
					// scenario.
					changeServerKeys(
					    data, keys, nowAssigned, currentVersion - 1, context, dataMoveReason, bulkLoadMetadata);
				}
			}

			processedStartKey = false;
		} else if (m.type == MutationRef::SetValue && m.param1.startsWith(data->sk)) {
			// Because of the implementation of the krm* functions, we expect changes in pairs, [begin,end)
			// We can also ignore clearRanges, because they are always accompanied by such a pair of sets with the same
			// keys
			startKey = m.param1;
			DataMoveType dataMoveType = DataMoveType::LOGICAL;
			dataMoveReason = DataMovementReason::INVALID;
			decodeServerKeysValue(m.param2, nowAssigned, emptyRange, dataMoveType, dataMoveId, dataMoveReason);
			if (dataMoveType != DataMoveType::LOGICAL && dataMoveType != DataMoveType::LOGICAL_BULKLOAD &&
			    !data->shardAware) {
				TraceEvent(SevWarnAlways, "SSNotSupportDataMoveType", data->thisServerID)
				    .detail("DataMoveType", dataMoveType)
				    .detail("KVStoreType", data->storage.getKeyValueStoreType())
				    .detail("DataMoveId", dataMoveId.toString());
				if (dataMoveType == DataMoveType::PHYSICAL || dataMoveType == DataMoveType::PHYSICAL_EXP) {
					dataMoveType = DataMoveType::LOGICAL;
				} else {
					ASSERT(dataMoveType == DataMoveType::PHYSICAL_BULKLOAD);
					dataMoveType = DataMoveType::LOGICAL_BULKLOAD;
				}
			}
			enablePSM = EnablePhysicalShardMove(dataMoveType == DataMoveType::PHYSICAL ||
			                                    (dataMoveType == DataMoveType::PHYSICAL_EXP && data->isTss()) ||
			                                    dataMoveType == DataMoveType::PHYSICAL_BULKLOAD);
			conductBulkLoad = ConductBulkLoad(dataMoveType == DataMoveType::LOGICAL_BULKLOAD ||
			                                  dataMoveType == DataMoveType::PHYSICAL_BULKLOAD);
			// conductBulkLoad represents the intention of the data move, which is ONLY used to suggest whether needs to
			// read data move metadata to get the bulk load task from system metadata. We rely on the existence of data
			// move metadata to decide if the SS should do bulk load task, rather than relying on the data move ID.
			if (conductBulkLoad && (dataMoveId == anonymousShardId || !dataMoveId.isValid())) {
				// If conductBulkLoad == true but dataMoveId is not usable, SS should ignore the request by setting the
				// conductBulkLoad to false. Then, a normal data move is triggered.
				TraceEvent(SevError, "SSBulkLoadTaskDataMoveIdInvalid", data->thisServerID)
				    .detail("Message",
				            "A bulkload request is converted to a normal data move because the data move id is either "
				            "anonymousShardId or invalid. Please check DD setting to see if the bulkload dependency is "
				            "correctly set.")
				    .detail("DataMoveType", dataMoveType)
				    .detail("KVStoreType", data->storage.getKeyValueStoreType())
				    .detail("DataMoveId", dataMoveId.toString());
				conductBulkLoad = ConductBulkLoad::False;
			}
			processedStartKey = true;
		} else if (m.type == MutationRef::SetValue && m.param1 == lastEpochEndPrivateKey) {
			// lastEpochEnd transactions are guaranteed by the master to be alone in their own batch (version)
			// That means we don't have to worry about the impact on changeServerKeys
			// ASSERT( /*isFirstVersionUpdateFromTLog && */!std::next(it) );

			Version rollbackVersion;
			BinaryReader br(m.param2, Unversioned());
			br >> rollbackVersion;

			if (rollbackVersion < fromVersion && rollbackVersion > restoredVersion) {
				CODE_PROBE(true, "ShardApplyPrivateData shard rollback");
				TraceEvent(SevWarn, "Rollback", data->thisServerID)
				    .detail("FromVersion", fromVersion)
				    .detail("ToVersion", rollbackVersion)
				    .detail("AtVersion", currentVersion)
				    .detail("RestoredVersion", restoredVersion)
				    .detail("StorageVersion", data->storageVersion());
				ASSERT(rollbackVersion >= data->storageVersion());
				rollback(data, rollbackVersion, currentVersion);
			} else {
				TraceEvent(SevDebug, "RollbackSkip", data->thisServerID)
				    .detail("FromVersion", fromVersion)
				    .detail("ToVersion", rollbackVersion)
				    .detail("AtVersion", currentVersion)
				    .detail("RestoredVersion", restoredVersion)
				    .detail("StorageVersion", data->storageVersion());
			}
			for (auto& it : data->uidChangeFeed) {
				if (!it.second->removing && currentVersion < it.second->stopVersion) {
					it.second->mutations.push_back(EncryptedMutationsAndVersionRef(currentVersion, rollbackVersion));
					it.second->mutations.back().mutations.push_back_deep(it.second->mutations.back().arena(), m);
					data->currentChangeFeeds.insert(it.first);
				}
			}

			data->recoveryVersionSkips.emplace_back(rollbackVersion, currentVersion - rollbackVersion);
		} else if (m.type == MutationRef::SetValue && m.param1 == killStoragePrivateKey) {
			TraceEvent("StorageServerWorkerRemoved", data->thisServerID).detail("Reason", "KillStorage");
			throw worker_removed();
		} else if ((m.type == MutationRef::SetValue || m.type == MutationRef::ClearRange) &&
		           m.param1.substr(1).startsWith(serverTagPrefix)) {
			UID serverTagKey = decodeServerTagKey(m.param1.substr(1));
			bool matchesThisServer = serverTagKey == data->thisServerID;
			bool matchesTssPair = data->isTss() ? serverTagKey == data->tssPairID.get() : false;
			// Remove SS if another SS is now assigned our tag, or this server was removed by deleting our tag entry
			// Since TSS don't have tags, they check for their pair's tag. If a TSS is in quarantine, it will stick
			// around until its pair is removed or it is finished quarantine.
			if ((m.type == MutationRef::SetValue &&
			     ((!data->isTss() && !matchesThisServer) || (data->isTss() && !matchesTssPair))) ||
			    (m.type == MutationRef::ClearRange &&
			     ((!data->isTSSInQuarantine() && matchesThisServer) || (data->isTss() && matchesTssPair)))) {
				TraceEvent("StorageServerWorkerRemoved", data->thisServerID)
				    .detail("Reason", "ServerTag")
				    .detail("MutationType", getTypeString(m.type))
				    .detail("TagMatches", matchesThisServer)
				    .detail("IsTSS", data->isTss());
				throw worker_removed();
			}
			if (!data->isTss() && m.type == MutationRef::ClearRange && data->ssPairID.present() &&
			    serverTagKey == data->ssPairID.get()) {
				data->clearSSWithTssPair();
				// Add ss pair id change to mutation log to make durable
				auto& mLV = data->addVersionToMutationLog(data->data().getLatestVersion());
				data->addMutationToMutationLog(
				    mLV, MutationRef(MutationRef::ClearRange, persistSSPairID, keyAfter(persistSSPairID)));
			}
		} else if (m.type == MutationRef::SetValue && m.param1 == rebootWhenDurablePrivateKey) {
			data->rebootAfterDurableVersion = currentVersion;
			TraceEvent("RebootWhenDurableSet", data->thisServerID)
			    .detail("DurableVersion", data->durableVersion.get())
			    .detail("RebootAfterDurableVersion", data->rebootAfterDurableVersion);
		} else if (m.type == MutationRef::SetValue && m.param1 == primaryLocalityPrivateKey) {
			data->primaryLocality = BinaryReader::fromStringRef<int8_t>(m.param2, Unversioned());
			auto& mLV = data->addVersionToMutationLog(data->data().getLatestVersion());
			data->addMutationToMutationLog(mLV, MutationRef(MutationRef::SetValue, persistPrimaryLocality, m.param2));
		} else if (m.type == MutationRef::SetValue && m.param1.startsWith(changeFeedPrivatePrefix)) {
			Key changeFeedId = m.param1.removePrefix(changeFeedPrivatePrefix);
			KeyRange changeFeedRange;
			Version popVersion;
			ChangeFeedStatus status;
			std::tie(changeFeedRange, popVersion, status) = decodeChangeFeedValue(m.param2);
			auto feed = data->uidChangeFeed.find(changeFeedId);

			TraceEvent(SevDebug, "ChangeFeedPrivateMutation", data->thisServerID)
			    .detail("FeedID", changeFeedId)
			    .detail("Range", changeFeedRange)
			    .detail("Version", currentVersion)
			    .detail("PopVersion", popVersion)
			    .detail("Status", status);

			// Because of data moves, we can get mutations operating on a change feed we don't yet know about,
			// because the metadata fetch hasn't started yet
			bool createdFeed = false;
			bool popMutationLog = false;
			bool addMutationToLog = false;
			if (feed == data->uidChangeFeed.end() && status != ChangeFeedStatus::CHANGE_FEED_DESTROY) {
				createdFeed = true;

				Reference<ChangeFeedInfo> changeFeedInfo(new ChangeFeedInfo());
				changeFeedInfo->range = changeFeedRange;
				changeFeedInfo->id = changeFeedId;
				if (status == ChangeFeedStatus::CHANGE_FEED_CREATE && popVersion == invalidVersion) {
					// for a create, the empty version should be now, otherwise it will be set in a later pop
					changeFeedInfo->emptyVersion = currentVersion - 1;
				} else {
					CODE_PROBE(true, "SS got non-create change feed private mutation before move created its metadata");
					changeFeedInfo->emptyVersion = invalidVersion;
				}
				changeFeedInfo->metadataCreateVersion = currentVersion;
				data->uidChangeFeed[changeFeedId] = changeFeedInfo;

				feed = data->uidChangeFeed.find(changeFeedId);
				ASSERT(feed != data->uidChangeFeed.end());

				TraceEvent(SevDebug, "AddingChangeFeed", data->thisServerID)
				    .detail("FeedID", changeFeedId)
				    .detail("Range", changeFeedRange)
				    .detail("EmptyVersion", feed->second->emptyVersion);

				auto rs = data->keyChangeFeed.modify(changeFeedRange);
				for (auto r = rs.begin(); r != rs.end(); ++r) {
					r->value().push_back(changeFeedInfo);
				}
				data->keyChangeFeed.coalesce(changeFeedRange.contents());
			} else if (feed != data->uidChangeFeed.end() && feed->second->removing && !feed->second->destroyed &&
			           status != ChangeFeedStatus::CHANGE_FEED_DESTROY) {
				// Because we got a private mutation for this change feed, the feed must have moved back after being
				// moved away. Normally we would later find out about this via a fetch, but in the particular case
				// where the private mutation is the creation of the change feed, and the following race occurred,
				// we must refresh it here:
				// 1. This SS found out about the feed from a fetch, from a SS with a higher version that already
				// got the feed create mutation
				// 2. The shard was moved away
				// 3. The shard was moved back, and this SS fetched change feed metadata from a different SS that
				// did not yet receive the private mutation, so the feed was not refreshed
				// 4. This SS gets the private mutation, the feed is still marked as removing
				TraceEvent(SevDebug, "ResetChangeFeedInfoFromPrivateMutation", data->thisServerID)
				    .detail("FeedID", changeFeedId)
				    .detail("Range", changeFeedRange)
				    .detail("Version", currentVersion);

				CODE_PROBE(true, "private mutation for feed scheduled for deletion! Un-mark it as removing");

				feed->second->removing = false;
				addMutationToLog = true;
				// reset fetch versions because everything previously fetched was cleaned up
				feed->second->fetchVersion = invalidVersion;
				feed->second->durableFetchVersion = NotifiedVersion();
			}
			if (feed != data->uidChangeFeed.end()) {
				feed->second->updateMetadataVersion(currentVersion);
			}

			if (popVersion != invalidVersion && status != ChangeFeedStatus::CHANGE_FEED_DESTROY) {
				// pop the change feed at pop version, no matter what state it is in
				if (popVersion - 1 > feed->second->emptyVersion) {
					feed->second->emptyVersion = popVersion - 1;
					while (!feed->second->mutations.empty() && feed->second->mutations.front().version < popVersion) {
						feed->second->mutations.pop_front();
					}
					if (feed->second->storageVersion != invalidVersion) {
						++data->counters.kvSystemClearRanges;
						// do this clear in the mutation log, as we want it to be committed consistently with the
						// popVersion update
						popMutationLog = true;
						if (popVersion > feed->second->storageVersion) {
							feed->second->storageVersion = invalidVersion;
							feed->second->durableVersion = invalidVersion;
						}
					}
					if (!feed->second->destroyed) {
						// if feed is destroyed, adding an extra mutation here would re-create it if SS restarted
						addMutationToLog = true;
					}
				}

			} else if (status == ChangeFeedStatus::CHANGE_FEED_CREATE && createdFeed) {
				TraceEvent(SevDebug, "CreatingChangeFeed", data->thisServerID)
				    .detail("FeedID", changeFeedId)
				    .detail("Range", changeFeedRange)
				    .detail("Version", currentVersion);
				// no-op, already created metadata
				addMutationToLog = true;
			}
			if (status == ChangeFeedStatus::CHANGE_FEED_STOP && currentVersion < feed->second->stopVersion) {
				TraceEvent(SevDebug, "StoppingChangeFeed", data->thisServerID)
				    .detail("FeedID", changeFeedId)
				    .detail("Range", changeFeedRange)
				    .detail("Version", currentVersion);
				feed->second->stopVersion = currentVersion;
				addMutationToLog = true;
			}
			if (status == ChangeFeedStatus::CHANGE_FEED_DESTROY && !createdFeed && feed != data->uidChangeFeed.end()) {
				TraceEvent(SevDebug, "DestroyingChangeFeed", data->thisServerID)
				    .detail("FeedID", changeFeedId)
				    .detail("Range", changeFeedRange)
				    .detail("Version", currentVersion);
				Key beginClearKey = changeFeedId.withPrefix(persistChangeFeedKeys.begin);
				Version cleanupVersion = data->data().getLatestVersion();
				auto& mLV = data->addVersionToMutationLog(cleanupVersion);
				data->addMutationToMutationLog(
				    mLV, MutationRef(MutationRef::ClearRange, beginClearKey, keyAfter(beginClearKey)));
				++data->counters.kvSystemClearRanges;
				data->addMutationToMutationLog(mLV,
				                               MutationRef(MutationRef::ClearRange,
				                                           changeFeedDurableKey(feed->second->id, 0),
				                                           changeFeedDurableKey(feed->second->id, currentVersion)));
				++data->counters.kvSystemClearRanges;

				feed->second->destroy(currentVersion);
				data->changeFeedCleanupDurable[feed->first] = cleanupVersion;

				if (BUGGIFY) {
					data->maybeInjectTargetedRestart(cleanupVersion);
				}
			}

			if (status == ChangeFeedStatus::CHANGE_FEED_DESTROY) {
				for (auto& it : data->changeFeedDestroys) {
					it.second.send(changeFeedId);
				}
			}

			if (addMutationToLog) {
				Version logV = data->data().getLatestVersion();
				auto& mLV = data->addVersionToMutationLog(logV);
				data->addMutationToMutationLog(
				    mLV,
				    MutationRef(MutationRef::SetValue,
				                persistChangeFeedKeys.begin.toString() + changeFeedId.toString(),
				                changeFeedSSValue(feed->second->range,
				                                  feed->second->emptyVersion + 1,
				                                  feed->second->stopVersion,
				                                  feed->second->metadataVersion)));
				if (popMutationLog) {
					++data->counters.kvSystemClearRanges;
					data->addMutationToMutationLog(mLV,
					                               MutationRef(MutationRef::ClearRange,
					                                           changeFeedDurableKey(feed->second->id, 0),
					                                           changeFeedDurableKey(feed->second->id, popVersion)));
				}
				if (BUGGIFY) {
					data->maybeInjectTargetedRestart(logV);
				}
			}
		} else if ((m.type == MutationRef::SetValue || m.type == MutationRef::ClearRange) &&
		           m.param1.startsWith(TenantMetadata::tenantMapPrivatePrefix())) {
			if (m.type == MutationRef::SetValue) {
				TenantName tenantName = m.param1.removePrefix(TenantMetadata::tenantMapPrivatePrefix());
				TenantMapEntry entry = TenantMapEntry::decode(m.param2);
				data->insertTenant(entry, currentVersion, true);
			} else if (m.type == MutationRef::ClearRange) {
				data->clearTenants(m.param1.removePrefix(TenantMetadata::tenantMapPrivatePrefix()),
				                   m.param2.removePrefix(TenantMetadata::tenantMapPrivatePrefix()),
				                   currentVersion);
			}
		} else if (m.param1.substr(1).startsWith(tssMappingKeys.begin) &&
		           (m.type == MutationRef::SetValue || m.type == MutationRef::ClearRange)) {
			if (!data->isTss()) {
				UID ssId = TupleCodec<UID>::unpack(m.param1.substr(1).removePrefix(tssMappingKeys.begin));
				ASSERT(ssId == data->thisServerID);
				// Add ss pair id change to mutation log to make durable
				auto& mLV = data->addVersionToMutationLog(data->data().getLatestVersion());
				if (m.type == MutationRef::SetValue) {
					UID tssId = TupleCodec<UID>::unpack(m.param2);
					data->setSSWithTssPair(tssId);
					data->addMutationToMutationLog(mLV,
					                               MutationRef(MutationRef::SetValue,
					                                           persistSSPairID,
					                                           BinaryWriter::toValue(tssId, Unversioned())));
				} else {
					data->clearSSWithTssPair();
					data->addMutationToMutationLog(
					    mLV, MutationRef(MutationRef::ClearRange, persistSSPairID, keyAfter(persistSSPairID)));
				}
			}
		} else if (m.param1.substr(1).startsWith(tssQuarantineKeys.begin) &&
		           (m.type == MutationRef::SetValue || m.type == MutationRef::ClearRange)) {
			if (data->isTss()) {
				UID ssId = decodeTssQuarantineKey(m.param1.substr(1));
				ASSERT(ssId == data->thisServerID);
				if (m.type == MutationRef::SetValue) {
					CODE_PROBE(true, "Putting TSS in quarantine");
					TraceEvent(SevWarn, "TSSQuarantineStart", data->thisServerID).log();
					data->startTssQuarantine();
				} else {
					TraceEvent(SevWarn, "TSSQuarantineStop", data->thisServerID).log();
					TraceEvent("StorageServerWorkerRemoved", data->thisServerID).detail("Reason", "TSSQuarantineStop");
					// dispose of this TSS
					throw worker_removed();
				}
			}
		} else if (SERVER_KNOBS->GENERATE_DATA_ENABLED && m.param1.substr(1).startsWith(constructDataKey)) {
			uint64_t valSize, keyCount, seed;
			Standalone<StringRef> prefix;
			std::tie(prefix, valSize, keyCount, seed) = decodeConstructKeys(m.param2);
			ASSERT(prefix.size() > 0 && keyCount < UINT16_MAX && valSize < CLIENT_KNOBS->VALUE_SIZE_LIMIT);
			uint8_t keyBuf[prefix.size() + sizeof(uint16_t)];
			uint8_t* keyPos = prefix.copyTo(keyBuf);
			uint8_t valBuf[valSize];
			setThreadLocalDeterministicRandomSeed(seed);
			for (uint32_t keyNum = 1; keyNum <= keyCount; keyNum += 1) {
				if ((keyNum % 0xff) == 0) {
					*keyPos++ = 0;
				}
				*keyPos = keyNum % 0xff;
				auto r = data->shards.rangeContaining(StringRef(keyBuf, keyPos - keyBuf + 1)).value();
				if (!r || !(r->adding || r->moveInShard || r->readWrite)) {
					break;
				}

				deterministicRandom()->randomBytes(&valBuf[0], valSize);
				data->constructedData.emplace_back(Standalone<StringRef>(StringRef(keyBuf, keyPos - keyBuf + 1)),
				                                   Standalone<StringRef>(StringRef(valBuf, valSize)));
			}
			TraceEvent(SevDebug, "ConstructDataBuilder")
			    .detail("Prefix", prefix)
			    .detail("KeyCount", keyCount)
			    .detail("ValSize", valSize)
			    .detail("Seed", seed);
		} else if (isAccumulativeChecksumMutation(m)) {
			if (data->acsValidator != nullptr) {
				ASSERT(m.checksum.present() && m.accumulativeChecksumIndex.present());
				AccumulativeChecksumState acsMutationState = decodeAccumulativeChecksum(m.param2);
				Optional<AccumulativeChecksumState> stateToPersist = data->acsValidator->processAccumulativeChecksum(
				    acsMutationState, data->thisServerID, data->tag, data->version.get());
				if (stateToPersist.present()) {
					auto& mLV = data->addVersionToMutationLog(data->data().getLatestVersion());
					data->addMutationToMutationLog(
					    mLV,
					    MutationRef(MutationRef::SetValue,
					                encodePersistAccumulativeChecksumKey(stateToPersist.get().acsIndex),
					                accumulativeChecksumValue(stateToPersist.get())));
				}
			}
		} else {
			ASSERT(false); // Unknown private mutation
		}
	}

	void applyPrivateCacheData(StorageServer* data, MutationRef const& m) {
		//TraceEvent(SevDebug, "SSPrivateCacheMutation", data->thisServerID).detail("Mutation", m);

		if (processedCacheStartKey) {
			// Because of the implementation of the krm* functions, we expect changes in pairs, [begin,end)
			ASSERT((m.type == MutationRef::SetValue) && m.param1.substr(1).startsWith(storageCachePrefix));
			KeyRangeRef keys(cacheStartKey.removePrefix(systemKeys.begin).removePrefix(storageCachePrefix),
			                 m.param1.removePrefix(systemKeys.begin).removePrefix(storageCachePrefix));
			data->cachedRangeMap.insert(keys, true);

			// Figure out the affected shard ranges and maintain the cached key-range information in the in-memory
			// map
			// TODO revisit- we are not splitting the cached ranges based on shards as of now.
			if (0) {
				auto cachedRanges = data->shards.intersectingRanges(keys);
				for (auto shard = cachedRanges.begin(); shard != cachedRanges.end(); ++shard) {
					KeyRangeRef intersectingRange = shard.range() & keys;
					TraceEvent(SevDebug, "SSPrivateCacheMutationInsertUnexpected", data->thisServerID)
					    .detail("Begin", intersectingRange.begin)
					    .detail("End", intersectingRange.end);
					data->cachedRangeMap.insert(intersectingRange, true);
				}
			}
			processedStartKey = false;
		} else if ((m.type == MutationRef::SetValue) && m.param1.substr(1).startsWith(storageCachePrefix)) {
			// Because of the implementation of the krm* functions, we expect changes in pairs, [begin,end)
			cacheStartKey = m.param1;
			processedCacheStartKey = true;
		} else {
			ASSERT(false); // Unknown private mutation
		}
	}

	// Handles checkpoint private mutations:
	// 1. Registers a pending checkpoint request, it will be fulfilled when the desired version is durable.
	// 2. Schedule deleting a checkpoint.
	void handleCheckpointPrivateMutation(StorageServer* data, const MutationRef& m, Version ver) {
		CheckpointMetaData checkpoint = decodeCheckpointValue(m.param2);
		const CheckpointMetaData::CheckpointState cState = checkpoint.getState();
		const UID checkpointID = decodeCheckpointKey(m.param1.substr(1));
		TraceEvent(SevDebug, "HandleCheckpointPrivateMutation", data->thisServerID)
		    .detail("Checkpoint", checkpoint.toString());
		if (!data->shardAware || data->isTss()) {
			return;
		}
		auto& mLV = data->addVersionToMutationLog(ver);
		if (cState == CheckpointMetaData::Pending) {
			checkpoint.version = ver;
			data->pendingCheckpoints[ver].push_back(checkpoint);
			const Key pendingCheckpointKey(persistPendingCheckpointKeys.begin.toString() + checkpointID.toString());
			data->addMutationToMutationLog(
			    mLV, MutationRef(MutationRef::SetValue, pendingCheckpointKey, checkpointValue(checkpoint)));

			TraceEvent(SevInfo, "RegisterPendingCheckpoint", data->thisServerID)
			    .detail("Key", pendingCheckpointKey)
			    .detail("Checkpoint", checkpoint.toString());
		} else if (cState == CheckpointMetaData::Deleting) {
			ASSERT(std::find(checkpoint.src.begin(), checkpoint.src.end(), data->thisServerID) != checkpoint.src.end());
			checkpoint.src.clear();
			checkpoint.src.push_back(data->thisServerID);
			checkpoint.dir = serverCheckpointDir(data->checkpointFolder, checkpoint.checkpointID);
			const Key persistCheckpointKey(persistCheckpointKeys.begin.toString() + checkpoint.checkpointID.toString());
			data->addMutationToMutationLog(
			    mLV, MutationRef(MutationRef::SetValue, persistCheckpointKey, checkpointValue(checkpoint)));
			data->actors.add(deleteCheckpointQ(data, ver, checkpoint));
			TraceEvent(SevInfo, "DeleteCheckpointScheduled", data->thisServerID)
			    .detail("Source", "PrivateMutation")
			    .detail("Checkpoint", checkpoint.toString());
		}
	}
};

void StorageServer::insertTenant(TenantMapEntry const& tenant, Version version, bool persist) {
	if (version >= tenantMap.getLatestVersion()) {
		TenantSSInfo tenantSSInfo{ tenant.tenantLockState };
		int64_t tenantId = TenantAPI::prefixToId(tenant.prefix);
		tenantMap.createNewVersion(version);
		tenantMap.insert(tenant.id, tenantSSInfo);

		if (persist) {
			auto& mLV = addVersionToMutationLog(version);
			addMutationToMutationLog(mLV,
			                         MutationRef(MutationRef::SetValue,
			                                     tenant.prefix.withPrefix(persistTenantMapKeys.begin),
			                                     ObjectWriter::toValue(tenantSSInfo, IncludeVersion())));
		}

		TraceEvent("InsertTenant", thisServerID).detail("Tenant", tenantId).detail("Version", version);
	}
}

void StorageServer::clearTenants(StringRef startTenant, StringRef endTenant, Version version) {
	if (version >= tenantMap.getLatestVersion()) {
		tenantMap.createNewVersion(version);

		auto view = tenantMap.at(version);
		auto& mLV = addVersionToMutationLog(version);
		std::set<int64_t> tenantsToClear;
		Optional<int64_t> startId = TenantIdCodec::lowerBound(startTenant);
		Optional<int64_t> endId = TenantIdCodec::lowerBound(endTenant);
		auto startItr = startId.present() ? view.lower_bound(startId.get()) : view.end();
		auto endItr = endId.present() ? view.lower_bound(endId.get()) : view.end();
		for (auto itr = startItr; itr != endItr; ++itr) {
			auto mapKey = itr.key();
			// Trigger any watches on the prefix associated with the tenant.
			TraceEvent("EraseTenant", thisServerID).detail("TenantID", mapKey).detail("Version", version);
			tenantWatches.sendError(mapKey, mapKey + 1, tenant_removed());
			tenantsToClear.insert(mapKey);
		}
		addMutationToMutationLog(mLV,
		                         MutationRef(MutationRef::ClearRange,
		                                     startTenant.withPrefix(persistTenantMapKeys.begin),
		                                     endTenant.withPrefix(persistTenantMapKeys.begin)));

		for (auto tenantId : tenantsToClear) {
			tenantMap.erase(tenantId);
		}
	}
}

ACTOR Future<Void> tssDelayForever() {
	loop {
		wait(delay(5.0));
		if (g_simulator->speedUpSimulation) {
			return Void();
		}
	}
}

ACTOR Future<Void> update(StorageServer* data, bool* pReceivedUpdate) {
	state double updateStart = g_network->timer();
	state double decryptionTime = 0;
	state double start;
	state bool enableClearRangeEagerReads =
	    (data->storage.getKeyValueStoreType() == KeyValueStoreType::SSD_ROCKSDB_V1 ||
	     data->storage.getKeyValueStoreType() == KeyValueStoreType::SSD_SHARDED_ROCKSDB)
	        ? SERVER_KNOBS->ROCKSDB_ENABLE_CLEAR_RANGE_EAGER_READS
	        : SERVER_KNOBS->ENABLE_CLEAR_RANGE_EAGER_READS;
	state UpdateEagerReadInfo eager(enableClearRangeEagerReads);
	try {

		// If we are disk bound and durableVersion is very old, we need to block updates or we could run out of
		// memory. This is often referred to as the storage server e-brake (emergency brake)

		// We allow the storage server to make some progress between e-brake periods, referred to as "overage", in
		// order to ensure that it advances desiredOldestVersion enough for updateStorage to make enough progress on
		// freeing up queue size. We also increase these limits if speed up simulation was set IF they were
		// buggified to a very small value.
		state int64_t hardLimit = SERVER_KNOBS->STORAGE_HARD_LIMIT_BYTES;
		state int64_t hardLimitOverage = SERVER_KNOBS->STORAGE_HARD_LIMIT_BYTES_OVERAGE;
		if (g_network->isSimulated() && g_simulator->speedUpSimulation) {
			hardLimit = SERVER_KNOBS->STORAGE_HARD_LIMIT_BYTES_SPEED_UP_SIM;
			hardLimitOverage = SERVER_KNOBS->STORAGE_HARD_LIMIT_BYTES_OVERAGE_SPEED_UP_SIM;
		}
		state double waitStartT = 0;
		if (data->queueSize() >= hardLimit && data->durableVersion.get() < data->desiredOldestVersion.get() &&
		    ((data->desiredOldestVersion.get() - SERVER_KNOBS->STORAGE_HARD_LIMIT_VERSION_OVERAGE >
		      data->lastDurableVersionEBrake) ||
		     (data->counters.bytesInput.getValue() - hardLimitOverage > data->lastBytesInputEBrake))) {

			while (data->queueSize() >= hardLimit && data->durableVersion.get() < data->desiredOldestVersion.get()) {
				if (now() - waitStartT >= 1) {
					TraceEvent(SevWarn, "StorageServerUpdateLag", data->thisServerID)
					    .detail("Version", data->version.get())
					    .detail("DurableVersion", data->durableVersion.get())
					    .detail("DesiredOldestVersion", data->desiredOldestVersion.get())
					    .detail("QueueSize", data->queueSize())
					    .detail("LastBytesInputEBrake", data->lastBytesInputEBrake)
					    .detail("LastDurableVersionEBrake", data->lastDurableVersionEBrake);
					waitStartT = now();
				}

				data->behind = true;
				wait(delayJittered(.005, TaskPriority::TLogPeekReply));
			}
			data->lastBytesInputEBrake = data->counters.bytesInput.getValue();
			data->lastDurableVersionEBrake = data->durableVersion.get();
		}

		if (g_network->isSimulated() && data->isTss() && g_simulator->tssMode == ISimulator::TSSMode::EnabledAddDelay &&
		    !g_simulator->speedUpSimulation && data->tssFaultInjectTime.present() &&
		    data->tssFaultInjectTime.get() < now()) {
			if (deterministicRandom()->random01() < 0.01) {
				TraceEvent(SevWarnAlways, "TSSInjectDelayForever", data->thisServerID).log();
				// small random chance to just completely get stuck here, each tss should eventually hit this in
				// this mode
				wait(tssDelayForever());
			} else {
				// otherwise pause for part of a second
				double delayTime = deterministicRandom()->random01();
				TraceEvent(SevWarnAlways, "TSSInjectDelay", data->thisServerID).detail("Delay", delayTime);
				wait(delay(delayTime));
			}
		}

		if (data->maybeInjectDelay()) {
			wait(delay(deterministicRandom()->random01() * 10.0));
		}

		while (data->byteSampleClearsTooLarge.get()) {
			wait(data->byteSampleClearsTooLarge.onChange());
		}

		state Reference<ILogSystem::IPeekCursor> cursor = data->logCursor;

		state double beforeTLogCursorReads = now();
		loop {
			wait(cursor->getMore());
			if (!cursor->isExhausted()) {
				break;
			}
		}
		data->tlogCursorReadsLatencyHistogram->sampleSeconds(now() - beforeTLogCursorReads);
		if (cursor->popped() > 0) {
			TraceEvent("StorageServerWorkerRemoved", data->thisServerID)
			    .detail("Reason", "PeekPoppedTLogData")
			    .detail("Version", cursor->popped());
			throw worker_removed();
		}

		++data->counters.updateBatches;
		data->lastTLogVersion = cursor->getMaxKnownVersion();
		if (cursor->getMinKnownCommittedVersion() > data->knownCommittedVersion.get()) {
			data->knownCommittedVersion.set(cursor->getMinKnownCommittedVersion());
		}
		data->versionLag = std::max<int64_t>(0, data->lastTLogVersion - data->version.get());

		ASSERT(*pReceivedUpdate == false);
		*pReceivedUpdate = true;

		start = now();
		wait(data->durableVersionLock.take(TaskPriority::TLogPeekReply, 1));
		state FlowLock::Releaser holdingDVL(data->durableVersionLock);
		if (now() - start > 0.1)
			TraceEvent("SSSlowTakeLock1", data->thisServerID)
			    .detailf("From", "%016llx", debug_lastLoadBalanceResultEndpointToken)
			    .detail("Duration", now() - start)
			    .detail("Version", data->version.get());
		data->ssVersionLockLatencyHistogram->sampleSeconds(now() - start);

		start = now();
		state FetchInjectionInfo fii;
		state Reference<ILogSystem::IPeekCursor> cloneCursor2 = cursor->cloneNoMore();
		state Optional<std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>>> cipherKeys;
		state bool collectingCipherKeys = false;

		// Collect eager read keys.
		// If encrypted mutation is encountered, we collect cipher details and fetch cipher keys, then start over.
		loop {
			state uint64_t changeCounter = data->shardChangeCounter;
			bool epochEnd = false;
			bool hasPrivateData = false;
			bool firstMutation = true;
			bool dbgLastMessageWasProtocol = false;

			std::unordered_set<BlobCipherDetails> cipherDetails;

			Reference<ILogSystem::IPeekCursor> cloneCursor1 = cloneCursor2->cloneNoMore();

			cloneCursor1->setProtocolVersion(data->logProtocol);

			for (; cloneCursor1->hasMessage(); cloneCursor1->nextMessage()) {
				ArenaReader& cloneReader = *cloneCursor1->reader();

				if (LogProtocolMessage::isNextIn(cloneReader)) {
					LogProtocolMessage lpm;
					cloneReader >> lpm;
					//TraceEvent(SevDebug, "SSReadingLPM", data->thisServerID).detail("Mutation", lpm);
					dbgLastMessageWasProtocol = true;
					cloneCursor1->setProtocolVersion(cloneReader.protocolVersion());
				} else if (cloneReader.protocolVersion().hasSpanContext() &&
				           SpanContextMessage::isNextIn(cloneReader)) {
					SpanContextMessage scm;
					cloneReader >> scm;
				} else if (cloneReader.protocolVersion().hasOTELSpanContext() &&
				           OTELSpanContextMessage::isNextIn(cloneReader)) {
					OTELSpanContextMessage scm;
					cloneReader >> scm;
				} else {
					MutationRef msg;
					cloneReader >> msg;
					ASSERT(data->encryptionMode.present());
					ASSERT(!data->encryptionMode.get().isEncryptionEnabled() || msg.isEncrypted() ||
					       isBackupLogMutation(msg) || isAccumulativeChecksumMutation(msg));
					if (msg.isEncrypted()) {
						if (!cipherKeys.present()) {
							msg.updateEncryptCipherDetails(cipherDetails);
							collectingCipherKeys = true;
						} else {
							double decryptionTimeV = 0;
							msg = msg.decrypt(
							    cipherKeys.get(), eager.arena, BlobCipherMetrics::TLOG, nullptr, &decryptionTimeV);
							decryptionTime += decryptionTimeV;
						}
					} else {
						if (!msg.validateChecksum()) {
							TraceEvent(SevError, "ValidateChecksumError", data->thisServerID)
							    .setMaxFieldLength(-1)
							    .setMaxEventLength(-1)
							    .detail("Mutation", msg);
							ASSERT(false);
						}
					}
					// TraceEvent(SevDebug, "SSReadingLog", data->thisServerID).detail("Mutation", msg);
					if (data->acsValidator != nullptr) {
						data->acsValidator->incrementTotalMutations();
						if (isAccumulativeChecksumMutation(msg)) {
							data->acsValidator->incrementTotalAcsMutations();
						}
					}
					if (!collectingCipherKeys) {
						if (firstMutation && msg.param1.startsWith(systemKeys.end))
							hasPrivateData = true;
						firstMutation = false;

						if (msg.param1 == lastEpochEndPrivateKey) {
							epochEnd = true;
							ASSERT(dbgLastMessageWasProtocol);
						}

						eager.addMutation(msg);
						dbgLastMessageWasProtocol = false;
					}
				}
			}

			if (collectingCipherKeys) {
				std::unordered_map<BlobCipherDetails, Reference<BlobCipherKey>> getCipherKeysResult =
				    wait(GetEncryptCipherKeys<ServerDBInfo>::getEncryptCipherKeys(
				        data->db, cipherDetails, BlobCipherMetrics::TLOG));
				cipherKeys = getCipherKeysResult;
				collectingCipherKeys = false;
				eager = UpdateEagerReadInfo(enableClearRangeEagerReads);
			} else {
				// Any fetchKeys which are ready to transition their shards to the adding,transferred state do so
				// now. If there is an epoch end we skip this step, to increase testability and to prevent inserting
				// a version in the middle of a rolled back version range.
				while (!hasPrivateData && !epochEnd && !data->readyFetchKeys.empty()) {
					auto fk = data->readyFetchKeys.back();
					data->readyFetchKeys.pop_back();
					fk.send(&fii);
					// fetchKeys() would put the data it fetched into the fii. The thread will not return back to
					// this actor until it was completed.
				}

				for (auto& c : fii.changes)
					eager.addMutations(c.mutations);

				wait(doEagerReads(data, &eager));
				if (data->shardChangeCounter == changeCounter)
					break;
				CODE_PROBE(
				    true,
				    "A fetchKeys completed while we were doing this, so eager might be outdated.  Read it again.");
				// SOMEDAY: Theoretically we could check the change counters of individual shards and retry the
				// reads only selectively
				eager = UpdateEagerReadInfo(enableClearRangeEagerReads);
				cloneCursor2 = cursor->cloneNoMore();
			}
		}
		data->eagerReadsLatencyHistogram->sampleSeconds(now() - start);

		if (now() - start > 0.1)
			TraceEvent("SSSlowTakeLock2", data->thisServerID)
			    .detailf("From", "%016llx", debug_lastLoadBalanceResultEndpointToken)
			    .detail("Duration", now() - start)
			    .detail("Version", data->version.get());

		data->updateEagerReads = &eager;
		data->debug_inApplyUpdate = true;

		state StorageUpdater updater(data->lastVersionWithData, data->restoredVersion);

		if (EXPENSIVE_VALIDATION)
			data->data().atLatest().validate();
		validate(data);

		state bool injectedChanges = false;
		state int changeNum = 0;
		state int mutationBytes = 0;
		state double beforeFetchKeysUpdates = now();
		for (; changeNum < fii.changes.size(); changeNum++) {
			state int mutationNum = 0;
			state VerUpdateRef* pUpdate = &fii.changes[changeNum];
			for (; mutationNum < pUpdate->mutations.size(); mutationNum++) {
				updater.applyMutation(
				    data, pUpdate->mutations[mutationNum], MutationRefAndCipherKeys(), pUpdate->version, true);
				mutationBytes += pUpdate->mutations[mutationNum].totalSize();
				// data->counters.mutationBytes or data->counters.mutations should not be updated because they
				// should have counted when the mutations arrive from cursor initially.
				injectedChanges = true;
				if (mutationBytes > SERVER_KNOBS->DESIRED_UPDATE_BYTES) {
					mutationBytes = 0;
					wait(delay(SERVER_KNOBS->UPDATE_DELAY));
				}
			}
		}
		data->fetchKeysPTreeUpdatesLatencyHistogram->sampleSeconds(now() - beforeFetchKeysUpdates);

		state Version ver = invalidVersion;
		cloneCursor2->setProtocolVersion(data->logProtocol);
		state SpanContext spanContext = SpanContext();
		state double beforeTLogMsgsUpdates = now();
		state std::set<Key> updatedChangeFeeds;
		for (; cloneCursor2->hasMessage(); cloneCursor2->nextMessage()) {
			if (mutationBytes > SERVER_KNOBS->DESIRED_UPDATE_BYTES) {
				mutationBytes = 0;
				// Instead of just yielding, leave time for the storage server to respond to reads
				wait(delay(SERVER_KNOBS->UPDATE_DELAY));
			}

			if (cloneCursor2->version().version > ver) {
				ASSERT(cloneCursor2->version().version > data->version.get());
			}

			auto& rd = *cloneCursor2->reader();

			if (cloneCursor2->version().version > ver && cloneCursor2->version().version > data->version.get()) {
				++data->counters.updateVersions;
				if (data->currentChangeFeeds.size()) {
					data->changeFeedVersions.emplace_back(
					    std::vector<Key>(data->currentChangeFeeds.begin(), data->currentChangeFeeds.end()), ver);
					updatedChangeFeeds.insert(data->currentChangeFeeds.begin(), data->currentChangeFeeds.end());
					data->currentChangeFeeds.clear();
				}
				ver = cloneCursor2->version().version;
			}

			if (LogProtocolMessage::isNextIn(rd)) {
				LogProtocolMessage lpm;
				rd >> lpm;

				data->logProtocol = rd.protocolVersion();
				data->storage.changeLogProtocol(ver, data->logProtocol);
				cloneCursor2->setProtocolVersion(rd.protocolVersion());
				spanContext.traceID = UID();
			} else if (rd.protocolVersion().hasSpanContext() && SpanContextMessage::isNextIn(rd)) {
				SpanContextMessage scm;
				rd >> scm;
				CODE_PROBE(true, "storageserveractor converting SpanContextMessage into OTEL SpanContext");
				spanContext =
				    SpanContext(UID(scm.spanContext.first(), scm.spanContext.second()),
				                0,
				                scm.spanContext.first() != 0 && scm.spanContext.second() != 0 ? TraceFlags::sampled
				                                                                              : TraceFlags::unsampled);
			} else if (rd.protocolVersion().hasOTELSpanContext() && OTELSpanContextMessage::isNextIn(rd)) {
				CODE_PROBE(true, "storageserveractor reading OTELSpanContextMessage");
				OTELSpanContextMessage scm;
				rd >> scm;
				spanContext = scm.spanContext;
			} else {
				MutationRef msg;
				MutationRefAndCipherKeys encryptedMutation;
				rd >> msg;
				ASSERT(data->encryptionMode.present());
				ASSERT(!data->encryptionMode.get().isEncryptionEnabled() || msg.isEncrypted() ||
				       isBackupLogMutation(msg) || isAccumulativeChecksumMutation(msg));
				if (msg.isEncrypted()) {
					ASSERT(cipherKeys.present());
					encryptedMutation.mutation = msg;
					encryptedMutation.cipherKeys = msg.getCipherKeys(cipherKeys.get());
					double decryptionTimeV = 0;
					msg = msg.decrypt(
					    encryptedMutation.cipherKeys, rd.arena(), BlobCipherMetrics::TLOG, nullptr, &decryptionTimeV);
					decryptionTime += decryptionTimeV;
				} else if (data->acsValidator != nullptr && msg.checksum.present() &&
				           msg.accumulativeChecksumIndex.present() && !isAccumulativeChecksumMutation(msg)) {
					// We have to check accumulative checksum when iterating through cloneCursor2,
					// where ss removal by tag assignment takes effect immediately
					data->acsValidator->addMutation(
					    msg, data->thisServerID, data->tag, data->version.get(), cloneCursor2->version().version);
				}

				Span span("SS:update"_loc, spanContext);

				// Drop non-private mutations if TSS fault injection is enabled in simulation, or if this is a TSS
				// in quarantine.
				if (g_network->isSimulated() && data->isTss() && !g_simulator->speedUpSimulation &&
				    g_simulator->tssMode == ISimulator::TSSMode::EnabledDropMutations &&
				    data->tssFaultInjectTime.present() && data->tssFaultInjectTime.get() < now() &&
				    (msg.type == MutationRef::SetValue || msg.type == MutationRef::ClearRange) &&
				    (msg.param1.size() < 2 || msg.param1[0] != 0xff || msg.param1[1] != 0xff) &&
				    deterministicRandom()->random01() < 0.05) {
					TraceEvent(SevWarnAlways, "TSSInjectDropMutation", data->thisServerID)
					    .detail("Mutation", msg)
					    .detail("Version", cloneCursor2->version().toString());
				} else if (data->isTSSInQuarantine() &&
				           (msg.param1.size() < 2 || msg.param1[0] != 0xff || msg.param1[1] != 0xff)) {
					TraceEvent("TSSQuarantineDropMutation", data->thisServerID)
					    .suppressFor(10.0)
					    .detail("Version", cloneCursor2->version().toString());
				} else if (ver != invalidVersion) { // This change belongs to a version < minVersion
					DEBUG_MUTATION("SSPeek", ver, msg, data->thisServerID);
					if (ver == data->initialClusterVersion) {
						//TraceEvent("SSPeekMutation", data->thisServerID).log();
						// The following trace event may produce a value with special characters
						TraceEvent("SSPeekMutation", data->thisServerID)
						    .detail("Mutation", msg)
						    .detail("Version", cloneCursor2->version().toString());
					}

					updater.applyMutation(data, msg, encryptedMutation, ver, false);
					mutationBytes += msg.totalSize();
					data->counters.mutationBytes += msg.totalSize();
					data->counters.logicalBytesInput += msg.expectedSize();
					++data->counters.mutations;
					switch (msg.type) {
					case MutationRef::SetValue:
						++data->counters.setMutations;
						break;
					case MutationRef::ClearRange:
						++data->counters.clearRangeMutations;
						break;
					case MutationRef::AddValue:
					case MutationRef::And:
					case MutationRef::AndV2:
					case MutationRef::AppendIfFits:
					case MutationRef::ByteMax:
					case MutationRef::ByteMin:
					case MutationRef::Max:
					case MutationRef::Min:
					case MutationRef::MinV2:
					case MutationRef::Or:
					case MutationRef::Xor:
					case MutationRef::CompareAndClear:
						++data->counters.atomicMutations;
						break;
					}
				} else
					TraceEvent(SevError, "DiscardingPeekedData", data->thisServerID)
					    .detail("Mutation", msg)
					    .detail("Version", cloneCursor2->version().toString());
			}
		}

		if (data->acsValidator != nullptr) {
			data->acsValidator->clearCache(data->thisServerID, data->tag, data->version.get());
		}

		if (SERVER_KNOBS->GENERATE_DATA_ENABLED && data->constructedData.size() && ver != invalidVersion) {
			int mutationCount =
			    std::min(static_cast<int>(data->constructedData.size()), SERVER_KNOBS->GENERATE_DATA_PER_VERSION_MAX);
			for (int m = 0; m < mutationCount; m++) {
				auto r = data->shards.rangeContaining(data->constructedData.front().first).value();
				if (r && (r->adding || r->moveInShard || r->readWrite)) {
					MutationRef constructedMutation(MutationRef::SetValue,
					                                data->constructedData.front().first,
					                                data->constructedData.front().second);
					// TraceEvent(SevDebug, "ConstructDataCommit").detail("Key", constructedMutation.param1).detail("V",ver);
					MutationRefAndCipherKeys encryptedMutation;
					updater.applyMutation(data, constructedMutation, encryptedMutation, ver, false);
					mutationBytes += constructedMutation.totalSize();
					data->counters.mutationBytes += constructedMutation.totalSize();
					data->counters.logicalBytesInput += constructedMutation.expectedSize();
					++data->counters.mutations;
					++data->counters.setMutations;
				}
				data->constructedData.pop_front();
			}
		}

		data->tLogMsgsPTreeUpdatesLatencyHistogram->sampleSeconds(now() - beforeTLogMsgsUpdates);
		if (data->currentChangeFeeds.size()) {
			data->changeFeedVersions.emplace_back(
			    std::vector<Key>(data->currentChangeFeeds.begin(), data->currentChangeFeeds.end()), ver);
			updatedChangeFeeds.insert(data->currentChangeFeeds.begin(), data->currentChangeFeeds.end());
			data->currentChangeFeeds.clear();
		}

		if (ver != invalidVersion) {
			data->lastVersionWithData = ver;
		}
		ver = cloneCursor2->version().version - 1;

		if (injectedChanges)
			data->lastVersionWithData = ver;

		data->updateEagerReads = nullptr;
		data->debug_inApplyUpdate = false;

		if (ver == invalidVersion && !fii.changes.empty()) {
			ver = updater.currentVersion;
		}

		if (ver != invalidVersion && ver > data->version.get()) {
			// TODO(alexmiller): Update to version tracking.
			// DEBUG_KEY_RANGE("SSUpdate", ver, KeyRangeRef());

			data->mutableData().createNewVersion(ver);
			if (data->otherError.getFuture().isReady())
				data->otherError.getFuture().get();

			data->counters.fetchedVersions += (ver - data->version.get());
			++data->counters.fetchesFromLogs;
			Optional<UID> curSourceTLogID = cursor->getCurrentPeekLocation();

			if (curSourceTLogID != data->sourceTLogID) {
				data->sourceTLogID = curSourceTLogID;

				TraceEvent("StorageServerSourceTLogID", data->thisServerID)
				    .detail("SourceTLogID",
				            data->sourceTLogID.present() ? data->sourceTLogID.get().toString() : "unknown")
				    .trackLatest(data->storageServerSourceTLogIDEventHolder->trackingKey);
			}

			data->noRecentUpdates.set(false);
			data->lastUpdate = now();

			data->prevVersion = data->version.get();
			data->version.set(ver); // Triggers replies to waiting gets for new version(s)

			for (auto& it : updatedChangeFeeds) {
				auto feed = data->uidChangeFeed.find(it);
				if (feed != data->uidChangeFeed.end()) {
					feed->second->newMutations.trigger();
				}
			}

			setDataVersion(data->thisServerID, data->version.get());
			if (data->otherError.getFuture().isReady())
				data->otherError.getFuture().get();

			Version maxVersionsInMemory =
			    (g_network->isSimulated() && g_simulator->speedUpSimulation)
			        ? std::max(5 * SERVER_KNOBS->VERSIONS_PER_SECOND, SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS)
			        : SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS;
			for (int i = 0; i < data->recoveryVersionSkips.size(); i++) {
				maxVersionsInMemory += data->recoveryVersionSkips[i].second;
			}

			// Trigger updateStorage if necessary
			Version proposedOldestVersion =
			    std::max(data->version.get(), cursor->getMinKnownCommittedVersion()) - maxVersionsInMemory;
			if (data->primaryLocality == tagLocalitySpecial || data->tag.locality == data->primaryLocality) {
				proposedOldestVersion = std::max(proposedOldestVersion, data->lastTLogVersion - maxVersionsInMemory);
			}
			proposedOldestVersion = std::min(proposedOldestVersion, data->version.get() - 1);
			proposedOldestVersion = std::max(proposedOldestVersion, data->oldestVersion.get());
			proposedOldestVersion = std::max(proposedOldestVersion, data->desiredOldestVersion.get());
			proposedOldestVersion = std::max(proposedOldestVersion, data->initialClusterVersion);

			//TraceEvent("StorageServerUpdated", data->thisServerID).detail("Ver", ver).detail("DataVersion", data->version.get())
			//	.detail("LastTLogVersion", data->lastTLogVersion).detail("NewOldest",
			// data->oldestVersion.get()).detail("DesiredOldest",data->desiredOldestVersion.get())
			//	.detail("MaxVersionInMemory", maxVersionsInMemory).detail("Proposed",
			// proposedOldestVersion).detail("PrimaryLocality", data->primaryLocality).detail("Tag",
			// data->tag.toString());

			while (!data->recoveryVersionSkips.empty() &&
			       proposedOldestVersion > data->recoveryVersionSkips.front().first) {
				data->recoveryVersionSkips.pop_front();
			}
			data->desiredOldestVersion.set(proposedOldestVersion);
		}

		validate(data);

		if ((data->lastTLogVersion - data->version.get()) < SERVER_KNOBS->STORAGE_RECOVERY_VERSION_LAG_LIMIT) {
			if (data->registerInterfaceAcceptingRequests.canBeSet()) {
				data->registerInterfaceAcceptingRequests.send(Void());
				ErrorOr<Void> e = wait(errorOr(data->interfaceRegistered));
				if (e.isError()) {
					TraceEvent(SevWarn, "StorageInterfaceRegistrationFailed", data->thisServerID).error(e.getError());
					throw e.getError();
				}
			}
		}

		data->logCursor->advanceTo(cloneCursor2->version());
		if (cursor->version().version >= data->lastTLogVersion) {
			if (data->behind) {
				TraceEvent("StorageServerNoLongerBehind", data->thisServerID)
				    .detail("CursorVersion", cursor->version().version)
				    .detail("TLogVersion", data->lastTLogVersion);
			}
			data->behind = false;
		}
		const double duration = g_network->timer() - updateStart;
		data->counters.updateEncryptionLatencySample.addMeasurement(decryptionTime);
		data->counters.updateLatencySample.addMeasurement(duration);

		return Void(); // update will get called again ASAP
	} catch (Error& err) {
		state Error e = err;
		if (e.code() == error_code_encrypt_keys_fetch_failed) {
			TraceEvent(SevWarn, "SSUpdateError", data->thisServerID).error(e).backtrace();
		} else if (e.code() != error_code_worker_removed && e.code() != error_code_please_reboot) {
			TraceEvent(SevError, "SSUpdateError", data->thisServerID).error(e).backtrace();
		} else if (e.code() == error_code_please_reboot) {
			wait(data->durableInProgress);
		}
		throw e;
	}
}

ACTOR Future<bool> createSstFileForCheckpointShardBytesSample(StorageServer* data,
                                                              CheckpointMetaData metaData,
                                                              std::string bytesSampleFile) {
	state int failureCount = 0;
	state std::unique_ptr<IRocksDBSstFileWriter> sstWriter;
	state int64_t numGetRangeQueries;
	state int64_t numSampledKeys;
	state std::vector<KeyRange>::iterator metaDataRangesIter;
	state Key readBegin;
	state Key readEnd;
	state bool anyFileCreated;
	TraceEvent(SevDebug, "CheckpointbytesSampleBegin", data->thisServerID).detail("Checkpoint", metaData.toString());

	loop {
		try {
			// Any failure leads to retry until retryCount reaches maximum
			// For each retry, cleanup the bytesSampleFile created in last time
			ASSERT(directoryExists(parentDirectory(bytesSampleFile)));
			if (fileExists(abspath(bytesSampleFile))) {
				deleteFile(abspath(bytesSampleFile));
			}
			anyFileCreated = false;
			sstWriter = newRocksDBSstFileWriter();
			sstWriter->open(bytesSampleFile);
			if (sstWriter == nullptr) {
				break;
			}
			ASSERT(metaData.ranges.size() > 0);
			std::sort(metaData.ranges.begin(), metaData.ranges.end(), [](KeyRange a, KeyRange b) {
				// Debug usage: make sure no overlapping between compared two ranges
				/* if (a.begin < b.begin) {
				    ASSERT(a.end <= b.begin);
				} else if (a.begin > b.begin) {
				    ASSERT(a.end >= b.begin);
				} else {
				    ASSERT(false);
				} */
				// metaData.ranges must be in ascending order
				// sstWriter requires written keys to be in ascending order
				return a.begin < b.begin;
			});

			numGetRangeQueries = 0;
			numSampledKeys = 0;
			metaDataRangesIter = metaData.ranges.begin();
			while (metaDataRangesIter != metaData.ranges.end()) {
				KeyRange range = *metaDataRangesIter;
				readBegin = range.begin.withPrefix(persistByteSampleKeys.begin);
				readEnd = range.end.withPrefix(persistByteSampleKeys.begin);
				loop {
					try {
						RangeResult readResult = wait(data->storage.readRange(KeyRangeRef(readBegin, readEnd),
						                                                      SERVER_KNOBS->STORAGE_LIMIT_BYTES,
						                                                      SERVER_KNOBS->STORAGE_LIMIT_BYTES));
						numGetRangeQueries++;
						for (int i = 0; i < readResult.size(); i++) {
							ASSERT(!readResult[i].key.empty() && !readResult[i].value.empty());
							int64_t size = BinaryReader::fromStringRef<int64_t>(readResult[i].value, Unversioned());
							KeyRef key = readResult[i].key.removePrefix(persistByteSampleKeys.begin);
							TraceEvent(SevDebug, "CheckpointbytesSampleKey", data->thisServerID)
							    // .setMaxFieldLength(10000)
							    .detail("Checkpoint", metaData.toString())
							    .detail("SampleKey", key)
							    .detail("Size", size);
							sstWriter->write(readResult[i].key, readResult[i].value);
							numSampledKeys++;
						}
						if (readResult.more) {
							readBegin = readResult.getReadThrough();
							ASSERT(readBegin <= readEnd);
						} else {
							break; // finish for current metaDataRangesIter
						}
					} catch (Error& e) {
						if (failureCount < SERVER_KNOBS->ROCKSDB_CREATE_BYTES_SAMPLE_FILE_RETRY_MAX) {
							throw retry(); // retry from sketch
						} else {
							throw e;
						}
					}
				}
				metaDataRangesIter++;
			}
			anyFileCreated = sstWriter->finish();
			ASSERT((numSampledKeys > 0 && anyFileCreated) || (numSampledKeys == 0 && !anyFileCreated));
			TraceEvent(SevDebug, "DumpCheckPointMetaData", data->thisServerID)
			    .detail("NumSampledKeys", numSampledKeys)
			    .detail("NumGetRangeQueries", numGetRangeQueries)
			    .detail("CheckpointID", metaData.checkpointID.toString())
			    .detail("BytesSampleTempFile", anyFileCreated ? bytesSampleFile : "noFileCreated");
			break;

		} catch (Error& e) {
			if (e.code() == error_code_retry) {
				wait(delay(0.5));
				failureCount++;
				continue;
			} else {
				TraceEvent(SevDebug, "StorageCreateCheckpointMetaDataSstFileDumpedFailure", data->thisServerID)
				    .detail("PendingCheckpoint", metaData.toString())
				    .detail("Error", e.name());
				throw e;
			}
		}
	}

	return anyFileCreated;
}

ACTOR Future<Void> createCheckpoint(StorageServer* data, CheckpointMetaData metaData) {
	TraceEvent(SevDebug, "SSCreateCheckpoint", data->thisServerID).detail("CheckpointMeta", metaData.toString());
	ASSERT(std::find(metaData.src.begin(), metaData.src.end(), data->thisServerID) != metaData.src.end() &&
	       !metaData.ranges.empty());
	state std::string checkpointDir = serverCheckpointDir(data->checkpointFolder, metaData.checkpointID);
	state std::string bytesSampleFile = abspath(joinPath(checkpointDir, checkpointBytesSampleFileName));
	state std::string bytesSampleTempDir = data->folder + checkpointBytesSampleTempFolder;
	state std::string bytesSampleTempFile =
	    bytesSampleTempDir + "/" + metaData.checkpointID.toString() + "_" + checkpointBytesSampleFileName;
	const CheckpointRequest req(metaData.version,
	                            metaData.ranges,
	                            static_cast<CheckpointFormat>(metaData.format),
	                            metaData.checkpointID,
	                            checkpointDir);
	state CheckpointMetaData checkpointResult;
	state bool sampleByteSstFileCreated;
	std::vector<Future<Void>> createCheckpointActors;

	try {
		// Create checkpoint
		createCheckpointActors.push_back(store(checkpointResult, data->storage.checkpoint(req)));
		// Dump the checkpoint meta data to the sst file of metadata.
		if (!directoryExists(abspath(bytesSampleTempDir))) {
			platform::createDirectory(abspath(bytesSampleTempDir));
		}
		createCheckpointActors.push_back(store(
		    sampleByteSstFileCreated, createSstFileForCheckpointShardBytesSample(data, metaData, bytesSampleTempFile)));
		wait(waitForAll(createCheckpointActors));
		// Move sst file to the checkpoint folder
		if (sampleByteSstFileCreated) {
			ASSERT(directoryExists(abspath(checkpointDir)));
			ASSERT(!fileExists(abspath(bytesSampleFile)));
			ASSERT(fileExists(abspath(bytesSampleTempFile)));
			renameFile(abspath(bytesSampleTempFile), abspath(bytesSampleFile));
		}
		checkpointResult.bytesSampleFile = sampleByteSstFileCreated ? bytesSampleFile : Optional<std::string>();
		ASSERT(checkpointResult.src.empty() && checkpointResult.getState() == CheckpointMetaData::Complete);
		checkpointResult.src.push_back(data->thisServerID);
		checkpointResult.actionId = metaData.actionId;
		checkpointResult.dir = checkpointDir;
		data->checkpoints[checkpointResult.checkpointID] = checkpointResult;

		TraceEvent("StorageCreatedCheckpoint", data->thisServerID)
		    .detail("Checkpoint", checkpointResult.toString())
		    .detail("BytesSampleFile", sampleByteSstFileCreated ? bytesSampleFile : "noFileCreated");
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		// If checkpoint creation fails, the failure is persisted.
		checkpointResult = metaData;
		checkpointResult.setState(CheckpointMetaData::Fail);
		TraceEvent("StorageCreateCheckpointFailure", data->thisServerID)
		    .detail("PendingCheckpoint", checkpointResult.toString());
	}

	// Persist the checkpoint meta data.
	try {
		Key pendingCheckpointKey(persistPendingCheckpointKeys.begin.toString() +
		                         checkpointResult.checkpointID.toString());
		Key persistCheckpointKey(persistCheckpointKeys.begin.toString() + checkpointResult.checkpointID.toString());
		data->storage.clearRange(singleKeyRange(pendingCheckpointKey));
		data->storage.writeKeyValue(KeyValueRef(persistCheckpointKey, checkpointValue(checkpointResult)));
		wait(data->storage.commit());
		TraceEvent("StorageCreateCheckpointPersisted", data->thisServerID)
		    .detail("Checkpoint", checkpointResult.toString());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		// If the checkpoint meta data is not persisted successfully, remove the checkpoint.
		TraceEvent(SevWarn, "StorageCreateCheckpointPersistFailure", data->thisServerID)
		    .errorUnsuppressed(e)
		    .detail("Checkpoint", checkpointResult.toString());
		data->checkpoints[checkpointResult.checkpointID].setState(CheckpointMetaData::Deleting);
		data->actors.add(deleteCheckpointQ(data, metaData.version, checkpointResult));
	}

	return Void();
}

struct UpdateStorageCommitStats {
	double beforeStorageUpdates;
	double beforeStorageCommit;
	double whenCommit;
	double duration;
	double commitDuration;
	double incompleteCommitDuration;
	uint64_t mutationBytes;
	uint64_t fetchKeyBytes;
	int64_t seqId;

	UpdateStorageCommitStats()
	  : seqId(0), whenCommit(0), beforeStorageUpdates(0), beforeStorageCommit(0), duration(0), commitDuration(0),
	    incompleteCommitDuration(0), mutationBytes(0), fetchKeyBytes(0) {}

	void log(UID ssid, std::string reason) const {
		TraceEvent(SevInfo, "UpdateStorageCommitStats", ssid)
		    .detail("LogReason", reason)
		    .detail("SequenceID", seqId)
		    .detail("IncompleteCommitDuration", incompleteCommitDuration)
		    .detail("CommitDuration", commitDuration)
		    .detail("Duration", duration)
		    .detail("BeforeStorageUpdates", beforeStorageUpdates)
		    .detail("BeforeStorageCommit", beforeStorageCommit)
		    .detail("WhenCommit", whenCommit)
		    .detail("MutationBytes", mutationBytes)
		    .detail("FetchKeyBytes", fetchKeyBytes);
	}
};

ACTOR Future<Void> updateStorage(StorageServer* data) {
	state UnlimitedCommitBytes unlimitedCommitBytes = UnlimitedCommitBytes::False;
	state Future<Void> durableDelay = Void();
	state std::deque<UpdateStorageCommitStats> recentCommitStats;

	loop {
		while (recentCommitStats.size() > SERVER_KNOBS->LOGGING_RECENT_STORAGE_COMMIT_SIZE) {
			recentCommitStats.pop_front();
		}
		recentCommitStats.push_back(UpdateStorageCommitStats());
		unlimitedCommitBytes = UnlimitedCommitBytes::False;
		ASSERT(data->durableVersion.get() == data->storageVersion());
		if (g_network->isSimulated()) {
			double endTime =
			    g_simulator->checkDisabled(format("%s/updateStorage", data->thisServerID.toString().c_str()));
			if (endTime > now()) {
				wait(delay(endTime - now(), TaskPriority::UpdateStorage));
			}
		}

		// If the fetch keys budget is not used up then we have already waited for the storage commit delay so
		// wait for either a new mutation version or the budget to be used up.
		// Otherwise, don't wait at all.
		if (!data->fetchKeysBudgetUsed.get()) {
			wait(data->desiredOldestVersion.whenAtLeast(data->storageVersion() + 1) ||
			     data->fetchKeysBudgetUsed.onChange());
		}

		// Yield to TaskPriority::UpdateStorage in case more mutations have arrived but were not processed yet.
		// If the fetch keys budget has already been used up, then we likely arrived here without waiting the
		// full post storage commit delay, so this will allow the update actor to process some mutations
		// before we proceed.
		wait(delay(0, TaskPriority::UpdateStorage));

		state Promise<Void> durableInProgress;
		data->durableInProgress = durableInProgress.getFuture();

		state Version startOldestVersion = data->storageVersion();
		state Version newOldestVersion = data->storageVersion();
		state Version desiredVersion = data->desiredOldestVersion.get();
		state int64_t bytesLeft = SERVER_KNOBS->STORAGE_COMMIT_BYTES;

		// Clean up stale checkpoint requests, this is not supposed to happen, since checkpoints are cleaned up on
		// failures. This is kept as a safeguard.
		while (!data->pendingCheckpoints.empty() && data->pendingCheckpoints.begin()->first <= startOldestVersion) {
			for (int idx = 0; idx < data->pendingCheckpoints.begin()->second.size(); ++idx) {
				auto& metaData = data->pendingCheckpoints.begin()->second[idx];
				data->actors.add(deleteCheckpointQ(data, startOldestVersion, metaData));
				TraceEvent(SevWarnAlways, "StorageStaleCheckpointRequest", data->thisServerID)
				    .detail("PendingCheckpoint", metaData.toString())
				    .detail("DurableVersion", startOldestVersion);
			}
			data->pendingCheckpoints.erase(data->pendingCheckpoints.begin());
		}

		// Create checkpoint if the pending request version is within (startOldestVersion, desiredVersion].
		// Versions newer than the checkpoint version won't be committed before the checkpoint is created.
		state bool requireCheckpoint = false;
		if (!data->pendingCheckpoints.empty()) {
			const Version cVer = data->pendingCheckpoints.begin()->first;
			if (cVer <= desiredVersion) {
				TraceEvent(SevDebug, "CheckpointVersionSatisfied", data->thisServerID)
				    .detail("DesiredVersion", desiredVersion)
				    .detail("DurableVersion", data->durableVersion.get())
				    .detail("CheckPointVersion", cVer);
				desiredVersion = cVer;
				requireCheckpoint = true;
			}
		}

		state bool removeKVSRanges = false;
		if (!data->pendingRemoveRanges.empty()) {
			const Version aVer = data->pendingRemoveRanges.begin()->first;
			if (aVer <= desiredVersion) {
				TraceEvent(SevDebug, "RemoveRangeVersionSatisfied", data->thisServerID)
				    .detail("DesiredVersion", desiredVersion)
				    .detail("DurableVersion", data->durableVersion.get())
				    .detail("RemoveRangeVersion", aVer);
				desiredVersion = aVer;
				removeKVSRanges = true;
			}
		}

		state bool addedRanges = false;
		if (!data->pendingAddRanges.empty()) {
			const Version aVer = data->pendingAddRanges.begin()->first;
			if (aVer <= desiredVersion) {
				TraceEvent(SevDebug, "AddRangeVersionSatisfied", data->thisServerID)
				    .detail("DesiredVersion", desiredVersion)
				    .detail("DurableVersion", data->durableVersion.get())
				    .detail("AddRangeVersion", aVer);
				desiredVersion = aVer;
				ASSERT(!data->pendingAddRanges.begin()->second.empty());
				TraceEvent(SevVerbose, "SSAddKVSRangeBegin", data->thisServerID)
				    .detail("Version", data->pendingAddRanges.begin()->first)
				    .detail("DurableVersion", data->durableVersion.get())
				    .detail("NewRanges", describe(data->pendingAddRanges.begin()->second));
				state std::vector<Future<Void>> fAddRanges;
				for (const auto& shard : data->pendingAddRanges.begin()->second) {
					TraceEvent(SevInfo, "SSAddKVSRange", data->thisServerID)
					    .detail("Range", shard.range)
					    .detail("PhysicalShardID", shard.shardId);
					fAddRanges.push_back(data->storage.addRange(shard.range, shard.shardId));
				}
				wait(waitForAll(fAddRanges));
				TraceEvent(SevVerbose, "SSAddKVSRangeEnd", data->thisServerID)
				    .detail("Version", data->pendingAddRanges.begin()->first)
				    .detail("DurableVersion", data->durableVersion.get());
				addedRanges = true;
				// Remove commit byte limit to make sure the private mutaiton(s) associated with the
				// `addRange` are committed.
				unlimitedCommitBytes = UnlimitedCommitBytes::True;
			}
		}

		// Write mutations to storage until we reach the desiredVersion or have written too much (bytesleft)
		state double beforeStorageUpdates = now();
		loop {
			state bool done = data->storage.makeVersionMutationsDurable(
			    newOldestVersion, desiredVersion, bytesLeft, unlimitedCommitBytes);
			if (data->tenantMap.getLatestVersion() < newOldestVersion) {
				data->tenantMap.createNewVersion(newOldestVersion);
			}
			// We want to forget things from these data structures atomically with changing oldestVersion (and
			// "before", since oldestVersion.set() may trigger waiting actors) forgetVersionsBeforeAsync visibly
			// forgets immediately (without waiting) but asynchronously frees memory.
			Future<Void> finishedForgetting =
			    data->mutableData().forgetVersionsBeforeAsync(newOldestVersion, TaskPriority::UpdateStorage) &&
			    data->tenantMap.forgetVersionsBeforeAsync(newOldestVersion, TaskPriority::UpdateStorage);
			data->oldestVersion.set(newOldestVersion);
			wait(finishedForgetting);
			wait(yield(TaskPriority::UpdateStorage));
			if (done)
				break;
		}

		recentCommitStats.back().mutationBytes = SERVER_KNOBS->STORAGE_COMMIT_BYTES - bytesLeft;
		recentCommitStats.back().beforeStorageUpdates = beforeStorageUpdates;

		// Allow data fetch to use an additional bytesLeft but don't penalize fetch budget if bytesLeft is negative
		if (SERVER_KNOBS->STORAGE_FETCH_KEYS_USE_COMMIT_BUDGET && bytesLeft > 0) {
			data->fetchKeysBytesBudget += bytesLeft;
			data->fetchKeysBudgetUsed.set(data->fetchKeysBytesBudget <= 0);

			// Dependng on how negative the fetchKeys budget was it could still be used up
			if (!data->fetchKeysBudgetUsed.get()) {
				wait(durableDelay || data->fetchKeysBudgetUsed.onChange());
			}
		}

		if (removeKVSRanges) {
			TraceEvent(SevDebug, "RemoveKVSRangesVersionDurable", data->thisServerID)
			    .detail("NewDurableVersion", newOldestVersion)
			    .detail("DesiredVersion", desiredVersion)
			    .detail("OldestRemoveKVSRangesVersion", data->pendingRemoveRanges.begin()->first);
			ASSERT(newOldestVersion <= data->pendingRemoveRanges.begin()->first);
			if (newOldestVersion == data->pendingRemoveRanges.begin()->first) {
				for (const auto& range : data->pendingRemoveRanges.begin()->second) {
					data->storage.persistRangeMapping(range, false);
				}
			}
		}

		if (addedRanges) {
			TraceEvent(SevVerbose, "SSAddKVSRangeMetaData", data->thisServerID)
			    .detail("NewDurableVersion", newOldestVersion)
			    .detail("DesiredVersion", desiredVersion)
			    .detail("OldestRemoveKVSRangesVersion", data->pendingAddRanges.begin()->first);
			ASSERT(newOldestVersion == data->pendingAddRanges.begin()->first);
			ASSERT(newOldestVersion == desiredVersion);
			for (const auto& shard : data->pendingAddRanges.begin()->second) {
				data->storage.persistRangeMapping(shard.range, true);
			}
			data->pendingAddRanges.erase(data->pendingAddRanges.begin());
		}

		// Handle MoveInShard::MoveInUpdates.
		TraceEvent(SevVerbose, "MoveInUpdatesPrePersist", data->thisServerID)
		    .detail("NewOldestVersion", newOldestVersion)
		    .detail("StartOldestVersion", startOldestVersion);
		for (const auto& [_, moveInShard] : data->moveInShards) {
			const auto& queue = moveInShard->updates->getUpdatesQueue();
			for (auto it = queue.begin(); it != queue.end(); ++it) {
				if (it->version > newOldestVersion) {
					break;
				}
				if (it->version > startOldestVersion) {
					TraceEvent(SevDebug, "MoveInUpdatesPersist", moveInShard->id())
					    .detail("MoveInShard", moveInShard->toString())
					    .detail("Version", it->version)
					    .detail("Mutations", it->mutations.size());
					data->storage.writeKeyValue(
					    KeyValueRef(persistUpdatesKey(moveInShard->id(), it->version),
					                BinaryWriter::toValue<VerUpdateRef>(*it, IncludeVersion())));
				}
			}
		}

		std::set<Key> modifiedChangeFeeds = data->fetchingChangeFeeds;
		data->fetchingChangeFeeds.clear();
		while (!data->changeFeedVersions.empty() && data->changeFeedVersions.front().second <= newOldestVersion) {
			modifiedChangeFeeds.insert(data->changeFeedVersions.front().first.begin(),
			                           data->changeFeedVersions.front().first.end());
			data->changeFeedVersions.pop_front();
		}

		state std::vector<std::pair<Key, Version>> feedFetchVersions;

		state std::vector<Key> updatedChangeFeeds(modifiedChangeFeeds.begin(), modifiedChangeFeeds.end());
		state int curFeed = 0;
		state int64_t durableChangeFeedMutations = 0;
		while (curFeed < updatedChangeFeeds.size()) {
			auto info = data->uidChangeFeed.find(updatedChangeFeeds[curFeed]);
			if (info != data->uidChangeFeed.end()) {
				// Cannot yield in mutation updating loop because of race with fetchVersion
				Version alreadyFetched = std::max(info->second->fetchVersion, info->second->durableFetchVersion.get());
				if (info->second->removing) {
					auto cleanupPending = data->changeFeedCleanupDurable.find(info->second->id);
					if (cleanupPending != data->changeFeedCleanupDurable.end() &&
					    cleanupPending->second <= newOldestVersion) {
						// due to a race, we just applied a cleanup mutation, but feed updates happen just after.
						// Don't write any mutations for this feed.
						curFeed++;
						continue;
					}
				}
				for (auto& it : info->second->mutations) {
					if (it.version <= alreadyFetched) {
						continue;
					} else if (it.version > newOldestVersion) {
						break;
					}
					data->storage.writeKeyValue(
					    KeyValueRef(changeFeedDurableKey(info->second->id, it.version),
					                changeFeedDurableValue(it.encrypted.present() ? it.encrypted.get() : it.mutations,
					                                       it.knownCommittedVersion)));
					// FIXME: there appears to be a bug somewhere where the exact same mutation appears twice in a
					// row in the stream. We should fix this assert to be strictly > and re-enable it
					ASSERT(it.version >= info->second->storageVersion);
					info->second->storageVersion = it.version;
					durableChangeFeedMutations++;
				}

				if (info->second->fetchVersion != invalidVersion && !info->second->removing) {
					feedFetchVersions.push_back(std::pair(info->second->id, info->second->fetchVersion));
				}
				// handle case where fetch had version ahead of last in-memory mutation
				if (alreadyFetched > info->second->storageVersion) {
					info->second->storageVersion = std::min(alreadyFetched, newOldestVersion);
					if (alreadyFetched > info->second->storageVersion) {
						// This change feed still has pending mutations fetched and written to storage that are
						// higher than the new durableVersion. To ensure its storage and durable version get
						// updated, we need to add it back to fetchingChangeFeeds
						data->fetchingChangeFeeds.insert(info->first);
					}
				}
				wait(yield(TaskPriority::UpdateStorage));
			}
			curFeed++;
		}

		// Set the new durable version as part of the outstanding change set, before commit
		if (startOldestVersion != newOldestVersion)
			data->storage.makeVersionDurable(newOldestVersion);
		data->storageUpdatesDurableLatencyHistogram->sampleSeconds(now() - beforeStorageUpdates);
		data->fetchKeysHistograms.bytesPerCommit->sample(data->fetchKeysTotalCommitBytes);
		recentCommitStats.back().fetchKeyBytes = data->fetchKeysTotalCommitBytes;
		data->fetchKeysTotalCommitBytes = 0;

		debug_advanceMaxCommittedVersion(data->thisServerID, newOldestVersion);
		state double beforeStorageCommit = now();
		recentCommitStats.back().beforeStorageCommit = beforeStorageCommit;
		wait(data->storage.canCommit());
		state Future<Void> durable = data->storage.commit();
		++data->counters.kvCommits;
		recentCommitStats.back().seqId = data->counters.kvCommits.getValue();

		// If the mutation bytes budget was not fully used then wait some time before the next commit
		durableDelay =
		    (bytesLeft > 0) ? delay(SERVER_KNOBS->STORAGE_COMMIT_INTERVAL, TaskPriority::UpdateStorage) : Void();

		recentCommitStats.back().whenCommit = now();
		try {
			loop {
				choose {
					when(wait(ioTimeoutErrorIfCleared(durable,
					                                  SERVER_KNOBS->MAX_STORAGE_COMMIT_TIME,
					                                  data->getEncryptCipherKeysMonitor->degraded(),
					                                  "StorageCommit"))) {
						break;
					}
					when(wait(delay(60.0))) {
						TraceEvent(SevWarn, "CommitTooLong", data->thisServerID)
						    .detail("FetchBytes", data->fetchKeysTotalCommitBytes)
						    .detail("CommitBytes", SERVER_KNOBS->STORAGE_COMMIT_BYTES - bytesLeft);

						if (data->storage.getKeyValueStoreType() == KeyValueStoreType::SSD_SHARDED_ROCKSDB &&
						    SERVER_KNOBS->LOGGING_ROCKSDB_BG_WORK_WHEN_IO_TIMEOUT) {
							data->storage.logRecentRocksDBBackgroundWorkStats(data->thisServerID, "CommitTooLong");
						}
					}
				}
			}
		} catch (Error& e) {
			if (e.code() == error_code_io_timeout) {
				if (SERVER_KNOBS->LOGGING_STORAGE_COMMIT_WHEN_IO_TIMEOUT) {
					recentCommitStats.back().incompleteCommitDuration = now() - recentCommitStats.back().whenCommit;
					for (const auto& commitStats : recentCommitStats) {
						commitStats.log(data->thisServerID, "I/O timeout error");
					}
				}
				if (data->storage.getKeyValueStoreType() == KeyValueStoreType::SSD_SHARDED_ROCKSDB &&
				    SERVER_KNOBS->LOGGING_ROCKSDB_BG_WORK_WHEN_IO_TIMEOUT) {
					data->storage.logRecentRocksDBBackgroundWorkStats(data->thisServerID, "I/O timeout error");
				}
			}
			throw e;
		}
		recentCommitStats.back().commitDuration = now() - recentCommitStats.back().whenCommit;
		recentCommitStats.back().duration = now() - beforeStorageCommit;

		if (SERVER_KNOBS->LOGGING_COMPLETE_STORAGE_COMMIT_PROBABILITY > 0 &&
		    deterministicRandom()->random01() < SERVER_KNOBS->LOGGING_COMPLETE_STORAGE_COMMIT_PROBABILITY) {
			recentCommitStats.back().log(data->thisServerID, "normal");
		}
		if (data->storage.getKeyValueStoreType() == KeyValueStoreType::SSD_SHARDED_ROCKSDB &&
		    SERVER_KNOBS->LOGGING_ROCKSDB_BG_WORK_PROBABILITY > 0 &&
		    deterministicRandom()->random01() < SERVER_KNOBS->LOGGING_ROCKSDB_BG_WORK_PROBABILITY) {
			data->storage.logRecentRocksDBBackgroundWorkStats(data->thisServerID, "normal");
		}

		data->storageCommitLatencyHistogram->sampleSeconds(now() - beforeStorageCommit);

		debug_advanceMinCommittedVersion(data->thisServerID, data->storageMinRecoverVersion);

		if (removeKVSRanges) {
			TraceEvent(SevDebug, "RemoveKVSRangesComitted", data->thisServerID)
			    .detail("NewDurableVersion", newOldestVersion)
			    .detail("DesiredVersion", desiredVersion)
			    .detail("OldestRemoveKVSRangesVersion", data->pendingRemoveRanges.begin()->first);
			ASSERT(newOldestVersion <= data->pendingRemoveRanges.begin()->first);
			if (newOldestVersion == data->pendingRemoveRanges.begin()->first) {
				for (const auto& range : data->pendingRemoveRanges.begin()->second) {
					data->storage.removeRange(range);
				}
				data->pendingRemoveRanges.erase(data->pendingRemoveRanges.begin());
			}
			removeKVSRanges = false;
		}

		if (requireCheckpoint) {
			// `pendingCheckpoints` is a queue of checkpoint requests ordered by their versions, and
			// `newOldestVersion` is chosen such that it is no larger than the smallest pending checkpoint
			// version. When the exact desired checkpoint version is committed, updateStorage() is blocked
			// and a checkpoint will be created at that version from the underlying storage engine.
			// Note a pending checkpoint is only dequeued after the corresponding checkpoint is created
			// successfully.
			TraceEvent(SevDebug, "CheckpointVersionDurable", data->thisServerID)
			    .detail("NewDurableVersion", newOldestVersion)
			    .detail("DesiredVersion", desiredVersion)
			    .detail("SmallestCheckPointVersion", data->pendingCheckpoints.begin()->first);
			// newOldestVersion could be smaller than the desired version due to byte limit.
			ASSERT(newOldestVersion <= data->pendingCheckpoints.begin()->first);
			if (newOldestVersion == data->pendingCheckpoints.begin()->first) {
				std::vector<Future<Void>> createCheckpoints;
				// TODO: Combine these checkpoints if necessary.
				for (int idx = 0; idx < data->pendingCheckpoints.begin()->second.size(); ++idx) {
					createCheckpoints.push_back(createCheckpoint(data, data->pendingCheckpoints.begin()->second[idx]));
				}
				wait(waitForAll(createCheckpoints));
				// Erase the pending checkpoint after the checkpoint has been created successfully.
				ASSERT(newOldestVersion == data->pendingCheckpoints.begin()->first);
				data->pendingCheckpoints.erase(data->pendingCheckpoints.begin());
			}
			requireCheckpoint = false;
		}

		if (newOldestVersion > data->rebootAfterDurableVersion) {
			TraceEvent("RebootWhenDurableTriggered", data->thisServerID)
			    .detail("NewOldestVersion", newOldestVersion)
			    .detail("RebootAfterDurableVersion", data->rebootAfterDurableVersion);
			CODE_PROBE(true, "SS rebooting after durable");
			// To avoid brokenPromise error, which is caused by the sender of the durableInProgress (i.e., this
			// process) never sets durableInProgress, we should set durableInProgress before send the
			// please_reboot() error. Otherwise, in the race situation when storage server receives both reboot and
			// brokenPromise of durableInProgress, the worker of the storage server will die.
			// We will eventually end up with no worker for storage server role.
			// The data distributor's buildTeam() will get stuck in building a team
			durableInProgress.sendError(please_reboot());
			throw please_reboot();
		}

		curFeed = 0;
		while (curFeed < updatedChangeFeeds.size()) {
			auto info = data->uidChangeFeed.find(updatedChangeFeeds[curFeed]);
			if (info != data->uidChangeFeed.end()) {
				while (!info->second->mutations.empty() && info->second->mutations.front().version < newOldestVersion) {
					info->second->mutations.pop_front();
				}
				ASSERT(info->second->storageVersion >= info->second->durableVersion);
				info->second->durableVersion = info->second->storageVersion;
				wait(yield(TaskPriority::UpdateStorage));
			}
			curFeed++;
		}

		// if commit included fetched data from this change feed, update the fetched durable version
		curFeed = 0;
		while (curFeed < feedFetchVersions.size()) {
			auto info = data->uidChangeFeed.find(feedFetchVersions[curFeed].first);
			// Don't update if the feed is pending cleanup. Either it will get cleaned up and destroyed, or it will
			// get fetched again, where the fetch version will get reset.
			if (info != data->uidChangeFeed.end() && !data->changeFeedCleanupDurable.contains(info->second->id)) {
				if (feedFetchVersions[curFeed].second > info->second->durableFetchVersion.get()) {
					info->second->durableFetchVersion.set(feedFetchVersions[curFeed].second);
				}
				if (feedFetchVersions[curFeed].second == info->second->fetchVersion) {
					// haven't fetched anything else since commit started, reset fetch version
					info->second->fetchVersion = invalidVersion;
				}
			}
			curFeed++;
		}

		// remove any entries from changeFeedCleanupPending that were persisted
		auto cfCleanup = data->changeFeedCleanupDurable.begin();
		while (cfCleanup != data->changeFeedCleanupDurable.end()) {
			if (cfCleanup->second <= newOldestVersion) {
				// remove from the data structure here, if it wasn't added back by another fetch or something
				auto feed = data->uidChangeFeed.find(cfCleanup->first);
				ASSERT(feed != data->uidChangeFeed.end());
				if (feed->second->removing) {
					auto rs = data->keyChangeFeed.modify(feed->second->range);
					for (auto r = rs.begin(); r != rs.end(); ++r) {
						auto& feedList = r->value();
						for (int i = 0; i < feedList.size(); i++) {
							if (feedList[i]->id == cfCleanup->first) {
								swapAndPop(&feedList, i--);
							}
						}
					}
					data->keyChangeFeed.coalesce(feed->second->range.contents());

					data->uidChangeFeed.erase(feed);
				} else {
					CODE_PROBE(true, "Feed re-fetched after remove");
				}
				cfCleanup = data->changeFeedCleanupDurable.erase(cfCleanup);
			} else {
				cfCleanup++;
			}
		}

		data->counters.changeFeedMutationsDurable += durableChangeFeedMutations;

		durableInProgress.send(Void());
		wait(delay(0, TaskPriority::UpdateStorage)); // Setting durableInProgess could cause the storage server to
		                                             // shut down, so delay to check for cancellation

		// Taking and releasing the durableVersionLock ensures that no eager reads both begin before the commit was
		// effective and are applied after we change the durable version. Also ensure that we have to lock while
		// calling changeDurableVersion, because otherwise the latest version of mutableData might be partially
		// loaded.
		state double beforeSSDurableVersionUpdate = now();
		wait(data->durableVersionLock.take());
		data->popVersion(data->storageMinRecoverVersion + 1);

		while (!changeDurableVersion(data, newOldestVersion)) {
			if (g_network->check_yield(TaskPriority::UpdateStorage)) {
				data->durableVersionLock.release();
				wait(delay(0, TaskPriority::UpdateStorage));
				wait(data->durableVersionLock.take());
			}
		}

		data->durableVersionLock.release();
		data->ssDurableVersionUpdateLatencyHistogram->sampleSeconds(now() - beforeSSDurableVersionUpdate);

		//TraceEvent("StorageServerDurable", data->thisServerID).detail("Version", newOldestVersion);
		if (data->shardAware) {
			data->fetchKeysBytesBudget = SERVER_KNOBS->STORAGE_ROCKSDB_FETCH_BYTES;
		} else {
			data->fetchKeysBytesBudget = SERVER_KNOBS->STORAGE_FETCH_BYTES;
		}

		data->fetchKeysBudgetUsed.set(false);
		if (!data->fetchKeysBudgetUsed.get()) {
			wait(durableDelay || data->fetchKeysBudgetUsed.onChange());
		}

		data->fetchKeysLimiter.settle();
	}
}

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

////////////////////////////////// StorageServerDisk ///////////////////////////////////////
#ifndef __INTEL_COMPILER
#pragma region StorageServerDisk
#endif

void StorageServerDisk::makeNewStorageServerDurable(const bool shardAware) {
	if (shardAware) {
		storage->set(persistShardAwareFormat);
	} else {
		storage->set(persistFormat);
	}
	storage->set(KeyValueRef(persistID, BinaryWriter::toValue(data->thisServerID, Unversioned())));
	if (data->tssPairID.present()) {
		storage->set(KeyValueRef(persistTssPairID, BinaryWriter::toValue(data->tssPairID.get(), Unversioned())));
	}
	storage->set(KeyValueRef(persistVersion, BinaryWriter::toValue(data->version.get(), Unversioned())));

	if (shardAware) {
		storage->set(KeyValueRef(persistStorageServerShardKeys.begin.toString(),
		                         ObjectWriter::toValue(StorageServerShard::notAssigned(allKeys, 0), IncludeVersion())));
	} else {
		storage->set(KeyValueRef(persistShardAssignedKeys.begin.toString(), "0"_sr));
		storage->set(KeyValueRef(persistShardAvailableKeys.begin.toString(), "0"_sr));
		storage->set(
		    KeyValueRef(persistBulkLoadTaskKeys.begin.toString(), ssBulkLoadMetadataValue(SSBulkLoadMetadata())));
	}

	auto view = data->tenantMap.atLatest();
	for (auto itr = view.begin(); itr != view.end(); ++itr) {
		auto val = ObjectWriter::toValue(*itr, IncludeVersion());
		storage->set(KeyValueRef(TenantAPI::idToPrefix(itr.key()).withPrefix(persistTenantMapKeys.begin), val));
	}
}

void setAvailableStatus(StorageServer* self, KeyRangeRef keys, bool available) {
	// ASSERT( self->debug_inApplyUpdate );
	ASSERT(!keys.empty());

	Version logV = self->data().getLatestVersion();
	auto& mLV = self->addVersionToMutationLog(logV);

	KeyRange availableKeys = KeyRangeRef(persistShardAvailableKeys.begin.toString() + keys.begin.toString(),
	                                     persistShardAvailableKeys.begin.toString() + keys.end.toString());
	//TraceEvent("SetAvailableStatus", self->thisServerID).detail("Version", mLV.version).detail("RangeBegin", availableKeys.begin).detail("RangeEnd", availableKeys.end);

	self->addMutationToMutationLog(mLV, MutationRef(MutationRef::ClearRange, availableKeys.begin, availableKeys.end));
	++self->counters.kvSystemClearRanges;
	self->addMutationToMutationLog(
	    mLV, MutationRef(MutationRef::SetValue, availableKeys.begin, available ? "1"_sr : "0"_sr));
	if (keys.end != allKeys.end) {
		bool endAvailable = self->shards.rangeContaining(keys.end)->value()->isReadWritePending();
		self->addMutationToMutationLog(
		    mLV, MutationRef(MutationRef::SetValue, availableKeys.end, endAvailable ? "1"_sr : "0"_sr));
	}

	if (BUGGIFY) {
		self->maybeInjectTargetedRestart(logV);
	}
}

void updateStorageShard(StorageServer* data, StorageServerShard shard) {
	StorageServerShard::ShardState shardState = shard.getShardState();
	// Added to evaluate the invariant: Only the following four state can be seen in the storage shard metadata.
	ASSERT_WE_THINK(shardState == StorageServerShard::NotAssigned || shardState == StorageServerShard::Adding ||
	                shardState == StorageServerShard::MovingIn || shardState == StorageServerShard::ReadWrite);
	auto& mLV = data->addVersionToMutationLog(data->data().getLatestVersion());

	KeyRange shardKeys = KeyRangeRef(persistStorageServerShardKeys.begin.toString() + shard.range.begin.toString(),
	                                 persistStorageServerShardKeys.begin.toString() + shard.range.end.toString());
	TraceEvent(SevVerbose, "UpdateStorageServerShard", data->thisServerID)
	    .detail("Version", mLV.version)
	    .detail("Shard", shard.toString())
	    .detail("ShardKey", shardKeys.begin);

	data->addMutationToMutationLog(mLV, MutationRef(MutationRef::ClearRange, shardKeys.begin, shardKeys.end));
	++data->counters.kvSystemClearRanges;
	data->addMutationToMutationLog(
	    mLV, MutationRef(MutationRef::SetValue, shardKeys.begin, ObjectWriter::toValue(shard, IncludeVersion())));
	if (shard.range.end != allKeys.end) {
		StorageServerShard endShard = data->shards.rangeContaining(shard.range.end)->value()->toStorageServerShard();
		if (endShard.getShardState() == StorageServerShard::ReadWritePending) {
			endShard.setShardState(StorageServerShard::ReadWrite);
		}
		TraceEvent(SevVerbose, "UpdateStorageServerShardEndShard", data->thisServerID)
		    .detail("Version", mLV.version)
		    .detail("Shard", endShard.toString())
		    .detail("ShardKey", shardKeys.end);
		data->addMutationToMutationLog(
		    mLV, MutationRef(MutationRef::SetValue, shardKeys.end, ObjectWriter::toValue(endShard, IncludeVersion())));
	}
}

void setAssignedStatus(StorageServer* self, KeyRangeRef keys, bool nowAssigned) {
	ASSERT(!keys.empty());
	Version logV = self->data().getLatestVersion();
	auto& mLV = self->addVersionToMutationLog(logV);
	KeyRange assignedKeys = KeyRangeRef(persistShardAssignedKeys.begin.toString() + keys.begin.toString(),
	                                    persistShardAssignedKeys.begin.toString() + keys.end.toString());
	//TraceEvent("SetAssignedStatus", self->thisServerID).detail("Version", mLV.version).detail("RangeBegin", assignedKeys.begin).detail("RangeEnd", assignedKeys.end);
	self->addMutationToMutationLog(mLV, MutationRef(MutationRef::ClearRange, assignedKeys.begin, assignedKeys.end));
	++self->counters.kvSystemClearRanges;
	self->addMutationToMutationLog(
	    mLV, MutationRef(MutationRef::SetValue, assignedKeys.begin, nowAssigned ? "1"_sr : "0"_sr));
	if (keys.end != allKeys.end) {
		bool endAssigned = self->shards.rangeContaining(keys.end)->value()->assigned();
		self->addMutationToMutationLog(
		    mLV, MutationRef(MutationRef::SetValue, assignedKeys.end, endAssigned ? "1"_sr : "0"_sr));
	}

	if (BUGGIFY) {
		self->maybeInjectTargetedRestart(logV);
	}
}

void setRangeBasedBulkLoadStatus(StorageServer* self, KeyRangeRef keys, const SSBulkLoadMetadata& ssBulkLoadMetadata) {
	ASSERT(!keys.empty());
	Version logV = self->data().getLatestVersion();
	auto& mLV = self->addVersionToMutationLog(logV);
	KeyRange dataMoveKeys = keys.withPrefix(persistBulkLoadTaskKeys.begin);
	//TraceEvent("SetRangeBasedBulkLoadStatus", self->thisServerID).detail("Version", mLV.version).detail("RangeBegin", dataMoveKeys.begin).detail("RangeEnd", dataMoveKeys.end);
	self->addMutationToMutationLog(mLV, MutationRef(MutationRef::ClearRange, dataMoveKeys.begin, dataMoveKeys.end));
	++self->counters.kvSystemClearRanges;
	self->addMutationToMutationLog(
	    mLV, MutationRef(MutationRef::SetValue, dataMoveKeys.begin, ssBulkLoadMetadataValue(ssBulkLoadMetadata)));
	if (keys.end != allKeys.end) {
		SSBulkLoadMetadata endBulkLoadMetadata =
		    self->shards.rangeContaining(keys.end)->value()->getSSBulkLoadMetadata();
		self->addMutationToMutationLog(
		    mLV, MutationRef(MutationRef::SetValue, dataMoveKeys.end, ssBulkLoadMetadataValue(endBulkLoadMetadata)));
	}
	if (BUGGIFY) {
		self->maybeInjectTargetedRestart(logV);
	}
}

void StorageServerDisk::clearRange(KeyRangeRef keys) {
	storage->clear(keys);
	++(*kvClearRanges);
	if (keys.singleKeyRange()) {
		++(*kvClearSingleKey);
	}
}

void StorageServerDisk::writeKeyValue(KeyValueRef kv) {
	storage->set(kv);
	*kvCommitLogicalBytes += kv.expectedSize();
}

void StorageServerDisk::writeMutation(MutationRef mutation) {
	if (mutation.type == MutationRef::SetValue) {
		storage->set(KeyValueRef(mutation.param1, mutation.param2));
		*kvCommitLogicalBytes += mutation.expectedSize();
	} else if (mutation.type == MutationRef::ClearRange) {
		storage->clear(KeyRangeRef(mutation.param1, mutation.param2));
		++(*kvClearRanges);
		if (KeyRangeRef(mutation.param1, mutation.param2).singleKeyRange()) {
			++(*kvClearSingleKey);
		}
	} else
		ASSERT(false);
}

void StorageServerDisk::writeMutationsBuggy(const VectorRef<MutationRef>& mutations,
                                            Version debugVersion,
                                            const char* debugContext) {
	auto bug = SimBugInjector().get<StorageCorruptionBug>(StorageCorruptionBugID());
	if (!bug) {
		writeMutations(mutations, debugVersion, debugContext);
	}
	int begin = 0;
	while (begin < mutations.size()) {
		int i;
		for (i = begin; i < mutations.size(); ++i) {
			if (deterministicRandom()->random01() < bug->corruptionProbability) {
				bug->hit();
				break;
			}
		}
		writeMutations(mutations.slice(begin, i), debugVersion, debugContext);
		// we want to drop the mutation at i (unless i == mutations.size(), in which case this will just finish the
		// loop)
		begin = i + 1;
	}
}

void StorageServerDisk::writeMutations(const VectorRef<MutationRef>& mutations,
                                       Version debugVersion,
                                       const char* debugContext) {
	for (const auto& m : mutations) {
		DEBUG_MUTATION(debugContext, debugVersion, m, data->thisServerID);
		ASSERT(m.validateChecksum());
		if (m.type == MutationRef::SetValue) {
			storage->set(KeyValueRef(m.param1, m.param2));
			*kvCommitLogicalBytes += m.expectedSize();
		} else if (m.type == MutationRef::ClearRange) {
			storage->clear(KeyRangeRef(m.param1, m.param2));
			++(*kvClearRanges);
			if (KeyRangeRef(m.param1, m.param2).singleKeyRange()) {
				++(*kvClearSingleKey);
			}
		}
	}
}

bool StorageServerDisk::makeVersionMutationsDurable(Version& prevStorageVersion,
                                                    Version newStorageVersion,
                                                    int64_t& bytesLeft,
                                                    UnlimitedCommitBytes unlimitedCommitBytes) {
	if (!unlimitedCommitBytes && bytesLeft <= 0)
		return true;

	// Apply mutations from the mutationLog
	auto u = data->getMutationLog().upper_bound(prevStorageVersion);
	if (u != data->getMutationLog().end() && u->first <= newStorageVersion) {
		VerUpdateRef const& v = u->second;
		ASSERT(v.version > prevStorageVersion && v.version <= newStorageVersion);
		// TODO(alexmiller): Update to version tracking.
		// DEBUG_KEY_RANGE("makeVersionMutationsDurable", v.version, KeyRangeRef());
		if (!SimBugInjector().isEnabled()) {
			writeMutations(v.mutations, v.version, "makeVersionDurable");
		} else {
			writeMutationsBuggy(v.mutations, v.version, "makeVersionDurable");
		}
		for (const auto& m : v.mutations)
			bytesLeft -= mvccStorageBytes(m);
		prevStorageVersion = v.version;
		return false;
	} else {
		prevStorageVersion = newStorageVersion;
		return true;
	}
}

// Update data->storage to persist the changes from (data->storageVersion(),version]
void StorageServerDisk::makeVersionDurable(Version version) {
	storage->set(KeyValueRef(persistVersion, BinaryWriter::toValue(version, Unversioned())));
	*kvCommitLogicalBytes += persistVersion.expectedSize() + sizeof(Version);

	// TraceEvent("MakeDurable", data->thisServerID)
	//     .detail("FromVersion", prevStorageVersion)
	//     .detail("ToVersion", version);
}

// Update data->storage to persist tss quarantine state
void StorageServerDisk::makeTssQuarantineDurable() {
	storage->set(KeyValueRef(persistTssQuarantine, "1"_sr));
}

void StorageServerDisk::changeLogProtocol(Version version, ProtocolVersion protocol) {
	data->addMutationToMutationLogOrStorage(
	    version,
	    MutationRef(MutationRef::SetValue, persistLogProtocol, BinaryWriter::toValue(protocol, Unversioned())));
}

ACTOR Future<Void> applyByteSampleResult(StorageServer* data,
                                         IKeyValueStore* storage,
                                         Key begin,
                                         Key end,
                                         std::vector<Standalone<VectorRef<KeyValueRef>>>* results = nullptr) {
	state int totalFetches = 0;
	state int totalKeys = 0;
	state int totalBytes = 0;
	state ReadOptions readOptions(ReadType::NORMAL, CacheResult::False);

	loop {
		RangeResult bs = wait(storage->readRange(KeyRangeRef(begin, end),
		                                         SERVER_KNOBS->STORAGE_LIMIT_BYTES,
		                                         SERVER_KNOBS->STORAGE_LIMIT_BYTES,
		                                         readOptions));
		if (results) {
			results->push_back(bs.castTo<VectorRef<KeyValueRef>>());
			data->bytesRestored += bs.logicalSize();
			data->counters.kvScanBytes += bs.logicalSize();
		}
		int rangeSize = bs.expectedSize();
		totalFetches++;
		totalKeys += bs.size();
		totalBytes += rangeSize;
		for (int j = 0; j < bs.size(); j++) {
			KeyRef key = bs[j].key.removePrefix(persistByteSampleKeys.begin);
			if (!data->byteSampleClears.rangeContaining(key).value()) {
				data->metrics.byteSample.sample.insert(
				    key, BinaryReader::fromStringRef<int32_t>(bs[j].value, Unversioned()), false);
			}
		}
		if (rangeSize >= SERVER_KNOBS->STORAGE_LIMIT_BYTES) {
			Key nextBegin = keyAfter(bs.back().key);
			data->byteSampleClears.insert(KeyRangeRef(begin, nextBegin).removePrefix(persistByteSampleKeys.begin),
			                              true);
			data->byteSampleClearsTooLarge.set(data->byteSampleClears.size() >
			                                   SERVER_KNOBS->MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE);
			begin = nextBegin;
			if (begin == end) {
				break;
			}
		} else {
			data->byteSampleClears.insert(KeyRangeRef(begin.removePrefix(persistByteSampleKeys.begin),
			                                          end == persistByteSampleKeys.end
			                                              ? "\xff\xff\xff"_sr
			                                              : end.removePrefix(persistByteSampleKeys.begin)),
			                              true);
			data->byteSampleClearsTooLarge.set(data->byteSampleClears.size() >
			                                   SERVER_KNOBS->MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE);
			break;
		}

		if (!results) {
			wait(delay(SERVER_KNOBS->BYTE_SAMPLE_LOAD_DELAY));
		}
	}
	TraceEvent("RecoveredByteSampleRange", data->thisServerID)
	    .detail("Begin", begin)
	    .detail("End", end)
	    .detail("Fetches", totalFetches)
	    .detail("Keys", totalKeys)
	    .detail("ReadBytes", totalBytes);
	return Void();
}

ACTOR Future<Void> restoreByteSample(StorageServer* data,
                                     IKeyValueStore* storage,
                                     Promise<Void> byteSampleSampleRecovered,
                                     Future<Void> startRestore) {
	state std::vector<Standalone<VectorRef<KeyValueRef>>> byteSampleSample;
	wait(applyByteSampleResult(
	    data, storage, persistByteSampleSampleKeys.begin, persistByteSampleSampleKeys.end, &byteSampleSample));
	byteSampleSampleRecovered.send(Void());
	wait(startRestore);
	wait(delay(SERVER_KNOBS->BYTE_SAMPLE_START_DELAY));

	size_t bytes_per_fetch = 0;
	// Since the expected size also includes (as of now) the space overhead of the container, we calculate our own
	// number here
	for (auto& it : byteSampleSample) {
		for (auto& kv : it) {
			bytes_per_fetch += BinaryReader::fromStringRef<int32_t>(kv.value, Unversioned());
		}
	}
	bytes_per_fetch = (bytes_per_fetch / SERVER_KNOBS->BYTE_SAMPLE_LOAD_PARALLELISM) + 1;

	state std::vector<Future<Void>> sampleRanges;
	int accumulatedSize = 0;
	Key lastStart =
	    persistByteSampleKeys.begin; // make sure the first range starts at the absolute beginning of the byte sample
	for (auto& it : byteSampleSample) {
		for (auto& kv : it) {
			if (accumulatedSize >= bytes_per_fetch) {
				accumulatedSize = 0;
				Key realKey = kv.key.removePrefix(persistByteSampleKeys.begin);
				sampleRanges.push_back(applyByteSampleResult(data, storage, lastStart, realKey));
				lastStart = realKey;
			}
			accumulatedSize += BinaryReader::fromStringRef<int32_t>(kv.value, Unversioned());
		}
	}
	// make sure that the last range goes all the way to the end of the byte sample
	sampleRanges.push_back(applyByteSampleResult(data, storage, lastStart, persistByteSampleKeys.end));

	wait(waitForAll(sampleRanges));
	TraceEvent("RecoveredByteSampleChunkedRead", data->thisServerID).detail("Ranges", sampleRanges.size());

	if (BUGGIFY)
		wait(delay(deterministicRandom()->random01() * 10.0));

	return Void();
}

ACTOR Future<bool> restoreDurableState(StorageServer* data, IKeyValueStore* storage) {
	state Future<Optional<Value>> fFormat = storage->readValue(persistFormat.key);
	state Future<Optional<Value>> fID = storage->readValue(persistID);
	state Future<Optional<Value>> ftssPairID = storage->readValue(persistTssPairID);
	state Future<Optional<Value>> fssPairID = storage->readValue(persistSSPairID);
	state Future<Optional<Value>> fTssQuarantine = storage->readValue(persistTssQuarantine);
	state Future<Optional<Value>> fVersion = storage->readValue(persistVersion);
	state Future<Optional<Value>> fLogProtocol = storage->readValue(persistLogProtocol);
	state Future<Optional<Value>> fPrimaryLocality = storage->readValue(persistPrimaryLocality);
	state Future<RangeResult> fShardAssigned = storage->readRange(persistShardAssignedKeys);
	state Future<RangeResult> fShardAvailable = storage->readRange(persistShardAvailableKeys);
	state Future<RangeResult> fChangeFeeds = storage->readRange(persistChangeFeedKeys);
	state Future<RangeResult> fPendingCheckpoints = storage->readRange(persistPendingCheckpointKeys);
	state Future<RangeResult> fCheckpoints = storage->readRange(persistCheckpointKeys);
	state Future<RangeResult> fMoveInShards = storage->readRange(persistMoveInShardsKeyRange());
	state Future<RangeResult> fTenantMap = storage->readRange(persistTenantMapKeys);
	state Future<RangeResult> fStorageShards = storage->readRange(persistStorageServerShardKeys);
	state Future<RangeResult> fAccumulativeChecksum = storage->readRange(persistAccumulativeChecksumKeys);
	state Future<RangeResult> fBulkLoadTask = storage->readRange(persistBulkLoadTaskKeys);

	state Promise<Void> byteSampleSampleRecovered;
	state Promise<Void> startByteSampleRestore;
	data->byteSampleRecovery =
	    restoreByteSample(data, storage, byteSampleSampleRecovered, startByteSampleRestore.getFuture());

	TraceEvent("ReadingDurableState", data->thisServerID).log();
	wait(waitForAll(
	    std::vector{ fFormat, fID, ftssPairID, fssPairID, fTssQuarantine, fVersion, fLogProtocol, fPrimaryLocality }));
	wait(waitForAll(std::vector{ fShardAssigned,
	                             fShardAvailable,
	                             fChangeFeeds,
	                             fPendingCheckpoints,
	                             fCheckpoints,
	                             fMoveInShards,
	                             fTenantMap,
	                             fStorageShards,
	                             fAccumulativeChecksum,
	                             fBulkLoadTask }));
	wait(byteSampleSampleRecovered.getFuture());
	TraceEvent("RestoringDurableState", data->thisServerID).log();

	if (!fFormat.get().present()) {
		// The DB was never initialized
		TraceEvent("DBNeverInitialized", data->thisServerID).log();
		TraceEvent("KVSRemoved", data->thisServerID).detail("Reason", "DBNeverInitialized");
		storage->dispose();
		data->thisServerID = UID();
		data->sk = Key();
		return false;
	} else {
		TraceEvent(SevVerbose, "RestoringStorageServerWithPhysicalShards", data->thisServerID).log();
		data->shardAware = fFormat.get().get() == persistShardAwareFormat.value;
	}
	data->bytesRestored += fFormat.get().expectedSize();
	if (!persistFormatReadableRange.contains(fFormat.get().get())) {
		TraceEvent(SevError, "UnsupportedDBFormat")
		    .detail("Format", fFormat.get().get().toString())
		    .detail("Expected", persistFormat.value.toString());
		throw worker_recovery_failed();
	}
	data->thisServerID = BinaryReader::fromStringRef<UID>(fID.get().get(), Unversioned());
	data->bytesRestored += fID.get().expectedSize();
	if (ftssPairID.get().present()) {
		data->setTssPair(BinaryReader::fromStringRef<UID>(ftssPairID.get().get(), Unversioned()));
		data->bytesRestored += ftssPairID.get().expectedSize();
	}

	if (fssPairID.get().present()) {
		data->setSSWithTssPair(BinaryReader::fromStringRef<UID>(fssPairID.get().get(), Unversioned()));
		data->bytesRestored += fssPairID.get().expectedSize();
	}

	// It's a bit sketchy to rely on an untrusted storage engine to persist its quarantine state when the quarantine
	// state means the storage engine already had a durability or correctness error, but it should get
	// re-quarantined very quickly because of a mismatch if it starts trying to do things again
	if (fTssQuarantine.get().present()) {
		CODE_PROBE(true, "TSS restarted while quarantined", probe::decoration::rare);
		data->tssInQuarantine = true;
		data->bytesRestored += fTssQuarantine.get().expectedSize();
	}

	data->sk = serverKeysPrefixFor((data->tssPairID.present()) ? data->tssPairID.get() : data->thisServerID)
	               .withPrefix(systemKeys.begin); // FFFF/serverKeys/[this server]/

	if (fLogProtocol.get().present()) {
		data->logProtocol = BinaryReader::fromStringRef<ProtocolVersion>(fLogProtocol.get().get(), Unversioned());
		data->bytesRestored += fLogProtocol.get().expectedSize();
	}

	if (fPrimaryLocality.get().present()) {
		data->primaryLocality = BinaryReader::fromStringRef<int8_t>(fPrimaryLocality.get().get(), Unversioned());
		data->bytesRestored += fPrimaryLocality.get().expectedSize();
	}

	state Version version = BinaryReader::fromStringRef<Version>(fVersion.get().get(), Unversioned());
	debug_checkRestoredVersion(data->thisServerID, version, "StorageServer");
	data->setInitialVersion(version);
	data->bytesRestored += fVersion.get().expectedSize();

	TraceEvent(SevInfo, "StorageServerRestoreVersion", data->thisServerID).detail("Version", version);

	state RangeResult pendingCheckpoints = fPendingCheckpoints.get();
	state int pCLoc;
	for (pCLoc = 0; pCLoc < pendingCheckpoints.size(); ++pCLoc) {
		CheckpointMetaData metaData = decodeCheckpointValue(pendingCheckpoints[pCLoc].value);
		data->pendingCheckpoints[metaData.version].push_back(metaData);
		wait(yield());
	}

	state RangeResult checkpoints = fCheckpoints.get();
	state int cLoc;
	for (cLoc = 0; cLoc < checkpoints.size(); ++cLoc) {
		CheckpointMetaData metaData = decodeCheckpointValue(checkpoints[cLoc].value);
		data->checkpoints[metaData.checkpointID] = metaData;
		if (metaData.getState() == CheckpointMetaData::Deleting) {
			data->actors.add(deleteCheckpointQ(data, version, metaData));
		}
		wait(yield());
	}

	state RangeResult available = fShardAvailable.get();
	data->bytesRestored += available.logicalSize();
	state int availableLoc;
	for (availableLoc = 0; availableLoc < available.size(); availableLoc++) {
		KeyRangeRef keys(available[availableLoc].key.removePrefix(persistShardAvailableKeys.begin),
		                 availableLoc + 1 == available.size()
		                     ? allKeys.end
		                     : available[availableLoc + 1].key.removePrefix(persistShardAvailableKeys.begin));
		ASSERT(!keys.empty());

		bool nowAvailable = available[availableLoc].value != "0"_sr;
		/*if(nowAvailable)
		TraceEvent("AvailableShard", data->thisServerID).detail("RangeBegin", keys.begin).detail("RangeEnd", keys.end);*/
		data->newestAvailableVersion.insert(keys, nowAvailable ? latestVersion : invalidVersion);
		wait(yield());
	}

	// Restore acs validator from persisted disk
	if (data->acsValidator != nullptr) {
		RangeResult accumulativeChecksums = fAccumulativeChecksum.get();
		data->bytesRestored += accumulativeChecksums.logicalSize();
		for (int acsLoc = 0; acsLoc < accumulativeChecksums.size(); acsLoc++) {
			uint16_t acsIndex = decodePersistAccumulativeChecksumKey(accumulativeChecksums[acsLoc].key);
			AccumulativeChecksumState acsState = decodeAccumulativeChecksum(accumulativeChecksums[acsLoc].value);
			ASSERT(acsIndex == acsState.acsIndex);
			data->acsValidator->restore(acsState, data->thisServerID, data->tag, data->version.get());
		}
	}

	state KeyRangeMap<Optional<UID>> bulkLoadTaskRangeMap; // store dataMoveId on ranges with active bulkload tasks
	bulkLoadTaskRangeMap.insert(allKeys, Optional<UID>());
	state RangeResult bulkLoadTasks = fBulkLoadTask.get();
	for (int i = 0; i < bulkLoadTasks.size() - 1; i++) {
		ASSERT(!bulkLoadTasks[i].value.empty()); // Important invariant
		SSBulkLoadMetadata metadata = decodeSSBulkLoadMetadata(bulkLoadTasks[i].value);
		if (!metadata.getConductBulkLoad()) {
			continue;
		}
		KeyRange bulkLoadRange =
		    KeyRangeRef(bulkLoadTasks[i].key, bulkLoadTasks[i + 1].key).removePrefix(persistBulkLoadTaskKeys.begin);
		TraceEvent(SevInfo, "SSBulkLoadTaskMetaDataRestore", data->thisServerID)
		    .detail("DataMoveId", metadata.getDataMoveId())
		    .detail("Range", bulkLoadRange);
		// Assert checks the invariant: any bulkload task range cannot exceed the boundary of user key space.
		ASSERT(normalKeys.contains(bulkLoadRange));
		bulkLoadTaskRangeMap.insert(bulkLoadRange, metadata.getDataMoveId());
	}
	// BulkLoadTaskRangeMap range boundary is aligned to the shard assignment boundary, because we persist the bulkload
	// task metadata and the shard assignment metadata at the same version with the same shard boundary.

	state RangeResult assigned = fShardAssigned.get();
	data->bytesRestored += assigned.logicalSize();
	data->bytesRestored += fStorageShards.get().logicalSize();
	data->bytesRestored += fMoveInShards.get().logicalSize();
	state int assignedLoc;
	if (data->shardAware) {
		// TODO(psm): Avoid copying RangeResult around.
		wait(restoreShards(data, version, fStorageShards.get(), fMoveInShards.get(), assigned, available));
	} else {
		for (assignedLoc = 0; assignedLoc < assigned.size(); assignedLoc++) {
			KeyRangeRef keys(assigned[assignedLoc].key.removePrefix(persistShardAssignedKeys.begin),
			                 assignedLoc + 1 == assigned.size()
			                     ? allKeys.end
			                     : assigned[assignedLoc + 1].key.removePrefix(persistShardAssignedKeys.begin));
			ASSERT(!keys.empty());
			bool nowAssigned = assigned[assignedLoc].value != "0"_sr;
			/*if(nowAssigned)
			TraceEvent("AssignedShard", data->thisServerID).detail("RangeBegin", keys.begin).detail("RangeEnd", keys.end);*/
			// Decide dataMoveId and conductBulkLoad for calling changeServerKeys.
			// dataMoveId is used only when conductBulkLoad is true.
			UID dataMoveId = UID();
			ConductBulkLoad conductBulkLoad = ConductBulkLoad::False;
			for (auto bulkLoadIt : bulkLoadTaskRangeMap.intersectingRanges(keys)) {
				// we persist the bulkload task metadata and the shard assignment metadata at the same version with
				// the same shard boundary.
				if (!bulkLoadIt->value().present()) {
					continue;
				}
				// Assert checks the invariant: any bulkload task data move has set to assign the range and the range
				// must align to the shard assignment boundary.
				ASSERT(bulkLoadIt->range() == keys && nowAssigned);
				dataMoveId = bulkLoadIt->value().get();
				conductBulkLoad = ConductBulkLoad::True;
				break;
			}
			if (conductBulkLoad) {
				TraceEvent(SevInfo, "SSBulkLoadTaskSSStateRestore", data->thisServerID)
				    .detail("Range", keys)
				    .detail("DataMoveId", dataMoveId.toString());
			}
			changeServerKeys(data,
			                 keys,
			                 nowAssigned,
			                 version,
			                 CSK_RESTORE,
			                 DataMovementReason::INVALID,
			                 SSBulkLoadMetadata(dataMoveId, conductBulkLoad));

			if (!nowAssigned)
				ASSERT(data->newestAvailableVersion.allEqual(keys, invalidVersion));
			wait(yield());
		}
	}

	state RangeResult changeFeeds = fChangeFeeds.get();
	data->bytesRestored += changeFeeds.logicalSize();
	state int feedLoc;
	for (feedLoc = 0; feedLoc < changeFeeds.size(); feedLoc++) {
		Key changeFeedId = changeFeeds[feedLoc].key.removePrefix(persistChangeFeedKeys.begin);
		KeyRange changeFeedRange;
		Version popVersion, stopVersion, metadataVersion;
		std::tie(changeFeedRange, popVersion, stopVersion, metadataVersion) =
		    decodeChangeFeedSSValue(changeFeeds[feedLoc].value);
		TraceEvent(SevDebug, "RestoringChangeFeed", data->thisServerID)
		    .detail("FeedID", changeFeedId)
		    .detail("Range", changeFeedRange)
		    .detail("StopVersion", stopVersion)
		    .detail("PopVer", popVersion)
		    .detail("MetadataVersion", metadataVersion);
		Reference<ChangeFeedInfo> changeFeedInfo(new ChangeFeedInfo());
		changeFeedInfo->range = changeFeedRange;
		changeFeedInfo->id = changeFeedId;
		changeFeedInfo->durableVersion = version;
		changeFeedInfo->storageVersion = version;
		changeFeedInfo->emptyVersion = popVersion - 1;
		changeFeedInfo->stopVersion = stopVersion;
		changeFeedInfo->metadataVersion = metadataVersion;
		data->uidChangeFeed[changeFeedId] = changeFeedInfo;
		auto rs = data->keyChangeFeed.modify(changeFeedRange);
		for (auto r = rs.begin(); r != rs.end(); ++r) {
			r->value().push_back(changeFeedInfo);
		}
		wait(yield());
	}
	data->keyChangeFeed.coalesce(allKeys);

	state RangeResult tenantMap = fTenantMap.get();
	state int tenantMapLoc;
	for (tenantMapLoc = 0; tenantMapLoc < tenantMap.size(); tenantMapLoc++) {
		auto const& result = tenantMap[tenantMapLoc];
		int64_t tenantId = TenantAPI::prefixToId(result.key.substr(persistTenantMapKeys.begin.size()));

		data->tenantMap.insert(tenantId, ObjectReader::fromStringRef<TenantSSInfo>(result.value, IncludeVersion()));

		TraceEvent("RestoringTenant", data->thisServerID)
		    .detail("Key", tenantMap[tenantMapLoc].key)
		    .detail("Tenant", tenantId);

		wait(yield());
	}

	// TODO: why is this seemingly random delay here?
	wait(delay(0.0001));

	if (!data->shardAware) {
		// Erase data which isn't available (it is from some fetch at a later version)
		// SOMEDAY: Keep track of keys that might be fetching, make sure we don't have any data elsewhere?
		for (auto it = data->newestAvailableVersion.ranges().begin(); it != data->newestAvailableVersion.ranges().end();
		     ++it) {
			if (it->value() == invalidVersion) {
				KeyRangeRef clearRange(it->begin(), it->end());
				++data->counters.kvSystemClearRanges;
				// TODO(alexmiller): Figure out how to selectively enable spammy data distribution events.
				// DEBUG_KEY_RANGE("clearInvalidVersion", invalidVersion, clearRange);
				storage->clear(clearRange);
				++data->counters.kvSystemClearRanges;
				data->byteSampleApplyClear(clearRange, invalidVersion);
			}
		}
	}

	validate(data, true);
	startByteSampleRestore.send(Void());

	return true;
}

Future<bool> StorageServerDisk::restoreDurableState() {
	return ::restoreDurableState(data, storage);
}

// Determines whether a particular key-value pair should be included in a byte sample.
//
// This is part of the process to randomly and uniformly sample the key space.
//
// The sample will consist of (key, sampled_size) pairs, where sampled_size is
// an estimate of the size of the values associated with that key. These sizes
// are used to estimate the size of key ranges and determine split points.
//
// It's assumed that there's some overhead involved in the sample,
// BYTE_SAMPLING_OVERHEAD, which defaults to 100 bytes per entry.
//
// The rough goal is for the sample size to be a fixed fraction of the total
// size of all keys and values, 1/BYTE_SAMPLING_FACTOR, which defaults to 1/250.
// This includes the overhead, mentioned above.
//
// NOTE: This BYTE_SAMPLING_FACTOR and BYTE_SAMPLING_OVERHEAD knobs can't be
// changed after a database has been created. Data which has been already
// sampled can't be resampled, and the estimates of the size of key ranges
// implicitly includes these constants.
//
// This functions returns a struct containing
//   * inSample: true if we've selected this key-value pair for sampling.
//   * size: |key + value|
//   * probability: probability we select a key-value pair of this size, at random.
//   * sampledSize: always |key + value| / probability
//                  represents the amount of key-value space covered by that key.
ByteSampleInfo isKeyValueInSample(const KeyRef key, int64_t totalKvSize) {
	ASSERT(totalKvSize >= key.size());
	ByteSampleInfo info;

	// Pathological case: key size == value size == 0
	//
	// It's probability of getting chosen is 0, so let's skip the
	// computation and avoid dividing by zero.
	if (totalKvSize == 0) {
		info.size = 0;
		info.probability = 0.0;
		info.inSample = false;
		info.sampledSize = 0;
		return info;
	}

	info.size = totalKvSize;

	uint32_t a = 0;
	uint32_t b = 0;
	hashlittle2(key.begin(), key.size(), &a, &b);

	info.probability =
	    (double)info.size / (key.size() + SERVER_KNOBS->BYTE_SAMPLING_OVERHEAD) / SERVER_KNOBS->BYTE_SAMPLING_FACTOR;
	// MIN_BYTE_SAMPLING_PROBABILITY is 0.99 only for testing
	// MIN_BYTE_SAMPLING_PROBABILITY is 0 for other cases
	info.probability = std::clamp(info.probability, SERVER_KNOBS->MIN_BYTE_SAMPLING_PROBABILITY, 1.0);
	info.inSample = a / ((1 << 30) * 4.0) < info.probability;
	info.sampledSize = info.size / info.probability;

	return info;
}

void StorageServer::addMutationToMutationLogOrStorage(Version ver, MutationRef m) {
	if (ver != invalidVersion) {
		addMutationToMutationLog(addVersionToMutationLog(ver), m);
	} else {
		storage.writeMutation(m);
		byteSampleApplyMutation(m, ver);
	}
}

void StorageServer::byteSampleApplySet(KeyValueRef kv, Version ver) {
	// Update byteSample in memory and (eventually) on disk and notify waiting metrics

	ByteSampleInfo sampleInfo = isKeyValueInSample(kv);
	auto& byteSample = metrics.byteSample.sample;

	int64_t delta = 0;
	const KeyRef key = kv.key;

	auto old = byteSample.find(key);
	if (old != byteSample.end())
		delta = -byteSample.getMetric(old);
	if (sampleInfo.inSample) {
		delta += sampleInfo.sampledSize;
		byteSample.insert(key, sampleInfo.sampledSize);
		addMutationToMutationLogOrStorage(ver,
		                                  MutationRef(MutationRef::SetValue,
		                                              key.withPrefix(persistByteSampleKeys.begin),
		                                              BinaryWriter::toValue(sampleInfo.sampledSize, Unversioned())));
	} else {
		bool any = old != byteSample.end();
		if (!byteSampleRecovery.isReady()) {
			if (!byteSampleClears.rangeContaining(key).value()) {
				byteSampleClears.insert(key, true);
				byteSampleClearsTooLarge.set(byteSampleClears.size() > SERVER_KNOBS->MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE);
				any = true;
			}
		}
		if (any) {
			byteSample.erase(old);
			auto diskRange = singleKeyRange(key.withPrefix(persistByteSampleKeys.begin));
			addMutationToMutationLogOrStorage(ver,
			                                  MutationRef(MutationRef::ClearRange, diskRange.begin, diskRange.end));
			++counters.kvSystemClearRanges;
		}
	}

	if (delta) {
		metrics.notifyBytes(key, delta);
	}
}

void StorageServer::byteSampleApplyClear(KeyRangeRef range, Version ver) {
	// Update byteSample in memory and (eventually) on disk via the mutationLog and notify waiting metrics

	auto& byteSample = metrics.byteSample.sample;
	bool any = false;

	if (range.begin < allKeys.end) {
		// NotifyBytes should not be called for keys past allKeys.end
		KeyRangeRef searchRange = KeyRangeRef(range.begin, std::min(range.end, allKeys.end));
		counters.sampledBytesCleared += byteSample.sumRange(searchRange.begin, searchRange.end);

		auto r = metrics.waitMetricsMap.intersectingRanges(searchRange);
		for (auto shard = r.begin(); shard != r.end(); ++shard) {
			KeyRangeRef intersectingRange = shard.range() & range;
			int64_t bytes = byteSample.sumRange(intersectingRange.begin, intersectingRange.end);
			metrics.notifyBytes(shard, -bytes);
			any = any || bytes > 0;
		}
	}

	if (range.end > allKeys.end && byteSample.sumRange(std::max(allKeys.end, range.begin), range.end) > 0)
		any = true;

	if (!byteSampleRecovery.isReady()) {
		auto clearRanges = byteSampleClears.intersectingRanges(range);
		for (auto it : clearRanges) {
			if (!it.value()) {
				byteSampleClears.insert(range, true);
				byteSampleClearsTooLarge.set(byteSampleClears.size() > SERVER_KNOBS->MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE);
				any = true;
				break;
			}
		}
	}

	if (any) {
		byteSample.eraseAsync(range.begin, range.end);
		auto diskRange = range.withPrefix(persistByteSampleKeys.begin);
		addMutationToMutationLogOrStorage(ver, MutationRef(MutationRef::ClearRange, diskRange.begin, diskRange.end));
		++counters.kvSystemClearRanges;
	}
}

ACTOR Future<Void> waitMetrics(StorageServerMetrics* self, WaitMetricsRequest req, Future<Void> timeout) {
	state PromiseStream<StorageMetrics> change;
	state StorageMetrics metrics = self->getMetrics(req.keys);
	state Error error = success();
	state bool timedout = false;

	// state UID metricReqId = deterministicRandom()->randomUniqueID();
	DisabledTraceEvent(SevDebug, "WaitMetrics", metricReqId)
	    .detail("Keys", req.keys)
	    .detail("Metrics", metrics.toString())
	    .detail("ReqMin", req.min.toString())
	    .detail("ReqMax", req.max.toString());
	if (!req.min.allLessOrEqual(metrics) || !metrics.allLessOrEqual(req.max)) {
		CODE_PROBE(true, "ShardWaitMetrics return case 1 (quickly)");
		req.reply.send(metrics);
		return Void();
	}

	{
		auto rs = self->waitMetricsMap.modify(req.keys);
		for (auto r = rs.begin(); r != rs.end(); ++r)
			r->value().push_back(change);
		loop {
			try {
				choose {
					when(StorageMetrics c = waitNext(change.getFuture())) {
						metrics += c;

						// SOMEDAY: validation! The changes here are possibly partial changes (we receive multiple
						// messages per
						//  update to our requested range). This means that the validation would have to occur after
						//  all the messages for one clear or set have been dispatched.

						/*StorageMetrics m = getMetrics( data, req.keys );
						bool b = ( m.bytes != metrics.bytes || m.bytesWrittenPerKSecond !=
						metrics.bytesWrittenPerKSecond
						|| m.iosPerKSecond != metrics.iosPerKSecond ); if (b) { printf("keys: '%s' - '%s' @%p\n",
						printable(req.keys.begin).c_str(), printable(req.keys.end).c_str(), this);
						printf("waitMetrics: desync %d (%lld %lld %lld) != (%lld %lld %lld); +(%lld %lld %lld)\n",
						b, m.bytes, m.bytesWrittenPerKSecond, m.iosPerKSecond, metrics.bytes,
						metrics.bytesWrittenPerKSecond, metrics.iosPerKSecond, c.bytes, c.bytesWrittenPerKSecond,
						c.iosPerKSecond);

						}*/
					}
					when(wait(timeout)) {
						timedout = true;
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw; // This is only cancelled when the main loop had exited...no need in this case to clean
					       // up self
				error = e;
				break;
			}

			if (timedout) {
				CODE_PROBE(true, "ShardWaitMetrics return on timeout");
				// FIXME: instead of using random chance, send wrong_shard_server when the call in from
				// waitMetricsMultiple (requires additional information in the request)
				if (deterministicRandom()->random01() < SERVER_KNOBS->WAIT_METRICS_WRONG_SHARD_CHANCE) {
					req.reply.sendError(wrong_shard_server());
				} else {
					req.reply.send(metrics);
				}
				break;
			}

			if (!req.min.allLessOrEqual(metrics) || !metrics.allLessOrEqual(req.max)) {
				CODE_PROBE(true, "ShardWaitMetrics return case 2 (delayed)");
				req.reply.send(metrics);
				break;
			}
		}

		wait(delay(0)); // prevent iterator invalidation of functions sending changes
	}
	// fmt::print("PopWaitMetricsMap {}\n", req.keys.toString());
	auto rs = self->waitMetricsMap.modify(req.keys);
	for (auto i = rs.begin(); i != rs.end(); ++i) {
		auto& x = i->value();
		for (int j = 0; j < x.size(); j++) {
			if (x[j] == change) {
				swapAndPop(&x, j);
				break;
			}
		}
	}
	self->waitMetricsMap.coalesce(req.keys);

	if (error.code() != error_code_success) {
		if (error.code() != error_code_wrong_shard_server)
			throw error;
		CODE_PROBE(true, "ShardWaitMetrics delayed wrong_shard_server()");
		req.reply.sendError(error);
	}

	return Void();
}

Future<Void> StorageServerMetrics::waitMetrics(WaitMetricsRequest req, Future<Void> delay) {
	return ::waitMetrics(this, req, delay);
}

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

/////////////////////////////// Core //////////////////////////////////////
#ifndef __INTEL_COMPILER
#pragma region Core
#endif

ACTOR Future<Void> waitMetricsTenantAware_internal(StorageServer* self, WaitMetricsRequest req) {
	if (req.tenantInfo.hasTenant()) {
		try {
			// The call to `waitForMinVersion()` can throw `future_version()`.
			// It is okay for the caller to retry after a delay to give the server some time to catch up.
			state Version version = wait(waitForMinVersion(self, req.minVersion));
			self->checkTenantEntry(version, req.tenantInfo, true);
		} catch (Error& e) {
			if (!canReplyWith(e))
				throw;
			self->sendErrorWithPenalty(req.reply, e, self->getPenalty());
			return Void();
		}

		req.keys = req.keys.withPrefix(req.tenantInfo.prefix.get(), req.arena);
	}

	if (!self->isReadable(req.keys)) {
		self->sendErrorWithPenalty(req.reply, wrong_shard_server(), self->getPenalty());
	} else {
		wait(self->metrics.waitMetrics(req, delayJittered(SERVER_KNOBS->STORAGE_METRIC_TIMEOUT)));
	}
	return Void();
}

Future<Void> StorageServer::waitMetricsTenantAware(const WaitMetricsRequest& req) {
	return waitMetricsTenantAware_internal(this, req);
}

ACTOR Future<Void> metricsCore(StorageServer* self, StorageServerInterface ssi) {

	wait(self->byteSampleRecovery);
	TraceEvent("StorageServerRestoreDurableState", self->thisServerID).detail("RestoredBytes", self->bytesRestored);

	// Logs all counters in `counters.cc` and reset the interval.
	self->actors.add(self->counters.cc.traceCounters(
	    "StorageMetrics",
	    self->thisServerID,
	    SERVER_KNOBS->STORAGE_LOGGING_DELAY,
	    self->thisServerID.toString() + "/StorageMetrics",
	    [self = self](TraceEvent& te) {
		    te.detail("StorageEngine", self->storage.getKeyValueStoreType().toString());
		    te.detail("RocksDBVersion", format("%d.%d.%d", FDB_ROCKSDB_MAJOR, FDB_ROCKSDB_MINOR, FDB_ROCKSDB_PATCH));
		    te.detail("Tag", self->tag.toString());
		    std::vector<int> rpr = self->readPriorityRanks;
		    te.detail("ReadsTotalActive", self->ssLock->getRunnersCount());
		    te.detail("ReadsTotalWaiting", self->ssLock->getWaitersCount());
		    int type = (int)ReadType::FETCH;
		    te.detail("ReadFetchActive", self->ssLock->getRunnersCount(rpr[type]));
		    te.detail("ReadFetchWaiting", self->ssLock->getWaitersCount(rpr[type]));
		    type = (int)ReadType::LOW;
		    te.detail("ReadLowActive", self->ssLock->getRunnersCount(rpr[type]));
		    te.detail("ReadLowWaiting", self->ssLock->getWaitersCount(rpr[type]));
		    type = (int)ReadType::NORMAL;
		    te.detail("ReadNormalActive", self->ssLock->getRunnersCount(rpr[type]));
		    te.detail("ReadNormalWaiting", self->ssLock->getWaitersCount(rpr[type]));
		    type = (int)ReadType::HIGH;
		    te.detail("ReadHighActive", self->ssLock->getRunnersCount(rpr[type]));
		    te.detail("ReadHighWaiting", self->ssLock->getWaitersCount(rpr[type]));
		    StorageBytes sb = self->storage.getStorageBytes();
		    te.detail("KvstoreBytesUsed", sb.used);
		    te.detail("KvstoreBytesFree", sb.free);
		    te.detail("KvstoreBytesAvailable", sb.available);
		    te.detail("KvstoreBytesTotal", sb.total);
		    te.detail("KvstoreBytesTemp", sb.temp);
		    if (self->isTss()) {
			    te.detail("TSSPairID", self->tssPairID);
			    te.detail("TSSJointID",
			              UID(self->thisServerID.first() ^ self->tssPairID.get().first(),
			                  self->thisServerID.second() ^ self->tssPairID.get().second()));
		    } else if (self->isSSWithTSSPair()) {
			    te.detail("SSPairID", self->ssPairID);
			    te.detail("TSSJointID",
			              UID(self->thisServerID.first() ^ self->ssPairID.get().first(),
			                  self->thisServerID.second() ^ self->ssPairID.get().second()));
		    }
		    if (self->acsValidator != nullptr) {
			    te.detail("ACSCheckedMutationsSinceLastPrint", self->acsValidator->getAndClearCheckedMutations());
			    te.detail("ACSCheckedVersionsSinceLastPrint", self->acsValidator->getAndClearCheckedVersions());
			    te.detail("TotalMutations", self->acsValidator->getAndClearTotalMutations());
			    te.detail("TotalAcsMutations", self->acsValidator->getAndClearTotalAcsMutations());
			    te.detail("TotalAddedMutations", self->acsValidator->getAndClearTotalAddedMutations());
		    }
	    }));

	wait(serveStorageMetricsRequests(self, ssi));
	return Void();
}

ACTOR Future<Void> logLongByteSampleRecovery(Future<Void> recovery) {
	choose {
		when(wait(recovery)) {}
		when(wait(delay(SERVER_KNOBS->LONG_BYTE_SAMPLE_RECOVERY_DELAY))) {
			TraceEvent(g_network->isSimulated() ? SevWarn : SevWarnAlways, "LongByteSampleRecovery");
		}
	}

	return Void();
}

ACTOR Future<Void> checkBehind(StorageServer* self) {
	state int behindCount = 0;
	loop {
		wait(delay(SERVER_KNOBS->BEHIND_CHECK_DELAY));
		state Transaction tr(self->cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Version readVersion = wait(tr.getRawReadVersion());
				if (readVersion > self->version.get() + SERVER_KNOBS->BEHIND_CHECK_VERSIONS) {
					behindCount++;
				} else {
					behindCount = 0;
				}
				self->versionBehind = behindCount >= SERVER_KNOBS->BEHIND_CHECK_COUNT;
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}

ACTOR Future<Void> serveGetValueRequests(StorageServer* self, FutureStream<GetValueRequest> getValue) {
	getCurrentLineage()->modify(&TransactionLineage::operation) = TransactionLineage::Operation::GetValue;
	loop {
		GetValueRequest req = waitNext(getValue);
		// Warning: This code is executed at extremely high priority (TaskPriority::LoadBalancedEndpoint), so
		// downgrade before doing real work
		if (req.options.present() && req.options.get().debugID.present())
			g_traceBatch.addEvent("GetValueDebug",
			                      req.options.get().debugID.get().first(),
			                      "storageServer.received"); //.detail("TaskID", g_network->getCurrentTask());

		if (SHORT_CIRCUT_ACTUAL_STORAGE && normalKeys.contains(req.key))
			req.reply.send(GetValueReply());
		else
			self->actors.add(self->readGuard(req, getValueQ));
	}
}

ACTOR Future<Void> serveGetKeyValuesRequests(StorageServer* self, FutureStream<GetKeyValuesRequest> getKeyValues) {
	getCurrentLineage()->modify(&TransactionLineage::operation) = TransactionLineage::Operation::GetKeyValues;
	loop {
		GetKeyValuesRequest req = waitNext(getKeyValues);

		// Warning: This code is executed at extremely high priority (TaskPriority::LoadBalancedEndpoint), so
		// downgrade before doing real work
		self->actors.add(self->readGuard(req, getKeyValuesQ));
	}
}

ACTOR Future<Void> serveGetMappedKeyValuesRequests(StorageServer* self,
                                                   FutureStream<GetMappedKeyValuesRequest> getMappedKeyValues) {
	// TODO: Is it fine to keep TransactionLineage::Operation::GetKeyValues here?
	getCurrentLineage()->modify(&TransactionLineage::operation) = TransactionLineage::Operation::GetKeyValues;
	loop {
		GetMappedKeyValuesRequest req = waitNext(getMappedKeyValues);

		// Warning: This code is executed at extremely high priority (TaskPriority::LoadBalancedEndpoint), so
		// downgrade before doing real work
		self->actors.add(self->readGuard(req, getMappedKeyValuesQ));
	}
}

ACTOR Future<Void> serveGetKeyValuesStreamRequests(StorageServer* self,
                                                   FutureStream<GetKeyValuesStreamRequest> getKeyValuesStream) {
	loop {
		GetKeyValuesStreamRequest req = waitNext(getKeyValuesStream);
		// Warning: This code is executed at extremely high priority (TaskPriority::LoadBalancedEndpoint), so
		// downgrade before doing real work
		// FIXME: add readGuard again
		self->actors.add(getKeyValuesStreamQ(self, req));
	}
}

ACTOR Future<Void> serveGetKeyRequests(StorageServer* self, FutureStream<GetKeyRequest> getKey) {
	getCurrentLineage()->modify(&TransactionLineage::operation) = TransactionLineage::Operation::GetKey;
	loop {
		GetKeyRequest req = waitNext(getKey);
		// Warning: This code is executed at extremely high priority (TaskPriority::LoadBalancedEndpoint), so
		// downgrade before doing real work
		self->actors.add(self->readGuard(req, getKeyQ));
	}
}

ACTOR Future<Void> watchValueWaitForVersion(StorageServer* self,
                                            WatchValueRequest req,
                                            PromiseStream<WatchValueRequest> stream) {
	state Span span("SS:watchValueWaitForVersion"_loc, req.spanContext);

	getCurrentLineage()->modify(&TransactionLineage::txID) = req.spanContext.traceID;
	try {
		wait(success(waitForVersionNoTooOld(self, req.version)));
		self->checkTenantEntry(latestVersion, req.tenantInfo, false);
		if (req.tenantInfo.hasTenant()) {
			req.key = req.key.withPrefix(req.tenantInfo.prefix.get());
		}
		stream.send(req);
	} catch (Error& e) {
		if (!canReplyWith(e))
			throw e;
		self->sendErrorWithPenalty(req.reply, e, self->getPenalty());
	}
	return Void();
}

ACTOR Future<Void> serveWatchValueRequestsImpl(StorageServer* self, FutureStream<WatchValueRequest> stream) {
	loop {
		getCurrentLineage()->modify(&TransactionLineage::txID) = UID();
		state WatchValueRequest req = waitNext(stream);
		state Reference<ServerWatchMetadata> metadata =
		    self->getWatchMetadata(req.key.contents(), req.tenantInfo.tenantId);
		state Span span("SS:serveWatchValueRequestsImpl"_loc, req.spanContext);
		getCurrentLineage()->modify(&TransactionLineage::txID) = req.spanContext.traceID;
		state ReadOptions options;

		// case 1: no watch set for the current key
		if (!metadata.isValid()) {
			metadata = makeReference<ServerWatchMetadata>(
			    req.key, req.value, req.version, req.tags, req.debugID, req.tenantInfo.tenantId);
			KeyRef key = self->setWatchMetadata(metadata);
			metadata->watch_impl = forward(watchWaitForValueChange(self, span.context, key, req.tenantInfo.tenantId),
			                               metadata->versionPromise);
			self->actors.add(watchValueSendReply(self, req, metadata->versionPromise.getFuture(), span.context));
		}
		// case 2: there is a watch in the map and it has the same value so just update version
		else if (metadata->value == req.value) {
			if (req.debugID.present()) {
				if (metadata->debugID.present()) {
					g_traceBatch.addAttach(
					    "WatchRequestCase2", req.debugID.get().first(), metadata->debugID.get().first());
				} else {
					g_traceBatch.addEvent(
					    "WatchValueDebug", metadata->debugID.get().first(), "watchValueSendReply.Case2");
				}
			}

			if (req.version > metadata->version) {
				metadata->version = req.version;
				metadata->tags = req.tags;
				if (req.debugID.present()) {
					metadata->debugID = req.debugID;
				}
			}

			self->actors.add(watchValueSendReply(self, req, metadata->versionPromise.getFuture(), span.context));
		}
		// case 3: version in map has a lower version so trigger watch and create a new entry in map
		else if (req.version > metadata->version) {
			self->deleteWatchMetadata(req.key.contents(), req.tenantInfo.tenantId);
			metadata->versionPromise.send(req.version);
			metadata->watch_impl.cancel();

			metadata = makeReference<ServerWatchMetadata>(
			    req.key, req.value, req.version, req.tags, req.debugID, req.tenantInfo.tenantId);
			KeyRef key = self->setWatchMetadata(metadata);
			metadata->watch_impl = forward(watchWaitForValueChange(self, span.context, key, req.tenantInfo.tenantId),
			                               metadata->versionPromise);

			self->actors.add(watchValueSendReply(self, req, metadata->versionPromise.getFuture(), span.context));
		}
		// case 4: version in the map is higher so immediately trigger watch
		else if (req.version < metadata->version) {
			CODE_PROBE(true, "watch version in map is higher so trigger watch (case 4)");
			req.reply.send(WatchValueReply{ metadata->version });
		}
		// case 5: watch value differs but their versions are the same (rare case) so check with the SS
		else {
			CODE_PROBE(true, "watch version in the map is the same but value is different (case 5)");
			loop {
				try {
					state Version latest = self->version.get();
					options.debugID = metadata->debugID;

					GetValueRequest getReq(
					    span.context, TenantInfo(), metadata->key, latest, metadata->tags, options, VersionVector());
					state Future<Void> getValue = getValueQ(self, getReq);
					GetValueReply reply = wait(getReq.reply.getFuture());
					metadata = self->getWatchMetadata(req.key.contents(), req.tenantInfo.tenantId);

					if (metadata.isValid() && reply.value != metadata->value) { // valSS != valMap
						self->deleteWatchMetadata(req.key.contents(), req.tenantInfo.tenantId);
						metadata->versionPromise.send(req.version);
						metadata->watch_impl.cancel();
					}

					if (reply.value == req.value) { // valSS == valreq
						metadata = makeReference<ServerWatchMetadata>(
						    req.key, req.value, req.version, req.tags, req.debugID, req.tenantInfo.tenantId);
						KeyRef key = self->setWatchMetadata(metadata);
						metadata->watch_impl =
						    forward(watchWaitForValueChange(self, span.context, key, req.tenantInfo.tenantId),
						            metadata->versionPromise);
						self->actors.add(
						    watchValueSendReply(self, req, metadata->versionPromise.getFuture(), span.context));
					} else {
						req.reply.send(WatchValueReply{ latest });
					}
					break;
				} catch (Error& e) {
					if (e.code() != error_code_transaction_too_old) {
						if (!canReplyWith(e))
							throw e;
						self->sendErrorWithPenalty(req.reply, e, self->getPenalty());
						break;
					}
					CODE_PROBE(
					    true, "Reading a watched key failed with transaction_too_old case 5", probe::decoration::rare);
				}
			}
		}
	}
}

ACTOR Future<Void> serveWatchValueRequests(StorageServer* self, FutureStream<WatchValueRequest> watchValue) {
	state PromiseStream<WatchValueRequest> stream;
	getCurrentLineage()->modify(&TransactionLineage::operation) = TransactionLineage::Operation::WatchValue;
	self->actors.add(serveWatchValueRequestsImpl(self, stream.getFuture()));

	loop {
		WatchValueRequest req = waitNext(watchValue);
		// TODO: fast load balancing?
		if (self->shouldRead(req)) {
			self->actors.add(watchValueWaitForVersion(self, req, stream));
		}
	}
}

ACTOR Future<Void> serveChangeFeedStreamRequests(StorageServer* self,
                                                 FutureStream<ChangeFeedStreamRequest> changeFeedStream) {
	loop {
		ChangeFeedStreamRequest req = waitNext(changeFeedStream);
		// must notify change feed that its shard is moved away ASAP
		self->actors.add(changeFeedStreamQ(self, req) || stopChangeFeedOnMove(self, req));
	}
}

ACTOR Future<Void> serveOverlappingChangeFeedsRequests(
    StorageServer* self,
    FutureStream<OverlappingChangeFeedsRequest> overlappingChangeFeeds) {
	loop {
		OverlappingChangeFeedsRequest req = waitNext(overlappingChangeFeeds);
		self->actors.add(self->readGuard(req, overlappingChangeFeedsQ));
	}
}

ACTOR Future<Void> serveChangeFeedPopRequests(StorageServer* self, FutureStream<ChangeFeedPopRequest> changeFeedPops) {
	loop {
		ChangeFeedPopRequest req = waitNext(changeFeedPops);
		self->actors.add(self->readGuard(req, changeFeedPopQ));
	}
}

ACTOR Future<Void> serveChangeFeedVersionUpdateRequests(
    StorageServer* self,
    FutureStream<ChangeFeedVersionUpdateRequest> changeFeedVersionUpdate) {
	loop {
		ChangeFeedVersionUpdateRequest req = waitNext(changeFeedVersionUpdate);
		self->actors.add(self->readGuard(req, changeFeedVersionUpdateQ));
	}
}

ACTOR Future<Void> storageEngineConsistencyCheck(StorageServer* self) {
	if (SERVER_KNOBS->STORAGE_SHARD_CONSISTENCY_CHECK_INTERVAL <= 0.0) {
		return Void();
	}

	if (self->storage.getKeyValueStoreType() != KeyValueStoreType::SSD_SHARDED_ROCKSDB) {
		return Void();
	}

	loop {
		wait(delay(SERVER_KNOBS->STORAGE_SHARD_CONSISTENCY_CHECK_INTERVAL));
		// Only validate when storage server and storage engine are expected to have the same shard mapping.
		while (self->pendingAddRanges.size() > 0 || self->pendingRemoveRanges.size() > 0) {
			wait(delay(5.0));
		}

		CoalescedKeyRangeMap<std::string> currentShards;
		currentShards.insert(allKeys, "");
		auto fullRange = self->shards.ranges();
		for (auto it = fullRange.begin(); it != fullRange.end(); ++it) {
			if (!it.value()) {
				continue;
			}
			if (it.value()->assigned()) {
				currentShards.insert(it.range(), format("%016llx", it.value()->shardId));
			}
		}

		auto kvShards = self->storage.getExistingRanges();

		TraceEvent(SevInfo, "StorageEngineConsistencyCheckStarted").log();

		auto kvRanges = kvShards.ranges();
		for (auto it = kvRanges.begin(); it != kvRanges.end(); ++it) {
			if (it.value() == "") {
				continue;
			}

			for (auto v : currentShards.intersectingRanges(it.range())) {
				if (v.value() == "") {
					TraceEvent(SevWarn, "MissingShardSS").detail("Range", v.range()).detail("ShardIdKv", it.value());
				} else if (v.value() != it.value()) {
					TraceEvent(SevWarn, "ShardMismatch")
					    .detail("Range", v.range())
					    .detail("ShardIdKv", it.value())
					    .detail("ShardIdSs", v.value());
				}
			}
		}

		for (auto it : currentShards.ranges()) {
			if (it.value() == "") {
				continue;
			}

			for (auto v : kvShards.intersectingRanges(it.range())) {
				if (v.value() == "") {
					TraceEvent(SevWarn, "MissingShardKv").detail("Range", v.range()).detail("ShardIdSS", it.value());
				}
			}
		}

		TraceEvent(SevInfo, "StorageEngineConsistencyCheckComplete");
	}
}

ACTOR Future<Void> reportStorageServerState(StorageServer* self) {
	if (!SERVER_KNOBS->REPORT_DD_METRICS) {
		return Void();
	}

	loop {
		wait(delay(SERVER_KNOBS->DD_METRICS_REPORT_INTERVAL));

		const auto numRunningFetchKeys = self->currentRunningFetchKeys.numRunning();
		if (numRunningFetchKeys == 0) {
			continue;
		}

		const auto longestRunningFetchKeys = self->currentRunningFetchKeys.longestTime();

		auto level = SevInfo;
		if (longestRunningFetchKeys.first >= SERVER_KNOBS->FETCH_KEYS_TOO_LONG_TIME_CRITERIA) {
			level = SevWarnAlways;
		}

		TraceEvent(level, "FetchKeysCurrentStatus", self->thisServerID)
		    .detail("Timestamp", now())
		    .detail("LongestRunningTime", longestRunningFetchKeys.first)
		    .detail("StartKey", longestRunningFetchKeys.second.begin)
		    .detail("EndKey", longestRunningFetchKeys.second.end)
		    .detail("NumRunning", numRunningFetchKeys);
	}
}

ACTOR Future<Void> storageServerCore(StorageServer* self, StorageServerInterface ssi) {
	state Future<Void> doUpdate = Void();
	state bool updateReceived = false; // true iff the current update() actor assigned to doUpdate has already
	                                   // received an update from the tlog
	state double lastLoopTopTime = now();
	state Future<Void> dbInfoChange = Void();
	state Future<Void> checkLastUpdate = Void();
	state Future<Void> updateProcessStatsTimer = delay(SERVER_KNOBS->FASTRESTORE_UPDATE_PROCESS_STATS_INTERVAL);

	self->actors.add(updateStorage(self));
	self->actors.add(waitFailureServer(ssi.waitFailure.getFuture()));
	self->actors.add(self->otherError.getFuture());
	self->actors.add(metricsCore(self, ssi));
	self->actors.add(logLongByteSampleRecovery(self->byteSampleRecovery));
	self->actors.add(checkBehind(self));
	self->actors.add(serveGetValueRequests(self, ssi.getValue.getFuture()));
	self->actors.add(serveGetKeyValuesRequests(self, ssi.getKeyValues.getFuture()));
	self->actors.add(serveGetMappedKeyValuesRequests(self, ssi.getMappedKeyValues.getFuture()));
	self->actors.add(serveGetKeyValuesStreamRequests(self, ssi.getKeyValuesStream.getFuture()));
	self->actors.add(serveGetKeyRequests(self, ssi.getKey.getFuture()));
	self->actors.add(serveWatchValueRequests(self, ssi.watchValue.getFuture()));
	self->actors.add(serveChangeFeedStreamRequests(self, ssi.changeFeedStream.getFuture()));
	self->actors.add(serveOverlappingChangeFeedsRequests(self, ssi.overlappingChangeFeeds.getFuture()));
	self->actors.add(serveChangeFeedPopRequests(self, ssi.changeFeedPop.getFuture()));
	self->actors.add(serveChangeFeedVersionUpdateRequests(self, ssi.changeFeedVersionUpdate.getFuture()));
	self->actors.add(traceRole(Role::STORAGE_SERVER, ssi.id()));
	self->actors.add(reportStorageServerState(self));
	self->actors.add(storageEngineConsistencyCheck(self));

	self->transactionTagCounter.startNewInterval();
	self->actors.add(
	    recurring([&]() { self->transactionTagCounter.startNewInterval(); }, SERVER_KNOBS->TAG_MEASUREMENT_INTERVAL));

	self->coreStarted.send(Void());

	loop {
		++self->counters.loops;

		double loopTopTime = now();
		double elapsedTime = loopTopTime - lastLoopTopTime;
		if (elapsedTime > 0.050) {
			if (deterministicRandom()->random01() < 0.01)
				TraceEvent(SevWarn, "SlowSSLoopx100", self->thisServerID).detail("Elapsed", elapsedTime);
		}
		lastLoopTopTime = loopTopTime;

		choose {
			when(wait(checkLastUpdate)) {
				if (now() - self->lastUpdate >= CLIENT_KNOBS->NO_RECENT_UPDATES_DURATION) {
					self->noRecentUpdates.set(true);
					checkLastUpdate = delay(CLIENT_KNOBS->NO_RECENT_UPDATES_DURATION);
				} else {
					checkLastUpdate =
					    delay(std::max(CLIENT_KNOBS->NO_RECENT_UPDATES_DURATION - (now() - self->lastUpdate), 0.1));
				}
			}
			when(wait(dbInfoChange)) {
				CODE_PROBE(self->logSystem, "shardServer dbInfo changed");
				dbInfoChange = self->db->onChange();
				if (self->db->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS) {
					self->logSystem = ILogSystem::fromServerDBInfo(self->thisServerID, self->db->get());
					if (self->logSystem) {
						if (self->db->get().logSystemConfig.recoveredAt.present()) {
							self->poppedAllAfter = self->db->get().logSystemConfig.recoveredAt.get();
						}
						self->logCursor = self->logSystem->peekSingle(
						    self->thisServerID, self->version.get() + 1, self->tag, self->history);
						self->popVersion(self->storageMinRecoverVersion + 1, true);
					}
					// If update() is waiting for results from the tlog, it might never get them, so needs to be
					// cancelled.  But if it is waiting later, cancelling it could cause problems (e.g. fetchKeys
					// that already committed to transitioning to waiting state)
					if (!updateReceived) {
						doUpdate = Void();
					}
				}

				Optional<LatencyBandConfig> newLatencyBandConfig = self->db->get().latencyBandConfig;
				if (newLatencyBandConfig.present() != self->latencyBandConfig.present() ||
				    (newLatencyBandConfig.present() &&
				     newLatencyBandConfig.get().readConfig != self->latencyBandConfig.get().readConfig)) {
					self->latencyBandConfig = newLatencyBandConfig;
					self->counters.readLatencyBands.clearBands();
					TraceEvent("LatencyBandReadUpdatingConfig").detail("Present", newLatencyBandConfig.present());
					if (self->latencyBandConfig.present()) {
						for (auto band : self->latencyBandConfig.get().readConfig.bands) {
							self->counters.readLatencyBands.addThreshold(band);
						}
					}
				}
			}
			when(GetShardStateRequest req = waitNext(ssi.getShardState.getFuture())) {
				if (req.mode == GetShardStateRequest::NO_WAIT) {
					if (self->isReadable(req.keys))
						req.reply.send(GetShardStateReply{ self->version.get(), self->durableVersion.get() });
					else
						req.reply.sendError(wrong_shard_server());
				} else {
					self->actors.add(getShardStateQ(self, req));
				}
			}
			when(StorageQueuingMetricsRequest req = waitNext(ssi.getQueuingMetrics.getFuture())) {
				getQueuingMetrics(self, req);
			}
			when(ReplyPromise<KeyValueStoreType> reply = waitNext(ssi.getKeyValueStoreType.getFuture())) {
				reply.send(self->storage.getKeyValueStoreType());
			}
			when(wait(doUpdate)) {
				updateReceived = false;
				if (!self->logSystem)
					doUpdate = Never();
				else
					doUpdate = update(self, &updateReceived);
			}
			when(GetCheckpointRequest req = waitNext(ssi.checkpoint.getFuture())) {
				self->actors.add(getCheckpointQ(self, req));
			}
			when(FetchCheckpointRequest req = waitNext(ssi.fetchCheckpoint.getFuture())) {
				self->actors.add(fetchCheckpointQ(self, req));
			}
			when(UpdateCommitCostRequest req = waitNext(ssi.updateCommitCostRequest.getFuture())) {
				// Ratekeeper might change with a new ID. In this case, always accept the data.
				if (req.ratekeeperID != self->busiestWriteTagContext.ratekeeperID) {
					TraceEvent("RatekeeperIDChange")
					    .detail("OldID", self->busiestWriteTagContext.ratekeeperID)
					    .detail("OldLastUpdateTime", self->busiestWriteTagContext.lastUpdateTime)
					    .detail("NewID", req.ratekeeperID)
					    .detail("LastUpdateTime", req.postTime);
					self->busiestWriteTagContext.ratekeeperID = req.ratekeeperID;
					self->busiestWriteTagContext.lastUpdateTime = -1;
				}
				// In case we received an old request/duplicate request, due to, e.g. network problem
				ASSERT(req.postTime > 0);
				if (req.postTime < self->busiestWriteTagContext.lastUpdateTime) {
					continue;
				}

				self->busiestWriteTagContext.lastUpdateTime = req.postTime;
				TraceEvent("BusiestWriteTag", self->thisServerID)
				    .detail("Elapsed", req.elapsed)
				    .detail("Tag", req.busiestTag)
				    .detail("TagOps", req.opsSum)
				    .detail("TagCost", req.costSum)
				    .detail("TotalCost", req.totalWriteCosts)
				    .detail("Reported", req.reported)
				    .trackLatest(self->busiestWriteTagContext.busiestWriteTagTrackingKey);

				req.reply.send(Void());
			}
			when(FetchCheckpointKeyValuesRequest req = waitNext(ssi.fetchCheckpointKeyValues.getFuture())) {
				self->actors.add(fetchCheckpointKeyValuesQ(self, req));
			}
			when(AuditStorageRequest req = waitNext(ssi.auditStorage.getFuture())) {
				// Check req
				if (!req.id.isValid() || !req.ddId.isValid() || req.range.empty() ||
				    req.getType() == AuditType::ValidateLocationMetadata) {
					// ddId is used when persist progress
					TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
					           "AuditRequestInvalid") // unexpected
					    .detail("AuditRange", req.range)
					    .detail("DDId", req.ddId)
					    .detail("AuditId", req.id)
					    .detail("AuditType", req.getType())
					    .detail("AuditRange", req.range);
					req.reply.sendError(audit_storage_cancelled());
					continue;
				}
				// Start the new audit task
				if (req.getType() == AuditType::ValidateHA) {
					self->actors.add(auditStorageShardReplicaQ(self, req));
				} else if (req.getType() == AuditType::ValidateReplica) {
					self->actors.add(auditStorageShardReplicaQ(self, req));
				} else if (req.getType() == AuditType::ValidateStorageServerShard) {
					self->actors.add(auditStorageServerShardQ(self, req));
				} else {
					req.reply.sendError(not_implemented());
				}
			}
			when(BulkDumpRequest req = waitNext(ssi.bulkdump.getFuture())) {
				TraceEvent(SevInfo, "SSBulkDumpRequestReceived", self->thisServerID)
				    .detail("BulkDumpRequest", req.toString());
				self->actors.add(bulkDumpQ(self, req));
			}
			when(wait(updateProcessStatsTimer)) {
				updateProcessStats(self);
				updateProcessStatsTimer = delay(SERVER_KNOBS->FASTRESTORE_UPDATE_PROCESS_STATS_INTERVAL);
			}
			when(GetHotShardsRequest req = waitNext(ssi.getHotShards.getFuture())) {
				struct ComparePair {
					bool operator()(const std::pair<KeyRange, int64_t>& lhs, const std::pair<KeyRange, int64_t>& rhs) {
						return lhs.second > rhs.second;
					}
				};
				std::
				    priority_queue<std::pair<KeyRange, int64_t>, std::vector<std::pair<KeyRange, int64_t>>, ComparePair>
				        topRanges;

				for (auto& s : self->shards.ranges()) {
					KeyRange keyRange = KeyRange(s.range());
					int64_t bytesWrittenPerKSecond = self->metrics.getHotShards(keyRange);
					if (systemKeys.intersects(keyRange) ||
					    (bytesWrittenPerKSecond <= SERVER_KNOBS->SHARD_MAX_BYTES_PER_KSEC)) {
						continue;
					}
					if (topRanges.size() < SERVER_KNOBS->HOT_SHARD_THROTTLING_TRACKED) {
						topRanges.push(std::make_pair(keyRange, bytesWrittenPerKSecond));
					} else if (bytesWrittenPerKSecond > topRanges.top().second) {
						topRanges.pop();
						topRanges.push(std::make_pair(keyRange, bytesWrittenPerKSecond));
					}
				}

				// TraceEvent(SevDebug, "ReceivedGetHotShards").detail("TopRanges", topRanges.size());
				GetHotShardsReply reply;

				while (!topRanges.empty()) {
					reply.hotShards.push_back(topRanges.top().first);
					topRanges.pop();
				}

				req.reply.send(reply);
			}
			when(GetStorageCheckSumRequest req = waitNext(ssi.getCheckSum.getFuture())) {
				TraceEvent(SevError, "GetStorageCheckSumHasNotImplemented", ssi.id());
				req.reply.sendError(not_implemented());
			}
			when(wait(self->actors.getResult())) {}
		}
	}
}

bool storageServerTerminated(StorageServer& self, IKeyValueStore* persistentData, Error const& e) {
	self.shuttingDown = true;

	// Clearing shards shuts down any fetchKeys actors; these may do things on cancellation that are best done with
	// self still valid
	self.addShard(ShardInfo::newNotAssigned(allKeys));

	// Dispose the IKVS (destroying its data permanently) only if this shutdown is definitely permanent.  Otherwise
	// just close it.
	if (e.code() == error_code_please_reboot) {
		// do nothing.
	} else if (e.code() == error_code_worker_removed || e.code() == error_code_recruitment_failed) {
		// SOMEDAY: could close instead of dispose if tss in quarantine gets removed so it could still be
		// investigated?
		TraceEvent("KVSRemoved", self.thisServerID).detail("Reason", e.name());
		persistentData->dispose();
	} else {
		persistentData->close();
	}

	if (e.code() == error_code_worker_removed || e.code() == error_code_recruitment_failed ||
	    e.code() == error_code_file_not_found || e.code() == error_code_actor_cancelled ||
	    e.code() == error_code_remote_kvs_cancelled) {
		TraceEvent("StorageServerTerminated", self.thisServerID).errorUnsuppressed(e);
		return true;
	} else
		return false;
}

ACTOR Future<Void> memoryStoreRecover(IKeyValueStore* store, Reference<IClusterConnectionRecord> connRecord, UID id) {
	if (store->getType() != KeyValueStoreType::MEMORY || connRecord.getPtr() == nullptr) {
		return Never();
	}

	// create a temp client connect to DB
	Database cx = Database::createDatabase(connRecord, ApiVersion::LATEST_VERSION);

	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state int noCanRemoveCount = 0;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			state bool canRemove = wait(canRemoveStorageServer(tr, id));
			if (!canRemove) {
				CODE_PROBE(true,
				           "it's possible that the caller had a transaction in flight that assigned keys to the "
				           "server. Wait for it to reverse its mistake.");
				wait(delayJittered(SERVER_KNOBS->REMOVE_RETRY_DELAY, TaskPriority::UpdateStorage));
				tr->reset();
				TraceEvent("RemoveStorageServerRetrying")
				    .detail("Count", noCanRemoveCount++)
				    .detail("ServerID", id)
				    .detail("CanRemove", canRemove);
			} else {
				return Void();
			}
		} catch (Error& e) {
			state Error err = e;
			wait(tr->onError(e));
			TraceEvent("RemoveStorageServerRetrying").error(err);
		}
	}
}

ACTOR Future<Void> initTenantMap(StorageServer* self) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);

	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state Version version = wait(tr->getReadVersion());
			// This limits the number of tenants, but eventually we shouldn't need to do this at all
			// when SSs store only the local tenants
			KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> entries =
			    wait(TenantMetadata::tenantMap().getRange(tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER + 1));
			ASSERT(entries.results.size() <= CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER && !entries.more);

			TraceEvent("InitTenantMap", self->thisServerID)
			    .detail("Version", version)
			    .detail("NumTenants", entries.results.size());

			for (auto const& [_, entry] : entries.results) {
				self->insertTenant(entry, version, false);
			}
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	return Void();
}

ACTOR Future<Void> replaceInterface(StorageServer* self, StorageServerInterface ssi) {
	ASSERT(!ssi.isTss());
	state EncryptionAtRestMode encryptionMode = wait(self->storage.encryptionMode());
	state Transaction tr(self->cx);

	loop {
		state Future<Void> infoChanged = self->db->onChange();
		state Reference<CommitProxyInfo> commitProxies(new CommitProxyInfo(self->db->get().client.commitProxies));
		choose {
			when(GetStorageServerRejoinInfoReply _rep =
			         wait(commitProxies->size()
			                  ? basicLoadBalance(commitProxies,
			                                     &CommitProxyInterface::getStorageServerRejoinInfo,
			                                     GetStorageServerRejoinInfoRequest(ssi.id(), ssi.locality.dcId()))
			                  : Never())) {
				state GetStorageServerRejoinInfoReply rep = _rep;
				if (rep.encryptMode != encryptionMode) {
					TraceEvent(SevWarnAlways, "SSEncryptModeMismatch", self->thisServerID)
					    .detail("StorageEncryptionMode", encryptionMode)
					    .detail("ClusterEncryptionMode", rep.encryptMode);
					throw encrypt_mode_mismatch();
				}

				try {
					tr.reset();
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr.setOption(FDBTransactionOptions::LOCK_AWARE);
					tr.setVersion(rep.version);

					tr.addReadConflictRange(singleKeyRange(serverListKeyFor(ssi.id())));
					tr.addReadConflictRange(singleKeyRange(serverTagKeyFor(ssi.id())));
					tr.addReadConflictRange(serverTagHistoryRangeFor(ssi.id()));
					tr.addReadConflictRange(singleKeyRange(tagLocalityListKeyFor(ssi.locality.dcId())));

					tr.set(serverListKeyFor(ssi.id()), serverListValue(ssi));

					if (rep.newLocality) {
						tr.addReadConflictRange(tagLocalityListKeys);
						tr.set(tagLocalityListKeyFor(ssi.locality.dcId()),
						       tagLocalityListValue(rep.newTag.get().locality));
					}

					// this only should happen if SS moved datacenters
					if (rep.newTag.present()) {
						KeyRange conflictRange = singleKeyRange(serverTagConflictKeyFor(rep.newTag.get()));
						tr.addReadConflictRange(conflictRange);
						tr.addWriteConflictRange(conflictRange);
						tr.setOption(FDBTransactionOptions::FIRST_IN_BATCH);
						tr.set(serverTagKeyFor(ssi.id()), serverTagValue(rep.newTag.get()));
						tr.atomicOp(serverTagHistoryKeyFor(ssi.id()),
						            serverTagValue(rep.tag),
						            MutationRef::SetVersionstampedKey);
					}

					if (rep.history.size() && rep.history.back().first < self->version.get()) {
						tr.clear(serverTagHistoryRangeBefore(ssi.id(), self->version.get()));
					}

					choose {
						when(wait(tr.commit())) {
							self->history = rep.history;

							if (rep.newTag.present()) {
								self->tag = rep.newTag.get();
								self->history.insert(self->history.begin(),
								                     std::make_pair(tr.getCommittedVersion(), rep.tag));
							} else {
								self->tag = rep.tag;
							}
							self->allHistory = self->history;

							TraceEvent("SSTag", self->thisServerID).detail("MyTag", self->tag.toString());
							for (auto it : self->history) {
								TraceEvent("SSHistory", self->thisServerID)
								    .detail("Ver", it.first)
								    .detail("Tag", it.second.toString());
							}

							if (self->history.size() && BUGGIFY) {
								TraceEvent("SSHistoryReboot", self->thisServerID).log();
								throw please_reboot();
							}

							break;
						}
						when(wait(infoChanged)) {}
					}
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
			when(wait(infoChanged)) {}
		}
	}

	return Void();
}

ACTOR Future<Void> replaceTSSInterface(StorageServer* self, StorageServerInterface ssi) {
	// RYW for KeyBackedMap
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);
	state KeyBackedMap<UID, UID> tssMapDB = KeyBackedMap<UID, UID>(tssMappingKeys.begin);

	ASSERT(ssi.isTss());

	loop {
		try {
			state Tag myTag;

			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			Optional<Value> pairTagValue = wait(tr->get(serverTagKeyFor(self->tssPairID.get())));

			if (!pairTagValue.present()) {
				CODE_PROBE(true, "Race where tss was down, pair was removed, tss starts back up");
				TraceEvent("StorageServerWorkerRemoved", self->thisServerID).detail("Reason", "TssPairMissing");
				throw worker_removed();
			}

			myTag = decodeServerTagValue(pairTagValue.get());

			tr->addReadConflictRange(singleKeyRange(serverListKeyFor(ssi.id())));
			tr->set(serverListKeyFor(ssi.id()), serverListValue(ssi));

			// add itself back to tss mapping
			if (!self->isTSSInQuarantine()) {
				tssMapDB.set(tr, self->tssPairID.get(), ssi.id());
			}

			wait(tr->commit());
			self->tag = myTag;

			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	return Void();
}

ACTOR Future<Void> storageInterfaceRegistration(StorageServer* self,
                                                StorageServerInterface ssi,
                                                Optional<Future<Void>> readyToAcceptRequests) {

	if (readyToAcceptRequests.present()) {
		wait(readyToAcceptRequests.get());
		ssi.startAcceptingRequests();
	} else {
		ssi.stopAcceptingRequests();
	}

	try {
		if (self->isTss()) {
			wait(replaceTSSInterface(self, ssi));
		} else {
			wait(replaceInterface(self, ssi));
		}
	} catch (Error& e) {
		throw;
	}

	return Void();
}

ACTOR Future<Void> rocksdbLogCleaner(std::string folder) {
	std::replace(folder.begin(), folder.end(), '/', '_');
	if (!folder.empty() && folder[0] == '_') {
		folder.erase(0, 1);
	}
	try {
		loop {
			wait(delayJittered(SERVER_KNOBS->STORAGE_ROCKSDB_LOG_CLEAN_UP_DELAY));
			TraceEvent("CleanUpRocksDBLogs").detail("LogPrefix", folder);
			auto logFiles = platform::listFiles(SERVER_KNOBS->LOG_DIRECTORY);
			for (const auto& f : logFiles) {
				if (f.find(folder) != std::string::npos) {
					auto filePath = joinPath(SERVER_KNOBS->LOG_DIRECTORY, f);
					auto t = fileModifiedTime(filePath);
					if (now() - t > SERVER_KNOBS->STORAGE_ROCKSDB_LOG_TTL) {
						deleteFile(filePath);
						TraceEvent("DeleteRocksDBLog").detail("FileName", f);
					}
				}
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled) {
			TraceEvent(SevError, "RocksDBLogCleanerError").errorUnsuppressed(e);
		}
	}
	return Void();
}

// for creating a new storage server
ACTOR Future<Void> storageServer(IKeyValueStore* persistentData,
                                 StorageServerInterface ssi,
                                 Tag seedTag,
                                 Version startVersion,
                                 Version tssSeedVersion,
                                 ReplyPromise<InitializeStorageReply> recruitReply,
                                 Reference<AsyncVar<ServerDBInfo> const> db,
                                 std::string folder,
                                 Reference<GetEncryptCipherKeysMonitor> encryptionMonitor) {
	state StorageServer self(persistentData, db, ssi, encryptionMonitor);
	self.shardAware = persistentData->shardAware();
	state Future<Void> ssCore;
	self.initialClusterVersion = startVersion;
	if (ssi.isTss()) {
		self.setTssPair(ssi.tssPairID.get());
		ASSERT(self.isTss());
	}
	static_assert(sizeof(self) < 16384, "FastAlloc doesn't allow allocations larger than 16KB");
	TraceEvent("StorageServerInitProgress", ssi.id())
	    .detail("EngineType", self.storage.getKeyValueStoreType().toString())
	    .detail("Step", "4.StartInit");

	self.sk = serverKeysPrefixFor(self.tssPairID.present() ? self.tssPairID.get() : self.thisServerID)
	              .withPrefix(systemKeys.begin); // FFFF/serverKeys/[this server]/
	self.folder = folder;
	self.checkpointFolder = joinPath(self.folder, serverCheckpointFolder);
	self.fetchedCheckpointFolder = joinPath(self.folder, fetchedCheckpointFolder);
	self.bulkDumpFolder = joinPath(self.folder, serverBulkDumpFolder);
	self.bulkLoadFolder = joinPath(self.folder, serverBulkLoadFolder);
	self.actors.add(rocksdbLogCleaner(folder));

	try {
		wait(self.storage.init());
		TraceEvent("StorageServerInitProgress", ssi.id())
		    .detail("EngineType", self.storage.getKeyValueStoreType().toString())
		    .detail("Step", "5.StorageInited");
		wait(self.storage.commit());
		TraceEvent("StorageServerInitProgress", ssi.id())
		    .detail("EngineType", self.storage.getKeyValueStoreType().toString())
		    .detail("Step", "6.StorageCommitted");
		++self.counters.kvCommits;

		platform::createDirectory(self.checkpointFolder);
		platform::createDirectory(self.fetchedCheckpointFolder);

		clearFileFolder(self.bulkDumpFolder, self.thisServerID, /*ignoreError=*/false);
		clearFileFolder(self.bulkLoadFolder, self.thisServerID, /*ignoreError=*/false);

		EncryptionAtRestMode encryptionMode = wait(self.storage.encryptionMode());
		TraceEvent("StorageServerInitProgress", ssi.id())
		    .detail("EngineType", self.storage.getKeyValueStoreType().toString())
		    .detail("Step", "7.EncryptionMode");
		self.encryptionMode = encryptionMode;

		if (seedTag == invalidTag) {
			ssi.startAcceptingRequests();
			self.registerInterfaceAcceptingRequests.send(Void());

			// Might throw recruitment_failed in case of simultaneous master failure
			std::pair<Version, Tag> verAndTag = wait(addStorageServer(self.cx, ssi));
			TraceEvent("StorageServerInitProgress", ssi.id())
			    .detail("EngineType", self.storage.getKeyValueStoreType().toString())
			    .detail("Step", "8.StorageServerAdded");

			self.tag = verAndTag.second;
			if (ssi.isTss()) {
				self.setInitialVersion(tssSeedVersion);
			} else {
				self.setInitialVersion(verAndTag.first - 1);
			}

			wait(initTenantMap(&self));
			TraceEvent("StorageServerInitProgress", ssi.id())
			    .detail("EngineType", self.storage.getKeyValueStoreType().toString())
			    .detail("Step", "9.TenantMapInited");
		} else {
			self.tag = seedTag;
		}

		self.storage.makeNewStorageServerDurable(self.shardAware);
		wait(self.storage.commit());
		TraceEvent("StorageServerInitProgress", ssi.id())
		    .detail("EngineType", self.storage.getKeyValueStoreType().toString())
		    .detail("Step", "10.NewStorageServerDurable");
		++self.counters.kvCommits;

		self.interfaceRegistered =
		    storageInterfaceRegistration(&self, ssi, self.registerInterfaceAcceptingRequests.getFuture());
		wait(delay(0));

		TraceEvent("StorageServerInit", ssi.id())
		    .detail("EngineType", self.storage.getKeyValueStoreType().toString())
		    .detail("Version", self.version.get())
		    .detail("SeedTag", seedTag.toString())
		    .detail("TssPair", ssi.isTss() ? ssi.tssPairID.get().toString() : "");
		InitializeStorageReply rep;
		rep.interf = ssi;
		rep.addedVersion = self.version.get();
		recruitReply.send(rep);
		self.byteSampleRecovery = Void();
		TraceEvent("StorageServerInitProgress", ssi.id())
		    .detail("EngineType", self.storage.getKeyValueStoreType().toString())
		    .detail("Step", "11.RecruitReplied");

		ssCore = storageServerCore(&self, ssi);
		wait(ssCore);

		throw internal_error();
	} catch (Error& e) {
		// If we die with an error before replying to the recruitment request, send the error to the recruiter
		// (ClusterController, and from there to the DataDistributionTeamCollection)
		if (!recruitReply.isSet())
			recruitReply.sendError(recruitment_failed());

		// If the storage server dies while something that uses self is still on the stack,
		// we want that actor to complete before we terminate and that memory goes out of scope

		self.ssLock->halt();

		self.moveInShards.clear();

		state Error err = e;
		if (storageServerTerminated(self, persistentData, err)) {
			ssCore.cancel();
			self.actors = ActorCollection(false);
			wait(delay(0));
			return Void();
		}
		ssCore.cancel();
		self.actors = ActorCollection(false);
		wait(delay(0));
		throw err;
	}
}

// for recovering an existing storage server
ACTOR Future<Void> storageServer(IKeyValueStore* persistentData,
                                 StorageServerInterface ssi,
                                 Reference<AsyncVar<ServerDBInfo> const> db,
                                 std::string folder,
                                 Promise<Void> recovered,
                                 Reference<IClusterConnectionRecord> connRecord,
                                 Reference<GetEncryptCipherKeysMonitor> encryptionMonitor) {
	state StorageServer self(persistentData, db, ssi, encryptionMonitor);
	state Future<Void> ssCore;
	self.folder = folder;
	self.checkpointFolder = joinPath(self.folder, serverCheckpointFolder);
	self.fetchedCheckpointFolder = joinPath(self.folder, fetchedCheckpointFolder);
	self.bulkDumpFolder = joinPath(self.folder, serverBulkDumpFolder);
	self.bulkLoadFolder = joinPath(self.folder, serverBulkLoadFolder);

	if (!directoryExists(self.checkpointFolder)) {
		TraceEvent(SevWarnAlways, "SSRebootCheckpointDirNotExists", self.thisServerID);
		platform::createDirectory(self.checkpointFolder);
	}
	if (!directoryExists(self.fetchedCheckpointFolder)) {
		TraceEvent(SevWarnAlways, "SSRebootFetchedCheckpointDirNotExists", self.thisServerID);
		platform::createDirectory(self.fetchedCheckpointFolder);
	}

	clearFileFolder(self.bulkDumpFolder, self.thisServerID, /*ignoreError=*/false);
	clearFileFolder(self.bulkLoadFolder, self.thisServerID, /*ignoreError=*/false);

	self.actors.add(rocksdbLogCleaner(folder));
	try {
		state double start = now();
		TraceEvent("StorageServerRebootStart", self.thisServerID).log();

		wait(self.storage.init());
		choose {
			// after a rollback there might be uncommitted changes.
			// for memory storage engine type, wait until recovery is done before commit
			when(wait(self.storage.commit())) {}

			when(wait(memoryStoreRecover(persistentData, connRecord, self.thisServerID))) {
				TraceEvent("DisposeStorageServer", self.thisServerID).log();
				throw worker_removed();
			}
		}
		++self.counters.kvCommits;

		EncryptionAtRestMode encryptionMode = wait(self.storage.encryptionMode());
		self.encryptionMode = encryptionMode;

		bool ok = wait(self.storage.restoreDurableState());
		if (!ok) {
			if (recovered.canBeSet())
				recovered.send(Void());
			return Void();
		}
		TraceEvent("SSTimeRestoreDurableState", self.thisServerID).detail("TimeTaken", now() - start);

		// if this is a tss storage file, use that as source of truth for this server being a tss instead of the
		// presence of the tss pair key in the storage engine
		if (ssi.isTss()) {
			ASSERT(self.isTss());
			ssi.tssPairID = self.tssPairID.get();
		} else {
			ASSERT(!self.isTss());
		}

		ASSERT(self.thisServerID == ssi.id());

		self.sk = serverKeysPrefixFor(self.tssPairID.present() ? self.tssPairID.get() : self.thisServerID)
		              .withPrefix(systemKeys.begin); // FFFF/serverKeys/[this server]/

		TraceEvent("StorageServerReboot", self.thisServerID).detail("Version", self.version.get());

		if (recovered.canBeSet())
			recovered.send(Void());

		state Future<Void> f = storageInterfaceRegistration(&self, ssi, {});
		wait(delay(0));
		ErrorOr<Void> e = wait(errorOr(f));
		if (e.isError()) {
			throw f.getError();
		}

		self.interfaceRegistered =
		    storageInterfaceRegistration(&self, ssi, self.registerInterfaceAcceptingRequests.getFuture());
		wait(delay(0));

		TraceEvent("StorageServerStartingCore", self.thisServerID).detail("TimeTaken", now() - start);

		ssCore = storageServerCore(&self, ssi);
		wait(ssCore);

		throw internal_error();
	} catch (Error& e) {

		self.ssLock->halt();

		if (self.byteSampleRecovery.isValid()) {
			self.byteSampleRecovery.cancel();
		}

		if (recovered.canBeSet())
			recovered.send(Void());

		// If the storage server dies while something that uses self is still on the stack,
		// we want that actor to complete before we terminate and that memory goes out of scope
		state Error err = e;
		if (storageServerTerminated(self, persistentData, err)) {
			ssCore.cancel();
			self.actors = ActorCollection(false);
			wait(delay(0));
			return Void();
		}
		ssCore.cancel();
		self.actors = ActorCollection(false);
		wait(delay(0));
		throw err;
	}
}

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

/*
4 Reference count
4 priority
24 pointers
8 lastUpdateVersion
2 updated, replacedPointer
--
42 PTree overhead

8 Version insertVersion
--
50 VersionedMap overhead

12 KeyRef
12 ValueRef
1  isClear
--
25 payload


50 overhead
25 payload
21 structure padding
32 allocator rounds up
---
128 allocated

To reach 64, need to save: 11 bytes + all padding

Possibilities:
-8 Combine lastUpdateVersion, insertVersion?
-2 Fold together updated, replacedPointer, isClear bits
-3 Fold away updated, replacedPointer, isClear
-8 Move value lengths into arena
-4 Replace priority with H(pointer)
-12 Compress pointers (using special allocator)
-4 Modular lastUpdateVersion (make sure no node survives 4 billion updates)
*/

void versionedMapTest() {
	VersionedMap<int, int> vm;

	printf("SS Ptree node is %zu bytes\n", sizeof(StorageServer::VersionedData::PTreeT));

	const int NSIZE = sizeof(VersionedMap<int, int>::PTreeT);
	const int ASIZE = NSIZE <= 64 ? 64 : nextFastAllocatedSize(NSIZE);

	auto before = FastAllocator<ASIZE>::getTotalMemory();

	for (int v = 1; v <= 1000; ++v) {
		vm.createNewVersion(v);
		for (int i = 0; i < 1000; i++) {
			int k = deterministicRandom()->randomInt(0, 2000000);
			/*for(int k2=k-5; k2<k+5; k2++)
			    if (vm.atLatest().find(k2) != vm.atLatest().end())
			        vm.erase(k2);*/
			vm.erase(k - 5, k + 5);
			vm.insert(k, v);
		}
	}

	auto after = FastAllocator<ASIZE>::getTotalMemory();

	int count = 0;
	for (auto i = vm.atLatest().begin(); i != vm.atLatest().end(); ++i)
		++count;

	printf("PTree node is %d bytes, allocated as %d bytes\n", NSIZE, ASIZE);
	printf("%d distinct after %d insertions\n", count, 1000 * 1000);
	printf("Memory used: %f MB\n", (after - before) / 1e6);
}
