/*
 * StorageServer.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbserver/StorageServerDisk.h"
#include "fdbserver/TransactionTagCounter.h"
#include "flow/PriorityMultiLock.actor.h"

#define PERSIST_PREFIX "\xff\xff"

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
static const std::string rocksdbCheckpointDirPrefix = "/rockscheckpoints_";

struct AddingShard : NonCopyable {
	KeyRange keys;
	Future<Void> fetchClient; // holds FetchKeys() actor
	Promise<Void> fetchComplete;
	Promise<Void> readWrite;

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

	AddingShard(StorageServer* server, KeyRangeRef const& keys);

	// When fetchKeys "partially completes" (splits an adding shard in two), this is used to construct the left half
	AddingShard(AddingShard* prev, KeyRange const& keys)
	  : keys(keys), fetchClient(prev->fetchClient), server(prev->server), transferredVersion(prev->transferredVersion),
	    fetchVersion(prev->fetchVersion), phase(prev->phase) {}
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
};

class ShardInfo : public ReferenceCounted<ShardInfo>, NonCopyable {
	ShardInfo(KeyRange keys, std::unique_ptr<AddingShard>&& adding, StorageServer* readWrite)
	  : adding(std::move(adding)), readWrite(readWrite), keys(keys), shardId(0LL), desiredShardId(0LL), version(0) {}

public:
	// A shard has 3 mutual exclusive states: adding, readWrite and notAssigned.
	std::unique_ptr<AddingShard> adding;
	struct StorageServer* readWrite;
	KeyRange keys;
	uint64_t changeCounter;
	uint64_t shardId;
	uint64_t desiredShardId;
	Version version;

	static ShardInfo* newNotAssigned(KeyRange keys) { return new ShardInfo(keys, nullptr, nullptr); }
	static ShardInfo* newReadWrite(KeyRange keys, StorageServer* data) { return new ShardInfo(keys, nullptr, data); }
	static ShardInfo* newAdding(StorageServer* data, KeyRange keys) {
		return new ShardInfo(keys, std::make_unique<AddingShard>(data, keys), nullptr);
	}
	static ShardInfo* addingSplitLeft(KeyRange keys, AddingShard* oldShard) {
		return new ShardInfo(keys, std::make_unique<AddingShard>(oldShard, keys), nullptr);
	}

	static ShardInfo* newShard(StorageServer* data, const StorageServerShard& shard) {
		ShardInfo* res = nullptr;
		switch (shard.getShardState()) {
		case StorageServerShard::NotAssigned:
			res = newNotAssigned(shard.range);
			break;
		case StorageServerShard::MovingIn:
		case StorageServerShard::ReadWritePending:
			res = newAdding(data, shard.range);
			break;
		case StorageServerShard::ReadWrite:
			res = newReadWrite(shard.range, data);
			break;
		default:
			TraceEvent(SevError, "UnknownShardState").detail("State", shard.shardState);
		}
		res->populateShard(shard);
		return res;
	}

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
		if (this->isReadable()) {
			st = StorageServerShard::ReadWrite;
		} else if (!this->assigned()) {
			st = StorageServerShard::NotAssigned;
		} else {
			ASSERT(this->adding);
			st = this->adding->phase == AddingShard::Waiting ? StorageServerShard::ReadWritePending
			                                                 : StorageServerShard::MovingIn;
		}
		return StorageServerShard(this->keys, this->version, this->shardId, this->desiredShardId, st);
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

	bool isReadable() const { return readWrite != nullptr; }
	bool notAssigned() const { return !readWrite && !adding; }
	bool assigned() const { return readWrite || adding; }
	bool isInVersionedData() const { return readWrite || (adding && adding->isDataTransferred()); }
	bool isCFInVersionedData() const { return readWrite || (adding && adding->isDataAndCFTransferred()); }
	void addMutation(Version version,
	                 bool fromFetch,
	                 MutationRef const& mutation,
	                 MutationRefAndCipherKeys const& encryptedMutation);
	bool isFetched() const { return readWrite || (adding && adding->fetchComplete.isSet()); }

	const char* debugDescribeState() const {
		if (notAssigned())
			return "NotAssigned";
		else if (adding && !adding->isDataAndCFTransferred())
			return "AddingFetchingCF";
		else if (adding && !adding->isDataTransferred())
			return "AddingFetching";
		else if (adding)
			return "AddingTransferred";
		else
			return "ReadWrite";
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
	// A stopped change feed no longer adds new mutations, but is still queriable.
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

	ServerWatchMetadata(Key key, Optional<Value> value, Version version, Optional<TagSet> tags, Optional<UID> debugID)
	  : key(key), value(value), version(version), tags(tags), debugID(debugID) {}
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

private:
	const int64_t id;
	std::vector<Reference<ShardInfo>> ranges;
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
	std::unordered_map<KeyRef, Reference<ServerWatchMetadata>> watchMap; // keep track of server watches

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
	VersionedMap<int64_t, TenantName> tenantMap;
	std::map<Version, std::vector<PendingNewShard>>
	    pendingAddRanges; // Pending requests to add ranges to physical shards
	std::map<Version, std::vector<KeyRange>>
	    pendingRemoveRanges; // Pending requests to remove ranges from physical shards

	Reference<IPageEncryptionKeyProvider> encryptionKeyProvider;

	bool shardAware; // True if the storage server is aware of the physical shards.

	// Histograms
	struct FetchKeysHistograms {
		const Reference<Histogram> latency;
		const Reference<Histogram> bytes;
		const Reference<Histogram> bandwidth;

		FetchKeysHistograms()
		  : latency(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
		                                    FETCH_KEYS_LATENCY_HISTOGRAM,
		                                    Histogram::Unit::milliseconds)),
		    bytes(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
		                                  FETCH_KEYS_BYTES_HISTOGRAM,
		                                  Histogram::Unit::bytes)),
		    bandwidth(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
		                                      FETCH_KEYS_BYTES_PER_SECOND_HISTOGRAM,
		                                      Histogram::Unit::bytes_per_second)) {}
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
	Reference<ServerWatchMetadata> getWatchMetadata(KeyRef key) const;
	KeyRef setWatchMetadata(Reference<ServerWatchMetadata> metadata);
	void deleteWatchMetadata(KeyRef key);
	void clearWatchMetadata();

	// tenant map operations
	void insertTenant(StringRef tenantPrefix, TenantName tenantName, Version version, bool persist);
	void clearTenants(StringRef startTenant, StringRef endTenant, Version version);

	void checkTenantEntry(Version version, TenantInfo tenant);
	KeyRangeRef clampRangeToTenant(KeyRangeRef range, TenantInfo const& tenantInfo, Arena& arena);

	std::vector<StorageServerShard> getStorageServerShards(KeyRangeRef range);

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
	int64_t watchBytes;
	int64_t numWatches;
	AsyncVar<bool> noRecentUpdates;
	double lastUpdate;

	std::string folder;

	// defined only during splitMutations()/addMutation()
	UpdateEagerReadInfo* updateEagerReads;

	FlowLock durableVersionLock;
	FlowLock fetchKeysParallelismLock;
	// Extra lock that prevents too much post-initial-fetch work from building up, such as mutation applying and change
	// feed tail fetching
	FlowLock fetchKeysParallelismFullLock;
	int64_t fetchKeysBytesBudget;
	AsyncVar<bool> fetchKeysBudgetUsed;
	std::vector<Promise<FetchInjectionInfo*>> readyFetchKeys;

	FlowLock serveFetchCheckpointParallelismLock;

	Reference<PriorityMultiLock> ssLock;
	std::vector<int> readPriorityRanks;

	Future<PriorityMultiLock::Lock> getReadLock(const Optional<ReadOptions>& options) {
		int readType = (int)(options.present() ? options.get().type : ReadType::NORMAL);
		readType = std::clamp<int>(readType, 0, readPriorityRanks.size() - 1);
		return ssLock->lock(readPriorityRanks[readType]);
	}

	FlowLock serveAuditStorageParallelismLock;

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

	struct Counters {
		CounterCollection cc;
		Counter allQueries, systemKeyQueries, getKeyQueries, getValueQueries, getRangeQueries, getRangeSystemKeyQueries,
		    getMappedRangeQueries, getRangeStreamQueries, finishedQueries, lowPriorityQueries, rowsQueried,
		    bytesQueried, watchQueries, emptyQueries, feedRowsQueried, feedBytesQueried, feedStreamQueries,
		    rejectedFeedStreamQueries, feedVersionQueries;

		// Bytes of the mutations that have been added to the memory of the storage server. When the data is durable
		// and cleared from the memory, we do not subtract it but add it to bytesDurable.
		Counter bytesInput;
		// Bytes pulled from TLogs, it counts the size of the key value pairs, e.g., key-value pair ("a", "b") is
		// counted as 2 Bytes.
		Counter logicalBytesInput;
		// Bytes pulled from TLogs for moving-in shards, it counts the mutations sent to the moving-in shard during
		// Fetching and Waiting phases.
		Counter logicalBytesMoveInOverhead;
		// Bytes committed to the underlying storage engine by SS, it counts the size of key value pairs.
		Counter kvCommitLogicalBytes;
		// Count of all clearRange operatons to the storage engine.
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
		// Bytes fetched by fetchKeys() for data movements. The size is counted as a collection of KeyValueRef.
		Counter bytesFetched;
		// Like bytesInput but without MVCC accounting. The size is counted as how much it takes when serialized. It
		// is basically the size of both parameters of the mutation and a 12 bytes overhead that keeps mutation type
		// and the lengths of both parameters.
		Counter mutationBytes;

		// Bytes fetched by fetchChangeFeed for data movements.
		Counter feedBytesFetched;

		Counter sampledBytesCleared;
		// The number of key-value pairs fetched by fetchKeys()
		Counter kvFetched;
		Counter mutations, setMutations, clearRangeMutations, atomicMutations, changeFeedMutations,
		    changeFeedMutationsDurable;
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

		LatencySample readLatencySample;
		LatencySample readKeyLatencySample;
		LatencySample readValueLatencySample;
		LatencySample readRangeLatencySample;
		LatencySample readVersionWaitSample;
		LatencySample readQueueWaitSample;
		LatencySample kvReadRangeLatencySample;
		LatencySample updateLatencySample;

		LatencyBands readLatencyBands;
		LatencySample mappedRangeSample; // Samples getMappedRange latency
		LatencySample mappedRangeRemoteSample; // Samples getMappedRange remote subquery latency
		LatencySample mappedRangeLocalSample; // Samples getMappedRange local subquery latency

		Counters(StorageServer* self)
		  : cc("StorageServer", self->thisServerID.toString()), allQueries("QueryQueue", cc),
		    systemKeyQueries("SystemKeyQueries", cc), getKeyQueries("GetKeyQueries", cc),
		    getValueQueries("GetValueQueries", cc), getRangeQueries("GetRangeQueries", cc),
		    getRangeSystemKeyQueries("GetRangeSystemKeyQueries", cc),
		    getMappedRangeQueries("GetMappedRangeQueries", cc), getRangeStreamQueries("GetRangeStreamQueries", cc),
		    finishedQueries("FinishedQueries", cc), lowPriorityQueries("LowPriorityQueries", cc),
		    rowsQueried("RowsQueried", cc), bytesQueried("BytesQueried", cc), watchQueries("WatchQueries", cc),
		    emptyQueries("EmptyQueries", cc), feedRowsQueried("FeedRowsQueried", cc),
		    feedBytesQueried("FeedBytesQueried", cc), feedStreamQueries("FeedStreamQueries", cc),
		    rejectedFeedStreamQueries("RejectedFeedStreamQueries", cc), feedVersionQueries("FeedVersionQueries", cc),
		    bytesInput("BytesInput", cc), logicalBytesInput("LogicalBytesInput", cc),
		    logicalBytesMoveInOverhead("LogicalBytesMoveInOverhead", cc),
		    kvCommitLogicalBytes("KVCommitLogicalBytes", cc), kvClearRanges("KVClearRanges", cc),
		    kvClearSingleKey("KVClearSingleKey", cc), kvSystemClearRanges("KVSystemClearRanges", cc),
		    bytesDurable("BytesDurable", cc), bytesFetched("BytesFetched", cc), mutationBytes("MutationBytes", cc),
		    feedBytesFetched("FeedBytesFetched", cc), sampledBytesCleared("SampledBytesCleared", cc),
		    kvFetched("KVFetched", cc), mutations("Mutations", cc), setMutations("SetMutations", cc),
		    clearRangeMutations("ClearRangeMutations", cc), atomicMutations("AtomicMutations", cc),
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
		    readLatencyBands("ReadLatencyBands", self->thisServerID, SERVER_KNOBS->STORAGE_LOGGING_DELAY),
		    mappedRangeSample("GetMappedRangeMetrics",
		                      self->thisServerID,
		                      SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                      SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
		    mappedRangeRemoteSample("GetMappedRangeRemoteMetrics",
		                            self->thisServerID,
		                            SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
		                            SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
		    mappedRangeLocalSample("GetMappedRangeLocalMetrics",
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
		                        SERVER_KNOBS->LATENCY_SKETCH_ACCURACY) {
			specialCounter(cc, "LastTLogVersion", [self]() { return self->lastTLogVersion; });
			specialCounter(cc, "Version", [self]() { return self->version.get(); });
			specialCounter(cc, "StorageVersion", [self]() { return self->storageVersion(); });
			specialCounter(cc, "DurableVersion", [self]() { return self->durableVersion.get(); });
			specialCounter(cc, "DesiredOldestVersion", [self]() { return self->desiredOldestVersion.get(); });
			specialCounter(cc, "VersionLag", [self]() { return self->versionLag; });
			specialCounter(cc, "LocalRate", [self] { return int64_t(self->currentRate() * 100); });

			specialCounter(cc, "BytesReadSampleCount", [self]() { return self->metrics.bytesReadSample.queue.size(); });
			specialCounter(
			    cc, "FetchKeysFetchActive", [self]() { return self->fetchKeysParallelismLock.activePermits(); });
			specialCounter(cc, "FetchKeysWaiting", [self]() { return self->fetchKeysParallelismLock.waiters(); });
			specialCounter(cc, "FetchKeysFullFetchActive", [self]() {
				return self->fetchKeysParallelismFullLock.activePermits();
			});
			specialCounter(
			    cc, "FetchKeysFullFetchWaiting", [self]() { return self->fetchKeysParallelismFullLock.waiters(); });
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
			specialCounter(cc, "QueryQueueMax", [self]() { return self->getAndResetMaxQueryQueueSize(); });
			specialCounter(cc, "BytesStored", [self]() { return self->metrics.byteSample.getEstimate(allKeys); });
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

	// Connection to blob store for fetchKeys()
	Reference<BlobConnectionProvider> blobConn;

	StorageServer(IKeyValueStore* storage,
	              Reference<AsyncVar<ServerDBInfo> const> const& db,
	              StorageServerInterface const& ssi,
	              Reference<IPageEncryptionKeyProvider> encryptionKeyProvider)
	  : encryptionKeyProvider(encryptionKeyProvider), shardAware(false),
	    tlogCursorReadsLatencyHistogram(Histogram::getHistogram(STORAGESERVER_HISTOGRAM_GROUP,
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
	    byteSampleClears(false, "\xff\xff\xff"_sr), durableInProgress(Void()), watchBytes(0), numWatches(0),
	    noRecentUpdates(false), lastUpdate(now()), updateEagerReads(nullptr),
	    fetchKeysParallelismLock(SERVER_KNOBS->FETCH_KEYS_PARALLELISM),
	    fetchKeysParallelismFullLock(SERVER_KNOBS->FETCH_KEYS_PARALLELISM_FULL),
	    fetchKeysBytesBudget(SERVER_KNOBS->STORAGE_FETCH_BYTES), fetchKeysBudgetUsed(false),
	    serveFetchCheckpointParallelismLock(SERVER_KNOBS->SERVE_FETCH_CHECKPOINT_PARALLELISM),
	    ssLock(makeReference<PriorityMultiLock>(SERVER_KNOBS->STORAGE_SERVER_READ_CONCURRENCY,
	                                            SERVER_KNOBS->STORAGESERVER_READ_PRIORITIES)),
	    serveAuditStorageParallelismLock(SERVER_KNOBS->SERVE_AUDIT_STORAGE_PARALLELISM),
	    instanceID(deterministicRandom()->randomUniqueID().first()), shuttingDown(false), behind(false),
	    versionBehind(false), debug_inApplyUpdate(false), debug_lastValidateTime(0), lastBytesInputEBrake(0),
	    lastDurableVersionEBrake(0), maxQueryQueue(0), transactionTagCounter(ssi.id()),
	    busiestWriteTagContext(ssi.id()), counters(this),
	    storageServerSourceTLogIDEventHolder(
	        makeReference<EventCacheHolder>(ssi.id().toString() + "/StorageServerSourceTLogID")) {
		readPriorityRanks = parseStringToVector<int>(SERVER_KNOBS->STORAGESERVER_READTYPE_PRIORITY_MAP, ',');
		ASSERT(readPriorityRanks.size() > (int)ReadType::MAX);
		version.initMetric("StorageServer.Version"_sr, counters.cc.getId());
		oldestVersion.initMetric("StorageServer.OldestVersion"_sr, counters.cc.getId());
		durableVersion.initMetric("StorageServer.DurableVersion"_sr, counters.cc.getId());
		desiredOldestVersion.initMetric("StorageServer.DesiredOldestVersion"_sr, counters.cc.getId());

		newestAvailableVersion.insert(allKeys, invalidVersion);
		newestDirtyVersion.insert(allKeys, invalidVersion);
		if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA && storage->shardAware()) {
			addShard(ShardInfo::newShard(this, StorageServerShard::notAssigned(allKeys)));
		} else {
			addShard(ShardInfo::newNotAssigned(allKeys));
		}

		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, LockAware::True);

		if (SERVER_KNOBS->BG_METADATA_SOURCE != "tenant") {
			try {
				blobConn = BlobConnectionProvider::newBlobConnectionProvider(SERVER_KNOBS->BG_URL);
			} catch (Error& e) {
				// Skip any error when establishing blob connection
			}
		}
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
		//TraceEvent("AddShard", this->thisServerID).detail("KeyBegin", newShard->keys.begin).detail("KeyEnd", newShard->keys.end).detail("State", newShard->isReadable() ? "Readable" : newShard->notAssigned() ? "NotAssigned" : "Adding").detail("Version", this->version.get());
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
			checkTenantEntry(version.get(), req.tenantInfo);
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
		metrics.getStorageMetrics(req, sb, counters.bytesInput.getRate(), versionLag, lastUpdate);
	}
}; // struct StorageServer
