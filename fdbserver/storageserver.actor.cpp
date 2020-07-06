/*
 * storageserver.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/LoadBalance.h"
#include "flow/IndexedSet.h"
#include "flow/Hash3.h"
#include "flow/ActorCollection.h"
#include "flow/SystemMonitor.h"
#include "flow/Util.h"
#include "fdbclient/Atomic.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/MasterProxyInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Notified.h"
#include "fdbclient/StatusClient.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/VersionedMap.h"
#include "fdbserver/FDBExecHelper.actor.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LatencyBandConfig.h"
#include "fdbserver/LogProtocolMessage.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/MutationTracking.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/StorageMetrics.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbrpc/sim_validation.h"
#include "fdbrpc/Smoother.h"
#include "flow/Stats.h"
#include "flow/TDMetric.actor.h"
#include <type_traits>
#include "flow/actorcompiler.h"  // This must be the last #include.

using std::pair;
using std::make_pair;

#ifndef __INTEL_COMPILER
#pragma region Data Structures
#endif

#define SHORT_CIRCUT_ACTUAL_STORAGE 0

inline bool canReplyWith(Error e) {
	switch(e.code()) {
		case error_code_transaction_too_old:
		case error_code_future_version:
		case error_code_wrong_shard_server:
		case error_code_process_behind:
		//case error_code_all_alternatives_failed:
			return true;
		default:
			return false;
	};
}

struct AddingShard : NonCopyable {
	KeyRange keys;
	Future<Void> fetchClient;			// holds FetchKeys() actor
	Promise<Void> fetchComplete;
	Promise<Void> readWrite;

	std::deque< Standalone<VerUpdateRef> > updates;  // during the Fetching phase, mutations with key in keys and version>=(fetchClient's) fetchVersion;

	struct StorageServer* server;
	Version transferredVersion;

	enum Phase { WaitPrevious, Fetching, Waiting };

	Phase phase;

	AddingShard( StorageServer* server, KeyRangeRef const& keys );

	// When fetchKeys "partially completes" (splits an adding shard in two), this is used to construct the left half
	AddingShard( AddingShard* prev, KeyRange const& keys )
		: keys(keys), fetchClient(prev->fetchClient), server(prev->server), transferredVersion(prev->transferredVersion), phase(prev->phase)
	{
	}
	~AddingShard() {
		if( !fetchComplete.isSet() )
			fetchComplete.send(Void());
		if( !readWrite.isSet() )
			readWrite.send(Void());
	}

	void addMutation( Version version, MutationRef const& mutation );

	bool isTransferred() const { return phase == Waiting; }
};

struct ShardInfo : ReferenceCounted<ShardInfo>, NonCopyable {
	AddingShard* adding;
	struct StorageServer* readWrite;
	KeyRange keys;
	uint64_t changeCounter;

	ShardInfo(KeyRange keys, AddingShard* adding, StorageServer* readWrite)
		: adding(adding), readWrite(readWrite), keys(keys)
	{
	}

	~ShardInfo() {
		delete adding;
	}

	static ShardInfo* newNotAssigned(KeyRange keys) { return new ShardInfo(keys, NULL, NULL); }
	static ShardInfo* newReadWrite(KeyRange keys, StorageServer* data) { return new ShardInfo(keys, NULL, data); }
	static ShardInfo* newAdding(StorageServer* data, KeyRange keys) { return new ShardInfo(keys, new AddingShard(data, keys), NULL); }
	static ShardInfo* addingSplitLeft( KeyRange keys, AddingShard* oldShard) { return new ShardInfo(keys, new AddingShard(oldShard, keys), NULL); }

	bool isReadable() const { return readWrite!=NULL; }
	bool notAssigned() const { return !readWrite && !adding; }
	bool assigned() const { return readWrite || adding; }
	bool isInVersionedData() const { return readWrite || (adding && adding->isTransferred()); }
	void addMutation( Version version, MutationRef const& mutation );
	bool isFetched() const { return readWrite || ( adding && adding->fetchComplete.isSet() ); }

	const char* debugDescribeState() const {
		if (notAssigned()) return "NotAssigned";
		else if (adding && !adding->isTransferred()) return "AddingFetching";
		else if (adding) return "AddingTransferred";
		else return "ReadWrite";
	}
};

struct StorageServerDisk {
	explicit StorageServerDisk( struct StorageServer* data, IKeyValueStore* storage ) : data(data), storage(storage) {}

	void makeNewStorageServerDurable();
	bool makeVersionMutationsDurable( Version& prevStorageVersion, Version newStorageVersion, int64_t& bytesLeft );
	void makeVersionDurable( Version version );
	Future<bool> restoreDurableState();

	void changeLogProtocol(Version version, ProtocolVersion protocol);

	void writeMutation( MutationRef mutation );
	void writeKeyValue( KeyValueRef kv );
	void clearRange( KeyRangeRef keys );

	Future<Void> getError() { return storage->getError(); }
	Future<Void> init() { return storage->init(); }
	Future<Void> commit() { return storage->commit(); }

	// SOMEDAY: Put readNextKeyInclusive in IKeyValueStore
	Future<Key> readNextKeyInclusive( KeyRef key ) { return readFirstKey(storage, KeyRangeRef(key, allKeys.end)); }
	Future<Optional<Value>> readValue( KeyRef key, Optional<UID> debugID = Optional<UID>() ) { return storage->readValue(key, debugID); }
	Future<Optional<Value>> readValuePrefix( KeyRef key, int maxLength, Optional<UID> debugID = Optional<UID>() ) { return storage->readValuePrefix(key, maxLength, debugID); }
	Future<Standalone<RangeResultRef>> readRange( KeyRangeRef keys, int rowLimit = 1<<30, int byteLimit = 1<<30 ) { return storage->readRange(keys, rowLimit, byteLimit); }

	KeyValueStoreType getKeyValueStoreType() const { return storage->getType(); }
	StorageBytes getStorageBytes() const { return storage->getStorageBytes(); }
	std::tuple<size_t, size_t, size_t> getSize() const { return storage->getSize(); }

private:
	struct StorageServer* data;
	IKeyValueStore* storage;

	void writeMutations( MutationListRef mutations, Version debugVersion, const char* debugContext );

	ACTOR static Future<Key> readFirstKey( IKeyValueStore* storage, KeyRangeRef range ) {
		Standalone<RangeResultRef> r = wait( storage->readRange( range, 1 ) );
		if (r.size()) return r[0].key;
		else return range.end;
	}
};

struct UpdateEagerReadInfo {
	std::vector<KeyRef> keyBegin;
	std::vector<Key> keyEnd; // these are for ClearRange

	std::vector<std::pair<KeyRef, int>> keys;
	std::vector<Optional<Value>> value;

	Arena arena;

	void addMutations( VectorRef<MutationRef> const& mutations ) {
		for(auto& m : mutations)
			addMutation(m);
	}

	void addMutation( MutationRef const& m ) {
		// SOMEDAY: Theoretically we can avoid a read if there is an earlier overlapping ClearRange
		if (m.type == MutationRef::ClearRange && !m.param2.startsWith(systemKeys.end))
			keyBegin.push_back( m.param2 );
		else if (m.type == MutationRef::CompareAndClear) {
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
		else if (isAtomicOp((MutationRef::Type) m.type))
			keys.emplace_back(m.param1, m.param2.size());
	}

	void finishKeyBegin() {
		std::sort(keyBegin.begin(), keyBegin.end());
		keyBegin.resize( std::unique(keyBegin.begin(), keyBegin.end()) - keyBegin.begin() );
		std::sort(keys.begin(), keys.end(), [](const pair<KeyRef, int>& lhs, const pair<KeyRef, int>& rhs) { return (lhs.first < rhs.first) || (lhs.first == rhs.first && lhs.second > rhs.second); } );
		keys.resize(std::unique(keys.begin(), keys.end(), [](const pair<KeyRef, int>& lhs, const pair<KeyRef, int>& rhs) { return lhs.first == rhs.first; } ) - keys.begin());
		//value gets populated in doEagerReads
	}

	Optional<Value>& getValue(KeyRef key) {
		int i = std::lower_bound(keys.begin(), keys.end(), pair<KeyRef, int>(key, 0), [](const pair<KeyRef, int>& lhs, const pair<KeyRef, int>& rhs) { return lhs.first < rhs.first; } ) - keys.begin();
		ASSERT( i < keys.size() && keys[i].first == key );
		return value[i];
	}

	KeyRef getKeyEnd( KeyRef key ) {
		int i = std::lower_bound(keyBegin.begin(), keyBegin.end(), key) - keyBegin.begin();
		ASSERT( i < keyBegin.size() && keyBegin[i] == key );
		return keyEnd[i];
	}
};

const int VERSION_OVERHEAD = 64 + sizeof(Version) + sizeof(Standalone<VersionUpdateRef>) + //mutationLog, 64b overhead for map
							 2 * (64 + sizeof(Version) + sizeof(Reference<VersionedMap<KeyRef, ValueOrClearToRef>::PTreeT>)); //versioned map [ x2 for createNewVersion(version+1) ], 64b overhead for map
static int mvccStorageBytes( MutationRef const& m ) { return VersionedMap<KeyRef, ValueOrClearToRef>::overheadPerItem * 2 + (MutationRef::OVERHEAD_BYTES + m.param1.size() + m.param2.size()) * 2; }

struct FetchInjectionInfo {
	Arena arena;
	vector<VerUpdateRef> changes;
};

struct StorageServer {
	typedef VersionedMap<KeyRef, ValueOrClearToRef> VersionedData;

private:
	// versionedData contains sets and clears.

	// * Nonoverlapping: No clear overlaps a set or another clear, or adjoins another clear.
	// ~ Clears are maximal: If versionedData.at(v) contains a clear [b,e) then
	//      there is a key data[e]@v, or e==allKeys.end, or a shard boundary or former boundary at e

	// * Reads are possible: When k is in a readable shard, for any v in [storageVersion, version.get()],
	//      storage[k] + versionedData.at(v)[k] = database[k] @ v    (storage[k] might be @ any version in [durableVersion, storageVersion])

	// * Transferred shards are partially readable: When k is in an adding, transferred shard, for any v in [transferredVersion, version.get()],
	//      storage[k] + versionedData.at(v)[k] = database[k] @ v

	// * versionedData contains versions [storageVersion(), version.get()].  It might also contain version (version.get()+1), in which changeDurableVersion may be deleting ghosts, and/or it might
	//      contain later versions if applyUpdate is on the stack.

	// * Old shards are erased: versionedData.atLatest() has entries (sets or intersecting clears) only for keys in readable or adding,transferred shards.
	//   Earlier versions may have extra entries for shards that *were* readable or adding,transferred when those versions were the latest, but they eventually are forgotten.

	// * Old mutations are erased: All items in versionedData.atLatest() have insertVersion() > durableVersion(), but views
	//   at older versions may contain older items which are also in storage (this is OK because of idempotency)

	VersionedData versionedData;
	std::map<Version, Standalone<VersionUpdateRef>> mutationLog; // versions (durableVersion, version]

public:
	Tag tag;
	vector<pair<Version,Tag>> history;
	vector<pair<Version,Tag>> allHistory;
	Version poppedAllAfter;
	std::map<Version, Arena> freeable;  // for each version, an Arena that must be held until that version is < oldestVersion
	Arena lastArena;
	double cpuUsage;
	double diskUsage;

	std::map<Version, Standalone<VersionUpdateRef>> const& getMutationLog() const { return mutationLog; }
	std::map<Version, Standalone<VersionUpdateRef>>& getMutableMutationLog() { return mutationLog; }
	VersionedData const& data() const { return versionedData; }
	VersionedData& mutableData() { return versionedData; }

	double old_rate = 1.0;
	double currentRate() {
		auto versionLag = version.get() - durableVersion.get();
		double res;
		if (versionLag >= SERVER_KNOBS->STORAGE_DURABILITY_LAG_HARD_MAX) {
			res = 0.0;
		} else if (versionLag > SERVER_KNOBS->STORAGE_DURABILITY_LAG_SOFT_MAX) {
			res = 1.0 - (double(versionLag - SERVER_KNOBS->STORAGE_DURABILITY_LAG_SOFT_MAX) / double(SERVER_KNOBS->STORAGE_DURABILITY_LAG_HARD_MAX-SERVER_KNOBS->STORAGE_DURABILITY_LAG_SOFT_MAX));
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

	void addMutationToMutationLogOrStorage( Version ver, MutationRef m ); // Appends m to mutationLog@ver, or to storage if ver==invalidVersion

	// Update the byteSample, and write the updates to the mutation log@ver, or to storage if ver==invalidVersion
	void byteSampleApplyMutation( MutationRef const& m, Version ver );
	void byteSampleApplySet( KeyValueRef kv, Version ver );
	void byteSampleApplyClear( KeyRangeRef range, Version ver );

	void popVersion(Version v, bool popAllTags = false) {
		if(logSystem) {
			if(v > poppedAllAfter) {
				popAllTags = true;
				poppedAllAfter = std::numeric_limits<Version>::max();
			}

			vector<pair<Version,Tag>>* hist = &history;
			vector<pair<Version,Tag>> allHistoryCopy;
			if(popAllTags) {
				allHistoryCopy = allHistory;
				hist = &allHistoryCopy;
			}

			while(hist->size() && v > hist->back().first ) {
				logSystem->pop( v, hist->back().second );
				hist->pop_back();
			}
			if(hist->size()) {
				logSystem->pop( v, hist->back().second );
			} else {
				logSystem->pop( v, tag );
			}
		}
	}

	Standalone<VersionUpdateRef>& addVersionToMutationLog(Version v) {
		// return existing version...
		auto m = mutationLog.find(v);
		if (m != mutationLog.end())
			return m->second;

		// ...or create a new one
		auto& u = mutationLog[v];
		u.version = v;
		if (lastArena.getSize() >= 65536) lastArena = Arena(4096);
		u.arena() = lastArena;
		counters.bytesInput += VERSION_OVERHEAD;
		return u;
	}

	MutationRef addMutationToMutationLog(Standalone<VersionUpdateRef> &mLV, MutationRef const& m){
		byteSampleApplyMutation(m, mLV.version);
		counters.bytesInput += mvccStorageBytes(m);
		return mLV.mutations.push_back_deep( mLV.arena(), m );
	}

	StorageServerDisk storage;

	KeyRangeMap< Reference<ShardInfo> > shards;
	uint64_t shardChangeCounter;      // max( shards->changecounter )

	KeyRangeMap <bool> cachedRangeMap; // indicates if a key-range is being cached

	// newestAvailableVersion[k]
	//   == invalidVersion -> k is unavailable at all versions
	//   <= storageVersion -> k is unavailable at all versions (but might be read anyway from storage if we are in the process of committing makeShardDurable)
	//   == v              -> k is readable (from storage+versionedData) @ [storageVersion,v], and not being updated when version increases
	//   == latestVersion  -> k is readable (from storage+versionedData) @ [storageVersion,version.get()], and thus stays available when version increases
	CoalescedKeyRangeMap< Version > newestAvailableVersion;

	CoalescedKeyRangeMap< Version > newestDirtyVersion; // Similar to newestAvailableVersion, but includes (only) keys that were only partly available (due to cancelled fetchKeys)

	// The following are in rough order from newest to oldest
	Version lastTLogVersion, lastVersionWithData, restoredVersion;
	NotifiedVersion version;
	NotifiedVersion desiredOldestVersion;    // We can increase oldestVersion (and then durableVersion) to this version when the disk permits
	NotifiedVersion oldestVersion;           // See also storageVersion()
	NotifiedVersion durableVersion; 	     // At least this version will be readable from storage after a power failure
	Version rebootAfterDurableVersion;
	int8_t primaryLocality;

	Deque<std::pair<Version,Version>> recoveryVersionSkips;
	int64_t versionLag; // An estimate for how many versions it takes for the data to move from the logs to this storage server

	ProtocolVersion logProtocol;

	Reference<ILogSystem> logSystem;
	Reference<ILogSystem::IPeekCursor> logCursor;

	UID thisServerID;
	Key sk;
	Reference<AsyncVar<ServerDBInfo>> db;
	Database cx;
	ActorCollection actors;

	StorageServerMetrics metrics;
	CoalescedKeyRangeMap<bool, int64_t, KeyBytesMetric<int64_t>> byteSampleClears;
	AsyncVar<bool> byteSampleClearsTooLarge;
	Future<Void> byteSampleRecovery;
	Future<Void> durableInProgress;

	AsyncMap<Key,bool> watches;
	int64_t watchBytes;
	int64_t numWatches;
	AsyncVar<bool> noRecentUpdates;
	double lastUpdate;

	Int64MetricHandle readQueueSizeMetric;

	std::string folder;

	// defined only during splitMutations()/addMutation()
	UpdateEagerReadInfo *updateEagerReads;

	FlowLock durableVersionLock;
	FlowLock fetchKeysParallelismLock;
	vector< Promise<FetchInjectionInfo*> > readyFetchKeys;

	int64_t instanceID;

	Promise<Void> otherError;
	Promise<Void> coreStarted;
	bool shuttingDown;

	bool behind;
	bool versionBehind;

	Version readTxnLifetime = 5 * SERVER_KNOBS->VERSIONS_PER_SECOND;

	bool debug_inApplyUpdate;
	double debug_lastValidateTime;

	int maxQueryQueue;
	int getAndResetMaxQueryQueueSize() {
		int val = maxQueryQueue;
		maxQueryQueue = 0;
		return val;
	}

	struct TransactionTagCounter {
		struct TagInfo {
			TransactionTag tag;
			double rate;
			double fractionalBusyness;

			TagInfo(TransactionTag const& tag, double rate, double fractionalBusyness) 
			  : tag(tag), rate(rate), fractionalBusyness(fractionalBusyness) {}
		};

		TransactionTagMap<int64_t> intervalCounts;
		int64_t intervalTotalSampledCount = 0;
		TransactionTag busiestTag;
		int64_t busiestTagCount = 0;
		double intervalStart = 0;

		Optional<TagInfo> previousBusiestTag;

		int64_t costFunction(int64_t bytes) {
			return bytes / SERVER_KNOBS->OPERATION_COST_BYTE_FACTOR + 1;
		}

		void addRequest(Optional<TagSet> const& tags, int64_t bytes) {
			if(tags.present()) {
				TEST(true); // Tracking tag on storage server
				double cost = costFunction(bytes);
				for(auto& tag : tags.get()) {
					int64_t &count = intervalCounts[TransactionTag(tag, tags.get().arena)];
					count += cost;
					if(count > busiestTagCount) {
						busiestTagCount = count;
						busiestTag = tag;
					}
				}

				intervalTotalSampledCount += cost;
			}
		}

		void startNewInterval(UID id) {
			double elapsed = now() - intervalStart;
			previousBusiestTag.reset();
			if (intervalStart > 0 && CLIENT_KNOBS->READ_TAG_SAMPLE_RATE > 0 && elapsed > 0) {
				double rate = busiestTagCount / CLIENT_KNOBS->READ_TAG_SAMPLE_RATE / elapsed;
				if(rate > SERVER_KNOBS->MIN_TAG_PAGES_READ_RATE) {
					previousBusiestTag = TagInfo(busiestTag, rate, (double)busiestTagCount / intervalTotalSampledCount);
				}

				TraceEvent("BusiestReadTag", id)
					.detail("Elapsed", elapsed)
					.detail("Tag", printable(busiestTag))
					.detail("TagCost", busiestTagCount)
					.detail("TotalSampledCost", intervalTotalSampledCount)
					.detail("Reported", previousBusiestTag.present())
					.trackLatest(id.toString() + "/BusiestReadTag");
			} 

			intervalCounts.clear();
			intervalTotalSampledCount = 0;
			busiestTagCount = 0;
			intervalStart = now();
		}

		Optional<TagInfo> getBusiestTag() const {
			return previousBusiestTag;
		}
	};

	TransactionTagCounter transactionTagCounter;

	Optional<LatencyBandConfig> latencyBandConfig;

	struct Counters {
		CounterCollection cc;
		Counter allQueries, getKeyQueries, getValueQueries, getRangeQueries, finishedQueries, rowsQueried, bytesQueried, watchQueries, emptyQueries;
		Counter bytesInput, bytesDurable, bytesFetched,
			mutationBytes;  // Like bytesInput but without MVCC accounting
		Counter sampledBytesCleared;
		Counter mutations, setMutations, clearRangeMutations, atomicMutations;
		Counter updateBatches, updateVersions;
		Counter loops;
		Counter fetchWaitingMS, fetchWaitingCount, fetchExecutingMS, fetchExecutingCount;
		Counter readsRejected;

		LatencyBands readLatencyBands;

		Counters(StorageServer* self)
			: cc("StorageServer", self->thisServerID.toString()),
			getKeyQueries("GetKeyQueries", cc),
			getValueQueries("GetValueQueries",cc),
			getRangeQueries("GetRangeQueries", cc),
			allQueries("QueryQueue", cc),
			finishedQueries("FinishedQueries", cc),
			rowsQueried("RowsQueried", cc),
			bytesQueried("BytesQueried", cc),
			watchQueries("WatchQueries", cc),
			emptyQueries("EmptyQueries", cc),
			bytesInput("BytesInput", cc),
			bytesDurable("BytesDurable", cc),
			bytesFetched("BytesFetched", cc),
			mutationBytes("MutationBytes", cc),
			sampledBytesCleared("SampledBytesCleared", cc),
			mutations("Mutations", cc),
			setMutations("SetMutations", cc),
			clearRangeMutations("ClearRangeMutations", cc),
			atomicMutations("AtomicMutations", cc),
			updateBatches("UpdateBatches", cc),
			updateVersions("UpdateVersions", cc),
			loops("Loops", cc),
			fetchWaitingMS("FetchWaitingMS", cc),
			fetchWaitingCount("FetchWaitingCount", cc),
			fetchExecutingMS("FetchExecutingMS", cc),
			fetchExecutingCount("FetchExecutingCount", cc),
			readsRejected("ReadsRejected", cc),
			readLatencyBands("ReadLatencyMetrics", self->thisServerID, SERVER_KNOBS->STORAGE_LOGGING_DELAY)
		{
			specialCounter(cc, "LastTLogVersion", [self](){ return self->lastTLogVersion; });
			specialCounter(cc, "Version", [self](){ return self->version.get(); });
			specialCounter(cc, "StorageVersion", [self](){ return self->storageVersion(); });
			specialCounter(cc, "DurableVersion", [self](){ return self->durableVersion.get(); });
			specialCounter(cc, "DesiredOldestVersion", [self](){ return self->desiredOldestVersion.get(); });
			specialCounter(cc, "VersionLag", [self](){ return self->versionLag; });
			specialCounter(cc, "LocalRate", [self]{ return self->currentRate() * 100; });

			specialCounter(cc, "BytesReadSampleCount", [self]() { return self->metrics.bytesReadSample.queue.size(); });

			specialCounter(cc, "FetchKeysFetchActive", [self](){ return self->fetchKeysParallelismLock.activePermits(); });
			specialCounter(cc, "FetchKeysWaiting", [self](){ return self->fetchKeysParallelismLock.waiters(); });

			specialCounter(cc, "QueryQueueMax", [self](){ return self->getAndResetMaxQueryQueueSize(); });

			specialCounter(cc, "BytesStored", [self](){ return self->metrics.byteSample.getEstimate(allKeys); });
			specialCounter(cc, "ActiveWatches", [self](){ return self->numWatches; });
			specialCounter(cc, "WatchBytes", [self](){ return self->watchBytes; });

			specialCounter(cc, "KvstoreBytesUsed", [self](){ return self->storage.getStorageBytes().used; });
			specialCounter(cc, "KvstoreBytesFree", [self](){ return self->storage.getStorageBytes().free; });
			specialCounter(cc, "KvstoreBytesAvailable", [self](){ return self->storage.getStorageBytes().available; });
			specialCounter(cc, "KvstoreBytesTotal", [self](){ return self->storage.getStorageBytes().total; });
			specialCounter(cc, "KvstoreSizeTotal", [self]() { return std::get<0>(self->storage.getSize()); });
			specialCounter(cc, "KvstoreNodeTotal", [self]() { return std::get<1>(self->storage.getSize()); });
			specialCounter(cc, "KvstoreInlineKey", [self]() { return std::get<2>(self->storage.getSize()); });
		}
	} counters;

	StorageServer(IKeyValueStore* storage, Reference<AsyncVar<ServerDBInfo>> const& db, StorageServerInterface const& ssi)
		:	instanceID(deterministicRandom()->randomUniqueID().first()),
			storage(this, storage), db(db), actors(false),
			lastTLogVersion(0), lastVersionWithData(0), restoredVersion(0),
			rebootAfterDurableVersion(std::numeric_limits<Version>::max()),
			durableInProgress(Void()),
			versionLag(0), primaryLocality(tagLocalityInvalid),
			updateEagerReads(0),
			shardChangeCounter(0),
			fetchKeysParallelismLock(SERVER_KNOBS->FETCH_KEYS_PARALLELISM_BYTES),
			shuttingDown(false), debug_inApplyUpdate(false), debug_lastValidateTime(0), watchBytes(0), numWatches(0),
			logProtocol(0), counters(this), tag(invalidTag), maxQueryQueue(0), thisServerID(ssi.id()),
			readQueueSizeMetric(LiteralStringRef("StorageServer.ReadQueueSize")),
			behind(false), versionBehind(false), byteSampleClears(false, LiteralStringRef("\xff\xff\xff")), noRecentUpdates(false),
			lastUpdate(now()), poppedAllAfter(std::numeric_limits<Version>::max()), cpuUsage(0.0), diskUsage(0.0)
	{
		version.initMetric(LiteralStringRef("StorageServer.Version"), counters.cc.id);
		oldestVersion.initMetric(LiteralStringRef("StorageServer.OldestVersion"), counters.cc.id);
		durableVersion.initMetric(LiteralStringRef("StorageServer.DurableVersion"), counters.cc.id);
		desiredOldestVersion.initMetric(LiteralStringRef("StorageServer.DesiredOldestVersion"), counters.cc.id);

		newestAvailableVersion.insert(allKeys, invalidVersion);
		newestDirtyVersion.insert(allKeys, invalidVersion);
		addShard( ShardInfo::newNotAssigned( allKeys ) );

		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, true, true);
	}

	//~StorageServer() { fclose(log); }

	// Puts the given shard into shards.  The caller is responsible for adding shards
	//   for all ranges in shards.getAffectedRangesAfterInsertion(newShard->keys)), because these
	//   shards are invalidated by the call.
	void addShard( ShardInfo* newShard ) {
		ASSERT( !newShard->keys.empty() );
		newShard->changeCounter = ++shardChangeCounter;
		//TraceEvent("AddShard", this->thisServerID).detail("KeyBegin", newShard->keys.begin).detail("KeyEnd", newShard->keys.end).detail("State", newShard->isReadable() ? "Readable" : newShard->notAssigned() ? "NotAssigned" : "Adding").detail("Version", this->version.get());
		/*auto affected = shards.getAffectedRangesAfterInsertion( newShard->keys, Reference<ShardInfo>() );
		for(auto i = affected.begin(); i != affected.end(); ++i)
			shards.insert( *i, Reference<ShardInfo>() );*/
		shards.insert( newShard->keys, Reference<ShardInfo>(newShard) );
	}
	void addMutation(Version version, MutationRef const& mutation, KeyRangeRef const& shard, UpdateEagerReadInfo* eagerReads );
	void setInitialVersion(Version ver) {
		version = ver;
		desiredOldestVersion = ver;
		oldestVersion = ver;
		durableVersion = ver;
		lastVersionWithData = ver;
		restoredVersion = ver;

		mutableData().createNewVersion(ver);
		mutableData().forgetVersionsBefore(ver);
	}

	// This is the maximum version that might be read from storage (the minimum version is durableVersion)
	Version storageVersion() const { return oldestVersion.get(); }

	bool isReadable( KeyRangeRef const& keys ) {
		auto sh = shards.intersectingRanges(keys);
		for(auto i = sh.begin(); i != sh.end(); ++i)
			if (!i->value()->isReadable())
				return false;
		return true;
	}

	void checkChangeCounter( uint64_t oldShardChangeCounter, KeyRef const& key ) {
		if (oldShardChangeCounter != shardChangeCounter &&
			shards[key]->changeCounter > oldShardChangeCounter)
		{
			TEST(true); // shard change during getValueQ
			throw wrong_shard_server();
		}
	}

	void checkChangeCounter( uint64_t oldShardChangeCounter, KeyRangeRef const& keys ) {
		if (oldShardChangeCounter != shardChangeCounter) {
			auto sh = shards.intersectingRanges(keys);
			for(auto i = sh.begin(); i != sh.end(); ++i)
				if (i->value()->changeCounter > oldShardChangeCounter) {
					TEST(true); // shard change during range operation
					throw wrong_shard_server();
				}
		}
	}

	Counter::Value queueSize() {
		return counters.bytesInput.getValue() - counters.bytesDurable.getValue();
	}

	double getPenalty() {
		return std::max(std::max(1.0, (queueSize() - (SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER -
													  2.0 * SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER)) /
								 SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER),
						(currentRate() < 1e-6 ? 1e6 : 1.0 / currentRate()));
	}

	template<class Reply>
	using isLoadBalancedReply = std::is_base_of<LoadBalancedReply, Reply>;

	template <class Reply>
	static typename std::enable_if<isLoadBalancedReply<Reply>::value, void>::type sendErrorWithPenalty(
		const ReplyPromise<Reply>& promise, const Error& err, double penalty) {
		Reply reply;
		reply.error = err;
		reply.penalty = penalty;
		promise.send(reply);
	}

	template <class Reply>
	static typename std::enable_if<!isLoadBalancedReply<Reply>::value, void>::type sendErrorWithPenalty(
		const ReplyPromise<Reply>& promise, const Error& err, double) {
		promise.sendError(err);
	}

	template<class Request, class HandleFunction>
	Future<Void> readGuard(const Request& request, const HandleFunction& fun) {
		auto rate = currentRate();
		if (rate < SERVER_KNOBS->STORAGE_DURABILITY_LAG_REJECT_THRESHOLD && deterministicRandom()->random01() > std::max(SERVER_KNOBS->STORAGE_DURABILITY_LAG_MIN_RATE, rate/SERVER_KNOBS->STORAGE_DURABILITY_LAG_REJECT_THRESHOLD)) {
			//request.error = future_version();
			sendErrorWithPenalty(request.reply, server_overloaded(), getPenalty());
			++counters.readsRejected;
			return Void();
		}
		return fun(this, request);
	}
};

// If and only if key:=value is in (storage+versionedData),    // NOT ACTUALLY: and key < allKeys.end,
//   and H(key) < |key+value|/bytesPerSample,
//     let sampledSize = max(|key+value|,bytesPerSample)
//     persistByteSampleKeys.begin()+key := sampledSize is in storage
//     (key,sampledSize) is in byteSample

// So P(key is sampled) * sampledSize == |key+value|

void StorageServer::byteSampleApplyMutation( MutationRef const& m, Version ver ){
	if (m.type == MutationRef::ClearRange)
		byteSampleApplyClear( KeyRangeRef(m.param1, m.param2), ver );
	else if (m.type == MutationRef::SetValue)
		byteSampleApplySet( KeyValueRef(m.param1, m.param2), ver );
	else
		ASSERT(false); // Mutation of unknown type modfying byte sample
}

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

/////////////////////////////////// Validation ///////////////////////////////////////
#ifndef __INTEL_COMPILER
#pragma region Validation
#endif
bool validateRange( StorageServer::VersionedData::ViewAtVersion const& view, KeyRangeRef range, Version version, UID id, Version minInsertVersion ) {
	// * Nonoverlapping: No clear overlaps a set or another clear, or adjoins another clear.
	// * Old mutations are erased: All items in versionedData.atLatest() have insertVersion() > durableVersion()

	//TraceEvent("ValidateRange", id).detail("KeyBegin", range.begin).detail("KeyEnd", range.end).detail("Version", version);
	KeyRef k;
	bool ok = true;
	bool kIsClear = false;
	auto i = view.lower_bound(range.begin);
	if (i != view.begin()) --i;
	for(; i != view.end() && i.key() < range.end; ++i) {
		ASSERT( i.insertVersion() > minInsertVersion );
		if (kIsClear && i->isClearTo() ? i.key() <= k : i.key() < k) {
			TraceEvent(SevError,"InvalidRange",id).detail("Key1", k).detail("Key2", i.key()).detail("Version", version);
			ok = false;
		}
		//ASSERT( i.key() >= k );
		kIsClear = i->isClearTo();
		k = kIsClear ? i->getEndKey() : i.key();
	}
	return ok;
}

void validate(StorageServer* data, bool force = false) {
	try {
		if (force || (EXPENSIVE_VALIDATION)) {
			data->newestAvailableVersion.validateCoalesced();
			data->newestDirtyVersion.validateCoalesced();

			for(auto s = data->shards.ranges().begin(); s != data->shards.ranges().end(); ++s) {
				ASSERT( s->value()->keys == s->range() );
				ASSERT( !s->value()->keys.empty() );
			}

			for(auto s = data->shards.ranges().begin(); s != data->shards.ranges().end(); ++s)
				if (s->value()->isReadable()) {
					auto ar = data->newestAvailableVersion.intersectingRanges(s->range());
					for(auto a = ar.begin(); a != ar.end(); ++a)
						ASSERT( a->value() == latestVersion );
				}

			// * versionedData contains versions [storageVersion(), version.get()].  It might also contain version (version.get()+1), in which changeDurableVersion may be deleting ghosts, and/or it might
			//      contain later versions if applyUpdate is on the stack.
			ASSERT( data->data().getOldestVersion() == data->storageVersion() );
			ASSERT( data->data().getLatestVersion() == data->version.get() || data->data().getLatestVersion() == data->version.get()+1 || (data->debug_inApplyUpdate && data->data().getLatestVersion() > data->version.get()) );

			auto latest = data->data().atLatest();

			// * Old shards are erased: versionedData.atLatest() has entries (sets or clear *begins*) only for keys in readable or adding,transferred shards.
			for(auto s = data->shards.ranges().begin(); s != data->shards.ranges().end(); ++s) {
				ShardInfo* shard = s->value().getPtr();
				if (!shard->isInVersionedData()) {
					if (latest.lower_bound(s->begin()) != latest.lower_bound(s->end())) {
						TraceEvent(SevError, "VF", data->thisServerID).detail("LastValidTime", data->debug_lastValidateTime).detail("KeyBegin", s->begin()).detail("KeyEnd", s->end())
							.detail("FirstKey", latest.lower_bound(s->begin()).key()).detail("FirstInsertV", latest.lower_bound(s->begin()).insertVersion());
					}
					ASSERT( latest.lower_bound(s->begin()) == latest.lower_bound(s->end()) );
				}
			}

			latest.validate();
			validateRange(latest, allKeys, data->version.get(), data->thisServerID, data->durableVersion.get());

			data->debug_lastValidateTime = now();
		}
	} catch (...) {
		TraceEvent(SevError, "ValidationFailure", data->thisServerID).detail("LastValidTime", data->debug_lastValidateTime);
		throw;
	}
}
#ifndef __INTEL_COMPILER
#pragma endregion
#endif

void
updateProcessStats(StorageServer* self)
{
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

ACTOR Future<Version> waitForVersionActor(StorageServer* data, Version version) {
	choose {
		when(wait(data->version.whenAtLeast(version))) {
			// FIXME: A bunch of these can block with or without the following delay 0.
			// wait( delay(0) );  // don't do a whole bunch of these at once
			if (version < data->oldestVersion.get()) throw transaction_too_old();  // just in case
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
 
Future<Version> waitForVersion(StorageServer* data, Version version) {
	if (version == latestVersion) {
		version = std::max(Version(1), data->version.get());
	}

	if (version < data->oldestVersion.get() || version <= 0) {
		return transaction_too_old();
	} else if (version <= data->version.get()) {
		return version;
	}

	if ((data->behind || data->versionBehind) && version > data->version.get()) {
		return process_behind();
	}

	if (deterministicRandom()->random01() < 0.001) {
		TraceEvent("WaitForVersion1000x");
	}
	return waitForVersionActor(data, version);
}

ACTOR Future<Version> waitForVersionNoTooOld( StorageServer* data, Version version ) {
	// This could become an Actor transparently, but for now it just does the lookup
	if (version == latestVersion)
		version = std::max(Version(1), data->version.get());
	if (version <= data->version.get())
		return version;
	choose {
		when ( wait( data->version.whenAtLeast(version) ) ) {
			return version;
		}
		when ( wait( delay( SERVER_KNOBS->FUTURE_VERSION_DELAY ) ) ) {
			if(deterministicRandom()->random01() < 0.001)
				TraceEvent(SevWarn, "ShardServerFutureVersion1000x", data->thisServerID)
					.detail("Version", version)
					.detail("MyVersion", data->version.get())
					.detail("ServerID", data->thisServerID);
			throw future_version();
		}
	}
}

ACTOR Future<Void> getValueQ( StorageServer* data, GetValueRequest req ) {
	state int64_t resultSize = 0;

	try {
		++data->counters.getValueQueries;
		++data->counters.allQueries;
		++data->readQueueSizeMetric;
		data->maxQueryQueue = std::max<int>( data->maxQueryQueue, data->counters.allQueries.getValue() - data->counters.finishedQueries.getValue());

		// Active load balancing runs at a very high priority (to obtain accurate queue lengths)
		// so we need to downgrade here
		wait( delay(0, TaskPriority::DefaultEndpoint) );

		if( req.debugID.present() )
			g_traceBatch.addEvent("GetValueDebug", req.debugID.get().first(), "getValueQ.DoRead"); //.detail("TaskID", g_network->getCurrentTask());

		state Optional<Value> v;
		state Version version = wait( waitForVersion( data, req.version ) );
		if( req.debugID.present() )
			g_traceBatch.addEvent("GetValueDebug", req.debugID.get().first(), "getValueQ.AfterVersion"); //.detail("TaskID", g_network->getCurrentTask());

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
			Optional<Value> vv = wait( data->storage.readValue( req.key, req.debugID ) );
			// Validate that while we were reading the data we didn't lose the version or shard
			if (version < data->storageVersion()) {
				TEST(true); // transaction_too_old after readValue
				throw transaction_too_old();
			}
			data->checkChangeCounter(changeCounter, req.key);
			v = vv;
		}

		DEBUG_MUTATION("ShardGetValue", version, MutationRef(MutationRef::DebugKey, req.key, v.present()?v.get():LiteralStringRef("<null>")));
		DEBUG_MUTATION("ShardGetPath", version, MutationRef(MutationRef::DebugKey, req.key, path==0?LiteralStringRef("0"):path==1?LiteralStringRef("1"):LiteralStringRef("2")));

		/*
		StorageMetrics m;
		m.bytesPerKSecond = req.key.size() + (v.present() ? v.get().size() : 0);
		m.iosPerKSecond = 1;
		data->metrics.notify(req.key, m);
		*/

		if (v.present()) {
			++data->counters.rowsQueried;
			resultSize = v.get().size();
			data->counters.bytesQueried += resultSize;
		}
		else {
			++data->counters.emptyQueries;
		}

		if (SERVER_KNOBS->READ_SAMPLING_ENABLED) {
			// If the read yields no value, randomly sample the empty read.
			int64_t bytesReadPerKSecond =
			    v.present() ? std::max((int64_t)(req.key.size() + v.get().size()), SERVER_KNOBS->EMPTY_READ_PENALTY)
			                : SERVER_KNOBS->EMPTY_READ_PENALTY;
			data->metrics.notifyBytesReadPerKSecond(req.key, bytesReadPerKSecond);
		}

		if( req.debugID.present() )
			g_traceBatch.addEvent("GetValueDebug", req.debugID.get().first(), "getValueQ.AfterRead"); //.detail("TaskID", g_network->getCurrentTask());

		// Check if the desired key might be cached
		auto cached = data->cachedRangeMap[req.key];
		//if (cached)
		//	TraceEvent(SevDebug, "SSGetValueCached").detail("Key", req.key);

		GetValueReply reply(v, cached);
		reply.penalty = data->getPenalty();
		req.reply.send(reply);
	} catch (Error& e) {
		if(!canReplyWith(e))
			throw;
		data->sendErrorWithPenalty(req.reply, e, data->getPenalty());
	}

	data->transactionTagCounter.addRequest(req.tags, resultSize);

	++data->counters.finishedQueries;
	--data->readQueueSizeMetric;
	if(data->latencyBandConfig.present()) {
		int maxReadBytes = data->latencyBandConfig.get().readConfig.maxReadBytes.orDefault(std::numeric_limits<int>::max());
		data->counters.readLatencyBands.addMeasurement(timer() - req.requestTime(), resultSize > maxReadBytes);
	}

	return Void();
};

ACTOR Future<Void> watchValue_impl( StorageServer* data, WatchValueRequest req ) {
	try {
		++data->counters.watchQueries;

		if( req.debugID.present() )
			g_traceBatch.addEvent("WatchValueDebug", req.debugID.get().first(), "watchValueQ.Before"); //.detail("TaskID", g_network->getCurrentTask());

		wait(success(waitForVersionNoTooOld(data, req.version)));
		if( req.debugID.present() )
			g_traceBatch.addEvent("WatchValueDebug", req.debugID.get().first(), "watchValueQ.AfterVersion"); //.detail("TaskID", g_network->getCurrentTask());

		state Version minVersion = data->data().latestVersion;
		state Future<Void> watchFuture = data->watches.onChange(req.key);
		loop {
			try {
				state Version latest = data->version.get();
				TEST(latest >= minVersion && latest < data->data().latestVersion); // Starting watch loop with latestVersion > data->version
				GetValueRequest getReq( req.key, latest, req.tags, req.debugID );
				state Future<Void> getValue = getValueQ( data, getReq ); //we are relying on the delay zero at the top of getValueQ, if removed we need one here
				GetValueReply reply = wait( getReq.reply.getFuture() );
				//TraceEvent("WatcherCheckValue").detail("Key",  req.key  ).detail("Value",  req.value  ).detail("CurrentValue",  v  ).detail("Ver", latest);

				if(reply.error.present()) {
					ASSERT(reply.error.get().code() != error_code_future_version);
					throw reply.error.get();
				}
				if(BUGGIFY) {
					throw transaction_too_old();
				}
				
				DEBUG_MUTATION("ShardWatchValue", latest, MutationRef(MutationRef::DebugKey, req.key, reply.value.present() ? StringRef( reply.value.get() ) : LiteralStringRef("<null>") ) );

				if( req.debugID.present() )
					g_traceBatch.addEvent("WatchValueDebug", req.debugID.get().first(), "watchValueQ.AfterRead"); //.detail("TaskID", g_network->getCurrentTask());

				if( reply.value != req.value ) {
					req.reply.send(WatchValueReply{ latest });
					return Void();
				}

				if( data->watchBytes > SERVER_KNOBS->MAX_STORAGE_SERVER_WATCH_BYTES ) {
					TEST(true); //Too many watches, reverting to polling
					data->sendErrorWithPenalty(req.reply, watch_cancelled(), data->getPenalty());
					return Void();
				}

				++data->numWatches;
				data->watchBytes += ( req.key.expectedSize() + req.value.expectedSize() + 1000 );
				try {
					if(latest < minVersion) {
						// If the version we read is less than minVersion, then we may fail to be notified of any changes that occur up to or including minVersion
						// To prevent that, we'll check the key again once the version reaches our minVersion
						watchFuture = watchFuture || data->version.whenAtLeast(minVersion);
					}
					if(BUGGIFY) {
						// Simulate a trigger on the watch that results in the loop going around without the value changing
						watchFuture = watchFuture || delay(deterministicRandom()->random01());
					}
					wait(watchFuture);
					--data->numWatches;
					data->watchBytes -= ( req.key.expectedSize() + req.value.expectedSize() + 1000 );
				} catch( Error &e ) {
					--data->numWatches;
					data->watchBytes -= ( req.key.expectedSize() + req.value.expectedSize() + 1000 );
					throw;
				}
			} catch( Error &e ) {
				if( e.code() != error_code_transaction_too_old ) {
					throw;
				}

				TEST(true); // Reading a watched key failed with transaction_too_old
			}

			watchFuture = data->watches.onChange(req.key);
			wait(data->version.whenAtLeast(data->data().latestVersion));
		}
	} catch (Error& e) {
		if(!canReplyWith(e))
			throw;
		data->sendErrorWithPenalty(req.reply, e, data->getPenalty());
	}
	return Void();
}

ACTOR Future<Void> watchValueQ( StorageServer* data, WatchValueRequest req ) {
	state Future<Void> watch = watchValue_impl( data, req );
	state double startTime = now();

	loop {
		double timeoutDelay = -1;
		if(data->noRecentUpdates.get()) {
			timeoutDelay = std::max(CLIENT_KNOBS->FAST_WATCH_TIMEOUT - (now() - startTime), 0.0);
		} else if(!BUGGIFY) {
			timeoutDelay = std::max(CLIENT_KNOBS->WATCH_TIMEOUT - (now() - startTime), 0.0);
		}
		choose {
			when( wait( watch ) ) {
				return Void();
			}
			when( wait( timeoutDelay < 0 ? Never() : delay(timeoutDelay) ) ) {
				data->sendErrorWithPenalty(req.reply, timed_out(), data->getPenalty());
				return Void();
			}
			when( wait( data->noRecentUpdates.onChange()) ) {}
		}
	}
}

ACTOR Future<Void> getShardState_impl( StorageServer* data, GetShardStateRequest req ) {
	ASSERT( req.mode != GetShardStateRequest::NO_WAIT );

	loop {
		std::vector<Future<Void>> onChange;

		for( auto t : data->shards.intersectingRanges( req.keys ) ) {
			if( !t.value()->assigned() ) {
				onChange.push_back( delay( SERVER_KNOBS->SHARD_READY_DELAY ) );
				break;
			}

			if( req.mode == GetShardStateRequest::READABLE && !t.value()->isReadable() )
				onChange.push_back( t.value()->adding->readWrite.getFuture() );

			if( req.mode == GetShardStateRequest::FETCHING && !t.value()->isFetched() )
				onChange.push_back( t.value()->adding->fetchComplete.getFuture() );
		}

		if( !onChange.size() ) {
			req.reply.send(GetShardStateReply{ data->version.get(), data->durableVersion.get() });
			return Void();
		}

		wait( waitForAll( onChange ) );
		wait( delay(0) ); //onChange could have been triggered by cancellation, let things settle before rechecking
	}
}

ACTOR Future<Void> getShardStateQ( StorageServer* data, GetShardStateRequest req ) {
	choose {
		when( wait( getShardState_impl( data, req ) ) ) {}
		when( wait( delay( g_network->isSimulated() ? 10 : 60 ) ) ) {
			data->sendErrorWithPenalty(req.reply, timed_out(), data->getPenalty());
		}
	}
	return Void();
}

void merge( Arena& arena, VectorRef<KeyValueRef, VecSerStrategy::String>& output,
			VectorRef<KeyValueRef> const& vm_output,
			VectorRef<KeyValueRef> const& base,
			int& vCount, int limit, bool stopAtEndOfBase, int& pos, int limitBytes = 1<<30 )
// Combines data from base (at an older version) with sets from newer versions in [start, end) and appends the first (up to) |limit| rows to output
// If limit<0, base and output are in descending order, and start->key()>end->key(), but start is still inclusive and end is exclusive
{
	ASSERT(limit != 0);

	bool forward = limit>0;
	if (!forward) limit = -limit;
	int adjustedLimit = limit + output.size();
	int accumulatedBytes = 0;
	KeyValueRef const* baseStart = base.begin();
	KeyValueRef const* baseEnd = base.end();
	while (baseStart!=baseEnd && vCount>0 && output.size() < adjustedLimit && accumulatedBytes < limitBytes) {
		if (forward ? baseStart->key < vm_output[pos].key : baseStart->key > vm_output[pos].key) {
			output.push_back_deep( arena, *baseStart++ );
		}
		else {
			output.push_back_deep( arena, vm_output[pos]);
			if (baseStart->key == vm_output[pos].key) ++baseStart;
			++pos;
			vCount--;
		}
		accumulatedBytes += sizeof(KeyValueRef) + output.end()[-1].expectedSize();
	}
	while (baseStart!=baseEnd && output.size() < adjustedLimit && accumulatedBytes < limitBytes) {
		output.push_back_deep( arena, *baseStart++ );
		accumulatedBytes += sizeof(KeyValueRef) + output.end()[-1].expectedSize();
	}
	if( !stopAtEndOfBase ) {
		while (vCount>0 && output.size() < adjustedLimit && accumulatedBytes < limitBytes) {
			output.push_back_deep( arena, vm_output[pos]);
			accumulatedBytes += sizeof(KeyValueRef) + output.end()[-1].expectedSize();
			++pos;
			vCount--;
		}
	}
}

// If limit>=0, it returns the first rows in the range (sorted ascending), otherwise the last rows (sorted descending).
// readRange has O(|result|) + O(log |data|) cost
ACTOR Future<GetKeyValuesReply> readRange( StorageServer* data, Version version, KeyRange range, int limit, int* pLimitBytes ) {
	state GetKeyValuesReply result;
	state StorageServer::VersionedData::ViewAtVersion view = data->data().at(version);
	state StorageServer::VersionedData::iterator vCurrent = view.end();
	state KeyRef readBegin;
	state KeyRef readEnd;
	state Key readBeginTemp;
	state int vCount = 0;

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

		vCurrent = view.lower_bound(readBegin);

		while (limit>0 && *pLimitBytes>0 && readBegin < range.end) {
			ASSERT( !vCurrent || vCurrent.key() >= readBegin );
			ASSERT( data->storageVersion() <= version );

			/* Traverse the PTree further, if thare are no unconsumed resultCache items */
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
					resultCache.push_back(result.arena, KeyValueRef(vCurrent.key(), vCurrent->getValue()));
					vSize += sizeof(KeyValueRef) + resultCache.cback().expectedSize();
					++vCount;
					++vCurrent;
				}
			}

			// Read the data on disk up to vCurrent (or the end of the range)
			readEnd = vCurrent ? std::min( vCurrent.key(), range.end ) : range.end;
			Standalone<RangeResultRef> atStorageVersion = wait(
				data->storage.readRange( KeyRangeRef(readBegin, readEnd), limit, *pLimitBytes ) );

			ASSERT( atStorageVersion.size() <= limit );
			if (data->storageVersion() > version) throw transaction_too_old();

			// merge the sets in resultCache with the sets on disk, stopping at the last key from disk if there is 'more'
			int prevSize = result.data.size();
			merge( result.arena, result.data, resultCache,
				   atStorageVersion, vCount, limit, atStorageVersion.more, pos, *pLimitBytes );
			limit -= result.data.size() - prevSize;

			for (auto i = result.data.begin() + prevSize; i != result.data.end(); i++) {
				*pLimitBytes -= sizeof(KeyValueRef) + i->expectedSize();
			}

			if (limit <=0 || *pLimitBytes <= 0) {
				break;
			}

			// Setup for the next iteration
			// If we hit our limits reading from disk but then combining with MVCC gave us back more room
			if (atStorageVersion.more) { // if there might be more data, begin reading right after what we already found to find out
				ASSERT(result.data.end()[-1].key == atStorageVersion.end()[-1].key);
				readBegin = readBeginTemp = keyAfter( result.data.end()[-1].key );
			} else if (vCurrent && vCurrent->isClearTo()){ // if vCurrent is a clear, skip it.
				ASSERT(vCurrent->getEndKey() > readBegin);
				readBegin = vCurrent->getEndKey();  // next disk read should start at the end of the clear
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

			/* Traverse the PTree further, if thare are no unconsumed resultCache items */
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
					resultCache.push_back(result.arena, KeyValueRef(vCurrent.key(), vCurrent->getValue()));
					vSize += sizeof(KeyValueRef) + resultCache.cback().expectedSize();
					++vCount;
					--vCurrent;
				}
			}

			readBegin = vCurrent ? std::max(vCurrent->isClearTo() ? vCurrent->getEndKey() : vCurrent.key(), range.begin) : range.begin;
			Standalone<RangeResultRef> atStorageVersion =
			    wait(data->storage.readRange(KeyRangeRef(readBegin, readEnd), limit, *pLimitBytes));

			ASSERT(atStorageVersion.size() <= -limit);
			if (data->storageVersion() > version) throw transaction_too_old();

			int prevSize = result.data.size();
			merge( result.arena, result.data, resultCache,
				   atStorageVersion, vCount, limit, atStorageVersion.more, pos, *pLimitBytes );
			limit += result.data.size() - prevSize;

			for (auto i = result.data.begin() + prevSize; i != result.data.end(); i++) {
				*pLimitBytes -= sizeof(KeyValueRef) + i->expectedSize();
			}

			if (limit >=0 || *pLimitBytes <= 0) {
				break;
			}

			if (atStorageVersion.more) {
				ASSERT(result.data.end()[-1].key == atStorageVersion.end()[-1].key);
				readEnd = result.data.end()[-1].key;
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

	// all but the last item are less than *pLimitBytes
	ASSERT(result.data.size() == 0 || *pLimitBytes + result.data.end()[-1].expectedSize() + sizeof(KeyValueRef) > 0);
	result.more = limit == 0 || *pLimitBytes<=0;  // FIXME: Does this have to be exact?
	result.version = version;
	return result;
}

//bool selectorInRange( KeySelectorRef const& sel, KeyRangeRef const& range ) {
	// Returns true if the given range suffices to at least begin to resolve the given KeySelectorRef
//	return sel.getKey() >= range.begin && (sel.isBackward() ? sel.getKey() <= range.end : sel.getKey() < range.end);
//}

ACTOR Future<Key> findKey( StorageServer* data, KeySelectorRef sel, Version version, KeyRange range, int* pOffset)
// Attempts to find the key indicated by sel in the data at version, within range.
// Precondition: selectorInRange(sel, range)
// If it is found, offset is set to 0 and a key is returned which falls inside range.
// If the search would depend on any key outside range OR if the key selector offset is too large (range read returns too many bytes), it returns either
//   a negative offset and a key in [range.begin, sel.getKey()], indicating the key is (the first key <= returned key) + offset, or
//   a positive offset and a key in (sel.getKey(), range.end], indicating the key is (the first key >= returned key) + offset-1
// The range passed in to this function should specify a shard.  If range.begin is repeatedly not the beginning of a shard, then it is possible to get stuck looping here
{
	ASSERT( version != latestVersion );
	ASSERT( selectorInRange(sel, range) && version >= data->oldestVersion.get() );

	// Count forward or backward distance items, skipping the first one if it == key and skipEqualKey
	state bool forward = sel.offset > 0;                  // If forward, result >= sel.getKey(); else result <= sel.getKey()
	state int sign = forward ? +1 : -1;
	state bool skipEqualKey = sel.orEqual == forward;
	state int distance = forward ? sel.offset : 1-sel.offset;

	//Don't limit the number of bytes if this is a trivial key selector (there will be at most two items returned from the read range in this case)
	state int maxBytes;
	if (sel.offset <= 1 && sel.offset >= 0)
		maxBytes = std::numeric_limits<int>::max();
	else
		maxBytes = BUGGIFY ? SERVER_KNOBS->BUGGIFY_LIMIT_BYTES : SERVER_KNOBS->STORAGE_LIMIT_BYTES;

	state GetKeyValuesReply rep = wait( readRange( data, version, forward ? KeyRangeRef(sel.getKey(), range.end) : KeyRangeRef(range.begin, keyAfter(sel.getKey())), (distance + skipEqualKey)*sign, &maxBytes ) );
	state bool more = rep.more && rep.data.size() != distance + skipEqualKey;

	//If we get only one result in the reverse direction as a result of the data being too large, we could get stuck in a loop
	if(more && !forward && rep.data.size() == 1) {
		TEST(true); //Reverse key selector returned only one result in range read
		maxBytes = std::numeric_limits<int>::max();
		GetKeyValuesReply rep2 = wait( readRange( data, version, KeyRangeRef(range.begin, keyAfter(sel.getKey())), -2, &maxBytes ) );
		rep = rep2;
		more = rep.more && rep.data.size() != distance + skipEqualKey;
		ASSERT(rep.data.size() == 2 || !more);
	}

	int index = distance-1;
	if (skipEqualKey && rep.data.size() && rep.data[0].key == sel.getKey() )
		++index;

	if (index < rep.data.size()) {
		*pOffset = 0;

		if (SERVER_KNOBS->READ_SAMPLING_ENABLED) {
			int64_t bytesReadPerKSecond =
			    std::max((int64_t)rep.data[index].key.size(), SERVER_KNOBS->EMPTY_READ_PENALTY);
			data->metrics.notifyBytesReadPerKSecond(sel.getKey(), bytesReadPerKSecond);
		}

		return rep.data[ index ].key;
	} else {
		if (SERVER_KNOBS->READ_SAMPLING_ENABLED) {
			int64_t bytesReadPerKSecond = SERVER_KNOBS->EMPTY_READ_PENALTY;
			data->metrics.notifyBytesReadPerKSecond(sel.getKey(), bytesReadPerKSecond);
		}

		// FIXME: If range.begin=="" && !forward, return success?
		*pOffset = index - rep.data.size() + 1;
		if (!forward) *pOffset = -*pOffset;

		if (more) {
			TEST(true); // Key selector read range had more results

			ASSERT(rep.data.size());
			Key returnKey = forward ? keyAfter(rep.data.back().key) : rep.data.back().key;

			//This is possible if key/value pairs are very large and only one result is returned on a last less than query
			//SOMEDAY: graceful handling of exceptionally sized values
			ASSERT(returnKey != sel.getKey());

			return returnKey;
		} else
			return forward ? range.end : range.begin;
	}
}

KeyRange getShardKeyRange( StorageServer* data, const KeySelectorRef& sel )
// Returns largest range such that the shard state isReadable and selectorInRange(sel, range) or wrong_shard_server if no such range exists
{
	auto i = sel.isBackward() ? data->shards.rangeContainingKeyBefore( sel.getKey() ) : data->shards.rangeContaining( sel.getKey() );
	if (!i->value()->isReadable()) throw wrong_shard_server();
	ASSERT( selectorInRange(sel, i->range()) );
	return i->range();
}

ACTOR Future<Void> getKeyValuesQ( StorageServer* data, GetKeyValuesRequest req )
// Throws a wrong_shard_server if the keys in the request or result depend on data outside this server OR if a large selector offset prevents
// all data from being read in one range read
{
	state int64_t resultSize = 0;

	++data->counters.getRangeQueries;
	++data->counters.allQueries;
	++data->readQueueSizeMetric;
	data->maxQueryQueue = std::max<int>( data->maxQueryQueue, data->counters.allQueries.getValue() - data->counters.finishedQueries.getValue());

	// Active load balancing runs at a very high priority (to obtain accurate queue lengths)
	// so we need to downgrade here
	if (SERVER_KNOBS->FETCH_KEYS_LOWER_PRIORITY && req.isFetchKeys) {
		wait( delay(0, TaskPriority::FetchKeys) );
	// } else if (false) {
	// 	// Placeholder for up-prioritizing fetches for important requests
	// 	taskType = TaskPriority::DefaultDelay;
	} else {
		wait( delay(0, TaskPriority::DefaultEndpoint) );
	}
	
	try {
		if( req.debugID.present() )
			g_traceBatch.addEvent("TransactionDebug", req.debugID.get().first(), "storageserver.getKeyValues.Before");
		state Version version = wait( waitForVersion( data, req.version ) );

		state uint64_t changeCounter = data->shardChangeCounter;
//		try {
		state KeyRange shard = getShardKeyRange( data, req.begin );

		if( req.debugID.present() )
			g_traceBatch.addEvent("TransactionDebug", req.debugID.get().first(), "storageserver.getKeyValues.AfterVersion");
		//.detail("ShardBegin", shard.begin).detail("ShardEnd", shard.end);
		//} catch (Error& e) { TraceEvent("WrongShardServer", data->thisServerID).detail("Begin", req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("Shard", "None").detail("In", "getKeyValues>getShardKeyRange"); throw e; }

		if ( !selectorInRange(req.end, shard) && !(req.end.isFirstGreaterOrEqual() && req.end.getKey() == shard.end) ) {
//			TraceEvent("WrongShardServer1", data->thisServerID).detail("Begin", req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("ShardBegin", shard.begin).detail("ShardEnd", shard.end).detail("In", "getKeyValues>checkShardExtents");
			throw wrong_shard_server();
		}

		state int offset1;
		state int offset2;
		state Future<Key> fBegin = req.begin.isFirstGreaterOrEqual() ? Future<Key>(req.begin.getKey()) : findKey( data, req.begin, version, shard, &offset1 );
		state Future<Key> fEnd = req.end.isFirstGreaterOrEqual() ? Future<Key>(req.end.getKey()) : findKey( data, req.end, version, shard, &offset2 );
		state Key begin = wait(fBegin);
		state Key end = wait(fEnd);
		if( req.debugID.present() )
			g_traceBatch.addEvent("TransactionDebug", req.debugID.get().first(), "storageserver.getKeyValues.AfterKeys");
		//.detail("Off1",offset1).detail("Off2",offset2).detail("ReqBegin",req.begin.getKey()).detail("ReqEnd",req.end.getKey());

		// Offsets of zero indicate begin/end keys in this shard, which obviously means we can answer the query
		// An end offset of 1 is also OK because the end key is exclusive, so if the first key of the next shard is the end the last actual key returned must be from this shard.
		// A begin offset of 1 is also OK because then either begin is past end or equal to end (so the result is definitely empty)
		if ((offset1 && offset1!=1) || (offset2 && offset2!=1)) {
			TEST(true);  // wrong_shard_server due to offset
			// We could detect when offset1 takes us off the beginning of the database or offset2 takes us off the end, and return a clipped range rather
			// than an error (since that is what the NativeAPI.getRange will do anyway via its "slow path"), but we would have to add some flags to the response
			// to encode whether we went off the beginning and the end, since it needs that information.
			//TraceEvent("WrongShardServer2", data->thisServerID).detail("Begin", req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("ShardBegin", shard.begin).detail("ShardEnd", shard.end).detail("In", "getKeyValues>checkOffsets").detail("BeginKey", begin).detail("EndKey", end).detail("BeginOffset", offset1).detail("EndOffset", offset2);
			throw wrong_shard_server();
		}

		if (begin >= end) {
			if( req.debugID.present() )
				g_traceBatch.addEvent("TransactionDebug", req.debugID.get().first(), "storageserver.getKeyValues.Send");
			//.detail("Begin",begin).detail("End",end);

			GetKeyValuesReply none;
			none.version = version;
			none.more = false;
			none.penalty = data->getPenalty();

			data->checkChangeCounter( changeCounter, KeyRangeRef( std::min<KeyRef>(req.begin.getKey(), req.end.getKey()), std::max<KeyRef>(req.begin.getKey(), req.end.getKey()) ) );
			req.reply.send( none );
		} else {
			state int remainingLimitBytes = req.limitBytes;

			GetKeyValuesReply _r = wait( readRange(data, version, KeyRangeRef(begin, end), req.limit, &remainingLimitBytes) );
			GetKeyValuesReply r = _r;

			if( req.debugID.present() )
				g_traceBatch.addEvent("TransactionDebug", req.debugID.get().first(), "storageserver.getKeyValues.AfterReadRange");
			//.detail("Begin",begin).detail("End",end).detail("SizeOf",r.data.size());
			data->checkChangeCounter( changeCounter, KeyRangeRef( std::min<KeyRef>(begin, std::min<KeyRef>(req.begin.getKey(), req.end.getKey())), std::max<KeyRef>(end, std::max<KeyRef>(req.begin.getKey(), req.end.getKey())) ) );
			if (EXPENSIVE_VALIDATION) {
				for (int i = 0; i < r.data.size(); i++)
					ASSERT(r.data[i].key >= begin && r.data[i].key < end);
				ASSERT(r.data.size() <= std::abs(req.limit));
			}

			/*for( int i = 0; i < r.data.size(); i++ ) {
				StorageMetrics m;
				m.bytesPerKSecond = r.data[i].expectedSize();
				m.iosPerKSecond = 1; //FIXME: this should be 1/r.data.size(), but we cannot do that because it is an int
				data->metrics.notify(r.data[i].key, m);
			}*/

			// For performance concerns, the cost of a range read is billed to the start key and end key of the range.
			int64_t totalByteSize = 0;
			for (int i = 0; i < r.data.size(); i++) {
				totalByteSize += r.data[i].expectedSize();
			}
			if (totalByteSize > 0 && SERVER_KNOBS->READ_SAMPLING_ENABLED) {
				int64_t bytesReadPerKSecond = std::max(totalByteSize, SERVER_KNOBS->EMPTY_READ_PENALTY) / 2;
				data->metrics.notifyBytesReadPerKSecond(r.data[0].key, bytesReadPerKSecond);
				data->metrics.notifyBytesReadPerKSecond(r.data[r.data.size() - 1].key, bytesReadPerKSecond);
			}

			r.penalty = data->getPenalty();
			req.reply.send( r );

			resultSize = req.limitBytes - remainingLimitBytes;
			data->counters.bytesQueried += resultSize;
			data->counters.rowsQueried += r.data.size();
			if(r.data.size() == 0) {
				++data->counters.emptyQueries;
			}
		}
	} catch (Error& e) {
		if(!canReplyWith(e))
			throw;
		data->sendErrorWithPenalty(req.reply, e, data->getPenalty());
	}

	data->transactionTagCounter.addRequest(req.tags, resultSize);
	++data->counters.finishedQueries;
	--data->readQueueSizeMetric;

	if(data->latencyBandConfig.present()) {
		int maxReadBytes = data->latencyBandConfig.get().readConfig.maxReadBytes.orDefault(std::numeric_limits<int>::max());
		int maxSelectorOffset = data->latencyBandConfig.get().readConfig.maxKeySelectorOffset.orDefault(std::numeric_limits<int>::max());
		data->counters.readLatencyBands.addMeasurement(
		    timer() - req.requestTime(), resultSize > maxReadBytes || abs(req.begin.offset) > maxSelectorOffset ||
		                                     abs(req.end.offset) > maxSelectorOffset);
	}

	return Void();
}

ACTOR Future<Void> getKeyQ( StorageServer* data, GetKeyRequest req ) {
	state int64_t resultSize = 0;

	++data->counters.getKeyQueries;
	++data->counters.allQueries;
	++data->readQueueSizeMetric;
	data->maxQueryQueue = std::max<int>( data->maxQueryQueue, data->counters.allQueries.getValue() - data->counters.finishedQueries.getValue());

	// Active load balancing runs at a very high priority (to obtain accurate queue lengths)
	// so we need to downgrade here
	wait( delay(0, TaskPriority::DefaultEndpoint) );

	try {
		state Version version = wait( waitForVersion( data, req.version ) );
		state uint64_t changeCounter = data->shardChangeCounter;
		state KeyRange shard = getShardKeyRange( data, req.sel );

		state int offset;
		Key k = wait( findKey( data, req.sel, version, shard, &offset ) );

		data->checkChangeCounter( changeCounter, KeyRangeRef( std::min<KeyRef>(req.sel.getKey(), k), std::max<KeyRef>(req.sel.getKey(), k) ) );

		KeySelector updated;
		if (offset < 0)
			updated = firstGreaterOrEqual(k)+offset;  // first thing on this shard OR (large offset case) smallest key retrieved in range read
		else if (offset > 0)
			updated = firstGreaterOrEqual(k)+offset-1;	// first thing on next shard OR (large offset case) keyAfter largest key retrieved in range read
		else
			updated = KeySelectorRef(k,true,0); //found

		resultSize = k.size();
		data->counters.bytesQueried += resultSize;
		++data->counters.rowsQueried;

		// Check if the desired key might be cached
		auto cached = data->cachedRangeMap[k];
		//if (cached)
		//	TraceEvent(SevDebug, "SSGetKeyCached").detail("Key", k).detail("Begin", shard.begin.printable()).detail("End", shard.end.printable());

		GetKeyReply reply(updated, cached);
		reply.penalty = data->getPenalty();

		req.reply.send(reply);
	}
	catch (Error& e) {
		//if (e.code() == error_code_wrong_shard_server) TraceEvent("WrongShardServer").detail("In","getKey");
		if(!canReplyWith(e))
			throw;
		data->sendErrorWithPenalty(req.reply, e, data->getPenalty());
	}

	// SOMEDAY: The size reported here is an undercount of the bytes read due to the fact that we have to scan for the key
	// It would be more accurate to count all the read bytes, but it's not critical because this function is only used if
	// read-your-writes is disabled
	data->transactionTagCounter.addRequest(req.tags, resultSize);

	++data->counters.finishedQueries;
	--data->readQueueSizeMetric;
	if(data->latencyBandConfig.present()) {
		int maxReadBytes = data->latencyBandConfig.get().readConfig.maxReadBytes.orDefault(std::numeric_limits<int>::max());
		int maxSelectorOffset = data->latencyBandConfig.get().readConfig.maxKeySelectorOffset.orDefault(std::numeric_limits<int>::max());
		data->counters.readLatencyBands.addMeasurement(
		    timer() - req.requestTime(), resultSize > maxReadBytes || abs(req.sel.offset) > maxSelectorOffset);
	}

	return Void();
}

void getQueuingMetrics( StorageServer* self, StorageQueuingMetricsRequest const& req ) {
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

	Optional<StorageServer::TransactionTagCounter::TagInfo> busiestTag = self->transactionTagCounter.getBusiestTag();
	reply.busiestTag = busiestTag.map<TransactionTag>([](StorageServer::TransactionTagCounter::TagInfo tagInfo) { return tagInfo.tag; });
	reply.busiestTagFractionalBusyness = busiestTag.present() ? busiestTag.get().fractionalBusyness : 0.0;
	reply.busiestTagRate = busiestTag.present() ? busiestTag.get().rate : 0.0;

	req.reply.send( reply );
}

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

/////////////////////////// Updates ////////////////////////////////
#ifndef __INTEL_COMPILER
#pragma region Updates
#endif

ACTOR Future<Void> doEagerReads( StorageServer* data, UpdateEagerReadInfo* eager ) {
	eager->finishKeyBegin();

	vector<Future<Key>> keyEnd( eager->keyBegin.size() );
	for(int i=0; i<keyEnd.size(); i++)
		keyEnd[i] = data->storage.readNextKeyInclusive( eager->keyBegin[i] );

	state Future<vector<Key>> futureKeyEnds = getAll(keyEnd);

	vector<Future<Optional<Value>>> value( eager->keys.size() );
	for(int i=0; i<value.size(); i++)
		value[i] = data->storage.readValuePrefix( eager->keys[i].first, eager->keys[i].second );

	state Future<vector<Optional<Value>>> futureValues = getAll(value);
	state vector<Key> keyEndVal = wait( futureKeyEnds );
	vector<Optional<Value>> optionalValues = wait ( futureValues);

	eager->keyEnd = keyEndVal;
	eager->value = optionalValues;

	return Void();
}

bool changeDurableVersion( StorageServer* data, Version desiredDurableVersion ) {
	// Remove entries from the latest version of data->versionedData that haven't changed since they were inserted
	//   before or at desiredDurableVersion, to maintain the invariants for versionedData.
	// Such entries remain in older versions of versionedData until they are forgotten, because it is expensive to dig them out.
	// We also remove everything up to and including newDurableVersion from mutationLog, and everything
	//   up to but excluding desiredDurableVersion from freeable
	// May return false if only part of the work has been done, in which case the caller must call again with the same parameters

	auto& verData = data->mutableData();
	ASSERT( verData.getLatestVersion() == data->version.get() || verData.getLatestVersion() == data->version.get()+1 );

	Version nextDurableVersion = desiredDurableVersion;

	auto mlv = data->getMutationLog().begin();
	if (mlv != data->getMutationLog().end() && mlv->second.version <= desiredDurableVersion) {
		auto& v = mlv->second;
		nextDurableVersion = v.version;
		data->freeable[ data->version.get() ].dependsOn( v.arena() );

		if (verData.getLatestVersion() <= data->version.get())
			verData.createNewVersion( data->version.get()+1 );

		int64_t bytesDurable = VERSION_OVERHEAD;
		for(auto m = v.mutations.begin(); m; ++m) {
			bytesDurable += mvccStorageBytes(*m);
			auto i = verData.atLatest().find(m->param1);
			if (i) {
				ASSERT( i.key() == m->param1 );
				ASSERT( i.insertVersion() >= nextDurableVersion );
				if (i.insertVersion() == nextDurableVersion)
					verData.erase(i);
			}
			if (m->type == MutationRef::SetValue) {
				// A set can split a clear, so there might be another entry immediately after this one that should also be cleaned up
				i = verData.atLatest().upper_bound(m->param1);
				if (i) {
					ASSERT( i.insertVersion() >= nextDurableVersion );
					if (i.insertVersion() == nextDurableVersion)
						verData.erase(i);
				}
			}
		}
		data->counters.bytesDurable += bytesDurable;
	}

	if (EXPENSIVE_VALIDATION) {
		// Check that the above loop did its job
		auto view = data->data().atLatest();
		for(auto i = view.begin(); i != view.end(); ++i)
			ASSERT( i.insertVersion() > nextDurableVersion );
	}
	data->getMutableMutationLog().erase(data->getMutationLog().begin(), data->getMutationLog().upper_bound(nextDurableVersion));
	data->freeable.erase( data->freeable.begin(), data->freeable.lower_bound(nextDurableVersion) );

	Future<Void> checkFatalError = data->otherError.getFuture();
	data->durableVersion.set( nextDurableVersion );
	setDataDurableVersion(data->thisServerID, data->durableVersion.get());
	if (checkFatalError.isReady()) checkFatalError.get();

	//TraceEvent("ForgotVersionsBefore", data->thisServerID).detail("Version", nextDurableVersion);
	validate(data);

	return nextDurableVersion == desiredDurableVersion;
}

Optional<MutationRef> clipMutation( MutationRef const& m, KeyRangeRef range ) {
	if (isSingleKeyMutation((MutationRef::Type) m.type)) {
		if (range.contains(m.param1)) return m;
	}
	else if (m.type == MutationRef::ClearRange) {
		KeyRangeRef i = range & KeyRangeRef(m.param1, m.param2);
		if (!i.empty())
			return MutationRef( (MutationRef::Type)m.type, i.begin, i.end );
	}
	else
		ASSERT(false);
	return Optional<MutationRef>();
}

bool expandMutation( MutationRef& m, StorageServer::VersionedData const& data, UpdateEagerReadInfo* eager, KeyRef eagerTrustedEnd, Arena& ar ) {
	// After this function call, m should be copied into an arena immediately (before modifying data, shards, or eager)
	if (m.type == MutationRef::ClearRange) {
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
		} else {
			// Expand to the next set or clear (from storage or latestVersion), and if it
			// is a clear, engulf it as well
			i = d.lower_bound(m.param2);
			KeyRef endKeyAtStorageVersion = m.param2 == eagerTrustedEnd ? eagerTrustedEnd : std::min( eager->getKeyEnd( m.param2 ), eagerTrustedEnd );
			if (!i || endKeyAtStorageVersion < i.key())
				m.param2 = endKeyAtStorageVersion;
			else if (i->isClearTo())
				m.param2 = i->getEndKey();
			else
				m.param2 = i.key();
		}
	}
	else if (m.type != MutationRef::SetValue && (m.type)) {

		Optional<StringRef> oldVal;
		auto it = data.atLatest().lastLessOrEqual(m.param1);
		if (it != data.atLatest().end() && it->isValue() && it.key() == m.param1)
			oldVal = it->getValue();
		else if (it != data.atLatest().end() && it->isClearTo() && it->getEndKey() > m.param1) {
			TEST(true); // Atomic op right after a clear.
		}
		else {
			Optional<Value>& oldThing = eager->getValue(m.param1);
			if (oldThing.present())
				oldVal = oldThing.get();
		}

		switch(m.type) {
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
				return expandMutation(m, data, eager, eagerTrustedEnd, ar);
			}
			return false;
		}
		m.type = MutationRef::SetValue;
	}

	return true;
}

void applyMutation( StorageServer *self, MutationRef const& m, Arena& arena, StorageServer::VersionedData &data ) {
	// m is expected to be in arena already
	// Clear split keys are added to arena
	StorageMetrics metrics;
	metrics.bytesPerKSecond = mvccStorageBytes( m ) / 2;
	metrics.iosPerKSecond = 1;
	self->metrics.notify(m.param1, metrics);

	if (m.type == MutationRef::SetValue) {
		auto prev = data.atLatest().lastLessOrEqual(m.param1);
		if (prev && prev->isClearTo() && prev->getEndKey() > m.param1) {
			ASSERT( prev.key() <= m.param1 );
			KeyRef end = prev->getEndKey();
			// the insert version of the previous clear is preserved for the "left half", because in changeDurableVersion() the previous clear is still responsible for removing it
			// insert() invalidates prev, so prev.key() is not safe to pass to it by reference
			data.insert( KeyRef(prev.key()), ValueOrClearToRef::clearTo( m.param1 ), prev.insertVersion() );  // overwritten by below insert if empty
			KeyRef nextKey = keyAfter(m.param1, arena);
			if ( end != nextKey ) {
				ASSERT( end > nextKey );
				// the insert version of the "right half" is not preserved, because in changeDurableVersion() this set is responsible for removing it
				// FIXME: This copy is technically an asymptotic problem, definitely a waste of memory (copy of keyAfter is a waste, but not asymptotic)
				data.insert( nextKey, ValueOrClearToRef::clearTo( KeyRef(arena, end) ) );
			}
		}
		data.insert( m.param1, ValueOrClearToRef::value(m.param2) );
		self->watches.trigger( m.param1 );
	} else if (m.type == MutationRef::ClearRange) {
		data.erase( m.param1, m.param2 );
		ASSERT( m.param2 > m.param1 );
		ASSERT( !data.isClearContaining( data.atLatest(), m.param1 ) );
		data.insert( m.param1, ValueOrClearToRef::clearTo(m.param2) );
		self->watches.triggerRange( m.param1, m.param2 );
	}

}

void removeDataRange( StorageServer *ss, Standalone<VersionUpdateRef> &mLV, KeyRangeMap<Reference<ShardInfo>>& shards, KeyRangeRef range ) {
	// modify the latest version of data to remove all sets and trim all clears to exclude range.
	// Add a clear to mLV (mutationLog[data.getLatestVersion()]) that ensures all keys in range are removed from the disk when this latest version becomes durable
	// mLV is also modified if necessary to ensure that split clears can be forgotten

	MutationRef clearRange( MutationRef::ClearRange, range.begin, range.end );
	clearRange = ss->addMutationToMutationLog( mLV, clearRange );

	auto& data = ss->mutableData();

	// Expand the range to the right to include other shards not in versionedData
	for( auto r = shards.rangeContaining(range.end); r != shards.ranges().end() && !r->value()->isInVersionedData(); ++r )
		range = KeyRangeRef(range.begin, r->end());

	auto endClear = data.atLatest().lastLess( range.end );
	if (endClear && endClear->isClearTo() && endClear->getEndKey() > range.end ) {
		// This clear has been bumped up to insertVersion==data.getLatestVersion and needs a corresponding mutation log entry to forget
		MutationRef m( MutationRef::ClearRange, range.end, endClear->getEndKey() );
		m = ss->addMutationToMutationLog( mLV, m );
		data.insert( m.param1, ValueOrClearToRef::clearTo( m.param2 ) );
	}

	auto beginClear = data.atLatest().lastLess( range.begin );
	if (beginClear && beginClear->isClearTo() && beginClear->getEndKey() > range.begin ) {
		// We don't need any special mutationLog entry - because the begin key and insert version are unchanged the original clear
		//   mutation works to forget this one - but we need range.begin in the right arena
		KeyRef rb(  mLV.arena(), range.begin );
		// insert() invalidates beginClear, so beginClear.key() is not safe to pass to it by reference
		data.insert( KeyRef(beginClear.key()), ValueOrClearToRef::clearTo( rb ), beginClear.insertVersion() );
	}

	data.erase( range.begin, range.end );
}

void setAvailableStatus( StorageServer* self, KeyRangeRef keys, bool available );
void setAssignedStatus( StorageServer* self, KeyRangeRef keys, bool nowAssigned );

void coalesceShards(StorageServer *data, KeyRangeRef keys) {
	auto shardRanges = data->shards.intersectingRanges(keys);
	auto fullRange = data->shards.ranges();

	auto iter = shardRanges.begin();
	if( iter != fullRange.begin() ) --iter;
	auto iterEnd = shardRanges.end();
	if( iterEnd != fullRange.end() ) ++iterEnd;

	bool lastReadable = false;
	bool lastNotAssigned = false;
	KeyRangeMap<Reference<ShardInfo>>::Iterator lastRange;

	for( ; iter != iterEnd; ++iter) {
		if( lastReadable && iter->value()->isReadable() ) {
			KeyRange range = KeyRangeRef( lastRange->begin(), iter->end() );
			data->addShard( ShardInfo::newReadWrite( range, data) );
			iter = data->shards.rangeContaining(range.begin);
		} else if( lastNotAssigned && iter->value()->notAssigned() ) {
			KeyRange range = KeyRangeRef( lastRange->begin(), iter->end() );
			data->addShard( ShardInfo::newNotAssigned( range) );
			iter = data->shards.rangeContaining(range.begin);
		}

		lastReadable = iter->value()->isReadable();
		lastNotAssigned = iter->value()->notAssigned();
		lastRange = iter;
	}
}

ACTOR Future<Standalone<RangeResultRef>> tryGetRange( Database cx, Version version, KeyRangeRef keys, GetRangeLimits limits, bool* isTooOld ) {
	state Transaction tr( cx );
	state Standalone<RangeResultRef> output;
	state KeySelectorRef begin = firstGreaterOrEqual( keys.begin );
	state KeySelectorRef end = firstGreaterOrEqual( keys.end );

	if( *isTooOld )
		throw transaction_too_old();

	ASSERT(!cx->switchable);
	tr.setVersion( version );
	tr.info.taskID = TaskPriority::FetchKeys;
	limits.minRows = 0;

	try {
		loop {
			Standalone<RangeResultRef> rep = wait( tr.getRange( begin, end, limits, true ) );
			limits.decrement( rep );

			if( limits.isReached() || !rep.more ) {
				if( output.size() ) {
					output.arena().dependsOn( rep.arena() );
					output.append( output.arena(), rep.begin(), rep.size() );
					if( limits.isReached() && rep.readThrough.present() )
						output.readThrough = rep.readThrough.get();
				} else {
					output = rep;
				}

				output.more = limits.isReached();

				return output;
			} else if( rep.readThrough.present() ) {
				output.arena().dependsOn( rep.arena() );
				if( rep.size() ) {
					output.append( output.arena(), rep.begin(), rep.size() );
					ASSERT( rep.readThrough.get() > rep.end()[-1].key );
				} else {
					ASSERT( rep.readThrough.get() > keys.begin );
				}
				begin = firstGreaterOrEqual( rep.readThrough.get() );
			} else {
				output.arena().dependsOn( rep.arena() );
				output.append( output.arena(), rep.begin(), rep.size() );
				begin = firstGreaterThan( output.end()[-1].key );
			}
		}
	} catch( Error &e ) {
		if( begin.getKey() != keys.begin && ( e.code() == error_code_transaction_too_old || e.code() == error_code_future_version || e.code() == error_code_process_behind ) ) {
			if( e.code() == error_code_transaction_too_old )
				*isTooOld = true;
			output.more = true;
			if( begin.isFirstGreaterOrEqual() )
				output.readThrough = begin.getKey();
			return output;
		}
		throw;
	}
}

template <class T>
void addMutation( T& target, Version version, MutationRef const& mutation ) {
	target.addMutation( version, mutation );
}

template <class T>
void addMutation( Reference<T>& target, Version version, MutationRef const& mutation ) {
	addMutation(*target, version, mutation);
}

template <class T>
void splitMutations(StorageServer* data, KeyRangeMap<T>& map, VerUpdateRef const& update) {
	for(int i = 0; i < update.mutations.size(); i++) {
		splitMutation(data, map, update.mutations[i], update.version);
	}
}

template <class T>
void splitMutation(StorageServer* data, KeyRangeMap<T>& map, MutationRef const& m, Version ver) {
	if(isSingleKeyMutation((MutationRef::Type) m.type)) {
		if ( !SHORT_CIRCUT_ACTUAL_STORAGE || !normalKeys.contains(m.param1) )
			addMutation( map.rangeContaining(m.param1)->value(), ver, m );
	}
	else if (m.type == MutationRef::ClearRange) {
		KeyRangeRef mKeys( m.param1, m.param2 );
		if ( !SHORT_CIRCUT_ACTUAL_STORAGE || !normalKeys.contains(mKeys) ){
			auto r = map.intersectingRanges( mKeys );
			for(auto i = r.begin(); i != r.end(); ++i) {
				KeyRangeRef k = mKeys & i->range();
				addMutation( i->value(), ver, MutationRef((MutationRef::Type)m.type, k.begin, k.end) );
			}
		}
	} else
		ASSERT(false);  // Unknown mutation type in splitMutations
}

ACTOR Future<Void> logFetchKeysWarning(AddingShard* shard) {
	state double startTime = now();
	loop {
		state double waitSeconds = BUGGIFY ? 5.0 : 600.0;
		wait(delay(waitSeconds));
		TraceEvent(waitSeconds > 300.0 ? SevWarnAlways : SevInfo, "FetchKeysTooLong").detail("Duration", now() - startTime).detail("Phase", shard->phase).detail("Begin", shard->keys.begin.printable()).detail("End", shard->keys.end.printable());
	}
}

ACTOR Future<Void> fetchKeys( StorageServer *data, AddingShard* shard ) {
	state TraceInterval interval("FetchKeys");
	state KeyRange keys = shard->keys;
	state Future<Void> warningLogger = logFetchKeysWarning(shard);
	state double startt = now();
	state int fetchBlockBytes = BUGGIFY ? SERVER_KNOBS->BUGGIFY_BLOCK_BYTES : SERVER_KNOBS->FETCH_BLOCK_BYTES;

	// delay(0) to force a return to the run loop before the work of fetchKeys is started.
	//  This allows adding->start() to be called inline with CSK.
	wait( data->coreStarted.getFuture() && delay( 0 ) );

	try {
		DEBUG_KEY_RANGE("fetchKeysBegin", data->version.get(), shard->keys);

		TraceEvent(SevDebug, interval.begin(), data->thisServerID)
			.detail("KeyBegin", shard->keys.begin)
			.detail("KeyEnd",shard->keys.end);

		validate(data);

		// Wait (if necessary) for the latest version at which any key in keys was previously available (+1) to be durable
		auto navr = data->newestAvailableVersion.intersectingRanges( keys );
		Version lastAvailable = invalidVersion;
		for(auto r=navr.begin(); r!=navr.end(); ++r) {
			ASSERT( r->value() != latestVersion );
			lastAvailable = std::max(lastAvailable, r->value());
		}
		auto ndvr = data->newestDirtyVersion.intersectingRanges( keys );
		for(auto r=ndvr.begin(); r!=ndvr.end(); ++r)
			lastAvailable = std::max(lastAvailable, r->value());

		if (lastAvailable != invalidVersion && lastAvailable >= data->durableVersion.get()) {
			TEST(true);  // FetchKeys waits for previous available version to be durable
			wait( data->durableVersion.whenAtLeast(lastAvailable+1) );
		}

		TraceEvent(SevDebug, "FetchKeysVersionSatisfied", data->thisServerID).detail("FKID", interval.pairID);

		wait( data->fetchKeysParallelismLock.take( TaskPriority::DefaultYield, fetchBlockBytes ) );
		state FlowLock::Releaser holdingFKPL( data->fetchKeysParallelismLock, fetchBlockBytes );

		state double executeStart = now();
		++data->counters.fetchWaitingCount;
		data->counters.fetchWaitingMS += 1000*(executeStart - startt);

		// Fetch keys gets called while the update actor is processing mutations. data->version will not be updated until all mutations for a version
		// have been processed. We need to take the durableVersionLock to ensure data->version is greater than the version of the mutation which caused
		// the fetch to be initiated.
		wait( data->durableVersionLock.take() );

		shard->phase = AddingShard::Fetching;
		state Version fetchVersion = data->version.get();

		data->durableVersionLock.release();

		wait(delay(0));

		TraceEvent(SevDebug, "FetchKeysUnblocked", data->thisServerID).detail("FKID", interval.pairID).detail("Version", fetchVersion);

		// Get the history
		state int debug_getRangeRetries = 0;
		state int debug_nextRetryToLog = 1;
		state bool isTooOld = false;

		//FIXME: The client cache does not notice when servers are added to a team. To read from a local storage server we must refresh the cache manually.
		data->cx->invalidateCache(keys);

		loop {
			try {
				TEST(true);		// Fetching keys for transferred shard

				state Standalone<RangeResultRef> this_block =
				    wait(tryGetRange(data->cx, fetchVersion, keys,
				                     GetRangeLimits(GetRangeLimits::ROW_LIMIT_UNLIMITED, fetchBlockBytes), &isTooOld));

				int expectedSize = (int)this_block.expectedSize() + (8-(int)sizeof(KeyValueRef))*this_block.size();

				TraceEvent(SevDebug, "FetchKeysBlock", data->thisServerID).detail("FKID", interval.pairID)
					.detail("BlockRows", this_block.size()).detail("BlockBytes", expectedSize)
					.detail("KeyBegin", keys.begin).detail("KeyEnd", keys.end)
					.detail("Last", this_block.size() ? this_block.end()[-1].key : std::string())
					.detail("Version", fetchVersion).detail("More", this_block.more);
				DEBUG_KEY_RANGE("fetchRange", fetchVersion, keys);
				for(auto k = this_block.begin(); k != this_block.end(); ++k) DEBUG_MUTATION("fetch", fetchVersion, MutationRef(MutationRef::SetValue, k->key, k->value));

				data->counters.bytesFetched += expectedSize;
				if( fetchBlockBytes > expectedSize ) {
					holdingFKPL.release( fetchBlockBytes - expectedSize );
				}

				// Wait for permission to proceed
				//wait( data->fetchKeysStorageWriteLock.take() );
				//state FlowLock::Releaser holdingFKSWL( data->fetchKeysStorageWriteLock );

				// Write this_block to storage
				state KeyValueRef *kvItr = this_block.begin();
				for(; kvItr != this_block.end(); ++kvItr) {
					data->storage.writeKeyValue( *kvItr );
					wait(yield());
				}

				kvItr = this_block.begin();
				for(; kvItr != this_block.end(); ++kvItr) {
					data->byteSampleApplySet( *kvItr, invalidVersion );
					wait(yield());
				}

				if (this_block.more) {
					Key nfk = this_block.readThrough.present() ? this_block.readThrough.get() : keyAfter( this_block.end()[-1].key );
					if (nfk != keys.end) {
						std::deque< Standalone<VerUpdateRef> > updatesToSplit = std::move( shard->updates );

						// This actor finishes committing the keys [keys.begin,nfk) that we already fetched.
						// The remaining unfetched keys [nfk,keys.end) will become a separate AddingShard with its own fetchKeys.
						shard->server->addShard( ShardInfo::addingSplitLeft( KeyRangeRef(keys.begin, nfk), shard ) );
						shard->server->addShard( ShardInfo::newAdding( data, KeyRangeRef(nfk, keys.end) ) );
						shard = data->shards.rangeContaining( keys.begin ).value()->adding;
						warningLogger = logFetchKeysWarning(shard);
						AddingShard* otherShard = data->shards.rangeContaining( nfk ).value()->adding;
						keys = shard->keys;

						// Split our prior updates.  The ones that apply to our new, restricted key range will go back into shard->updates,
						// and the ones delivered to the new shard will be discarded because it is in WaitPrevious phase (hasn't chosen a fetchVersion yet).
						// What we are doing here is expensive and could get more expensive if we started having many more blocks per shard. May need optimization in the future.
						std::deque< Standalone<VerUpdateRef> >::iterator u = updatesToSplit.begin();
						for(; u != updatesToSplit.end(); ++u) {
							splitMutations(data, data->shards, *u);
						}

						TEST( true );
						TEST( shard->updates.size() );
						ASSERT( otherShard->updates.empty() );
					}
				}

				this_block = Standalone<RangeResultRef>();

				if (BUGGIFY) wait( delay( 1 ) );

				break;
			} catch (Error& e) {
				TraceEvent("FKBlockFail", data->thisServerID).error(e,true).suppressFor(1.0).detail("FKID", interval.pairID);
				if (e.code() == error_code_transaction_too_old){
					TEST(true); // A storage server has forgotten the history data we are fetching
					Version lastFV = fetchVersion;
					fetchVersion = data->version.get();
					isTooOld = false;

					// Throw away deferred updates from before fetchVersion, since we don't need them to use blocks fetched at that version
					while (!shard->updates.empty() && shard->updates[0].version <= fetchVersion) shard->updates.pop_front();

					//FIXME: remove when we no longer support upgrades from 5.X
					if(debug_getRangeRetries >= 100) {
						data->cx->enableLocalityLoadBalance = false;
					}

					debug_getRangeRetries++;
					if (debug_nextRetryToLog==debug_getRangeRetries){
						debug_nextRetryToLog += std::min(debug_nextRetryToLog, 1024);
						TraceEvent(SevWarn, "FetchPast", data->thisServerID).detail("TotalAttempts", debug_getRangeRetries).detail("FKID", interval.pairID).detail("V", lastFV).detail("N", fetchVersion).detail("E", data->version.get());
					}
				} else if (e.code() == error_code_future_version || e.code() == error_code_process_behind) {
					TEST(true); // fetchKeys got future_version or process_behind, so there must be a huge storage lag somewhere.  Keep trying.
				} else {
					throw;
				}
				wait( delayJittered( FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY ) );
			}
		}

		//FIXME: remove when we no longer support upgrades from 5.X
		data->cx->enableLocalityLoadBalance = true;

		// We have completed the fetch and write of the data, now we wait for MVCC window to pass.
		//  As we have finished this work, we will allow more work to start...
		shard->fetchComplete.send(Void());

		TraceEvent(SevDebug, "FKBeforeFinalCommit", data->thisServerID).detail("FKID", interval.pairID).detail("SV", data->storageVersion()).detail("DV", data->durableVersion.get());
		// Directly commit()ing the IKVS would interfere with updateStorage, possibly resulting in an incomplete version being recovered.
		// Instead we wait for the updateStorage loop to commit something (and consequently also what we have written)

		wait( data->durableVersion.whenAtLeast( data->storageVersion()+1 ) );
		holdingFKPL.release();

		TraceEvent(SevDebug, "FKAfterFinalCommit", data->thisServerID).detail("FKID", interval.pairID).detail("SV", data->storageVersion()).detail("DV", data->durableVersion.get());

		// Wait to run during update(), after a new batch of versions is received from the tlog but before eager reads take place.
		Promise<FetchInjectionInfo*> p;
		data->readyFetchKeys.push_back( p );

		FetchInjectionInfo* batch = wait( p.getFuture() );
		TraceEvent(SevDebug, "FKUpdateBatch", data->thisServerID).detail("FKID", interval.pairID);

		shard->phase = AddingShard::Waiting;

		// Choose a transferredVersion.  This choice and timing ensure that
		//   * The transferredVersion can be mutated in versionedData
		//   * The transferredVersion isn't yet committed to storage (so we can write the availability status change)
		//   * The transferredVersion is <= the version of any of the updates in batch, and if there is an equal version
		//     its mutations haven't been processed yet
		shard->transferredVersion = data->version.get() + 1;
		//shard->transferredVersion = batch->changes[0].version;  //< FIXME: This obeys the documented properties, and seems "safer" because it never introduces extra versions into the data structure, but violates some ASSERTs currently
		data->mutableData().createNewVersion( shard->transferredVersion );
		ASSERT( shard->transferredVersion > data->storageVersion() );
		ASSERT( shard->transferredVersion == data->data().getLatestVersion() );

		TraceEvent(SevDebug, "FetchKeysHaveData", data->thisServerID).detail("FKID", interval.pairID)
			.detail("Version", shard->transferredVersion).detail("StorageVersion", data->storageVersion());
		validate(data);

		// Put the updates that were collected during the FinalCommit phase into the batch at the transferredVersion.  Eager reads will be done
		// for them by update(), and the mutations will come back through AddingShard::addMutations and be applied to versionedMap and mutationLog as normal.
		// The lie about their version is acceptable because this shard will never be read at versions < transferredVersion
		for(auto i=shard->updates.begin(); i!=shard->updates.end(); ++i) {
			i->version = shard->transferredVersion;
			batch->arena.dependsOn(i->arena());
		}

		int startSize = batch->changes.size();
		TEST(startSize); //Adding fetch data to a batch which already has changes
		batch->changes.resize( batch->changes.size()+shard->updates.size() );

		//FIXME: pass the deque back rather than copy the data
		std::copy( shard->updates.begin(), shard->updates.end(), batch->changes.begin()+startSize );
		Version checkv = shard->transferredVersion;

		for(auto b = batch->changes.begin()+startSize; b != batch->changes.end(); ++b ) {
			ASSERT( b->version >= checkv );
			checkv = b->version;
			for(auto& m : b->mutations)
				DEBUG_MUTATION("fetchKeysFinalCommitInject", batch->changes[0].version, m);
		}

		shard->updates.clear();

		setAvailableStatus(data, keys, true); // keys will be available when getLatestVersion()==transferredVersion is durable

		// Wait for the transferredVersion (and therefore the shard data) to be committed and durable.
		wait( data->durableVersion.whenAtLeast( shard->transferredVersion ) );

		ASSERT( data->shards[shard->keys.begin]->assigned() && data->shards[shard->keys.begin]->keys == shard->keys );  // We aren't changing whether the shard is assigned
		data->newestAvailableVersion.insert(shard->keys, latestVersion);
		shard->readWrite.send(Void());
		data->addShard( ShardInfo::newReadWrite(shard->keys, data) );   // invalidates shard!
		coalesceShards(data, keys);

		validate(data);

		++data->counters.fetchExecutingCount;
		data->counters.fetchExecutingMS += 1000*(now() - executeStart);

		TraceEvent(SevDebug, interval.end(), data->thisServerID);
	} catch (Error &e){
		TraceEvent(SevDebug, interval.end(), data->thisServerID).error(e, true).detail("Version", data->version.get());

		if (e.code() == error_code_actor_cancelled && !data->shuttingDown && shard->phase >= AddingShard::Fetching) {
			if (shard->phase < AddingShard::Waiting) {
				data->storage.clearRange( keys );
				data->byteSampleApplyClear( keys, invalidVersion );
			} else {
				ASSERT( data->data().getLatestVersion() > data->version.get() );
				removeDataRange( data, data->addVersionToMutationLog(data->data().getLatestVersion()), data->shards, keys );
				setAvailableStatus(data, keys, false);
				// Prevent another, overlapping fetchKeys from entering the Fetching phase until data->data().getLatestVersion() is durable
				data->newestDirtyVersion.insert( keys, data->data().getLatestVersion() );
			}
		}

		TraceEvent(SevError, "FetchKeysError", data->thisServerID)
			.error(e)
			.detail("Elapsed", now()-startt)
			.detail("KeyBegin", keys.begin)
			.detail("KeyEnd",keys.end);
		if (e.code() != error_code_actor_cancelled)
			data->otherError.sendError(e);  // Kill the storage server.  Are there any recoverable errors?
		throw; // goes nowhere
	}

	return Void();
};

AddingShard::AddingShard( StorageServer* server, KeyRangeRef const& keys )
	: server(server), keys(keys), transferredVersion(invalidVersion), phase(WaitPrevious)
{
	fetchClient = fetchKeys(server, this);
}

void AddingShard::addMutation( Version version, MutationRef const& mutation ){
	if (mutation.type == mutation.ClearRange) {
		ASSERT( keys.begin<=mutation.param1 && mutation.param2<=keys.end );
	}
	else if (isSingleKeyMutation((MutationRef::Type) mutation.type)) {
		ASSERT( keys.contains(mutation.param1) );
	}

	if (phase == WaitPrevious) {
		// Updates can be discarded
	} else if (phase == Fetching) {
		if (!updates.size() || version > updates.end()[-1].version) {
			VerUpdateRef v;
			v.version = version;
			v.isPrivateData = false;
			updates.push_back(v);
		} else {
			ASSERT( version == updates.end()[-1].version );
		}
		updates.back().mutations.push_back_deep( updates.back().arena(), mutation );
	} else if (phase == Waiting) {
		server->addMutation(version, mutation, keys, server->updateEagerReads);
	} else ASSERT(false);
}

void ShardInfo::addMutation(Version version, MutationRef const& mutation) {
	ASSERT( (void *)this);
	ASSERT( keys.contains( mutation.param1 ) );
	if (adding)
		adding->addMutation(version, mutation);
	else if (readWrite)
		readWrite->addMutation(version, mutation, this->keys, readWrite->updateEagerReads);
	else if (mutation.type != MutationRef::ClearRange) {
		TraceEvent(SevError, "DeliveredToNotAssigned").detail("Version", version).detail("Mutation", mutation.toString());
		ASSERT(false);  // Mutation delivered to notAssigned shard!
	}
}

enum ChangeServerKeysContext { CSK_UPDATE, CSK_RESTORE };
const char* changeServerKeysContextName[] = { "Update", "Restore" };

void changeServerKeys( StorageServer* data, const KeyRangeRef& keys, bool nowAssigned, Version version, ChangeServerKeysContext context ) {
	ASSERT( !keys.empty() );

	//TraceEvent("ChangeServerKeys", data->thisServerID)
	//	.detail("KeyBegin", keys.begin)
	//	.detail("KeyEnd", keys.end)
	//	.detail("NowAssigned", nowAssigned)
	//	.detail("Version", version)
	//	.detail("Context", changeServerKeysContextName[(int)context]);
	validate(data);

	// TODO(alexmiller): Figure out how to selectively enable spammy data distribution events.
	//DEBUG_KEY_RANGE( nowAssigned ? "KeysAssigned" : "KeysUnassigned", version, keys );

	bool isDifferent = false;
	auto existingShards = data->shards.intersectingRanges(keys);
	for( auto it = existingShards.begin(); it != existingShards.end(); ++it ) {
		if( nowAssigned != it->value()->assigned() ) {
			isDifferent = true;
			/*TraceEvent("CSKRangeDifferent", data->thisServerID)
			  .detail("KeyBegin", it->range().begin)
			  .detail("KeyEnd", it->range().end);*/
			break;
		}
	}
	if( !isDifferent ) {
		//TraceEvent("CSKShortCircuit", data->thisServerID)
		//	.detail("KeyBegin", keys.begin)
		//	.detail("KeyEnd", keys.end);
		return;
	}

	// Save a backup of the ShardInfo references before we start messing with shards, in order to defer fetchKeys cancellation (and
	// its potential call to removeDataRange()) until shards is again valid
	vector< Reference<ShardInfo> > oldShards;
	auto os = data->shards.intersectingRanges(keys);
	for(auto r = os.begin(); r != os.end(); ++r)
		oldShards.push_back( r->value() );

	// As addShard (called below)'s documentation requires, reinitialize any overlapping range(s)
	auto ranges = data->shards.getAffectedRangesAfterInsertion( keys, Reference<ShardInfo>() );  // null reference indicates the range being changed
	for(int i=0; i<ranges.size(); i++) {
		if (!ranges[i].value) {
			ASSERT( (KeyRangeRef&)ranges[i] == keys ); // there shouldn't be any nulls except for the range being inserted
		} else if (ranges[i].value->notAssigned())
			data->addShard( ShardInfo::newNotAssigned(ranges[i]) );
		else if (ranges[i].value->isReadable())
			data->addShard( ShardInfo::newReadWrite(ranges[i], data) );
		else {
			ASSERT( ranges[i].value->adding );
			data->addShard( ShardInfo::newAdding( data, ranges[i] ) );
			TEST( true );	// ChangeServerKeys reFetchKeys
		}
	}

	// Shard state depends on nowAssigned and whether the data is available (actually assigned in memory or on the disk) up to the given
	// version.  The latter depends on data->newestAvailableVersion, so loop over the ranges of that.
	// SOMEDAY: Could this just use shards?  Then we could explicitly do the removeDataRange here when an adding/transferred shard is cancelled
	auto vr = data->newestAvailableVersion.intersectingRanges(keys);
	std::vector<std::pair<KeyRange,Version>> changeNewestAvailable;
	std::vector<KeyRange> removeRanges;
	for (auto r = vr.begin(); r != vr.end(); ++r) {
		KeyRangeRef range = keys & r->range();
		bool dataAvailable = r->value()==latestVersion || r->value() >= version;
		/*TraceEvent("CSKRange", data->thisServerID)
			.detail("KeyBegin", range.begin)
			.detail("KeyEnd", range.end)
			.detail("Available", dataAvailable)
			.detail("NowAssigned", nowAssigned)
			.detail("NewestAvailable", r->value())
			.detail("ShardState0", data->shards[range.begin]->debugDescribeState());*/
		if (!nowAssigned) {
			if (dataAvailable) {
				ASSERT( r->value() == latestVersion);  // Not that we care, but this used to be checked instead of dataAvailable
				ASSERT( data->mutableData().getLatestVersion() > version || context == CSK_RESTORE );
				changeNewestAvailable.emplace_back(range, version);
				removeRanges.push_back( range );
			}
			data->addShard( ShardInfo::newNotAssigned(range) );
			data->watches.triggerRange( range.begin, range.end );
		} else if (!dataAvailable) {
			// SOMEDAY: Avoid restarting adding/transferred shards
			if (version==0){ // bypass fetchkeys; shard is known empty at version 0
				changeNewestAvailable.emplace_back(range, latestVersion);
				data->addShard( ShardInfo::newReadWrite(range, data) );
				setAvailableStatus(data, range, true);
			} else {
				auto& shard = data->shards[range.begin];
				if( !shard->assigned() || shard->keys != range )
					data->addShard( ShardInfo::newAdding(data, range) );
			}
		} else {
			changeNewestAvailable.emplace_back(range, latestVersion);
			data->addShard( ShardInfo::newReadWrite(range, data) );
		}
	}
	// Update newestAvailableVersion when a shard becomes (un)available (in a separate loop to avoid invalidating vr above)
	for(auto r = changeNewestAvailable.begin(); r != changeNewestAvailable.end(); ++r)
		data->newestAvailableVersion.insert( r->first, r->second );

	if (!nowAssigned)
		data->metrics.notifyNotReadable( keys );

	coalesceShards( data, KeyRangeRef(ranges[0].begin, ranges[ranges.size()-1].end) );

	// Now it is OK to do removeDataRanges, directly and through fetchKeys cancellation (and we have to do so before validate())
	oldShards.clear();
	ranges.clear();
	for(auto r=removeRanges.begin(); r!=removeRanges.end(); ++r) {
		removeDataRange( data, data->addVersionToMutationLog(data->data().getLatestVersion()), data->shards, *r );
		setAvailableStatus(data, *r, false);
	}
	validate(data);
}

void rollback( StorageServer* data, Version rollbackVersion, Version nextVersion ) {
	TEST(true); // call to shard rollback
	DEBUG_KEY_RANGE("Rollback", rollbackVersion, allKeys);

	// We used to do a complicated dance to roll back in MVCC history.  It's much simpler, and more testable,
	// to simply restart the storage server actor and restore from the persistent disk state, and then roll
	// forward from the TLog's history.  It's not quite as efficient, but we rarely have to do this in practice.

	// FIXME: This code is relying for liveness on an undocumented property of the log system implementation: that after a rollback the rolled back versions will
	// eventually be missing from the peeked log.  A more sophisticated approach would be to make the rollback range durable and, after reboot, skip over
	// those versions if they appear in peek results.

	throw please_reboot();
}

void StorageServer::addMutation(Version version, MutationRef const& mutation, KeyRangeRef const& shard, UpdateEagerReadInfo* eagerReads ) {
	MutationRef expanded = mutation;
	auto& mLog = addVersionToMutationLog(version);

	if ( !expandMutation( expanded, data(), eagerReads, shard.end, mLog.arena()) ) {
		return;
	}
	expanded = addMutationToMutationLog(mLog, expanded);
	DEBUG_MUTATION("applyMutation", version, expanded).detail("UID", thisServerID).detail("ShardBegin", shard.begin).detail("ShardEnd", shard.end);
	applyMutation( this, expanded, mLog.arena(), mutableData() );
	//printf("\nSSUpdate: Printing versioned tree after applying mutation\n");
	//mutableData().printTree(version);
}

struct OrderByVersion {
	bool operator()( const VersionUpdateRef& a, const VersionUpdateRef& b ) {
		if (a.version != b.version) return a.version < b.version;
		if (a.isPrivateData != b.isPrivateData) return a.isPrivateData;
		return false;
	}
};

#define PERSIST_PREFIX "\xff\xff"

// Immutable
static const KeyValueRef persistFormat( LiteralStringRef( PERSIST_PREFIX "Format" ), LiteralStringRef("FoundationDB/StorageServer/1/4") );
static const KeyRangeRef persistFormatReadableRange( LiteralStringRef("FoundationDB/StorageServer/1/2"), LiteralStringRef("FoundationDB/StorageServer/1/5") );
static const KeyRef persistID = LiteralStringRef( PERSIST_PREFIX "ID" );

// (Potentially) change with the durable version or when fetchKeys completes
static const KeyRef persistVersion = LiteralStringRef( PERSIST_PREFIX "Version" );
static const KeyRangeRef persistShardAssignedKeys = KeyRangeRef( LiteralStringRef( PERSIST_PREFIX "ShardAssigned/" ), LiteralStringRef( PERSIST_PREFIX "ShardAssigned0" ) );
static const KeyRangeRef persistShardAvailableKeys = KeyRangeRef( LiteralStringRef( PERSIST_PREFIX "ShardAvailable/" ), LiteralStringRef( PERSIST_PREFIX "ShardAvailable0" ) );
static const KeyRangeRef persistByteSampleKeys = KeyRangeRef( LiteralStringRef( PERSIST_PREFIX "BS/" ), LiteralStringRef( PERSIST_PREFIX "BS0" ) );
static const KeyRangeRef persistByteSampleSampleKeys = KeyRangeRef( LiteralStringRef( PERSIST_PREFIX "BS/" PERSIST_PREFIX "BS/" ), LiteralStringRef( PERSIST_PREFIX "BS/" PERSIST_PREFIX "BS0" ) );
static const KeyRef persistLogProtocol = LiteralStringRef(PERSIST_PREFIX "LogProtocol");
static const KeyRef persistPrimaryLocality = LiteralStringRef( PERSIST_PREFIX "PrimaryLocality" );
// data keys are unmangled (but never start with PERSIST_PREFIX because they are always in allKeys)

class StorageUpdater {
public:
	StorageUpdater() : fromVersion(invalidVersion), currentVersion(invalidVersion), restoredVersion(invalidVersion), processedStartKey(false), processedCacheStartKey(false) {}
	StorageUpdater(Version fromVersion, Version restoredVersion) : fromVersion(fromVersion), currentVersion(fromVersion), restoredVersion(restoredVersion), processedStartKey(false), processedCacheStartKey(false) {}

	void applyMutation(StorageServer* data, MutationRef const& m, Version ver) {
		//TraceEvent("SSNewVersion", data->thisServerID).detail("VerWas", data->mutableData().latestVersion).detail("ChVer", ver);

		if(currentVersion != ver) {
			fromVersion = currentVersion;
			currentVersion = ver;
			data->mutableData().createNewVersion(ver);
		}

		if (m.param1.startsWith( systemKeys.end )) {
			if ((m.type == MutationRef::SetValue) && m.param1.substr(1).startsWith(storageCachePrefix))
				applyPrivateCacheData( data, m);
			else {
				applyPrivateData( data, m );
			}
		} else {
			// FIXME: enable when DEBUG_MUTATION is active
			//for(auto m = changes[c].mutations.begin(); m; ++m) {
			//	DEBUG_MUTATION("SSUpdateMutation", changes[c].version, *m);
			//}

			splitMutation(data, data->shards, m, ver);
		}

		if (data->otherError.getFuture().isReady()) data->otherError.getFuture().get();
	}

	Version currentVersion;
private:
	Version fromVersion;
	Version restoredVersion;

	KeyRef startKey;
	bool nowAssigned;
	bool processedStartKey;

	KeyRef cacheStartKey;
	bool processedCacheStartKey;

	void applyPrivateData( StorageServer* data, MutationRef const& m ) {
		TraceEvent(SevDebug, "SSPrivateMutation", data->thisServerID).detail("Mutation", m.toString());

		if (processedStartKey) {
			// Because of the implementation of the krm* functions, we expect changes in pairs, [begin,end)
			// We can also ignore clearRanges, because they are always accompanied by such a pair of sets with the same keys
			ASSERT (m.type == MutationRef::SetValue && m.param1.startsWith(data->sk));
			KeyRangeRef keys( startKey.removePrefix( data->sk ), m.param1.removePrefix( data->sk ));

			// add changes in shard assignment to the mutation log
			setAssignedStatus( data, keys, nowAssigned );

			// The changes for version have already been received (and are being processed now).  We need
			// to fetch the data for change.version-1 (changes from versions < change.version)
			changeServerKeys( data, keys, nowAssigned, currentVersion-1, CSK_UPDATE );
			processedStartKey = false;
		} else if (m.type == MutationRef::SetValue && m.param1.startsWith( data->sk )) {
			// Because of the implementation of the krm* functions, we expect changes in pairs, [begin,end)
			// We can also ignore clearRanges, because they are always accompanied by such a pair of sets with the same keys
			startKey = m.param1;
			nowAssigned = m.param2 != serverKeysFalse;
			processedStartKey = true;
		} else if (m.type == MutationRef::SetValue && m.param1 == lastEpochEndPrivateKey) {
			// lastEpochEnd transactions are guaranteed by the master to be alone in their own batch (version)
			// That means we don't have to worry about the impact on changeServerKeys
			//ASSERT( /*isFirstVersionUpdateFromTLog && */!std::next(it) );

			Version rollbackVersion;
			BinaryReader br(m.param2, Unversioned());
			br >> rollbackVersion;

			if ( rollbackVersion < fromVersion && rollbackVersion > restoredVersion ) {
				TEST( true );  // ShardApplyPrivateData shard rollback
				TraceEvent(SevWarn, "Rollback", data->thisServerID)
					.detail("FromVersion", fromVersion)
					.detail("ToVersion", rollbackVersion)
					.detail("AtVersion", currentVersion)
					.detail("StorageVersion", data->storageVersion());
				ASSERT( rollbackVersion >= data->storageVersion() );
				rollback( data, rollbackVersion, currentVersion );
			}

			data->recoveryVersionSkips.emplace_back(rollbackVersion, currentVersion - rollbackVersion);
		} else if (m.type == MutationRef::SetValue && m.param1 == killStoragePrivateKey) {
			throw worker_removed();
		} else if ((m.type == MutationRef::SetValue || m.type == MutationRef::ClearRange) && m.param1.substr(1).startsWith(serverTagPrefix)) {
			bool matchesThisServer = decodeServerTagKey(m.param1.substr(1)) == data->thisServerID;
			if( (m.type == MutationRef::SetValue && !matchesThisServer) || (m.type == MutationRef::ClearRange && matchesThisServer) )
				throw worker_removed();
		} else if (m.type == MutationRef::SetValue && m.param1 == rebootWhenDurablePrivateKey) {
			data->rebootAfterDurableVersion = currentVersion;
			TraceEvent("RebootWhenDurableSet", data->thisServerID).detail("DurableVersion", data->durableVersion.get()).detail("RebootAfterDurableVersion", data->rebootAfterDurableVersion);
		} else if (m.type == MutationRef::SetValue && m.param1 == primaryLocalityPrivateKey) {
			data->primaryLocality = BinaryReader::fromStringRef<int8_t>(m.param2, Unversioned());
			auto& mLV = data->addVersionToMutationLog( data->data().getLatestVersion() );
			data->addMutationToMutationLog( mLV, MutationRef(MutationRef::SetValue, persistPrimaryLocality, m.param2) );
		} else if (m.type == MutationRef::SetValue && m.param1 == readTxnLifetimeKey) {
			TraceEvent("SetReadTransactionLifetime", data->thisServerID)
			    .detail("OldReadTxnLifetime", data->readTxnLifetime)
			    .detail("NewReadTxnLifetime", BinaryReader::fromStringRef<int64_t>(m.param2, Unversioned()));
			data->readTxnLifetime = BinaryReader::fromStringRef<int64_t>(m.param2, Unversioned());
		} else {
			ASSERT(false);  // Unknown private mutation
		}
	}

	void applyPrivateCacheData( StorageServer* data, MutationRef const& m ) {
		//TraceEvent(SevDebug, "SSPrivateCacheMutation", data->thisServerID).detail("Mutation", m.toString());

		if (processedCacheStartKey) {
			// Because of the implementation of the krm* functions, we expect changes in pairs, [begin,end)
			ASSERT((m.type == MutationRef::SetValue) && m.param1.substr(1).startsWith(storageCachePrefix));
			KeyRangeRef keys( cacheStartKey.removePrefix(systemKeys.begin).removePrefix( storageCachePrefix ),
							  m.param1.removePrefix(systemKeys.begin).removePrefix( storageCachePrefix ));
			data->cachedRangeMap.insert(keys, true);

			//Figure out the affected shard ranges and maintain the cached key-range information in the in-memory map
			// TODO revisit- we are not splitting the cached ranges based on shards as of now.
			if (0) {
				auto cachedRanges = data->shards.intersectingRanges(keys);
				for(auto shard = cachedRanges.begin(); shard != cachedRanges.end(); ++shard) {
					KeyRangeRef intersectingRange = shard.range() & keys;
					TraceEvent(SevDebug, "SSPrivateCacheMutationInsertUnexpected", data->thisServerID).detail("Begin", intersectingRange.begin).detail("End", intersectingRange.end);
					data->cachedRangeMap.insert(intersectingRange, true);
				}
			}
			processedStartKey = false;
		} else if ((m.type == MutationRef::SetValue) && m.param1.substr(1).startsWith(storageCachePrefix)) {
			// Because of the implementation of the krm* functions, we expect changes in pairs, [begin,end)
			cacheStartKey = m.param1;
			processedCacheStartKey = true;
		} else {
			ASSERT(false);  // Unknown private mutation
		}
	}
};

ACTOR Future<Void> update( StorageServer* data, bool* pReceivedUpdate )
{
	state double start;
	try {
		// If we are disk bound and durableVersion is very old, we need to block updates or we could run out of memory
		// This is often referred to as the storage server e-brake (emergency brake)
		state double waitStartT = 0;
		while ( data->queueSize() >= SERVER_KNOBS->STORAGE_HARD_LIMIT_BYTES && data->durableVersion.get() < data->desiredOldestVersion.get() ) {
			if (now() - waitStartT >= 1) {
				TraceEvent(SevWarn, "StorageServerUpdateLag", data->thisServerID)
					.detail("Version", data->version.get())
					.detail("DurableVersion", data->durableVersion.get());
				waitStartT = now();
			}

			data->behind = true;
			wait( delayJittered(.005, TaskPriority::TLogPeekReply) );
		}

		while( data->byteSampleClearsTooLarge.get() ) {
			wait( data->byteSampleClearsTooLarge.onChange() );
		}

		state Reference<ILogSystem::IPeekCursor> cursor = data->logCursor;

		loop {
			wait( cursor->getMore() );
			if(!cursor->isExhausted()) {
				break;
			}
		}
		if(cursor->popped() > 0)
			throw worker_removed();

		++data->counters.updateBatches;
		data->lastTLogVersion = cursor->getMaxKnownVersion();
		data->versionLag = std::max<int64_t>(0, data->lastTLogVersion - data->version.get());

		ASSERT(*pReceivedUpdate == false);
		*pReceivedUpdate = true;

		start = now();
		wait( data->durableVersionLock.take(TaskPriority::TLogPeekReply,1) );
		state FlowLock::Releaser holdingDVL( data->durableVersionLock );
		if(now() - start > 0.1)
			TraceEvent("SSSlowTakeLock1", data->thisServerID).detailf("From", "%016llx", debug_lastLoadBalanceResultEndpointToken).detail("Duration", now() - start).detail("Version", data->version.get());

		start = now();
		state UpdateEagerReadInfo eager;
		state FetchInjectionInfo fii;
		state Reference<ILogSystem::IPeekCursor> cloneCursor2;

		loop{
			state uint64_t changeCounter = data->shardChangeCounter;
			bool epochEnd = false;
			bool hasPrivateData = false;
			bool firstMutation = true;
			bool dbgLastMessageWasProtocol = false;

			Reference<ILogSystem::IPeekCursor> cloneCursor1 = cursor->cloneNoMore();
			cloneCursor2 = cursor->cloneNoMore();

			cloneCursor1->setProtocolVersion(data->logProtocol);

			for (; cloneCursor1->hasMessage(); cloneCursor1->nextMessage()) {
				ArenaReader& cloneReader = *cloneCursor1->reader();

				if (LogProtocolMessage::isNextIn(cloneReader)) {
					LogProtocolMessage lpm;
					cloneReader >> lpm;
					//TraceEvent(SevDebug, "SSReadingLPM", data->thisServerID).detail("Mutation", lpm.toString());
					dbgLastMessageWasProtocol = true;
					cloneCursor1->setProtocolVersion(cloneReader.protocolVersion());
				}
				else {
					MutationRef msg;
					cloneReader >> msg;
					//TraceEvent(SevDebug, "SSReadingLog", data->thisServerID).detail("Mutation", msg.toString());

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

			// Any fetchKeys which are ready to transition their shards to the adding,transferred state do so now.
			// If there is an epoch end we skip this step, to increase testability and to prevent inserting a version in the middle of a rolled back version range.
			while(!hasPrivateData && !epochEnd && !data->readyFetchKeys.empty()) {
				auto fk = data->readyFetchKeys.back();
				data->readyFetchKeys.pop_back();
				fk.send( &fii );
			}

			for(auto& c : fii.changes)
				eager.addMutations(c.mutations);

			wait( doEagerReads( data, &eager ) );
			if (data->shardChangeCounter == changeCounter) break;
			TEST(true); // A fetchKeys completed while we were doing this, so eager might be outdated.  Read it again.
			// SOMEDAY: Theoretically we could check the change counters of individual shards and retry the reads only selectively
			eager = UpdateEagerReadInfo();
		}

		if(now() - start > 0.1)
			TraceEvent("SSSlowTakeLock2", data->thisServerID).detailf("From", "%016llx", debug_lastLoadBalanceResultEndpointToken).detail("Duration", now() - start).detail("Version", data->version.get());

		data->updateEagerReads = &eager;
		data->debug_inApplyUpdate = true;

		state StorageUpdater updater(data->lastVersionWithData, data->restoredVersion);

		if (EXPENSIVE_VALIDATION) data->data().atLatest().validate();
		validate(data);

		state bool injectedChanges = false;
		state int changeNum = 0;
		state int mutationBytes = 0;
		for(; changeNum < fii.changes.size(); changeNum++) {
			state int mutationNum = 0;
			state VerUpdateRef* pUpdate = &fii.changes[changeNum];
			for(; mutationNum < pUpdate->mutations.size(); mutationNum++) {
				updater.applyMutation(data, pUpdate->mutations[mutationNum], pUpdate->version);
				mutationBytes += pUpdate->mutations[mutationNum].totalSize();
				injectedChanges = true;
				if(mutationBytes > SERVER_KNOBS->DESIRED_UPDATE_BYTES) {
					mutationBytes = 0;
					wait(delay(SERVER_KNOBS->UPDATE_DELAY));
				}
			}
		}

		state Version ver = invalidVersion;
		cloneCursor2->setProtocolVersion(data->logProtocol);
		for (;cloneCursor2->hasMessage(); cloneCursor2->nextMessage()) {
			if(mutationBytes > SERVER_KNOBS->DESIRED_UPDATE_BYTES) {
				mutationBytes = 0;
				//Instead of just yielding, leave time for the storage server to respond to reads
				wait(delay(SERVER_KNOBS->UPDATE_DELAY));
			}

			if (cloneCursor2->version().version > ver) {
				ASSERT(cloneCursor2->version().version > data->version.get());
			}

			auto &rd = *cloneCursor2->reader();

			if (cloneCursor2->version().version > ver && cloneCursor2->version().version > data->version.get()) {
				++data->counters.updateVersions;
				ver = cloneCursor2->version().version;
			}

			if (LogProtocolMessage::isNextIn(rd)) {
				LogProtocolMessage lpm;
				rd >> lpm;

				data->logProtocol = rd.protocolVersion();
				data->storage.changeLogProtocol(ver, data->logProtocol);
				cloneCursor2->setProtocolVersion(rd.protocolVersion());
			}
			else {
				MutationRef msg;
				rd >> msg;

				if (ver != invalidVersion) {  // This change belongs to a version < minVersion
					DEBUG_MUTATION("SSPeek", ver, msg).detail("ServerID", data->thisServerID);
					if (ver == 1) {
						TraceEvent("SSPeekMutation", data->thisServerID);
						// The following trace event may produce a value with special characters
						//TraceEvent("SSPeekMutation", data->thisServerID).detail("Mutation", msg.toString()).detail("Version", cloneCursor2->version().toString());
					}

					updater.applyMutation(data, msg, ver);
					mutationBytes += msg.totalSize();
					data->counters.mutationBytes += msg.totalSize();
					++data->counters.mutations;
					switch(msg.type) {
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
				}
				else
					TraceEvent(SevError, "DiscardingPeekedData", data->thisServerID).detail("Mutation", msg.toString()).detail("Version", cloneCursor2->version().toString());
			}
		}

		if(ver != invalidVersion) {
			data->lastVersionWithData = ver;
		} 
		ver = cloneCursor2->version().version - 1;

		if(injectedChanges) data->lastVersionWithData = ver;

		data->updateEagerReads = NULL;
		data->debug_inApplyUpdate = false;

		if(ver == invalidVersion && !fii.changes.empty() ) {
			ver = updater.currentVersion;
		}

		if(ver != invalidVersion && ver > data->version.get()) {
			// TODO(alexmiller): Update to version tracking.
			DEBUG_KEY_RANGE("SSUpdate", ver, KeyRangeRef());

			data->mutableData().createNewVersion(ver);
			if (data->otherError.getFuture().isReady()) data->otherError.getFuture().get();

			data->noRecentUpdates.set(false);
			data->lastUpdate = now();
			data->version.set( ver );		// Triggers replies to waiting gets for new version(s)
			setDataVersion(data->thisServerID, data->version.get());
			if (data->otherError.getFuture().isReady()) data->otherError.getFuture().get();

			Version maxVersionsInMemory = data->readTxnLifetime;
			for(int i = 0; i < data->recoveryVersionSkips.size(); i++) {
				maxVersionsInMemory += data->recoveryVersionSkips[i].second;
			}

			// Trigger updateStorage if necessary
			Version proposedOldestVersion = std::max(data->version.get(), cursor->getMinKnownCommittedVersion()) - maxVersionsInMemory;
			if(data->primaryLocality == tagLocalitySpecial || data->tag.locality == data->primaryLocality) {
				proposedOldestVersion = std::max(proposedOldestVersion, data->lastTLogVersion - maxVersionsInMemory);
			}
			proposedOldestVersion = std::min(proposedOldestVersion, data->version.get()-1);
			proposedOldestVersion = std::max(proposedOldestVersion, data->oldestVersion.get());
			proposedOldestVersion = std::max(proposedOldestVersion, data->desiredOldestVersion.get());

			//TraceEvent("StorageServerUpdated", data->thisServerID).detail("Ver", ver).detail("DataVersion", data->version.get())
			//	.detail("LastTLogVersion", data->lastTLogVersion).detail("NewOldest", data->oldestVersion.get()).detail("DesiredOldest",data->desiredOldestVersion.get())
			//	.detail("MaxVersionInMemory", maxVersionsInMemory).detail("Proposed", proposedOldestVersion).detail("PrimaryLocality", data->primaryLocality).detail("Tag", data->tag.toString());

			while(!data->recoveryVersionSkips.empty() && proposedOldestVersion > data->recoveryVersionSkips.front().first) {
				data->recoveryVersionSkips.pop_front();
			}
			data->desiredOldestVersion.set(proposedOldestVersion);

		}

		validate(data);

		data->logCursor->advanceTo( cloneCursor2->version() );
		if(cursor->version().version >= data->lastTLogVersion) {
			if(data->behind) {
				TraceEvent("StorageServerNoLongerBehind", data->thisServerID).detail("CursorVersion", cursor->version().version).detail("TLogVersion", data->lastTLogVersion);
			}
			data->behind = false;
		}

		return Void();  // update will get called again ASAP
	} catch (Error& err) {
		state Error e = err;
		if (e.code() != error_code_worker_removed && e.code() != error_code_please_reboot) {
			TraceEvent(SevError, "SSUpdateError", data->thisServerID).error(e).backtrace();
		} else if (e.code() == error_code_please_reboot) {
			wait( data->durableInProgress );
		}
		throw e;
	}
}

ACTOR Future<Void> updateStorage(StorageServer* data) {
	loop {
		ASSERT( data->durableVersion.get() == data->storageVersion() );
		if (g_network->isSimulated()) {
			double endTime = g_simulator.checkDisabled(format("%s/updateStorage", data->thisServerID.toString().c_str()));
			if(endTime > now()) {
				wait(delay(endTime - now(), TaskPriority::UpdateStorage));
			}
		}
		wait( data->desiredOldestVersion.whenAtLeast( data->storageVersion()+1 ) );
		wait( delay(0, TaskPriority::UpdateStorage) );

		state Promise<Void> durableInProgress;
		data->durableInProgress = durableInProgress.getFuture();

		state Version startOldestVersion = data->storageVersion();
		state Version newOldestVersion = data->storageVersion();
		state Version desiredVersion = data->desiredOldestVersion.get();
		state int64_t bytesLeft = SERVER_KNOBS->STORAGE_COMMIT_BYTES;

		// Write mutations to storage until we reach the desiredVersion or have written too much (bytesleft)
		loop {
			state bool done = data->storage.makeVersionMutationsDurable(newOldestVersion, desiredVersion, bytesLeft);
			// We want to forget things from these data structures atomically with changing oldestVersion (and "before", since oldestVersion.set() may trigger waiting actors)
			// forgetVersionsBeforeAsync visibly forgets immediately (without waiting) but asynchronously frees memory.
			Future<Void> finishedForgetting = data->mutableData().forgetVersionsBeforeAsync( newOldestVersion, TaskPriority::UpdateStorage );
			data->oldestVersion.set( newOldestVersion );
			wait( finishedForgetting );
			wait( yield(TaskPriority::UpdateStorage) );
			if (done) break;
		}

		// Set the new durable version as part of the outstanding change set, before commit
		if (startOldestVersion != newOldestVersion)
			data->storage.makeVersionDurable( newOldestVersion );

		debug_advanceMaxCommittedVersion( data->thisServerID, newOldestVersion );
		state Future<Void> durable = data->storage.commit();
		state Future<Void> durableDelay = Void();

		if (bytesLeft > 0) {
			durableDelay = delay(SERVER_KNOBS->STORAGE_COMMIT_INTERVAL, TaskPriority::UpdateStorage);
		}

		wait( durable );

		debug_advanceMinCommittedVersion( data->thisServerID, newOldestVersion );

		if(newOldestVersion > data->rebootAfterDurableVersion) {
			TraceEvent("RebootWhenDurableTriggered", data->thisServerID).detail("NewOldestVersion", newOldestVersion).detail("RebootAfterDurableVersion", data->rebootAfterDurableVersion);
			// To avoid brokenPromise error, which is caused by the sender of the durableInProgress (i.e., this process)
			// never sets durableInProgress, we should set durableInProgress before send the please_reboot() error.
			// Otherwise, in the race situation when storage server receives both reboot and
			// brokenPromise of durableInProgress, the worker of the storage server will die.
			// We will eventually end up with no worker for storage server role.
			// The data distributor's buildTeam() will get stuck in building a team
			durableInProgress.sendError(please_reboot());
			throw please_reboot();
		}

		durableInProgress.send(Void());
		wait( delay(0, TaskPriority::UpdateStorage) ); //Setting durableInProgess could cause the storage server to shut down, so delay to check for cancellation

		// Taking and releasing the durableVersionLock ensures that no eager reads both begin before the commit was effective and
		// are applied after we change the durable version. Also ensure that we have to lock while calling changeDurableVersion,
		// because otherwise the latest version of mutableData might be partially loaded.
		wait( data->durableVersionLock.take() );
		data->popVersion( data->durableVersion.get() + 1 );

		while (!changeDurableVersion( data, newOldestVersion )) {
			if(g_network->check_yield(TaskPriority::UpdateStorage)) {
				data->durableVersionLock.release();
				wait(delay(0, TaskPriority::UpdateStorage));
				wait( data->durableVersionLock.take() );
			}
		}

		data->durableVersionLock.release();

		//TraceEvent("StorageServerDurable", data->thisServerID).detail("Version", newOldestVersion);

		wait( durableDelay );
	}
}

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

////////////////////////////////// StorageServerDisk ///////////////////////////////////////
#ifndef __INTEL_COMPILER
#pragma region StorageServerDisk
#endif

void StorageServerDisk::makeNewStorageServerDurable() {
	storage->set( persistFormat );
	storage->set( KeyValueRef(persistID, BinaryWriter::toValue(data->thisServerID, Unversioned())) );
	storage->set( KeyValueRef(persistVersion, BinaryWriter::toValue(data->version.get(), Unversioned())) );
	storage->set( KeyValueRef(persistShardAssignedKeys.begin.toString(), LiteralStringRef("0")) );
	storage->set( KeyValueRef(persistShardAvailableKeys.begin.toString(), LiteralStringRef("0")) );
}

void setAvailableStatus( StorageServer* self, KeyRangeRef keys, bool available ) {
	//ASSERT( self->debug_inApplyUpdate );
	ASSERT( !keys.empty() );

	auto& mLV = self->addVersionToMutationLog( self->data().getLatestVersion() );

	KeyRange availableKeys = KeyRangeRef( persistShardAvailableKeys.begin.toString() + keys.begin.toString(), persistShardAvailableKeys.begin.toString() + keys.end.toString() );
	//TraceEvent("SetAvailableStatus", self->thisServerID).detail("Version", mLV.version).detail("RangeBegin", availableKeys.begin).detail("RangeEnd", availableKeys.end);

	self->addMutationToMutationLog( mLV, MutationRef( MutationRef::ClearRange, availableKeys.begin, availableKeys.end ) );
	self->addMutationToMutationLog( mLV, MutationRef( MutationRef::SetValue, availableKeys.begin, available ? LiteralStringRef("1") : LiteralStringRef("0") ) );
	if (keys.end != allKeys.end) {
		bool endAvailable = self->shards.rangeContaining( keys.end )->value()->isInVersionedData();
		self->addMutationToMutationLog( mLV, MutationRef( MutationRef::SetValue, availableKeys.end, endAvailable ? LiteralStringRef("1") : LiteralStringRef("0") ) );
	}

}

void setAssignedStatus( StorageServer* self, KeyRangeRef keys, bool nowAssigned ) {
	ASSERT( !keys.empty() );
	auto& mLV = self->addVersionToMutationLog( self->data().getLatestVersion() );
	KeyRange assignedKeys = KeyRangeRef(
		persistShardAssignedKeys.begin.toString() + keys.begin.toString(),
		persistShardAssignedKeys.begin.toString() + keys.end.toString() );
	//TraceEvent("SetAssignedStatus", self->thisServerID).detail("Version", mLV.version).detail("RangeBegin", assignedKeys.begin).detail("RangeEnd", assignedKeys.end);
	self->addMutationToMutationLog( mLV, MutationRef( MutationRef::ClearRange, assignedKeys.begin, assignedKeys.end ) );
	self->addMutationToMutationLog( mLV, MutationRef( MutationRef::SetValue, assignedKeys.begin,
			nowAssigned ? LiteralStringRef("1") : LiteralStringRef("0") ) );
	if (keys.end != allKeys.end) {
		bool endAssigned = self->shards.rangeContaining( keys.end )->value()->assigned();
		self->addMutationToMutationLog( mLV, MutationRef( MutationRef::SetValue, assignedKeys.end, endAssigned ? LiteralStringRef("1") : LiteralStringRef("0") ) );
	}
}

void StorageServerDisk::clearRange( KeyRangeRef keys ) {
	storage->clear(keys);
}

void StorageServerDisk::writeKeyValue( KeyValueRef kv ) {
	storage->set( kv );
}

void StorageServerDisk::writeMutation( MutationRef mutation ) {
	// FIXME: DEBUG_MUTATION(debugContext, debugVersion, *m);
	if (mutation.type == MutationRef::SetValue) {
		storage->set( KeyValueRef(mutation.param1, mutation.param2) );
	} else if (mutation.type == MutationRef::ClearRange) {
		storage->clear( KeyRangeRef(mutation.param1, mutation.param2) );
	} else
		ASSERT(false);
}

void StorageServerDisk::writeMutations( MutationListRef mutations, Version debugVersion, const char* debugContext ) {
	for(auto m = mutations.begin(); m; ++m) {
		DEBUG_MUTATION(debugContext, debugVersion, *m).detail("UID", data->thisServerID);
		if (m->type == MutationRef::SetValue) {
			storage->set( KeyValueRef(m->param1, m->param2) );
		} else if (m->type == MutationRef::ClearRange) {
			storage->clear( KeyRangeRef(m->param1, m->param2) );
		}
	}
}

bool StorageServerDisk::makeVersionMutationsDurable( Version& prevStorageVersion, Version newStorageVersion, int64_t& bytesLeft ) {
	if (bytesLeft <= 0) return true;

	// Apply mutations from the mutationLog
	auto u = data->getMutationLog().upper_bound(prevStorageVersion);
	if (u != data->getMutationLog().end() && u->first <= newStorageVersion) {
		VersionUpdateRef const& v = u->second;
		ASSERT( v.version > prevStorageVersion && v.version <= newStorageVersion );
		// TODO(alexmiller): Update to version tracking.
		DEBUG_KEY_RANGE("makeVersionMutationsDurable", v.version, KeyRangeRef());
		writeMutations(v.mutations, v.version, "makeVersionDurable");
		for(auto m=v.mutations.begin(); m; ++m)
			bytesLeft -= mvccStorageBytes(*m);
		prevStorageVersion = v.version;
		return false;
	} else {
		prevStorageVersion = newStorageVersion;
		return true;
	}
}

// Update data->storage to persist the changes from (data->storageVersion(),version]
void StorageServerDisk::makeVersionDurable( Version version ) {
	storage->set( KeyValueRef(persistVersion, BinaryWriter::toValue(version, Unversioned())) );

	//TraceEvent("MakeDurable", data->thisServerID).detail("FromVersion", prevStorageVersion).detail("ToVersion", version);
}

void StorageServerDisk::changeLogProtocol(Version version, ProtocolVersion protocol) {
	data->addMutationToMutationLogOrStorage(version, MutationRef(MutationRef::SetValue, persistLogProtocol, BinaryWriter::toValue(protocol, Unversioned())));
}

ACTOR Future<Void> applyByteSampleResult( StorageServer* data, IKeyValueStore* storage, Key begin, Key end, std::vector<Standalone<VectorRef<KeyValueRef>>>* results = NULL) {
	state int totalFetches = 0;
	state int totalKeys = 0;
	state int totalBytes = 0;
	loop {
		Standalone<RangeResultRef> bs = wait( storage->readRange( KeyRangeRef(begin, end), SERVER_KNOBS->STORAGE_LIMIT_BYTES, SERVER_KNOBS->STORAGE_LIMIT_BYTES ) );
		if(results) results->push_back(bs.castTo<VectorRef<KeyValueRef>>());
		int rangeSize = bs.expectedSize();
		totalFetches++;
		totalKeys += bs.size();
		totalBytes += rangeSize;
		for( int j = 0; j < bs.size(); j++ ) {
			KeyRef key = bs[j].key.removePrefix(persistByteSampleKeys.begin);
			if(!data->byteSampleClears.rangeContaining(key).value()) {
				data->metrics.byteSample.sample.insert( key, BinaryReader::fromStringRef<int32_t>(bs[j].value, Unversioned()), false );
			}
		}
		if( rangeSize >= SERVER_KNOBS->STORAGE_LIMIT_BYTES ) {
			Key nextBegin = keyAfter(bs.back().key);
			data->byteSampleClears.insert(KeyRangeRef(begin, nextBegin).removePrefix(persistByteSampleKeys.begin), true);
			data->byteSampleClearsTooLarge.set(data->byteSampleClears.size() > SERVER_KNOBS->MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE);
			begin = nextBegin;
			if(begin == end) {
				break;
			}
		} else {
			data->byteSampleClears.insert(KeyRangeRef(begin.removePrefix(persistByteSampleKeys.begin), end == persistByteSampleKeys.end ? LiteralStringRef("\xff\xff\xff") : end.removePrefix(persistByteSampleKeys.begin)), true);
			data->byteSampleClearsTooLarge.set(data->byteSampleClears.size() > SERVER_KNOBS->MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE);
			break;
		}

		if(!results) {
			wait(delay(SERVER_KNOBS->BYTE_SAMPLE_LOAD_DELAY));
		}
	}
	TraceEvent("RecoveredByteSampleRange", data->thisServerID).detail("Begin", begin).detail("End", end).detail("Fetches", totalFetches).detail("Keys", totalKeys).detail("ReadBytes", totalBytes);
	return Void();
}

ACTOR Future<Void> restoreByteSample(StorageServer* data, IKeyValueStore* storage, Promise<Void> byteSampleSampleRecovered, Future<Void> startRestore) {
	state std::vector<Standalone<VectorRef<KeyValueRef>>> byteSampleSample;
	wait( applyByteSampleResult(data, storage, persistByteSampleSampleKeys.begin, persistByteSampleSampleKeys.end, &byteSampleSample) );
	byteSampleSampleRecovered.send(Void());
	wait( startRestore );
	wait( delay(SERVER_KNOBS->BYTE_SAMPLE_START_DELAY) );

	size_t bytes_per_fetch = 0;
	// Since the expected size also includes (as of now) the space overhead of the container, we calculate our own number here
	for( auto& it : byteSampleSample ) {
		for( auto& kv : it ) {
			bytes_per_fetch += BinaryReader::fromStringRef<int32_t>(kv.value, Unversioned());
		}
	}
	bytes_per_fetch = (bytes_per_fetch/SERVER_KNOBS->BYTE_SAMPLE_LOAD_PARALLELISM) + 1;

	state std::vector<Future<Void>> sampleRanges;
	int accumulatedSize = 0;
	Key lastStart = persistByteSampleKeys.begin; // make sure the first range starts at the absolute beginning of the byte sample
	for( auto& it : byteSampleSample ) {
		for( auto& kv : it ) {
			if( accumulatedSize >= bytes_per_fetch ) {
				accumulatedSize = 0;
				Key realKey = kv.key.removePrefix(  persistByteSampleKeys.begin );
				sampleRanges.push_back( applyByteSampleResult(data, storage, lastStart, realKey) );
				lastStart = realKey;
			}
			accumulatedSize += BinaryReader::fromStringRef<int32_t>(kv.value, Unversioned());
		}
	}
	// make sure that the last range goes all the way to the end of the byte sample
	sampleRanges.push_back( applyByteSampleResult(data, storage, lastStart, persistByteSampleKeys.end) );

	wait( waitForAll( sampleRanges ) );
	TraceEvent("RecoveredByteSampleChunkedRead", data->thisServerID).detail("Ranges",sampleRanges.size());

	if( BUGGIFY )
		wait( delay( deterministicRandom()->random01() * 10.0 ) );

	return Void();
}

ACTOR Future<bool> restoreDurableState( StorageServer* data, IKeyValueStore* storage ) {
	state Future<Optional<Value>> fFormat = storage->readValue(persistFormat.key);
	state Future<Optional<Value>> fID = storage->readValue(persistID);
	state Future<Optional<Value>> fVersion = storage->readValue(persistVersion);
	state Future<Optional<Value>> fLogProtocol = storage->readValue(persistLogProtocol);
	state Future<Optional<Value>> fPrimaryLocality = storage->readValue(persistPrimaryLocality);
	state Future<Standalone<RangeResultRef>> fShardAssigned = storage->readRange(persistShardAssignedKeys);
	state Future<Standalone<RangeResultRef>> fShardAvailable = storage->readRange(persistShardAvailableKeys);

	state Promise<Void> byteSampleSampleRecovered;
	state Promise<Void> startByteSampleRestore;
	data->byteSampleRecovery = restoreByteSample(data, storage, byteSampleSampleRecovered, startByteSampleRestore.getFuture());

	TraceEvent("ReadingDurableState", data->thisServerID);
	wait( waitForAll( std::vector{ fFormat, fID, fVersion, fLogProtocol, fPrimaryLocality } ) );
	wait( waitForAll( std::vector{ fShardAssigned, fShardAvailable } ) );
	wait( byteSampleSampleRecovered.getFuture() );
	TraceEvent("RestoringDurableState", data->thisServerID);

	if (!fFormat.get().present()) {
		// The DB was never initialized
		TraceEvent("DBNeverInitialized", data->thisServerID);
		storage->dispose();
		data->thisServerID = UID();
		data->sk = Key();
		return false;
	}
	if (!persistFormatReadableRange.contains( fFormat.get().get() )) {
		TraceEvent(SevError, "UnsupportedDBFormat").detail("Format", fFormat.get().get().toString()).detail("Expected", persistFormat.value.toString());
		throw worker_recovery_failed();
	}
	data->thisServerID = BinaryReader::fromStringRef<UID>(fID.get().get(), Unversioned());
	data->sk = serverKeysPrefixFor( data->thisServerID ).withPrefix(systemKeys.begin);  // FFFF/serverKeys/[this server]/

	if (fLogProtocol.get().present())
		data->logProtocol = BinaryReader::fromStringRef<ProtocolVersion>(fLogProtocol.get().get(), Unversioned());

	if (fPrimaryLocality.get().present())
		data->primaryLocality = BinaryReader::fromStringRef<int8_t>(fPrimaryLocality.get().get(), Unversioned());

	state Version version = BinaryReader::fromStringRef<Version>( fVersion.get().get(), Unversioned() );
	debug_checkRestoredVersion( data->thisServerID, version, "StorageServer" );
	data->setInitialVersion( version );

	state Standalone<RangeResultRef> available = fShardAvailable.get();
	state int availableLoc;
	for(availableLoc=0; availableLoc<available.size(); availableLoc++) {
		KeyRangeRef keys(
			available[availableLoc].key.removePrefix(persistShardAvailableKeys.begin),
			availableLoc+1==available.size() ? allKeys.end : available[availableLoc+1].key.removePrefix(persistShardAvailableKeys.begin));
		ASSERT( !keys.empty() );
		bool nowAvailable = available[availableLoc].value!=LiteralStringRef("0");
		/*if(nowAvailable)
		  TraceEvent("AvailableShard", data->thisServerID).detail("RangeBegin", keys.begin).detail("RangeEnd", keys.end);*/
		data->newestAvailableVersion.insert( keys, nowAvailable ? latestVersion : invalidVersion );
		wait(yield());
	}

	state Standalone<RangeResultRef> assigned = fShardAssigned.get();
	state int assignedLoc;
	for(assignedLoc=0; assignedLoc<assigned.size(); assignedLoc++) {
		KeyRangeRef keys(
			assigned[assignedLoc].key.removePrefix(persistShardAssignedKeys.begin),
			assignedLoc+1==assigned.size() ? allKeys.end : assigned[assignedLoc+1].key.removePrefix(persistShardAssignedKeys.begin));
		ASSERT( !keys.empty() );
		bool nowAssigned = assigned[assignedLoc].value!=LiteralStringRef("0");
		/*if(nowAssigned)
		  TraceEvent("AssignedShard", data->thisServerID).detail("RangeBegin", keys.begin).detail("RangeEnd", keys.end);*/
		changeServerKeys(data, keys, nowAssigned, version, CSK_RESTORE);

		if (!nowAssigned) ASSERT( data->newestAvailableVersion.allEqual(keys, invalidVersion) );
		wait(yield());
	}

	wait( delay( 0.0001 ) );

	{
		// Erase data which isn't available (it is from some fetch at a later version)
		// SOMEDAY: Keep track of keys that might be fetching, make sure we don't have any data elsewhere?
		for(auto it = data->newestAvailableVersion.ranges().begin(); it != data->newestAvailableVersion.ranges().end(); ++it) {
			if (it->value() == invalidVersion) {
				KeyRangeRef clearRange(it->begin(), it->end());
				// TODO(alexmiller): Figure out how to selectively enable spammy data distribution events.
				//DEBUG_KEY_RANGE("clearInvalidVersion", invalidVersion, clearRange);
				storage->clear( clearRange );
				data->byteSampleApplyClear( clearRange, invalidVersion );
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

//Determines whether a key-value pair should be included in a byte sample
//Also returns size information about the sample
ByteSampleInfo isKeyValueInSample(KeyValueRef keyValue) {
	ByteSampleInfo info;

	const KeyRef key = keyValue.key;
	info.size = key.size() + keyValue.value.size();

	uint32_t a = 0;
	uint32_t b = 0;
	hashlittle2( key.begin(), key.size(), &a, &b );

	double probability = (double)info.size / (key.size() + SERVER_KNOBS->BYTE_SAMPLING_OVERHEAD) / SERVER_KNOBS->BYTE_SAMPLING_FACTOR;
	info.inSample = a / ((1 << 30) * 4.0) < probability;
	info.sampledSize = info.size / std::min(1.0, probability);

	return info;
}

void StorageServer::addMutationToMutationLogOrStorage( Version ver, MutationRef m ) {
	if (ver != invalidVersion) {
		addMutationToMutationLog( addVersionToMutationLog(ver), m );
	} else {
		storage.writeMutation( m );
		byteSampleApplyMutation( m, ver );
	}
}

void StorageServer::byteSampleApplySet( KeyValueRef kv, Version ver ) {
	// Update byteSample in memory and (eventually) on disk and notify waiting metrics

	ByteSampleInfo sampleInfo = isKeyValueInSample(kv);
	auto& byteSample = metrics.byteSample.sample;

	int64_t delta = 0;
	const KeyRef key = kv.key;

	auto old = byteSample.find(key);
	if (old != byteSample.end()) delta = -byteSample.getMetric(old);
	if (sampleInfo.inSample) {
		delta += sampleInfo.sampledSize;
		byteSample.insert( key, sampleInfo.sampledSize );
		addMutationToMutationLogOrStorage( ver, MutationRef(MutationRef::SetValue, key.withPrefix(persistByteSampleKeys.begin), BinaryWriter::toValue( sampleInfo.sampledSize, Unversioned() )) );
	} else {
		bool any = old != byteSample.end();
		if(!byteSampleRecovery.isReady() ) {
			if(!byteSampleClears.rangeContaining(key).value()) {
				byteSampleClears.insert(key, true);
				byteSampleClearsTooLarge.set(byteSampleClears.size() > SERVER_KNOBS->MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE);
				any = true;
			}
		}
		if (any) {
			byteSample.erase(old);
			auto diskRange = singleKeyRange(key.withPrefix(persistByteSampleKeys.begin));
			addMutationToMutationLogOrStorage( ver, MutationRef(MutationRef::ClearRange, diskRange.begin, diskRange.end) );
		}
	}

	if (delta) metrics.notifyBytes( key, delta );
}

void StorageServer::byteSampleApplyClear( KeyRangeRef range, Version ver ) {
	// Update byteSample in memory and (eventually) on disk via the mutationLog and notify waiting metrics

	auto& byteSample = metrics.byteSample.sample;
	bool any = false;

	if(range.begin < allKeys.end) {
		//NotifyBytes should not be called for keys past allKeys.end
		KeyRangeRef searchRange = KeyRangeRef(range.begin, std::min(range.end, allKeys.end));
		counters.sampledBytesCleared += byteSample.sumRange(searchRange.begin, searchRange.end);

		auto r = metrics.waitMetricsMap.intersectingRanges(searchRange);
		for(auto shard = r.begin(); shard != r.end(); ++shard) {
			KeyRangeRef intersectingRange = shard.range() & range;
			int64_t bytes = byteSample.sumRange(intersectingRange.begin, intersectingRange.end);
			metrics.notifyBytes(shard, -bytes);
			any = any || bytes > 0;
		}
	}

	if(range.end > allKeys.end && byteSample.sumRange(std::max(allKeys.end, range.begin), range.end) > 0)
		any = true;

	if(!byteSampleRecovery.isReady()) {
		auto clearRanges = byteSampleClears.intersectingRanges(range);
		for(auto it : clearRanges) {
			if(!it.value()) {
				byteSampleClears.insert(range, true);
				byteSampleClearsTooLarge.set(byteSampleClears.size() > SERVER_KNOBS->MAX_BYTE_SAMPLE_CLEAR_MAP_SIZE);
				any = true;
				break;
			}
		}
	}

	if (any) {
		byteSample.eraseAsync( range.begin, range.end );
		auto diskRange = range.withPrefix( persistByteSampleKeys.begin );
		addMutationToMutationLogOrStorage( ver, MutationRef(MutationRef::ClearRange, diskRange.begin, diskRange.end) );
	}
}

ACTOR Future<Void> waitMetrics( StorageServerMetrics* self, WaitMetricsRequest req, Future<Void> timeout ) {
	state PromiseStream< StorageMetrics > change;
	state StorageMetrics metrics = self->getMetrics( req.keys );
	state Error error = success();
	state bool timedout = false;

	if ( !req.min.allLessOrEqual( metrics ) || !metrics.allLessOrEqual( req.max ) ) {
		TEST( true ); // ShardWaitMetrics return case 1 (quickly)
		req.reply.send( metrics );
		return Void();
	}

	{
		auto rs = self->waitMetricsMap.modify( req.keys );
		for(auto r = rs.begin(); r != rs.end(); ++r)
			r->value().push_back( change );
		loop {
			try {
				choose {
					when( StorageMetrics c = waitNext( change.getFuture() ) ) {
						metrics += c;

						// SOMEDAY: validation! The changes here are possibly partial changes (we receive multiple messages per
						//  update to our requested range). This means that the validation would have to occur after all
						//  the messages for one clear or set have been dispatched.

						/*StorageMetrics m = getMetrics( data, req.keys );
						  bool b = ( m.bytes != metrics.bytes || m.bytesPerKSecond != metrics.bytesPerKSecond || m.iosPerKSecond != metrics.iosPerKSecond );
						  if (b) {
						  printf("keys: '%s' - '%s' @%p\n", printable(req.keys.begin).c_str(), printable(req.keys.end).c_str(), this);
						  printf("waitMetrics: desync %d (%lld %lld %lld) != (%lld %lld %lld); +(%lld %lld %lld)\n", b, m.bytes, m.bytesPerKSecond, m.iosPerKSecond, metrics.bytes, metrics.bytesPerKSecond, metrics.iosPerKSecond, c.bytes, c.bytesPerKSecond, c.iosPerKSecond);

						  }*/
					}
					when( wait( timeout ) ) {
						timedout = true;
					}
				}
			} catch (Error& e) {
				if( e.code() == error_code_actor_cancelled ) throw; // This is only cancelled when the main loop had exited...no need in this case to clean up self
				error = e;
				break;
			}

			if( timedout ) {
				TEST( true ); // ShardWaitMetrics return on timeout
				//FIXME: instead of using random chance, send wrong_shard_server when the call in from waitMetricsMultiple (requires additional information in the request)
				if(deterministicRandom()->random01() < SERVER_KNOBS->WAIT_METRICS_WRONG_SHARD_CHANCE) {
					req.reply.sendError( wrong_shard_server() );
				} else {
					req.reply.send( metrics );
				}
				break;
			}

			if ( !req.min.allLessOrEqual( metrics ) || !metrics.allLessOrEqual( req.max ) ) {
				TEST( true ); // ShardWaitMetrics return case 2 (delayed)
				req.reply.send( metrics );
				break;
			}
		}

		wait( delay(0) ); //prevent iterator invalidation of functions sending changes
	}

	auto rs = self->waitMetricsMap.modify( req.keys );
	for(auto i = rs.begin(); i != rs.end(); ++i) {
		auto &x = i->value();
		for( int j = 0; j < x.size(); j++ ) {
			if( x[j] == change ) {
				swapAndPop(&x, j);
				break;
			}
		}
	}
	self->waitMetricsMap.coalesce( req.keys );

	if (error.code() != error_code_success ) {
		if (error.code() != error_code_wrong_shard_server) throw error;
		TEST( true );	// ShardWaitMetrics delayed wrong_shard_server()
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

ACTOR Future<Void> metricsCore( StorageServer* self, StorageServerInterface ssi ) {
	state Future<Void> doPollMetrics = Void();

	wait( self->byteSampleRecovery );

	self->actors.add(traceCounters("StorageMetrics", self->thisServerID, SERVER_KNOBS->STORAGE_LOGGING_DELAY, &self->counters.cc, self->thisServerID.toString() + "/StorageMetrics"));

	loop {
		choose {
			when (WaitMetricsRequest req = waitNext(ssi.waitMetrics.getFuture())) {
				if (!self->isReadable( req.keys )) {
					TEST( true );	// waitMetrics immediate wrong_shard_server()
					self->sendErrorWithPenalty(req.reply, wrong_shard_server(), self->getPenalty());
				} else {
					self->actors.add( self->metrics.waitMetrics( req, delayJittered( SERVER_KNOBS->STORAGE_METRIC_TIMEOUT ) ) );
				}
			}
			when (SplitMetricsRequest req = waitNext(ssi.splitMetrics.getFuture())) {
				if (!self->isReadable( req.keys )) {
					TEST( true );	// splitMetrics immediate wrong_shard_server()
					self->sendErrorWithPenalty(req.reply, wrong_shard_server(), self->getPenalty());
				} else {
					self->metrics.splitMetrics( req );
				}
			}
			when (GetStorageMetricsRequest req = waitNext(ssi.getStorageMetrics.getFuture())) {
				StorageBytes sb = self->storage.getStorageBytes();
				self->metrics.getStorageMetrics( req, sb, self->counters.bytesInput.getRate(), self->versionLag, self->lastUpdate );
			}
			when(ReadHotSubRangeRequest req = waitNext(ssi.getReadHotRanges.getFuture())) {
				if (!self->isReadable(req.keys)) {
					TEST(true); // readHotSubRanges immediate wrong_shard_server()
					self->sendErrorWithPenalty(req.reply, wrong_shard_server(), self->getPenalty());
				} else {
					self->metrics.getReadHotRanges(req);
				}
			}
			when (wait(doPollMetrics) ) {
				self->metrics.poll();
				doPollMetrics = delay(SERVER_KNOBS->STORAGE_SERVER_POLL_METRICS_DELAY);
			}
		}
	}
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

ACTOR Future<Void> checkBehind( StorageServer* self ) {
	state int behindCount = 0;
	loop {
		wait( delay(SERVER_KNOBS->BEHIND_CHECK_DELAY) );
		state Transaction tr(self->cx);
		loop {
			try {
				Version readVersion = wait( tr.getRawReadVersion() );
				if( readVersion > self->version.get() + SERVER_KNOBS->BEHIND_CHECK_VERSIONS ) {
					behindCount++;
				} else {
					behindCount = 0;
				}
				self->versionBehind = behindCount >= SERVER_KNOBS->BEHIND_CHECK_COUNT;
				break;
			} catch( Error &e ) {
				wait(tr.onError(e));
			}
		}
	}
}

ACTOR Future<Void> serveGetValueRequests( StorageServer* self, FutureStream<GetValueRequest> getValue ) {
	loop {
		GetValueRequest req = waitNext(getValue);
		// Warning: This code is executed at extremely high priority (TaskPriority::LoadBalancedEndpoint), so downgrade before doing real work
		if( req.debugID.present() )
			g_traceBatch.addEvent("GetValueDebug", req.debugID.get().first(), "storageServer.received"); //.detail("TaskID", g_network->getCurrentTask());

		if (SHORT_CIRCUT_ACTUAL_STORAGE && normalKeys.contains(req.key))
			req.reply.send(GetValueReply());
		else
			self->actors.add(self->readGuard(req , getValueQ));
	}
}

ACTOR Future<Void> serveGetKeyValuesRequests( StorageServer* self, FutureStream<GetKeyValuesRequest> getKeyValues ) {
	loop {
		GetKeyValuesRequest req = waitNext(getKeyValues);
		// Warning: This code is executed at extremely high priority (TaskPriority::LoadBalancedEndpoint), so downgrade before doing real work
		self->actors.add(self->readGuard(req, getKeyValuesQ));
	}
}

ACTOR Future<Void> serveGetKeyRequests( StorageServer* self, FutureStream<GetKeyRequest> getKey ) {
	loop {
		GetKeyRequest req = waitNext(getKey);
		// Warning: This code is executed at extremely high priority (TaskPriority::LoadBalancedEndpoint), so downgrade before doing real work
		self->actors.add(self->readGuard(req , getKeyQ));
	}
}

ACTOR Future<Void> serveWatchValueRequests( StorageServer* self, FutureStream<WatchValueRequest> watchValue ) {
	loop {
		WatchValueRequest req = waitNext(watchValue);
		// TODO: fast load balancing?
		// SOMEDAY: combine watches for the same key/value into a single watch
		self->actors.add(self->readGuard(req, watchValueQ));
	}
}

ACTOR Future<Void> storageServerCore( StorageServer* self, StorageServerInterface ssi )
{
	state Future<Void> doUpdate = Void();
	state bool updateReceived = false;  // true iff the current update() actor assigned to doUpdate has already received an update from the tlog
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
	self->actors.add(serveGetKeyRequests(self, ssi.getKey.getFuture()));
	self->actors.add(serveWatchValueRequests(self, ssi.watchValue.getFuture()));
	self->actors.add(traceRole(Role::STORAGE_SERVER, ssi.id()));

	self->transactionTagCounter.startNewInterval(self->thisServerID);
	self->actors.add(recurring([&](){ self->transactionTagCounter.startNewInterval(self->thisServerID); }, SERVER_KNOBS->READ_TAG_MEASUREMENT_INTERVAL));

	self->coreStarted.send( Void() );

	loop {
		++self->counters.loops;

		double loopTopTime = now();
		double elapsedTime = loopTopTime - lastLoopTopTime;
		if( elapsedTime > 0.050 ) {
			if (deterministicRandom()->random01() < 0.01)
				TraceEvent(SevWarn, "SlowSSLoopx100", self->thisServerID).detail("Elapsed", elapsedTime);
		}
		lastLoopTopTime = loopTopTime;

		choose {
			when( wait( checkLastUpdate ) ) {
				if(now() - self->lastUpdate >= CLIENT_KNOBS->NO_RECENT_UPDATES_DURATION) {
					self->noRecentUpdates.set(true);
					checkLastUpdate = delay(CLIENT_KNOBS->NO_RECENT_UPDATES_DURATION);
				} else {
					checkLastUpdate = delay( std::max(CLIENT_KNOBS->NO_RECENT_UPDATES_DURATION-(now()-self->lastUpdate), 0.1) );
				}
			}
			when( wait( dbInfoChange ) ) {
				TEST( self->logSystem );  // shardServer dbInfo changed
				dbInfoChange = self->db->onChange();
				if( self->db->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS ) {
					self->logSystem = ILogSystem::fromServerDBInfo( self->thisServerID, self->db->get() );
					if (self->logSystem) {
						if(self->db->get().logSystemConfig.recoveredAt.present()) {
							self->poppedAllAfter = self->db->get().logSystemConfig.recoveredAt.get();
						}
						self->logCursor = self->logSystem->peekSingle( self->thisServerID, self->version.get() + 1, self->tag, self->history );
						self->popVersion( self->durableVersion.get() + 1, true );
					}
					// If update() is waiting for results from the tlog, it might never get them, so needs to be cancelled.  But if it is waiting later,
					// cancelling it could cause problems (e.g. fetchKeys that already committed to transitioning to waiting state)
					if (!updateReceived) {
						doUpdate = Void();
					}
				}

				Optional<LatencyBandConfig> newLatencyBandConfig = self->db->get().latencyBandConfig;
				if(newLatencyBandConfig.present() != self->latencyBandConfig.present()
					|| (newLatencyBandConfig.present() && newLatencyBandConfig.get().readConfig != self->latencyBandConfig.get().readConfig))
				{
					self->latencyBandConfig = newLatencyBandConfig;
					self->counters.readLatencyBands.clearBands();
					TraceEvent("LatencyBandReadUpdatingConfig").detail("Present", newLatencyBandConfig.present());
					if(self->latencyBandConfig.present()) {
						for(auto band : self->latencyBandConfig.get().readConfig.bands) {
							self->counters.readLatencyBands.addThreshold(band);
						}
					}
				}
			}
			when (GetShardStateRequest req = waitNext(ssi.getShardState.getFuture()) ) {
				if (req.mode == GetShardStateRequest::NO_WAIT ) {
					if( self->isReadable( req.keys ) )
						req.reply.send(GetShardStateReply{ self->version.get(), self->durableVersion.get() });
					else
						req.reply.sendError(wrong_shard_server());
				} else {
					self->actors.add( getShardStateQ( self, req ) );
				}
			}
			when (StorageQueuingMetricsRequest req = waitNext(ssi.getQueuingMetrics.getFuture())) {
				getQueuingMetrics(self, req);
			}
			when( ReplyPromise<KeyValueStoreType> reply = waitNext(ssi.getKeyValueStoreType.getFuture()) ) {
				reply.send( self->storage.getKeyValueStoreType() );
			}
			when( wait(doUpdate) ) {
				updateReceived = false;
				if (!self->logSystem)
					doUpdate = Never();
				else
					doUpdate = update( self, &updateReceived );
			}
			when(wait(updateProcessStatsTimer)) {
				updateProcessStats(self);
				updateProcessStatsTimer = delay(SERVER_KNOBS->FASTRESTORE_UPDATE_PROCESS_STATS_INTERVAL);
			}
			when(wait(self->actors.getResult())) {}
		}
	}
}

bool storageServerTerminated(StorageServer& self, IKeyValueStore* persistentData, Error const& e) {
	self.shuttingDown = true;

	// Clearing shards shuts down any fetchKeys actors; these may do things on cancellation that are best done with self still valid
	self.shards.insert( allKeys, Reference<ShardInfo>() );

	// Dispose the IKVS (destroying its data permanently) only if this shutdown is definitely permanent.  Otherwise just close it.
	if (e.code() == error_code_please_reboot) {
		// do nothing.
	} else if (e.code() == error_code_worker_removed || e.code() == error_code_recruitment_failed) {
		persistentData->dispose();
	} else {
		persistentData->close();
	}

	if ( e.code() == error_code_worker_removed ||
		 e.code() == error_code_recruitment_failed ||
		 e.code() == error_code_file_not_found ||
		 e.code() == error_code_actor_cancelled )
	{
		TraceEvent("StorageServerTerminated", self.thisServerID).error(e, true);
		return true;
	} else
		return false;
}

ACTOR Future<Void> memoryStoreRecover(IKeyValueStore* store, Reference<ClusterConnectionFile> connFile, UID id)
{
	if (store->getType() != KeyValueStoreType::MEMORY || connFile.getPtr() == nullptr) {
		return Never();
	}

	// create a temp client connect to DB
	Database cx = Database::createDatabase(connFile, Database::API_VERSION_LATEST);

	state Transaction tr( cx );
	state int noCanRemoveCount = 0;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			state bool canRemove = wait( canRemoveStorageServer( &tr, id ) );
			if (!canRemove) {
				TEST(true); // it's possible that the caller had a transaction in flight that assigned keys to the server. Wait for it to reverse its mistake.
				wait( delayJittered(SERVER_KNOBS->REMOVE_RETRY_DELAY, TaskPriority::UpdateStorage) );
				tr.reset();
				TraceEvent("RemoveStorageServerRetrying").detail("Count", noCanRemoveCount++).detail("ServerID", id).detail("CanRemove", canRemove);
			} else {
				return Void();
			}
		} catch (Error& e) {
			state Error err = e;
			wait(tr.onError(e));
			TraceEvent("RemoveStorageServerRetrying").error(err);
		}
	}
}

ACTOR Future<Void> storageServer( IKeyValueStore* persistentData, StorageServerInterface ssi, Tag seedTag, ReplyPromise<InitializeStorageReply> recruitReply,
	Reference<AsyncVar<ServerDBInfo>> db, std::string folder )
{
	state StorageServer self(persistentData, db, ssi);

	self.sk = serverKeysPrefixFor( self.thisServerID ).withPrefix(systemKeys.begin);  // FFFF/serverKeys/[this server]/
	self.folder = folder;

	try {
		wait( self.storage.init() );
		wait( self.storage.commit() );

		if (seedTag == invalidTag) {
			std::pair<Version, Tag> verAndTag = wait( addStorageServer(self.cx, ssi) ); // Might throw recruitment_failed in case of simultaneous master failure
			self.tag = verAndTag.second;
			self.setInitialVersion( verAndTag.first-1 );
		} else {
			self.tag = seedTag;
		}

		self.storage.makeNewStorageServerDurable();
		wait( self.storage.commit() );

		TraceEvent("StorageServerInit", ssi.id()).detail("Version", self.version.get()).detail("SeedTag", seedTag.toString());
		InitializeStorageReply rep;
		rep.interf = ssi;
		rep.addedVersion = self.version.get();
		recruitReply.send(rep);
		self.byteSampleRecovery = Void();
		wait( storageServerCore(&self, ssi) );

		throw internal_error();
	} catch (Error& e) {
		// If we die with an error before replying to the recruitment request, send the error to the recruiter (ClusterController, and from there to the DataDistributionTeamCollection)
		if (!recruitReply.isSet())
			recruitReply.sendError( recruitment_failed() );
		if (storageServerTerminated(self, persistentData, e))
			return Void();
		throw e;
	}
}

ACTOR Future<Void> replaceInterface( StorageServer* self, StorageServerInterface ssi )
{
	state Transaction tr(self->cx);

	loop {
		state Future<Void> infoChanged = self->db->onChange();
		state Reference<ProxyInfo> proxies( new ProxyInfo(self->db->get().client.proxies) );
		choose {
			when( GetStorageServerRejoinInfoReply _rep = wait( proxies->size() ? basicLoadBalance( proxies, &MasterProxyInterface::getStorageServerRejoinInfo, GetStorageServerRejoinInfoRequest(ssi.id(), ssi.locality.dcId()) ) : Never() ) ) {
				state GetStorageServerRejoinInfoReply rep = _rep;
				try {
					tr.reset();
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr.setVersion( rep.version );

					tr.addReadConflictRange(singleKeyRange(serverListKeyFor(ssi.id())));
					tr.addReadConflictRange(singleKeyRange(serverTagKeyFor(ssi.id())));
					tr.addReadConflictRange(serverTagHistoryRangeFor(ssi.id()));
					tr.addReadConflictRange(singleKeyRange(tagLocalityListKeyFor(ssi.locality.dcId())));

					tr.set(serverListKeyFor(ssi.id()), serverListValue(ssi));

					if(rep.newLocality) {
						tr.addReadConflictRange(tagLocalityListKeys);
						tr.set( tagLocalityListKeyFor(ssi.locality.dcId()), tagLocalityListValue(rep.newTag.get().locality) );
					}

					if(rep.newTag.present()) {
						KeyRange conflictRange = singleKeyRange(serverTagConflictKeyFor(rep.newTag.get()));
						tr.addReadConflictRange( conflictRange );
						tr.addWriteConflictRange( conflictRange );
						tr.setOption(FDBTransactionOptions::FIRST_IN_BATCH);
						tr.set( serverTagKeyFor(ssi.id()), serverTagValue(rep.newTag.get()) );
						tr.atomicOp( serverTagHistoryKeyFor(ssi.id()), serverTagValue(rep.tag), MutationRef::SetVersionstampedKey );
					}

					if(rep.history.size() && rep.history.back().first < self->version.get()) {
						tr.clear(serverTagHistoryRangeBefore(ssi.id(), self->version.get()));
					}

					choose {
						when ( wait( tr.commit() ) ) {
							self->history = rep.history;

							if(rep.newTag.present()) {
								self->tag = rep.newTag.get();
								self->history.insert(self->history.begin(), std::make_pair(tr.getCommittedVersion(), rep.tag));
							} else {
								self->tag = rep.tag;
							}
							self->allHistory = self->history;

							TraceEvent("SSTag", self->thisServerID).detail("MyTag", self->tag.toString());
							for(auto it : self->history) {
								TraceEvent("SSHistory", self->thisServerID).detail("Ver", it.first).detail("Tag", it.second.toString());
							}

							if(self->history.size() && BUGGIFY) {
								TraceEvent("SSHistoryReboot", self->thisServerID);
								throw please_reboot();
							}

							break;
						}
						when ( wait(infoChanged) ) {}
					}
				} catch (Error& e) {
					wait( tr.onError(e) );
				}
			}
			when ( wait(infoChanged) ) {}
		}
	}

	return Void();
}

ACTOR Future<Void> storageServer( IKeyValueStore* persistentData, StorageServerInterface ssi, Reference<AsyncVar<ServerDBInfo>> db, std::string folder, Promise<Void> recovered, Reference<ClusterConnectionFile> connFile)
{
	state StorageServer self(persistentData, db, ssi);
	self.folder = folder;
	self.sk = serverKeysPrefixFor( self.thisServerID ).withPrefix(systemKeys.begin);  // FFFF/serverKeys/[this server]/
	try {
		state double start = now();
		TraceEvent("StorageServerRebootStart", self.thisServerID);

		wait(self.storage.init());
		choose {
			//after a rollback there might be uncommitted changes.
			//for memory storage engine type, wait until recovery is done before commit
			when( wait(self.storage.commit()))  {}

			when( wait(memoryStoreRecover (persistentData, connFile, self.thisServerID))) {
				TraceEvent("DisposeStorageServer", self.thisServerID);
				throw worker_removed();
			}
		}

		bool ok = wait( self.storage.restoreDurableState() );
		if (!ok) {
			if(recovered.canBeSet()) recovered.send(Void());
			return Void();
		}
		TraceEvent("SSTimeRestoreDurableState", self.thisServerID).detail("TimeTaken", now() - start);

		ASSERT( self.thisServerID == ssi.id() );
		TraceEvent("StorageServerReboot", self.thisServerID)
			.detail("Version", self.version.get());

		if(recovered.canBeSet()) recovered.send(Void());

		wait( replaceInterface( &self, ssi ) );

		TraceEvent("StorageServerStartingCore", self.thisServerID).detail("TimeTaken", now() - start);

		//wait( delay(0) );  // To make sure self->zkMasterInfo.onChanged is available to wait on
		wait( storageServerCore(&self, ssi) );

		throw internal_error();
	} catch (Error& e) {
		if(recovered.canBeSet()) recovered.send(Void());
		if (storageServerTerminated(self, persistentData, e))
			return Void();
		throw e;
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
	VersionedMap<int,int> vm;

	printf("SS Ptree node is %zu bytes\n", sizeof( StorageServer::VersionedData::PTreeT ) );

	const int NSIZE = sizeof(VersionedMap<int,int>::PTreeT);
	const int ASIZE = NSIZE <= 64 ? 64 : nextFastAllocatedSize(NSIZE);

	auto before = FastAllocator< ASIZE >::getTotalMemory();

	for(int v=1; v<=1000; ++v) {
		vm.createNewVersion(v);
		for(int i=0; i<1000; i++) {
			int k = deterministicRandom()->randomInt(0, 2000000);
			/*for(int k2=k-5; k2<k+5; k2++)
				if (vm.atLatest().find(k2) != vm.atLatest().end())
					vm.erase(k2);*/
			vm.erase( k-5, k+5 );
			vm.insert( k, v );
		}
	}

	auto after = FastAllocator< ASIZE >::getTotalMemory();

	int count = 0;
	for(auto i = vm.atLatest().begin(); i != vm.atLatest().end(); ++i)
		++count;

	printf("PTree node is %d bytes, allocated as %d bytes\n", NSIZE, ASIZE);
	printf("%d distinct after %d insertions\n", count, 1000*1000);
	printf("Memory used: %f MB\n",
		 (after - before)/ 1e6);
}

