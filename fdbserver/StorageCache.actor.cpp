/*
 * StorageCache.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/VersionedMap.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/Atomic.h"
#include "fdbclient/Notified.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// TODO storageCache server shares quite a bit of storageServer functionality, although simplified
// Need to look into refactoring common code out for better code readability and to avoid duplication

// TODO rename wrong_shard_server error to wrong_cache_server
inline bool canReplyWith(Error e) {
	switch (e.code()) {
	case error_code_transaction_too_old:
	case error_code_future_version:
	case error_code_wrong_shard_server:
	case error_code_process_behind:
		// case error_code_all_alternatives_failed:
		return true;
	default:
		return false;
	};
}

const int VERSION_OVERHEAD =
    64 + sizeof(Version) + sizeof(Standalone<VerUpdateRef>) + // mutationLog, 64b overhead for map
    2 * (64 + sizeof(Version) +
         sizeof(
             Reference<VersionedMap<KeyRef,
                                    ValueOrClearToRef>::PTreeT>)); // versioned map [ x2 for createNewVersion(version+1)
                                                                   // ], 64b overhead for map
static int mvccStorageBytes(MutationRef const& m) {
	return VersionedMap<KeyRef, ValueOrClearToRef>::overheadPerItem * 2 +
	       (MutationRef::OVERHEAD_BYTES + m.param1.size() + m.param2.size()) * 2;
}

struct StorageCacheData {
	typedef VersionedMap<KeyRef, ValueOrClearToRef> VersionedData;

private:
	// in-memory versioned struct (PTree as of now. Subject to change)
	VersionedData versionedData;
	// in-memory mutationLog that the versionedData contains references to
	// TODO change it to a deque, already contains mutations in version order
	std::map<Version, Standalone<VerUpdateRef>> mutationLog; // versions (durableVersion, version]

public:
	UID thisServerID; // unique id
	uint16_t index; // server index
	Reference<AsyncVar<Reference<ILogSystem>>> logSystem;
	Key ck; // cacheKey
	KeyRangeMap<bool> cachedRangeMap; // map of cached key-ranges

	// The following are in rough order from newest to oldest
	// TODO double check which ones we need for storageCache servers
	Version lastTLogVersion, lastVersionWithData;
	NotifiedVersion version; // current version i.e. the max version that can be read from the cache
	NotifiedVersion desiredOldestVersion; // oldestVersion can be increased to this after compaction
	NotifiedVersion oldestVersion; // Min version that might be read from the cache

	// TODO not really in use as of now. may need in some failure cases. Revisit and remove if no plausible use
	Future<Void> compactionInProgress;

	// TODO do we need otherError here?
	Promise<Void> otherError;

	int64_t versionLag; // An estimate for how many versions it takes for the data to move from the logs to this cache
	                    // server
	bool behind;

	// TODO double check which ones we need for storageCache servers
	struct Counters {
		CounterCollection cc;
		Counter allQueries, getKeyQueries, getValueQueries, getRangeQueries, finishedQueries, rowsQueried, bytesQueried,
		    watchQueries;
		Counter bytesInput, mutationBytes; // Like bytesInput but without MVCC accounting
		Counter mutations, setMutations, clearRangeMutations, atomicMutations;
		Counter updateBatches, updateVersions;
		Counter loops;
		Counter readsRejected;

		// LatencyBands readLatencyBands;

		Counters(StorageCacheData* self)
		  : cc("StorageCacheServer", self->thisServerID.toString()), getKeyQueries("GetKeyQueries", cc),
		    getValueQueries("GetValueQueries", cc), getRangeQueries("GetRangeQueries", cc),
		    allQueries("QueryQueue", cc), finishedQueries("FinishedQueries", cc), rowsQueried("RowsQueried", cc),
		    bytesQueried("BytesQueried", cc), watchQueries("WatchQueries", cc), bytesInput("BytesInput", cc),
		    mutationBytes("MutationBytes", cc), mutations("Mutations", cc), setMutations("SetMutations", cc),
		    clearRangeMutations("ClearRangeMutations", cc), atomicMutations("AtomicMutations", cc),
		    updateBatches("UpdateBatches", cc), updateVersions("UpdateVersions", cc), loops("Loops", cc),
		    readsRejected("ReadsRejected", cc) {
			specialCounter(cc, "LastTLogVersion", [self]() { return self->lastTLogVersion; });
			specialCounter(cc, "Version", [self]() { return self->version.get(); });
			specialCounter(cc, "VersionLag", [self]() { return self->versionLag; });
		}
	} counters;

	explicit StorageCacheData(UID thisServerID, uint16_t index)
	  : thisServerID(thisServerID), index(index), logSystem(new AsyncVar<Reference<ILogSystem>>()), lastTLogVersion(0),
	    lastVersionWithData(0), compactionInProgress(Void()), versionLag(0), behind(false), counters(this) {
		version.initMetric(LiteralStringRef("StorageCacheData.Version"), counters.cc.id);
		desiredOldestVersion.initMetric(LiteralStringRef("StorageCacheData.DesriedOldestVersion"), counters.cc.id);
		oldestVersion.initMetric(LiteralStringRef("StorageCacheData.OldestVersion"), counters.cc.id);
	}

	void addMutation(KeyRangeRef const& cachedKeyRange, Version version, MutationRef const& mutation);

	bool isReadable(KeyRangeRef const& keys) {
		auto cr = cachedRangeMap.intersectingRanges(keys);
		for (auto i = cr.begin(); i != cr.end(); ++i)
			if (!i->value())
				return false;
		return true;
	}

	Arena lastArena;
	std::map<Version, Standalone<VerUpdateRef>> const& getMutationLog() { return mutationLog; }
	std::map<Version, Standalone<VerUpdateRef>>& getMutableMutationLog() { return mutationLog; }
	VersionedData const& data() const { return versionedData; }
	VersionedData& mutableData() { return versionedData; }

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
		// TODO find out more
		// byteSampleApplyMutation(m, mLV.version);
		counters.bytesInput += mvccStorageBytes(m);
		return mLV.push_back_deep(mLV.arena(), m);
	}
};

///////////////////////////////////// Queries /////////////////////////////////
#pragma region Queries
ACTOR Future<Version> waitForVersion(StorageCacheData* data, Version version) {
	// This could become an Actor transparently, but for now it just does the lookup
	if (version == latestVersion)
		version = std::max(Version(1), data->version.get());
	if (version < data->oldestVersion.get() || version <= 0)
		throw transaction_too_old();
	else if (version <= data->version.get())
		return version;

	if (data->behind && version > data->version.get()) {
		throw process_behind();
	}

	if (deterministicRandom()->random01() < 0.001)
		TraceEvent("WaitForVersion1000x");
	choose {
		when(wait(data->version.whenAtLeast(version))) {
			// FIXME: A bunch of these can block with or without the following delay 0.
			// wait( delay(0) );  // don't do a whole bunch of these at once
			if (version < data->oldestVersion.get())
				throw transaction_too_old();
			return version;
		}
		when(wait(delay(SERVER_KNOBS->FUTURE_VERSION_DELAY))) {
			if (deterministicRandom()->random01() < 0.001)
				TraceEvent(SevWarn, "CacheServerFutureVersion1000x", data->thisServerID)
				    .detail("Version", version)
				    .detail("MyVersion", data->version.get())
				    .detail("ServerID", data->thisServerID);
			throw future_version();
		}
	}
}

ACTOR Future<Version> waitForVersionNoTooOld(StorageCacheData* data, Version version) {
	// This could become an Actor transparently, but for now it just does the lookup
	if (version == latestVersion)
		version = std::max(Version(1), data->version.get());
	if (version <= data->version.get())
		return version;
	choose {
		when(wait(data->version.whenAtLeast(version))) { return version; }
		when(wait(delay(SERVER_KNOBS->FUTURE_VERSION_DELAY))) {
			if (deterministicRandom()->random01() < 0.001)
				TraceEvent(SevWarn, "CacheServerFutureVersion1000x", data->thisServerID)
				    .detail("Version", version)
				    .detail("MyVersion", data->version.get())
				    .detail("ServerID", data->thisServerID);
			throw future_version();
		}
	}
}

ACTOR Future<Void> getValueQ(StorageCacheData* data, GetValueRequest req) {
	state int64_t resultSize = 0;

	try {
		++data->counters.getValueQueries;
		++data->counters.allQueries;
		//++data->readQueueSizeMetric;
		// TODO later
		// data->maxQueryQueue = std::max<int>( data->maxQueryQueue, data->counters.allQueries.getValue() -
		// data->counters.finishedQueries.getValue());

		// Active load balancing runs at a very high priority (to obtain accurate queue lengths)
		// so we need to downgrade here

		// TODO what's this?
		wait(delay(0, TaskPriority::DefaultEndpoint));

		if (req.debugID.present())
			g_traceBatch.addEvent("GetValueDebug",
			                      req.debugID.get().first(),
			                      "getValueQ.DoRead"); //.detail("TaskID", g_network->getCurrentTask());

		state Optional<Value> v;
		state Version version = wait(waitForVersion(data, req.version));
		if (req.debugID.present())
			g_traceBatch.addEvent("GetValueDebug",
			                      req.debugID.get().first(),
			                      "getValueQ.AfterVersion"); //.detail("TaskID", g_network->getCurrentTask());

		if (!data->cachedRangeMap[req.key]) {
			//TraceEvent("WrongCacheServer", data->thisServerID).detail("Key", req.key).detail("Version", version).detail("In", "getValueQ");
			throw wrong_shard_server();
		}

		state int path = 0;
		auto i = data->data().at(version).lastLessOrEqual(req.key);
		if (i && i->isValue() && i.key() == req.key) {
			v = (Value)i->getValue();
			path = 1;
		}

		// debugMutation("CacheGetValue", version, MutationRef(MutationRef::DebugKey, req.key,
		// v.present()?v.get():LiteralStringRef("<null>"))); debugMutation("CacheGetPath", version,
		// MutationRef(MutationRef::DebugKey, req.key,
		// path==0?LiteralStringRef("0"):path==1?LiteralStringRef("1"):LiteralStringRef("2")));

		if (v.present()) {
			++data->counters.rowsQueried;
			resultSize = v.get().size();
			data->counters.bytesQueried += resultSize;
		}

		if (req.debugID.present())
			g_traceBatch.addEvent("GetValueDebug",
			                      req.debugID.get().first(),
			                      "getValueQ.AfterRead"); //.detail("TaskID", g_network->getCurrentTask());

		GetValueReply reply(v);
		req.reply.send(reply);
	} catch (Error& e) {
		if (!canReplyWith(e))
			throw;
		req.reply.sendError(e);
	}

	++data->counters.finishedQueries;
	//--data->readQueueSizeMetric;
	// if(data->latencyBandConfig.present()) {
	//	int maxReadBytes =
	// data->latencyBandConfig.get().readConfig.maxReadBytes.orDefault(std::numeric_limits<int>::max());
	//	data->counters.readLatencyBands.addMeasurement(timer() - req.requestTime(), resultSize > maxReadBytes);
	//}

	return Void();
};

// TODO Implement the reverse readRange
GetKeyValuesReply readRange(StorageCacheData* data, Version version, KeyRangeRef range, int limit, int* pLimitBytes) {
	GetKeyValuesReply result;
	StorageCacheData::VersionedData::ViewAtVersion view = data->data().at(version);
	StorageCacheData::VersionedData::iterator vCurrent = view.end();
	KeyRef readBegin;
	KeyRef rangeBegin = range.begin;
	KeyRef rangeEnd = range.end;

	// We might care about a clear beginning before start that runs into range
	vCurrent = view.lastLessOrEqual(rangeBegin);
	if (vCurrent && vCurrent->isClearTo() && vCurrent->getEndKey() > rangeBegin)
		readBegin = vCurrent->getEndKey();
	else
		readBegin = rangeBegin;

	vCurrent = view.lower_bound(readBegin);
	ASSERT(!vCurrent || vCurrent.key() >= readBegin);
	if (vCurrent) {
		auto b = vCurrent;
		--b;
		ASSERT(!b || b.key() < readBegin);
	}
	int accumulatedBytes = 0;
	while (vCurrent && vCurrent.key() < rangeEnd && limit > 0 && accumulatedBytes < *pLimitBytes) {
		if (!vCurrent->isClearTo()) {
			result.data.push_back_deep(result.arena, KeyValueRef(vCurrent.key(), vCurrent->getValue()));
			accumulatedBytes += sizeof(KeyValueRef) + result.data.end()[-1].expectedSize();
			--limit;
		}
		++vCurrent;
	}

	*pLimitBytes -= accumulatedBytes;
	ASSERT(result.data.size() == 0 || *pLimitBytes + result.data.end()[-1].expectedSize() + sizeof(KeyValueRef) > 0);
	result.more = limit == 0 || *pLimitBytes <= 0; // FIXME: Does this have to be exact?
	result.version = version;
	return result;
}

Key findKey(StorageCacheData* data, KeySelectorRef sel, Version version, KeyRange range, int* pOffset)
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
	bool forward = sel.offset > 0; // If forward, result >= sel.getKey(); else result <= sel.getKey()
	int sign = forward ? +1 : -1;
	bool skipEqualKey = sel.orEqual == forward;
	int distance = forward ? sel.offset : 1 - sel.offset;

	// Don't limit the number of bytes if this is a trivial key selector (there will be at most two items returned from
	// the read range in this case)
	int maxBytes;
	if (sel.offset <= 1 && sel.offset >= 0)
		maxBytes = std::numeric_limits<int>::max();
	else
		maxBytes = BUGGIFY ? SERVER_KNOBS->BUGGIFY_LIMIT_BYTES : SERVER_KNOBS->STORAGE_LIMIT_BYTES;

	GetKeyValuesReply rep =
	    readRange(data,
	              version,
	              forward ? KeyRangeRef(sel.getKey(), range.end) : KeyRangeRef(range.begin, keyAfter(sel.getKey())),
	              (distance + skipEqualKey) * sign,
	              &maxBytes);
	bool more = rep.more && rep.data.size() != distance + skipEqualKey;

	// If we get only one result in the reverse direction as a result of the data being too large, we could get stuck in
	// a loop
	if (more && !forward && rep.data.size() == 1) {
		TEST(true); // Reverse key selector returned only one result in range read
		maxBytes = std::numeric_limits<int>::max();
		GetKeyValuesReply rep2 =
		    readRange(data, version, KeyRangeRef(range.begin, keyAfter(sel.getKey())), -2, &maxBytes);
		rep = rep2;
		more = rep.more && rep.data.size() != distance + skipEqualKey;
		ASSERT(rep.data.size() == 2 || !more);
	}

	int index = distance - 1;
	if (skipEqualKey && rep.data.size() && rep.data[0].key == sel.getKey())
		++index;

	if (index < rep.data.size()) {
		*pOffset = 0;
		return rep.data[index].key;
	} else {
		// FIXME: If range.begin=="" && !forward, return success?
		*pOffset = index - rep.data.size() + 1;
		if (!forward)
			*pOffset = -*pOffset;

		if (more) {
			TEST(true); // Key selector read range had more results

			ASSERT(rep.data.size());
			Key returnKey = forward ? keyAfter(rep.data.back().key) : rep.data.back().key;

			// This is possible if key/value pairs are very large and only one result is returned on a last less than
			// query SOMEDAY: graceful handling of exceptionally sized values
			ASSERT(returnKey != sel.getKey());

			return returnKey;
		} else
			return forward ? range.end : range.begin;
	}
}

KeyRange getCachedKeyRange(StorageCacheData* data, const KeySelectorRef& sel)
// Returns largest range that is cached on this server and selectorInRange(sel, range) or wrong_shard_server if no such
// range exists
{
	auto i = sel.isBackward() ? data->cachedRangeMap.rangeContainingKeyBefore(sel.getKey())
	                          : data->cachedRangeMap.rangeContaining(sel.getKey());
	if (!i->value())
		throw wrong_shard_server();
	ASSERT(selectorInRange(sel, i->range()));
	return i->range();
}

ACTOR Future<Void> getKeyValues(StorageCacheData* data, GetKeyValuesRequest req)
// Throws a wrong_shard_server if the keys in the request or result depend on data outside this server OR if a large
// selector offset prevents all data from being read in one range read
{
	state int64_t resultSize = 0;

	++data->counters.getRangeQueries;
	++data->counters.allQueries;
	//++data->readQueueSizeMetric;
	// data->maxQueryQueue = std::max<int>( data->maxQueryQueue, data->counters.allQueries.getValue() -
	// data->counters.finishedQueries.getValue());

	// Active load balancing runs at a very high priority (to obtain accurate queue lengths)
	// so we need to downgrade here
	TaskPriority taskType = TaskPriority::DefaultEndpoint;
	if (SERVER_KNOBS->FETCH_KEYS_LOWER_PRIORITY && req.isFetchKeys) {
		taskType = TaskPriority::FetchKeys;
		// } else if (false) {
		// 	// Placeholder for up-prioritizing fetches for important requests
		// 	taskType = TaskPriority::DefaultDelay;
	}
	wait(delay(0, taskType));

	try {
		if (req.debugID.present())
			g_traceBatch.addEvent("TransactionDebug", req.debugID.get().first(), "storagecache.getKeyValues.Before");
		state Version version = wait(waitForVersion(data, req.version));

		state KeyRange cachedKeyRange;
		try {
			cachedKeyRange = getCachedKeyRange(data, req.begin);

			if (req.debugID.present())
				g_traceBatch.addEvent(
				    "TransactionDebug", req.debugID.get().first(), "storagecache.getKeyValues.AfterVersion");
			//.detail("ShardBegin", shard.begin).detail("ShardEnd", shard.end);
		} catch (Error& e) {
			TraceEvent("WrongShardServer", data->thisServerID)
			    .detail("Begin", req.begin.toString())
			    .detail("End", req.end.toString())
			    .detail("Version", version)
			    .detail("Shard", "None")
			    .detail("In", "getKeyValues>getShardKeyRange");
			throw e;
		}

		if (!selectorInRange(req.end, cachedKeyRange) &&
		    !(req.end.isFirstGreaterOrEqual() && req.end.getKey() == cachedKeyRange.end)) {
			//			TraceEvent("WrongShardServer1", data->thisServerID).detail("Begin",
			// req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("ShardBegin",
			// shard.begin).detail("ShardEnd", shard.end).detail("In", "getKeyValues>checkShardExtents");
			throw wrong_shard_server();
		}

		state int offset1;
		state int offset2;
		state Key begin = req.begin.isFirstGreaterOrEqual()
		                      ? req.begin.getKey()
		                      : findKey(data, req.begin, version, cachedKeyRange, &offset1);
		state Key end = req.end.isFirstGreaterOrEqual() ? req.end.getKey()
		                                                : findKey(data, req.end, version, cachedKeyRange, &offset2);
		if (req.debugID.present())
			g_traceBatch.addEvent("TransactionDebug", req.debugID.get().first(), "storagecache.getKeyValues.AfterKeys");
		//.detail("Off1",offset1).detail("Off2",offset2).detail("ReqBegin",req.begin.getKey()).detail("ReqEnd",req.end.getKey());

		// Offsets of zero indicate begin/end keys in this cachedKeyRange, which obviously means we can answer the query
		// An end offset of 1 is also OK because the end key is exclusive, so if the first key of the next
		// cachedKeyRange is the end the last actual key returned must be from this cachedKeyRange. A begin offset of 1
		// is also OK because then either begin is past end or equal to end (so the result is definitely empty)
		if ((offset1 && offset1 != 1) || (offset2 && offset2 != 1)) {
			TEST(true); // wrong_cache_server due to offset
			// We could detect when offset1 takes us off the beginning of the database or offset2 takes us off the end,
			// and return a clipped range rather than an error (since that is what the NativeAPI.getRange will do anyway
			// via its "slow path"), but we would have to add some flags to the response to encode whether we went off
			// the beginning and the end, since it needs that information.
			//TraceEvent("WrongShardServer2", data->thisServerID).detail("Begin", req.begin.toString()).detail("End", req.end.toString()).detail("Version", version).detail("ShardBegin", shard.begin).detail("ShardEnd", shard.end).detail("In", "getKeyValues>checkOffsets").detail("BeginKey", begin).detail("EndKey", end).detail("BeginOffset", offset1).detail("EndOffset", offset2);
			throw wrong_shard_server();
		}

		if (begin >= end) {
			if (req.debugID.present())
				g_traceBatch.addEvent("TransactionDebug", req.debugID.get().first(), "storagecache.getKeyValues.Send");
			//.detail("Begin",begin).detail("End",end);

			GetKeyValuesReply none;
			none.version = version;
			none.more = false;
			req.reply.send(none);
		} else {
			state int remainingLimitBytes = req.limitBytes;

			GetKeyValuesReply _r = readRange(data, version, KeyRangeRef(begin, end), req.limit, &remainingLimitBytes);
			GetKeyValuesReply r = _r;

			if (req.debugID.present())
				g_traceBatch.addEvent(
				    "TransactionDebug", req.debugID.get().first(), "storagecache.getKeyValues.AfterReadRange");
			//.detail("Begin",begin).detail("End",end).detail("SizeOf",r.data.size());
			if (EXPENSIVE_VALIDATION) {
				for (int i = 0; i < r.data.size(); i++)
					ASSERT(r.data[i].key >= begin && r.data[i].key < end);
				ASSERT(r.data.size() <= std::abs(req.limit));
			}

			req.reply.send(r);

			resultSize = req.limitBytes - remainingLimitBytes;
			data->counters.bytesQueried += resultSize;
			data->counters.rowsQueried += r.data.size();
		}
	} catch (Error& e) {
		if (!canReplyWith(e))
			throw;
		req.reply.sendError(e);
	}

	++data->counters.finishedQueries;

	return Void();
}

ACTOR Future<Void> getKey(StorageCacheData* data, GetKeyRequest req) {
	state int64_t resultSize = 0;

	++data->counters.getKeyQueries;
	++data->counters.allQueries;

	// Active load balancing runs at a very high priority (to obtain accurate queue lengths)
	// so we need to downgrade here
	wait(delay(0, TaskPriority::DefaultEndpoint));

	try {
		state Version version = wait(waitForVersion(data, req.version));
		state KeyRange cachedKeyRange = getCachedKeyRange(data, req.sel);

		state int offset;
		Key k = findKey(data, req.sel, version, cachedKeyRange, &offset);

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

		GetKeyReply reply(updated);
		req.reply.send(reply);
	} catch (Error& e) {
		if (e.code() == error_code_wrong_shard_server)
			TraceEvent("WrongShardServer").detail("In", "getKey");
		if (!canReplyWith(e))
			throw;
		req.reply.sendError(e);
	}

	++data->counters.finishedQueries;

	return Void();
}

#pragma endregion

bool expandMutation(MutationRef& m, StorageCacheData::VersionedData const& data, KeyRef eagerTrustedEnd, Arena& ar) {
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
			// KeyRef endKeyAtStorageVersion = m.param2 == eagerTrustedEnd ? eagerTrustedEnd : std::min(
			// eager->getKeyEnd( m.param2 ), eagerTrustedEnd );
			// TODO check if the following is correct
			KeyRef endKeyAtStorageVersion = eagerTrustedEnd;
			if (!i || endKeyAtStorageVersion < i.key())
				m.param2 = endKeyAtStorageVersion;
			else if (i->isClearTo())
				m.param2 = i->getEndKey();
			else
				m.param2 = i.key();
		}
	} else if (m.type != MutationRef::SetValue && (m.type)) {

		Optional<StringRef> oldVal;
		auto it = data.atLatest().lastLessOrEqual(m.param1);
		if (it != data.atLatest().end() && it->isValue() && it.key() == m.param1)
			oldVal = it->getValue();
		else if (it != data.atLatest().end() && it->isClearTo() && it->getEndKey() > m.param1) {
			TEST(true); // Atomic op right after a clear.
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
				return expandMutation(m, data, eagerTrustedEnd, ar);
			}
			return false;
		}
		m.type = MutationRef::SetValue;
	}

	return true;
}

// Applies a write mutation (SetValue or ClearRange) to the in-memory versioned data structure
void applyMutation(StorageCacheData* self, MutationRef const& m, Arena& arena, StorageCacheData::VersionedData& data) {
	// m is expected to be in arena already
	// Clear split keys are added to arena

	if (m.type == MutationRef::SetValue) {
		auto prev = data.atLatest().lastLessOrEqual(m.param1);
		if (prev && prev->isClearTo() && prev->getEndKey() > m.param1) {
			ASSERT(prev.key() <= m.param1);
			KeyRef end = prev->getEndKey();
			// TODO double check if the insert version of the previous clear needs to be preserved for the "left half",
			// insert() invalidates prev, so prev.key() is not safe to pass to it by reference
			data.insert(KeyRef(prev.key()),
			            ValueOrClearToRef::clearTo(m.param1),
			            prev.insertVersion()); // overwritten by below insert if empty
			KeyRef nextKey = keyAfter(m.param1, arena);
			if (end != nextKey) {
				ASSERT(end > nextKey);
				// TODO double check if it's okay to let go of the the insert version of the "right half"
				// FIXME: This copy is technically an asymptotic problem, definitely a waste of memory (copy of keyAfter
				// is a waste, but not asymptotic)
				data.insert(nextKey, ValueOrClearToRef::clearTo(KeyRef(arena, end)));
			}
		}
		data.insert(m.param1, ValueOrClearToRef::value(m.param2));
	} else if (m.type == MutationRef::ClearRange) {
		data.erase(m.param1, m.param2);
		ASSERT(m.param2 > m.param1);
		ASSERT(!data.isClearContaining(data.atLatest(), m.param1));
		data.insert(m.param1, ValueOrClearToRef::clearTo(m.param2));
	}
}

template <class T>
void splitMutation(StorageCacheData* data, KeyRangeMap<T>& map, MutationRef const& m, Version ver) {
	if (isSingleKeyMutation((MutationRef::Type)m.type)) {
		auto i = map.rangeContaining(m.param1);
		if (i->value()) // If this key lies in the cached key-range on this server
			data->addMutation(i->range(), ver, m);
	} else if (m.type == MutationRef::ClearRange) {
		KeyRangeRef mKeys(m.param1, m.param2);
		auto r = map.intersectingRanges(mKeys);
		for (auto i = r.begin(); i != r.end(); ++i) {
			if (i->value()) { // if this sub-range exists on this cache server
				KeyRangeRef k = mKeys & i->range();
				data->addMutation(i->range(), ver, MutationRef((MutationRef::Type)m.type, k.begin, k.end));
			}
		}
	} else
		ASSERT(false); // Unknown mutation type in splitMutations
}

void StorageCacheData::addMutation(KeyRangeRef const& cachedKeyRange, Version version, MutationRef const& mutation) {
	MutationRef expanded = mutation;
	auto& mLog = addVersionToMutationLog(version);

	if (!expandMutation(expanded, data(), cachedKeyRange.end, mLog.arena())) {
		return;
	}
	expanded = addMutationToMutationLog(mLog, expanded);
	if (debugMutation("expandedMutation", version, expanded)) {
		const char* type = mutation.type == MutationRef::SetValue        ? "SetValue"
		                   : mutation.type == MutationRef::ClearRange    ? "ClearRange"
		                   : mutation.type == MutationRef::DebugKeyRange ? "DebugKeyRange"
		                   : mutation.type == MutationRef::DebugKey      ? "DebugKey"
		                                                                 : "UnknownMutation";
		printf("DEBUGMUTATION:\t%.6f\t%s\t%s\t%s\t%s\t%s\n",
		       now(),
		       g_network->getLocalAddress().toString().c_str(),
		       "originalMutation",
		       type,
		       printable(mutation.param1).c_str(),
		       printable(mutation.param2).c_str());
		printf("  Cached Key-range: %s - %s\n",
		       printable(cachedKeyRange.begin).c_str(),
		       printable(cachedKeyRange.end).c_str());
	}
	applyMutation(this, expanded, mLog.arena(), mutableData());
	printf("\nSCUpdate: Printing versioned tree after applying mutation\n");
	mutableData().printTree(version);
}

// Helper class for updating the storage cache (i.e. applying mutations)
class StorageCacheUpdater {
public:
	StorageCacheUpdater() : currentVersion(invalidVersion), processedCacheStartKey(false) {}
	StorageCacheUpdater(Version currentVersion) : currentVersion(currentVersion), processedCacheStartKey(false) {}

	void applyMutation(StorageCacheData* data, MutationRef const& m, Version ver) {
		//TraceEvent("SCNewVersion", data->thisServerID).detail("VerWas", data->mutableData().latestVersion).detail("ChVer", ver);

		if (currentVersion != ver) {
			currentVersion = ver;
			data->mutableData().createNewVersion(ver);
		}

		if (m.param1.startsWith(systemKeys.end)) {
			//TraceEvent("PrivateData", data->thisServerID).detail("Mutation", m.toString()).detail("Version", ver);
			applyPrivateCacheData(data, m);
		} else {
			// FIXME: enable when debugMutation is active
			// for(auto m = changes[c].mutations.begin(); m; ++m) {
			//	debugMutation("SCUpdateMutation", changes[c].version, *m);
			//}

			splitMutation(data, data->cachedRangeMap, m, ver);
		}

		// TODO
		if (data->otherError.getFuture().isReady())
			data->otherError.getFuture().get();
	}

	Version currentVersion;

private:
	KeyRef cacheStartKey;
	bool processedCacheStartKey;

	// Applies private mutations, as the name suggests. It's basically establishes the key-ranges
	// that this cache server is responsible for
	// TODO Revisit during failure handling. Might we loose some private mutations?
	void applyPrivateCacheData(StorageCacheData* data, MutationRef const& m) {
		TraceEvent(SevDebug, "SCPrivateCacheMutation", data->thisServerID).detail("Mutation", m.toString());

		if (processedCacheStartKey) {
			// we expect changes in pairs, [begin,end). This mutation is for end key of the range
			ASSERT(m.type == MutationRef::SetValue && m.param1.startsWith(data->ck));
			KeyRangeRef keys(cacheStartKey.removePrefix(data->ck), m.param1.removePrefix(data->ck));
			data->cachedRangeMap.insert(keys, true);
			fprintf(stderr,
			        "SCPrivateCacheMutation: begin: %s, end: %s\n",
			        printable(keys.begin).c_str(),
			        printable(keys.end).c_str());

			processedCacheStartKey = false;
		} else if (m.type == MutationRef::SetValue && m.param1.startsWith(data->ck)) {
			// We expect changes in pairs, [begin,end), This mutation is for start key of the range
			cacheStartKey = m.param1;
			processedCacheStartKey = true;
		} else {
			fprintf(stderr, "SCPrivateCacheMutation: Unknown private mutation\n");
			ASSERT(false); // Unknown private mutation
		}
	}
};

// Compacts the in-memory VersionedMap, i.e. removes versions below the desiredOldestVersion
// TODO revisit if we change the data structure
ACTOR Future<Void> compactCache(StorageCacheData* data) {
	loop {
		// TODO understand this, should we add delay here?
		// if (g_network->isSimulated()) {
		//	double endTime = g_simulator.checkDisabled(format("%s/compactCache",
		// data->thisServerID.toString().c_str())); 	if(endTime > now()) { 		wait(delay(endTime - now(),
		// TaskPriority::CompactCache));
		//	}
		//}

		// Wait until the desiredOldestVersion is greater than the current oldestVersion
		wait(data->desiredOldestVersion.whenAtLeast(data->oldestVersion.get() + 1));
		wait(delay(0, TaskPriority::CompactCache));

		// TODO not really in use as of now. may need in some failure cases. Revisit and remove if no plausible use
		state Promise<Void> compactionInProgress;
		data->compactionInProgress = compactionInProgress.getFuture();
		state Version desiredVersion = data->desiredOldestVersion.get();
		// Call the compaction routine that does the actual work,
		// TODO It's a synchronous function call as of now. Should it asynch?
		data->mutableData().compact(desiredVersion);
		Future<Void> finishedForgetting =
		    data->mutableData().forgetVersionsBeforeAsync(desiredVersion, TaskPriority::CompactCache);
		data->oldestVersion.set(desiredVersion);
		wait(finishedForgetting);
		// TODO how do we yield here? This may not be enough, because compact() does the heavy lifting
		// of compating the VersionedMap. We should probably look into per version compaction and then
		// we can yield after compacting one version
		wait(yield(TaskPriority::CompactCache));

		// TODO what flowlock to acquire during compaction?
		compactionInProgress.send(Void());
		wait(delay(0, TaskPriority::CompactCache)); // Setting compactionInProgess could cause the cache server to shut
		                                            // down, so delay to check for cancellation
	}
}

ACTOR Future<Void> pullAsyncData(StorageCacheData* data) {
	state Future<Void> dbInfoChange = Void();
	state Reference<ILogSystem::IPeekCursor> r;
	state Version tagAt = 0;

	state StorageCacheUpdater updater(data->lastVersionWithData);
	state Version ver = invalidVersion;
	// data->lastTLogVersion = r->getMaxKnownVersion();
	// data->versionLag = std::max<int64_t>(0, data->lastTLogVersion - data->version.get());
	++data->counters.updateBatches;

	loop {
		loop {
			choose {
				when(wait(r ? r->getMore(TaskPriority::TLogCommit) : Never())) { break; }
				when(wait(dbInfoChange)) {
					if (data->logSystem->get())
						r = data->logSystem->get()->peek(
						    data->thisServerID, tagAt, Optional<Version>(), cacheTag, true);
					else
						r = Reference<ILogSystem::IPeekCursor>();
					dbInfoChange = data->logSystem->onChange();
				}
			}
		}
		// FIXME: if the popped version is greater than our last version, we need to clear the cache

		// FIXME: ensure this can only read data from the current version
		r->setProtocolVersion(currentProtocolVersion);

		// Now process the mutations
		for (; r->hasMessage(); r->nextMessage()) {
			ArenaReader& reader = *r->reader();

			MutationRef msg;
			reader >> msg;
			fprintf(stderr, "%lld : %s\n", r->version().version, msg.toString().c_str());

			if (r->version().version > ver && r->version().version > data->version.get()) {
				++data->counters.updateVersions;
				ver = r->version().version;
			}
			if (ver != invalidVersion) // This change belongs to a version < minVersion
			{
				updater.applyMutation(data, msg, ver);
				// TODO
				// mutationBytes += msg.totalSize();
				data->counters.mutationBytes += msg.totalSize();
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
				    .detail("Mutation", msg.toString())
				    .detail("Version", r->version().toString());

			tagAt = r->version().version + 1;
		}

		if (ver != invalidVersion) {
			data->lastVersionWithData = ver;
		} else {
			// TODO double check
			ver = r->version().version - 1;
		}

		if (ver != invalidVersion && ver > data->version.get()) {
			debugKeyRange("SCUpdate", ver, allKeys);

			data->mutableData().createNewVersion(ver);

			// TODO what about otherError
			if (data->otherError.getFuture().isReady())
				data->otherError.getFuture().get();

			// TODO may enable these later
			// data->noRecentUpdates.set(false);
			// data->lastUpdate = now();
			data->version.set(ver); // Triggers replies to waiting gets for new version(s)
			// TODO double check
			// setDataVersion(data->thisServerID, data->version.get());

			// TODO what about otherError
			if (data->otherError.getFuture().isReady())
				data->otherError.getFuture().get();

			// we can get rid of versions beyond maxVerionsInMemory at any point. Update the
			// desiredOldestVersion and that may invoke the compaction actor
			Version maxVersionsInMemory = SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS;
			Version proposedOldestVersion = data->version.get() - maxVersionsInMemory;
			proposedOldestVersion = std::max(proposedOldestVersion, data->oldestVersion.get());
			data->desiredOldestVersion.set(proposedOldestVersion);
		}

		// TODO implement a validate function for the cache
		// validate(data);

		if (r->version().version >= data->lastTLogVersion) {
			if (data->behind) {
				TraceEvent("StorageCacheNoLongerBehind", data->thisServerID)
				    .detail("CursorVersion", r->version().version)
				    .detail("TLogVersion", data->lastTLogVersion);
			}
			data->behind = false;
		}

		tagAt = std::max(tagAt, r->version().version);
	}
}

ACTOR Future<Void> storageCache(StorageServerInterface ssi, uint16_t id, Reference<AsyncVar<ServerDBInfo>> db) {
	state StorageCacheData self(ssi.id(), id);
	state ActorCollection actors(false);
	state Future<Void> dbInfoChange = Void();

	// This helps identify the private mutations meant for this cache server
	self.ck = cacheKeysPrefixFor(id).withPrefix(systemKeys.begin); // FFFF/02cacheKeys/[this server]/

	actors.add(waitFailureServer(ssi.waitFailure.getFuture()));

	// compactCache actor will periodically compact the cache when certain version condityion is met
	actors.add(compactCache(&self));

	// pullAsyncData actor pulls mutations from the TLog and also applies them.
	actors.add(pullAsyncData(&self));
	actors.add(traceRole(Role::STORAGE_CACHE, ssi.id()));

	loop {
		++self.counters.loops;
		choose {
			when(wait(dbInfoChange)) {
				dbInfoChange = db->onChange();
				self.logSystem->set(ILogSystem::fromServerDBInfo(ssi.id(), db->get(), true));
			}
			when(GetValueRequest req = waitNext(ssi.getValue.getFuture())) {
				// TODO do we need to add throttling for cache servers? Probably not
				// actors.add(self->readGuard(req , getValueQ));
				actors.add(getValueQ(&self, req));
			}
			when(WatchValueRequest req = waitNext(ssi.watchValue.getFuture())) { ASSERT(false); }
			when(GetKeyRequest req = waitNext(ssi.getKey.getFuture())) { actors.add(getKey(&self, req)); }
			when(GetKeyValuesRequest req = waitNext(ssi.getKeyValues.getFuture())) {
				actors.add(getKeyValues(&self, req));
			}
			when(GetShardStateRequest req = waitNext(ssi.getShardState.getFuture())) { ASSERT(false); }
			when(StorageQueuingMetricsRequest req = waitNext(ssi.getQueuingMetrics.getFuture())) { ASSERT(false); }
			// when( ReplyPromise<Version> reply = waitNext(ssi.getVersion.getFuture()) ) {
			//	ASSERT(false);
			//}
			when(ReplyPromise<KeyValueStoreType> reply = waitNext(ssi.getKeyValueStoreType.getFuture())) {
				ASSERT(false);
			}
			when(wait(actors.getResult())) {}
		}
	}
}
