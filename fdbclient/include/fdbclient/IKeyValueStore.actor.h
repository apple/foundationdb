/*
 * IKeyValueStore.actor.h
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

#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_IKEYVALUESTORE_ACTOR_G_H)
#define FDBCLIENT_IKEYVALUESTORE_ACTOR_G_H
#include "fdbclient/IKeyValueStore.actor.g.h"
#elif !defined(FDBCLIENT_IKEYVALUESTORE_ACTOR_H)
#define FDBCLIENT_IKEYVALUESTORE_ACTOR_H

#include "flow/Trace.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/StorageCheckpoint.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/IClosable.h"
#include "fdbclient/KeyRangeMap.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct CheckpointRequest {
	const Version version; // The FDB version at which the checkpoint is created.
	const std::vector<KeyRange> ranges; // Keyranges this checkpoint must contain.
	const CheckpointFormat format;
	const UID checkpointID;
	const std::string checkpointDir; // The local directory where the checkpoint file will be created.

	CheckpointRequest(const Version version,
	                  const std::vector<KeyRange>& ranges,
	                  const CheckpointFormat format,
	                  const UID& id,
	                  const std::string& checkpointDir)
	  : version(version), ranges(ranges), format(format), checkpointID(id), checkpointDir(checkpointDir) {}
};

ACTOR static Future<Void> replaceRange_impl(class IKeyValueStore* self,
                                            KeyRange range,
                                            Standalone<VectorRef<KeyValueRef>> data);

class IKeyValueStore : public IClosable {
public:
	virtual KeyValueStoreType getType() const = 0;
	// Returns true if the KV store supports shards, i.e., implements addRange(), removeRange(), and
	// persistRangeMapping().
	virtual bool shardAware() const { return false; }
	virtual void set(KeyValueRef keyValue, const Arena* arena = nullptr) = 0;
	virtual void clear(KeyRangeRef range, const Arena* arena = nullptr) = 0;
	virtual Future<Void> canCommit() { return Void(); }
	virtual Future<Void> commit(
	    bool sequential = false) = 0; // returns when prior sets and clears are (atomically) durable

	virtual Future<Optional<Value>> readValue(KeyRef key, Optional<ReadOptions> options = Optional<ReadOptions>()) = 0;

	// Like readValue(), but returns only the first maxLength bytes of the value if it is longer
	virtual Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                                int maxLength,
	                                                Optional<ReadOptions> options = Optional<ReadOptions>()) = 0;

	// If rowLimit>=0, reads first rows sorted ascending, otherwise reads last rows sorted descending
	// The total size of the returned value (less the last entry) will be less than byteLimit
	virtual Future<RangeResult> readRange(KeyRangeRef keys,
	                                      int rowLimit = 1 << 30,
	                                      int byteLimit = 1 << 30,
	                                      Optional<ReadOptions> options = Optional<ReadOptions>()) = 0;

	// Shard management APIs.
	// Adds key range to a physical shard.
	virtual Future<Void> addRange(KeyRangeRef range, std::string id, bool active = true) { return Void(); }

	// Creates new physical shards for key ranges.
	virtual Future<Void> addRanges(std::vector<std::pair<KeyRange, std::string>> ranges, bool active = true) {
		return Void();
	}

	// Removes a key range from KVS and returns a list of empty physical shards after the removal.
	virtual std::vector<std::string> removeRange(KeyRangeRef range) { return std::vector<std::string>(); }

	// Replace the specified range, the default implementation ignores `blockRange` and writes the key one by one.
	virtual Future<Void> replaceRange(KeyRange range, Standalone<VectorRef<KeyValueRef>> data) {
		return replaceRange_impl(this, range, data);
	}

	// Marks a key range as active and prepares it for future read.
	virtual void markRangeAsActive(KeyRangeRef range) {}

	// Persists key range and physical shard mapping.
	virtual void persistRangeMapping(KeyRangeRef range, bool isAdd) {}

	// Returns key range to physical shard mapping.
	virtual CoalescedKeyRangeMap<std::string> getExistingRanges() { throw not_implemented(); }

	// To debug MEMORY_RADIXTREE type ONLY
	// Returns (1) how many key & value pairs have been inserted (2) how many nodes have been created (3) how many
	// key size is less than 12 bytes
	virtual std::tuple<size_t, size_t, size_t> getSize() const { return std::make_tuple(0, 0, 0); }

	// Returns the amount of free and total space for this store, in bytes
	virtual StorageBytes getStorageBytes() const = 0;

	virtual void logRecentRocksDBBackgroundWorkStats(UID ssId, std::string logReason) { throw not_implemented(); }

	virtual void resyncLog() {}

	virtual void enableSnapshot() {}

	// Create a checkpoint.
	virtual Future<CheckpointMetaData> checkpoint(const CheckpointRequest& request) { throw not_implemented(); }

	// Restore from a checkpoint.
	virtual Future<Void> restore(const std::vector<CheckpointMetaData>& checkpoints) { throw not_implemented(); }

	// Same as above, with a target shardId, and a list of target ranges, ranges must be a subset of the checkpoint
	// ranges.
	virtual Future<Void> restore(const std::string& shardId,
	                             const std::vector<KeyRange>& ranges,
	                             const std::vector<CheckpointMetaData>& checkpoints) {
		throw not_implemented();
	}

	// Delete a checkpoint.
	virtual Future<Void> deleteCheckpoint(const CheckpointMetaData& checkpoint) { throw not_implemented(); }

	/*
	Concurrency contract
	    Causal consistency:
	        A read which begins after a commit ends sees the effects of the commit.
	        A read which ends before a commit begins does not see the effects of the commit.

	    Thus, a read returns a version as of a call to commit which began before the read ends such that no subsequent
	commit ended before the read begins:

	        commit()		// can't be this version (subsequent commit ends before read begins)
	            endcommit()
	        commit()		// could be this or any later version (no subsequent commit ends before read begins)
	            endcommit()
	        commit()
	        read()
	*/
	// `init()` MUST be idempotent as it will be called more than once on a KeyValueStore in case
	// of a rollback.
	virtual Future<Void> init() { return Void(); }

	// Obtain the encryption mode of the storage. The encryption mode needs to match the encryption mode of the cluster.
	virtual Future<EncryptionAtRestMode> encryptionMode() = 0;

protected:
	virtual ~IKeyValueStore() {}
};

ACTOR static Future<Void> replaceRange_impl(IKeyValueStore* self,
                                            KeyRange range,
                                            Standalone<VectorRef<KeyValueRef>> data) {
	state int sinceYield = 0;
	state const KeyValueRef* kvItr = data.begin();
	state KeyRangeRef rangeRef = range;
	if (rangeRef.empty()) {
		return Void();
	}
	self->clear(rangeRef);
	for (; kvItr != data.end(); kvItr++) {
		self->set(*kvItr);
		if (++sinceYield > 1000) {
			wait(yield());
			sinceYield = 0;
		}
	}
	return Void();
}

#include "flow/unactorcompiler.h"
#endif
