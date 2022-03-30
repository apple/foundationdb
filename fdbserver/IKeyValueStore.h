/*
 * IKeyValueStore.h
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

#ifndef FDBSERVER_IKEYVALUESTORE_H
#define FDBSERVER_IKEYVALUESTORE_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbserver/Knobs.h"
#include "fdbclient/StorageCheckpoint.h"

struct CheckpointRequest {
	const Version version; // The FDB version at which the checkpoint is created.
	const KeyRange range; // Keyrange this checkpoint must contain.
	const CheckpointFormat format;
	const UID checkpointID;
	const std::string checkpointDir; // The local directory where the checkpoint file will be created.

	CheckpointRequest(const Version version,
	                  const KeyRange& range,
	                  const CheckpointFormat format,
	                  const UID& id,
	                  const std::string& checkpointDir)
	  : version(version), range(range), format(format), checkpointID(id), checkpointDir(checkpointDir) {}
};

class IClosable {
public:
	// IClosable is a base interface for any disk-backed data structure that needs to support asynchronous errors,
	// shutdown and deletion

	virtual Future<Void> getError()
	    const = 0; // asynchronously throws an error if there is an internal error. Never set
	               // inside (on the stack of) a call to another API function on this object.
	virtual Future<Void> onClosed()
	    const = 0; // the future is set to Void when this is totally shut down after dispose() or
	               // close().  But this function cannot be called after dispose or close!
	virtual void dispose() = 0; // permanently delete the data AND invalidate this interface
	virtual void close() = 0; // invalidate this interface, but do not delete the data.  Outstanding operations may or
	                          // may not take effect in the background.
};

class IKeyValueStore : public IClosable {
public:
	virtual KeyValueStoreType getType() const = 0;
	virtual void set(KeyValueRef keyValue, const Arena* arena = nullptr) = 0;
	virtual void clear(KeyRangeRef range, const Arena* arena = nullptr) = 0;
	virtual Future<Void> canCommit() { return Void(); }
	virtual Future<Void> commit(
	    bool sequential = false) = 0; // returns when prior sets and clears are (atomically) durable

	enum class ReadType {
		EAGER,
		FETCH,
		LOW,
		NORMAL,
		HIGH,
	};

	virtual Future<Optional<Value>> readValue(KeyRef key,
	                                          ReadType type = ReadType::NORMAL,
	                                          Optional<UID> debugID = Optional<UID>()) = 0;

	// Like readValue(), but returns only the first maxLength bytes of the value if it is longer
	virtual Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                                int maxLength,
	                                                ReadType type = ReadType::NORMAL,
	                                                Optional<UID> debugID = Optional<UID>()) = 0;

	// If rowLimit>=0, reads first rows sorted ascending, otherwise reads last rows sorted descending
	// The total size of the returned value (less the last entry) will be less than byteLimit
	virtual Future<RangeResult> readRange(KeyRangeRef keys,
	                                      int rowLimit = 1 << 30,
	                                      int byteLimit = 1 << 30,
	                                      ReadType type = ReadType::NORMAL) = 0;

	// To debug MEMORY_RADIXTREE type ONLY
	// Returns (1) how many key & value pairs have been inserted (2) how many nodes have been created (3) how many
	// key size is less than 12 bytes
	virtual std::tuple<size_t, size_t, size_t> getSize() const { return std::make_tuple(0, 0, 0); }

	// Returns the amount of free and total space for this store, in bytes
	virtual StorageBytes getStorageBytes() const = 0;

	virtual void resyncLog() {}

	virtual void enableSnapshot() {}

	// Create a checkpoint.
	virtual Future<CheckpointMetaData> checkpoint(const CheckpointRequest& request) { throw not_implemented(); }

	// Restore from a checkpoint.
	virtual Future<Void> restore(const std::vector<CheckpointMetaData>& checkpoints) { throw not_implemented(); }

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

protected:
	virtual ~IKeyValueStore() {}
};

extern IKeyValueStore* keyValueStoreSQLite(std::string const& filename,
                                           UID logID,
                                           KeyValueStoreType storeType,
                                           bool checkChecksums = false,
                                           bool checkIntegrity = false);
extern IKeyValueStore* keyValueStoreRedwoodV1(std::string const& filename, UID logID);
extern IKeyValueStore* keyValueStoreRocksDB(std::string const& path,
                                            UID logID,
                                            KeyValueStoreType storeType,
                                            bool checkChecksums = false,
                                            bool checkIntegrity = false);
extern IKeyValueStore* keyValueStoreMemory(std::string const& basename,
                                           UID logID,
                                           int64_t memoryLimit,
                                           std::string ext = "fdq",
                                           KeyValueStoreType storeType = KeyValueStoreType::MEMORY);
extern IKeyValueStore* keyValueStoreLogSystem(class IDiskQueue* queue,
                                              UID logID,
                                              int64_t memoryLimit,
                                              bool disableSnapshot,
                                              bool replaceContent,
                                              bool exactRecovery);

inline IKeyValueStore* openKVStore(KeyValueStoreType storeType,
                                   std::string const& filename,
                                   UID logID,
                                   int64_t memoryLimit,
                                   bool checkChecksums = false,
                                   bool checkIntegrity = false) {
	switch (storeType) {
	case KeyValueStoreType::SSD_BTREE_V1:
		return keyValueStoreSQLite(filename, logID, KeyValueStoreType::SSD_BTREE_V1, false, checkIntegrity);
	case KeyValueStoreType::SSD_BTREE_V2:
		return keyValueStoreSQLite(filename, logID, KeyValueStoreType::SSD_BTREE_V2, checkChecksums, checkIntegrity);
	case KeyValueStoreType::MEMORY:
		return keyValueStoreMemory(filename, logID, memoryLimit);
	case KeyValueStoreType::SSD_REDWOOD_V1:
		return keyValueStoreRedwoodV1(filename, logID);
	case KeyValueStoreType::SSD_ROCKSDB_V1:
		return keyValueStoreRocksDB(filename, logID, storeType);
	case KeyValueStoreType::MEMORY_RADIXTREE:
		return keyValueStoreMemory(filename,
		                           logID,
		                           memoryLimit,
		                           "fdr",
		                           KeyValueStoreType::MEMORY_RADIXTREE); // for radixTree type, set file ext to "fdr"
	default:
		UNREACHABLE();
	}
	UNREACHABLE(); // FIXME: is this right?
}

void GenerateIOLogChecksumFile(std::string filename);
Future<Void> KVFileCheck(std::string const& filename, bool const& integrity);
Future<Void> KVFileDump(std::string const& filename);

#endif
