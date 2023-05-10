/*
 * IKeyValueStore.actor.cpp
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

#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbclient/IKeyValueStore.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

IKeyValueStore* openKVStore(KeyValueStoreType storeType,
                            std::string const& filename,
                            UID logID,
                            int64_t memoryLimit,
                            bool checkChecksums,
                            bool checkIntegrity,
                            bool openRemotely,
                            Reference<AsyncVar<ServerDBInfo> const> db,
                            Optional<EncryptionAtRestMode> encryptionMode) {
	// Only Redwood support encryption currently.
	if (encryptionMode.present() && encryptionMode.get().isEncryptionEnabled() &&
	    storeType != KeyValueStoreType::SSD_REDWOOD_V1) {
		TraceEvent(SevWarn, "KVStoreTypeNotSupportingEncryption")
		    .detail("KVStoreType", storeType)
		    .detail("EncryptionMode", encryptionMode);
		throw encrypt_mode_mismatch();
	}
	if (openRemotely) {
		return openRemoteKVStore(storeType, filename, logID, memoryLimit, checkChecksums, checkIntegrity);
	}
	switch (storeType) {
	case KeyValueStoreType::SSD_BTREE_V1:
		return keyValueStoreSQLite(filename, logID, KeyValueStoreType::SSD_BTREE_V1, false, checkIntegrity);
	case KeyValueStoreType::SSD_BTREE_V2:
		return keyValueStoreSQLite(filename, logID, KeyValueStoreType::SSD_BTREE_V2, checkChecksums, checkIntegrity);
	case KeyValueStoreType::MEMORY:
		return keyValueStoreMemory(filename, logID, memoryLimit);
	case KeyValueStoreType::SSD_REDWOOD_V1:
		return keyValueStoreRedwoodV1(filename, logID, db, encryptionMode);
	case KeyValueStoreType::SSD_ROCKSDB_V1:
		return keyValueStoreRocksDB(filename, logID, storeType);
	case KeyValueStoreType::SSD_SHARDED_ROCKSDB:
		return keyValueStoreShardedRocksDB(filename, logID, storeType, checkChecksums, checkIntegrity);
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

// Default implementation for replaceRange(), which writes the key one by one.
Future<Void> IKeyValueStore::replaceRange(KeyRange range, Standalone<VectorRef<KeyValueRef>> data) {
	return replaceRange_impl(this, range, data);
}
