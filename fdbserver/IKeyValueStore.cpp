/*
 * IKeyValueStore.cpp
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
#include "fdbserver/IKeyValueStore.h"
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
                            Optional<EncryptionAtRestMode> encryptionMode,
                            int64_t pageCacheBytes) {
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
		return keyValueStoreRedwoodV1(filename, logID, db, encryptionMode, pageCacheBytes);
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