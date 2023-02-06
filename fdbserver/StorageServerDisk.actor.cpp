/*
 * StorageServerDisk.actor.cpp
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

#include "fdbclient/BlobConnectionProvider.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/MutationTracking.h"
#include "fdbserver/ServerCheckpoint.actor.h"
#include "fdbserver/StorageServer.h"
#include "fdbserver/StorageServerDisk.h"
#include "flow/PriorityMultiLock.actor.h"
#include "flow/actorcompiler.h" // must be last include

FDB_DEFINE_BOOLEAN_PARAM(UnlimitedCommitBytes);

ACTOR static Future<Key> readFirstKey(IKeyValueStore* storage, KeyRangeRef range, Optional<ReadOptions> options) {
	RangeResult r = wait(storage->readRange(range, 1, 1 << 30, options));
	if (r.size())
		return r[0].key;
	else
		return range.end;
}

Future<Key> StorageServerDisk::readNextKeyInclusive(KeyRef key, Optional<ReadOptions> options) {
	++(data->counters.kvScans);
	return readFirstKey(storage, KeyRangeRef(key, allKeys.end), options);
}

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
	}

	auto view = data->tenantMap.atLatest();
	for (auto itr = view.begin(); itr != view.end(); ++itr) {
		storage->set(KeyValueRef(TenantAPI::idToPrefix(itr.key()).withPrefix(persistTenantMapKeys.begin), *itr));
	}
}

void StorageServerDisk::clearRange(KeyRangeRef keys) {
	storage->clear(keys, &data->metrics);
	++(data->counters.kvClearRanges);
	if (keys.singleKeyRange()) {
		++(data->counters.kvClearSingleKey);
	}
}

void StorageServerDisk::writeKeyValue(KeyValueRef kv) {
	storage->set(kv);
	data->counters.kvCommitLogicalBytes += kv.expectedSize();
}

void StorageServerDisk::writeMutation(MutationRef mutation) {
	if (mutation.type == MutationRef::SetValue) {
		storage->set(KeyValueRef(mutation.param1, mutation.param2));
		data->counters.kvCommitLogicalBytes += mutation.expectedSize();
	} else if (mutation.type == MutationRef::ClearRange) {
		storage->clear(KeyRangeRef(mutation.param1, mutation.param2), &data->metrics);
		++(data->counters.kvClearRanges);
		if (KeyRangeRef(mutation.param1, mutation.param2).singleKeyRange()) {
			++(data->counters.kvClearSingleKey);
		}
	} else
		ASSERT(false);
}

void StorageServerDisk::writeMutations(const VectorRef<MutationRef>& mutations,
                                       Version debugVersion,
                                       const char* debugContext) {
	for (const auto& m : mutations) {
		DEBUG_MUTATION(debugContext, debugVersion, m, data->thisServerID);
		if (m.type == MutationRef::SetValue) {
			storage->set(KeyValueRef(m.param1, m.param2));
			data->counters.kvCommitLogicalBytes += m.expectedSize();
		} else if (m.type == MutationRef::ClearRange) {
			storage->clear(KeyRangeRef(m.param1, m.param2), &data->metrics);
			++(data->counters.kvClearRanges);
			if (KeyRangeRef(m.param1, m.param2).singleKeyRange()) {
				++(data->counters.kvClearSingleKey);
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
		writeMutations(v.mutations, v.version, "makeVersionDurable");
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
	data->counters.kvCommitLogicalBytes += persistVersion.expectedSize() + sizeof(Version);

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

Future<Optional<Value>> StorageServerDisk::readValue(KeyRef key, Optional<ReadOptions> options) {
	++(data->counters.kvGets);
	return storage->readValue(key, options);
}

Future<Optional<Value>> StorageServerDisk::readValuePrefix(KeyRef key, int maxLength, Optional<ReadOptions> options) {
	++(data->counters.kvGets);
	return storage->readValuePrefix(key, maxLength, options);
}

Future<RangeResult> StorageServerDisk::readRange(KeyRangeRef keys,
                                                 int rowLimit,
                                                 int byteLimit,
                                                 Optional<ReadOptions> options) {
	++(data->counters.kvScans);
	return storage->readRange(keys, rowLimit, byteLimit, options);
}
