#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbserver/IKeyValueStore.h"

FDB_DECLARE_BOOLEAN_PARAM(UnlimitedCommitBytes);

struct StorageServerDisk {
	explicit StorageServerDisk(struct StorageServer* data, IKeyValueStore* storage) : data(data), storage(storage) {}

	void makeNewStorageServerDurable(const bool shardAware);
	bool makeVersionMutationsDurable(Version& prevStorageVersion,
	                                 Version newStorageVersion,
	                                 int64_t& bytesLeft,
	                                 UnlimitedCommitBytes unlimitedCommitBytes);
	void makeVersionDurable(Version version);
	void makeTssQuarantineDurable();
	Future<bool> restoreDurableState();

	void changeLogProtocol(Version version, ProtocolVersion protocol);

	void writeMutation(MutationRef mutation);
	void writeKeyValue(KeyValueRef kv);
	void clearRange(KeyRangeRef keys);

	Future<Void> addRange(KeyRangeRef range, std::string id) { return storage->addRange(range, id); }

	std::vector<std::string> removeRange(KeyRangeRef range) { return storage->removeRange(range); }

	void persistRangeMapping(KeyRangeRef range, bool isAdd) { storage->persistRangeMapping(range, isAdd); }

	Future<Void> getError() { return storage->getError(); }
	Future<Void> init() { return storage->init(); }
	Future<Void> canCommit() { return storage->canCommit(); }
	Future<Void> commit() { return storage->commit(); }

	// SOMEDAY: Put readNextKeyInclusive in IKeyValueStore
	// Read the key that is equal or greater then 'key' from the storage engine.
	// For example, readNextKeyInclusive("a") should return:
	//  - "a", if key "a" exist
	//  - "b", if key "a" doesn't exist, and "b" is the next existing key in total order
	//  - allKeys.end, if keyrange [a, allKeys.end) is empty
	Future<Key> readNextKeyInclusive(KeyRef key, Optional<ReadOptions> options = Optional<ReadOptions>());
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

	Future<Void> deleteCheckpoint(const CheckpointMetaData& checkpoint) {
		return storage->deleteCheckpoint(checkpoint);
	}

	KeyValueStoreType getKeyValueStoreType() const { return storage->getType(); }
	StorageBytes getStorageBytes() const { return storage->getStorageBytes(); }
	std::tuple<size_t, size_t, size_t> getSize() const { return storage->getSize(); }

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
};
