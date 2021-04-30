/*
 * MultiVersionTransaction.actor.cpp
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

#include <limits.h>

#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/MultiVersionAssignmentVars.h"
#include "fdbclient/ThreadSafeTransaction.h"

#include "flow/Platform.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // This must be the last #include.

void throwIfError(FdbCApi::fdb_error_t e) {
	if (e) {
		throw Error(e);
	}
}

// DLTransaction
void DLTransaction::cancel() {
	api->transactionCancel(tr);
}

void DLTransaction::setVersion(Version v) {
	api->transactionSetReadVersion(tr, v);
}

ThreadFuture<Version> DLTransaction::getReadVersion() {
	FdbCApi::FDBFuture* f = api->transactionGetReadVersion(tr);

	return toThreadFuture<Version>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		int64_t version;
		FdbCApi::fdb_error_t error = api->futureGetInt64(f, &version);
		ASSERT(!error);
		return version;
	});
}

ThreadFuture<Optional<Value>> DLTransaction::get(const KeyRef& key, bool snapshot) {
	FdbCApi::FDBFuture* f = api->transactionGet(tr, key.begin(), key.size(), snapshot);

	return toThreadFuture<Optional<Value>>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		FdbCApi::fdb_bool_t present;
		const uint8_t* value;
		int valueLength;
		FdbCApi::fdb_error_t error = api->futureGetValue(f, &present, &value, &valueLength);
		ASSERT(!error);
		if (present) {
			// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
			return Optional<Value>(Value(ValueRef(value, valueLength), Arena()));
		} else {
			return Optional<Value>();
		}
	});
}

ThreadFuture<Key> DLTransaction::getKey(const KeySelectorRef& key, bool snapshot) {
	FdbCApi::FDBFuture* f =
	    api->transactionGetKey(tr, key.getKey().begin(), key.getKey().size(), key.orEqual, key.offset, snapshot);

	return toThreadFuture<Key>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const uint8_t* key;
		int keyLength;
		FdbCApi::fdb_error_t error = api->futureGetKey(f, &key, &keyLength);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Key(KeyRef(key, keyLength), Arena());
	});
}

ThreadFuture<Standalone<RangeResultRef>> DLTransaction::getRange(const KeySelectorRef& begin,
                                                                 const KeySelectorRef& end,
                                                                 int limit,
                                                                 bool snapshot,
                                                                 bool reverse) {
	return getRange(begin, end, GetRangeLimits(limit), snapshot, reverse);
}

ThreadFuture<Standalone<RangeResultRef>> DLTransaction::getRange(const KeySelectorRef& begin,
                                                                 const KeySelectorRef& end,
                                                                 GetRangeLimits limits,
                                                                 bool snapshot,
                                                                 bool reverse) {
	FdbCApi::FDBFuture* f = api->transactionGetRange(tr,
	                                                 begin.getKey().begin(),
	                                                 begin.getKey().size(),
	                                                 begin.orEqual,
	                                                 begin.offset,
	                                                 end.getKey().begin(),
	                                                 end.getKey().size(),
	                                                 end.orEqual,
	                                                 end.offset,
	                                                 limits.rows,
	                                                 limits.bytes,
	                                                 FDBStreamingModes::EXACT,
	                                                 0,
	                                                 snapshot,
	                                                 reverse);
	return toThreadFuture<Standalone<RangeResultRef>>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const FdbCApi::FDBKeyValue* kvs;
		int count;
		FdbCApi::fdb_bool_t more;
		FdbCApi::fdb_error_t error = api->futureGetKeyValueArray(f, &kvs, &count, &more);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Standalone<RangeResultRef>(RangeResultRef(VectorRef<KeyValueRef>((KeyValueRef*)kvs, count), more),
		                                  Arena());
	});
}

ThreadFuture<Standalone<RangeResultRef>> DLTransaction::getRange(const KeyRangeRef& keys,
                                                                 int limit,
                                                                 bool snapshot,
                                                                 bool reverse) {
	return getRange(
	    firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), GetRangeLimits(limit), snapshot, reverse);
}

ThreadFuture<Standalone<RangeResultRef>> DLTransaction::getRange(const KeyRangeRef& keys,
                                                                 GetRangeLimits limits,
                                                                 bool snapshot,
                                                                 bool reverse) {
	return getRange(firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), limits, snapshot, reverse);
}

ThreadFuture<Standalone<VectorRef<const char*>>> DLTransaction::getAddressesForKey(const KeyRef& key) {
	FdbCApi::FDBFuture* f = api->transactionGetAddressesForKey(tr, key.begin(), key.size());

	return toThreadFuture<Standalone<VectorRef<const char*>>>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const char** addresses;
		int count;
		FdbCApi::fdb_error_t error = api->futureGetStringArray(f, &addresses, &count);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Standalone<VectorRef<const char*>>(VectorRef<const char*>(addresses, count), Arena());
	});
}

ThreadFuture<Standalone<StringRef>> DLTransaction::getVersionstamp() {
	if (!api->transactionGetVersionstamp) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->transactionGetVersionstamp(tr);

	return toThreadFuture<Standalone<StringRef>>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const uint8_t* str;
		int strLength;
		FdbCApi::fdb_error_t error = api->futureGetKey(f, &str, &strLength);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Standalone<StringRef>(StringRef(str, strLength), Arena());
	});
}

ThreadFuture<int64_t> DLTransaction::getEstimatedRangeSizeBytes(const KeyRangeRef& keys) {
	if (!api->transactionGetEstimatedRangeSizeBytes) {
		return unsupported_operation();
	}
	FdbCApi::FDBFuture* f = api->transactionGetEstimatedRangeSizeBytes(
	    tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size());

	return toThreadFuture<int64_t>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		int64_t sampledSize;
		FdbCApi::fdb_error_t error = api->futureGetInt64(f, &sampledSize);
		ASSERT(!error);
		return sampledSize;
	});
}

void DLTransaction::addReadConflictRange(const KeyRangeRef& keys) {
	throwIfError(api->transactionAddConflictRange(
	    tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size(), FDBConflictRangeTypes::READ));
}

void DLTransaction::atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) {
	api->transactionAtomicOp(
	    tr, key.begin(), key.size(), value.begin(), value.size(), (FDBMutationTypes::Option)operationType);
}

void DLTransaction::set(const KeyRef& key, const ValueRef& value) {
	api->transactionSet(tr, key.begin(), key.size(), value.begin(), value.size());
}

void DLTransaction::clear(const KeyRef& begin, const KeyRef& end) {
	api->transactionClearRange(tr, begin.begin(), begin.size(), end.begin(), end.size());
}

void DLTransaction::clear(const KeyRangeRef& range) {
	api->transactionClearRange(tr, range.begin.begin(), range.begin.size(), range.end.begin(), range.end.size());
}

void DLTransaction::clear(const KeyRef& key) {
	api->transactionClear(tr, key.begin(), key.size());
}

ThreadFuture<Void> DLTransaction::watch(const KeyRef& key) {
	FdbCApi::FDBFuture* f = api->transactionWatch(tr, key.begin(), key.size());

	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) { return Void(); });
}

void DLTransaction::addWriteConflictRange(const KeyRangeRef& keys) {
	throwIfError(api->transactionAddConflictRange(
	    tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size(), FDBConflictRangeTypes::WRITE));
}

ThreadFuture<Void> DLTransaction::commit() {
	FdbCApi::FDBFuture* f = api->transactionCommit(tr);

	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) { return Void(); });
}

Version DLTransaction::getCommittedVersion() {
	int64_t version;
	throwIfError(api->transactionGetCommittedVersion(tr, &version));
	return version;
}

ThreadFuture<int64_t> DLTransaction::getApproximateSize() {
	if (!api->transactionGetApproximateSize) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->transactionGetApproximateSize(tr);
	return toThreadFuture<int64_t>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		int64_t size = 0;
		FdbCApi::fdb_error_t error = api->futureGetInt64(f, &size);
		ASSERT(!error);
		return size;
	});
}

void DLTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	throwIfError(api->transactionSetOption(
	    tr, option, value.present() ? value.get().begin() : NULL, value.present() ? value.get().size() : 0));
}

ThreadFuture<Void> DLTransaction::onError(Error const& e) {
	FdbCApi::FDBFuture* f = api->transactionOnError(tr, e.code());

	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) { return Void(); });
}

void DLTransaction::reset() {
	api->transactionReset(tr);
}

// DLDatabase
DLDatabase::DLDatabase(Reference<FdbCApi> api, ThreadFuture<FdbCApi::FDBDatabase*> dbFuture) : api(api), db(nullptr) {
	addref();
	ready = mapThreadFuture<FdbCApi::FDBDatabase*, Void>(dbFuture, [this](ErrorOr<FdbCApi::FDBDatabase*> db) {
		if (db.isError()) {
			delref();
			return ErrorOr<Void>(db.getError());
		}

		this->db = db.get();
		delref();
		return ErrorOr<Void>(Void());
	});
}

ThreadFuture<Void> DLDatabase::onReady() {
	return ready;
}

Reference<ITransaction> DLDatabase::createTransaction() {
	FdbCApi::FDBTransaction* tr;
	api->databaseCreateTransaction(db, &tr);
	return Reference<ITransaction>(new DLTransaction(api, tr));
}

void DLDatabase::setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	throwIfError(api->databaseSetOption(
	    db, option, value.present() ? value.get().begin() : NULL, value.present() ? value.get().size() : 0));
}

// Returns the protocol version reported by the coordinator this client is connected to
// If an expected version is given, the future won't return until the protocol version is different than expected
// Note: this will never return if the server is running a protocol from FDB 5.0 or older
ThreadFuture<ProtocolVersion> DLDatabase::getServerProtocol(Optional<ProtocolVersion> expectedVersion) {
	// Version 6.3 doesn't support getting the server protocol from an external client
	ASSERT(false);
	throw internal_error();
}

// DLApi
template <class T>
void loadClientFunction(T* fp, void* lib, std::string libPath, const char* functionName, bool requireFunction = true) {
	*(void**)(fp) = loadFunction(lib, functionName);
	if (*fp == NULL && requireFunction) {
		TraceEvent(SevError, "ErrorLoadingFunction").detail("LibraryPath", libPath).detail("Function", functionName);
		throw platform_error();
	}
}

DLApi::DLApi(std::string fdbCPath, bool unlinkOnLoad)
  : api(new FdbCApi()), fdbCPath(fdbCPath), unlinkOnLoad(unlinkOnLoad), networkSetup(false) {}

void DLApi::init() {
	if (isLibraryLoaded(fdbCPath.c_str())) {
		throw external_client_already_loaded();
	}

	void* lib = loadLibrary(fdbCPath.c_str());
	if (lib == NULL) {
		TraceEvent(SevError, "ErrorLoadingExternalClientLibrary").detail("LibraryPath", fdbCPath);
		throw platform_error();
	}
	if (unlinkOnLoad) {
		int err = unlink(fdbCPath.c_str());
		if (err) {
			TraceEvent(SevError, "ErrorUnlinkingTempClientLibraryFile").GetLastError().detail("LibraryPath", fdbCPath);
			throw platform_error();
		}
	}

	loadClientFunction(&api->selectApiVersion, lib, fdbCPath, "fdb_select_api_version_impl");
	loadClientFunction(&api->getClientVersion, lib, fdbCPath, "fdb_get_client_version", headerVersion >= 410);
	loadClientFunction(&api->setNetworkOption, lib, fdbCPath, "fdb_network_set_option");
	loadClientFunction(&api->setupNetwork, lib, fdbCPath, "fdb_setup_network");
	loadClientFunction(&api->runNetwork, lib, fdbCPath, "fdb_run_network");
	loadClientFunction(&api->stopNetwork, lib, fdbCPath, "fdb_stop_network");
	loadClientFunction(&api->createDatabase, lib, fdbCPath, "fdb_create_database", headerVersion >= 610);

	loadClientFunction(&api->databaseCreateTransaction, lib, fdbCPath, "fdb_database_create_transaction");
	loadClientFunction(&api->databaseSetOption, lib, fdbCPath, "fdb_database_set_option");
	loadClientFunction(&api->databaseDestroy, lib, fdbCPath, "fdb_database_destroy");

	loadClientFunction(&api->transactionSetOption, lib, fdbCPath, "fdb_transaction_set_option");
	loadClientFunction(&api->transactionDestroy, lib, fdbCPath, "fdb_transaction_destroy");
	loadClientFunction(&api->transactionSetReadVersion, lib, fdbCPath, "fdb_transaction_set_read_version");
	loadClientFunction(&api->transactionGetReadVersion, lib, fdbCPath, "fdb_transaction_get_read_version");
	loadClientFunction(&api->transactionGet, lib, fdbCPath, "fdb_transaction_get");
	loadClientFunction(&api->transactionGetKey, lib, fdbCPath, "fdb_transaction_get_key");
	loadClientFunction(&api->transactionGetAddressesForKey, lib, fdbCPath, "fdb_transaction_get_addresses_for_key");
	loadClientFunction(&api->transactionGetRange, lib, fdbCPath, "fdb_transaction_get_range");
	loadClientFunction(
	    &api->transactionGetVersionstamp, lib, fdbCPath, "fdb_transaction_get_versionstamp", headerVersion >= 410);
	loadClientFunction(&api->transactionSet, lib, fdbCPath, "fdb_transaction_set");
	loadClientFunction(&api->transactionClear, lib, fdbCPath, "fdb_transaction_clear");
	loadClientFunction(&api->transactionClearRange, lib, fdbCPath, "fdb_transaction_clear_range");
	loadClientFunction(&api->transactionAtomicOp, lib, fdbCPath, "fdb_transaction_atomic_op");
	loadClientFunction(&api->transactionCommit, lib, fdbCPath, "fdb_transaction_commit");
	loadClientFunction(&api->transactionGetCommittedVersion, lib, fdbCPath, "fdb_transaction_get_committed_version");
	loadClientFunction(&api->transactionGetApproximateSize,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_get_approximate_size",
	                   headerVersion >= 620);
	loadClientFunction(&api->transactionWatch, lib, fdbCPath, "fdb_transaction_watch");
	loadClientFunction(&api->transactionOnError, lib, fdbCPath, "fdb_transaction_on_error");
	loadClientFunction(&api->transactionReset, lib, fdbCPath, "fdb_transaction_reset");
	loadClientFunction(&api->transactionCancel, lib, fdbCPath, "fdb_transaction_cancel");
	loadClientFunction(&api->transactionAddConflictRange, lib, fdbCPath, "fdb_transaction_add_conflict_range");
	loadClientFunction(&api->transactionGetEstimatedRangeSizeBytes,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_get_estimated_range_size_bytes",
	                   headerVersion >= 630);

	loadClientFunction(
	    &api->futureGetInt64, lib, fdbCPath, headerVersion >= 620 ? "fdb_future_get_int64" : "fdb_future_get_version");
	loadClientFunction(&api->futureGetError, lib, fdbCPath, "fdb_future_get_error");
	loadClientFunction(&api->futureGetKey, lib, fdbCPath, "fdb_future_get_key");
	loadClientFunction(&api->futureGetValue, lib, fdbCPath, "fdb_future_get_value");
	loadClientFunction(&api->futureGetStringArray, lib, fdbCPath, "fdb_future_get_string_array");
	loadClientFunction(&api->futureGetKeyValueArray, lib, fdbCPath, "fdb_future_get_keyvalue_array");
	loadClientFunction(&api->futureSetCallback, lib, fdbCPath, "fdb_future_set_callback");
	loadClientFunction(&api->futureCancel, lib, fdbCPath, "fdb_future_cancel");
	loadClientFunction(&api->futureDestroy, lib, fdbCPath, "fdb_future_destroy");

	loadClientFunction(&api->futureGetDatabase, lib, fdbCPath, "fdb_future_get_database", headerVersion < 610);
	loadClientFunction(&api->createCluster, lib, fdbCPath, "fdb_create_cluster", headerVersion < 610);
	loadClientFunction(&api->clusterCreateDatabase, lib, fdbCPath, "fdb_cluster_create_database", headerVersion < 610);
	loadClientFunction(&api->clusterDestroy, lib, fdbCPath, "fdb_cluster_destroy", headerVersion < 610);
	loadClientFunction(&api->futureGetCluster, lib, fdbCPath, "fdb_future_get_cluster", headerVersion < 610);
}

void DLApi::selectApiVersion(int apiVersion) {
	// External clients must support at least this version
	// Versions newer than what we understand are rejected in the C bindings
	headerVersion = std::max(apiVersion, 400);

	init();
	throwIfError(api->selectApiVersion(apiVersion, headerVersion));
	throwIfError(api->setNetworkOption(FDBNetworkOptions::EXTERNAL_CLIENT, NULL, 0));
}

const char* DLApi::getClientVersion() {
	if (!api->getClientVersion) {
		return "unknown";
	}

	return api->getClientVersion();
}

void DLApi::setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	throwIfError(api->setNetworkOption(
	    option, value.present() ? value.get().begin() : NULL, value.present() ? value.get().size() : 0));
}

void DLApi::setupNetwork() {
	networkSetup = true;
	throwIfError(api->setupNetwork());
}

void DLApi::runNetwork() {
	auto e = api->runNetwork();

	for (auto& hook : threadCompletionHooks) {
		try {
			hook.first(hook.second);
		} catch (Error& e) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(e);
		} catch (...) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(unknown_error());
		}
	}

	throwIfError(e);
}

void DLApi::stopNetwork() {
	if (networkSetup) {
		throwIfError(api->stopNetwork());
	}
}

Reference<IDatabase> DLApi::createDatabase609(const char* clusterFilePath) {
	FdbCApi::FDBFuture* f = api->createCluster(clusterFilePath);

	auto clusterFuture = toThreadFuture<FdbCApi::FDBCluster*>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		FdbCApi::FDBCluster* cluster;
		api->futureGetCluster(f, &cluster);
		return cluster;
	});

	Reference<FdbCApi> innerApi = api;
	auto dbFuture = flatMapThreadFuture<FdbCApi::FDBCluster*, FdbCApi::FDBDatabase*>(
	    clusterFuture, [innerApi](ErrorOr<FdbCApi::FDBCluster*> cluster) {
		    if (cluster.isError()) {
			    return ErrorOr<ThreadFuture<FdbCApi::FDBDatabase*>>(cluster.getError());
		    }

		    auto innerDbFuture =
		        toThreadFuture<FdbCApi::FDBDatabase*>(innerApi,
		                                              innerApi->clusterCreateDatabase(cluster.get(), (uint8_t*)"DB", 2),
		                                              [](FdbCApi::FDBFuture* f, FdbCApi* api) {
			                                              FdbCApi::FDBDatabase* db;
			                                              api->futureGetDatabase(f, &db);
			                                              return db;
		                                              });

		    return ErrorOr<ThreadFuture<FdbCApi::FDBDatabase*>>(
		        mapThreadFuture<FdbCApi::FDBDatabase*, FdbCApi::FDBDatabase*>(
		            innerDbFuture, [cluster, innerApi](ErrorOr<FdbCApi::FDBDatabase*> db) {
			            innerApi->clusterDestroy(cluster.get());
			            return db;
		            }));
	    });

	return Reference<DLDatabase>(new DLDatabase(api, dbFuture));
}

Reference<IDatabase> DLApi::createDatabase(const char* clusterFilePath) {
	if (headerVersion >= 610) {
		FdbCApi::FDBDatabase* db;
		api->createDatabase(clusterFilePath, &db);
		return Reference<IDatabase>(new DLDatabase(api, db));
	} else {
		return DLApi::createDatabase609(clusterFilePath);
	}
}

void DLApi::addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) {
	MutexHolder holder(lock);
	threadCompletionHooks.push_back(std::make_pair(hook, hookParameter));
}

// MultiVersionTransaction
MultiVersionTransaction::MultiVersionTransaction(Reference<MultiVersionDatabase> db,
                                                 UniqueOrderedOptionList<FDBTransactionOptions> defaultOptions)
  : db(db) {
	setDefaultOptions(defaultOptions);
	updateTransaction();
}

void MultiVersionTransaction::setDefaultOptions(UniqueOrderedOptionList<FDBTransactionOptions> options) {
	MutexHolder holder(db->dbState->optionLock);
	std::copy(options.begin(), options.end(), std::back_inserter(persistentOptions));
}

void MultiVersionTransaction::updateTransaction() {
	auto currentDb = db->dbState->dbVar->get();

	TransactionInfo newTr;
	if (currentDb.value) {
		newTr.transaction = currentDb.value->createTransaction();

		Optional<StringRef> timeout;
		for (auto option : persistentOptions) {
			if (option.first == FDBTransactionOptions::TIMEOUT) {
				timeout = option.second.castTo<StringRef>();
			} else {
				newTr.transaction->setOption(option.first, option.second.castTo<StringRef>());
			}
		}

		// Setting a timeout can immediately cause a transaction to fail. The only timeout
		// that matters is the one most recently set, so we ignore any earlier set timeouts
		// that might inadvertently fail the transaction.
		if (timeout.present()) {
			newTr.transaction->setOption(FDBTransactionOptions::TIMEOUT, timeout);
		}
	}

	newTr.onChange = currentDb.onChange;

	lock.enter();
	transaction = newTr;
	lock.leave();
}

MultiVersionTransaction::TransactionInfo MultiVersionTransaction::getTransaction() {
	lock.enter();
	MultiVersionTransaction::TransactionInfo currentTr(transaction);
	lock.leave();

	return currentTr;
}

void MultiVersionTransaction::cancel() {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->cancel();
	}
}

void MultiVersionTransaction::setVersion(Version v) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->setVersion(v);
	}
}
ThreadFuture<Version> MultiVersionTransaction::getReadVersion() {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getReadVersion() : ThreadFuture<Version>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Optional<Value>> MultiVersionTransaction::get(const KeyRef& key, bool snapshot) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->get(key, snapshot) : ThreadFuture<Optional<Value>>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Key> MultiVersionTransaction::getKey(const KeySelectorRef& key, bool snapshot) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getKey(key, snapshot) : ThreadFuture<Key>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Standalone<RangeResultRef>> MultiVersionTransaction::getRange(const KeySelectorRef& begin,
                                                                           const KeySelectorRef& end,
                                                                           int limit,
                                                                           bool snapshot,
                                                                           bool reverse) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getRange(begin, end, limit, snapshot, reverse)
	                        : ThreadFuture<Standalone<RangeResultRef>>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Standalone<RangeResultRef>> MultiVersionTransaction::getRange(const KeySelectorRef& begin,
                                                                           const KeySelectorRef& end,
                                                                           GetRangeLimits limits,
                                                                           bool snapshot,
                                                                           bool reverse) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getRange(begin, end, limits, snapshot, reverse)
	                        : ThreadFuture<Standalone<RangeResultRef>>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Standalone<RangeResultRef>> MultiVersionTransaction::getRange(const KeyRangeRef& keys,
                                                                           int limit,
                                                                           bool snapshot,
                                                                           bool reverse) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getRange(keys, limit, snapshot, reverse)
	                        : ThreadFuture<Standalone<RangeResultRef>>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Standalone<RangeResultRef>> MultiVersionTransaction::getRange(const KeyRangeRef& keys,
                                                                           GetRangeLimits limits,
                                                                           bool snapshot,
                                                                           bool reverse) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getRange(keys, limits, snapshot, reverse)
	                        : ThreadFuture<Standalone<RangeResultRef>>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Standalone<StringRef>> MultiVersionTransaction::getVersionstamp() {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getVersionstamp() : ThreadFuture<Standalone<StringRef>>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Standalone<VectorRef<const char*>>> MultiVersionTransaction::getAddressesForKey(const KeyRef& key) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getAddressesForKey(key)
	                        : ThreadFuture<Standalone<VectorRef<const char*>>>(Never());
	return abortableFuture(f, tr.onChange);
}

void MultiVersionTransaction::addReadConflictRange(const KeyRangeRef& keys) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->addReadConflictRange(keys);
	}
}

ThreadFuture<int64_t> MultiVersionTransaction::getEstimatedRangeSizeBytes(const KeyRangeRef& keys) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getEstimatedRangeSizeBytes(keys) : ThreadFuture<int64_t>(Never());
	return abortableFuture(f, tr.onChange);
}

void MultiVersionTransaction::atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->atomicOp(key, value, operationType);
	}
}

void MultiVersionTransaction::set(const KeyRef& key, const ValueRef& value) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->set(key, value);
	}
}

void MultiVersionTransaction::clear(const KeyRef& begin, const KeyRef& end) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->clear(begin, end);
	}
}

void MultiVersionTransaction::clear(const KeyRangeRef& range) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->clear(range);
	}
}

void MultiVersionTransaction::clear(const KeyRef& key) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->clear(key);
	}
}

ThreadFuture<Void> MultiVersionTransaction::watch(const KeyRef& key) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->watch(key) : ThreadFuture<Void>(Never());
	return abortableFuture(f, tr.onChange);
}

void MultiVersionTransaction::addWriteConflictRange(const KeyRangeRef& keys) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->addWriteConflictRange(keys);
	}
}

ThreadFuture<Void> MultiVersionTransaction::commit() {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->commit() : ThreadFuture<Void>(Never());
	return abortableFuture(f, tr.onChange);
}

Version MultiVersionTransaction::getCommittedVersion() {
	auto tr = getTransaction();
	if (tr.transaction) {
		return tr.transaction->getCommittedVersion();
	}

	return invalidVersion;
}

ThreadFuture<int64_t> MultiVersionTransaction::getApproximateSize() {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getApproximateSize() : ThreadFuture<int64_t>(Never());
	return abortableFuture(f, tr.onChange);
}

void MultiVersionTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	auto itr = FDBTransactionOptions::optionInfo.find(option);
	if (itr == FDBTransactionOptions::optionInfo.end()) {
		TraceEvent("UnknownTransactionOption").detail("Option", option);
		throw invalid_option();
	}

	if (MultiVersionApi::apiVersionAtLeast(610) && itr->second.persistent) {
		persistentOptions.emplace_back(option, value.castTo<Standalone<StringRef>>());
	}
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->setOption(option, value);
	}
}

ThreadFuture<Void> MultiVersionTransaction::onError(Error const& e) {
	if (e.code() == error_code_cluster_version_changed) {
		updateTransaction();
		return ThreadFuture<Void>(Void());
	} else {
		auto tr = getTransaction();
		auto f = tr.transaction ? tr.transaction->onError(e) : ThreadFuture<Void>(Never());
		f = abortableFuture(f, tr.onChange);

		return flatMapThreadFuture<Void, Void>(f, [this, e](ErrorOr<Void> ready) {
			if (!ready.isError() || ready.getError().code() != error_code_cluster_version_changed) {
				if (ready.isError()) {
					return ErrorOr<ThreadFuture<Void>>(ready.getError());
				}

				return ErrorOr<ThreadFuture<Void>>(Void());
			}

			updateTransaction();
			return ErrorOr<ThreadFuture<Void>>(onError(e));
		});
	}
}

void MultiVersionTransaction::reset() {
	persistentOptions.clear();
	setDefaultOptions(db->dbState->transactionDefaultOptions);
	updateTransaction();
}

// MultiVersionDatabase
MultiVersionDatabase::MultiVersionDatabase(MultiVersionApi* api,
                                           int threadIdx,
                                           std::string clusterFilePath,
                                           Reference<IDatabase> db,
                                           Reference<IDatabase> versionMonitorDb,
                                           bool openConnectors)
  : dbState(new DatabaseState(clusterFilePath, versionMonitorDb)) {
	dbState->db = db;
	dbState->dbVar->set(db);

	if (openConnectors) {
		if (!api->localClientDisabled) {
			dbState->addClient(api->getLocalClient());
		}

		api->runOnExternalClients(threadIdx, [this](Reference<ClientInfo> client) { dbState->addClient(client); });

		if (!externalClientsInitialized.test_and_set()) {
			api->runOnExternalClientsAllThreads([&clusterFilePath](Reference<ClientInfo> client) {
				// This creates a database to initialize some client state on the external library
				// We only do this on 6.2+ clients to avoid some bugs associated with older versions
				// This deletes the new database immediately to discard its connections
				if (client->protocolVersion.hasCloseUnusedConnection()) {
					Reference<IDatabase> newDb = client->api->createDatabase(clusterFilePath.c_str());
				}
			});
		}

		// For clients older than 6.2 we create and maintain our database connection
		api->runOnExternalClients(threadIdx, [this, &clusterFilePath](Reference<ClientInfo> client) {
			if (!client->protocolVersion.hasCloseUnusedConnection()) {
				dbState->legacyDatabaseConnections[client->protocolVersion] =
				    client->api->createDatabase(clusterFilePath.c_str());
			}
		});

		Reference<DatabaseState> dbStateRef = dbState;
		onMainThreadVoid([dbStateRef]() { dbStateRef->protocolVersionMonitor = dbStateRef->monitorProtocolVersion(); },
		                 nullptr);
	}
}

MultiVersionDatabase::~MultiVersionDatabase() {
	dbState->close();
}

// Create a MultiVersionDatabase that wraps an already created IDatabase object
// For internal use in testing
Reference<IDatabase> MultiVersionDatabase::debugCreateFromExistingDatabase(Reference<IDatabase> db) {
	return Reference<IDatabase>(new MultiVersionDatabase(MultiVersionApi::api, 0, "", db, db, false));
}

Reference<ITransaction> MultiVersionDatabase::createTransaction() {
	return Reference<ITransaction>(
	    new MultiVersionTransaction(Reference<MultiVersionDatabase>::addRef(this), dbState->transactionDefaultOptions));
}

void MultiVersionDatabase::setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	MutexHolder holder(dbState->optionLock);

	auto itr = FDBDatabaseOptions::optionInfo.find(option);
	if (itr == FDBDatabaseOptions::optionInfo.end()) {
		TraceEvent("UnknownDatabaseOption").detail("Option", option);
		throw invalid_option();
	}

	int defaultFor = itr->second.defaultFor;
	if (defaultFor >= 0) {
		ASSERT(FDBTransactionOptions::optionInfo.find((FDBTransactionOptions::Option)defaultFor) !=
		       FDBTransactionOptions::optionInfo.end());
		dbState->transactionDefaultOptions.addOption((FDBTransactionOptions::Option)defaultFor,
		                                             value.castTo<Standalone<StringRef>>());
	}

	dbState->options.push_back(std::make_pair(option, value.castTo<Standalone<StringRef>>()));

	if (dbState->db) {
		dbState->db->setOption(option, value);
	}
}

// Returns the protocol version reported by the coordinator this client is connected to
// If an expected version is given, the future won't return until the protocol version is different than expected
// Note: this will never return if the server is running a protocol from FDB 5.0 or older
ThreadFuture<ProtocolVersion> MultiVersionDatabase::getServerProtocol(Optional<ProtocolVersion> expectedVersion) {
	return dbState->versionMonitorDb->getServerProtocol(expectedVersion);
}

MultiVersionDatabase::DatabaseState::DatabaseState(std::string clusterFilePath, Reference<IDatabase> versionMonitorDb)
  : clusterFilePath(clusterFilePath), versionMonitorDb(versionMonitorDb),
    dbVar(new ThreadSafeAsyncVar<Reference<IDatabase>>(Reference<IDatabase>(nullptr))) {}

// Adds a client (local or externally loaded) that can be used to connect to the cluster
void MultiVersionDatabase::DatabaseState::addClient(Reference<ClientInfo> client) {
	ProtocolVersion baseVersion = client->protocolVersion.normalizedVersion();
	auto [itr, inserted] = clients.insert({ baseVersion, client });
	if (!inserted) {
		// SOMEDAY: prefer client with higher release version if protocol versions are compatible
		Reference<ClientInfo> keptClient = itr->second;
		Reference<ClientInfo> discardedClient = client;
		if (client->canReplace(itr->second)) {
			std::swap(keptClient, discardedClient);
			clients[baseVersion] = client;
		}

		discardedClient->failed = true;
		TraceEvent(SevWarn, "DuplicateClientVersion")
		    .detail("Keeping", keptClient->libPath)
		    .detail("KeptProtocolVersion", keptClient->protocolVersion)
		    .detail("Disabling", discardedClient->libPath)
		    .detail("DisabledProtocolVersion", discardedClient->protocolVersion);

		MultiVersionApi::api->updateSupportedVersions();
	}

	if (!client->protocolVersion.hasInexpensiveMultiVersionClient() && !client->failed) {
		TraceEvent("AddingLegacyVersionMonitor")
		    .detail("LibPath", client->libPath)
		    .detail("ProtocolVersion", client->protocolVersion);

		legacyVersionMonitors.emplace_back(new LegacyVersionMonitor(client));
	}
}

// Watch the cluster protocol version for changes and update the database state when it does.
// Must be called from the main thread
ThreadFuture<Void> MultiVersionDatabase::DatabaseState::monitorProtocolVersion() {
	startLegacyVersionMonitors();

	Optional<ProtocolVersion> expected = dbProtocolVersion;
	ThreadFuture<ProtocolVersion> f = versionMonitorDb->getServerProtocol(dbProtocolVersion);

	Reference<DatabaseState> self = Reference<DatabaseState>::addRef(this);
	return mapThreadFuture<ProtocolVersion, Void>(f, [self, expected](ErrorOr<ProtocolVersion> cv) {
		if (cv.isError()) {
			if (cv.getError().code() == error_code_operation_cancelled) {
				return ErrorOr<Void>(cv.getError());
			}

			TraceEvent("ErrorGettingClusterProtocolVersion")
			    .detail("ExpectedProtocolVersion", expected)
			    .error(cv.getError());
		}

		ProtocolVersion clusterVersion =
		    !cv.isError() ? cv.get() : self->dbProtocolVersion.orDefault(currentProtocolVersion);
		onMainThreadVoid([self, clusterVersion]() { self->protocolVersionChanged(clusterVersion); }, nullptr);
		return ErrorOr<Void>(Void());
	});
}

// Called when a change to the protocol version of the cluster has been detected.
// Must be called from the main thread
void MultiVersionDatabase::DatabaseState::protocolVersionChanged(ProtocolVersion protocolVersion) {
	// If the protocol version changed but is still compatible, update our local version but keep the same connection
	if (dbProtocolVersion.present() &&
	    protocolVersion.normalizedVersion() == dbProtocolVersion.get().normalizedVersion()) {
		dbProtocolVersion = protocolVersion;

		ASSERT(protocolVersionMonitor.isValid());
		protocolVersionMonitor.cancel();
		protocolVersionMonitor = monitorProtocolVersion();
	}

	// The protocol version has changed to a different, incompatible version
	else {
		TraceEvent("ProtocolVersionChanged")
		    .detail("NewProtocolVersion", protocolVersion)
		    .detail("OldProtocolVersion", dbProtocolVersion);

		dbProtocolVersion = protocolVersion;

		auto itr = clients.find(protocolVersion.normalizedVersion());
		if (itr != clients.end()) {
			auto& client = itr->second;
			TraceEvent("CreatingDatabaseOnClient")
			    .detail("LibraryPath", client->libPath)
			    .detail("Failed", client->failed)
			    .detail("External", client->external);

			Reference<IDatabase> newDb = client->api->createDatabase(clusterFilePath.c_str());

			if (client->external && !MultiVersionApi::apiVersionAtLeast(610)) {
				// Old API versions return a future when creating the database, so we need to wait for it
				Reference<DatabaseState> self = Reference<DatabaseState>::addRef(this);
				dbReady = mapThreadFuture<Void, Void>(
				    newDb.castTo<DLDatabase>()->onReady(), [self, newDb, client](ErrorOr<Void> ready) {
					    if (!ready.isError()) {
						    onMainThreadVoid([self, newDb, client]() { self->updateDatabase(newDb, client); }, nullptr);
					    } else {
						    onMainThreadVoid([self, client]() { self->updateDatabase(Reference<IDatabase>(), client); },
						                     nullptr);
					    }

					    return ready;
				    });
			} else {
				updateDatabase(newDb, client);
			}
		} else {
			// We don't have a client matching the current protocol
			updateDatabase(Reference<IDatabase>(), Reference<ClientInfo>());
		}
	}
}

// Replaces the active database connection with a new one. Must be called from the main thread.
void MultiVersionDatabase::DatabaseState::updateDatabase(Reference<IDatabase> newDb, Reference<ClientInfo> client) {
	if (newDb) {
		optionLock.enter();
		for (auto option : options) {
			try {
				// In practice, this will set a deferred error instead of throwing. If that happens, the database
				// will be unusable (attempts to use it will throw errors).
				newDb->setOption(option.first, option.second.castTo<StringRef>());
			} catch (Error& e) {
				optionLock.leave();

				// If we can't set all of the options on a cluster, we abandon the client
				TraceEvent(SevError, "ClusterVersionChangeOptionError")
				    .error(e)
				    .detail("Option", option.first)
				    .detail("OptionValue", option.second)
				    .detail("LibPath", client->libPath);
				client->failed = true;
				MultiVersionApi::api->updateSupportedVersions();
				newDb = Reference<IDatabase>();
				break;
			}
		}

		db = newDb;
		optionLock.leave();
	} else {
		// We don't have a database connection, so use the local client to monitor the protocol version
		db = Reference<IDatabase>();
	}

	dbVar->set(db);

	ASSERT(protocolVersionMonitor.isValid());
	protocolVersionMonitor.cancel();
	protocolVersionMonitor = monitorProtocolVersion();
}

// Starts version monitors for old client versions that don't support connect packet monitoring (<= 5.0).
// Must be called from the main thread
void MultiVersionDatabase::DatabaseState::startLegacyVersionMonitors() {
	for (auto itr = legacyVersionMonitors.begin(); itr != legacyVersionMonitors.end(); ++itr) {
		while (itr != legacyVersionMonitors.end() && (*itr)->client->failed) {
			(*itr)->close();
			itr = legacyVersionMonitors.erase(itr);
		}
		if (itr != legacyVersionMonitors.end() &&
		    (!dbProtocolVersion.present() || (*itr)->client->protocolVersion != dbProtocolVersion.get())) {
			(*itr)->startConnectionMonitor(Reference<DatabaseState>::addRef(this));
		}
	}
}

// Cleans up state for the legacy version monitors to break reference cycles
void MultiVersionDatabase::DatabaseState::close() {
	Reference<DatabaseState> self = Reference<DatabaseState>::addRef(this);
	onMainThreadVoid(
	    [self]() {
		    if (self->protocolVersionMonitor.isValid()) {
			    self->protocolVersionMonitor.cancel();
		    }
		    for (auto monitor : self->legacyVersionMonitors) {
			    monitor->close();
		    }

		    self->legacyVersionMonitors.clear();
	    },
	    nullptr);
}

// Starts the connection monitor by creating a database object at an old version.
// Must be called from the main thread
void MultiVersionDatabase::LegacyVersionMonitor::startConnectionMonitor(
    Reference<MultiVersionDatabase::DatabaseState> dbState) {
	if (!monitorRunning) {
		monitorRunning = true;

		auto itr = dbState->legacyDatabaseConnections.find(client->protocolVersion);
		ASSERT(itr != dbState->legacyDatabaseConnections.end());

		db = itr->second;
		tr = Reference<ITransaction>();

		TraceEvent("StartingLegacyVersionMonitor").detail("ProtocolVersion", client->protocolVersion);
		Reference<LegacyVersionMonitor> self = Reference<LegacyVersionMonitor>::addRef(this);
		versionMonitor =
		    mapThreadFuture<Void, Void>(db.castTo<DLDatabase>()->onReady(), [self, dbState](ErrorOr<Void> ready) {
			    onMainThreadVoid(
			        [self, ready, dbState]() {
				        if (ready.isError()) {
					        if (ready.getError().code() != error_code_operation_cancelled) {
						        TraceEvent(SevError, "FailedToOpenDatabaseOnClient")
						            .error(ready.getError())
						            .detail("LibPath", self->client->libPath);

						        self->client->failed = true;
						        MultiVersionApi::api->updateSupportedVersions();
					        }
				        } else {
					        self->runGrvProbe(dbState);
				        }
			        },
			        nullptr);

			    return ready;
		    });
	}
}

// Runs a GRV probe on the cluster to determine if the client version is compatible with the cluster.
// Must be called from main thread
void MultiVersionDatabase::LegacyVersionMonitor::runGrvProbe(Reference<MultiVersionDatabase::DatabaseState> dbState) {
	tr = db->createTransaction();
	Reference<LegacyVersionMonitor> self = Reference<LegacyVersionMonitor>::addRef(this);
	versionMonitor = mapThreadFuture<Version, Void>(tr->getReadVersion(), [self, dbState](ErrorOr<Version> v) {
		// If the version attempt returns an error, we regard that as a connection (except operation_cancelled)
		if (!v.isError() || v.getError().code() != error_code_operation_cancelled) {
			onMainThreadVoid(
			    [self, dbState]() {
				    self->monitorRunning = false;
				    dbState->protocolVersionChanged(self->client->protocolVersion);
			    },
			    nullptr);
		}

		return v.map<Void>([](Version v) { return Void(); });
	});
}

void MultiVersionDatabase::LegacyVersionMonitor::close() {
	if (versionMonitor.isValid()) {
		versionMonitor.cancel();
	}
}

std::atomic_flag MultiVersionDatabase::externalClientsInitialized = ATOMIC_FLAG_INIT;

// MultiVersionApi
bool MultiVersionApi::apiVersionAtLeast(int minVersion) {
	ASSERT_NE(MultiVersionApi::api->apiVersion, 0);
	return MultiVersionApi::api->apiVersion >= minVersion || MultiVersionApi::api->apiVersion < 0;
}

void MultiVersionApi::runOnExternalClientsAllThreads(std::function<void(Reference<ClientInfo>)> func,
                                                     bool runOnFailedClients) {
	for (int i = 0; i < threadCount; i++) {
		runOnExternalClients(i, func, runOnFailedClients);
	}
}

// runOnFailedClients should be used cautiously. Some failed clients may not have successfully loaded all symbols.
void MultiVersionApi::runOnExternalClients(int threadIdx,
                                           std::function<void(Reference<ClientInfo>)> func,
                                           bool runOnFailedClients) {
	bool newFailure = false;

	auto c = externalClients.begin();
	while (c != externalClients.end()) {
		auto client = c->second[threadIdx];
		try {
			if (!client->failed || runOnFailedClients) { // TODO: Should we ignore some failures?
				func(client);
			}
		} catch (Error& e) {
			if (e.code() == error_code_external_client_already_loaded) {
				TraceEvent(SevInfo, "ExternalClientAlreadyLoaded").error(e).detail("LibPath", c->first);
				c = externalClients.erase(c);
				continue;
			} else {
				TraceEvent(SevWarnAlways, "ExternalClientFailure").error(e).detail("LibPath", c->first);
				client->failed = true;
				newFailure = true;
			}
		}

		++c;
	}

	if (newFailure) {
		updateSupportedVersions();
	}
}

Reference<ClientInfo> MultiVersionApi::getLocalClient() {
	return localClient;
}

void MultiVersionApi::selectApiVersion(int apiVersion) {
	if (!localClient) {
		localClient = Reference<ClientInfo>(new ClientInfo(ThreadSafeApi::api));
	}

	if (this->apiVersion != 0 && this->apiVersion != apiVersion) {
		throw api_version_already_set();
	}

	localClient->api->selectApiVersion(apiVersion);
	this->apiVersion = apiVersion;
}

const char* MultiVersionApi::getClientVersion() {
	return localClient->api->getClientVersion();
}

void validateOption(Optional<StringRef> value, bool canBePresent, bool canBeAbsent, bool canBeEmpty = true) {
	ASSERT(canBePresent || canBeAbsent);

	if (!canBePresent && value.present() && (!canBeEmpty || value.get().size() > 0)) {
		throw invalid_option_value();
	}
	if (!canBeAbsent && (!value.present() || (!canBeEmpty && value.get().size() == 0))) {
		throw invalid_option_value();
	}
}

void MultiVersionApi::disableMultiVersionClientApi() {
	MutexHolder holder(lock);
	if (networkStartSetup || localClientDisabled) {
		throw invalid_option();
	}

	bypassMultiClientApi = true;
}

void MultiVersionApi::setCallbacksOnExternalThreads() {
	MutexHolder holder(lock);
	if (networkStartSetup) {
		throw invalid_option();
	}

	callbackOnMainThread = false;
}
void MultiVersionApi::addExternalLibrary(std::string path) {
	std::string filename = basename(path);

	if (filename.empty() || !fileExists(path)) {
		TraceEvent("ExternalClientNotFound").detail("LibraryPath", filename);
		throw file_not_found();
	}

	MutexHolder holder(lock);
	if (networkStartSetup) {
		throw invalid_option(); // SOMEDAY: it might be good to allow clients to be added after the network is setup
	}

	// external libraries always run on their own thread; ensure we allocate at least one thread to run this library.
	threadCount = std::max(threadCount, 1);

	if (externalClientDescriptions.count(filename) == 0) {
		TraceEvent("AddingExternalClient").detail("LibraryPath", filename);
		externalClientDescriptions.emplace(std::make_pair(filename, ClientDesc(path, true)));
	}
}

void MultiVersionApi::addExternalLibraryDirectory(std::string path) {
	TraceEvent("AddingExternalClientDirectory").detail("Directory", path);
	std::vector<std::string> files = platform::listFiles(path, DYNAMIC_LIB_EXT);

	MutexHolder holder(lock);
	if (networkStartSetup) {
		throw invalid_option(); // SOMEDAY: it might be good to allow clients to be added after the network is setup
	}

	// external libraries always run on their own thread; ensure we allocate at least one thread to run this library.
	threadCount = std::max(threadCount, 1);

	for (auto filename : files) {
		std::string lib = abspath(joinPath(path, filename));
		if (externalClientDescriptions.count(filename) == 0) {
			TraceEvent("AddingExternalClient").detail("LibraryPath", filename);
			externalClientDescriptions.emplace(std::make_pair(filename, ClientDesc(lib, true)));
		}
	}
}
#if defined(__unixish__)
std::vector<std::pair<std::string, bool>> MultiVersionApi::copyExternalLibraryPerThread(std::string path) {
	ASSERT_GE(threadCount, 1);
	// Copy library for each thread configured per version
	std::vector<std::pair<std::string, bool>> paths;

	if (threadCount == 1) {
		paths.push_back({ path, false });
	} else {
		// It's tempting to use the so once without copying. However, we don't know
		// if the thing we're about to copy is the shared object executing this code
		// or not, so this optimization is unsafe.
		// paths.push_back({path, false});
		for (int ii = 0; ii < threadCount; ++ii) {
			std::string filename = basename(path);

			char tempName[PATH_MAX + 12];
			sprintf(tempName, "/tmp/%s-XXXXXX", filename.c_str());
			int tempFd = mkstemp(tempName);
			int fd;

			if ((fd = open(path.c_str(), O_RDONLY)) == -1) {
				TraceEvent("ExternalClientNotFound").detail("LibraryPath", path);
				throw file_not_found();
			}

			TraceEvent("CopyingExternalClient")
			    .detail("FileName", filename)
			    .detail("LibraryPath", path)
			    .detail("TempPath", tempName);

			constexpr size_t buf_sz = 4096;
			char buf[buf_sz];
			while (1) {
				ssize_t readCount = read(fd, buf, buf_sz);
				if (readCount == 0) {
					// eof
					break;
				}
				if (readCount == -1) {
					TraceEvent(SevError, "ExternalClientCopyFailedReadError")
					    .GetLastError()
					    .detail("LibraryPath", path);
					throw platform_error();
				}
				ssize_t written = 0;
				while (written != readCount) {
					ssize_t writeCount = write(tempFd, buf + written, readCount - written);
					if (writeCount == -1) {
						TraceEvent(SevError, "ExternalClientCopyFailedWriteError")
						    .GetLastError()
						    .detail("LibraryPath", path);
						throw platform_error();
					}
					written += writeCount;
				}
			}

			close(fd);
			close(tempFd);

			paths.push_back({ tempName, true }); // use + delete temporary copies of the library.
		}
	}

	return paths;
}
#else
std::vector<std::pair<std::string, bool>> MultiVersionApi::copyExternalLibraryPerThread(std::string path) {
	if (threadCount > 1) {
		TraceEvent(SevError, "MultipleClientThreadsUnsupportedOnWindows");
		throw unsupported_operation();
	}
	std::vector<std::pair<std::string, bool>> paths;
	paths.push_back({ path, false });
	return paths;
}
#endif

void MultiVersionApi::disableLocalClient() {
	MutexHolder holder(lock);
	if (networkStartSetup || bypassMultiClientApi) {
		throw invalid_option();
	}
	threadCount = std::max(threadCount, 1);
	localClientDisabled = true;
}

void MultiVersionApi::setSupportedClientVersions(Standalone<StringRef> versions) {
	MutexHolder holder(lock);
	ASSERT(networkSetup);

	// This option must be set on the main thread because it modifies structures that can be used concurrently by the
	// main thread
	onMainThreadVoid(
	    [this, versions]() {
		    localClient->api->setNetworkOption(FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS, versions);
	    },
	    NULL);

	if (!bypassMultiClientApi) {
		runOnExternalClientsAllThreads([versions](Reference<ClientInfo> client) {
			client->api->setNetworkOption(FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS, versions);
		});
	}
}

void MultiVersionApi::setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	if (option != FDBNetworkOptions::EXTERNAL_CLIENT &&
	    !externalClient) { // This is the first option set for external clients
		loadEnvironmentVariableNetworkOptions();
	}

	setNetworkOptionInternal(option, value);
}

void MultiVersionApi::setNetworkOptionInternal(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	auto itr = FDBNetworkOptions::optionInfo.find(option);
	if (itr != FDBNetworkOptions::optionInfo.end()) {
		TraceEvent("SetNetworkOption").detail("Option", itr->second.name);
	} else {
		TraceEvent("UnknownNetworkOption").detail("Option", option);
		throw invalid_option();
	}

	if (option == FDBNetworkOptions::DISABLE_MULTI_VERSION_CLIENT_API) {
		validateOption(value, false, true);
		disableMultiVersionClientApi();
	} else if (option == FDBNetworkOptions::CALLBACKS_ON_EXTERNAL_THREADS) {
		validateOption(value, false, true);
		setCallbacksOnExternalThreads();
	} else if (option == FDBNetworkOptions::EXTERNAL_CLIENT_LIBRARY) {
		validateOption(value, true, false, false);
		addExternalLibrary(abspath(value.get().toString()));
	} else if (option == FDBNetworkOptions::EXTERNAL_CLIENT_DIRECTORY) {
		validateOption(value, true, false, false);
		addExternalLibraryDirectory(value.get().toString());
	} else if (option == FDBNetworkOptions::DISABLE_LOCAL_CLIENT) {
		validateOption(value, false, true);
		disableLocalClient();
	} else if (option == FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS) {
		ASSERT(value.present());
		setSupportedClientVersions(value.get());
	} else if (option == FDBNetworkOptions::EXTERNAL_CLIENT) {
		MutexHolder holder(lock);
		ASSERT(!value.present() && !networkStartSetup);
		externalClient = true;
		bypassMultiClientApi = true;
	} else if (option == FDBNetworkOptions::CLIENT_THREADS_PER_VERSION) {
		MutexHolder holder(lock);
		validateOption(value, true, false, false);
		ASSERT(!networkStartSetup);
#if defined(__unixish__)
		threadCount = extractIntOption(value, 1, 1024);
#else
		// multiple client threads are not supported on windows.
		threadCount = extractIntOption(value, 1, 1);
#endif
		if (threadCount > 1) {
			disableLocalClient();
		}
	} else {
		MutexHolder holder(lock);
		localClient->api->setNetworkOption(option, value);

		if (!bypassMultiClientApi) {
			if (networkSetup) {
				runOnExternalClientsAllThreads(
				    [option, value](Reference<ClientInfo> client) { client->api->setNetworkOption(option, value); });
			} else {
				options.push_back(std::make_pair(option, value.castTo<Standalone<StringRef>>()));
			}
		}
	}
}

void MultiVersionApi::setupNetwork() {
	if (!externalClient) {
		loadEnvironmentVariableNetworkOptions();
	}

	uint64_t transportId = 0;
	{ // lock scope
		MutexHolder holder(lock);
		if (networkStartSetup) {
			throw network_already_setup();
		}

		for (auto i : externalClientDescriptions) {
			std::string path = i.second.libPath;
			std::string filename = basename(path);

			// Copy external lib for each thread
			if (externalClients.count(filename) == 0) {
				externalClients[filename] = {};
				for (const auto& tmp : copyExternalLibraryPerThread(path)) {
					externalClients[filename].push_back(Reference<ClientInfo>(
					    new ClientInfo(new DLApi(tmp.first, tmp.second /*unlink on load*/), path)));
				}
			}
		}

		if (externalClients.empty() && localClientDisabled) {
			// SOMEDAY: this should be allowed when it's possible to add external clients after the
			// network is setup.
			//
			// Typically we would create a more specific error for this case, but since we expect
			// this case to go away soon, we can use a trace event and a generic error.
			TraceEvent(SevWarn, "CannotSetupNetwork")
			    .detail("Reason", "Local client is disabled and no external clients configured");

			throw client_invalid_operation();
		}

		networkStartSetup = true;

		if (externalClients.empty()) {
			bypassMultiClientApi = true; // SOMEDAY: we won't be able to set this option once it becomes possible to
			                             // add clients after setupNetwork is called
		}

		if (!bypassMultiClientApi) {
			transportId = (uint64_t(uint32_t(platform::getRandomSeed())) << 32) ^ uint32_t(platform::getRandomSeed());
			if (transportId <= 1)
				transportId += 2;
			localClient->api->setNetworkOption(FDBNetworkOptions::EXTERNAL_CLIENT_TRANSPORT_ID,
			                                   std::to_string(transportId));
		}
		localClient->api->setupNetwork();
	}

	localClient->loadProtocolVersion();

	if (!bypassMultiClientApi) {
		runOnExternalClientsAllThreads([this](Reference<ClientInfo> client) {
			TraceEvent("InitializingExternalClient").detail("LibraryPath", client->libPath);
			client->api->selectApiVersion(apiVersion);
			client->loadProtocolVersion();
		});

		MutexHolder holder(lock);
		runOnExternalClientsAllThreads([this, transportId](Reference<ClientInfo> client) {
			for (auto option : options) {
				client->api->setNetworkOption(option.first, option.second.castTo<StringRef>());
			}
			client->api->setNetworkOption(FDBNetworkOptions::EXTERNAL_CLIENT_TRANSPORT_ID, std::to_string(transportId));

			client->api->setupNetwork();
		});

		networkSetup = true; // Needs to be guarded by mutex
	} else {
		networkSetup = true;
	}

	options.clear();
	updateSupportedVersions();
}

THREAD_FUNC_RETURN runNetworkThread(void* param) {
	try {
		((ClientInfo*)param)->api->runNetwork();
	} catch (Error& e) {
		TraceEvent(SevError, "RunNetworkError").error(e);
	}

	THREAD_RETURN;
}

void MultiVersionApi::runNetwork() {
	lock.enter();
	if (!networkSetup) {
		lock.leave();
		throw network_not_setup();
	}

	lock.leave();

	std::vector<THREAD_HANDLE> handles;
	if (!bypassMultiClientApi) {
		runOnExternalClientsAllThreads([&handles](Reference<ClientInfo> client) {
			if (client->external) {
				handles.push_back(g_network->startThread(&runNetworkThread, client.getPtr()));
			}
		});
	}

	localClient->api->runNetwork();

	for (auto h : handles) {
		waitThread(h);
	}
}

void MultiVersionApi::stopNetwork() {
	lock.enter();
	if (!networkSetup) {
		lock.leave();
		throw network_not_setup();
	}
	lock.leave();

	localClient->api->stopNetwork();

	if (!bypassMultiClientApi) {
		runOnExternalClientsAllThreads([](Reference<ClientInfo> client) { client->api->stopNetwork(); }, true);
	}
}

void MultiVersionApi::addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) {
	lock.enter();
	if (!networkSetup) {
		lock.leave();
		throw network_not_setup();
	}
	lock.leave();

	localClient->api->addNetworkThreadCompletionHook(hook, hookParameter);

	if (!bypassMultiClientApi) {
		runOnExternalClientsAllThreads([hook, hookParameter](Reference<ClientInfo> client) {
			client->api->addNetworkThreadCompletionHook(hook, hookParameter);
		});
	}
}

// Creates an IDatabase object that represents a connection to the cluster
Reference<IDatabase> MultiVersionApi::createDatabase(const char* clusterFilePath) {
	lock.enter();
	if (!networkSetup) {
		lock.leave();
		throw network_not_setup();
	}
	std::string clusterFile(clusterFilePath);

	if (localClientDisabled) {
		ASSERT(!bypassMultiClientApi);

		int threadIdx = nextThread;
		nextThread = (nextThread + 1) % threadCount;
		lock.leave();

		Reference<IDatabase> localDb = localClient->api->createDatabase(clusterFilePath);
		return Reference<IDatabase>(
		    new MultiVersionDatabase(this, threadIdx, clusterFile, Reference<IDatabase>(), localDb));
	}

	lock.leave();

	ASSERT_LE(threadCount, 1);

	Reference<IDatabase> localDb = localClient->api->createDatabase(clusterFilePath);
	if (bypassMultiClientApi) {
		return localDb;
	} else {
		return Reference<IDatabase>(new MultiVersionDatabase(this, 0, clusterFile, Reference<IDatabase>(), localDb));
	}
}

void MultiVersionApi::updateSupportedVersions() {
	if (networkSetup) {
		Standalone<VectorRef<uint8_t>> versionStr;

		// not mutating the client, so just call on one instance of each client version.
		// thread 0 always exists.
		runOnExternalClients(0, [&versionStr](Reference<ClientInfo> client) {
			const char* ver = client->api->getClientVersion();
			versionStr.append(versionStr.arena(), (uint8_t*)ver, (int)strlen(ver));
			versionStr.append(versionStr.arena(), (uint8_t*)";", 1);
		});

		if (!localClient->failed) {
			const char* local = localClient->api->getClientVersion();
			versionStr.append(versionStr.arena(), (uint8_t*)local, (int)strlen(local));
		} else {
			versionStr.resize(versionStr.arena(), std::max(0, versionStr.size() - 1));
		}

		setNetworkOption(FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS,
		                 StringRef(versionStr.begin(), versionStr.size()));
	}
}

std::vector<std::string> parseOptionValues(std::string valueStr) {
	std::string specialCharacters = "\\";
	specialCharacters += ENV_VAR_PATH_SEPARATOR;

	std::vector<std::string> values;

	size_t index = 0;
	size_t nextIndex = 0;
	std::stringstream ss;
	while (true) {
		nextIndex = valueStr.find_first_of(specialCharacters, index);
		char c = nextIndex == valueStr.npos ? ENV_VAR_PATH_SEPARATOR : valueStr[nextIndex];

		if (c == '\\') {
			if (valueStr.size() == nextIndex + 1 || specialCharacters.find(valueStr[nextIndex + 1]) == valueStr.npos) {
				throw invalid_option_value();
			}

			ss << valueStr.substr(index, nextIndex - index);
			ss << valueStr[nextIndex + 1];

			index = nextIndex + 2;
		} else if (c == ENV_VAR_PATH_SEPARATOR) {
			ss << valueStr.substr(index, nextIndex - index);
			values.push_back(ss.str());
			ss.str(std::string());

			if (nextIndex == valueStr.npos) {
				break;
			}
			index = nextIndex + 1;
		} else {
			ASSERT(false);
		}
	}

	return values;
}

// This function sets all environment variable options which have not been set previously by a call to this function.
// If an option has multiple values and setting one of those values failed with an error, then only those options
// which were not successfully set will be set on subsequent calls.
void MultiVersionApi::loadEnvironmentVariableNetworkOptions() {
	if (envOptionsLoaded) {
		return;
	}

	for (auto option : FDBNetworkOptions::optionInfo) {
		if (!option.second.hidden) {
			std::string valueStr;
			try {
				if (platform::getEnvironmentVar(("FDB_NETWORK_OPTION_" + option.second.name).c_str(), valueStr)) {
					for (auto value : parseOptionValues(valueStr)) {
						Standalone<StringRef> currentValue = StringRef(value);
						{ // lock scope
							MutexHolder holder(lock);
							if (setEnvOptions[option.first].count(currentValue) == 0) {
								setNetworkOptionInternal(option.first, currentValue);
								setEnvOptions[option.first].insert(currentValue);
							}
						}
					}
				}
			} catch (Error& e) {
				TraceEvent(SevError, "EnvironmentVariableNetworkOptionFailed")
				    .error(e)
				    .detail("Option", option.second.name)
				    .detail("Value", valueStr);
				throw environment_variable_network_option_failed();
			}
		}
	}

	MutexHolder holder(lock);
	envOptionsLoaded = true;
}

MultiVersionApi::MultiVersionApi()
  : bypassMultiClientApi(false), networkStartSetup(false), networkSetup(false), callbackOnMainThread(true),
    externalClient(false), localClientDisabled(false), apiVersion(0), envOptionsLoaded(false), threadCount(0) {}

MultiVersionApi* MultiVersionApi::api = new MultiVersionApi();

// ClientInfo
void ClientInfo::loadProtocolVersion() {
	std::string version = api->getClientVersion();
	if (version == "unknown") {
		protocolVersion = ProtocolVersion(0);
		return;
	}

	char* next;
	std::string protocolVersionStr = ClientVersionRef(StringRef(version)).protocolVersion.toString();
	protocolVersion = ProtocolVersion(strtoull(protocolVersionStr.c_str(), &next, 16));

	ASSERT(protocolVersion.version() != 0 && protocolVersion.version() != ULLONG_MAX);
	ASSERT_EQ(next, &protocolVersionStr[protocolVersionStr.length()]);
}

bool ClientInfo::canReplace(Reference<ClientInfo> other) const {
	if (protocolVersion > other->protocolVersion) {
		return true;
	}

	if (protocolVersion == other->protocolVersion && !external) {
		return true;
	}

	return !protocolVersion.isCompatible(other->protocolVersion);
}

// UNIT TESTS
extern bool noUnseed;

TEST_CASE("/fdbclient/multiversionclient/EnvironmentVariableParsing") {
	auto vals = parseOptionValues("a");
	ASSERT(vals.size() == 1 && vals[0] == "a");

	vals = parseOptionValues("abcde");
	ASSERT(vals.size() == 1 && vals[0] == "abcde");

	vals = parseOptionValues("");
	ASSERT(vals.size() == 1 && vals[0] == "");

	vals = parseOptionValues("a:b:c:d:e");
	ASSERT(vals.size() == 5 && vals[0] == "a" && vals[1] == "b" && vals[2] == "c" && vals[3] == "d" && vals[4] == "e");

	vals = parseOptionValues("\\\\a\\::\\:b:\\\\");
	ASSERT(vals.size() == 3 && vals[0] == "\\a:" && vals[1] == ":b" && vals[2] == "\\");

	vals = parseOptionValues("abcd:");
	ASSERT(vals.size() == 2 && vals[0] == "abcd" && vals[1] == "");

	vals = parseOptionValues(":abcd");
	ASSERT(vals.size() == 2 && vals[0] == "" && vals[1] == "abcd");

	vals = parseOptionValues(":");
	ASSERT(vals.size() == 2 && vals[0] == "" && vals[1] == "");

	try {
		vals = parseOptionValues("\\x");
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_invalid_option_value);
	}

	return Void();
}

class ValidateFuture : public ThreadCallback {
public:
	ValidateFuture(ThreadFuture<int> f, ErrorOr<int> expectedValue, std::set<int> legalErrors)
	  : f(f), expectedValue(expectedValue), legalErrors(legalErrors) {}

	virtual bool canFire(int notMadeActive) { return true; }

	virtual void fire(const Void& unused, int& userParam) {
		ASSERT(!f.isError() && !expectedValue.isError() && f.get() == expectedValue.get());
		delete this;
	}

	virtual void error(const Error& e, int& userParam) {
		ASSERT(legalErrors.count(e.code()) > 0 ||
		       (f.isError() && expectedValue.isError() && f.getError().code() == expectedValue.getError().code()));
		delete this;
	}

private:
	ThreadFuture<int> f;
	ErrorOr<int> expectedValue;
	std::set<int> legalErrors;
};

struct FutureInfo {
	FutureInfo() {
		if (deterministicRandom()->coinflip()) {
			expectedValue = Error(deterministicRandom()->randomInt(1, 100));
		} else {
			expectedValue = deterministicRandom()->randomInt(0, 100);
		}
	}

	FutureInfo(ThreadFuture<int> future, ErrorOr<int> expectedValue, std::set<int> legalErrors = std::set<int>())
	  : future(future), expectedValue(expectedValue), legalErrors(legalErrors) {}

	void validate() {
		int userParam;
		future.callOrSetAsCallback(new ValidateFuture(future, expectedValue, legalErrors), userParam, 0);
	}

	ThreadFuture<int> future;
	ErrorOr<int> expectedValue;
	std::set<int> legalErrors;
	std::vector<THREAD_HANDLE> threads;
};

FutureInfo createVarOnMainThread(bool canBeNever = true) {
	FutureInfo f;

	if (deterministicRandom()->coinflip()) {
		f.future = onMainThread([f, canBeNever]() {
			Future<Void> sleep;
			if (canBeNever && deterministicRandom()->coinflip()) {
				sleep = Never();
			} else {
				sleep = delay(0.1 * deterministicRandom()->random01());
			}

			if (f.expectedValue.isError()) {
				return tagError<int>(sleep, f.expectedValue.getError());
			} else {
				return tag(sleep, f.expectedValue.get());
			}
		});
	} else if (f.expectedValue.isError()) {
		f.future = f.expectedValue.getError();
	} else {
		f.future = f.expectedValue.get();
	}

	return f;
}

THREAD_FUNC setAbort(void* arg) {
	threadSleep(0.1 * deterministicRandom()->random01());
	try {
		((ThreadSingleAssignmentVar<Void>*)arg)->send(Void());
		((ThreadSingleAssignmentVar<Void>*)arg)->delref();
	} catch (Error& e) {
		printf("Caught error in setAbort: %s\n", e.name());
		ASSERT(false);
	}
	THREAD_RETURN;
}

THREAD_FUNC releaseMem(void* arg) {
	threadSleep(0.1 * deterministicRandom()->random01());
	try {
		// Must get for releaseMemory to work
		((ThreadSingleAssignmentVar<int>*)arg)->get();
	} catch (Error&) {
		// Swallow
	}
	try {
		((ThreadSingleAssignmentVar<int>*)arg)->releaseMemory();
	} catch (Error& e) {
		printf("Caught error in releaseMem: %s\n", e.name());
		ASSERT(false);
	}
	THREAD_RETURN;
}

THREAD_FUNC destroy(void* arg) {
	threadSleep(0.1 * deterministicRandom()->random01());
	try {
		((ThreadSingleAssignmentVar<int>*)arg)->cancel();
	} catch (Error& e) {
		printf("Caught error in destroy: %s\n", e.name());
		ASSERT(false);
	}
	THREAD_RETURN;
}

THREAD_FUNC cancel(void* arg) {
	threadSleep(0.1 * deterministicRandom()->random01());
	try {
		((ThreadSingleAssignmentVar<int>*)arg)->addref();
		destroy(arg);
	} catch (Error& e) {
		printf("Caught error in cancel: %s\n", e.name());
		ASSERT(false);
	}
	THREAD_RETURN;
}

ACTOR Future<Void> checkUndestroyedFutures(std::vector<ThreadSingleAssignmentVar<int>*> undestroyed) {
	state int fNum;
	state ThreadSingleAssignmentVar<int>* f;
	state double start = now();

	for (fNum = 0; fNum < undestroyed.size(); ++fNum) {
		f = undestroyed[fNum];

		while (!f->isReady() && start + 5 >= now()) {
			wait(delay(1.0));
		}

		ASSERT(f->isReady());
	}

	wait(delay(1.0));

	for (fNum = 0; fNum < undestroyed.size(); ++fNum) {
		f = undestroyed[fNum];

		ASSERT_EQ(f->debugGetReferenceCount(), 1);
		ASSERT(f->isReady());

		f->cancel();
	}

	return Void();
}

template <class T>
THREAD_FUNC runSingleAssignmentVarTest(void* arg) {
	noUnseed = true;

	volatile bool* done = (volatile bool*)arg;
	try {
		for (int i = 0; i < 25; ++i) {
			FutureInfo f = createVarOnMainThread(false);
			FutureInfo tf = T::createThreadFuture(f);
			tf.validate();

			tf.future.extractPtr(); // leaks
		}

		for (int numRuns = 0; numRuns < 25; ++numRuns) {
			std::vector<ThreadSingleAssignmentVar<int>*> undestroyed;
			std::vector<THREAD_HANDLE> threads;
			for (int i = 0; i < 10; ++i) {
				FutureInfo f = createVarOnMainThread();
				f.legalErrors.insert(error_code_operation_cancelled);

				FutureInfo tf = T::createThreadFuture(f);
				for (auto t : tf.threads) {
					threads.push_back(t);
				}

				tf.legalErrors.insert(error_code_operation_cancelled);
				tf.validate();

				auto tfp = tf.future.extractPtr();

				if (deterministicRandom()->coinflip()) {
					if (deterministicRandom()->coinflip()) {
						threads.push_back(g_network->startThread(releaseMem, tfp));
					}
					threads.push_back(g_network->startThread(cancel, tfp));
					undestroyed.push_back((ThreadSingleAssignmentVar<int>*)tfp);
				} else {
					threads.push_back(g_network->startThread(destroy, tfp));
				}
			}

			for (auto t : threads) {
				waitThread(t);
			}

			ThreadFuture<Void> checkUndestroyed =
			    onMainThread([undestroyed]() { return checkUndestroyedFutures(undestroyed); });

			checkUndestroyed.blockUntilReady();
		}

		onMainThreadVoid([done]() { *done = true; }, NULL);
	} catch (Error& e) {
		printf("Caught error in test: %s\n", e.name());
		*done = true;
		ASSERT(false);
	}

	THREAD_RETURN;
}

struct AbortableTest {
	static FutureInfo createThreadFuture(FutureInfo f) {
		ThreadSingleAssignmentVar<Void>* abort = new ThreadSingleAssignmentVar<Void>();
		abort->addref(); // this leaks if abort is never set

		auto newFuture =
		    FutureInfo(abortableFuture(f.future, ThreadFuture<Void>(abort)), f.expectedValue, f.legalErrors);

		if (!abort->isReady() && deterministicRandom()->coinflip()) {
			ASSERT_EQ(abort->status, ThreadSingleAssignmentVarBase::Unset);
			newFuture.threads.push_back(g_network->startThread(setAbort, abort));
		}

		newFuture.legalErrors.insert(error_code_cluster_version_changed);
		return newFuture;
	}
};

TEST_CASE("/fdbclient/multiversionclient/AbortableSingleAssignmentVar") {
	state volatile bool done = false;
	g_network->startThread(runSingleAssignmentVarTest<AbortableTest>, (void*)&done);

	while (!done) {
		wait(delay(1.0));
	}

	return Void();
}

class CAPICallback : public ThreadCallback {
public:
	CAPICallback(void (*callbackf)(FdbCApi::FDBFuture*, void*), FdbCApi::FDBFuture* f, void* userdata)
	  : callbackf(callbackf), f(f), userdata(userdata) {}

	virtual bool canFire(int notMadeActive) { return true; }
	virtual void fire(const Void& unused, int& userParam) {
		(*callbackf)(f, userdata);
		delete this;
	}
	virtual void error(const Error& e, int& userParam) {
		(*callbackf)(f, userdata);
		delete this;
	}

private:
	void (*callbackf)(FdbCApi::FDBFuture*, void*);
	FdbCApi::FDBFuture* f;
	void* userdata;
};

struct DLTest {
	static FutureInfo createThreadFuture(FutureInfo f) {
		return FutureInfo(
		    toThreadFuture<int>(getApi(),
		                        (FdbCApi::FDBFuture*)f.future.extractPtr(),
		                        [](FdbCApi::FDBFuture* f, FdbCApi* api) {
			                        ASSERT_GE(((ThreadSingleAssignmentVar<int>*)f)->debugGetReferenceCount(), 1);
			                        return ((ThreadSingleAssignmentVar<int>*)f)->get();
		                        }),
		    f.expectedValue,
		    f.legalErrors);
	}

	static Reference<FdbCApi> getApi() {
		static Reference<FdbCApi> api;
		if (!api) {
			api = Reference<FdbCApi>(new FdbCApi());

			// Functions needed for DLSingleAssignmentVar
			api->futureSetCallback = [](FdbCApi::FDBFuture* f, FdbCApi::FDBCallback callback, void* callbackParameter) {
				try {
					CAPICallback* cb = new CAPICallback(callback, f, callbackParameter);
					int ignore;
					((ThreadSingleAssignmentVarBase*)f)->callOrSetAsCallback(cb, ignore, 0);
					return FdbCApi::fdb_error_t(error_code_success);
				} catch (Error& e) {
					return FdbCApi::fdb_error_t(e.code());
				}
			};
			api->futureCancel = [](FdbCApi::FDBFuture* f) {
				((ThreadSingleAssignmentVarBase*)f)->addref();
				((ThreadSingleAssignmentVarBase*)f)->cancel();
			};
			api->futureGetError = [](FdbCApi::FDBFuture* f) {
				return FdbCApi::fdb_error_t(((ThreadSingleAssignmentVarBase*)f)->getErrorCode());
			};
			api->futureDestroy = [](FdbCApi::FDBFuture* f) { ((ThreadSingleAssignmentVarBase*)f)->cancel(); };
		}

		return api;
	}
};

TEST_CASE("/fdbclient/multiversionclient/DLSingleAssignmentVar") {
	state volatile bool done = false;

	MultiVersionApi::api->callbackOnMainThread = true;
	g_network->startThread(runSingleAssignmentVarTest<DLTest>, (void*)&done);

	while (!done) {
		wait(delay(1.0));
	}

	done = false;
	MultiVersionApi::api->callbackOnMainThread = false;
	g_network->startThread(runSingleAssignmentVarTest<DLTest>, (void*)&done);

	while (!done) {
		wait(delay(1.0));
	}

	return Void();
}

struct MapTest {
	static FutureInfo createThreadFuture(FutureInfo f) {
		FutureInfo newFuture;
		newFuture.legalErrors = f.legalErrors;
		newFuture.future = mapThreadFuture<int, int>(f.future, [f, newFuture](ErrorOr<int> v) {
			if (v.isError()) {
				ASSERT(f.legalErrors.count(v.getError().code()) > 0 ||
				       (f.expectedValue.isError() && f.expectedValue.getError().code() == v.getError().code()));
			} else {
				ASSERT(!f.expectedValue.isError() && f.expectedValue.get() == v.get());
			}

			return newFuture.expectedValue;
		});

		return newFuture;
	}
};

TEST_CASE("/fdbclient/multiversionclient/MapSingleAssignmentVar") {
	state volatile bool done = false;
	g_network->startThread(runSingleAssignmentVarTest<MapTest>, (void*)&done);

	while (!done) {
		wait(delay(1.0));
	}

	return Void();
}

struct FlatMapTest {
	static FutureInfo createThreadFuture(FutureInfo f) {
		FutureInfo mapFuture = createVarOnMainThread();

		return FutureInfo(
		    flatMapThreadFuture<int, int>(
		        f.future,
		        [f, mapFuture](ErrorOr<int> v) {
			        if (v.isError()) {
				        ASSERT(f.legalErrors.count(v.getError().code()) > 0 ||
				               (f.expectedValue.isError() && f.expectedValue.getError().code() == v.getError().code()));
			        } else {
				        ASSERT(!f.expectedValue.isError() && f.expectedValue.get() == v.get());
			        }

			        if (mapFuture.expectedValue.isError() && deterministicRandom()->coinflip()) {
				        return ErrorOr<ThreadFuture<int>>(mapFuture.expectedValue.getError());
			        } else {
				        return ErrorOr<ThreadFuture<int>>(mapFuture.future);
			        }
		        }),
		    mapFuture.expectedValue,
		    f.legalErrors);
	}
};

TEST_CASE("/fdbclient/multiversionclient/FlatMapSingleAssignmentVar") {
	state volatile bool done = false;
	g_network->startThread(runSingleAssignmentVarTest<FlatMapTest>, (void*)&done);

	while (!done) {
		wait(delay(1.0));
	}

	return Void();
}
