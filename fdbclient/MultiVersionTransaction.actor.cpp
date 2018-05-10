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

#include "MultiVersionTransaction.h"
#include "MultiVersionAssignmentVars.h"
#include "ThreadSafeTransaction.h"

#include "flow/Platform.h"
#include "flow/UnitTest.h"

void throwIfError(FdbCApi::fdb_error_t e) {
	if(e) {
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
	FdbCApi::FDBFuture *f = api->transactionGetReadVersion(tr);

	return toThreadFuture<Version>(api, f, [](FdbCApi::FDBFuture *f, FdbCApi *api) {
		int64_t version;
		FdbCApi::fdb_error_t error = api->futureGetVersion(f, &version); 
		ASSERT(!error);
		return version;
	});
}

ThreadFuture<Optional<Value>> DLTransaction::get(const KeyRef& key, bool snapshot) {
	FdbCApi::FDBFuture *f = api->transactionGet(tr, key.begin(), key.size(), snapshot);

	return toThreadFuture<Optional<Value>>(api, f, [](FdbCApi::FDBFuture *f, FdbCApi *api) {
		FdbCApi::fdb_bool_t present;
		const uint8_t *value;
		int valueLength;
		FdbCApi::fdb_error_t error = api->futureGetValue(f, &present, &value, &valueLength);
		ASSERT(!error);
		if(present) {
			// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
			return Optional<Value>(Value(ValueRef(value, valueLength), Arena()));
		}
		else {
			return Optional<Value>();
		}
	});
}

ThreadFuture<Key> DLTransaction::getKey(const KeySelectorRef& key, bool snapshot) {
	FdbCApi::FDBFuture *f = api->transactionGetKey(tr, key.getKey().begin(), key.getKey().size(), key.orEqual, key.offset, snapshot);

	return toThreadFuture<Key>(api, f, [](FdbCApi::FDBFuture *f, FdbCApi *api) {
		const uint8_t *key;
		int keyLength;
		FdbCApi::fdb_error_t error = api->futureGetKey(f, &key, &keyLength);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Key(KeyRef(key, keyLength), Arena());
	});
}

ThreadFuture<Standalone<RangeResultRef>> DLTransaction::getRange(const KeySelectorRef& begin, const KeySelectorRef& end, int limit, bool snapshot, bool reverse) {
	return getRange(begin, end, GetRangeLimits(limit), snapshot, reverse);
}

ThreadFuture<Standalone<RangeResultRef>> DLTransaction::getRange(const KeySelectorRef& begin, const KeySelectorRef& end, GetRangeLimits limits, bool snapshot, bool reverse) {
	FdbCApi::FDBFuture *f = api->transactionGetRange(tr, begin.getKey().begin(), begin.getKey().size(), begin.orEqual, begin.offset, end.getKey().begin(), end.getKey().size(), end.orEqual, end.offset,
														limits.rows, limits.bytes, FDBStreamingModes::EXACT, 0, snapshot, reverse);
	return toThreadFuture<Standalone<RangeResultRef>>(api, f, [](FdbCApi::FDBFuture *f, FdbCApi *api) {
		const FdbCApi::FDBKeyValue *kvs;
		int count;
		FdbCApi::fdb_bool_t more;
		FdbCApi::fdb_error_t error = api->futureGetKeyValueArray(f, &kvs, &count, &more);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Standalone<RangeResultRef>(RangeResultRef(VectorRef<KeyValueRef>((KeyValueRef*)kvs, count), more), Arena());
	});
}

ThreadFuture<Standalone<RangeResultRef>> DLTransaction::getRange(const KeyRangeRef& keys, int limit, bool snapshot, bool reverse) {
	return getRange(firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), GetRangeLimits(limit), snapshot, reverse);
}

ThreadFuture<Standalone<RangeResultRef>> DLTransaction::getRange(const KeyRangeRef& keys, GetRangeLimits limits, bool snapshot, bool reverse) {
	return getRange(firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), limits, snapshot, reverse);
}

ThreadFuture<Standalone<VectorRef<const char*>>> DLTransaction::getAddressesForKey(const KeyRef& key) {
	FdbCApi::FDBFuture *f = api->transactionGetAddressesForKey(tr, key.begin(), key.size());

	return toThreadFuture<Standalone<VectorRef<const char*>>>(api, f, [](FdbCApi::FDBFuture *f, FdbCApi *api) {
		const char **addresses;
		int count;
		FdbCApi::fdb_error_t error = api->futureGetStringArray(f, &addresses, &count);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Standalone<VectorRef<const char*>>(VectorRef<const char*>(addresses, count), Arena());
	});
}

ThreadFuture<Standalone<StringRef>> DLTransaction::getVersionstamp() {
	if(!api->transactionGetVersionstamp) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture *f = api->transactionGetVersionstamp(tr);

	return toThreadFuture<Standalone<StringRef>>(api, f, [](FdbCApi::FDBFuture *f, FdbCApi *api) {
		const uint8_t *str;
		int strLength;
		FdbCApi::fdb_error_t error = api->futureGetKey(f, &str, &strLength);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Standalone<StringRef>(StringRef(str, strLength), Arena());
	});
}

void DLTransaction::addReadConflictRange(const KeyRangeRef& keys) {
	throwIfError(api->transactionAddConflictRange(tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size(), FDBConflictRangeTypes::READ));
}

void DLTransaction::atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) {
	api->transactionAtomicOp(tr, key.begin(), key.size(), value.begin(), value.size(), (FDBMutationTypes::Option)operationType);
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
	FdbCApi::FDBFuture *f = api->transactionWatch(tr, key.begin(), key.size());

	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture *f, FdbCApi *api) {
		return Void();
	});
}

void DLTransaction::addWriteConflictRange(const KeyRangeRef& keys) {
	throwIfError(api->transactionAddConflictRange(tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size(), FDBConflictRangeTypes::WRITE));
}

ThreadFuture<Void> DLTransaction::commit() {
	FdbCApi::FDBFuture *f = api->transactionCommit(tr);

	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture *f, FdbCApi *api) {
		return Void();
	});
}

Version DLTransaction::getCommittedVersion() {
	int64_t version;
	throwIfError(api->transactionGetCommittedVersion(tr, &version));
	return version;
}

void DLTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	throwIfError(api->transactionSetOption(tr, option, value.present() ? value.get().begin() : NULL, value.present() ? value.get().size() : 0));
}

ThreadFuture<Void> DLTransaction::onError(Error const& e) {
	FdbCApi::FDBFuture *f = api->transactionOnError(tr, e.code());

	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture *f, FdbCApi *api) {
		return Void();
	});
}

void DLTransaction::reset() {
	api->transactionReset(tr);
}

// DLDatabase
Reference<ITransaction> DLDatabase::createTransaction() {
	FdbCApi::FDBTransaction *tr;
	api->databaseCreateTransaction(db, &tr);
	return Reference<ITransaction>(new DLTransaction(api, tr));
}

void DLDatabase::setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	throwIfError(api->databaseSetOption(db, option, value.present() ? value.get().begin() : NULL, value.present() ? value.get().size() : 0));
}
	
// DLCluster
ThreadFuture<Reference<IDatabase>> DLCluster::createDatabase(Standalone<StringRef> dbName) {
	FdbCApi::FDBFuture *f = api->clusterCreateDatabase(cluster, (uint8_t*)dbName.toString().c_str(), dbName.size());

	return toThreadFuture<Reference<IDatabase>>(api, f, [](FdbCApi::FDBFuture *f, FdbCApi *api) {
		FdbCApi::FDBDatabase *db;
		api->futureGetDatabase(f, &db);
		return Reference<IDatabase>(new DLDatabase(Reference<FdbCApi>::addRef(api), db));
	});
}

void DLCluster::setOption(FDBClusterOptions::Option option, Optional<StringRef> value) {
	throwIfError(api->clusterSetOption(cluster, option, value.present() ? value.get().begin() : NULL, value.present() ? value.get().size() : 0));
}

// DLApi
template<class T>
void loadClientFunction(T *fp, void *lib, std::string libPath, const char *functionName, bool requireFunction = true) {
	*(void**)(fp) = loadFunction(lib, functionName);
	if(*fp == NULL && requireFunction) {
		TraceEvent(SevError, "ErrorLoadingFunction").detail("LibraryPath", libPath).detail("Function", functionName);
		throw platform_error();
	}
}

DLApi::DLApi(std::string fdbCPath) : api(new FdbCApi()), fdbCPath(fdbCPath), networkSetup(false) {}

void DLApi::init() {
	if(isLibraryLoaded(fdbCPath.c_str())) {
		throw external_client_already_loaded();
	}

	void* lib = loadLibrary(fdbCPath.c_str());
	if(lib == NULL) {
		TraceEvent(SevError, "ErrorLoadingExternalClientLibrary").detail("LibraryPath", fdbCPath);
		throw platform_error();
	}

	loadClientFunction(&api->selectApiVersion, lib, fdbCPath, "fdb_select_api_version_impl");
	loadClientFunction(&api->getClientVersion, lib, fdbCPath, "fdb_get_client_version", headerVersion >= 410);
	loadClientFunction(&api->setNetworkOption, lib, fdbCPath, "fdb_network_set_option");
	loadClientFunction(&api->setupNetwork, lib, fdbCPath, "fdb_setup_network");
	loadClientFunction(&api->runNetwork, lib, fdbCPath, "fdb_run_network");
	loadClientFunction(&api->stopNetwork, lib, fdbCPath, "fdb_stop_network");
	loadClientFunction(&api->createCluster, lib, fdbCPath, "fdb_create_cluster");

	loadClientFunction(&api->clusterCreateDatabase, lib, fdbCPath, "fdb_cluster_create_database");
	loadClientFunction(&api->clusterSetOption, lib, fdbCPath, "fdb_cluster_set_option");
	loadClientFunction(&api->clusterDestroy, lib, fdbCPath, "fdb_cluster_destroy");

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
	loadClientFunction(&api->transactionGetVersionstamp, lib, fdbCPath, "fdb_transaction_get_versionstamp", headerVersion >= 410);
	loadClientFunction(&api->transactionSet, lib, fdbCPath, "fdb_transaction_set");
	loadClientFunction(&api->transactionClear, lib, fdbCPath, "fdb_transaction_clear");
	loadClientFunction(&api->transactionClearRange, lib, fdbCPath, "fdb_transaction_clear_range");
	loadClientFunction(&api->transactionAtomicOp, lib, fdbCPath, "fdb_transaction_atomic_op");
	loadClientFunction(&api->transactionCommit, lib, fdbCPath, "fdb_transaction_commit");
	loadClientFunction(&api->transactionGetCommittedVersion, lib, fdbCPath, "fdb_transaction_get_committed_version");
	loadClientFunction(&api->transactionWatch, lib, fdbCPath, "fdb_transaction_watch");
	loadClientFunction(&api->transactionOnError, lib, fdbCPath, "fdb_transaction_on_error");
	loadClientFunction(&api->transactionReset, lib, fdbCPath, "fdb_transaction_reset");
	loadClientFunction(&api->transactionCancel, lib, fdbCPath, "fdb_transaction_cancel");
	loadClientFunction(&api->transactionAddConflictRange, lib, fdbCPath, "fdb_transaction_add_conflict_range");

	loadClientFunction(&api->futureGetCluster, lib, fdbCPath, "fdb_future_get_cluster");
	loadClientFunction(&api->futureGetDatabase, lib, fdbCPath, "fdb_future_get_database");
	loadClientFunction(&api->futureGetVersion, lib, fdbCPath, "fdb_future_get_version");
	loadClientFunction(&api->futureGetError, lib, fdbCPath, "fdb_future_get_error");
	loadClientFunction(&api->futureGetKey, lib, fdbCPath, "fdb_future_get_key");
	loadClientFunction(&api->futureGetValue, lib, fdbCPath, "fdb_future_get_value");
	loadClientFunction(&api->futureGetStringArray, lib, fdbCPath, "fdb_future_get_string_array");
	loadClientFunction(&api->futureGetKeyValueArray, lib, fdbCPath, "fdb_future_get_keyvalue_array");
	loadClientFunction(&api->futureSetCallback, lib, fdbCPath, "fdb_future_set_callback");
	loadClientFunction(&api->futureCancel, lib, fdbCPath, "fdb_future_cancel");
	loadClientFunction(&api->futureDestroy, lib, fdbCPath, "fdb_future_destroy");
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
	if(!api->getClientVersion) {
		return "unknown";
	}

	return api->getClientVersion();
}

void DLApi::setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	throwIfError(api->setNetworkOption(option, value.present() ? value.get().begin() : NULL, value.present() ? value.get().size() : 0));
}

void DLApi::setupNetwork() {
	networkSetup = true;
	throwIfError(api->setupNetwork());
}

void DLApi::runNetwork() {
	auto e = api->runNetwork();

	for(auto &hook : threadCompletionHooks) {
		try {
			hook.first(hook.second);
		}
		catch(Error &e) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(e);
		}
		catch(...) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(unknown_error());
		}
	}

	throwIfError(e);
}

void DLApi::stopNetwork() {
	if(networkSetup) {
		throwIfError(api->stopNetwork());
	}
}

ThreadFuture<Reference<ICluster>> DLApi::createCluster(const char *clusterFilePath) {
	FdbCApi::FDBFuture *f = api->createCluster(clusterFilePath);

	return toThreadFuture<Reference<ICluster>>(api, f, [](FdbCApi::FDBFuture *f, FdbCApi *api) {
		FdbCApi::FDBCluster *cluster;
		api->futureGetCluster(f, &cluster);
		return Reference<ICluster>(new DLCluster(Reference<FdbCApi>::addRef(api), cluster));
	});
}

void DLApi::addNetworkThreadCompletionHook(void (*hook)(void*), void *hookParameter) {
	MutexHolder holder(lock);
	threadCompletionHooks.push_back(std::make_pair(hook, hookParameter));
}

// MultiVersionTransaction
MultiVersionTransaction::MultiVersionTransaction(Reference<MultiVersionDatabase> db) : db(db) {
	updateTransaction();
}

// SOMEDAY: This function is unsafe if it's possible to set Database options that affect subsequently created transactions. There are currently no such options.
void MultiVersionTransaction::updateTransaction() {
	auto currentDb = db->dbState->dbVar->get();

	TransactionInfo newTr;
	if(currentDb.value) {
		newTr.transaction = currentDb.value->createTransaction();
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
	if(tr.transaction) {
		tr.transaction->cancel();
	}
}

void MultiVersionTransaction::setVersion(Version v) {
	auto tr = getTransaction();
	if(tr.transaction) {
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

ThreadFuture<Standalone<RangeResultRef>> MultiVersionTransaction::getRange(const KeySelectorRef& begin, const KeySelectorRef& end, int limit, bool snapshot, bool reverse) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getRange(begin, end, limit, snapshot, reverse) : ThreadFuture<Standalone<RangeResultRef>>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Standalone<RangeResultRef>> MultiVersionTransaction::getRange(const KeySelectorRef& begin, const KeySelectorRef& end, GetRangeLimits limits, bool snapshot, bool reverse) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getRange(begin, end, limits, snapshot, reverse) : ThreadFuture<Standalone<RangeResultRef>>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Standalone<RangeResultRef>> MultiVersionTransaction::getRange(const KeyRangeRef& keys, int limit, bool snapshot, bool reverse) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getRange(keys, limit, snapshot, reverse) : ThreadFuture<Standalone<RangeResultRef>>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Standalone<RangeResultRef>> MultiVersionTransaction::getRange(const KeyRangeRef& keys, GetRangeLimits limits, bool snapshot, bool reverse) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getRange(keys, limits, snapshot, reverse) : ThreadFuture<Standalone<RangeResultRef>>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Standalone<StringRef>> MultiVersionTransaction::getVersionstamp() {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getVersionstamp() : ThreadFuture<Standalone<StringRef>>(Never());
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<Standalone<VectorRef<const char*>>> MultiVersionTransaction::getAddressesForKey(const KeyRef& key) {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getAddressesForKey(key) : ThreadFuture<Standalone<VectorRef<const char*>>>(Never());
	return abortableFuture(f, tr.onChange);
}

void MultiVersionTransaction::addReadConflictRange(const KeyRangeRef& keys) {
	auto tr = getTransaction();
	if(tr.transaction) {
		tr.transaction->addReadConflictRange(keys);
	}
}

void MultiVersionTransaction::atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) {
	auto tr = getTransaction();
	if(tr.transaction) {
		tr.transaction->atomicOp(key, value, operationType);
	}
}

void MultiVersionTransaction::set(const KeyRef& key, const ValueRef& value) {
	auto tr = getTransaction();
	if(tr.transaction) {
		tr.transaction->set(key, value);
	}
}

void MultiVersionTransaction::clear(const KeyRef& begin, const KeyRef& end) {
	auto tr = getTransaction();
	if(tr.transaction) {
		tr.transaction->clear(begin, end);
	}
}

void MultiVersionTransaction::clear(const KeyRangeRef& range) {
	auto tr = getTransaction();
	if(tr.transaction) {
		tr.transaction->clear(range);
	}
}

void MultiVersionTransaction::clear(const KeyRef& key) {
	auto tr = getTransaction();
	if(tr.transaction) {
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
	if(tr.transaction) {
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
	if(tr.transaction) {
		return tr.transaction->getCommittedVersion();
	}

	return invalidVersion;
}

void MultiVersionTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	auto tr = getTransaction();
	if(tr.transaction) {
		tr.transaction->setOption(option, value);
	}
}

ThreadFuture<Void> MultiVersionTransaction::onError(Error const& e) {
	if(e.code() == error_code_cluster_version_changed) {
		updateTransaction();
		return ThreadFuture<Void>(Void());
	}
	else {
		auto tr = getTransaction();
		auto f = tr.transaction ? tr.transaction->onError(e) : ThreadFuture<Void>(Never());
		return abortableFuture(f, tr.onChange);
	}
}

void MultiVersionTransaction::reset() {
	updateTransaction();
}

// MultiVersionDatabase
MultiVersionDatabase::MultiVersionDatabase(Reference<MultiVersionCluster> cluster, Standalone<StringRef> dbName, Reference<IDatabase> db, ThreadFuture<Void> changed) 
	: dbState(new DatabaseState(cluster, dbName, db, changed)) {}

MultiVersionDatabase::~MultiVersionDatabase() {
	dbState->cancelCallbacks();
}

Reference<IDatabase> MultiVersionDatabase::debugCreateFromExistingDatabase(Reference<IDatabase> db) {
	auto cluster = Reference<ThreadSafeAsyncVar<Reference<ICluster>>>(new ThreadSafeAsyncVar<Reference<ICluster>>(Reference<ICluster>(NULL)));
	return Reference<IDatabase>(new MultiVersionDatabase(Reference<MultiVersionCluster>::addRef(new MultiVersionCluster()), LiteralStringRef("DB"), db, ThreadFuture<Void>(Never())));
}

Reference<ITransaction> MultiVersionDatabase::createTransaction() {
	return Reference<ITransaction>(new MultiVersionTransaction(Reference<MultiVersionDatabase>::addRef(this)));
}

void MultiVersionDatabase::setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	MutexHolder holder(dbState->optionLock);

	if(dbState->db) {
		dbState->db->setOption(option, value);
	}

	dbState->options.push_back(std::make_pair(option, value.cast_to<Standalone<StringRef>>()));
}

MultiVersionDatabase::DatabaseState::DatabaseState(Reference<MultiVersionCluster> cluster, Standalone<StringRef> dbName, Reference<IDatabase> db, ThreadFuture<Void> changed)
	: cluster(cluster), dbName(dbName), db(db), dbVar(new ThreadSafeAsyncVar<Reference<IDatabase>>(db)), cancelled(false), changed(changed)
{
	addref();
	int userParam;
	changed.callOrSetAsCallback(this, userParam, false);
}

void MultiVersionDatabase::DatabaseState::fire(const Void &unused, int& userParam) {
	onMainThreadVoid([this]() {
		if(!cancelled) {
			if(changed.isReady()) {
				updateDatabase();
			}
			else if(dbFuture.isValid() && dbFuture.isReady()) {
				auto newDb = dbFuture.get();

				optionLock.enter();
				bool optionFailed = false;
				for(auto option : options) {
					try {
						newDb->setOption(option.first, option.second.cast_to<StringRef>()); // In practice, this will set a deferred error instead of throwing. If that happens, the database will be unusable (all transaction operations will throw errors).
					}
					catch(Error &e) {
						optionFailed = true;
						TraceEvent(SevError, "DatabaseVersionChangeOptionError").detail("Option", option.first).detail("OptionValue", printable(option.second)).error(e);
					}
				}

				if(!optionFailed) {
					db = newDb;
				}
				else {
					// TODO: does this constitute a client failure?
					db = Reference<IDatabase>(NULL); // If we can't set all options on the database, just leave us disconnected until we switch clients again
				}
				optionLock.leave();

				dbVar->set(db);
				dbFuture.cancel();
			}
		}

		delref();
	}, NULL);
}

void MultiVersionDatabase::DatabaseState::error(const Error& e, int& userParam) {
	if(e.code() == error_code_operation_cancelled) {
		delref();
		return;
	}

	// TODO: retry?
	TraceEvent(SevWarnAlways, "DatabaseCreationFailed").error(e);
	onMainThreadVoid([this]() {
		updateDatabase();
		delref();
	}, NULL);
}

void MultiVersionDatabase::DatabaseState::updateDatabase() {
	auto currentCluster = cluster->clusterState->clusterVar->get();
	changed = currentCluster.onChange;

	addref();
	int userParam;
	changed.callOrSetAsCallback(this, userParam, false);

	if(dbFuture.isValid()) {
		dbFuture.cancel();
	}

	if(currentCluster.value) {
		addref();
		dbFuture = currentCluster.value->createDatabase(dbName);
		dbFuture.callOrSetAsCallback(this, userParam, false);
	}
}

void MultiVersionDatabase::DatabaseState::cancelCallbacks() {
	addref();
	onMainThreadVoid([this]() {
		cancelled = true;
		if(dbFuture.isValid()) {
			dbFuture.cancel();
		}
		if(changed.isValid() && changed.clearCallback(this)) {
			delref();
		}
		delref();
	}, NULL);
}

// MultiVersionCluster
MultiVersionCluster::MultiVersionCluster(MultiVersionApi *api, std::string clusterFilePath, Reference<ICluster> cluster) : clusterState(new ClusterState()) {
	clusterState->cluster = cluster;
	clusterState->clusterVar->set(cluster);

	if(!api->localClientDisabled) {
		clusterState->currentClientIndex = 0;
		clusterState->addConnection(api->getLocalClient(), clusterFilePath);
	}
	else {
		clusterState->currentClientIndex = -1;
	}

	api->runOnExternalClients([this, clusterFilePath](Reference<ClientInfo> client) {
		clusterState->addConnection(client, clusterFilePath);
	});

	clusterState->startConnections();
}

MultiVersionCluster::~MultiVersionCluster() {
	clusterState->cancelConnections();
}

ThreadFuture<Reference<IDatabase>> MultiVersionCluster::createDatabase(Standalone<StringRef> dbName) {
	auto cluster = clusterState->clusterVar->get();

	if(cluster.value) {
		ThreadFuture<Reference<IDatabase>> dbFuture = abortableFuture(cluster.value->createDatabase(dbName), cluster.onChange);

		return mapThreadFuture<Reference<IDatabase>, Reference<IDatabase>>(dbFuture, [this, cluster, dbName](ErrorOr<Reference<IDatabase>> db) {
			if(db.isError() && db.getError().code() != error_code_cluster_version_changed) {
				return db;
			}

			Reference<IDatabase> newDb = db.isError() ? Reference<IDatabase>(NULL) : db.get();
			return ErrorOr<Reference<IDatabase>>(Reference<IDatabase>(new MultiVersionDatabase(Reference<MultiVersionCluster>::addRef(this), dbName, newDb, cluster.onChange)));
		});
	}
	else {
		return Reference<IDatabase>(new MultiVersionDatabase(Reference<MultiVersionCluster>::addRef(this), dbName, Reference<IDatabase>(), cluster.onChange));
	}
}

void MultiVersionCluster::setOption(FDBClusterOptions::Option option, Optional<StringRef> value) {
	MutexHolder holder(clusterState->optionLock);

	if(clusterState->cluster) {
		clusterState->cluster->setOption(option, value);
	}

	clusterState->options.push_back(std::make_pair(option, value.cast_to<Standalone<StringRef>>()));
}

void MultiVersionCluster::Connector::connect() {
	addref();
	onMainThreadVoid([this]() {
		if(!cancelled) {
			connected = false;
			if(connectionFuture.isValid()) {
				connectionFuture.cancel();
			}

			auto clusterFuture = client->api->createCluster(clusterFilePath.c_str());
			auto dbFuture = flatMapThreadFuture<Reference<ICluster>, Reference<IDatabase>>(clusterFuture, [this](ErrorOr<Reference<ICluster>> cluster) {
				if(cluster.isError()) {
					return ErrorOr<ThreadFuture<Reference<IDatabase>>>(cluster.getError());
				}
				else {
					candidateCluster = cluster.get();
					return ErrorOr<ThreadFuture<Reference<IDatabase>>>(cluster.get()->createDatabase(LiteralStringRef("DB")));
				}
			});

			connectionFuture = flatMapThreadFuture<Reference<IDatabase>, Void>(dbFuture, [this](ErrorOr<Reference<IDatabase>> db) {
				if(db.isError()) {
					return ErrorOr<ThreadFuture<Void>>(db.getError());
				}
				else {
					tr = db.get()->createTransaction();
					auto versionFuture = mapThreadFuture<Version, Void>(tr->getReadVersion(), [this](ErrorOr<Version> v) {
						// If the version attempt returns an error, we regard that as a connection (except operation_cancelled)
						if(v.isError() && v.getError().code() == error_code_operation_cancelled) {
							return ErrorOr<Void>(v.getError());
						}
						else {
							return ErrorOr<Void>(Void());
						}
					});

					return ErrorOr<ThreadFuture<Void>>(versionFuture);
				}
			});

			int userParam;
			connectionFuture.callOrSetAsCallback(this, userParam, 0);
		}
		else {
			delref();
		}
	}, NULL);
}

// Only called from main thread
void MultiVersionCluster::Connector::cancel() {
	connected = false;
	cancelled = true;
	if(connectionFuture.isValid()) {
		connectionFuture.cancel();
	}
}

void MultiVersionCluster::Connector::fire(const Void &unused, int& userParam) {
	onMainThreadVoid([this]() {
		if(!cancelled) {
			connected = true;
			clusterState->stateChanged();
		}
		delref();
	}, NULL);
}

void MultiVersionCluster::Connector::error(const Error& e, int& userParam) {
	if(e.code() != error_code_operation_cancelled) {
		// TODO: is it right to abandon this connection attempt?
		client->failed = true;
		MultiVersionApi::api->updateSupportedVersions();
		TraceEvent(SevError, "ClusterConnectionError").detail("ClientLibrary", this->client->libPath).error(e);
	}

	delref();
}

// Only called from main thread
void MultiVersionCluster::ClusterState::stateChanged() {
	int newIndex = -1;
	for(int i = 0; i < clients.size(); ++i) {
		if(i != currentClientIndex && connectionAttempts[i]->connected) {
			if(currentClientIndex >= 0 && !clients[i]->canReplace(clients[currentClientIndex])) {
				TraceEvent(SevWarn, "DuplicateClientVersion").detail("Keeping", clients[currentClientIndex]->libPath).detail("KeptClientProtocolVersion", clients[currentClientIndex]->protocolVersion).detail("Disabling", clients[i]->libPath).detail("DisabledClientProtocolVersion", clients[i]->protocolVersion);
				connectionAttempts[i]->connected = false; // Permanently disable this client in favor of the current one
				clients[i]->failed = true;
				MultiVersionApi::api->updateSupportedVersions();
				return;
			}

			newIndex = i;
			break;
		}
	}

	if(newIndex == -1) {
		ASSERT(currentClientIndex == 0); // This can only happen for the local client, which we set as the current connection before we know it's connected
		return;
	}

	// Restart connection for replaced client
	auto newCluster = connectionAttempts[newIndex]->candidateCluster;

	optionLock.enter();
	for(auto option : options) {
		try {
			newCluster->setOption(option.first, option.second.cast_to<StringRef>()); // In practice, this will set a deferred error instead of throwing. If that happens, the cluster will be unusable (attempts to use it will throw errors).
		}
		catch(Error &e) {
			optionLock.leave();
			TraceEvent(SevError, "ClusterVersionChangeOptionError").detail("Option", option.first).detail("OptionValue", printable(option.second)).detail("LibPath", clients[newIndex]->libPath).error(e);
			connectionAttempts[newIndex]->connected = false;
			clients[newIndex]->failed = true;
			MultiVersionApi::api->updateSupportedVersions();
			return; // If we can't set all of the options on a cluster, we abandon the client
		}
	}

	cluster = newCluster;
	optionLock.leave();

	clusterVar->set(cluster);

	if(currentClientIndex >= 0 && connectionAttempts[currentClientIndex]->connected) {
		connectionAttempts[currentClientIndex]->connected = false;
		connectionAttempts[currentClientIndex]->connect();
	}

	ASSERT(newIndex >= 0 && newIndex < clients.size());
	currentClientIndex = newIndex;
}

void MultiVersionCluster::ClusterState::addConnection(Reference<ClientInfo> client, std::string clusterFilePath) {
	clients.push_back(client);
	connectionAttempts.push_back(Reference<Connector>(new Connector(Reference<ClusterState>::addRef(this), client, clusterFilePath)));
}

void MultiVersionCluster::ClusterState::startConnections() {
	for(auto c : connectionAttempts) {
		c->connect();
	}
}

void MultiVersionCluster::ClusterState::cancelConnections() {
	addref();
	onMainThreadVoid([this](){
		for(auto c : connectionAttempts) {
			c->cancel();
		}

		connectionAttempts.clear();
		clients.clear();
		delref();
	}, NULL);
}

// MultiVersionApi

// runOnFailedClients should be used cautiously. Some failed clients may not have successfully loaded all symbols.
void MultiVersionApi::runOnExternalClients(std::function<void(Reference<ClientInfo>)> func, bool runOnFailedClients) {
	bool newFailure = false;

	auto c = externalClients.begin();
	while(c != externalClients.end()) {
		try {
			if(!c->second->failed || runOnFailedClients) { // TODO: Should we ignore some failures?
				func(c->second);
			}
		}
		catch(Error &e) {
			TraceEvent(SevWarnAlways, "ExternalClientFailure").detail("LibPath", c->second->libPath).error(e);
			if(e.code() == error_code_external_client_already_loaded) {
				c = externalClients.erase(c);
				continue;
			}
			else {
				c->second->failed = true;
				newFailure = true;
			}
		}

		++c;
	}

	if(newFailure) {
		updateSupportedVersions();
	}
}

Reference<ClientInfo> MultiVersionApi::getLocalClient() {
	return localClient;
}

void MultiVersionApi::selectApiVersion(int apiVersion) {
	if(!localClient) {
		localClient = Reference<ClientInfo>(new ClientInfo(ThreadSafeApi::api));
	}

	if(this->apiVersion != 0 && this->apiVersion != apiVersion) {
		throw api_version_already_set();
	}

	localClient->api->selectApiVersion(apiVersion);
	this->apiVersion = apiVersion;
}

const char* MultiVersionApi::getClientVersion() {
	return localClient->api->getClientVersion();
}

void validateOption(Optional<StringRef> value, bool canBePresent, bool canBeAbsent, bool canBeEmpty=true) {
	ASSERT(canBePresent || canBeAbsent);

	if(!canBePresent && value.present() && (!canBeEmpty || value.get().size() > 0)) {
		throw invalid_option_value();
	}
	if(!canBeAbsent && (!value.present() || (!canBeEmpty && value.get().size() == 0))) {
		throw invalid_option_value();
	}
}

void MultiVersionApi::disableMultiVersionClientApi() {
	MutexHolder holder(lock);
	if(networkStartSetup || localClientDisabled) {
		throw invalid_option();
	}

	bypassMultiClientApi = true;
}

void MultiVersionApi::setCallbacksOnExternalThreads() {
	MutexHolder holder(lock);
	if(networkStartSetup) {
		throw invalid_option();
	}

	callbackOnMainThread = false;
}

void MultiVersionApi::addExternalLibrary(std::string path) {
	std::string filename = basename(path);

	if(filename.empty() || !fileExists(path)) {
		throw file_not_found();
	}

	MutexHolder holder(lock);
	if(networkStartSetup) {
		throw invalid_option(); // SOMEDAY: it might be good to allow clients to be added after the network is setup
	}

	if(externalClients.count(filename) == 0) {
		TraceEvent("AddingExternalClient").detail("LibraryPath", filename);
		externalClients[filename] = Reference<ClientInfo>(new ClientInfo(new DLApi(path), path));
	}
}

void MultiVersionApi::addExternalLibraryDirectory(std::string path) {
	TraceEvent("AddingExternalClientDirectory").detail("Directory", path);
	std::vector<std::string> files = platform::listFiles(path, DYNAMIC_LIB_EXT);

	MutexHolder holder(lock);
	if(networkStartSetup) {
		throw invalid_option(); // SOMEDAY: it might be good to allow clients to be added after the network is setup. For directories, we can monitor them for the addition of new files.
	}

	for(auto filename : files) {
		std::string lib = abspath(joinPath(path, filename));
		if(externalClients.count(filename) == 0) {
			TraceEvent("AddingExternalClient").detail("LibraryPath", filename);
			externalClients[filename] = Reference<ClientInfo>(new ClientInfo(new DLApi(lib), lib));
		}	
	}
}

void MultiVersionApi::disableLocalClient() {
	MutexHolder holder(lock);
	if(networkStartSetup || bypassMultiClientApi) {
		throw invalid_option();
	}

	localClientDisabled = true;
}

void MultiVersionApi::setSupportedClientVersions(Standalone<StringRef> versions) {
	MutexHolder holder(lock);
	ASSERT(networkSetup);

	// This option must be set on the main thread because it modifes structures that can be used concurrently by the main thread
	onMainThreadVoid([this, versions](){
		localClient->api->setNetworkOption(FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS, versions);
	}, NULL);

	if(!bypassMultiClientApi) {
		runOnExternalClients([this, versions](Reference<ClientInfo> client){
			client->api->setNetworkOption(FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS, versions);
		});
	}
}

void MultiVersionApi::setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	if(option != FDBNetworkOptions::EXTERNAL_CLIENT && !externalClient) { // This is the first option set for external clients
		loadEnvironmentVariableNetworkOptions();
	}

	setNetworkOptionInternal(option, value);
}

void MultiVersionApi::setNetworkOptionInternal(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	if(option == FDBNetworkOptions::DISABLE_MULTI_VERSION_CLIENT_API) {
		validateOption(value, false, true);
		disableMultiVersionClientApi();
	}
	else if(option == FDBNetworkOptions::CALLBACKS_ON_EXTERNAL_THREADS) {
		validateOption(value, false, true);
		setCallbacksOnExternalThreads();
	}
	else if(option == FDBNetworkOptions::EXTERNAL_CLIENT_LIBRARY) {
		validateOption(value, true, false, false);
		addExternalLibrary(abspath(value.get().toString()));
	}
	else if(option == FDBNetworkOptions::EXTERNAL_CLIENT_DIRECTORY) {
		validateOption(value, true, false, false);
		addExternalLibraryDirectory(value.get().toString());
	}
	else if(option == FDBNetworkOptions::DISABLE_LOCAL_CLIENT) {
		validateOption(value, false, true);
		disableLocalClient();
	}
	else if(option == FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS) {
		ASSERT(value.present());
		setSupportedClientVersions(value.get());
	}
	else if(option == FDBNetworkOptions::EXTERNAL_CLIENT) {
		MutexHolder holder(lock);
		ASSERT(!value.present() && !networkStartSetup);
		externalClient = true;
		bypassMultiClientApi = true;
	}
	else {
		MutexHolder holder(lock);
		localClient->api->setNetworkOption(option, value);

		if(!bypassMultiClientApi) {
			if(networkSetup) {
				runOnExternalClients([this, option, value](Reference<ClientInfo> client) {
					client->api->setNetworkOption(option, value);
				});
			}
			else {
				options.push_back(std::make_pair(option, value.cast_to<Standalone<StringRef>>()));
			}
		}
	}
}

void MultiVersionApi::setupNetwork() {
	if(!externalClient) {
		loadEnvironmentVariableNetworkOptions();
	}

	uint64_t transportId = 0;
	{ // lock scope
		MutexHolder holder(lock);
		if(networkStartSetup) {
			throw network_already_setup();
		}

		networkStartSetup = true;

		if(externalClients.empty()) {
			bypassMultiClientApi = true; // SOMEDAY: we won't be able to set this option once it becomes possible to add clients after setupNetwork is called
		}

		if(!bypassMultiClientApi) {
			transportId = (uint64_t(uint32_t(platform::getRandomSeed())) << 32) ^ uint32_t(platform::getRandomSeed());
			if(transportId <= 1) transportId += 2;
			localClient->api->setNetworkOption(FDBNetworkOptions::EXTERNAL_CLIENT_TRANSPORT_ID, std::to_string(transportId));
		}
		localClient->api->setupNetwork();
	}

	localClient->loadProtocolVersion();

	if(!bypassMultiClientApi) {
		runOnExternalClients([this](Reference<ClientInfo> client) {
			TraceEvent("InitializingExternalClient").detail("LibraryPath", client->libPath);
			client->api->selectApiVersion(apiVersion);
			client->loadProtocolVersion();
		});

		MutexHolder holder(lock);
		runOnExternalClients([this, transportId](Reference<ClientInfo> client) {
			for(auto option : options) {
				client->api->setNetworkOption(option.first, option.second.cast_to<StringRef>());
			}
			client->api->setNetworkOption(FDBNetworkOptions::EXTERNAL_CLIENT_TRANSPORT_ID, std::to_string(transportId));

			client->api->setupNetwork();
		});

		networkSetup = true; // Needs to be guarded by mutex
	}
	else {
		networkSetup = true;
	}

	options.clear();
	updateSupportedVersions();
}

THREAD_FUNC_RETURN runNetworkThread(void *param) {
	try {
		((ClientInfo*)param)->api->runNetwork();
	}
	catch(Error &e) {
		TraceEvent(SevError, "RunNetworkError").error(e);
	}

	THREAD_RETURN;
}

void MultiVersionApi::runNetwork() {
	lock.enter();
	if(!networkSetup) {
		lock.leave();
		throw network_not_setup();
	}

	lock.leave();

	std::vector<THREAD_HANDLE> handles;
	if(!bypassMultiClientApi) {
		runOnExternalClients([&handles](Reference<ClientInfo> client) {
			if(client->external) {
				handles.push_back(g_network->startThread(&runNetworkThread, client.getPtr()));
			}
		});
	}

	localClient->api->runNetwork();

	for(auto h : handles) {
		waitThread(h);
	}
}

void MultiVersionApi::stopNetwork() {
	lock.enter();
	if(!networkSetup) {
		lock.leave();
		throw network_not_setup();
	}
	lock.leave();

	localClient->api->stopNetwork();

	if(!bypassMultiClientApi) {
		runOnExternalClients([](Reference<ClientInfo> client) {
			client->api->stopNetwork();
		}, true);
	}
}

void MultiVersionApi::addNetworkThreadCompletionHook(void (*hook)(void*), void *hookParameter) {
	lock.enter();
	if(!networkSetup) {
		lock.leave();
		throw network_not_setup();
	}
	lock.leave();

	localClient->api->addNetworkThreadCompletionHook(hook, hookParameter);

	if(!bypassMultiClientApi) {
		runOnExternalClients([hook, hookParameter](Reference<ClientInfo> client) {
			client->api->addNetworkThreadCompletionHook(hook, hookParameter);
		});
	}
}

ThreadFuture<Reference<ICluster>> MultiVersionApi::createCluster(const char *clusterFilePath) {
	lock.enter();
	if(!networkSetup) {
		lock.leave();
		return network_not_setup();
	}
	lock.leave();

	std::string clusterFile(clusterFilePath);
	if(localClientDisabled) {
		return Reference<ICluster>(new MultiVersionCluster(this, clusterFile, Reference<ICluster>()));
	}

	auto clusterFuture = localClient->api->createCluster(clusterFilePath);
	if(bypassMultiClientApi) {
		return clusterFuture;
	}
	else {
		for( auto it : externalClients ) {
			TraceEvent("CreatingClusterOnExternalClient").detail("LibraryPath", it.second->libPath).detail("failed", it.second->failed);
		}
		return mapThreadFuture<Reference<ICluster>, Reference<ICluster>>(clusterFuture, [this, clusterFile](ErrorOr<Reference<ICluster>> cluster) {
			if(cluster.isError()) {
				return cluster;
			}

			return ErrorOr<Reference<ICluster>>(Reference<ICluster>(new MultiVersionCluster(this, clusterFile, cluster.get())));
		});
	}
}

void MultiVersionApi::updateSupportedVersions() {
	if(networkSetup) {
		Standalone<VectorRef<uint8_t>> versionStr;

		runOnExternalClients([&versionStr](Reference<ClientInfo> client){
			const char *ver = client->api->getClientVersion();
			versionStr.append(versionStr.arena(), (uint8_t*)ver, (int)strlen(ver));
			versionStr.append(versionStr.arena(), (uint8_t*)";", 1);
		});

		if(!localClient->failed) {
			const char *local = localClient->api->getClientVersion();
			versionStr.append(versionStr.arena(), (uint8_t*)local, (int)strlen(local));
		}
		else {
			versionStr.resize(versionStr.arena(), std::max(0, versionStr.size()-1));
		}

		setNetworkOption(FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS, StringRef(versionStr.begin(), versionStr.size()));
	}
}

std::vector<std::string> parseOptionValues(std::string valueStr) {
	std::string specialCharacters = "\\";
	specialCharacters += ENV_VAR_PATH_SEPARATOR;

	std::vector<std::string> values;

	size_t index = 0;
	size_t nextIndex = 0;
	std::stringstream ss;
	while(true) {
		nextIndex = valueStr.find_first_of(specialCharacters, index);
		char c = nextIndex == valueStr.npos ? ENV_VAR_PATH_SEPARATOR : valueStr[nextIndex];
		
		if(c == '\\') {
			if(valueStr.size() == nextIndex + 1 || specialCharacters.find(valueStr[nextIndex+1]) == valueStr.npos) {
				throw invalid_option_value();
			}

			ss << valueStr.substr(index, nextIndex-index);
			ss << valueStr[nextIndex+1];

			index = nextIndex + 2;
		}
		else if(c == ENV_VAR_PATH_SEPARATOR) {
			ss << valueStr.substr(index, nextIndex-index);
			values.push_back(ss.str());
			ss.str(std::string());

			if(nextIndex == valueStr.npos) {
				break;
			}
			index = nextIndex + 1;
		}
		else {
			ASSERT(false);
		}
	}

	return values;
}

// This function sets all environment variable options which have not been set previously by a call to this function.
// If an option has multiple values and setting one of those values failed with an error, then only those options
// which were not successfully set will be set on subsequent calls.
void MultiVersionApi::loadEnvironmentVariableNetworkOptions() {
	if(envOptionsLoaded) {
		return;
	}

	for(auto option : FDBNetworkOptions::optionInfo) {
		if(!option.second.hidden) {
			std::string valueStr;
			try {
				if(platform::getEnvironmentVar(("FDB_NETWORK_OPTION_" + option.second.name).c_str(), valueStr)) {
					size_t index = 0;
					for(auto value : parseOptionValues(valueStr)) {
						Standalone<StringRef> currentValue = StringRef(value);
						{ // lock scope
							MutexHolder holder(lock);
							if(setEnvOptions[option.first].count(currentValue) == 0) {
								setNetworkOptionInternal(option.first, currentValue);
								setEnvOptions[option.first].insert(currentValue);
							}
						}
					}
				}
			}
			catch(Error &e) {
				TraceEvent(SevError, "EnvironmentVariableNetworkOptionFailed").detail("Option", option.second.name).detail("Value", valueStr).error(e);
				throw environment_variable_network_option_failed();
			}
		}
	}

	MutexHolder holder(lock);
	envOptionsLoaded = true;
}

MultiVersionApi::MultiVersionApi() : bypassMultiClientApi(false), networkStartSetup(false), networkSetup(false), callbackOnMainThread(true), externalClient(false), localClientDisabled(false), apiVersion(0), envOptionsLoaded(false) {}

MultiVersionApi* MultiVersionApi::api = new MultiVersionApi();

// ClientInfo
void ClientInfo::loadProtocolVersion() {
	std::string version = api->getClientVersion();
	if(version == "unknown") {
		protocolVersion = 0;
		return;
	}

	char *next;
	std::string protocolVersionStr = ClientVersionRef(version).protocolVersion.toString();
	protocolVersion = strtoull(protocolVersionStr.c_str(), &next, 16);

	ASSERT(protocolVersion != 0 && protocolVersion != ULLONG_MAX);
	ASSERT(next == &protocolVersionStr[protocolVersionStr.length()]);
}

bool ClientInfo::canReplace(Reference<ClientInfo> other) const {
	if(protocolVersion > other->protocolVersion) {
		return true;
	}

	if(protocolVersion == other->protocolVersion && !external) {
		return true;
	}

	return (protocolVersion & compatibleProtocolVersionMask) != (other->protocolVersion & compatibleProtocolVersionMask);
}

// UNIT TESTS
extern bool noUnseed;

TEST_CASE( "fdbclient/multiversionclient/EnvironmentVariableParsing" ) {
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
	}
	catch(Error &e) {
		ASSERT(e.code() == error_code_invalid_option_value);
	}

	return Void();
}

class ValidateFuture : public ThreadCallback {
public:
	ValidateFuture(ThreadFuture<int> f, ErrorOr<int> expectedValue, std::set<int> legalErrors) : f(f), expectedValue(expectedValue), legalErrors(legalErrors) { }

	virtual bool canFire(int notMadeActive) { return true; }

	virtual void fire(const Void &unused, int& userParam) {
		ASSERT(!f.isError() && !expectedValue.isError() && f.get() == expectedValue.get());
		delete this;
	}

	virtual void error(const Error& e, int& userParam) {
		ASSERT(legalErrors.count(e.code()) > 0 || (f.isError() && expectedValue.isError() && f.getError().code() == expectedValue.getError().code()));
		delete this;
	}

private:
	ThreadFuture<int> f;
	ErrorOr<int> expectedValue;
	std::set<int> legalErrors;
};

struct FutureInfo {
	FutureInfo() {
		if(g_random->coinflip()) {
			expectedValue = Error(g_random->randomInt(1, 100));
		}
		else {
			expectedValue = g_random->randomInt(0, 100);
		}
	}

	FutureInfo(ThreadFuture<int> future, ErrorOr<int> expectedValue, std::set<int> legalErrors = std::set<int>()) : future(future), expectedValue(expectedValue), legalErrors(legalErrors) {}

	void validate() {
		int userParam;
		future.callOrSetAsCallback(new ValidateFuture(future, expectedValue, legalErrors), userParam, 0);
	}

	ThreadFuture<int> future;
	ErrorOr<int> expectedValue;
	std::set<int> legalErrors;
	std::vector<THREAD_HANDLE> threads;
};

FutureInfo createVarOnMainThread(bool canBeNever=true) {
	FutureInfo f;
	
	if(g_random->coinflip()) {
		f.future = onMainThread([f, canBeNever]() {
			Future<Void> sleep ;
			if(canBeNever && g_random->coinflip()) {
				sleep = Never();
			}
			else {
				sleep = delay(0.1 * g_random->random01());
			}

			if(f.expectedValue.isError()) {
				return tagError<int>(sleep, f.expectedValue.getError());
			}
			else {
				return tag(sleep, f.expectedValue.get());
			}
		});
	}
	else if(f.expectedValue.isError()) {
		f.future = f.expectedValue.getError();
	}
	else {
		f.future = f.expectedValue.get();
	}

	return f;
}

THREAD_FUNC setAbort(void *arg) {
	threadSleep(0.1 * g_random->random01());
	try {
		((ThreadSingleAssignmentVar<Void>*)arg)->send(Void());
		((ThreadSingleAssignmentVar<Void>*)arg)->delref();
	}
	catch(Error &e) {
		printf("Caught error in setAbort: %s\n", e.name());
		ASSERT(false);
	}
	THREAD_RETURN;
}

THREAD_FUNC releaseMem(void *arg) {
	threadSleep(0.1 * g_random->random01());
	try {
		// Must get for releaseMemory to work
		((ThreadSingleAssignmentVar<int>*)arg)->get();
	}
	catch(Error &e) {
		// Swallow
	}
	try {
		((ThreadSingleAssignmentVar<int>*)arg)->releaseMemory();
	}
	catch(Error &e) {
		printf("Caught error in releaseMem: %s\n", e.name());
		ASSERT(false);
	}
	THREAD_RETURN;
}

THREAD_FUNC destroy(void *arg) {
	threadSleep(0.1 * g_random->random01());
	try {
		((ThreadSingleAssignmentVar<int>*)arg)->cancel();
	}
	catch(Error &e) {
		printf("Caught error in destroy: %s\n", e.name());
		ASSERT(false);
	}
	THREAD_RETURN;
}

THREAD_FUNC cancel(void *arg) {
	threadSleep(0.1 * g_random->random01());
	try {
		((ThreadSingleAssignmentVar<int>*)arg)->addref();
		destroy(arg);
	}
	catch(Error &e) {
		printf("Caught error in cancel: %s\n", e.name());
		ASSERT(false);
	}
	THREAD_RETURN;
}

ACTOR Future<Void> checkUndestroyedFutures(std::vector<ThreadSingleAssignmentVar<int>*> undestroyed) {
	state int fNum;
	state ThreadSingleAssignmentVar<int>* f;
	state double start = now();

	for(fNum = 0; fNum < undestroyed.size(); ++fNum) {
		f = undestroyed[fNum];
		
		while(!f->isReady() && start+5 >= now()) {
			Void _ = wait(delay(1.0));
		}

		ASSERT(f->isReady());
	}

	Void _ = wait(delay(1.0));

	for(fNum = 0; fNum < undestroyed.size(); ++fNum) {
		f = undestroyed[fNum];

		ASSERT(f->debugGetReferenceCount() == 1);
		ASSERT(f->isReady());

		f->cancel();
	}

	return Void();
}

template<class T>
THREAD_FUNC runSingleAssignmentVarTest(void *arg) {
	noUnseed = true;

	volatile bool *done = (volatile bool*)arg;
	try {
		for(int i = 0; i < 25; ++i) {
			FutureInfo f = createVarOnMainThread(false);
			FutureInfo tf = T::createThreadFuture(f);
			tf.validate();

			tf.future.extractPtr(); // leaks
		}

		for(int numRuns = 0; numRuns < 25; ++numRuns) {
			std::vector<ThreadSingleAssignmentVar<int>*> undestroyed;
			std::vector<THREAD_HANDLE> threads;
			for(int i = 0; i < 10; ++i) {
				FutureInfo f = createVarOnMainThread();
				f.legalErrors.insert(error_code_operation_cancelled);

				FutureInfo tf = T::createThreadFuture(f); 
				for(auto t : tf.threads) {
					threads.push_back(t);
				}

				tf.legalErrors.insert(error_code_operation_cancelled);
				tf.validate();

				auto tfp = tf.future.extractPtr();

				if(g_random->coinflip()) {
					if(g_random->coinflip()) {
						threads.push_back(g_network->startThread(releaseMem, tfp));
					}
					threads.push_back(g_network->startThread(cancel, tfp));
					undestroyed.push_back((ThreadSingleAssignmentVar<int>*)tfp);
				}
				else {
					threads.push_back(g_network->startThread(destroy, tfp));
				}
			}

			for(auto t : threads) {
				waitThread(t);
			}

			ThreadFuture<Void> checkUndestroyed = onMainThread([undestroyed]() {
				return checkUndestroyedFutures(undestroyed);
			});

			checkUndestroyed.blockUntilReady();
		}

		onMainThreadVoid([done](){
			*done = true;
		}, NULL);
	}
	catch(Error &e) {
		printf("Caught error in test: %s\n", e.name());
		*done = true;
		ASSERT(false);
	}

	THREAD_RETURN;
}

struct AbortableTest {
	static FutureInfo createThreadFuture(FutureInfo f) {
		ThreadSingleAssignmentVar<Void> *abort = new ThreadSingleAssignmentVar<Void>();
		abort->addref(); // this leaks if abort is never set

		auto newFuture = FutureInfo(abortableFuture(f.future, ThreadFuture<Void>(abort)), f.expectedValue, f.legalErrors);

		if(!abort->isReady() && g_random->coinflip()) {
			ASSERT(abort->status == ThreadSingleAssignmentVarBase::Unset);
			newFuture.threads.push_back(g_network->startThread(setAbort, abort));
		}

		newFuture.legalErrors.insert(error_code_cluster_version_changed);
		return newFuture;
	}
};

TEST_CASE( "fdbclient/multiversionclient/AbortableSingleAssignmentVar" ) {
	state volatile bool done = false;
	g_network->startThread(runSingleAssignmentVarTest<AbortableTest>, (void*)&done);

	while(!done) {
		Void _ = wait(delay(1.0));
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
		return FutureInfo(toThreadFuture<int>(getApi(), (FdbCApi::FDBFuture*)f.future.extractPtr(), [](FdbCApi::FDBFuture *f, FdbCApi *api) {
			ASSERT(((ThreadSingleAssignmentVar<int>*)f)->debugGetReferenceCount() >= 1);
			return ((ThreadSingleAssignmentVar<int>*)f)->get();
		}), f.expectedValue, f.legalErrors);
	}

	static Reference<FdbCApi> getApi() {
		static Reference<FdbCApi> api;
		if(!api) {
			api = Reference<FdbCApi>(new FdbCApi());

			// Functions needed for DLSingleAssignmentVar
			api->futureSetCallback = [](FdbCApi::FDBFuture *f, FdbCApi::FDBCallback callback, void *callbackParameter) {  
				try {
					CAPICallback* cb = new CAPICallback(callback, f, callbackParameter);
					int ignore;
					((ThreadSingleAssignmentVarBase*)f)->callOrSetAsCallback(cb, ignore, 0);
					return FdbCApi::fdb_error_t(error_code_success);
				}
				catch(Error &e) {
					return FdbCApi::fdb_error_t(e.code());
				}
			};
			api->futureCancel = [](FdbCApi::FDBFuture *f) { 
				((ThreadSingleAssignmentVarBase*)f)->addref(); 
				((ThreadSingleAssignmentVarBase*)f)->cancel(); 
			};
			api->futureGetError = [](FdbCApi::FDBFuture *f) { return FdbCApi::fdb_error_t(((ThreadSingleAssignmentVarBase*)f)->getErrorCode()); };
			api->futureDestroy = [](FdbCApi::FDBFuture *f) { ((ThreadSingleAssignmentVarBase*)f)->cancel(); };
		}

		return api;
	}
};

TEST_CASE( "fdbclient/multiversionclient/DLSingleAssignmentVar" ) {
	state volatile bool done = false;

	MultiVersionApi::api->callbackOnMainThread = true;
	g_network->startThread(runSingleAssignmentVarTest<DLTest>, (void*)&done);

	while(!done) {
		Void _ = wait(delay(1.0));
	}

	done = false;
	MultiVersionApi::api->callbackOnMainThread = false;
	g_network->startThread(runSingleAssignmentVarTest<DLTest>, (void*)&done);

	while(!done) {
		Void _ = wait(delay(1.0));
	}

	return Void();
}

struct MapTest {
	static FutureInfo createThreadFuture(FutureInfo f) {
		FutureInfo newFuture;
		newFuture.legalErrors = f.legalErrors;
		newFuture.future = mapThreadFuture<int, int>(f.future, [f, newFuture](ErrorOr<int> v) {
			if(v.isError()) {
				ASSERT(f.legalErrors.count(v.getError().code()) > 0 || (f.expectedValue.isError() && f.expectedValue.getError().code() == v.getError().code()));
			}
			else {
				ASSERT(!f.expectedValue.isError() && f.expectedValue.get() == v.get());
			}

			return newFuture.expectedValue;
		});

		return newFuture;
	}
};

TEST_CASE( "fdbclient/multiversionclient/MapSingleAssignmentVar" ) {
	state volatile bool done = false;
	g_network->startThread(runSingleAssignmentVarTest<MapTest>, (void*)&done);

	while(!done) {
		Void _ = wait(delay(1.0));
	}

	return Void();
}

struct FlatMapTest {
	static FutureInfo createThreadFuture(FutureInfo f) {
		FutureInfo mapFuture = createVarOnMainThread();

		return FutureInfo(flatMapThreadFuture<int, int>(f.future, [f, mapFuture](ErrorOr<int> v) {
			if(v.isError()) {
				ASSERT(f.legalErrors.count(v.getError().code()) > 0 || (f.expectedValue.isError() && f.expectedValue.getError().code() == v.getError().code()));
			}
			else {
				ASSERT(!f.expectedValue.isError() && f.expectedValue.get() == v.get());
			}

			if(mapFuture.expectedValue.isError() && g_random->coinflip()) {
				return ErrorOr<ThreadFuture<int>>(mapFuture.expectedValue.getError());
			}
			else {
				return ErrorOr<ThreadFuture<int>>(mapFuture.future);
			}
		}), mapFuture.expectedValue, f.legalErrors);
	}
};

TEST_CASE( "fdbclient/multiversionclient/FlatMapSingleAssignmentVar" ) {
	state volatile bool done = false;
	g_network->startThread(runSingleAssignmentVarTest<FlatMapTest>, (void*)&done);

	while(!done) {
		Void _ = wait(delay(1.0));
	}

	return Void();
}
