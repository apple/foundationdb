/*
 * ThreadSafeTransaction.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/ThreadSafeTransaction.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/versions.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/Arena.h"
#include "flow/ProtocolVersion.h"

// Users of ThreadSafeTransaction might share Reference<ThreadSafe...> between different threads as long as they don't
// call addRef (e.g. C API follows this). Therefore, it is unsafe to call (explicitly or implicitly) this->addRef in any
// of these functions.

ThreadFuture<Void> ThreadSafeDatabase::onConnected() {
	DatabaseContext* db = this->db;
	return onMainThread([db]() -> Future<Void> {
		db->checkDeferredError();
		return db->onConnected();
	});
}

ThreadFuture<Reference<IDatabase>> ThreadSafeDatabase::createFromExistingDatabase(Database db) {
	return onMainThread([db]() {
		db->checkDeferredError();
		DatabaseContext* cx = db.getPtr();
		cx->addref();
		return Future<Reference<IDatabase>>(Reference<IDatabase>(new ThreadSafeDatabase(cx)));
	});
}

Reference<ITransaction> ThreadSafeDatabase::createTransaction() {
	return Reference<ITransaction>(new ThreadSafeTransaction(db));
}

void ThreadSafeDatabase::setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	auto itr = FDBDatabaseOptions::optionInfo.find(option);
	if (itr != FDBDatabaseOptions::optionInfo.end()) {
		TraceEvent("SetDatabaseOption").detail("Option", itr->second.name);
	} else {
		TraceEvent("UnknownDatabaseOption").detail("Option", option);
		throw invalid_option();
	}
	DatabaseContext* db = this->db;
	Standalone<Optional<StringRef>> passValue = value;

	// ThreadSafeDatabase is not allowed to do anything with options except pass them through to RYW.
	onMainThreadVoid(
	    [db, option, passValue]() {
		    db->checkDeferredError();
		    db->setOption(option, passValue.contents());
	    },
	    db,
	    &DatabaseContext::deferredError);
}

ThreadFuture<int64_t> ThreadSafeDatabase::rebootWorker(const StringRef& address, bool check, int duration) {
	DatabaseContext* db = this->db;
	Key addressKey = address;
	return onMainThread([db, addressKey, check, duration]() -> Future<int64_t> {
		db->checkDeferredError();
		return db->rebootWorker(addressKey, check, duration);
	});
}

ThreadFuture<Void> ThreadSafeDatabase::forceRecoveryWithDataLoss(const StringRef& dcid) {
	DatabaseContext* db = this->db;
	Key dcidKey = dcid;
	return onMainThread([db, dcidKey]() -> Future<Void> {
		db->checkDeferredError();
		return db->forceRecoveryWithDataLoss(dcidKey);
	});
}

ThreadFuture<Void> ThreadSafeDatabase::createSnapshot(const StringRef& uid, const StringRef& snapshot_command) {
	DatabaseContext* db = this->db;
	Key snapUID = uid;
	Key cmd = snapshot_command;
	return onMainThread([db, snapUID, cmd]() -> Future<Void> {
		db->checkDeferredError();
		return db->createSnapshot(snapUID, cmd);
	});
}

ThreadFuture<DatabaseSharedState*> ThreadSafeDatabase::createSharedState() {
	DatabaseContext* db = this->db;
	return onMainThread([db]() -> Future<DatabaseSharedState*> { return db->initSharedState(); });
}

void ThreadSafeDatabase::setSharedState(DatabaseSharedState* p) {
	DatabaseContext* db = this->db;
	onMainThreadVoid([db, p]() { db->setSharedState(p); });
}

// Return the main network thread busyness
double ThreadSafeDatabase::getMainThreadBusyness() {
	ASSERT(g_network);
	return g_network->networkInfo.metrics.networkBusyness;
}

// Returns the protocol version reported by the coordinator this client is connected to
// If an expected version is given, the future won't return until the protocol version is different than expected
// Note: this will never return if the server is running a protocol from FDB 5.0 or older
ThreadFuture<ProtocolVersion> ThreadSafeDatabase::getServerProtocol(Optional<ProtocolVersion> expectedVersion) {
	DatabaseContext* db = this->db;
	return onMainThread([db, expectedVersion]() -> Future<ProtocolVersion> {
		db->checkDeferredError();
		return db->getClusterProtocol(expectedVersion);
	});
}

ThreadSafeDatabase::ThreadSafeDatabase(ConnectionRecordType connectionRecordType,
                                       std::string connectionRecordString,
                                       int apiVersion) {
	// Allocate memory for the Database from this thread (so the pointer is known for subsequent method calls)
	// but run its constructor on the main thread
	DatabaseContext* db = this->db = DatabaseContext::allocateOnForeignThread();

	onMainThreadVoid([db, connectionRecordType, connectionRecordString, apiVersion]() {
		try {
			Reference<IClusterConnectionRecord> connectionRecord =
			    connectionRecordType == ConnectionRecordType::FILE
			        ? Reference<IClusterConnectionRecord>(ClusterConnectionFile::openOrDefault(connectionRecordString))
			        : Reference<IClusterConnectionRecord>(
			              new ClusterConnectionMemoryRecord(ClusterConnectionString(connectionRecordString)));

			Database::createDatabase(connectionRecord, apiVersion, IsInternal::False, LocalityData(), db).extractPtr();
		} catch (Error& e) {
			new (db) DatabaseContext(e);
		} catch (...) {
			new (db) DatabaseContext(unknown_error());
		}
	});
}

ThreadFuture<Standalone<StringRef>> ThreadSafeDatabase::getClientStatus() {
	DatabaseContext* db = this->db;
	return onMainThread([db] { return Future<Standalone<StringRef>>(db->getClientStatus()); });
}

ThreadSafeDatabase::~ThreadSafeDatabase() {
	DatabaseContext* db = this->db;
	onMainThreadVoid([db]() { db->delref(); });
}

ThreadSafeTransaction::ThreadSafeTransaction(DatabaseContext* cx)
  : initialized(std::make_shared<std::atomic_bool>(false)) {
	// Allocate memory for the transaction from this thread (so the pointer is known for subsequent method calls)
	// but run its constructor on the main thread

	// It looks strange that the DatabaseContext::addref is deferred by the onMainThreadVoid call, but it is safe
	// because the reference count of the DatabaseContext is solely managed from the main thread.  If cx is destructed
	// immediately after this call, it will defer the DatabaseContext::delref (and onMainThread preserves the order of
	// these operations).
	auto tr = this->tr =
	    (ReadYourWritesTransaction*)ReadYourWritesTransaction::operator new(sizeof(ReadYourWritesTransaction));
	auto init = this->initialized;
	// No deferred error -- if the construction of the RYW transaction fails, we have no where to put it
	onMainThreadVoid([tr, cx, init]() {
		cx->addref();
		Database db(cx);
		new (tr) ReadYourWritesTransaction(db);
		*init = true;
	});
}

// This constructor is only used while refactoring fdbcli and only called from the main thread
ThreadSafeTransaction::ThreadSafeTransaction(ReadYourWritesTransaction* ryw)
  : tr(ryw), initialized(std::make_shared<std::atomic_bool>(true)) {
	if (tr)
		tr->addref();
}

ThreadSafeTransaction::~ThreadSafeTransaction() {
	ReadYourWritesTransaction* tr = this->tr;
	if (tr)
		onMainThreadVoid([tr]() { tr->delref(); });
}

void ThreadSafeTransaction::cancel() {
	ReadYourWritesTransaction* tr = this->tr;
	onMainThreadVoid([tr]() { tr->cancel(); });
}

void ThreadSafeTransaction::setVersion(Version v) {
	ReadYourWritesTransaction* tr = this->tr;
	onMainThreadVoid([tr, v]() { tr->setVersion(v); }, tr, &ReadYourWritesTransaction::deferredError);
}

ThreadFuture<Version> ThreadSafeTransaction::getReadVersion() {
	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<Version> {
		tr->checkDeferredError();
		return tr->getReadVersion();
	});
}

ThreadFuture<Optional<Value>> ThreadSafeTransaction::get(const KeyRef& key, bool snapshot) {
	Key k = key;

	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr, k, snapshot]() -> Future<Optional<Value>> {
		tr->checkDeferredError();
		return tr->get(k, Snapshot{ snapshot });
	});
}

ThreadFuture<Key> ThreadSafeTransaction::getKey(const KeySelectorRef& key, bool snapshot) {
	KeySelector k = key;

	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr, k, snapshot]() -> Future<Key> {
		tr->checkDeferredError();
		return tr->getKey(k, Snapshot{ snapshot });
	});
}

ThreadFuture<int64_t> ThreadSafeTransaction::getEstimatedRangeSizeBytes(const KeyRangeRef& keys) {
	KeyRange r = keys;

	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr, r]() -> Future<int64_t> {
		tr->checkDeferredError();
		return tr->getEstimatedRangeSizeBytes(r);
	});
}

ThreadFuture<Standalone<VectorRef<KeyRef>>> ThreadSafeTransaction::getRangeSplitPoints(const KeyRangeRef& range,
                                                                                       int64_t chunkSize) {
	KeyRange r = range;

	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr, r, chunkSize]() -> Future<Standalone<VectorRef<KeyRef>>> {
		tr->checkDeferredError();
		return tr->getRangeSplitPoints(r, chunkSize);
	});
}

ThreadFuture<RangeResult> ThreadSafeTransaction::getRange(const KeySelectorRef& begin,
                                                          const KeySelectorRef& end,
                                                          int limit,
                                                          bool snapshot,
                                                          bool reverse) {
	KeySelector b = begin;
	KeySelector e = end;

	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr, b, e, limit, snapshot, reverse]() -> Future<RangeResult> {
		tr->checkDeferredError();
		return tr->getRange(b, e, limit, Snapshot{ snapshot }, Reverse{ reverse });
	});
}

ThreadFuture<RangeResult> ThreadSafeTransaction::getRange(const KeySelectorRef& begin,
                                                          const KeySelectorRef& end,
                                                          GetRangeLimits limits,
                                                          bool snapshot,
                                                          bool reverse) {
	KeySelector b = begin;
	KeySelector e = end;

	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr, b, e, limits, snapshot, reverse]() -> Future<RangeResult> {
		tr->checkDeferredError();
		return tr->getRange(b, e, limits, Snapshot{ snapshot }, Reverse{ reverse });
	});
}

ThreadFuture<MappedRangeResult> ThreadSafeTransaction::getMappedRange(const KeySelectorRef& begin,
                                                                      const KeySelectorRef& end,
                                                                      const StringRef& mapper,
                                                                      GetRangeLimits limits,
                                                                      bool snapshot,
                                                                      bool reverse) {
	KeySelector b = begin;
	KeySelector e = end;
	Key h = mapper;

	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr, b, e, h, limits, snapshot, reverse]() -> Future<MappedRangeResult> {
		tr->checkDeferredError();
		return tr->getMappedRange(b, e, h, limits, Snapshot{ snapshot }, Reverse{ reverse });
	});
}

ThreadFuture<Standalone<VectorRef<const char*>>> ThreadSafeTransaction::getAddressesForKey(const KeyRef& key) {
	Key k = key;

	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr, k]() -> Future<Standalone<VectorRef<const char*>>> {
		tr->checkDeferredError();
		return tr->getAddressesForKey(k);
	});
}

void ThreadSafeTransaction::addReadConflictRange(const KeyRangeRef& keys) {
	KeyRange r = keys;

	ReadYourWritesTransaction* tr = this->tr;
	onMainThreadVoid([tr, r]() { tr->addReadConflictRange(r); }, tr, &ReadYourWritesTransaction::deferredError);
}

void ThreadSafeTransaction::makeSelfConflicting() {
	ReadYourWritesTransaction* tr = this->tr;
	onMainThreadVoid([tr]() { tr->makeSelfConflicting(); }, tr, &ReadYourWritesTransaction::deferredError);
}

void ThreadSafeTransaction::atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) {
	Key k = key;
	Value v = value;

	ReadYourWritesTransaction* tr = this->tr;
	onMainThreadVoid([tr, k, v, operationType]() { tr->atomicOp(k, v, operationType); },
	                 tr,
	                 &ReadYourWritesTransaction::deferredError);
}

void ThreadSafeTransaction::set(const KeyRef& key, const ValueRef& value) {
	Key k = key;
	Value v = value;

	ReadYourWritesTransaction* tr = this->tr;
	onMainThreadVoid([tr, k, v]() { tr->set(k, v); }, tr, &ReadYourWritesTransaction::deferredError);
}

void ThreadSafeTransaction::clear(const KeyRangeRef& range) {
	KeyRange r = range;

	ReadYourWritesTransaction* tr = this->tr;
	onMainThreadVoid([tr, r]() { tr->clear(r); }, tr, &ReadYourWritesTransaction::deferredError);
}

void ThreadSafeTransaction::clear(const KeyRef& begin, const KeyRef& end) {
	Key b = begin;
	Key e = end;

	ReadYourWritesTransaction* tr = this->tr;
	onMainThreadVoid(
	    [tr, b, e]() {
		    if (b > e)
			    throw inverted_range();

		    tr->clear(KeyRangeRef(b, e));
	    },
	    tr,
	    &ReadYourWritesTransaction::deferredError);
}

void ThreadSafeTransaction::clear(const KeyRef& key) {
	Key k = key;

	ReadYourWritesTransaction* tr = this->tr;
	onMainThreadVoid([tr, k]() { tr->clear(k); }, tr, &ReadYourWritesTransaction::deferredError);
}

ThreadFuture<Void> ThreadSafeTransaction::watch(const KeyRef& key) {
	Key k = key;

	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr, k]() -> Future<Void> {
		tr->checkDeferredError();
		return tr->watch(k);
	});
}

void ThreadSafeTransaction::addWriteConflictRange(const KeyRangeRef& keys) {
	KeyRange r = keys;

	ReadYourWritesTransaction* tr = this->tr;
	onMainThreadVoid([tr, r]() { tr->addWriteConflictRange(r); }, tr, &ReadYourWritesTransaction::deferredError);
}

ThreadFuture<Void> ThreadSafeTransaction::commit() {
	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<Void> {
		tr->checkDeferredError();
		return tr->commit();
	});
}

Version ThreadSafeTransaction::getCommittedVersion() {
	// This should be thread safe when called legally, but it is fragile
	if (!initialized || !*initialized) {
		return ::invalidVersion;
	}
	return tr->getCommittedVersion();
}

ThreadFuture<VersionVector> ThreadSafeTransaction::getVersionVector() {
	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<VersionVector> {
		tr->checkDeferredError();
		return tr->getVersionVector();
	});
}

ThreadFuture<SpanContext> ThreadSafeTransaction::getSpanContext() {
	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<SpanContext> {
		tr->checkDeferredError();
		return tr->getSpanContext();
	});
}

ThreadFuture<double> ThreadSafeTransaction::getTagThrottledDuration() {
	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<double> {
		tr->checkDeferredError();
		return tr->getTagThrottledDuration();
	});
}

ThreadFuture<int64_t> ThreadSafeTransaction::getTotalCost() {
	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<int64_t> {
		tr->checkDeferredError();
		return tr->getTotalCost();
	});
}

ThreadFuture<int64_t> ThreadSafeTransaction::getApproximateSize() {
	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<int64_t> {
		tr->checkDeferredError();
		return tr->getApproximateSize();
	});
}

ThreadFuture<Standalone<StringRef>> ThreadSafeTransaction::getVersionstamp() {
	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<Standalone<StringRef>> {
		tr->checkDeferredError();
		return tr->getVersionstamp();
	});
}

void ThreadSafeTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	auto itr = FDBTransactionOptions::optionInfo.find(option);
	if (itr == FDBTransactionOptions::optionInfo.end()) {
		TraceEvent("UnknownTransactionOption").detail("Option", option);
		throw invalid_option();
	}
	ReadYourWritesTransaction* tr = this->tr;
	Standalone<Optional<StringRef>> passValue = value;

	// ThreadSafeTransaction is not allowed to do anything with options except pass them through to RYW.
	onMainThreadVoid([tr, option, passValue]() { tr->setOption(option, passValue.contents()); },
	                 tr,
	                 &ReadYourWritesTransaction::deferredError);
}

ThreadFuture<Void> ThreadSafeTransaction::checkDeferredError() {
	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr]() {
		try {
			tr->checkDeferredError();
		} catch (Error& e) {
			tr->deferredError = Error();
			return Future<Void>(e);
		}
		return Future<Void>(Void());
	});
}

ThreadFuture<Void> ThreadSafeTransaction::onError(Error const& e) {
	ReadYourWritesTransaction* tr = this->tr;
	return onMainThread([tr, e]() { return tr->onError(e); });
}

void ThreadSafeTransaction::operator=(ThreadSafeTransaction&& r) noexcept {
	tr = r.tr;
	r.tr = nullptr;
	initialized = std::move(r.initialized);
}

ThreadSafeTransaction::ThreadSafeTransaction(ThreadSafeTransaction&& r) noexcept {
	tr = r.tr;
	r.tr = nullptr;
	initialized = std::move(r.initialized);
}

void ThreadSafeTransaction::reset() {
	ReadYourWritesTransaction* tr = this->tr;
	onMainThreadVoid([tr]() { tr->reset(); });
}

void ThreadSafeTransaction::debugTrace(BaseTraceEvent&& ev) {
	if (ev.isEnabled()) {
		ReadYourWritesTransaction* tr = this->tr;
		std::shared_ptr<BaseTraceEvent> evPtr = std::make_shared<BaseTraceEvent>(std::move(ev));
		onMainThreadVoid([tr, evPtr]() { tr->debugTrace(std::move(*evPtr)); });
	}
};

void ThreadSafeTransaction::debugPrint(std::string const& message) {
	ReadYourWritesTransaction* tr = this->tr;
	onMainThreadVoid([tr, message]() { tr->debugPrint(message); });
}

extern const char* getSourceVersion();

ThreadSafeApi::ThreadSafeApi() : apiVersion(-1), transportId(0) {}

void ThreadSafeApi::selectApiVersion(int apiVersion) {
	this->apiVersion = ApiVersion(apiVersion);
}

const char* ThreadSafeApi::getClientVersion() {
	// There is only one copy of the ThreadSafeAPI, and it never gets deleted.
	// Also, clientVersion is initialized on demand and never modified afterwards.
	if (clientVersion.empty()) {
		clientVersion = format("%s,%s,%llx", FDB_VT_VERSION, getSourceVersion(), currentProtocolVersion());
	}
	return clientVersion.c_str();
}

void ThreadSafeApi::useFutureProtocolVersion() {
	::useFutureProtocolVersion();
}

void ThreadSafeApi::setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	if (option == FDBNetworkOptions::EXTERNAL_CLIENT_TRANSPORT_ID) {
		if (value.present()) {
			transportId = std::stoull(value.get().toString().c_str());
		}
	} else {
		::setNetworkOption(option, value);
	}
}

void ThreadSafeApi::setupNetwork() {
	::setupNetwork(transportId);
}

void ThreadSafeApi::runNetwork() {
	Optional<Error> runErr;
	try {
		::runNetwork();
	} catch (const Error& e) {
		TraceEvent(SevError, "RunNetworkError").error(e);
		runErr = e;
	} catch (const std::exception& e) {
		runErr = unknown_error();
		TraceEvent(SevError, "RunNetworkError").error(unknown_error()).detail("RootException", e.what());
	} catch (...) {
		runErr = unknown_error();
		TraceEvent(SevError, "RunNetworkError").error(unknown_error());
	}

	for (auto& hook : threadCompletionHooks) {
		try {
			hook.first(hook.second);
		} catch (const Error& e) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(e);
		} catch (const std::exception& e) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(unknown_error()).detail("RootException", e.what());
		} catch (...) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(unknown_error());
		}
	}

	if (runErr.present()) {
		throw runErr.get();
	}

	TraceEvent("RunNetworkTerminating");
}

void ThreadSafeApi::stopNetwork() {
	::stopNetwork();
}

Reference<IDatabase> ThreadSafeApi::createDatabase(const char* clusterFilePath) {
	return Reference<IDatabase>(
	    new ThreadSafeDatabase(ThreadSafeDatabase::ConnectionRecordType::FILE, clusterFilePath, apiVersion.version()));
}

Reference<IDatabase> ThreadSafeApi::createDatabaseFromConnectionString(const char* connectionString) {
	return Reference<IDatabase>(new ThreadSafeDatabase(
	    ThreadSafeDatabase::ConnectionRecordType::CONNECTION_STRING, connectionString, apiVersion.version()));
}

void ThreadSafeApi::addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) {
	if (!g_network) {
		throw network_not_setup();
	}

	MutexHolder holder(lock); // We could use the network thread to protect this action, but then we can't guarantee
	                          // upon return that the hook is set.
	threadCompletionHooks.emplace_back(hook, hookParameter);
}
