/*
 * ThreadSafeTransaction.cpp
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

#include "fdbclient/BlobGranuleFiles.h"
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

Reference<ITenant> ThreadSafeDatabase::openTenant(TenantNameRef tenantName) {
	return makeReference<ThreadSafeTenant>(Reference<ThreadSafeDatabase>::addRef(this), tenantName);
}

Reference<ITransaction> ThreadSafeDatabase::createTransaction() {
	auto type = isConfigDB ? ISingleThreadTransaction::Type::PAXOS_CONFIG : ISingleThreadTransaction::Type::RYW;
	return Reference<ITransaction>(new ThreadSafeTransaction(db, type, Optional<TenantName>(), nullptr));
}

void ThreadSafeDatabase::setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	auto itr = FDBDatabaseOptions::optionInfo.find(option);
	if (itr != FDBDatabaseOptions::optionInfo.end()) {
		TraceEvent("SetDatabaseOption").detail("Option", itr->second.name);
	} else {
		TraceEvent("UnknownDatabaseOption").detail("Option", option);
		throw invalid_option();
	}
	if (itr->first == FDBDatabaseOptions::USE_CONFIG_DATABASE) {
		isConfigDB = true;
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

ThreadFuture<Key> ThreadSafeDatabase::purgeBlobGranules(const KeyRangeRef& keyRange, Version purgeVersion, bool force) {
	DatabaseContext* db = this->db;
	KeyRange range = keyRange;
	return onMainThread([db, range, purgeVersion, force]() -> Future<Key> {
		db->checkDeferredError();
		return db->purgeBlobGranules(range, purgeVersion, {}, force);
	});
}

ThreadFuture<Void> ThreadSafeDatabase::waitPurgeGranulesComplete(const KeyRef& purgeKey) {
	DatabaseContext* db = this->db;
	Key key = purgeKey;
	return onMainThread([db, key]() -> Future<Void> {
		db->checkDeferredError();
		return db->waitPurgeGranulesComplete(key);
	});
}

ThreadFuture<bool> ThreadSafeDatabase::blobbifyRange(const KeyRangeRef& keyRange) {
	DatabaseContext* db = this->db;
	KeyRange range = keyRange;
	return onMainThread([=]() -> Future<bool> {
		db->checkDeferredError();
		return db->blobbifyRange(range);
	});
}

ThreadFuture<bool> ThreadSafeDatabase::unblobbifyRange(const KeyRangeRef& keyRange) {
	DatabaseContext* db = this->db;
	KeyRange range = keyRange;
	return onMainThread([=]() -> Future<bool> {
		db->checkDeferredError();
		return db->unblobbifyRange(range);
	});
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> ThreadSafeDatabase::listBlobbifiedRanges(const KeyRangeRef& keyRange,
                                                                                          int rangeLimit) {
	DatabaseContext* db = this->db;
	KeyRange range = keyRange;
	return onMainThread([=]() -> Future<Standalone<VectorRef<KeyRangeRef>>> {
		db->checkDeferredError();
		return db->listBlobbifiedRanges(range, rangeLimit);
	});
}

ThreadFuture<Version> ThreadSafeDatabase::verifyBlobRange(const KeyRangeRef& keyRange, Optional<Version> version) {
	DatabaseContext* db = this->db;
	KeyRange range = keyRange;
	return onMainThread([=]() -> Future<Version> {
		db->checkDeferredError();
		return db->verifyBlobRange(range, version);
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

ThreadSafeTenant::ThreadSafeTenant(Reference<ThreadSafeDatabase> db, TenantName name) : db(db), name(name) {
	Tenant* tenant = this->tenant = Tenant::allocateOnForeignThread();
	DatabaseContext* cx = db->db;
	onMainThreadVoid([tenant, cx, name]() {
		cx->addref();
		new (tenant) Tenant(Database(cx), name);
	});
}

Reference<ITransaction> ThreadSafeTenant::createTransaction() {
	auto type = db->isConfigDB ? ISingleThreadTransaction::Type::PAXOS_CONFIG : ISingleThreadTransaction::Type::RYW;
	return Reference<ITransaction>(new ThreadSafeTransaction(db->db, type, name, tenant));
}

ThreadFuture<int64_t> ThreadSafeTenant::getId() {
	Tenant* tenant = this->tenant;
	return onMainThread([tenant]() -> Future<int64_t> { return tenant->getIdFuture(); });
}

ThreadFuture<Key> ThreadSafeTenant::purgeBlobGranules(const KeyRangeRef& keyRange, Version purgeVersion, bool force) {
	DatabaseContext* db = this->db->db;
	Tenant* tenantPtr = this->tenant;
	KeyRange range = keyRange;
	return onMainThread([db, range, purgeVersion, tenantPtr, force]() -> Future<Key> {
		db->addref();
		return db->purgeBlobGranules(range, purgeVersion, Reference<Tenant>::addRef(tenantPtr), force);
	});
}

ThreadFuture<Void> ThreadSafeTenant::waitPurgeGranulesComplete(const KeyRef& purgeKey) {
	DatabaseContext* db = this->db->db;
	Key key = purgeKey;
	return onMainThread([db, key]() -> Future<Void> {
		db->checkDeferredError();
		return db->waitPurgeGranulesComplete(key);
	});
}

ThreadFuture<bool> ThreadSafeTenant::blobbifyRange(const KeyRangeRef& keyRange) {
	DatabaseContext* db = this->db->db;
	KeyRange range = keyRange;
	return onMainThread([=]() -> Future<bool> {
		db->checkDeferredError();
		db->addref();
		return db->blobbifyRange(range, Reference<Tenant>::addRef(tenant));
	});
}

ThreadFuture<bool> ThreadSafeTenant::unblobbifyRange(const KeyRangeRef& keyRange) {
	DatabaseContext* db = this->db->db;
	KeyRange range = keyRange;
	return onMainThread([=]() -> Future<bool> {
		db->checkDeferredError();
		db->addref();
		return db->unblobbifyRange(range, Reference<Tenant>::addRef(tenant));
	});
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> ThreadSafeTenant::listBlobbifiedRanges(const KeyRangeRef& keyRange,
                                                                                        int rangeLimit) {
	DatabaseContext* db = this->db->db;
	KeyRange range = keyRange;
	return onMainThread([=]() -> Future<Standalone<VectorRef<KeyRangeRef>>> {
		db->checkDeferredError();
		db->addref();
		return db->listBlobbifiedRanges(range, rangeLimit, Reference<Tenant>::addRef(tenant));
	});
}

ThreadFuture<Version> ThreadSafeTenant::verifyBlobRange(const KeyRangeRef& keyRange, Optional<Version> version) {
	DatabaseContext* db = this->db->db;
	KeyRange range = keyRange;
	return onMainThread([=]() -> Future<Version> {
		db->checkDeferredError();
		db->addref();
		return db->verifyBlobRange(range, version, Reference<Tenant>::addRef(tenant));
	});
}

ThreadSafeTenant::~ThreadSafeTenant() {
	Tenant* t = this->tenant;
	if (t)
		onMainThreadVoid([t]() { t->delref(); });
}

ThreadSafeTransaction::ThreadSafeTransaction(DatabaseContext* cx,
                                             ISingleThreadTransaction::Type type,
                                             Optional<TenantName> tenantName,
                                             Tenant* tenantPtr)
  : tenantName(tenantName), initialized(std::make_shared<std::atomic_bool>(false)) {
	// Allocate memory for the transaction from this thread (so the pointer is known for subsequent method calls)
	// but run its constructor on the main thread

	// It looks strange that the DatabaseContext::addref is deferred by the onMainThreadVoid call, but it is safe
	// because the reference count of the DatabaseContext is solely managed from the main thread.  If cx is destructed
	// immediately after this call, it will defer the DatabaseContext::delref (and onMainThread preserves the order of
	// these operations).
	auto tr = this->tr = ISingleThreadTransaction::allocateOnForeignThread(type);
	auto init = this->initialized;
	// No deferred error -- if the construction of the RYW transaction fails, we have no where to put it
	onMainThreadVoid([tr, cx, type, tenantPtr, init]() {
		cx->addref();
		Database db(cx);
		if (tenantPtr) {
			Reference<Tenant> tenant = Reference<Tenant>::addRef(tenantPtr);
			if (type == ISingleThreadTransaction::Type::RYW) {
				new (tr) ReadYourWritesTransaction(db, tenant);
			} else {
				tr->construct(db, tenant);
			}
		} else {
			if (type == ISingleThreadTransaction::Type::RYW) {
				new (tr) ReadYourWritesTransaction(db);
			} else {
				tr->construct(db);
			}
		}
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
	ISingleThreadTransaction* tr = this->tr;
	if (tr)
		onMainThreadVoid([tr]() { tr->delref(); });
}

void ThreadSafeTransaction::cancel() {
	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr]() { tr->cancel(); });
}

void ThreadSafeTransaction::setVersion(Version v) {
	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, v]() { tr->setVersion(v); }, tr, &ISingleThreadTransaction::deferredError);
}

ThreadFuture<Version> ThreadSafeTransaction::getReadVersion() {
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<Version> {
		tr->checkDeferredError();
		return tr->getReadVersion();
	});
}

ThreadFuture<Optional<Value>> ThreadSafeTransaction::get(const KeyRef& key, bool snapshot) {
	Key k = key;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, k, snapshot]() -> Future<Optional<Value>> {
		tr->checkDeferredError();
		return tr->get(k, Snapshot{ snapshot });
	});
}

ThreadFuture<Key> ThreadSafeTransaction::getKey(const KeySelectorRef& key, bool snapshot) {
	KeySelector k = key;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, k, snapshot]() -> Future<Key> {
		tr->checkDeferredError();
		return tr->getKey(k, Snapshot{ snapshot });
	});
}

ThreadFuture<int64_t> ThreadSafeTransaction::getEstimatedRangeSizeBytes(const KeyRangeRef& keys) {
	KeyRange r = keys;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, r]() -> Future<int64_t> {
		tr->checkDeferredError();
		return tr->getEstimatedRangeSizeBytes(r);
	});
}

ThreadFuture<Standalone<VectorRef<KeyRef>>> ThreadSafeTransaction::getRangeSplitPoints(const KeyRangeRef& range,
                                                                                       int64_t chunkSize) {
	KeyRange r = range;

	ISingleThreadTransaction* tr = this->tr;
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

	ISingleThreadTransaction* tr = this->tr;
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

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, b, e, limits, snapshot, reverse]() -> Future<RangeResult> {
		tr->checkDeferredError();
		return tr->getRange(b, e, limits, Snapshot{ snapshot }, Reverse{ reverse });
	});
}

ThreadFuture<MappedRangeResult> ThreadSafeTransaction::getMappedRange(const KeySelectorRef& begin,
                                                                      const KeySelectorRef& end,
                                                                      const StringRef& mapper,
                                                                      GetRangeLimits limits,
                                                                      int matchIndex,
                                                                      bool snapshot,
                                                                      bool reverse) {
	KeySelector b = begin;
	KeySelector e = end;
	Key h = mapper;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, b, e, h, limits, matchIndex, snapshot, reverse]() -> Future<MappedRangeResult> {
		tr->checkDeferredError();
		return tr->getMappedRange(b, e, h, limits, matchIndex, Snapshot{ snapshot }, Reverse{ reverse });
	});
}

ThreadFuture<Standalone<VectorRef<const char*>>> ThreadSafeTransaction::getAddressesForKey(const KeyRef& key) {
	Key k = key;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, k]() -> Future<Standalone<VectorRef<const char*>>> {
		tr->checkDeferredError();
		return tr->getAddressesForKey(k);
	});
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> ThreadSafeTransaction::getBlobGranuleRanges(
    const KeyRangeRef& keyRange,
    int rangeLimit) {
	ISingleThreadTransaction* tr = this->tr;
	KeyRange r = keyRange;

	return onMainThread([=]() -> Future<Standalone<VectorRef<KeyRangeRef>>> {
		tr->checkDeferredError();
		return tr->getBlobGranuleRanges(r, rangeLimit);
	});
}

ThreadResult<RangeResult> ThreadSafeTransaction::readBlobGranules(const KeyRangeRef& keyRange,
                                                                  Version beginVersion,
                                                                  Optional<Version> readVersion,
                                                                  ReadBlobGranuleContext granule_context) {
	// This should not be called directly, bypassMultiversionApi should not be set
	return ThreadResult<RangeResult>(unsupported_operation());
}

ThreadFuture<Standalone<VectorRef<BlobGranuleChunkRef>>> ThreadSafeTransaction::readBlobGranulesStart(
    const KeyRangeRef& keyRange,
    Version beginVersion,
    Optional<Version> readVersion,
    Version* readVersionOut) {
	ISingleThreadTransaction* tr = this->tr;
	KeyRange r = keyRange;

	return onMainThread(
	    [tr, r, beginVersion, readVersion, readVersionOut]() -> Future<Standalone<VectorRef<BlobGranuleChunkRef>>> {
		    tr->checkDeferredError();
		    return tr->readBlobGranules(r, beginVersion, readVersion, readVersionOut);
	    });
}

ThreadResult<RangeResult> ThreadSafeTransaction::readBlobGranulesFinish(
    ThreadFuture<Standalone<VectorRef<BlobGranuleChunkRef>>> startFuture,
    const KeyRangeRef& keyRange,
    Version beginVersion,
    Version readVersion,
    ReadBlobGranuleContext granuleContext) {
	// do this work off of fdb network threads for performance!
	Standalone<VectorRef<BlobGranuleChunkRef>> files = startFuture.get();
	GranuleMaterializeStats stats;
	auto ret = loadAndMaterializeBlobGranules(files, keyRange, beginVersion, readVersion, granuleContext, stats);
	if (!ret.isError()) {
		ISingleThreadTransaction* tr = this->tr;
		onMainThreadVoid([tr, stats]() { tr->addGranuleMaterializeStats(stats); });
	}
	return ret;
}

ThreadFuture<Standalone<VectorRef<BlobGranuleSummaryRef>>> ThreadSafeTransaction::summarizeBlobGranules(
    const KeyRangeRef& keyRange,
    Optional<Version> summaryVersion,
    int rangeLimit) {
	ISingleThreadTransaction* tr = this->tr;
	KeyRange r = keyRange;

	return onMainThread([=]() -> Future<Standalone<VectorRef<BlobGranuleSummaryRef>>> {
		tr->checkDeferredError();
		return tr->summarizeBlobGranules(r, summaryVersion, rangeLimit);
	});
}

void ThreadSafeTransaction::addReadConflictRange(const KeyRangeRef& keys) {
	KeyRange r = keys;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, r]() { tr->addReadConflictRange(r); }, tr, &ISingleThreadTransaction::deferredError);
}

void ThreadSafeTransaction::makeSelfConflicting() {
	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr]() { tr->makeSelfConflicting(); }, tr, &ISingleThreadTransaction::deferredError);
}

void ThreadSafeTransaction::atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) {
	Key k = key;
	Value v = value;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, k, v, operationType]() { tr->atomicOp(k, v, operationType); },
	                 tr,
	                 &ISingleThreadTransaction::deferredError);
}

void ThreadSafeTransaction::set(const KeyRef& key, const ValueRef& value) {
	Key k = key;
	Value v = value;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, k, v]() { tr->set(k, v); }, tr, &ISingleThreadTransaction::deferredError);
}

void ThreadSafeTransaction::clear(const KeyRangeRef& range) {
	KeyRange r = range;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, r]() { tr->clear(r); }, tr, &ISingleThreadTransaction::deferredError);
}

void ThreadSafeTransaction::clear(const KeyRef& begin, const KeyRef& end) {
	Key b = begin;
	Key e = end;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid(
	    [tr, b, e]() {
		    if (b > e)
			    throw inverted_range();

		    tr->clear(KeyRangeRef(b, e));
	    },
	    tr,
	    &ISingleThreadTransaction::deferredError);
}

void ThreadSafeTransaction::clear(const KeyRef& key) {
	Key k = key;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, k]() { tr->clear(k); }, tr, &ISingleThreadTransaction::deferredError);
}

ThreadFuture<Void> ThreadSafeTransaction::watch(const KeyRef& key) {
	Key k = key;

	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, k]() -> Future<Void> {
		tr->checkDeferredError();
		return tr->watch(k);
	});
}

void ThreadSafeTransaction::addWriteConflictRange(const KeyRangeRef& keys) {
	KeyRange r = keys;

	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr, r]() { tr->addWriteConflictRange(r); }, tr, &ISingleThreadTransaction::deferredError);
}

ThreadFuture<Void> ThreadSafeTransaction::commit() {
	ISingleThreadTransaction* tr = this->tr;
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
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<VersionVector> {
		tr->checkDeferredError();
		return tr->getVersionVector();
	});
}

ThreadFuture<SpanContext> ThreadSafeTransaction::getSpanContext() {
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<SpanContext> {
		tr->checkDeferredError();
		return tr->getSpanContext();
	});
}

ThreadFuture<double> ThreadSafeTransaction::getTagThrottledDuration() {
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<double> {
		tr->checkDeferredError();
		return tr->getTagThrottledDuration();
	});
}

ThreadFuture<int64_t> ThreadSafeTransaction::getTotalCost() {
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<int64_t> {
		tr->checkDeferredError();
		return tr->getTotalCost();
	});
}

ThreadFuture<int64_t> ThreadSafeTransaction::getApproximateSize() {
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr]() -> Future<int64_t> {
		tr->checkDeferredError();
		return tr->getApproximateSize();
	});
}

ThreadFuture<Standalone<StringRef>> ThreadSafeTransaction::getVersionstamp() {
	ISingleThreadTransaction* tr = this->tr;
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
	ISingleThreadTransaction* tr = this->tr;
	Standalone<Optional<StringRef>> passValue = value;

	// ThreadSafeTransaction is not allowed to do anything with options except pass them through to RYW.
	onMainThreadVoid([tr, option, passValue]() { tr->setOption(option, passValue.contents()); },
	                 tr,
	                 &ISingleThreadTransaction::deferredError);
}

ThreadFuture<Void> ThreadSafeTransaction::checkDeferredError() {
	ISingleThreadTransaction* tr = this->tr;
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
	ISingleThreadTransaction* tr = this->tr;
	return onMainThread([tr, e]() { return tr->onError(e); });
}

Optional<TenantName> ThreadSafeTransaction::getTenant() {
	return tenantName;
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
	ISingleThreadTransaction* tr = this->tr;
	onMainThreadVoid([tr]() { tr->reset(); });
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
