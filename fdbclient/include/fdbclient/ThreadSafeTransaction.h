/*
 * ThreadSafeTransaction.h
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

#ifndef FDBCLIENT_THREADSAFETRANSACTION_H
#define FDBCLIENT_THREADSAFETRANSACTION_H
#include "flow/ApiVersion.h"
#include "flow/ProtocolVersion.h"
#pragma once

#include "fdbclient/ReadYourWrites.h"
#include "flow/ThreadHelper.actor.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/ISingleThreadTransaction.h"

// An implementation of IDatabase that serializes operations onto the network thread and interacts with the lower-level
// client APIs exposed by NativeAPI and ReadYourWrites.
class ThreadSafeDatabase : public IDatabase, public ThreadSafeReferenceCounted<ThreadSafeDatabase> {
public:
	~ThreadSafeDatabase() override;
	static ThreadFuture<Reference<IDatabase>> createFromExistingDatabase(Database cx);

	Reference<ITenant> openTenant(TenantNameRef tenantName) override;
	Reference<ITransaction> createTransaction() override;

	void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;
	double getMainThreadBusyness() override;

	// Returns the protocol version reported by the coordinator this client is connected to
	// If an expected version is given, the future won't return until the protocol version is different than expected
	// Note: this will never return if the server is running a protocol from FDB 5.0 or older
	ThreadFuture<ProtocolVersion> getServerProtocol(
	    Optional<ProtocolVersion> expectedVersion = Optional<ProtocolVersion>()) override;

	// Returns after a majority of coordination servers are available and have reported a leader. The
	// cluster file therefore is valid, but the database might be unavailable.
	ThreadFuture<Void> onConnected();

	void addref() override { ThreadSafeReferenceCounted<ThreadSafeDatabase>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<ThreadSafeDatabase>::delref(); }

	ThreadFuture<int64_t> rebootWorker(const StringRef& address, bool check, int duration) override;
	ThreadFuture<Void> forceRecoveryWithDataLoss(const StringRef& dcid) override;
	ThreadFuture<Void> createSnapshot(const StringRef& uid, const StringRef& snapshot_command) override;

	ThreadFuture<Key> purgeBlobGranules(const KeyRangeRef& keyRange, Version purgeVersion, bool force) override;
	ThreadFuture<Void> waitPurgeGranulesComplete(const KeyRef& purgeKey) override;

	ThreadFuture<bool> blobbifyRange(const KeyRangeRef& keyRange) override;
	ThreadFuture<bool> blobbifyRangeBlocking(const KeyRangeRef& keyRange) override;
	ThreadFuture<bool> unblobbifyRange(const KeyRangeRef& keyRange) override;
	ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> listBlobbifiedRanges(const KeyRangeRef& keyRange,
	                                                                      int rangeLimit) override;

	ThreadFuture<Version> verifyBlobRange(const KeyRangeRef& keyRange, Optional<Version> version) override;

	ThreadFuture<DatabaseSharedState*> createSharedState() override;
	void setSharedState(DatabaseSharedState* p) override;

	// Return a JSON string containing database client-side status information
	ThreadFuture<Standalone<StringRef>> getClientStatus() override;

private:
	friend class ThreadSafeTenant;
	friend class ThreadSafeTransaction;
	bool isConfigDB{ false };
	DatabaseContext* db;

public: // Internal use only
	enum class ConnectionRecordType { FILE, CONNECTION_STRING };
	ThreadSafeDatabase(ConnectionRecordType connectionRecordType, std::string connectionRecord, int apiVersion);
	ThreadSafeDatabase(DatabaseContext* db) : db(db) {}
	DatabaseContext* unsafeGetPtr() const { return db; }
};

class ThreadSafeTenant : public ITenant, ThreadSafeReferenceCounted<ThreadSafeTenant>, NonCopyable {
public:
	ThreadSafeTenant(Reference<ThreadSafeDatabase> db, TenantName name);
	~ThreadSafeTenant() override;

	Reference<ITransaction> createTransaction() override;

	ThreadFuture<int64_t> getId() override;
	ThreadFuture<Key> purgeBlobGranules(const KeyRangeRef& keyRange, Version purgeVersion, bool force) override;
	ThreadFuture<Void> waitPurgeGranulesComplete(const KeyRef& purgeKey) override;

	ThreadFuture<bool> blobbifyRange(const KeyRangeRef& keyRange) override;
	ThreadFuture<bool> blobbifyRangeBlocking(const KeyRangeRef& keyRange) override;
	ThreadFuture<bool> unblobbifyRange(const KeyRangeRef& keyRange) override;
	ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> listBlobbifiedRanges(const KeyRangeRef& keyRange,
	                                                                      int rangeLimit) override;

	ThreadFuture<Version> verifyBlobRange(const KeyRangeRef& keyRange, Optional<Version> version) override;

	void addref() override { ThreadSafeReferenceCounted<ThreadSafeTenant>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<ThreadSafeTenant>::delref(); }

private:
	Reference<ThreadSafeDatabase> db;
	TenantName name;
	Tenant* tenant;
};

// An implementation of ITransaction that serializes operations onto the network thread and interacts with the
// lower-level client APIs exposed by ISingleThreadTransaction
class ThreadSafeTransaction : public ITransaction, ThreadSafeReferenceCounted<ThreadSafeTransaction>, NonCopyable {
public:
	explicit ThreadSafeTransaction(DatabaseContext* cx,
	                               ISingleThreadTransaction::Type type,
	                               Optional<TenantName> tenantName,
	                               Tenant* tenantPtr);
	~ThreadSafeTransaction() override;

	// Note: used while refactoring fdbcli, need to be removed later
	explicit ThreadSafeTransaction(ReadYourWritesTransaction* ryw);

	void cancel() override;
	void setVersion(Version v) override;
	ThreadFuture<Version> getReadVersion() override;

	ThreadFuture<Optional<Value>> get(const KeyRef& key, bool snapshot = false) override;
	ThreadFuture<Key> getKey(const KeySelectorRef& key, bool snapshot = false) override;
	ThreadFuture<RangeResult> getRange(const KeySelectorRef& begin,
	                                   const KeySelectorRef& end,
	                                   int limit,
	                                   bool snapshot = false,
	                                   bool reverse = false) override;
	ThreadFuture<RangeResult> getRange(const KeySelectorRef& begin,
	                                   const KeySelectorRef& end,
	                                   GetRangeLimits limits,
	                                   bool snapshot = false,
	                                   bool reverse = false) override;
	ThreadFuture<RangeResult> getRange(const KeyRangeRef& keys,
	                                   int limit,
	                                   bool snapshot = false,
	                                   bool reverse = false) override {
		return getRange(firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), limit, snapshot, reverse);
	}
	ThreadFuture<RangeResult> getRange(const KeyRangeRef& keys,
	                                   GetRangeLimits limits,
	                                   bool snapshot = false,
	                                   bool reverse = false) override {
		return getRange(firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), limits, snapshot, reverse);
	}
	ThreadFuture<MappedRangeResult> getMappedRange(const KeySelectorRef& begin,
	                                               const KeySelectorRef& end,
	                                               const StringRef& mapper,
	                                               GetRangeLimits limits,
	                                               int matchIndex,
	                                               bool snapshot,
	                                               bool reverse) override;
	ThreadFuture<Standalone<VectorRef<const char*>>> getAddressesForKey(const KeyRef& key) override;
	ThreadFuture<Standalone<StringRef>> getVersionstamp() override;
	ThreadFuture<int64_t> getEstimatedRangeSizeBytes(const KeyRangeRef& keys) override;
	ThreadFuture<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(const KeyRangeRef& range,
	                                                                int64_t chunkSize) override;

	ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> getBlobGranuleRanges(const KeyRangeRef& keyRange,
	                                                                      int rangeLimit) override;

	ThreadResult<RangeResult> readBlobGranules(const KeyRangeRef& keyRange,
	                                           Version beginVersion,
	                                           Optional<Version> readVersion,
	                                           ReadBlobGranuleContext granuleContext) override;

	ThreadFuture<Standalone<VectorRef<BlobGranuleChunkRef>>> readBlobGranulesStart(const KeyRangeRef& keyRange,
	                                                                               Version beginVersion,
	                                                                               Optional<Version> readVersion,
	                                                                               Version* readVersionOut) override;

	ThreadResult<RangeResult> readBlobGranulesFinish(
	    ThreadFuture<Standalone<VectorRef<BlobGranuleChunkRef>>> startFuture,
	    const KeyRangeRef& keyRange,
	    Version beginVersion,
	    Version readVersion,
	    ReadBlobGranuleContext granuleContext) override;

	ThreadFuture<Standalone<VectorRef<BlobGranuleSummaryRef>>> summarizeBlobGranules(const KeyRangeRef& keyRange,
	                                                                                 Optional<Version> summaryVersion,
	                                                                                 int rangeLimit) override;

	void addReadConflictRange(const KeyRangeRef& keys) override;
	void makeSelfConflicting();

	void atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) override;
	void set(const KeyRef& key, const ValueRef& value) override;
	void clear(const KeyRef& begin, const KeyRef& end) override;
	void clear(const KeyRangeRef& range) override;
	void clear(const KeyRef& key) override;

	ThreadFuture<Void> watch(const KeyRef& key) override;

	void addWriteConflictRange(const KeyRangeRef& keys) override;

	ThreadFuture<Void> commit() override;
	Version getCommittedVersion() override;
	ThreadFuture<VersionVector> getVersionVector() override;
	ThreadFuture<SpanContext> getSpanContext() override;
	ThreadFuture<int64_t> getTotalCost() override;
	ThreadFuture<int64_t> getApproximateSize() override;

	ThreadFuture<uint64_t> getProtocolVersion();

	void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;

	ThreadFuture<Void> checkDeferredError();
	ThreadFuture<Void> onError(Error const& e) override;

	Optional<TenantName> getTenant() override;

	// These are to permit use as state variables in actors:
	ThreadSafeTransaction() : tr(nullptr), initialized(std::make_shared<std::atomic_bool>(false)) {}
	void operator=(ThreadSafeTransaction&& r) noexcept;
	ThreadSafeTransaction(ThreadSafeTransaction&& r) noexcept;

	void reset() override;

	void addref() override { ThreadSafeReferenceCounted<ThreadSafeTransaction>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<ThreadSafeTransaction>::delref(); }

private:
	ISingleThreadTransaction* tr;
	const Optional<TenantName> tenantName;
	std::shared_ptr<std::atomic_bool> initialized;
};

// An implementation of IClientApi that serializes operations onto the network thread and interacts with the lower-level
// client APIs exposed by NativeAPI and ReadYourWrites.
class ThreadSafeApi : public IClientApi, ThreadSafeReferenceCounted<ThreadSafeApi> {
public:
	void selectApiVersion(int apiVersion) override;
	const char* getClientVersion() override;
	void useFutureProtocolVersion() override;

	void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;
	void setupNetwork() override;
	void runNetwork() override;
	void stopNetwork() override;

	Reference<IDatabase> createDatabase(const char* clusterFilePath) override;
	Reference<IDatabase> createDatabaseFromConnectionString(const char* connectionString) override;

	void addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) override;

private:
	friend IClientApi* getLocalClientAPI();
	ThreadSafeApi();

	ApiVersion apiVersion;
	std::string clientVersion;
	uint64_t transportId;

	Mutex lock;
	std::vector<std::pair<void (*)(void*), void*>> threadCompletionHooks;
};

#endif
