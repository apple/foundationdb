/*
 * MultiVersionTransaction.h
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

#ifndef FDBCLIENT_MULTIVERSIONTRANSACTION_H
#define FDBCLIENT_MULTIVERSIONTRANSACTION_H
#include "flow/Arena.h"
#pragma once

#include "fdbclient/fdb_c_options.g.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/IClientApi.h"
#include "flow/ApiVersion.h"
#include "flow/ProtocolVersion.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/WipedString.h"

// FdbCApi is used as a wrapper around the FoundationDB C API that gets loaded from an external client library.
// All of the required functions loaded from that external library are stored in function pointers in this struct.
struct FdbCApi : public ThreadSafeReferenceCounted<FdbCApi> {
	typedef struct FDB_future FDBFuture;
	typedef struct FDB_result FDBResult;
	typedef struct FDB_cluster FDBCluster;
	typedef struct FDB_database FDBDatabase;
	typedef struct FDB_tenant FDBTenant;
	typedef struct FDB_transaction FDBTransaction;

	typedef int fdb_error_t;
	typedef int fdb_bool_t;

#pragma pack(push, 4)
	typedef struct key {
		const uint8_t* key;
		int keyLength;
	} FDBKey;
	typedef struct keyvalue {
		const void* key;
		int keyLength;
		const void* value;
		int valueLength;
	} FDBKeyValue;

#pragma pack(pop)

	/* Memory layout of KeySelectorRef. */
	typedef struct keyselector {
		FDBKey key;
		/* orEqual and offset have not be tested in C binding. Just a placeholder. */
		fdb_bool_t orEqual;
		int offset;
	} FDBKeySelector;

	/* Memory layout of GetRangeReqAndResultRef. */
	typedef struct getrangereqandresult {
		FDBKeySelector begin;
		FDBKeySelector end;
		FDBKeyValue* data;
		int m_size, m_capacity;
	} FDBGetRangeReqAndResult;

	typedef struct mappedkeyvalue {
		FDBKey key;
		FDBKey value;
		/* It's complicated to map a std::variant to C. For now we assume the underlying requests are always getRange
		 * and take the shortcut. */
		FDBGetRangeReqAndResult getRange;
		unsigned char buffer[32];
	} FDBMappedKeyValue;

#pragma pack(push, 4)
	typedef struct keyrange {
		const void* beginKey;
		int beginKeyLength;
		const void* endKey;
		int endKeyLength;
	} FDBKeyRange;

	typedef struct granulesummary {
		FDBKeyRange key_range;
		int64_t snapshot_version;
		int64_t snapshot_size;
		int64_t delta_version;
		int64_t delta_size;
	} FDBGranuleSummary;
#pragma pack(pop)

	typedef struct readgranulecontext {
		// User context to pass along to functions
		void* userContext;

		// Returns a unique id for the load. Asynchronous to support queueing multiple in parallel.
		int64_t (*start_load_f)(const char* filename,
		                        int filenameLength,
		                        int64_t offset,
		                        int64_t length,
		                        int64_t fullFileLength,
		                        void* context);

		// Returns data for the load. Pass the loadId returned by start_load_f
		uint8_t* (*get_load_f)(int64_t loadId, void* context);

		// Frees data from load. Pass the loadId returned by start_load_f
		void (*free_load_f)(int64_t loadId, void* context);

		// set this to true for testing if you don't want to read the granule files, just
		// do the request to the blob workers
		fdb_bool_t debugNoMaterialize;

		// number of granules to load in parallel (default 1)
		int granuleParallelism;
	} FDBReadBlobGranuleContext;

	typedef void (*FDBCallback)(FDBFuture* future, void* callback_parameter);

	// Network
	fdb_error_t (*selectApiVersion)(int runtimeVersion, int headerVersion);
	const char* (*getClientVersion)();
	void (*useFutureProtocolVersion)();

	fdb_error_t (*setNetworkOption)(FDBNetworkOption option, uint8_t const* value, int valueLength);
	fdb_error_t (*setupNetwork)();
	fdb_error_t (*runNetwork)();
	fdb_error_t (*stopNetwork)();
	fdb_error_t (*createDatabase)(const char* clusterFilePath, FDBDatabase** db);
	fdb_error_t (*createDatabaseFromConnectionString)(const char* connectionString, FDBDatabase** db);

	// Database
	fdb_error_t (*databaseOpenTenant)(FDBDatabase* database,
	                                  uint8_t const* tenantName,
	                                  int tenantNameLength,
	                                  FDBTenant** outTenant);
	fdb_error_t (*databaseCreateTransaction)(FDBDatabase* database, FDBTransaction** tr);
	fdb_error_t (*databaseSetOption)(FDBDatabase* database,
	                                 FDBDatabaseOption option,
	                                 uint8_t const* value,
	                                 int valueLength);
	void (*databaseDestroy)(FDBDatabase* database);
	FDBFuture* (*databaseRebootWorker)(FDBDatabase* database,
	                                   uint8_t const* address,
	                                   int addressLength,
	                                   fdb_bool_t check,
	                                   int duration);
	FDBFuture* (*databaseForceRecoveryWithDataLoss)(FDBDatabase* database, uint8_t const* dcid, int dcidLength);
	FDBFuture* (*databaseCreateSnapshot)(FDBDatabase* database,
	                                     uint8_t const* uid,
	                                     int uidLength,
	                                     uint8_t const* snapshotCommmand,
	                                     int snapshotCommandLength);
	FDBFuture* (*databaseCreateSharedState)(FDBDatabase* database);
	void (*databaseSetSharedState)(FDBDatabase* database, DatabaseSharedState* p);

	double (*databaseGetMainThreadBusyness)(FDBDatabase* database);
	FDBFuture* (*databaseGetServerProtocol)(FDBDatabase* database, uint64_t expectedVersion);

	FDBFuture* (*databasePurgeBlobGranules)(FDBDatabase* db,
	                                        uint8_t const* begin_key_name,
	                                        int begin_key_name_length,
	                                        uint8_t const* end_key_name,
	                                        int end_key_name_length,
	                                        int64_t purge_version,
	                                        fdb_bool_t force);

	FDBFuture* (*databaseWaitPurgeGranulesComplete)(FDBDatabase* db,
	                                                uint8_t const* purge_key_name,
	                                                int purge_key_name_length);

	FDBFuture* (*databaseBlobbifyRange)(FDBDatabase* db,
	                                    uint8_t const* begin_key_name,
	                                    int begin_key_name_length,
	                                    uint8_t const* end_key_name,
	                                    int end_key_name_length);

	FDBFuture* (*databaseBlobbifyRangeBlocking)(FDBDatabase* db,
	                                            uint8_t const* begin_key_name,
	                                            int begin_key_name_length,
	                                            uint8_t const* end_key_name,
	                                            int end_key_name_length);

	FDBFuture* (*databaseUnblobbifyRange)(FDBDatabase* db,
	                                      uint8_t const* begin_key_name,
	                                      int begin_key_name_length,
	                                      uint8_t const* end_key_name,
	                                      int end_key_name_length);

	FDBFuture* (*databaseListBlobbifiedRanges)(FDBDatabase* db,
	                                           uint8_t const* begin_key_name,
	                                           int begin_key_name_length,
	                                           uint8_t const* end_key_name,
	                                           int end_key_name_length,
	                                           int rangeLimit);

	FDBFuture* (*databaseVerifyBlobRange)(FDBDatabase* db,
	                                      uint8_t const* begin_key_name,
	                                      int begin_key_name_length,
	                                      uint8_t const* end_key_name,
	                                      int end_key_name_length,
	                                      int64_t version);

	FDBFuture* (*databaseFlushBlobRange)(FDBDatabase* db,
	                                     uint8_t const* begin_key_name,
	                                     int begin_key_name_length,
	                                     uint8_t const* end_key_name,
	                                     int end_key_name_length,
	                                     fdb_bool_t compact,
	                                     int64_t version);

	FDBFuture* (*databaseGetClientStatus)(FDBDatabase* db);

	// Tenant
	fdb_error_t (*tenantCreateTransaction)(FDBTenant* tenant, FDBTransaction** outTransaction);

	FDBFuture* (*tenantPurgeBlobGranules)(FDBTenant* db,
	                                      uint8_t const* begin_key_name,
	                                      int begin_key_name_length,
	                                      uint8_t const* end_key_name,
	                                      int end_key_name_length,
	                                      int64_t purge_version,
	                                      fdb_bool_t force);

	FDBFuture* (*tenantWaitPurgeGranulesComplete)(FDBTenant* db,
	                                              uint8_t const* purge_key_name,
	                                              int purge_key_name_length);

	FDBFuture* (*tenantBlobbifyRange)(FDBTenant* tenant,
	                                  uint8_t const* begin_key_name,
	                                  int begin_key_name_length,
	                                  uint8_t const* end_key_name,
	                                  int end_key_name_length);

	FDBFuture* (*tenantBlobbifyRangeBlocking)(FDBTenant* tenant,
	                                          uint8_t const* begin_key_name,
	                                          int begin_key_name_length,
	                                          uint8_t const* end_key_name,
	                                          int end_key_name_length);

	FDBFuture* (*tenantUnblobbifyRange)(FDBTenant* tenant,
	                                    uint8_t const* begin_key_name,
	                                    int begin_key_name_length,
	                                    uint8_t const* end_key_name,
	                                    int end_key_name_length);

	FDBFuture* (*tenantListBlobbifiedRanges)(FDBTenant* tenant,
	                                         uint8_t const* begin_key_name,
	                                         int begin_key_name_length,
	                                         uint8_t const* end_key_name,
	                                         int end_key_name_length,
	                                         int rangeLimit);

	FDBFuture* (*tenantVerifyBlobRange)(FDBTenant* tenant,
	                                    uint8_t const* begin_key_name,
	                                    int begin_key_name_length,
	                                    uint8_t const* end_key_name,
	                                    int end_key_name_length,
	                                    int64_t version);

	FDBFuture* (*tenantFlushBlobRange)(FDBTenant* db,
	                                   uint8_t const* begin_key_name,
	                                   int begin_key_name_length,
	                                   uint8_t const* end_key_name,
	                                   int end_key_name_length,
	                                   fdb_bool_t compact,
	                                   int64_t version);

	FDBFuture* (*tenantGetId)(FDBTenant* tenant);
	void (*tenantDestroy)(FDBTenant* tenant);

	// Transaction
	fdb_error_t (*transactionSetOption)(FDBTransaction* tr,
	                                    FDBTransactionOption option,
	                                    uint8_t const* value,
	                                    int valueLength);
	void (*transactionDestroy)(FDBTransaction* tr);

	void (*transactionSetReadVersion)(FDBTransaction* tr, int64_t version);
	FDBFuture* (*transactionGetReadVersion)(FDBTransaction* tr);

	FDBFuture* (*transactionGet)(FDBTransaction* tr, uint8_t const* keyName, int keyNameLength, fdb_bool_t snapshot);
	FDBFuture* (*transactionGetKey)(FDBTransaction* tr,
	                                uint8_t const* keyName,
	                                int keyNameLength,
	                                fdb_bool_t orEqual,
	                                int offset,
	                                fdb_bool_t snapshot);
	FDBFuture* (*transactionGetAddressesForKey)(FDBTransaction* tr, uint8_t const* keyName, int keyNameLength);
	FDBFuture* (*transactionGetRange)(FDBTransaction* tr,
	                                  uint8_t const* beginKeyName,
	                                  int beginKeyNameLength,
	                                  fdb_bool_t beginOrEqual,
	                                  int beginOffset,
	                                  uint8_t const* endKeyName,
	                                  int endKeyNameLength,
	                                  fdb_bool_t endOrEqual,
	                                  int endOffset,
	                                  int limit,
	                                  int targetBytes,
	                                  FDBStreamingMode mode,
	                                  int iteration,
	                                  fdb_bool_t snapshot,
	                                  fdb_bool_t reverse);
	FDBFuture* (*transactionGetMappedRange)(FDBTransaction* tr,
	                                        uint8_t const* beginKeyName,
	                                        int beginKeyNameLength,
	                                        fdb_bool_t beginOrEqual,
	                                        int beginOffset,
	                                        uint8_t const* endKeyName,
	                                        int endKeyNameLength,
	                                        fdb_bool_t endOrEqual,
	                                        int endOffset,
	                                        uint8_t const* mapper_name,
	                                        int mapper_name_length,
	                                        int limit,
	                                        int targetBytes,
	                                        FDBStreamingMode mode,
	                                        int iteration,
	                                        fdb_bool_t snapshot,
	                                        fdb_bool_t reverse);
	FDBFuture* (*transactionGetVersionstamp)(FDBTransaction* tr);

	void (*transactionSet)(FDBTransaction* tr,
	                       uint8_t const* keyName,
	                       int keyNameLength,
	                       uint8_t const* value,
	                       int valueLength);
	void (*transactionClear)(FDBTransaction* tr, uint8_t const* keyName, int keyNameLength);
	void (*transactionClearRange)(FDBTransaction* tr,
	                              uint8_t const* beginKeyName,
	                              int beginKeyNameLength,
	                              uint8_t const* endKeyName,
	                              int endKeyNameLength);
	void (*transactionAtomicOp)(FDBTransaction* tr,
	                            uint8_t const* keyName,
	                            int keyNameLength,
	                            uint8_t const* param,
	                            int paramLength,
	                            FDBMutationType operationType);

	FDBFuture* (*transactionGetEstimatedRangeSizeBytes)(FDBTransaction* tr,
	                                                    uint8_t const* begin_key_name,
	                                                    int begin_key_name_length,
	                                                    uint8_t const* end_key_name,
	                                                    int end_key_name_length);

	FDBFuture* (*transactionGetRangeSplitPoints)(FDBTransaction* tr,
	                                             uint8_t const* begin_key_name,
	                                             int begin_key_name_length,
	                                             uint8_t const* end_key_name,
	                                             int end_key_name_length,
	                                             int64_t chunkSize);

	FDBFuture* (*transactionGetBlobGranuleRanges)(FDBTransaction* tr,
	                                              uint8_t const* begin_key_name,
	                                              int begin_key_name_length,
	                                              uint8_t const* end_key_name,
	                                              int end_key_name_length,
	                                              int rangeLimit);

	FDBResult* (*transactionReadBlobGranules)(FDBTransaction* tr,
	                                          uint8_t const* begin_key_name,
	                                          int begin_key_name_length,
	                                          uint8_t const* end_key_name,
	                                          int end_key_name_length,
	                                          int64_t beginVersion,
	                                          int64_t readVersion);

	FDBFuture* (*transactionReadBlobGranulesStart)(FDBTransaction* tr,
	                                               uint8_t const* begin_key_name,
	                                               int begin_key_name_length,
	                                               uint8_t const* end_key_name,
	                                               int end_key_name_length,
	                                               int64_t beginVersion,
	                                               int64_t readVersion,
	                                               int64_t* readVersionOut);

	FDBResult* (*transactionReadBlobGranulesFinish)(FDBTransaction* tr,
	                                                FDBFuture* startFuture,
	                                                uint8_t const* begin_key_name,
	                                                int begin_key_name_length,
	                                                uint8_t const* end_key_name,
	                                                int end_key_name_length,
	                                                int64_t beginVersion,
	                                                int64_t readVersion,
	                                                FDBReadBlobGranuleContext* granule_context);

	FDBFuture* (*transactionSummarizeBlobGranules)(FDBTransaction* tr,
	                                               uint8_t const* begin_key_name,
	                                               int begin_key_name_length,
	                                               uint8_t const* end_key_name,
	                                               int end_key_name_length,
	                                               int64_t summaryVersion,
	                                               int rangeLimit);

	FDBFuture* (*transactionCommit)(FDBTransaction* tr);
	fdb_error_t (*transactionGetCommittedVersion)(FDBTransaction* tr, int64_t* outVersion);
	FDBFuture* (*transactionGetTagThrottledDuration)(FDBTransaction* tr);
	FDBFuture* (*transactionGetTotalCost)(FDBTransaction* tr);
	FDBFuture* (*transactionGetApproximateSize)(FDBTransaction* tr);
	FDBFuture* (*transactionWatch)(FDBTransaction* tr, uint8_t const* keyName, int keyNameLength);
	FDBFuture* (*transactionOnError)(FDBTransaction* tr, fdb_error_t error);
	void (*transactionReset)(FDBTransaction* tr);
	void (*transactionCancel)(FDBTransaction* tr);

	fdb_error_t (*transactionAddConflictRange)(FDBTransaction* tr,
	                                           uint8_t const* beginKeyName,
	                                           int beginKeyNameLength,
	                                           uint8_t const* endKeyName,
	                                           int endKeyNameLength,
	                                           FDBConflictRangeType);

	// Future
	fdb_error_t (*futureGetDatabase)(FDBFuture* f, FDBDatabase** outDb);
	fdb_error_t (*futureGetInt64)(FDBFuture* f, int64_t* outValue);
	fdb_error_t (*futureGetUInt64)(FDBFuture* f, uint64_t* outValue);
	fdb_error_t (*futureGetBool)(FDBFuture* f, fdb_bool_t* outValue);
	fdb_error_t (*futureGetDouble)(FDBFuture* f, double* outValue);
	fdb_error_t (*futureGetError)(FDBFuture* f);
	fdb_error_t (*futureGetKey)(FDBFuture* f, uint8_t const** outKey, int* outKeyLength);
	fdb_error_t (*futureGetValue)(FDBFuture* f, fdb_bool_t* outPresent, uint8_t const** outValue, int* outValueLength);
	fdb_error_t (*futureGetStringArray)(FDBFuture* f, const char*** outStrings, int* outCount);
	fdb_error_t (*futureGetKeyRangeArray)(FDBFuture* f, const FDBKeyRange** out_keyranges, int* outCount);
	fdb_error_t (*futureGetKeyArray)(FDBFuture* f, FDBKey const** outKeys, int* outCount);
	fdb_error_t (*futureGetKeyValueArray)(FDBFuture* f, FDBKeyValue const** outKV, int* outCount, fdb_bool_t* outMore);
	fdb_error_t (*futureGetMappedKeyValueArray)(FDBFuture* f,
	                                            FDBMappedKeyValue const** outKVM,
	                                            int* outCount,
	                                            fdb_bool_t* outMore);
	fdb_error_t (*futureGetGranuleSummaryArray)(FDBFuture* f, const FDBGranuleSummary** out_summaries, int* outCount);
	fdb_error_t (*futureGetSharedState)(FDBFuture* f, DatabaseSharedState** outPtr);
	fdb_error_t (*futureSetCallback)(FDBFuture* f, FDBCallback callback, void* callback_parameter);
	void (*futureCancel)(FDBFuture* f);
	void (*futureDestroy)(FDBFuture* f);

	fdb_error_t (*resultGetKeyValueArray)(FDBResult* f, FDBKeyValue const** outKV, int* outCount, fdb_bool_t* outMore);
	void (*resultDestroy)(FDBResult* f);

	// Legacy Support
	FDBFuture* (*createCluster)(const char* clusterFilePath);
	FDBFuture* (*clusterCreateDatabase)(FDBCluster* cluster, uint8_t* dbName, int dbNameLength);
	void (*clusterDestroy)(FDBCluster* cluster);
	fdb_error_t (*futureGetCluster)(FDBFuture* f, FDBCluster** outCluster);

	// Build ID
	const uint8_t* (*retrieveBuildID)();
};

// An implementation of ITransaction that wraps a transaction object created on an externally loaded client library.
// All API calls to that transaction are routed through the external library.
class DLTransaction : public ITransaction, ThreadSafeReferenceCounted<DLTransaction> {
public:
	DLTransaction(Reference<FdbCApi> api, FdbCApi::FDBTransaction* tr) : api(api), tr(tr) {}
	~DLTransaction() override { api->transactionDestroy(tr); }

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
	                                   bool reverse = false) override;
	ThreadFuture<RangeResult> getRange(const KeyRangeRef& keys,
	                                   GetRangeLimits limits,
	                                   bool snapshot = false,
	                                   bool reverse = false) override;
	ThreadFuture<MappedRangeResult> getMappedRange(const KeySelectorRef& begin,
	                                               const KeySelectorRef& end,
	                                               const StringRef& mapper,
	                                               GetRangeLimits limits,
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
	                                           ReadBlobGranuleContext granule_context) override;

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
	ThreadFuture<SpanContext> getSpanContext() override { return SpanContext(); };
	ThreadFuture<double> getTagThrottledDuration() override;
	ThreadFuture<int64_t> getTotalCost() override;
	ThreadFuture<int64_t> getApproximateSize() override;

	void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;

	ThreadFuture<Void> onError(Error const& e) override;
	void reset() override;

	Optional<TenantName> getTenant() override {
		ASSERT(false);
		throw internal_error();
	}

	void debugTrace(BaseTraceEvent&& event) override;
	void debugPrint(std::string const& message) override;

	void addref() override { ThreadSafeReferenceCounted<DLTransaction>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<DLTransaction>::delref(); }

private:
	const Reference<FdbCApi> api;
	FdbCApi::FDBTransaction* const tr;
};

class DLTenant : public ITenant, ThreadSafeReferenceCounted<DLTenant> {
public:
	DLTenant(Reference<FdbCApi> api, FdbCApi::FDBTenant* tenant) : api(api), tenant(tenant) {}
	~DLTenant() override {
		if (tenant) {
			api->tenantDestroy(tenant);
		}
	}

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
	ThreadFuture<bool> flushBlobRange(const KeyRangeRef& keyRange, bool compact, Optional<Version> version) override;

	void addref() override { ThreadSafeReferenceCounted<DLTenant>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<DLTenant>::delref(); }

private:
	const Reference<FdbCApi> api;
	FdbCApi::FDBTenant* tenant;
};

// An implementation of IDatabase that wraps a database object created on an externally loaded client library.
// All API calls to that database are routed through the external library.
class DLDatabase : public IDatabase, ThreadSafeReferenceCounted<DLDatabase> {
public:
	DLDatabase(Reference<FdbCApi> api, FdbCApi::FDBDatabase* db) : api(api), db(db), ready(Void()) {}
	DLDatabase(Reference<FdbCApi> api, ThreadFuture<FdbCApi::FDBDatabase*> dbFuture);
	~DLDatabase() override {
		if (db) {
			api->databaseDestroy(db);
		}
	}

	ThreadFuture<Void> onReady();

	Reference<ITenant> openTenant(TenantNameRef tenantName) override;
	Reference<ITransaction> createTransaction() override;
	void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;
	double getMainThreadBusyness() override;

	// Returns the protocol version reported by the coordinator this client is connected to
	// If an expected version is given, the future won't return until the protocol version is different than expected
	// Note: this will never return if the server is running a protocol from FDB 5.0 or older
	ThreadFuture<ProtocolVersion> getServerProtocol(
	    Optional<ProtocolVersion> expectedVersion = Optional<ProtocolVersion>()) override;

	void addref() override { ThreadSafeReferenceCounted<DLDatabase>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<DLDatabase>::delref(); }

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
	ThreadFuture<bool> flushBlobRange(const KeyRangeRef& keyRange, bool compact, Optional<Version> version) override;

	ThreadFuture<DatabaseSharedState*> createSharedState() override;
	void setSharedState(DatabaseSharedState* p) override;

	// Return a JSON string containing database client-side status information
	ThreadFuture<Standalone<StringRef>> getClientStatus() override;

private:
	const Reference<FdbCApi> api;
	FdbCApi::FDBDatabase*
	    db; // Always set if API version >= 610, otherwise guaranteed to be set when onReady future is set
	ThreadFuture<Void> ready;
};

// An implementation of IClientApi that re-issues API calls to the C API of an externally loaded client library.
// The DL prefix stands for "dynamic library".
class DLApi : public IClientApi {
public:
	DLApi(std::string fdbCPath, bool unlinkOnLoad = false);

	void selectApiVersion(int apiVersion) override;
	const char* getClientVersion() override;
	void useFutureProtocolVersion() override;
	const uint8_t* retrieveBuildID();

	void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;
	void setupNetwork() override;
	void runNetwork() override;
	void stopNetwork() override;

	Reference<IDatabase> createDatabase(const char* clusterFile) override;
	Reference<IDatabase> createDatabase609(const char* clusterFilePath); // legacy database creation

	Reference<IDatabase> createDatabaseFromConnectionString(const char* connectionString) override;

	void addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) override;

private:
	const std::string fdbCPath;
	const Reference<FdbCApi> api;
	const bool unlinkOnLoad;
	int headerVersion;
	bool networkSetup;

	Mutex lock;
	std::vector<std::pair<void (*)(void*), void*>> threadCompletionHooks;

	void init();
};

class MultiVersionDatabase;
class MultiVersionTenant;

// An implementation of ITransaction that wraps a transaction created either locally or through a dynamically loaded
// external client. When needed (e.g on cluster version change), the MultiVersionTransaction can automatically replace
// its wrapped transaction with one from another client.
class MultiVersionTransaction : public ITransaction, ThreadSafeReferenceCounted<MultiVersionTransaction> {
public:
	MultiVersionTransaction(Reference<MultiVersionDatabase> db,
	                        Optional<Reference<MultiVersionTenant>> tenant,
	                        UniqueOrderedOptionList<FDBTransactionOptions> defaultOptions);

	~MultiVersionTransaction() override;

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
	                                   bool reverse = false) override;
	ThreadFuture<RangeResult> getRange(const KeyRangeRef& keys,
	                                   GetRangeLimits limits,
	                                   bool snapshot = false,
	                                   bool reverse = false) override;
	ThreadFuture<MappedRangeResult> getMappedRange(const KeySelectorRef& begin,
	                                               const KeySelectorRef& end,
	                                               const StringRef& mapper,
	                                               GetRangeLimits limits,
	                                               bool snapshot,
	                                               bool reverse) override;
	ThreadFuture<Standalone<VectorRef<const char*>>> getAddressesForKey(const KeyRef& key) override;
	ThreadFuture<Standalone<StringRef>> getVersionstamp() override;

	void addReadConflictRange(const KeyRangeRef& keys) override;
	ThreadFuture<int64_t> getEstimatedRangeSizeBytes(const KeyRangeRef& keys) override;

	ThreadFuture<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(const KeyRangeRef& range,
	                                                                int64_t chunkSize) override;
	ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> getBlobGranuleRanges(const KeyRangeRef& keyRange,
	                                                                      int rangeLimit) override;

	ThreadResult<RangeResult> readBlobGranules(const KeyRangeRef& keyRange,
	                                           Version beginVersion,
	                                           Optional<Version> readVersion,
	                                           ReadBlobGranuleContext granule_context) override;

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
	ThreadFuture<double> getTagThrottledDuration() override;
	ThreadFuture<int64_t> getTotalCost() override;
	ThreadFuture<int64_t> getApproximateSize() override;

	void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;

	ThreadFuture<Void> onError(Error const& e) override;
	void reset() override;

	Optional<TenantName> getTenant() override;

	void addref() override { ThreadSafeReferenceCounted<MultiVersionTransaction>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<MultiVersionTransaction>::delref(); }

	// return true if the underlying transaction pointer is not empty
	bool isValid() override;

	// These currently only work for local clients. To support external clients,
	// we would likely need to store the events/messages in the MVC layer and add a hook
	// here to commit.
	void debugTrace(BaseTraceEvent&& event) override;
	void debugPrint(std::string const& message) override;

private:
	const Reference<MultiVersionDatabase> db;
	const Optional<Reference<MultiVersionTenant>> tenant;
	ThreadSpinLock lock;

	struct TransactionInfo {
		Reference<ITransaction> transaction;
		ThreadFuture<Void> onChange;
		ErrorOr<Void> dbError = ErrorOr<Void>(Void());
	};

	// Timeout related variables for MultiVersionTransaction objects that do not have an underlying ITransaction

	// The time when the MultiVersionTransaction was last created or reset
	std::atomic<double> startTime;

	// A lock that needs to be held if using timeoutTsav or currentTimeout
	ThreadSpinLock timeoutLock;

	// A single assignment var (i.e. promise) that gets set with an error when the timeout elapses or the transaction
	// is reset or destroyed.
	Reference<ThreadSingleAssignmentVar<Void>> timeoutTsav;

	// A reference to the current actor waiting for the timeout. This actor will set the timeoutTsav promise.
	ThreadFuture<Void> currentTimeout;

	// Configure a timeout based on the options set for this transaction. This timeout only applies
	// if we don't have an underlying database object to connect with.
	void setTimeout(Optional<StringRef> value);

	void resetTimeout();

	// Creates a ThreadFuture<T> that will signal an error if the transaction times out.
	template <class T>
	ThreadFuture<T> makeTimeout();

	template <class T>
	ThreadResult<T> abortableTimeoutResult(ThreadFuture<Void> abortSignal);

	template <class T>
	ThreadResult<T> abortableResult(ThreadResult<T> result, ThreadFuture<Void> abortSignal);

	TransactionInfo transaction;

	TransactionInfo getTransaction();
	void updateTransaction(bool setPersistentOptions);
	void setDefaultOptions(UniqueOrderedOptionList<FDBTransactionOptions> options);

	template <class T, class... Args>
	ThreadFuture<T> executeOperation(ThreadFuture<T> (ITransaction::*func)(Args...), Args&&... args);

	std::vector<std::pair<FDBTransactionOptions::Option, Optional<Standalone<StringRef>>>> persistentOptions;
	std::vector<std::pair<FDBTransactionOptions::Option, Optional<WipedString>>> sensitivePersistentOptions;
};

struct ClientDesc {
	std::string const libPath;
	bool const external;
	bool const useFutureVersion;

	ClientDesc(std::string libPath, bool external, bool useFutureVersion)
	  : libPath(libPath), external(external), useFutureVersion(useFutureVersion) {}
};

struct ClientInfo : ClientDesc, ThreadSafeReferenceCounted<ClientInfo> {
	ProtocolVersion protocolVersion;
	std::string releaseVersion = "unknown";
	IClientApi* api;
	bool failed;
	std::atomic_bool initialized;
	int threadIndex;
	std::vector<std::pair<void (*)(void*), void*>> threadCompletionHooks;

	ClientInfo()
	  : ClientDesc(std::string(), false, false), protocolVersion(0), api(nullptr), failed(true), initialized(false),
	    threadIndex(0) {}
	ClientInfo(IClientApi* api)
	  : ClientDesc("internal", false, false), protocolVersion(0), api(api), failed(false), initialized(false),
	    threadIndex(0) {}
	ClientInfo(IClientApi* api, std::string libPath, bool useFutureVersion, int threadIndex)
	  : ClientDesc(libPath, true, useFutureVersion), protocolVersion(0), api(api), failed(false), initialized(false),
	    threadIndex(threadIndex) {}

	void loadVersion();
	bool canReplace(Reference<ClientInfo> other) const;
	std::string getTraceFileIdentifier(const std::string& baseIdentifier);
};

class MultiVersionApi;

// An implementation of ITenant that wraps a tenant created either locally or through a dynamically loaded
// external client. The wrapped ITenant is automatically changed when the MultiVersionDatabase used to create
// it connects with a different version.
class MultiVersionTenant final : public ITenant, ThreadSafeReferenceCounted<MultiVersionTenant> {
public:
	MultiVersionTenant(Reference<MultiVersionDatabase> db, TenantNameRef tenantName);
	~MultiVersionTenant() override;

	Reference<ITransaction> createTransaction() override;

	template <class T, class... Args>
	ThreadFuture<T> executeOperation(ThreadFuture<T> (ITenant::*func)(Args...), Args&&... args);

	ThreadFuture<int64_t> getId() override;
	ThreadFuture<Key> purgeBlobGranules(const KeyRangeRef& keyRange, Version purgeVersion, bool force) override;
	ThreadFuture<Void> waitPurgeGranulesComplete(const KeyRef& purgeKey) override;

	ThreadFuture<bool> blobbifyRange(const KeyRangeRef& keyRange) override;
	ThreadFuture<bool> blobbifyRangeBlocking(const KeyRangeRef& keyRange) override;
	ThreadFuture<bool> unblobbifyRange(const KeyRangeRef& keyRange) override;
	ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> listBlobbifiedRanges(const KeyRangeRef& keyRange,
	                                                                      int rangeLimit) override;
	ThreadFuture<Version> verifyBlobRange(const KeyRangeRef& keyRange, Optional<Version> version) override;
	ThreadFuture<bool> flushBlobRange(const KeyRangeRef& keyRange, bool compact, Optional<Version> version) override;

	void addref() override { ThreadSafeReferenceCounted<MultiVersionTenant>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<MultiVersionTenant>::delref(); }

	// A struct that manages the current connection state of the MultiVersionDatabase. This wraps the underlying
	// IDatabase object that is currently interacting with the cluster.
	struct TenantState : ThreadSafeReferenceCounted<TenantState> {
		TenantState(Reference<MultiVersionDatabase> db, TenantNameRef tenantName);

		// Creates a new underlying tenant object whenever the database connection changes. This change is signaled
		// to open transactions via an AsyncVar.
		void updateTenant();

		// Cleans up local state to break reference cycles
		void close();

		Reference<ThreadSafeAsyncVar<Reference<ITenant>>> tenantVar;
		const TenantName tenantName;

		Reference<MultiVersionDatabase> db;

		Mutex tenantLock;
		ThreadFuture<Void> tenantUpdater;

		bool closed;
	};

	Reference<TenantState> tenantState;
};

class ClusterConnectionRecord {
private:
	enum class Type { FILE, CONNECTION_STRING };
	ClusterConnectionRecord(Type type, std::string const& recordStr) : type(type), recordStr(recordStr) {}

	Type type;
	std::string recordStr;

public:
	static ClusterConnectionRecord fromFile(std::string const& clusterFilePath) {
		return ClusterConnectionRecord(Type::FILE, clusterFilePath);
	}

	static ClusterConnectionRecord fromConnectionString(std::string const& connectionString) {
		return ClusterConnectionRecord(Type::CONNECTION_STRING, connectionString);
	}

	Reference<IDatabase> createDatabase(IClientApi* api) const {
		switch (type) {
		case Type::FILE:
			return api->createDatabase(recordStr.c_str());
		case Type::CONNECTION_STRING:
			return api->createDatabaseFromConnectionString(recordStr.c_str());
		default:
			ASSERT(false);
			throw internal_error();
		}
	}

	std::string toString() const {
		switch (type) {
		case Type::FILE:
			if (recordStr.empty()) {
				return "default file";
			} else {
				return "file: " + recordStr;
			}
		case Type::CONNECTION_STRING:
			return "connection string: " + recordStr;
		default:
			ASSERT(false);
			throw internal_error();
		}
	}
};

template <>
struct Traceable<ClusterConnectionRecord> : std::true_type {
	static std::string toString(ClusterConnectionRecord const& connectionRecord) { return connectionRecord.toString(); }
};

// An implementation of IDatabase that wraps a database created either locally or through a dynamically loaded
// external client. The MultiVersionDatabase monitors the protocol version of the cluster and automatically
// replaces the wrapped database when the protocol version changes.
class MultiVersionDatabase final : public IDatabase, ThreadSafeReferenceCounted<MultiVersionDatabase> {
public:
	MultiVersionDatabase(MultiVersionApi* api,
	                     int threadIdx,
	                     ClusterConnectionRecord const& connectionRecord,
	                     Reference<IDatabase> db,
	                     Reference<IDatabase> versionMonitorDb,
	                     bool openConnectors = true);

	~MultiVersionDatabase() override;

	Reference<ITenant> openTenant(TenantNameRef tenantName) override;
	Reference<ITransaction> createTransaction() override;
	void setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;
	double getMainThreadBusyness() override;

	// Returns the protocol version reported by the coordinator this client is connected to
	// If an expected version is given, the future won't return until the protocol version is different than expected
	// Note: this will never return if the server is running a protocol from FDB 5.0 or older
	ThreadFuture<ProtocolVersion> getServerProtocol(
	    Optional<ProtocolVersion> expectedVersion = Optional<ProtocolVersion>()) override;

	void addref() override { ThreadSafeReferenceCounted<MultiVersionDatabase>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<MultiVersionDatabase>::delref(); }

	// Create a MultiVersionDatabase that wraps an already created IDatabase object
	// For internal use in testing
	static Reference<IDatabase> debugCreateFromExistingDatabase(Reference<IDatabase> db);

	template <class T, class... Args>
	ThreadFuture<T> executeOperation(ThreadFuture<T> (IDatabase::*func)(Args...), Args&&... args);

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
	ThreadFuture<bool> flushBlobRange(const KeyRangeRef& keyRange, bool compact, Optional<Version> version) override;

	ThreadFuture<DatabaseSharedState*> createSharedState() override;
	void setSharedState(DatabaseSharedState* p) override;

	// Return a JSON string containing database client-side status information
	ThreadFuture<Standalone<StringRef>> getClientStatus() override;

	// private:

	struct LegacyVersionMonitor;

	// Database initialization state
	enum class InitializationState { INITIALIZING, INITIALIZATION_FAILED, CREATED, INCOMPATIBLE, CLOSED };

	// A struct that manages the current connection state of the MultiVersionDatabase. This wraps the underlying
	// IDatabase object that is currently interacting with the cluster.
	struct DatabaseState : ThreadSafeReferenceCounted<DatabaseState> {
		DatabaseState(ClusterConnectionRecord const& connectionRecord, Reference<IDatabase> versionMonitorDb);

		// Replaces the active database connection with a new one. Must be called from the main thread.
		void updateDatabase(Reference<IDatabase> newDb, Reference<ClientInfo> client);

		// Called when a change to the protocol version of the cluster has been detected.
		// Must be called from the main thread
		void protocolVersionChanged(ProtocolVersion protocolVersion);

		// Adds a client (local or externally loaded) that can be used to connect to the cluster
		void addClient(Reference<ClientInfo> client);

		// Watch the cluster protocol version for changes and update the database state when it does.
		// Must be called from the main thread
		ThreadFuture<Void> monitorProtocolVersion();

		// Starts version monitors for old client versions that don't support connect packet monitoring (<= 5.0).
		// Must be called from the main thread
		void startLegacyVersionMonitors();

		// Set a new database connection
		void setDatabase(Reference<IDatabase> db);

		// Get database initialization error if initialization failed
		ErrorOr<Void> getInitializationError();

		// Return a JSON string containing database client-side status information
		Standalone<StringRef> getClientStatus(ErrorOr<Standalone<StringRef>> dbContextStatus);

		// Cleans up state for the legacy version monitors to break reference cycles
		void close();

		Reference<IDatabase> db;
		const Reference<ThreadSafeAsyncVar<Reference<IDatabase>>> dbVar;
		ClusterConnectionRecord connectionRecord;
		std::string clusterId;

		// Used to monitor the cluster protocol version. Will be the same as db unless we have either not connected
		// yet or if the client version associated with db does not support protocol monitoring. In those cases,
		// this will be a specially created local db.
		Reference<IDatabase> versionMonitorDb;

		// The current database initialization state
		std::atomic<InitializationState> initializationState;

		// Last error received during database initialization
		// Set on transition to INITIALIZATION_FAILED state, never changed afterwards
		Error initializationError;

		ThreadFuture<Void> changed;
		ThreadFuture<Void> dbReady;
		ThreadFuture<Void> protocolVersionMonitor;

		Future<Void> sharedStateUpdater;
		bool isConfigDB;

		// Versions older than 6.1 do not benefit from having their database connections closed. Additionally,
		// there are various issues that result in negative behavior in some cases if the connections are closed.
		// Therefore, we leave them open.
		std::map<ProtocolVersion, Reference<IDatabase>> legacyDatabaseConnections;

		// Versions 5.0 and older do not support connection packet monitoring and require alternate techniques to
		// determine the cluster version.
		std::list<Reference<LegacyVersionMonitor>> legacyVersionMonitors;

		Optional<ProtocolVersion> dbProtocolVersion;

		// This maps a normalized protocol version to the client associated with it. This prevents compatible
		// differences in protocol version not matching each other.
		std::map<ProtocolVersion, Reference<ClientInfo>> clients;

		std::vector<std::pair<FDBDatabaseOptions::Option, Optional<Standalone<StringRef>>>> options;
		UniqueOrderedOptionList<FDBTransactionOptions> transactionDefaultOptions;
		Mutex optionLock;
	};

	// A struct that enables monitoring whether the cluster is running an old version (<= 5.0) that doesn't support
	// connect packet monitoring.
	struct LegacyVersionMonitor : ThreadSafeReferenceCounted<LegacyVersionMonitor> {
		LegacyVersionMonitor(Reference<ClientInfo> const& client) : client(client), monitorRunning(false) {}

		// Terminates the version monitor to break reference cycles
		void close();

		// Starts the connection monitor by creating a database object at an old version.
		// Must be called from the main thread
		void startConnectionMonitor(Reference<DatabaseState> dbState);

		// Runs a GRV probe on the cluster to determine if the client version is compatible with the cluster.
		// Must be called from main thread
		void runGrvProbe(Reference<DatabaseState> dbState);

		Reference<ClientInfo> client;
		Reference<IDatabase> db;
		Reference<ITransaction> tr;

		ThreadFuture<Void> versionMonitor;
		bool monitorRunning;
	};

	const Reference<DatabaseState> dbState;
	friend class MultiVersionTransaction;
};

// An implementation of IClientApi that can choose between multiple different client implementations either provided
// locally within the primary loaded fdb_c client or through any number of dynamically loaded clients.
//
// This functionality is used to provide support for multiple protocol versions simultaneously.
class MultiVersionApi : public IClientApi {
public:
	void selectApiVersion(int apiVersion) override;
	const char* getClientVersion() override;
	void useFutureProtocolVersion() override;

	void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>()) override;
	void setupNetwork() override;
	void runNetwork() override;
	void stopNetwork() override;
	void addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) override;

	// Creates an IDatabase object that represents a connection to the cluster
	Reference<IDatabase> createDatabase(ClusterConnectionRecord const& connectionRecord);
	Reference<IDatabase> createDatabase(const char* clusterFilePath) override;
	Reference<IDatabase> createDatabaseFromConnectionString(const char* connectionString) override;

	static MultiVersionApi* api;

	Reference<ClientInfo> getLocalClient();
	void runOnExternalClients(int threadId,
	                          std::function<void(Reference<ClientInfo>)>,
	                          bool runOnFailedClients = false,
	                          bool failOnError = false);
	void runOnExternalClientsAllThreads(std::function<void(Reference<ClientInfo>)>,
	                                    bool runOnFailedClients = false,
	                                    bool failOnError = false);
	void runOnExternalClientsThreadRange(std::function<void(Reference<ClientInfo>)>,
	                                     int startIdx,
	                                     int endIdx,
	                                     bool runOnFailedClients = false,
	                                     bool failOnError = false);

	bool hasNonFailedExternalClients();

	void updateSupportedVersions();

	bool callbackOnMainThread;
	bool localClientDisabled;
	Future<std::string> updateClusterSharedStateMap(ClusterConnectionRecord const& connectionRecord,
	                                                ProtocolVersion dbProtocolVersion,
	                                                Reference<IDatabase> db);
	void clearClusterSharedStateMapEntry(std::string clusterId, ProtocolVersion dbProtocolVersion);

	// Map of cluster ID -> DatabaseSharedState pointer Future
	// Upon cluster version upgrade, clear the map entry for that cluster
	struct SharedStateInfo {
		ThreadFuture<DatabaseSharedState*> sharedStateFuture;
		ProtocolVersion protocolVersion;
	};
	std::map<std::string, SharedStateInfo> clusterSharedStateMap;

	ApiVersion getApiVersion() { return apiVersion; }

private:
	MultiVersionApi();

	void loadEnvironmentVariableNetworkOptions();

	void disableMultiVersionClientApi();
	void setCallbacksOnExternalThreads();
	void addExternalLibrary(std::string path, bool useFutureVersion);
	void addExternalLibraryDirectory(std::string path);
	// Return a vector of (pathname, unlink_on_close) pairs.  Makes threadCount - 1 copies of the library stored in
	// path, and returns a vector of length threadCount.
	std::vector<std::pair<std::string, bool>> copyExternalLibraryPerThread(std::string path, std::string build_id);
	void disableLocalClient();
	void setSupportedClientVersions(Standalone<StringRef> versions);

	void setNetworkOptionInternal(FDBNetworkOptions::Option option, Optional<StringRef> value);

	Reference<ClientInfo> localClient;
	std::map<std::string, ClientDesc> externalClientDescriptions;
	std::map<std::string, std::vector<Reference<ClientInfo>>> externalClients;

	bool networkStartSetup;
	volatile bool networkSetup;
	bool disableBypass;
	volatile bool bypassMultiClientApi;
	volatile bool externalClient;
	bool ignoreExternalClientFailures;
	bool failIncompatibleClient;
	bool retainClientLibCopies;
	ApiVersion apiVersion;

	int nextThread = 0;
	int threadCount;
	std::string tmpDir;
	bool traceShareBaseNameAmongThreads;
	std::string traceFileIdentifier;

	Mutex lock;
	std::vector<std::pair<FDBNetworkOptions::Option, Optional<Standalone<StringRef>>>> options;
	std::map<FDBNetworkOptions::Option, std::set<Standalone<StringRef>>> setEnvOptions;
	volatile bool envOptionsLoaded;

	friend struct MultiVersionDatabase::DatabaseState;
};

#endif
