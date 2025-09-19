/*
 * NativeAPI.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_NATIVEAPI_ACTOR_G_H)
#define FDBCLIENT_NATIVEAPI_ACTOR_G_H
#include "fdbclient/NativeAPI.actor.g.h"
#elif !defined(FDBCLIENT_NATIVEAPI_ACTOR_H)
#define FDBCLIENT_NATIVEAPI_ACTOR_H

#include "flow/BooleanParam.h"
#include "flow/flow.h"
#include "flow/WipedString.h"
#include "flow/TDMetric.actor.h"
#include "flow/IRandom.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/ClientLogEvents.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/Tracing.h"
#include "flow/actorcompiler.h" // has to be last include

/*
// CLIENT_BUGGIFY should be used to randomly introduce failures at run time (like BUGGIFY but for client side testing)
// Unlike BUGGIFY, CLIENT_BUGGIFY can be enabled and disabled at runtime.
#define CLIENT_BUGGIFY_WITH_PROB(x)                                                                                    \
    (getSBVar(__FILE__, __LINE__, BuggifyType::Client) && deterministicRandom()->random01() < (x))
#define CLIENT_BUGGIFY CLIENT_BUGGIFY_WITH_PROB(P_BUGGIFIED_SECTION_FIRES[int(BuggifyType::Client)])
*/

FDB_BOOLEAN_PARAM(UseProvisionalProxies);

// Incomplete types that are reference counted
class DatabaseContext;
template <>
void addref(DatabaseContext* ptr);
template <>
void delref(DatabaseContext* ptr);

void validateOptionValuePresent(Optional<StringRef> value);
void validateOptionValueNotPresent(Optional<StringRef> value);

void enableClientInfoLogging();

struct NetworkOptions {
	std::string localAddress;
	std::string clusterFile;
	Optional<std::string> traceDirectory;
	uint64_t traceRollSize;
	uint64_t traceMaxLogsSize;
	std::string traceLogGroup;
	std::string traceFormat;
	std::string traceClockSource;
	std::string traceFileIdentifier;
	std::string tracePartialFileSuffix;
	bool traceInitializeOnSetup;
	Optional<bool> logClientInfo;
	Reference<ReferencedObject<Standalone<VectorRef<ClientVersionRef>>>> supportedVersions;
	bool runLoopProfilingEnabled;
	bool primaryClient;
	std::map<std::string, KnobValue> knobs;

	NetworkOptions();
};

class Database {
public:
	// Creates a database object that represents a connection to a cluster
	// This constructor uses a preallocated DatabaseContext that may have been created
	// on another thread
	static Database createDatabase(Reference<IClusterConnectionRecord> connRecord,
	                               int apiVersion,
	                               IsInternal internal = IsInternal::True,
	                               LocalityData const& clientLocality = LocalityData(),
	                               DatabaseContext* preallocatedDb = nullptr);

	static Database createDatabase(std::string connFileName,
	                               int apiVersion,
	                               IsInternal internal = IsInternal::True,
	                               LocalityData const& clientLocality = LocalityData());

	static Database createSimulatedExtraDatabase(std::string connectionString,
	                                             Optional<TenantName> defaultTenant = Optional<TenantName>());

	Database() {} // an uninitialized database can be destructed or reassigned safely; that's it
	void operator=(Database const& rhs) { db = rhs.db; }
	Database(Database const& rhs) : db(rhs.db) {}
	Database(Database&& r) noexcept : db(std::move(r.db)) {}
	void operator=(Database&& r) noexcept { db = std::move(r.db); }

	// For internal use by the native client:
	explicit Database(Reference<DatabaseContext> cx) : db(cx) {}
	explicit Database(DatabaseContext* cx) : db(cx) {}
	inline DatabaseContext* getPtr() const { return db.getPtr(); }
	inline DatabaseContext* extractPtr() { return db.extractPtr(); }
	DatabaseContext* operator->() const { return db.getPtr(); }
	Reference<DatabaseContext> getReference() const { return db; }

	const UniqueOrderedOptionList<FDBTransactionOptions>& getTransactionDefaults() const;

	template <std::invocable<Transaction*> Fun>
	Future<Void> run(Fun fun);

private:
	Reference<DatabaseContext> db;
};

void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>());

// Configures the global networking machinery
void setupNetwork(uint64_t transportId = 0, UseMetrics = UseMetrics::False);

// This call blocks while the network is running.  To use the API in a single-threaded
//  environment, the calling program must have ACTORs already launched that are waiting
//  to use the network.  In this case, the program can terminate by calling stopNetwork()
//  from a callback, thereby releasing this call to return.  In a multithreaded setup
//  this call can be called from a dedicated "networking" thread.  All the network-based
//  callbacks will happen on this second thread.  When a program is finished, the
//  call stopNetwork (from a non-networking thread) can cause the runNetwork() call to
//  return.
//
// Throws network_already_setup if g_network has already been initialized
void runNetwork();

// See above.  Can be called from a thread that is not the "networking thread"
//
// Throws network_not_setup if g_network has not been initialized
void stopNetwork();

struct StorageMetrics;

struct TransactionOptions {
	double maxBackoff;
	uint32_t getReadVersionFlags;
	uint32_t sizeLimit;
	int maxTransactionLoggingFieldLength;
	bool checkWritesEnabled : 1;
	bool causalWriteRisky : 1;
	bool commitOnFirstProxy : 1;
	bool debugDump : 1;
	bool lockAware : 1;
	bool readOnly : 1;
	bool firstInBatch : 1;
	bool includePort : 1;
	bool reportConflictingKeys : 1;
	bool expensiveClearCostEstimation : 1;
	bool useGrvCache : 1;
	bool skipGrvCache : 1;
	bool rawAccess : 1;
	bool bypassStorageQuota : 1;
	bool enableReplicaConsistencyCheck : 1;
	int requiredReplicas;

	TransactionPriority priority;

	TagSet tags; // All tags set on transaction
	TagSet readTags; // Tags that can be sent with read requests

	// update clear function if you add a new field

	TransactionOptions(Database const& cx);
	TransactionOptions();

	void reset(Database const& cx);

private:
	void clear();
};

class ReadYourWritesTransaction; // workaround cyclic dependency

struct TransactionLogInfo : public ReferenceCounted<TransactionLogInfo>, NonCopyable {
	enum LoggingLocation { DONT_LOG = 0, TRACE_LOG = 1, DATABASE = 2 };

	TransactionLogInfo() : logLocation(DONT_LOG), maxFieldLength(0) {}
	TransactionLogInfo(LoggingLocation location) : logLocation(location), maxFieldLength(0) {}
	TransactionLogInfo(std::string id, LoggingLocation location)
	  : logLocation(location), maxFieldLength(0), identifier(id) {}

	void setIdentifier(std::string id) { identifier = id; }
	void logTo(LoggingLocation loc) { logLocation = logLocation | loc; }

	template <typename T>
	void addLog(const T& event) {
		if (logLocation & TRACE_LOG) {
			ASSERT(!identifier.empty());
			event.logEvent(identifier, maxFieldLength);
		}

		if (flushed) {
			return;
		}

		if (logLocation & DATABASE) {
			logsAdded = true;
			static_assert(std::is_base_of<FdbClientLogEvents::Event, T>::value,
			              "Event should be derived class of FdbClientLogEvents::Event");
			trLogWriter << event;
		}
	}

	BinaryWriter trLogWriter{ IncludeVersion() };
	bool logsAdded{ false };
	bool flushed{ false };
	int logLocation;
	int maxFieldLength;
	std::string identifier;
};

struct Watch : public ReferenceCounted<Watch>, NonCopyable {
	Key key;
	Optional<Value> value;
	bool valuePresent;
	Optional<Value> setValue;
	bool setPresent;
	Promise<Void> onChangeTrigger;
	Promise<Void> onSetWatchTrigger;
	Future<Void> watchFuture;
	Optional<ReadOptions> readOptions;

	Watch() : valuePresent(false), setPresent(false), watchFuture(Never()) {}
	Watch(Key key) : key(key), valuePresent(false), setPresent(false), watchFuture(Never()) {}
	Watch(Key key, Optional<Value> val)
	  : key(key), value(val), valuePresent(true), setPresent(false), watchFuture(Never()) {}

	void setWatch(Future<Void> watchFuture);
};

class Tenant : public ReferenceCounted<Tenant>, public FastAllocated<Tenant>, NonCopyable {
public:
	Tenant(Database cx, TenantName name);
	explicit Tenant(int64_t id);
	Tenant(Future<int64_t> id, Optional<TenantName> name);

	static Tenant* allocateOnForeignThread() { return (Tenant*)Tenant::operator new(sizeof(Tenant)); }

	Future<Void> ready() const { return success(idFuture); }
	int64_t id() const;
	Future<int64_t> getIdFuture() const;
	KeyRef prefix() const;
	std::string description() const;

	Optional<TenantName> name;

private:
	mutable int64_t bigEndianId = -1;
	Future<int64_t> idFuture;
};

template <>
struct Traceable<Tenant> : std::true_type {
	static std::string toString(const Tenant& tenant) { return printable(tenant.description()); }
};

FDB_BOOLEAN_PARAM(AllowInvalidTenantID);
FDB_BOOLEAN_PARAM(ResolveDefaultTenant);

struct TransactionState : ReferenceCounted<TransactionState> {
	Database cx;
	Future<Version> readVersionFuture;
	Promise<Optional<Value>> metadataVersion;
	Optional<WipedString> authToken;
	Reference<TransactionLogInfo> trLogInfo;
	TransactionOptions options;
	Optional<ReadOptions> readOptions;

	TaskPriority taskID;
	SpanContext spanContext;
	UseProvisionalProxies useProvisionalProxies = UseProvisionalProxies::False;
	bool readVersionObtainedFromGrvProxy;
	// Measured by summing the bytes accessed by each read and write operation
	// after rounding up to the nearest page size and applying a write penalty
	int64_t totalCost = 0;

	double proxyTagThrottledDuration = 0.0;

	// Special flag to skip prepending tenant prefix to mutations and conflict ranges
	// when a dummy, internal transaction gets committed. The sole purpose of commitDummyTransaction() is to
	// resolve the state of earlier transaction that returned commit_unknown_result or request_maybe_delivered.
	// Therefore, the dummy transaction can simply reuse one conflict range of the earlier commit, if it already has
	// been prefixed.
	bool skipApplyTenantPrefix = false;

	int numErrors = 0;
	double startTime = 0;
	Promise<Standalone<StringRef>> versionstampPromise;

	Version committedVersion{ invalidVersion };

	// Used to save conflicting keys if FDBTransactionOptions::REPORT_CONFLICTING_KEYS is enabled
	// prefix/<key1> : '1' - any keys equal or larger than this key are (probably) conflicting keys
	// prefix/<key2> : '0' - any keys equal or larger than this key are (definitely) not conflicting keys
	std::shared_ptr<CoalescedKeyRangeMap<Value>> conflictingKeys;

	bool automaticIdempotency = false;

	Future<Void> startFuture;

	// Only available so that Transaction can have a default constructor, for use in state variables
	TransactionState(TaskPriority taskID, SpanContext spanContext)
	  : taskID(taskID), spanContext(spanContext), tenantSet(false) {}

	// VERSION_VECTOR changed default values of readVersionObtainedFromGrvProxy
	TransactionState(Database cx,
	                 Optional<Reference<Tenant>> tenant,
	                 TaskPriority taskID,
	                 SpanContext spanContext,
	                 Reference<TransactionLogInfo> trLogInfo);

	Reference<TransactionState> cloneAndReset(Reference<TransactionLogInfo> newTrLogInfo, bool generateNewSpan) const;

	Version readVersion() {
		ASSERT(readVersionFuture.isValid() && readVersionFuture.isReady());
		return readVersionFuture.get();
	}

	TenantInfo getTenantInfo(AllowInvalidTenantID allowInvalidTenantId = AllowInvalidTenantID::False);

	Optional<Reference<Tenant>> const& tenant();
	bool hasTenant(ResolveDefaultTenant ResolveDefaultTenant = ResolveDefaultTenant::True);
	int64_t tenantId() const { return tenant_.present() ? tenant_.get()->id() : TenantInfo::INVALID_TENANT; }

	void addClearCost();

	Future<Void> startTransaction(uint32_t readVersionFlags = 0);
	Future<Version> getReadVersion(uint32_t flags);

private:
	Optional<Reference<Tenant>> tenant_;
	bool tenantSet;
};

class Transaction : NonCopyable {
public:
	explicit Transaction(Database const& cx, Optional<Reference<Tenant>> const& tenant = Optional<Reference<Tenant>>());
	~Transaction();

	void setVersion(Version v);
	Future<Version> getReadVersion() {
		if (!trState->readVersionFuture.isValid()) {
			trState->readVersionFuture = trState->getReadVersion(0);
		}
		return trState->readVersionFuture;
	}
	Future<Version> getRawReadVersion();
	Optional<Version> getCachedReadVersion() const;

	[[nodiscard]] Future<Optional<Value>> get(const Key& key, Snapshot = Snapshot::False);
	[[nodiscard]] Future<Void> watch(Reference<Watch> watch);
	[[nodiscard]] Future<Key> getKey(const KeySelector& key, Snapshot = Snapshot::False);
	// Future< Optional<KeyValue> > get( const KeySelectorRef& key );
	[[nodiscard]] Future<RangeResult> getRange(const KeySelector& begin,
	                                           const KeySelector& end,
	                                           int limit,
	                                           Snapshot = Snapshot::False,
	                                           Reverse = Reverse::False);
	[[nodiscard]] Future<RangeResult> getRange(const KeySelector& begin,
	                                           const KeySelector& end,
	                                           GetRangeLimits limits,
	                                           Snapshot = Snapshot::False,
	                                           Reverse = Reverse::False);
	[[nodiscard]] Future<RangeResult> getRange(const KeyRange& keys,
	                                           int limit,
	                                           Snapshot snapshot = Snapshot::False,
	                                           Reverse reverse = Reverse::False) {
		return getRange(KeySelector(firstGreaterOrEqual(keys.begin), keys.arena()),
		                KeySelector(firstGreaterOrEqual(keys.end), keys.arena()),
		                limit,
		                snapshot,
		                reverse);
	}
	[[nodiscard]] Future<RangeResult> getRange(const KeyRange& keys,
	                                           GetRangeLimits limits,
	                                           Snapshot snapshot = Snapshot::False,
	                                           Reverse reverse = Reverse::False) {
		return getRange(KeySelector(firstGreaterOrEqual(keys.begin), keys.arena()),
		                KeySelector(firstGreaterOrEqual(keys.end), keys.arena()),
		                limits,
		                snapshot,
		                reverse);
	}

	[[nodiscard]] Future<MappedRangeResult> getMappedRange(const KeySelector& begin,
	                                                       const KeySelector& end,
	                                                       const Key& mapper,
	                                                       GetRangeLimits limits,
	                                                       Snapshot = Snapshot::False,
	                                                       Reverse = Reverse::False);

private:
	template <class GetKeyValuesFamilyRequest, class GetKeyValuesFamilyReply, class RangeResultFamily>
	Future<RangeResultFamily> getRangeInternal(const KeySelector& begin,
	                                           const KeySelector& end,
	                                           const Key& mapper,
	                                           GetRangeLimits limits,
	                                           Snapshot snapshot,
	                                           Reverse reverse);

public:
	// A method for streaming data from the storage server that is more efficient than getRange when reading large
	// amounts of data
	[[nodiscard]] Future<Void> getRangeStream(PromiseStream<Standalone<RangeResultRef>>& results,
	                                          const KeySelector& begin,
	                                          const KeySelector& end,
	                                          int limit,
	                                          Snapshot = Snapshot::False,
	                                          Reverse = Reverse::False);
	[[nodiscard]] Future<Void> getRangeStream(PromiseStream<Standalone<RangeResultRef>>& results,
	                                          const KeySelector& begin,
	                                          const KeySelector& end,
	                                          GetRangeLimits limits,
	                                          Snapshot = Snapshot::False,
	                                          Reverse = Reverse::False);
	[[nodiscard]] Future<Void> getRangeStream(PromiseStream<Standalone<RangeResultRef>>& results,
	                                          const KeyRange& keys,
	                                          int limit,
	                                          Snapshot snapshot = Snapshot::False,
	                                          Reverse reverse = Reverse::False) {
		return getRangeStream(results,
		                      KeySelector(firstGreaterOrEqual(keys.begin), keys.arena()),
		                      KeySelector(firstGreaterOrEqual(keys.end), keys.arena()),
		                      limit,
		                      snapshot,
		                      reverse);
	}
	[[nodiscard]] Future<Void> getRangeStream(PromiseStream<Standalone<RangeResultRef>>& results,
	                                          const KeyRange& keys,
	                                          GetRangeLimits limits,
	                                          Snapshot snapshot = Snapshot::False,
	                                          Reverse reverse = Reverse::False) {
		return getRangeStream(results,
		                      KeySelector(firstGreaterOrEqual(keys.begin), keys.arena()),
		                      KeySelector(firstGreaterOrEqual(keys.end), keys.arena()),
		                      limits,
		                      snapshot,
		                      reverse);
	}

	[[nodiscard]] Future<Standalone<VectorRef<const char*>>> getAddressesForKey(const Key& key);

	void enableCheckWrites();
	void addReadConflictRange(KeyRangeRef const& keys);
	void addWriteConflictRange(KeyRangeRef const& keys);
	void makeSelfConflicting();

	Future<Void> warmRange(KeyRange keys);

	// Try to split the given range into equally sized chunks based on estimated size.
	// The returned list would still be in form of [keys.begin, splitPoint1, splitPoint2, ... , keys.end]
	Future<Standalone<VectorRef<KeyRef>>> getRangeSplitPoints(KeyRange const& keys, int64_t chunkSize);

	Future<Standalone<VectorRef<KeyRangeRef>>> getBlobGranuleRanges(const KeyRange& range, int rangeLimit);
	Future<Standalone<VectorRef<BlobGranuleChunkRef>>> readBlobGranules(const KeyRange& range,
	                                                                    Version begin,
	                                                                    Optional<Version> readVersion,
	                                                                    Version* readVersionOut = nullptr);

	Future<Standalone<VectorRef<BlobGranuleSummaryRef>>> summarizeBlobGranules(const KeyRange& range,
	                                                                           Optional<Version> summaryVersion,
	                                                                           int rangeLimit);

	void addGranuleMaterializeStats(const GranuleMaterializeStats& stats);

	// If checkWriteConflictRanges is true, existing write conflict ranges will be searched for this key
	void set(const KeyRef& key, const ValueRef& value, AddConflictRange = AddConflictRange::True);
	void atomicOp(const KeyRef& key,
	              const ValueRef& value,
	              MutationRef::Type operationType,
	              AddConflictRange = AddConflictRange::True);
	void clear(const KeyRangeRef& range, AddConflictRange = AddConflictRange::True);
	void clear(const KeyRef& key, AddConflictRange = AddConflictRange::True);

	// Throws not_committed or commit_unknown_result errors in normal operation
	[[nodiscard]] Future<Void> commit();

	void setOption(FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>());

	// May be called only after commit() returns success
	Version getCommittedVersion() const { return trState->committedVersion; }

	int64_t getTotalCost() const { return trState->totalCost; }

	double getTagThrottledDuration() const;

	// Will be fulfilled only after commit() returns success
	[[nodiscard]] Future<Standalone<StringRef>> getVersionstamp();

	Future<uint64_t> getProtocolVersion();

	uint32_t getSize();
	[[nodiscard]] Future<Void> onError(Error const& e);
	void flushTrLogsIfEnabled();

	// These are to permit use as state variables in actors:
	Transaction();
	void operator=(Transaction&& r) noexcept;

	void reset();
	void fullReset();
	double getBackoff(int errCode);

	void debugTransaction(UID dID) {
		if (trState->readOptions.present()) {
			trState->readOptions.get().debugID = dID;
		} else {
			trState->readOptions = ReadOptions(dID);
		}
	}
	VersionVector getVersionVector() const;
	SpanContext getSpanContext() const { return trState->spanContext; }

	Future<Void> commitMutations();
	void setupWatches();
	void cancelWatches(Error const& e = transaction_cancelled());

	int apiVersionAtLeast(int minVersion) const;
	void checkDeferredError() const;

	Database getDatabase() const { return trState->cx; }
	static Reference<TransactionLogInfo> createTrLogInfoProbabilistically(const Database& cx);

	Transaction& getTransaction() { return *this; }
	void setTransactionID(UID id);
	void setToken(uint64_t token);

	const std::vector<Future<std::pair<Key, Key>>>& getExtraReadConflictRanges() const { return extraConflictRanges; }
	Standalone<VectorRef<KeyRangeRef>> readConflictRanges() const {
		return Standalone<VectorRef<KeyRangeRef>>(tr.transaction.read_conflict_ranges, tr.arena);
	}
	Standalone<VectorRef<KeyRangeRef>> writeConflictRanges() const {
		return Standalone<VectorRef<KeyRangeRef>>(tr.transaction.write_conflict_ranges, tr.arena);
	}

	Optional<Reference<Tenant>> getTenant() { return trState->tenant(); }

	Reference<TransactionState> trState;
	std::vector<Reference<Watch>> watches;
	TagSet const& getTags() const;
	Span span;

	// used in template functions as returned Future type
	template <typename Type>
	using FutureT = Future<Type>;

private:
	template <class GetKeyValuesFamilyRequest, class GetKeyValuesFamilyReply>
	Future<RangeResult> getRangeInternal(const KeySelector& begin,
	                                     const KeySelector& end,
	                                     const Key& mapper,
	                                     GetRangeLimits limits,
	                                     Snapshot snapshot,
	                                     Reverse reverse);

	void resetImpl(bool generateNewSpan);

	double backoff;
	CommitTransactionRequest tr;
	std::vector<Future<std::pair<Key, Key>>> extraConflictRanges;
	Promise<Void> commitResult;
	Future<Void> committing;
};

template <std::invocable<Transaction*> Fun>
Future<Void> Database::run(Fun fun) {
	Transaction tr(*this);
	Future<Void> onError;
	while (true) {
		if (onError.isValid()) {
			co_await onError;
			onError = Future<Void>();
		}
		try {
			co_await fun(&tr);
			co_return;
		} catch (Error& e) {
			onError = tr.onError(e);
		}
	}
}

ACTOR Future<Version> waitForCommittedVersion(Database cx, Version version, SpanContext spanContext);
ACTOR Future<Standalone<VectorRef<DDMetricsRef>>> waitDataDistributionMetricsList(Database cx,
                                                                                  KeyRange keys,
                                                                                  int shardLimit);

int64_t extractIntOption(Optional<StringRef> value,
                         int64_t minValue = std::numeric_limits<int64_t>::min(),
                         int64_t maxValue = std::numeric_limits<int64_t>::max());

// Takes a snapshot of the cluster, specifically the following persistent
// states: coordinator, TLog and storage state
ACTOR Future<Void> snapCreate(Database cx, Standalone<StringRef> snapCmd, UID snapUID);

// Adds necessary mutation(s) to the transaction, so that *one* checkpoint will be created for
// each and every shards overlapping with `ranges`.
// All checkpoint(s) will be created at the transaction's commit version.
Future<Void> createCheckpoint(Transaction* tr,
                              const std::vector<KeyRange>& ranges,
                              CheckpointFormat format,
                              Optional<UID> dataMoveId = Optional<UID>());

// Same as above.
Future<Void> createCheckpoint(Reference<ReadYourWritesTransaction> tr,
                              const std::vector<KeyRange>& ranges,
                              CheckpointFormat format,
                              Optional<UID> dataMoveId = Optional<UID>());

// Gets checkpoint metadata for `ranges` at the specific version, with the particular format.
// Returns a list of [range, checkpoint], where the `checkpoint` has data over `range`.
// checkpoint_not_found() error will be returned if the specific checkpoint cannot be found.
ACTOR Future<std::vector<std::pair<KeyRange, CheckpointMetaData>>> getCheckpointMetaData(
    Database cx,
    std::vector<KeyRange> ranges,
    Version version,
    CheckpointFormat format,
    Optional<UID> dataMoveId = Optional<UID>(),
    double timeout = 5.0);

// Checks with Data Distributor that it is safe to mark all servers in exclusions as failed
ACTOR Future<bool> checkSafeExclusions(Database cx, std::vector<AddressExclusion> exclusions);

// Measured in bytes, rounded up to the nearest page size. Multiply by fungibility ratio
// because writes are more expensive than reads.
inline uint64_t getWriteOperationCost(uint64_t bytes) {
	if (bytes == 0) {
		return CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	} else {
		return CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE *
		       ((bytes - 1) / CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE + 1);
	}
}

// Measured in bytes, rounded up to the nearest page size.
inline uint64_t getReadOperationCost(uint64_t bytes) {
	if (bytes == 0) {
		return CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	} else {
		return ((bytes - 1) / CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE + 1) * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	}
}

// Create a transaction to set the value of system key \xff/conf/perpetual_storage_wiggle. If enable == true, the value
// will be 1. Otherwise, the value will be 0. The caller should take care of the reset of StorageWiggleMetrics if
// necessary. Returns the FDB version at which the transaction was committed.
ACTOR Future<Version> setPerpetualStorageWiggle(Database cx, bool enable, LockAware lockAware = LockAware::False);

ACTOR Future<std::vector<std::pair<UID, StorageWiggleValue>>> readStorageWiggleValues(Database cx,
                                                                                      bool primary,
                                                                                      bool use_system_priority);

// Returns the maximum legal size of a key. This size will be determined by the prefix of the passed in key
// (system keys have a larger maximum size). This should be used for generic max key size requests.
int64_t getMaxKeySize(KeyRef const& key);

// Returns the maximum legal size of a key that can be read. Keys larger than this will be assumed not to exist.
int64_t getMaxReadKeySize(KeyRef const& key);

// Returns the maximum legal size of a key that can be written. If using raw access, writes to normal keys will
// be allowed to be slightly larger to accommodate the prefix.
int64_t getMaxWriteKeySize(KeyRef const& key, bool hasRawAccess);

// Returns the maximum legal size of a key that can be cleared. Keys larger than this will be assumed not to exist.
int64_t getMaxClearKeySize(KeyRef const& key);

struct KeyRangeLocationInfo;
// Return the aggregated StorageMetrics of range keys to the caller. The locations tell which interface should
// serve the request. The final result is within (min-permittedError/2, max + permittedError/2) if valid.
ACTOR Future<Optional<StorageMetrics>> waitStorageMetricsWithLocation(TenantInfo tenantInfo,
                                                                      Version version,
                                                                      KeyRange keys,
                                                                      std::vector<KeyRangeLocationInfo> locations,
                                                                      StorageMetrics min,
                                                                      StorageMetrics max,
                                                                      StorageMetrics permittedError);

// Return the suggested split points from storage server.The locations tell which interface should
// serve the request. `limit` is the current estimated storage metrics of `keys`.The returned points, if present,
// guarantee the metrics of split result is within limit.
ACTOR Future<Optional<Standalone<VectorRef<KeyRef>>>> splitStorageMetricsWithLocations(
    std::vector<KeyRangeLocationInfo> locations,
    KeyRange keys,
    StorageMetrics limit,
    StorageMetrics estimated,
    Optional<int> minSplitBytes);

namespace NativeAPI {
ACTOR Future<std::vector<std::pair<StorageServerInterface, ProcessClass>>> getServerListAndProcessClasses(
    Transaction* tr);
}
ACTOR Future<KeyRangeLocationInfo> getKeyLocation_internal(Database cx,
                                                           TenantInfo tenant,
                                                           Key key,
                                                           SpanContext spanContext,
                                                           Optional<UID> debugID,
                                                           UseProvisionalProxies useProvisionalProxies,
                                                           Reverse isBackward,
                                                           Version version);

ACTOR Future<Void> refreshTransaction(DatabaseContext* self, Transaction* tr);

#include "flow/unactorcompiler.h"
#endif
