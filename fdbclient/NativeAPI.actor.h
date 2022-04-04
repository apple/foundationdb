/*
 * NativeAPI.actor.h
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

#pragma once
#include "flow/IRandom.h"
#include "flow/Tracing.h"
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_NATIVEAPI_ACTOR_G_H)
#define FDBCLIENT_NATIVEAPI_ACTOR_G_H
#include "fdbclient/NativeAPI.actor.g.h"
#elif !defined(FDBCLIENT_NATIVEAPI_ACTOR_H)
#define FDBCLIENT_NATIVEAPI_ACTOR_H

#include "flow/BooleanParam.h"
#include "flow/flow.h"
#include "flow/TDMetric.actor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/ClientLogEvents.h"
#include "fdbclient/KeyRangeMap.h"
#include "flow/actorcompiler.h" // has to be last include

// CLIENT_BUGGIFY should be used to randomly introduce failures at run time (like BUGGIFY but for client side testing)
// Unlike BUGGIFY, CLIENT_BUGGIFY can be enabled and disabled at runtime.
#define CLIENT_BUGGIFY_WITH_PROB(x)                                                                                    \
	(getSBVar(__FILE__, __LINE__, BuggifyType::Client) && deterministicRandom()->random01() < (x))
#define CLIENT_BUGGIFY CLIENT_BUGGIFY_WITH_PROB(P_BUGGIFIED_SECTION_FIRES[int(BuggifyType::Client)])

FDB_DECLARE_BOOLEAN_PARAM(UseProvisionalProxies);

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
	Optional<bool> logClientInfo;
	Reference<ReferencedObject<Standalone<VectorRef<ClientVersionRef>>>> supportedVersions;
	bool runLoopProfilingEnabled;
	bool primaryClient;
	std::map<std::string, KnobValue> knobs;

	NetworkOptions();
};

class Database {
public:
	enum { API_VERSION_LATEST = -1 };

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

	Watch() : valuePresent(false), setPresent(false), watchFuture(Never()) {}
	Watch(Key key) : key(key), valuePresent(false), setPresent(false), watchFuture(Never()) {}
	Watch(Key key, Optional<Value> val)
	  : key(key), value(val), valuePresent(true), setPresent(false), watchFuture(Never()) {}

	void setWatch(Future<Void> watchFuture);
};

struct TransactionState : ReferenceCounted<TransactionState> {
	Database cx;
	int64_t tenantId = TenantInfo::INVALID_TENANT;
	Reference<TransactionLogInfo> trLogInfo;
	TransactionOptions options;

	Optional<UID> debugID;
	TaskPriority taskID;
	SpanID spanID;
	UseProvisionalProxies useProvisionalProxies = UseProvisionalProxies::False;
	bool readVersionObtainedFromGrvProxy;

	int numErrors = 0;
	double startTime = 0;
	Promise<Standalone<StringRef>> versionstampPromise;

	Version committedVersion{ invalidVersion };

	// Used to save conflicting keys if FDBTransactionOptions::REPORT_CONFLICTING_KEYS is enabled
	// prefix/<key1> : '1' - any keys equal or larger than this key are (probably) conflicting keys
	// prefix/<key2> : '0' - any keys equal or larger than this key are (definitely) not conflicting keys
	std::shared_ptr<CoalescedKeyRangeMap<Value>> conflictingKeys;

	// Only available so that Transaction can have a default constructor, for use in state variables
	TransactionState(TaskPriority taskID, SpanID spanID) : taskID(taskID), spanID(spanID), tenantSet(false) {}

	// VERSION_VECTOR changed default values of readVersionObtainedFromGrvProxy
	TransactionState(Database cx,
	                 Optional<TenantName> tenant,
	                 TaskPriority taskID,
	                 SpanID spanID,
	                 Reference<TransactionLogInfo> trLogInfo);

	Reference<TransactionState> cloneAndReset(Reference<TransactionLogInfo> newTrLogInfo, bool generateNewSpan) const;
	TenantInfo getTenantInfo();

	Optional<TenantName> const& tenant();
	bool hasTenant() const;

private:
	Optional<TenantName> tenant_;
	bool tenantSet;
};

class Transaction : NonCopyable {
public:
	explicit Transaction(Database const& cx, Optional<TenantName> const& tenant = Optional<TenantName>());
	~Transaction();

	void setVersion(Version v);
	Future<Version> getReadVersion() { return getReadVersion(0); }
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
	[[nodiscard]] Future<Void> getRangeStream(const PromiseStream<Standalone<RangeResultRef>>& results,
	                                          const KeySelector& begin,
	                                          const KeySelector& end,
	                                          int limit,
	                                          Snapshot = Snapshot::False,
	                                          Reverse = Reverse::False);
	[[nodiscard]] Future<Void> getRangeStream(const PromiseStream<Standalone<RangeResultRef>>& results,
	                                          const KeySelector& begin,
	                                          const KeySelector& end,
	                                          GetRangeLimits limits,
	                                          Snapshot = Snapshot::False,
	                                          Reverse = Reverse::False);
	[[nodiscard]] Future<Void> getRangeStream(const PromiseStream<Standalone<RangeResultRef>>& results,
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
	[[nodiscard]] Future<Void> getRangeStream(const PromiseStream<Standalone<RangeResultRef>>& results,
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

	Future<Standalone<VectorRef<KeyRangeRef>>> getBlobGranuleRanges(const KeyRange& range);
	Future<Standalone<VectorRef<BlobGranuleChunkRef>>> readBlobGranules(const KeyRange& range,
	                                                                    Version begin,
	                                                                    Optional<Version> readVersion,
	                                                                    Version* readVersionOut = nullptr);

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

	void debugTransaction(UID dID) { trState->debugID = dID; }
	VersionVector getVersionVector() const;
	UID getSpanID() const { return trState->spanID; }

	Future<Void> commitMutations();
	void setupWatches();
	void cancelWatches(Error const& e = transaction_cancelled());

	int apiVersionAtLeast(int minVersion) const;
	void checkDeferredError() const;

	Database getDatabase() const { return trState->cx; }
	static Reference<TransactionLogInfo> createTrLogInfoProbabilistically(const Database& cx);

	void setTransactionID(uint64_t id);
	void setToken(uint64_t token);

	const std::vector<Future<std::pair<Key, Key>>>& getExtraReadConflictRanges() const { return extraConflictRanges; }
	Standalone<VectorRef<KeyRangeRef>> readConflictRanges() const {
		return Standalone<VectorRef<KeyRangeRef>>(tr.transaction.read_conflict_ranges, tr.arena);
	}
	Standalone<VectorRef<KeyRangeRef>> writeConflictRanges() const {
		return Standalone<VectorRef<KeyRangeRef>>(tr.transaction.write_conflict_ranges, tr.arena);
	}

	Optional<TenantName> getTenant() { return trState->tenant(); }

	Reference<TransactionState> trState;
	std::vector<Reference<Watch>> watches;
	Span span;

private:
	Future<Version> getReadVersion(uint32_t flags);

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
	Future<Version> readVersion;
	Promise<Optional<Value>> metadataVersion;
	std::vector<Future<std::pair<Key, Key>>> extraConflictRanges;
	Promise<Void> commitResult;
	Future<Void> committing;
};

ACTOR Future<Version> waitForCommittedVersion(Database cx, Version version, SpanID spanContext);
ACTOR Future<Standalone<VectorRef<DDMetricsRef>>> waitDataDistributionMetricsList(Database cx,
                                                                                  KeyRange keys,
                                                                                  int shardLimit);

std::string unprintable(const std::string&);

int64_t extractIntOption(Optional<StringRef> value,
                         int64_t minValue = std::numeric_limits<int64_t>::min(),
                         int64_t maxValue = std::numeric_limits<int64_t>::max());

// Takes a snapshot of the cluster, specifically the following persistent
// states: coordinator, TLog and storage state
ACTOR Future<Void> snapCreate(Database cx, Standalone<StringRef> snapCmd, UID snapUID);

// Adds necessary mutation(s) to the transaction, so that *one* checkpoint will be created for
// each and every shards overlapping with `range`. Each checkpoint will be created at a random
// storage server for each shard.
// All checkpoint(s) will be created at the transaction's commit version.
Future<Void> createCheckpoint(Transaction* tr, KeyRangeRef range, CheckpointFormat format);

// Same as above.
Future<Void> createCheckpoint(Reference<ReadYourWritesTransaction> tr, KeyRangeRef range, CheckpointFormat format);

// Gets checkpoint metadata for `keys` at the specific version, with the particular format.
// One CheckpointMetaData will be returned for each distinctive shard.
// The collective keyrange of the returned checkpoint(s) is a super-set of `keys`.
// checkpoint_not_found() error will be returned if the specific checkpoint(s) cannot be found.
ACTOR Future<std::vector<CheckpointMetaData>> getCheckpointMetaData(Database cx,
                                                                    KeyRange keys,
                                                                    Version version,
                                                                    CheckpointFormat format,
                                                                    double timeout = 5.0);

// Checks with Data Distributor that it is safe to mark all servers in exclusions as failed
ACTOR Future<bool> checkSafeExclusions(Database cx, std::vector<AddressExclusion> exclusions);

inline uint64_t getWriteOperationCost(uint64_t bytes) {
	return bytes / std::max(1, CLIENT_KNOBS->WRITE_COST_BYTE_FACTOR) + 1;
}

// Create a transaction to set the value of system key \xff/conf/perpetual_storage_wiggle. If enable == true, the value
// will be 1. Otherwise, the value will be 0.
ACTOR Future<Void> setPerpetualStorageWiggle(Database cx, bool enable, LockAware lockAware = LockAware::False);

ACTOR Future<std::vector<std::pair<UID, StorageWiggleValue>>> readStorageWiggleValues(Database cx,
                                                                                      bool primary,
                                                                                      bool use_system_priority);

#include "flow/unactorcompiler.h"
#endif
