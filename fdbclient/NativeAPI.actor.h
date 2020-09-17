/*
 * NativeAPI.actor.h
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

#pragma once
#include "flow/IRandom.h"
#include "flow/Tracing.h"
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_NATIVEAPI_ACTOR_G_H)
	#define FDBCLIENT_NATIVEAPI_ACTOR_G_H
	#include "fdbclient/NativeAPI.actor.g.h"
#elif !defined(FDBCLIENT_NATIVEAPI_ACTOR_H)
	#define FDBCLIENT_NATIVEAPI_ACTOR_H

#include "flow/flow.h"
#include "flow/TDMetric.actor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/ClientLogEvents.h"
#include "fdbclient/KeyRangeMap.h"
#include "flow/actorcompiler.h" // has to be last include

// CLIENT_BUGGIFY should be used to randomly introduce failures at run time (like BUGGIFY but for client side testing)
// Unlike BUGGIFY, CLIENT_BUGGIFY can be enabled and disabled at runtime.
#define CLIENT_BUGGIFY_WITH_PROB(x) (getSBVar(__FILE__, __LINE__, BuggifyType::Client) && deterministicRandom()->random01() < (x))
#define CLIENT_BUGGIFY CLIENT_BUGGIFY_WITH_PROB(P_BUGGIFIED_SECTION_FIRES[int(BuggifyType::Client)])

// Incomplete types that are reference counted
class DatabaseContext;
template <> void addref( DatabaseContext* ptr );
template <> void delref( DatabaseContext* ptr );

void validateOptionValue(Optional<StringRef> value, bool shouldBePresent);

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
	Optional<bool> logClientInfo;
	Reference<ReferencedObject<Standalone<VectorRef<ClientVersionRef>>>> supportedVersions;
	bool runLoopProfilingEnabled;

	NetworkOptions();
};

class Database {
public:
	enum { API_VERSION_LATEST = -1 };

	static Database createDatabase( Reference<ClusterConnectionFile> connFile, int apiVersion, bool internal=true, LocalityData const& clientLocality=LocalityData(), DatabaseContext *preallocatedDb=nullptr );
	static Database createDatabase( std::string connFileName, int apiVersion, bool internal=true, LocalityData const& clientLocality=LocalityData() ); 

	Database() {}  // an uninitialized database can be destructed or reassigned safely; that's it
	void operator= ( Database const& rhs ) { db = rhs.db; }
	Database( Database const& rhs ) : db(rhs.db) {}
	Database(Database&& r) noexcept : db(std::move(r.db)) {}
	void operator=(Database&& r) noexcept { db = std::move(r.db); }

	// For internal use by the native client:
	explicit Database(Reference<DatabaseContext> cx) : db(cx) {}
	explicit Database( DatabaseContext* cx ) : db(cx) {}
	inline DatabaseContext* getPtr() const { return db.getPtr(); }
	inline DatabaseContext* extractPtr() { return db.extractPtr(); }
	DatabaseContext* operator->() const { return db.getPtr(); }

	const UniqueOrderedOptionList<FDBTransactionOptions>& getTransactionDefaults() const;

private:
	Reference<DatabaseContext> db;
};

void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>() );

// Configures the global networking machinery
void setupNetwork(uint64_t transportId = 0, bool useMetrics = false);

// This call blocks while the network is running.  To use the API in a single-threaded
//  environment, the calling program must have ACTORs already launched that are waiting
//  to use the network.  In this case, the program can terminate by calling stopNetwork()
//  from a callback, thereby releasing this call to return.  In a multithreaded setup
//  this call can be called from a dedicated "networking" thread.  All the network-based
//  callbacks will happen on this second thread.  When a program is finished, the
//  call stopNetwork (from a non-networking thread) can cause the runNetwork() call to
//  return.
//
// Throws network_already_setup if g_network has already been initalized
void runNetwork();

// See above.  Can be called from a thread that is not the "networking thread"
//
// Throws network_not_setup if g_network has not been initalized
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
struct TransactionInfo {
	Optional<UID> debugID;
	TaskPriority taskID;
	SpanID spanID;
	bool useProvisionalProxies;
	// Used to save conflicting keys if FDBTransactionOptions::REPORT_CONFLICTING_KEYS is enabled
	// prefix/<key1> : '1' - any keys equal or larger than this key are (probably) conflicting keys
	// prefix/<key2> : '0' - any keys equal or larger than this key are (definitely) not conflicting keys
	std::shared_ptr<CoalescedKeyRangeMap<Value>> conflictingKeys;

	explicit TransactionInfo(TaskPriority taskID, SpanID spanID)
	  : taskID(taskID), spanID(spanID), useProvisionalProxies(false) {}
};

struct TransactionLogInfo : public ReferenceCounted<TransactionLogInfo>, NonCopyable {
	enum LoggingLocation { DONT_LOG = 0, TRACE_LOG = 1, DATABASE = 2 };

	TransactionLogInfo() : logLocation(DONT_LOG), maxFieldLength(0) {}
	TransactionLogInfo(LoggingLocation location) : logLocation(location), maxFieldLength(0) {}
	TransactionLogInfo(std::string id, LoggingLocation location) : logLocation(location), identifier(id), maxFieldLength(0) {}

	void setIdentifier(std::string id) { identifier = id; }
	void logTo(LoggingLocation loc) { logLocation = logLocation | loc; }

	template <typename T>
	void addLog(const T& event) {
		if(logLocation & TRACE_LOG) {
			ASSERT(!identifier.empty());
			event.logEvent(identifier, maxFieldLength);
		}

		if (flushed) {
			return;
		}

		if(logLocation & DATABASE) {
			logsAdded = true;
			static_assert(std::is_base_of<FdbClientLogEvents::Event, T>::value, "Event should be derived class of FdbClientLogEvents::Event");
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

	Watch() : watchFuture(Never()), valuePresent(false), setPresent(false) { }
	Watch(Key key) : key(key), watchFuture(Never()), valuePresent(false), setPresent(false) { }
	Watch(Key key, Optional<Value> val) : key(key), value(val), watchFuture(Never()), valuePresent(true), setPresent(false) { }

	void setWatch(Future<Void> watchFuture);
};

class Transaction : NonCopyable {
public:
	explicit Transaction( Database const& cx );
	~Transaction();

	void preinitializeOnForeignThread() {
		committedVersion = invalidVersion;
	}

	void setVersion( Version v );
	Future<Version> getReadVersion() { return getReadVersion(0); }
	Future<Version> getRawReadVersion();
	Optional<Version> getCachedReadVersion();

	[[nodiscard]] Future<Optional<Value>> get(const Key& key, bool snapshot = false);
	[[nodiscard]] Future<Void> watch(Reference<Watch> watch);
	[[nodiscard]] Future<Key> getKey(const KeySelector& key, bool snapshot = false);
	//Future< Optional<KeyValue> > get( const KeySelectorRef& key );
	[[nodiscard]] Future<Standalone<RangeResultRef>> getRange(const KeySelector& begin, const KeySelector& end,
	                                                          int limit, bool snapshot = false, bool reverse = false);
	[[nodiscard]] Future<Standalone<RangeResultRef>> getRange(const KeySelector& begin, const KeySelector& end,
	                                                          GetRangeLimits limits, bool snapshot = false,
	                                                          bool reverse = false);
	[[nodiscard]] Future<Standalone<RangeResultRef>> getRange(const KeyRange& keys, int limit, bool snapshot = false,
	                                                          bool reverse = false) {
		return getRange(KeySelector(firstGreaterOrEqual(keys.begin), keys.arena()),
		                KeySelector(firstGreaterOrEqual(keys.end), keys.arena()), limit, snapshot, reverse);
	}
	[[nodiscard]] Future<Standalone<RangeResultRef>> getRange(const KeyRange& keys, GetRangeLimits limits,
	                                                          bool snapshot = false, bool reverse = false) {
		return getRange(KeySelector(firstGreaterOrEqual(keys.begin), keys.arena()),
		                KeySelector(firstGreaterOrEqual(keys.end), keys.arena()), limits, snapshot, reverse);
	}

	[[nodiscard]] Future<Standalone<VectorRef<const char*>>> getAddressesForKey(const Key& key);

	void enableCheckWrites();
	void addReadConflictRange( KeyRangeRef const& keys );
	void addWriteConflictRange( KeyRangeRef const& keys );
	void makeSelfConflicting();

	Future< Void > warmRange( Database cx, KeyRange keys );

	Future< std::pair<Optional<StorageMetrics>, int> > waitStorageMetrics( KeyRange const& keys, StorageMetrics const& min, StorageMetrics const& max, StorageMetrics const& permittedError, int shardLimit, int expectedShardCount );
	// Pass a negative value for `shardLimit` to indicate no limit on the shard number.
	Future< StorageMetrics > getStorageMetrics( KeyRange const& keys, int shardLimit );
	Future< Standalone<VectorRef<KeyRef>> > splitStorageMetrics( KeyRange const& keys, StorageMetrics const& limit, StorageMetrics const& estimated );
	Future<Standalone<VectorRef<ReadHotRangeWithMetrics>>> getReadHotRanges(KeyRange const& keys);

	// If checkWriteConflictRanges is true, existing write conflict ranges will be searched for this key
	void set( const KeyRef& key, const ValueRef& value, bool addConflictRange = true );
	void atomicOp( const KeyRef& key, const ValueRef& value, MutationRef::Type operationType, bool addConflictRange = true );
	void clear( const KeyRangeRef& range, bool addConflictRange = true );
	void clear( const KeyRef& key, bool addConflictRange = true );
	[[nodiscard]] Future<Void> commit(); // Throws not_committed or commit_unknown_result errors in normal operation

	void setOption( FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>() );

	Version getCommittedVersion() { return committedVersion; }   // May be called only after commit() returns success
	[[nodiscard]] Future<Standalone<StringRef>>
	getVersionstamp(); // Will be fulfilled only after commit() returns success

	Promise<Standalone<StringRef>> versionstampPromise;

	uint32_t getSize();
	[[nodiscard]] Future<Void> onError(Error const& e);
	void flushTrLogsIfEnabled();

	// These are to permit use as state variables in actors:
	Transaction()
	  : info(TaskPriority::DefaultEndpoint, deterministicRandom()->randomUniqueID()),
	    span(info.spanID, "Transaction"_loc) {}
	void operator=(Transaction&& r) noexcept;

	void reset();
	void fullReset();
	double getBackoff(int errCode);
	void debugTransaction(UID dID) { info.debugID = dID; }

	Future<Void> commitMutations();
	void setupWatches();
	void cancelWatches(Error const& e = transaction_cancelled());

	TransactionInfo info;
	int numErrors;

	std::vector<Reference<Watch>> watches;

	int apiVersionAtLeast(int minVersion) const;

	void checkDeferredError();

	Database getDatabase() const {
		return cx;
	}
	static Reference<TransactionLogInfo> createTrLogInfoProbabilistically(const Database& cx);
	TransactionOptions options;
	Span span;
	double startTime;
	Reference<TransactionLogInfo> trLogInfo;

	const vector<Future<std::pair<Key, Key>>>& getExtraReadConflictRanges() const { return extraConflictRanges; }
	Standalone<VectorRef<KeyRangeRef>> readConflictRanges() const {
		return Standalone<VectorRef<KeyRangeRef>>(tr.transaction.read_conflict_ranges, tr.arena);
	}
	Standalone<VectorRef<KeyRangeRef>> writeConflictRanges() const {
		return Standalone<VectorRef<KeyRangeRef>>(tr.transaction.write_conflict_ranges, tr.arena);
	}

private:
	Future<Version> getReadVersion(uint32_t flags);
	Database cx;

	double backoff;
	Version committedVersion;
	CommitTransactionRequest tr;
	Future<Version> readVersion;
	Promise<Optional<Value>> metadataVersion;
	vector<Future<std::pair<Key, Key>>> extraConflictRanges;
	Promise<Void> commitResult;
	Future<Void> committing;
};

ACTOR Future<Version> waitForCommittedVersion(Database cx, Version version, SpanID spanContext);
ACTOR Future<Standalone<VectorRef<DDMetricsRef>>> waitDataDistributionMetricsList(Database cx, KeyRange keys,
                                                                               int shardLimit);

std::string unprintable( const std::string& );

int64_t extractIntOption( Optional<StringRef> value, int64_t minValue = std::numeric_limits<int64_t>::min(), int64_t maxValue = std::numeric_limits<int64_t>::max() );

// Takes a snapshot of the cluster, specifically the following persistent
// states: coordinator, TLog and storage state
ACTOR Future<Void> snapCreate(Database cx, Standalone<StringRef> snapCmd, UID snapUID);

// Checks with Data Distributor that it is safe to mark all servers in exclusions as failed
ACTOR Future<bool> checkSafeExclusions(Database cx, vector<AddressExclusion> exclusions);

inline uint64_t getWriteOperationCost(uint64_t bytes) {
	return bytes / std::max(1, CLIENT_KNOBS->WRITE_COST_BYTE_FACTOR) + 1;
}
#include "flow/unactorcompiler.h"
#endif
