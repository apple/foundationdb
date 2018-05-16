/*
 * NativeAPI.h
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

#ifndef FDBCLIENT_NATIVEAPI_H
#define FDBCLIENT_NATIVEAPI_H
#pragma once

#include "flow/flow.h"
#include "flow/TDMetric.actor.h"
#include "FDBTypes.h"
#include "MasterProxyInterface.h"
#include "FDBOptions.g.h"
#include "CoordinationInterface.h"
#include "ClusterInterface.h"
#include "ClientLogEvents.h"

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
	Optional<bool> logClientInfo;
	Standalone<VectorRef<ClientVersionRef>> supportedVersions;
	bool slowTaskProfilingEnabled;

	// The default values, TRACE_DEFAULT_ROLL_SIZE and TRACE_DEFAULT_MAX_LOGS_SIZE are located in Trace.h.
	NetworkOptions() : localAddress(""), clusterFile(""), traceDirectory(Optional<std::string>()), traceRollSize(TRACE_DEFAULT_ROLL_SIZE), traceMaxLogsSize(TRACE_DEFAULT_MAX_LOGS_SIZE), traceLogGroup("default"),
	                   slowTaskProfilingEnabled(false)
	{ }
};


class Database {
public:
	Database() {}  // an uninitialized database can be destructed or reassigned safely; that's it
	void operator= ( Database const& rhs ) { db = rhs.db; }
	Database( Database const& rhs ) : db(rhs.db) {}
	Database(Database&& r) noexcept(true) : db(std::move(r.db)) {}
	void operator= (Database&& r) noexcept(true) { db = std::move(r.db); }

	// For internal use by the native client:
	explicit Database(Reference<DatabaseContext> cx) : db(cx) {}
	explicit Database( DatabaseContext* cx ) : db(cx) {}
	inline DatabaseContext* getPtr() const { return db.getPtr(); }
	DatabaseContext* operator->() const { return db.getPtr(); }

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

/*
 * Starts and holds the monitorLeader and failureMonitorClient actors
 */
class Cluster : public ReferenceCounted<Cluster>, NonCopyable {
public:
	enum { API_VERSION_LATEST = -1 };
	// Constructs a  Cluster.  This uses the global networking enging configured in setupNetwork()
	// apiVersion may be set to API_VERSION_LATEST 
	static Reference<Cluster> createCluster( Reference<ClusterConnectionFile> connFile, int apiVersion );
	static Reference<Cluster> createCluster(std::string connFileName, int apiVersion);

	// See DatabaseContext::createDatabase
	Future<Database> createDatabase( Standalone<StringRef> dbName, LocalityData locality = LocalityData() );

	void setOption(FDBClusterOptions::Option option, Optional<StringRef> value);

	Reference<ClusterConnectionFile> getConnectionFile() { return connectionFile; }

	~Cluster();

	int apiVersionAtLeast(int minVersion) { return apiVersion < 0 || apiVersion >= minVersion; }

	Future<Void> onConnected(); // Returns after a majority of coordination servers are available and have reported a leader. The cluster file therefore is valid, but the database might be unavailable.

	Error deferred_error;

	void checkDeferredError() { if (deferred_error.code() != invalid_error_code) throw deferred_error; }

private: friend class ThreadSafeCluster;
		 friend class AtomicOpsApiCorrectnessWorkload; // This is just for testing purposes. It needs to change apiVersion
		 friend class AtomicOpsWorkload; // This is just for testing purposes. It needs to change apiVersion
		 friend class VersionStampWorkload; // This is just for testing purposes. It needs to change apiVersion

	Cluster( Reference<ClusterConnectionFile> connFile, int apiVersion = API_VERSION_LATEST );

	Reference<AsyncVar<Optional<struct ClusterInterface>>> clusterInterface;
	Future<Void> leaderMon, failMon, connected;
	Reference<ClusterConnectionFile> connectionFile;
	int apiVersion;
};

struct StorageMetrics;

struct TransactionOptions {
	double maxBackoff;
	uint32_t getReadVersionFlags;
	uint32_t customTransactionSizeLimit;
	bool checkWritesEnabled : 1;
	bool causalWriteRisky : 1;
	bool commitOnFirstProxy : 1;
	bool debugDump : 1;
	bool lockAware : 1;
	bool readOnly : 1;

	TransactionOptions() {
		reset();
		if (BUGGIFY) {
			commitOnFirstProxy = true;
		}
	}

	void reset() {
		memset(this, 0, sizeof(*this));
		maxBackoff = CLIENT_KNOBS->DEFAULT_MAX_BACKOFF;
	}
};

struct TransactionInfo {
	Optional<UID> debugID;
	int taskID;

	explicit TransactionInfo( int taskID ) : taskID( taskID ) {}
};

struct TransactionLogInfo : public ReferenceCounted<TransactionLogInfo>, NonCopyable {
	TransactionLogInfo() : logToDatabase(true) {}
	TransactionLogInfo(std::string identifier) : logToDatabase(false), identifier(identifier) {}

	BinaryWriter trLogWriter{ IncludeVersion() };
	bool logsAdded{ false };
	bool flushed{ false };

	template <typename T>
	void addLog(const T& event) {
		ASSERT(logToDatabase || identifier.present());

		if(identifier.present()) {
			event.logEvent(identifier.get());
		}

		if (flushed) {
			return;
		}

		if(logToDatabase) {
			logsAdded = true;
			static_assert(std::is_base_of<FdbClientLogEvents::Event, T>::value, "Event should be derived class of FdbClientLogEvents::Event");
			trLogWriter << event;
		}
	}

	bool logToDatabase;
	Optional<std::string> identifier;
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

	Future< Optional<Value> > get( const Key& key, bool snapshot = false );
	Future< Void > watch( Reference<Watch> watch );
	Future< Key > getKey( const KeySelector& key, bool snapshot = false );
	//Future< Optional<KeyValue> > get( const KeySelectorRef& key );
	Future< Standalone<RangeResultRef> > getRange( const KeySelector& begin, const KeySelector& end, int limit, bool snapshot = false, bool reverse = false );
	Future< Standalone<RangeResultRef> > getRange( const KeySelector& begin, const KeySelector& end, GetRangeLimits limits, bool snapshot = false, bool reverse = false );
	Future< Standalone<RangeResultRef> > getRange( const KeyRange& keys, int limit, bool snapshot = false, bool reverse = false ) { 
		return getRange( KeySelector( firstGreaterOrEqual(keys.begin), keys.arena() ), 
			KeySelector( firstGreaterOrEqual(keys.end), keys.arena() ), limit, snapshot, reverse ); 
	}
	Future< Standalone<RangeResultRef> > getRange( const KeyRange& keys, GetRangeLimits limits, bool snapshot = false, bool reverse = false ) { 
		return getRange( KeySelector( firstGreaterOrEqual(keys.begin), keys.arena() ),
			KeySelector( firstGreaterOrEqual(keys.end), keys.arena() ), limits, snapshot, reverse ); 
	}

	Future< Standalone<VectorRef< const char*>>> getAddressesForKey (const Key& key );

	void enableCheckWrites();
	void addReadConflictRange( KeyRangeRef const& keys );
	void addWriteConflictRange( KeyRangeRef const& keys );
	void makeSelfConflicting();

	Future< Void > warmRange( Database cx, KeyRange keys );

	Future< StorageMetrics > waitStorageMetrics( KeyRange const& keys, StorageMetrics const& min, StorageMetrics const& max, StorageMetrics const& permittedError, int shardLimit );
	Future< StorageMetrics > getStorageMetrics( KeyRange const& keys, int shardLimit );
	Future< Standalone<VectorRef<KeyRef>> > splitStorageMetrics( KeyRange const& keys, StorageMetrics const& limit, StorageMetrics const& estimated );

	// If checkWriteConflictRanges is true, existing write conflict ranges will be searched for this key
	void set( const KeyRef& key, const ValueRef& value, bool addConflictRange = true );
	void atomicOp( const KeyRef& key, const ValueRef& value, MutationRef::Type operationType, bool addConflictRange = true );
	void clear( const KeyRangeRef& range, bool addConflictRange = true );
	void clear( const KeyRef& key, bool addConflictRange = true );
	Future<Void> commit(); // Throws not_committed or commit_unknown_result errors in normal operation

	void setOption( FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>() );

	Version getCommittedVersion() { return committedVersion; }   // May be called only after commit() returns success
	Future<Standalone<StringRef>> getVersionstamp(); // Will be fulfilled only after commit() returns success

	Promise<Standalone<StringRef>> versionstampPromise;

	Future<Void> onError( Error const& e );
	void flushTrLogsIfEnabled();

	// These are to permit use as state variables in actors:
	Transaction() : info( TaskDefaultEndpoint ) {}
	void operator=(Transaction&& r) noexcept(true);

	void reset();
	void fullReset();
	double getBackoff();
	void debugTransaction(UID dID) { info.debugID = dID; }

	Future<Void> commitMutations();
	void setupWatches();
	void cancelWatches(Error const& e = transaction_cancelled());

	TransactionInfo info;
	int numErrors;

	std::vector<Reference<Watch>> watches;

	int apiVersionAtLeast(int minVersion) const;

	void checkDeferredError();

	Database getDatabase(){
		return cx;
	}
	static Reference<TransactionLogInfo> createTrLogInfoProbabilistically(const Database& cx);
	TransactionOptions options;
	double startTime;
	Reference<TransactionLogInfo> trLogInfo;
private:
	Future<Version> getReadVersion(uint32_t flags);
	void setPriority(uint32_t priorityFlag);

	Database cx;

	double backoff;
	Version committedVersion;
	CommitTransactionRequest tr;
	Future<Version> readVersion;
	vector<Future<std::pair<Key, Key>>> extraConflictRanges;
	Promise<Void> commitResult;
	Future<Void> committing;
};

Future<Version> waitForCommittedVersion( Database const& cx, Version const& version );

std::string unprintable( const std::string& );

int64_t extractIntOption( Optional<StringRef> value, int64_t minValue = std::numeric_limits<int64_t>::min(), int64_t maxValue = std::numeric_limits<int64_t>::max() );

#endif
