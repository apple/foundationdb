/*
 * ThreadSafeTransaction.h
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

#ifndef FDBCLIENT_THREADSAFETRANSACTION_H
#define FDBCLIENT_THREADSAFETRANSACTION_H
#pragma once

#include "ReadYourWrites.h"
#include "flow/ThreadHelper.actor.h"
#include "ClusterInterface.h"
#include "IClientApi.h"

class ThreadSafeDatabase;

class ThreadSafeCluster : public ICluster, public ThreadSafeReferenceCounted<ThreadSafeCluster>, private NonCopyable {
public:
	static ThreadFuture<Reference<ICluster>> create( std::string connFilename, int apiVersion = -1 );
	~ThreadSafeCluster();
	ThreadFuture<Reference<IDatabase>> createDatabase( Standalone<StringRef> dbName );

	void setOption( FDBClusterOptions::Option option, Optional<StringRef> value  = Optional<StringRef>() );

	ThreadFuture<Void> onConnected();  // Returns after a majority of coordination servers are available and have reported a leader. The cluster file therefore is valid, but the database might be unavailable.

	void addref() { ThreadSafeReferenceCounted<ThreadSafeCluster>::addref(); }
	void delref() { ThreadSafeReferenceCounted<ThreadSafeCluster>::delref(); }

private:
	ThreadSafeCluster( Cluster* cluster ) : cluster(cluster) { }
	Cluster* cluster;
	friend Reference<ICluster> constructThreadSafeCluster( Cluster* cluster );
};

class ThreadSafeDatabase : public IDatabase, public ThreadSafeReferenceCounted<ThreadSafeDatabase> {
public:
	~ThreadSafeDatabase();
	static ThreadFuture<Reference<IDatabase>> createFromExistingDatabase(Database cx);

	Reference<ITransaction> createTransaction();

	void setOption( FDBDatabaseOptions::Option option, Optional<StringRef> value = Optional<StringRef>() );

	void addref() { ThreadSafeReferenceCounted<ThreadSafeDatabase>::addref(); }
	void delref() { ThreadSafeReferenceCounted<ThreadSafeDatabase>::delref(); }

private:
	friend class ThreadSafeCluster;
	friend class ThreadSafeTransaction;
	DatabaseContext* db;
public:  // Internal use only
	ThreadSafeDatabase( DatabaseContext* db ) : db(db) {}
	DatabaseContext* unsafeGetPtr() const { return db; }
	Database unsafeGetDatabase() const;  // This is thread unsafe (ONLY call from the network thread), but respects reference counting
};

class ThreadSafeTransaction : public ITransaction, ThreadSafeReferenceCounted<ThreadSafeTransaction>, NonCopyable {
public:
	explicit ThreadSafeTransaction( ThreadSafeDatabase *cx );
	~ThreadSafeTransaction();

	void cancel();
	void setVersion( Version v );
	ThreadFuture<Version> getReadVersion();

	ThreadFuture< Optional<Value> > get( const KeyRef& key, bool snapshot = false );
	ThreadFuture< Key > getKey( const KeySelectorRef& key, bool snapshot = false );
	ThreadFuture< Standalone<RangeResultRef> > getRange( const KeySelectorRef& begin, const KeySelectorRef& end, int limit, bool snapshot = false, bool reverse = false );
	ThreadFuture< Standalone<RangeResultRef> > getRange( const KeySelectorRef& begin, const KeySelectorRef& end, GetRangeLimits limits, bool snapshot = false, bool reverse = false );
	ThreadFuture< Standalone<RangeResultRef> > getRange( const KeyRangeRef& keys, int limit, bool snapshot = false, bool reverse = false ) {
		return getRange( firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), limit, snapshot, reverse );
	}
	ThreadFuture< Standalone<RangeResultRef> > getRange( const KeyRangeRef& keys, GetRangeLimits limits, bool snapshot = false, bool reverse = false ) {
		return getRange( firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), limits, snapshot, reverse );
	}

	ThreadFuture<Standalone<VectorRef<const char*>>> getAddressesForKey(const KeyRef& key);

	void addReadConflictRange( const KeyRangeRef& keys );
	void makeSelfConflicting();

	void atomicOp( const KeyRef& key, const ValueRef& value, uint32_t operationType );
	void set( const KeyRef& key, const ValueRef& value );
	void clear( const KeyRef& begin, const KeyRef& end);
	void clear( const KeyRangeRef& range );
	void clear( const KeyRef& key );

	ThreadFuture< Void > watch( const KeyRef& key );

	void addWriteConflictRange( const KeyRangeRef& keys );

	ThreadFuture<Void> commit();
	Version getCommittedVersion();
	ThreadFuture<Standalone<StringRef>> getVersionstamp();

	void setOption( FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>() );

	ThreadFuture<Void> checkDeferredError();
	ThreadFuture<Void> onError( Error const& e );

	// These are to permit use as state variables in actors:
	ThreadSafeTransaction() : tr(NULL) {}
	void operator=(ThreadSafeTransaction&& r) noexcept(true);
	ThreadSafeTransaction(ThreadSafeTransaction&& r) noexcept(true);

	void reset();

	void addref() { ThreadSafeReferenceCounted<ThreadSafeTransaction>::addref(); }
	void delref() { ThreadSafeReferenceCounted<ThreadSafeTransaction>::delref(); }

private:
	ReadYourWritesTransaction *tr;
};

class ThreadSafeApi : public IClientApi, ThreadSafeReferenceCounted<ThreadSafeApi> {
public:
	void selectApiVersion(int apiVersion);
	const char* getClientVersion();

	void setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value = Optional<StringRef>());
	void setupNetwork();
	void runNetwork();
	void stopNetwork();

	ThreadFuture<Reference<ICluster>> createCluster(const char *clusterFilePath);

	void addNetworkThreadCompletionHook(void (*hook)(void*), void *hookParameter);

	static IClientApi* api;

private:
	ThreadSafeApi();

	int apiVersion;
	const std::string clientVersion;
	uint64_t transportId;
	
	Mutex lock;
	std::vector<std::pair<void (*)(void*), void*>> threadCompletionHooks;
};

#endif
