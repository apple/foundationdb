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

#include "fdbclient/ReadYourWrites.h"
#include "flow/ThreadHelper.actor.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/IClientApi.h"

class ThreadSafeDatabase : public IDatabase, public ThreadSafeReferenceCounted<ThreadSafeDatabase> {
public:
	~ThreadSafeDatabase();
	static ThreadFuture<Reference<IDatabase>> createFromExistingDatabase(Database cx);

	Reference<ITransaction> createTransaction();

	void setOption( FDBDatabaseOptions::Option option, Optional<StringRef> value = Optional<StringRef>() );

	ThreadFuture<Void> onConnected();  // Returns after a majority of coordination servers are available and have reported a leader. The cluster file therefore is valid, but the database might be unavailable.

	void addref() { ThreadSafeReferenceCounted<ThreadSafeDatabase>::addref(); }
	void delref() { ThreadSafeReferenceCounted<ThreadSafeDatabase>::delref(); }

private:
	friend class ThreadSafeTransaction;
	DatabaseContext* db;
public:  // Internal use only
	ThreadSafeDatabase( std::string connFilename, int apiVersion );
	ThreadSafeDatabase( DatabaseContext* db ) : db(db) {}
	DatabaseContext* unsafeGetPtr() const { return db; }
};

class ThreadSafeTransaction : public ITransaction, ThreadSafeReferenceCounted<ThreadSafeTransaction>, NonCopyable {
public:
	explicit ThreadSafeTransaction(DatabaseContext* cx);
	~ThreadSafeTransaction();

	void cancel() override;
	void setVersion( Version v ) override;
	ThreadFuture<Version> getReadVersion() override;

	ThreadFuture< Optional<Value> > get( const KeyRef& key, bool snapshot = false ) override;
	ThreadFuture< Key > getKey( const KeySelectorRef& key, bool snapshot = false ) override;
	ThreadFuture< Standalone<RangeResultRef> > getRange( const KeySelectorRef& begin, const KeySelectorRef& end, int limit, bool snapshot = false, bool reverse = false ) override;
	ThreadFuture< Standalone<RangeResultRef> > getRange( const KeySelectorRef& begin, const KeySelectorRef& end, GetRangeLimits limits, bool snapshot = false, bool reverse = false ) override;
	ThreadFuture< Standalone<RangeResultRef> > getRange( const KeyRangeRef& keys, int limit, bool snapshot = false, bool reverse = false ) override {
		return getRange( firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), limit, snapshot, reverse );
	}
	ThreadFuture< Standalone<RangeResultRef> > getRange( const KeyRangeRef& keys, GetRangeLimits limits, bool snapshot = false, bool reverse = false ) override {
		return getRange( firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), limits, snapshot, reverse );
	}
	ThreadFuture<Standalone<VectorRef<const char*>>> getAddressesForKey(const KeyRef& key) override;
	ThreadFuture<Standalone<StringRef>> getVersionstamp() override;
	ThreadFuture<int64_t> getEstimatedRangeSizeBytes(const KeyRangeRef& keys) override;

	void addReadConflictRange( const KeyRangeRef& keys ) override;
	void makeSelfConflicting();

	void atomicOp( const KeyRef& key, const ValueRef& value, uint32_t operationType ) override;
	void set( const KeyRef& key, const ValueRef& value ) override;
	void clear( const KeyRef& begin, const KeyRef& end) override;
	void clear( const KeyRangeRef& range ) override;
	void clear( const KeyRef& key ) override;

	ThreadFuture< Void > watch( const KeyRef& key ) override;

	void addWriteConflictRange( const KeyRangeRef& keys ) override;

	ThreadFuture<Void> commit() override;
	Version getCommittedVersion() override;
	ThreadFuture<int64_t> getApproximateSize() override;

	void setOption( FDBTransactionOptions::Option option, Optional<StringRef> value = Optional<StringRef>() ) override;

	ThreadFuture<Void> checkDeferredError();
	ThreadFuture<Void> onError( Error const& e ) override;

	// These are to permit use as state variables in actors:
	ThreadSafeTransaction() : tr(nullptr) {}
	void operator=(ThreadSafeTransaction&& r) noexcept;
	ThreadSafeTransaction(ThreadSafeTransaction&& r) noexcept;

	void reset() override;

	void addref() override { ThreadSafeReferenceCounted<ThreadSafeTransaction>::addref(); }
	void delref() override { ThreadSafeReferenceCounted<ThreadSafeTransaction>::delref(); }

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

	Reference<IDatabase> createDatabase(const char *clusterFilePath);

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
