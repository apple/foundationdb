/*
 * ThreadSafeTransaction.actor.cpp
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

#include "ThreadSafeTransaction.h"
#include "ReadYourWrites.h"
#include "DatabaseContext.h"
#include <new>

#ifndef WIN32
#include "versions.h"
#endif

// Users of ThreadSafeTransaction might share Reference<ThreadSafe...> between different threads as long as they don't call addRef (e.g. C API follows this).
// Therefore, it is unsafe to call (explicitly or implicitly) this->addRef in any of these functions.

Reference<ICluster> constructThreadSafeCluster( Cluster* cluster ) {
	return Reference<ICluster>( new ThreadSafeCluster(cluster) );
}
Future<Reference<ICluster>> createThreadSafeCluster( std::string connFilename, int apiVersion ) {
	Reference<Cluster> cluster = Cluster::createCluster( connFilename, apiVersion );
	return constructThreadSafeCluster( cluster.extractPtr() );
}
ThreadFuture<Reference<ICluster>> ThreadSafeCluster::create( std::string connFilename, int apiVersion ) {
	if (!g_network) return ThreadFuture<Reference<ICluster>>(network_not_setup());
	return onMainThread( [connFilename, apiVersion](){ return createThreadSafeCluster( connFilename, apiVersion ); } );
}
ThreadFuture<Void> ThreadSafeCluster::onConnected() {
	Cluster* cluster = this->cluster;
	return onMainThread( [cluster]() -> Future<Void> {
		cluster->checkDeferredError();
		return cluster->onConnected();
	} );
}

void ThreadSafeCluster::setOption( FDBClusterOptions::Option option, Optional<StringRef> value ) {
	Cluster* cluster = this->cluster;
	Standalone<Optional<StringRef>> passValue = value;
	onMainThreadVoid( [cluster, option, passValue](){ cluster->setOption(option, passValue.contents()); }, &cluster->deferred_error );
}

ThreadSafeCluster::~ThreadSafeCluster() {
	Cluster* cluster = this->cluster;
	onMainThreadVoid( [cluster](){ cluster->delref(); }, NULL );
}

Future<Reference<IDatabase>> threadSafeCreateDatabase( Database db ) {
	db.getPtr()->addref();
	return Reference<IDatabase>(new ThreadSafeDatabase(db.getPtr()));
}

ACTOR Future<Reference<IDatabase>> threadSafeCreateDatabase( Cluster* cluster, Standalone<StringRef> name ) {
	Database db = wait( cluster->createDatabase(name) );
	Reference<IDatabase> threadSafeDb = wait(threadSafeCreateDatabase(db));
	return threadSafeDb;
}

ThreadFuture<Reference<IDatabase>> ThreadSafeCluster::createDatabase( Standalone<StringRef> dbName ) {
	Cluster* cluster = this->cluster;
	return onMainThread( [cluster, dbName](){
		cluster->checkDeferredError();
		return threadSafeCreateDatabase(cluster, dbName);
	} );
}

ThreadFuture<Reference<IDatabase>> ThreadSafeDatabase::createFromExistingDatabase(Database db) {
	return onMainThread( [db](){
		db->checkDeferredError();
		return threadSafeCreateDatabase(db);
	});
}

Reference<ITransaction> ThreadSafeDatabase::createTransaction() {
	return Reference<ITransaction>(new ThreadSafeTransaction(this));
}

Database ThreadSafeDatabase::unsafeGetDatabase() const {
	db->addref();
	return Database(db);
}

void ThreadSafeDatabase::setOption( FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	DatabaseContext *db = this->db;
	Standalone<Optional<StringRef>> passValue = value;
	onMainThreadVoid( [db, option, passValue](){ db->setOption(option, passValue.contents()); }, &db->deferred_error );
}

ThreadSafeDatabase::~ThreadSafeDatabase() {
	DatabaseContext* db = this->db;
	onMainThreadVoid( [db](){ db->delref(); }, NULL );
}

ThreadSafeTransaction::ThreadSafeTransaction( ThreadSafeDatabase *cx ) {
	// Allocate memory for the transaction from this thread (so the pointer is known for subsequent method calls)
	// but run its constructor on the main thread

	// It looks strange that the DatabaseContext::addref is deferred by the onMainThreadVoid call, but it is safe
	// because the reference count of the DatabaseContext is solely managed from the main thread.  If cx is destructed
	// immediately after this call, it will defer the DatabaseContext::delref (and onMainThread preserves the order of
	// these operations).
	DatabaseContext* db = cx->db;
	ReadYourWritesTransaction *tr = this->tr = ReadYourWritesTransaction::allocateOnForeignThread();
	// No deferred error -- if the construction of the RYW transaction fails, we have no where to put it
	onMainThreadVoid( [tr,db](){ db->addref(); new (tr) ReadYourWritesTransaction( Database(db) ); }, NULL );
}

ThreadSafeTransaction::~ThreadSafeTransaction() {
	ReadYourWritesTransaction *tr = this->tr;
	if (tr)
		onMainThreadVoid( [tr](){ tr->delref(); }, NULL );
}

void ThreadSafeTransaction::cancel() {
	ReadYourWritesTransaction *tr = this->tr;
	onMainThreadVoid( [tr](){ tr->cancel(); }, NULL );
}

void ThreadSafeTransaction::setVersion( Version v ) {
	ReadYourWritesTransaction *tr = this->tr;
	onMainThreadVoid( [tr, v](){ tr->setVersion(v); }, &tr->deferred_error );
}

ThreadFuture<Version> ThreadSafeTransaction::getReadVersion() {
	ReadYourWritesTransaction *tr = this->tr;
	return onMainThread( [tr]() -> Future<Version> {
			tr->checkDeferredError();
			return tr->getReadVersion();
		} );
}

ThreadFuture< Optional<Value> > ThreadSafeTransaction::get( const KeyRef& key, bool snapshot ) {
	Key k = key;

	ReadYourWritesTransaction *tr = this->tr;
	return onMainThread( [tr, k, snapshot]() -> Future< Optional<Value> > {
			tr->checkDeferredError();
			return tr->get(k, snapshot);
		} );
}

ThreadFuture< Key > ThreadSafeTransaction::getKey( const KeySelectorRef& key, bool snapshot ) {
	KeySelector k = key;

	ReadYourWritesTransaction *tr = this->tr;
	return onMainThread( [tr, k, snapshot]() -> Future< Key > {
			tr->checkDeferredError();
			return tr->getKey(k, snapshot);
		} );
}

ThreadFuture< Standalone<RangeResultRef> > ThreadSafeTransaction::getRange( const KeySelectorRef& begin, const KeySelectorRef& end, int limit, bool snapshot, bool reverse ) {
	KeySelector b = begin;
	KeySelector e = end;

	ReadYourWritesTransaction *tr = this->tr;
	return onMainThread( [tr, b, e, limit, snapshot, reverse]() -> Future< Standalone<RangeResultRef> > {
			tr->checkDeferredError();
			return tr->getRange(b, e, limit, snapshot, reverse);
		} );
}

ThreadFuture< Standalone<RangeResultRef> > ThreadSafeTransaction::getRange( const KeySelectorRef& begin, const KeySelectorRef& end, GetRangeLimits limits, bool snapshot, bool reverse ) {
	KeySelector b = begin;
	KeySelector e = end;

	ReadYourWritesTransaction *tr = this->tr;
	return onMainThread( [tr, b, e, limits, snapshot, reverse]() -> Future< Standalone<RangeResultRef> > {
			tr->checkDeferredError();
			return tr->getRange(b, e, limits, snapshot, reverse);
		} );
}

ThreadFuture<Standalone<VectorRef<const char*>>> ThreadSafeTransaction::getAddressesForKey( const KeyRef& key ) {
	Key k = key;

	ReadYourWritesTransaction *tr = this->tr;
	return onMainThread( [tr, k]() -> Future< Standalone<VectorRef<const char*> >> {
		tr->checkDeferredError();
		return tr->getAddressesForKey(k);
	} );
}

void ThreadSafeTransaction::addReadConflictRange( const KeyRangeRef& keys) {
	KeyRange r = keys;

	ReadYourWritesTransaction *tr = this->tr;
	onMainThreadVoid( [tr, r](){ tr->addReadConflictRange(r); }, &tr->deferred_error );
}

void ThreadSafeTransaction::makeSelfConflicting() {
	ReadYourWritesTransaction *tr = this->tr;
	onMainThreadVoid( [tr](){ tr->makeSelfConflicting(); }, &tr->deferred_error );
}

void ThreadSafeTransaction::atomicOp( const KeyRef& key, const ValueRef& value, uint32_t operationType ) {
	Key k = key;
	Value v = value;

	ReadYourWritesTransaction *tr = this->tr;
	onMainThreadVoid( [tr, k, v, operationType](){ tr->atomicOp(k, v, operationType); }, &tr->deferred_error );
}

void ThreadSafeTransaction::set( const KeyRef& key, const ValueRef& value ) {
	Key k = key;
	Value v = value;

	ReadYourWritesTransaction *tr = this->tr;
	onMainThreadVoid( [tr, k, v](){ tr->set(k, v); }, &tr->deferred_error );
}

void ThreadSafeTransaction::clear( const KeyRangeRef& range ) {
	KeyRange r = range;

	ReadYourWritesTransaction *tr = this->tr;
	onMainThreadVoid( [tr, r](){ tr->clear(r); }, &tr->deferred_error );
}

void ThreadSafeTransaction::clear( const KeyRef& begin, const KeyRef& end ) {
	Key b = begin;
	Key e = end;

	ReadYourWritesTransaction *tr = this->tr;
	onMainThreadVoid( [tr, b, e](){
		if(b > e)
			throw inverted_range();

		tr->clear(KeyRangeRef(b, e));
	}, &tr->deferred_error );
}

void ThreadSafeTransaction::clear( const KeyRef& key ) {
	Key k = key;

	ReadYourWritesTransaction *tr = this->tr;
	onMainThreadVoid( [tr, k](){ tr->clear(k); }, &tr->deferred_error );
}

ThreadFuture< Void > ThreadSafeTransaction::watch( const KeyRef& key ) {
	Key k = key;

	ReadYourWritesTransaction *tr = this->tr;
	return onMainThread( [tr, k]() -> Future< Void > {
		tr->checkDeferredError();
		return tr->watch(k);
	});
}

void ThreadSafeTransaction::addWriteConflictRange( const KeyRangeRef& keys) {
	KeyRange r = keys;

	ReadYourWritesTransaction *tr = this->tr;
	onMainThreadVoid( [tr, r](){ tr->addWriteConflictRange(r); }, &tr->deferred_error );
}

ThreadFuture< Void > ThreadSafeTransaction::commit() {
	ReadYourWritesTransaction *tr = this->tr;
	return onMainThread( [tr]() -> Future< Void > {
			tr->checkDeferredError();
			return tr->commit();
		} );
}

Version ThreadSafeTransaction::getCommittedVersion() {
	// This should be thread safe when called legally, but it is fragile
	Version v = tr->getCommittedVersion();
	return v;
}

ThreadFuture<Standalone<StringRef>> ThreadSafeTransaction::getVersionstamp() {
	ReadYourWritesTransaction *tr = this->tr;
	return onMainThread([tr]() -> Future < Standalone<StringRef> > {
		return tr->getVersionstamp();
	});
}

void ThreadSafeTransaction::setOption( FDBTransactionOptions::Option option, Optional<StringRef> value ) {
	ReadYourWritesTransaction *tr = this->tr;
	Standalone<Optional<StringRef>> passValue = value;
	onMainThreadVoid( [tr, option, passValue](){ tr->setOption(option, passValue.contents()); }, &tr->deferred_error );
}

ThreadFuture<Void> ThreadSafeTransaction::checkDeferredError() {
	ReadYourWritesTransaction *tr = this->tr;
	return onMainThread( [tr](){
		try {
			tr->checkDeferredError();
		} catch (Error &e) {
			tr->deferred_error = Error();
			return Future<Void>(e);
		}
		return Future<Void>(Void());
	} );
}

ThreadFuture<Void> ThreadSafeTransaction::onError( Error const& e ) {
	ReadYourWritesTransaction *tr = this->tr;
	return onMainThread( [tr, e](){ return tr->onError(e); } );
}

void ThreadSafeTransaction::operator=(ThreadSafeTransaction&& r) noexcept(true) {
	tr = r.tr;
	r.tr = NULL;
}

ThreadSafeTransaction::ThreadSafeTransaction(ThreadSafeTransaction&& r) noexcept(true) {
	tr = r.tr;
	r.tr = NULL;
}

void ThreadSafeTransaction::reset() {
	ReadYourWritesTransaction *tr = this->tr;
	onMainThreadVoid( [tr](){ tr->reset(); }, NULL );
}

extern const char* getHGVersion();

ThreadSafeApi::ThreadSafeApi() : apiVersion(-1), clientVersion(format("%s,%s,%llx", FDB_VT_VERSION, getHGVersion(), currentProtocolVersion)), transportId(0) {}

void ThreadSafeApi::selectApiVersion(int apiVersion) {
	this->apiVersion = apiVersion;
}

const char* ThreadSafeApi::getClientVersion() {
	// There is only one copy of the ThreadSafeAPI, and it never gets deleted. Also, clientVersion is never modified.
	return clientVersion.c_str();
}

void ThreadSafeApi::setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	if (option == FDBNetworkOptions::EXTERNAL_CLIENT_TRANSPORT_ID) {
		if(value.present()) {
			transportId = std::stoull(value.get().toString().c_str());
		}
	}
	else {
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
	}
	catch(Error &e) {
		runErr = e;
	}

	for(auto &hook : threadCompletionHooks) {
		try {
			hook.first(hook.second);
		}
		catch(Error &e) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(e);
		}
		catch(...) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(unknown_error());
		}
	}

	if(runErr.present()) {
		throw runErr.get();
	}

}

void ThreadSafeApi::stopNetwork() {
	::stopNetwork();
}

ThreadFuture<Reference<ICluster>> ThreadSafeApi::createCluster(const char *clusterFilePath) {
	return ThreadSafeCluster::create(clusterFilePath, apiVersion);
}

void ThreadSafeApi::addNetworkThreadCompletionHook(void (*hook)(void*), void *hookParameter) {
	if (!g_network) {
		throw network_not_setup();
	}

	MutexHolder holder(lock); // We could use the network thread to protect this action, but then we can't guarantee upon return that the hook is set.
	threadCompletionHooks.push_back(std::make_pair(hook, hookParameter));
}


IClientApi* ThreadSafeApi::api = new ThreadSafeApi();
