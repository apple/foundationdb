/*
 * fdb_flow.actor.cpp
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

#include "fdb_flow.h"

#include "flow/DeterministicRandom.h"
#include "flow/SystemMonitor.h"

#include <stdio.h>

using namespace FDB;

THREAD_FUNC networkThread(void* fdb) {
	((FDB::API*)fdb)->runNetwork();
	THREAD_RETURN;
}

ACTOR Future<Void> _test() {
	API *fdb = FDB::API::selectAPIVersion(520);
	auto c = fdb->createCluster( std::string() );
	auto db = c->createDatabase();
	state Reference<Transaction> tr( new Transaction(db) );

	// tr->setVersion(1);

	Version ver = wait( tr->getReadVersion() );
	printf("%lld\n", ver);

	state std::vector< Future<Version> > versions;

	state double starttime = timer_monotonic();
	state int i;
	// for (i = 0; i < 100000; i++) {
	// 	Version v = wait( tr->getReadVersion() );
	// }
	for ( i = 0; i < 100000; i++ ) {
		versions.push_back( tr->getReadVersion() );
	}
	for ( i = 0; i < 100000; i++ ) {
		Version v = wait( versions[i] );
	}
	// Void _ = wait( waitForAllReady( versions ) );
	printf("Elapsed: %lf\n", timer_monotonic() - starttime );

	tr->set( LiteralStringRef("foo"), LiteralStringRef("bar") );

	Optional< FDBStandalone<ValueRef> > v = wait( tr->get( LiteralStringRef("foo") ) );
	if ( v.present() ) {
		printf("%s\n", v.get().toString().c_str() );
	}

	FDBStandalone<RangeResultRef> r = wait( tr->getRange( KeyRangeRef( LiteralStringRef("a"), LiteralStringRef("z") ), 100 ) );

	for ( auto kv : r ) {
		printf("%s is %s\n", kv.key.toString().c_str(), kv.value.toString().c_str());
	}

	g_network->stop();
	return Void();
}

void fdb_flow_test() {
	API *fdb = FDB::API::selectAPIVersion(520);
	fdb->setupNetwork();
	startThread(networkThread, fdb);

	int randomSeed = platform::getRandomSeed();

	g_random = new DeterministicRandom(randomSeed);
	g_nondeterministic_random = new DeterministicRandom(platform::getRandomSeed());
	g_debug_random = new DeterministicRandom(platform::getRandomSeed());

	g_network = newNet2( NetworkAddress(), false );

	openTraceFile(NetworkAddress(), 1000000, 1000000, ".");
	systemMonitor();
	uncancellable(recurring(&systemMonitor, 5.0, TaskFlushTrace));

	Future<Void> t = _test();

	g_network->run();
}

namespace FDB {

	static inline void throw_on_error( fdb_error_t e ) {
		if (e)
			throw Error(e);
	}

	void CFuture::blockUntilReady() {
		throw_on_error( fdb_future_block_until_ready( f ) );
	}

	void backToFutureCallback( FDBFuture* f, void* data ) {
		g_network->onMainThread( Promise<Void>((SAV<Void>*)data), TaskDefaultOnMainThread ); // SOMEDAY: think about this priority
	}

	// backToFuture<Type>( FDBFuture*, (FDBFuture* -> Type) ) -> Future<Type>
	// Takes an FDBFuture (from the alien client world, with callbacks potentially firing on an alien thread)
	//   and converts it into a Future<T> (with callbacks working on this thread, cancellation etc).
	// You must pass as the second parameter a function which takes a ready FDBFuture* and returns a value of Type
	ACTOR template<class T, class Function> static Future<T> backToFuture( FDBFuture* _f, Function convertValue ) {
		state Reference<CFuture> f( new CFuture(_f) );

		Promise<Void> ready;
		Future<Void> onReady = ready.getFuture();

		throw_on_error( fdb_future_set_callback( f->f, backToFutureCallback, ready.extractRawPointer() ) );
		Void _ = wait( onReady );

		return convertValue( f );
	}

	void API::setNetworkOption( FDBNetworkOption option, Optional<StringRef> value ) {
		if ( value.present() )
			throw_on_error( fdb_network_set_option( option, value.get().begin(), value.get().size() ) );
		else
			throw_on_error( fdb_network_set_option( option, NULL, 0 ) );
	}

	API* API::instance = NULL;
	API::API(int version) : version(version) {}

	API* API::selectAPIVersion(int apiVersion) {
		if(API::instance) {
			if(apiVersion != API::instance->version) {
				throw api_version_already_set();
			}
			else {
				return API::instance;
			}
		}

		if(apiVersion < 500 || apiVersion > FDB_API_VERSION) {
			throw api_version_not_supported();
		}

		throw_on_error( fdb_select_api_version_impl(apiVersion, FDB_API_VERSION) );

		API::instance = new API(apiVersion);
		return API::instance;
	}

	bool API::isAPIVersionSelected() {
		return API::instance != NULL;
	}

	API* API::getInstance() {
		if(API::instance == NULL) {
			throw api_version_unset();
		}
		else {
			return API::instance;
		}
	}

	void API::setupNetwork() {
		throw_on_error( fdb_setup_network() );
	}

	void API::runNetwork() {
		throw_on_error( fdb_run_network() );
	}

	void API::stopNetwork() {
		throw_on_error( fdb_stop_network() );
	}

	bool API::evaluatePredicate(FDBErrorPredicate pred, Error const& e) {
		return fdb_error_predicate( pred, e.code() );
	}

	Reference<Cluster> API::createCluster( std::string const& connFilename ) {
		CFuture f( fdb_create_cluster( connFilename.c_str() ) );
		f.blockUntilReady();

		FDBCluster* c;
		throw_on_error( fdb_future_get_cluster( f.f, &c ) );

		return Reference<Cluster>( new Cluster(c) );
	}

	int API::getAPIVersion() const {
		return version;
	}

	Reference<DatabaseContext> Cluster::createDatabase() {
		const char *dbName = "DB";
		CFuture f( fdb_cluster_create_database( c, (uint8_t*)dbName, (int)strlen(dbName) ) );
		f.blockUntilReady();

		FDBDatabase* db;
		throw_on_error( fdb_future_get_database( f.f, &db ) );

		return Reference<DatabaseContext>( new DatabaseContext(db) );
	}

	void DatabaseContext::setDatabaseOption(FDBDatabaseOption option, Optional<StringRef> value) {
		if (value.present())
			throw_on_error(fdb_database_set_option(db, option, value.get().begin(), value.get().size()));
		else
			throw_on_error(fdb_database_set_option(db, option, NULL, 0));
	}

	Transaction::Transaction( Reference<DatabaseContext> const& db ) {
		throw_on_error( fdb_database_create_transaction( db->db, &tr ) );
	}

	void Transaction::setVersion( Version v ) {
		fdb_transaction_set_read_version( tr, v );
	}

	Future<Version> Transaction::getReadVersion() {
		return backToFuture<Version>( fdb_transaction_get_read_version( tr ), [](Reference<CFuture> f){
				Version value;

				throw_on_error( fdb_future_get_version( f->f, &value ) );

				return value;
			} );
	}

	Future< Optional<FDBStandalone<ValueRef>> > Transaction::get( const Key& key, bool snapshot ) {
		return backToFuture< Optional<FDBStandalone<ValueRef>> >( fdb_transaction_get( tr, key.begin(), key.size(), snapshot ), [](Reference<CFuture> f) {
				fdb_bool_t present;
				uint8_t const* value;
				int value_length;

				throw_on_error( fdb_future_get_value( f->f, &present, &value, &value_length ) );

				if ( present ) {
					return Optional<FDBStandalone<ValueRef>>( FDBStandalone<ValueRef>( f, ValueRef( value, value_length ) ) );
				} else {
					return Optional<FDBStandalone<ValueRef>>();
				}
			} );
	}

	Future< Void > Transaction::watch( const Key& key ) {
		return backToFuture< Void >( fdb_transaction_watch( tr, key.begin(), key.size() ), [](Reference<CFuture> f) {
				throw_on_error( fdb_future_get_error( f->f ) );
				return Void();
			} );
	}

	Future< FDBStandalone<KeyRef> > Transaction::getKey( const KeySelector& key, bool snapshot ) {
		return backToFuture< FDBStandalone<KeyRef> >( fdb_transaction_get_key( tr, key.key.begin(), key.key.size(), key.orEqual, key.offset, snapshot ), [](Reference<CFuture> f) {
				uint8_t const* key;
				int key_length;

				throw_on_error( fdb_future_get_key( f->f, &key, &key_length ) );

				return FDBStandalone<KeyRef>( f, KeyRef( key, key_length ) );
			} );
	}

	Future< FDBStandalone<RangeResultRef> > Transaction::getRange( const KeySelector& begin, const KeySelector& end, GetRangeLimits limits, bool snapshot, bool reverse, FDBStreamingMode streamingMode ) {
		// FIXME: iteration
		return backToFuture< FDBStandalone<RangeResultRef> >( fdb_transaction_get_range( tr, begin.key.begin(), begin.key.size(), begin.orEqual, begin.offset, end.key.begin(), end.key.size(), end.orEqual, end.offset, limits.rows, limits.bytes, streamingMode, 1, snapshot, reverse ), [](Reference<CFuture> f) {
				FDBKeyValue const* kv;
				int count;
				fdb_bool_t more;

				throw_on_error( fdb_future_get_keyvalue_array( f->f, &kv, &count, &more ) );

				return FDBStandalone<RangeResultRef>( f, RangeResultRef( VectorRef<KeyValueRef>( (KeyValueRef*)kv, count ), more ) );
			} );
	}

	void Transaction::addReadConflictRange( KeyRangeRef const& keys ) {
		throw_on_error( fdb_transaction_add_conflict_range( tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size(), FDB_CONFLICT_RANGE_TYPE_READ ) );
	}

	void Transaction::addReadConflictKey( KeyRef const& key ) {
		return addReadConflictRange(KeyRange(KeyRangeRef(key, keyAfter(key))));
	}

	void Transaction::addWriteConflictRange( KeyRangeRef const& keys ) {
		throw_on_error( fdb_transaction_add_conflict_range( tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size(), FDB_CONFLICT_RANGE_TYPE_WRITE ) );
	}

	void Transaction::addWriteConflictKey( KeyRef const& key ) {
		return addWriteConflictRange(KeyRange(KeyRangeRef(key, keyAfter(key))));
	}

	void Transaction::atomicOp( const KeyRef& key, const ValueRef& operand, FDBMutationType operationType ) {
		fdb_transaction_atomic_op( tr, key.begin(), key.size(), operand.begin(), operand.size(), operationType );
	}

	void Transaction::set( const KeyRef& key, const ValueRef& value ) {
		fdb_transaction_set( tr, key.begin(), key.size(), value.begin(), value.size() );
	}

	void Transaction::clear( const KeyRangeRef& range ) {
		fdb_transaction_clear_range( tr, range.begin.begin(), range.begin.size(), range.end.begin(), range.end.size() );
	}

	void Transaction::clear( const KeyRef& key ) {
		fdb_transaction_clear( tr, key.begin(), key.size() );
	}

	Future<Void> Transaction::commit() {
		return backToFuture< Void >( fdb_transaction_commit( tr ), [](Reference<CFuture> f) {
				throw_on_error( fdb_future_get_error( f->f ) );
				return Void();
			} );
	}

	Version Transaction::getCommittedVersion() {
		Version v;

		throw_on_error( fdb_transaction_get_committed_version( tr, &v ) );
		return v;
	}

	Future<FDBStandalone<StringRef>> Transaction::getVersionstamp() {
			return backToFuture< FDBStandalone<KeyRef> >( fdb_transaction_get_versionstamp( tr ), [](Reference<CFuture> f) {
			uint8_t const* key;
			int key_length;

			throw_on_error( fdb_future_get_key( f->f, &key, &key_length ) );

			return FDBStandalone<StringRef>( f, StringRef( key, key_length ) );
		} );
	}

	void Transaction::setOption( FDBTransactionOption option, Optional<StringRef> value ) {
		if ( value.present() ) {
			throw_on_error( fdb_transaction_set_option( tr, option, value.get().begin(), value.get().size() ) );
		} else {
			throw_on_error( fdb_transaction_set_option( tr, option, NULL, 0 ) );
		}
	}

	Future<Void> Transaction::onError( Error const& e ) {
		return backToFuture< Void >( fdb_transaction_on_error( tr, e.code() ), [](Reference<CFuture> f) {
				throw_on_error( fdb_future_get_error( f->f ) );
				return Void();
			} );
	}

	void Transaction::cancel() {
		fdb_transaction_cancel( tr );
	}

	void Transaction::reset() {
		fdb_transaction_reset( tr );
	}

	std::string printable( const StringRef& val ) {
		std::string s;
		for(int i=0; i<val.size(); i++) {
			uint8_t b = val[i];
			if (b >= 32 && b < 127 && b != '\\') s += (char)b;
			else if (b == '\\') s += "\\\\";
			else s += format("\\x%02x", b);
		}
		return s;
	}

}
