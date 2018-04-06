/*
 * fdb_flow.h
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

#ifndef FDB_FLOW_FDB_FLOW_H
#define FDB_FLOW_FDB_FLOW_H

#include <flow/flow.h>

#define FDB_API_VERSION 520
#include <bindings/c/foundationdb/fdb_c.h>
#undef DLLEXPORT

#include "FDBLoanerTypes.h"

namespace FDB {

	class DatabaseContext : public ReferenceCounted<DatabaseContext>, NonCopyable {
		friend class Cluster;
		friend class Transaction;
	public:
		~DatabaseContext() {
			fdb_database_destroy( db );
		}

		void setDatabaseOption(FDBDatabaseOption option, Optional<StringRef> value = Optional<StringRef>());

	private:
		FDBDatabase* db;
		explicit DatabaseContext( FDBDatabase* db ) : db(db) {}
	};

	class Cluster : public ReferenceCounted<Cluster>, NonCopyable {
	public:
		~Cluster() {
			fdb_cluster_destroy( c );
		}

		Reference<DatabaseContext> createDatabase();

	private:
		explicit Cluster( FDBCluster* c ) : c(c) {}
		FDBCluster* c;

		friend class API;
	};

	class API {
	public:
		static API* selectAPIVersion(int apiVersion);
		static API* getInstance();
		static bool isAPIVersionSelected();

		void setNetworkOption(FDBNetworkOption option, Optional<StringRef> value = Optional<StringRef>());

		void setupNetwork();
		void runNetwork();
		void stopNetwork();

		Reference<Cluster> createCluster( std::string const& connFilename );

		bool evaluatePredicate(FDBErrorPredicate pred, Error const& e);
		int getAPIVersion() const;

	private:
		static API* instance;

		API(int version);
		int version;
	};

	struct CFuture : NonCopyable, ReferenceCounted<CFuture>, FastAllocated<CFuture> {
		CFuture() : f(NULL) {}
		explicit CFuture( FDBFuture* f ) : f(f) {}
		~CFuture() {
			if (f) {
				fdb_future_destroy(f);
			}
		}

		void blockUntilReady();

		FDBFuture* f;
	};

	template <class T>
	class FDBStandalone : public T {
	public:
		FDBStandalone() {}
		FDBStandalone( Reference<CFuture> f, T const& t ) : T(t), f(f) {}
		FDBStandalone( FDBStandalone const& o ) : T((T const&)o), f(o.f) {}
	private:
		Reference<CFuture> f;
	};

	class Transaction : public ReferenceCounted<Transaction>, private NonCopyable, public FastAllocated<Transaction> {
	public:
		explicit Transaction( Reference<DatabaseContext> const& db );
		~Transaction() {
			if (tr) {
				fdb_transaction_destroy(tr);
			}
		}

		void setVersion( Version v );
		Future<Version> getReadVersion();

		Future< Optional<FDBStandalone<ValueRef>> > get( const Key& key, bool snapshot = false );
		Future< Void > watch( const Key& key );
		Future< FDBStandalone<KeyRef> > getKey( const KeySelector& key, bool snapshot = false );
		Future< FDBStandalone<RangeResultRef> > getRange( const KeySelector& begin, const KeySelector& end, GetRangeLimits limits = GetRangeLimits(), bool snapshot = false, bool reverse = false, FDBStreamingMode streamingMode = FDB_STREAMING_MODE_SERIAL);
		Future< FDBStandalone<RangeResultRef> > getRange( const KeySelector& begin, const KeySelector& end, int limit, bool snapshot = false, bool reverse = false, FDBStreamingMode streamingMode = FDB_STREAMING_MODE_SERIAL ) {
			return getRange( begin, end, GetRangeLimits(limit), snapshot, reverse, streamingMode );
		}
		Future< FDBStandalone<RangeResultRef> > getRange( const KeyRange& keys, int limit, bool snapshot = false, bool reverse = false, FDBStreamingMode streamingMode = FDB_STREAMING_MODE_SERIAL ) {
			return getRange( KeySelector( firstGreaterOrEqual(keys.begin), keys.arena() ),
							 KeySelector( firstGreaterOrEqual(keys.end), keys.arena() ),
							 limit, snapshot, reverse, streamingMode );
		}
		Future< FDBStandalone<RangeResultRef> > getRange( const KeyRange& keys, GetRangeLimits limits = GetRangeLimits(), bool snapshot = false, bool reverse = false, FDBStreamingMode streamingMode = FDB_STREAMING_MODE_SERIAL ) {
			return getRange( KeySelector( firstGreaterOrEqual(keys.begin), keys.arena() ),
							 KeySelector( firstGreaterOrEqual(keys.end), keys.arena() ),
							 limits, snapshot, reverse, streamingMode );
		}

		// Future< Standalone<VectorRef<const char*>> > getAddressesForKey(const Key& key);

		void addReadConflictRange( KeyRangeRef const& keys );
		void addReadConflictKey( KeyRef const& key );
		void addWriteConflictRange( KeyRangeRef const& keys );
		void addWriteConflictKey( KeyRef const& key );
		// void makeSelfConflicting() { tr.makeSelfConflicting(); }

		void atomicOp( const KeyRef& key, const ValueRef& operand, FDBMutationType operationType );
		void set( const KeyRef& key, const ValueRef& value );
		void clear( const KeyRangeRef& range );
		void clear( const KeyRef& key );

		Future<Void> commit();
		Version getCommittedVersion();
		Future<FDBStandalone<StringRef>> getVersionstamp();

		void setOption( FDBTransactionOption option, Optional<StringRef> value = Optional<StringRef>() );

		Future<Void> onError( Error const& e );

		void cancel();
		void reset();
		// double getBackoff() { return tr.getBackoff(); }
		// void debugTransaction(UID dID) { tr.debugTransaction(dID); }

		Transaction() : tr(NULL) {}
		Transaction( Transaction&& r ) noexcept(true) {
			tr = r.tr;
			r.tr = NULL;
		}
		Transaction& operator=( Transaction&& r ) noexcept(true) {
			tr = r.tr;
			r.tr = NULL;
			return *this;
		}

	private:
		FDBTransaction* tr;
	};

}

#endif
