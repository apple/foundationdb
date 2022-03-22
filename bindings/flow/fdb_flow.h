/*
 * fdb_flow.h
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

#ifndef FDB_FLOW_FDB_FLOW_H
#define FDB_FLOW_FDB_FLOW_H

#include <flow/flow.h>

#define FDB_API_VERSION 710
#include <bindings/c/foundationdb/fdb_c.h>
#undef DLLEXPORT

#include "FDBLoanerTypes.h"

namespace FDB {
struct CFuture : NonCopyable, ReferenceCounted<CFuture>, FastAllocated<CFuture> {
	CFuture() : f(nullptr) {}
	explicit CFuture(FDBFuture* f) : f(f) {}
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
	FDBStandalone(Reference<CFuture> f, T const& t) : T(t), f(f) {}
	FDBStandalone(FDBStandalone const& o) : T((T const&)o), f(o.f) {}

private:
	Reference<CFuture> f;
};

class ReadTransaction : public ReferenceCounted<ReadTransaction> {
public:
	virtual ~ReadTransaction(){};
	virtual void setReadVersion(Version v) = 0;
	virtual Future<Version> getReadVersion() = 0;

	virtual Future<Optional<FDBStandalone<ValueRef>>> get(const Key& key, bool snapshot = false) = 0;
	virtual Future<FDBStandalone<KeyRef>> getKey(const KeySelector& key, bool snapshot = false) = 0;
	virtual Future<Void> watch(const Key& key) = 0;

	virtual Future<FDBStandalone<RangeResultRef>> getRange(
	    const KeySelector& begin,
	    const KeySelector& end,
	    GetRangeLimits limits = GetRangeLimits(),
	    bool snapshot = false,
	    bool reverse = false,
	    FDBStreamingMode streamingMode = FDB_STREAMING_MODE_SERIAL) = 0;
	virtual Future<FDBStandalone<RangeResultRef>> getRange(const KeySelector& begin,
	                                                       const KeySelector& end,
	                                                       int limit,
	                                                       bool snapshot = false,
	                                                       bool reverse = false,
	                                                       FDBStreamingMode streamingMode = FDB_STREAMING_MODE_SERIAL) {
		return getRange(begin, end, GetRangeLimits(limit), snapshot, reverse, streamingMode);
	}
	virtual Future<FDBStandalone<RangeResultRef>> getRange(const KeyRange& keys,
	                                                       int limit,
	                                                       bool snapshot = false,
	                                                       bool reverse = false,
	                                                       FDBStreamingMode streamingMode = FDB_STREAMING_MODE_SERIAL) {
		return getRange(KeySelector(firstGreaterOrEqual(keys.begin), keys.arena()),
		                KeySelector(firstGreaterOrEqual(keys.end), keys.arena()),
		                limit,
		                snapshot,
		                reverse,
		                streamingMode);
	}
	virtual Future<FDBStandalone<RangeResultRef>> getRange(const KeyRange& keys,
	                                                       GetRangeLimits limits = GetRangeLimits(),
	                                                       bool snapshot = false,
	                                                       bool reverse = false,
	                                                       FDBStreamingMode streamingMode = FDB_STREAMING_MODE_SERIAL) {
		return getRange(KeySelector(firstGreaterOrEqual(keys.begin), keys.arena()),
		                KeySelector(firstGreaterOrEqual(keys.end), keys.arena()),
		                limits,
		                snapshot,
		                reverse,
		                streamingMode);
	}

	virtual Future<int64_t> getEstimatedRangeSizeBytes(const KeyRange& keys) = 0;
	virtual Future<FDBStandalone<VectorRef<KeyRef>>> getRangeSplitPoints(const KeyRange& range, int64_t chunkSize) = 0;

	virtual void addReadConflictRange(KeyRangeRef const& keys) = 0;
	virtual void addReadConflictKey(KeyRef const& key) = 0;

	virtual void setOption(FDBTransactionOption option, Optional<StringRef> value = Optional<StringRef>()) = 0;

	virtual Future<Void> onError(Error const& e) = 0;

	virtual void cancel() = 0;
	virtual void reset() = 0;
};

class Transaction : public ReadTransaction {
public:
	virtual void addWriteConflictRange(KeyRangeRef const& keys) = 0;
	virtual void addWriteConflictKey(KeyRef const& key) = 0;

	virtual void atomicOp(const KeyRef& key, const ValueRef& operand, FDBMutationType operationType) = 0;
	virtual void set(const KeyRef& key, const ValueRef& value) = 0;
	virtual void clear(const KeyRangeRef& range) = 0;
	virtual void clear(const KeyRef& key) = 0;

	virtual Future<Void> commit() = 0;
	virtual Version getCommittedVersion() = 0;
	virtual Future<int64_t> getApproximateSize() = 0;
	virtual Future<FDBStandalone<StringRef>> getVersionstamp() = 0;
};

class Database : public ReferenceCounted<Database> {
public:
	virtual ~Database(){};
	virtual Reference<Transaction> createTransaction() = 0;
	virtual void setDatabaseOption(FDBDatabaseOption option, Optional<StringRef> value = Optional<StringRef>()) = 0;
	virtual Future<int64_t> rebootWorker(const StringRef& address, bool check = false, int duration = 0) = 0;
	virtual Future<Void> forceRecoveryWithDataLoss(const StringRef& dcid) = 0;
	virtual Future<Void> createSnapshot(const StringRef& uid, const StringRef& snap_command) = 0;
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

	Reference<Database> createDatabase(std::string const& connFilename = "");

	bool evaluatePredicate(FDBErrorPredicate pred, Error const& e);
	int getAPIVersion() const;

private:
	static API* instance;

	API(int version);
	int version;
};
} // namespace FDB
#endif // FDB_FLOW_FDB_FLOW_H
