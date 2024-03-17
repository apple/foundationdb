/*
 * MultiVersionTransaction.actor.cpp
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

#include <chrono>
#include <filesystem>
#ifdef __unixish__
#include <fcntl.h>
#endif

#include "fdbclient/IClientApi.h"
#include "fdbclient/json_spirit/json_spirit_reader_template.h"
#include "fdbclient/json_spirit/json_spirit_writer_template.h"
#include "fdbclient/json_spirit/json_spirit_value.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/Trace.h"
#ifdef ADDRESS_SANITIZER
#include <sanitizer/lsan_interface.h>
#endif

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/GenericManagementAPI.actor.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbclient/MultiVersionAssignmentVars.h"
#include "fdbclient/ClientVersion.h"
#include "fdbclient/LocalClientAPI.h"
#include "fdbclient/VersionVector.h"
#include "fdbclient/buildid.h"

#include "flow/ThreadPrimitives.h"
#include "flow/network.h"
#include "flow/Platform.h"
#include "flow/ProtocolVersion.h"
#include "flow/UnitTest.h"
#include "flow/Trace.h"

#ifdef __unixish__
#include <fcntl.h>
#endif // __unixish__

#include "flow/actorcompiler.h" // This must be the last #include.

#ifdef FDBCLIENT_NATIVEAPI_ACTOR_H
#error "MVC should not depend on the Native API"
#endif

#define TIMER_START(i) auto start##i = std::chrono::steady_clock::now()

#define TIMER_END(i, desc)                                                                                             \
	auto end##i = std::chrono::steady_clock::now();                                                                    \
	std::cout << (externalClient ? "    " : "") << #desc << " took "                                                   \
	          << std::chrono::duration_cast<std::chrono::microseconds>(end##i - start##i).count() << " us.\n"

void throwIfError(FdbCApi::fdb_error_t e) {
	if (e) {
		throw Error(e);
	}
}

// DLTransaction
void DLTransaction::cancel() {
	api->transactionCancel(tr);
}

void DLTransaction::setVersion(Version v) {
	api->transactionSetReadVersion(tr, v);
}

ThreadFuture<Version> DLTransaction::getReadVersion() {
	FdbCApi::FDBFuture* f = api->transactionGetReadVersion(tr);

	return toThreadFuture<Version>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		int64_t version;
		FdbCApi::fdb_error_t error = api->futureGetInt64(f, &version);
		ASSERT(!error);
		return version;
	});
}

ThreadFuture<Optional<Value>> DLTransaction::get(const KeyRef& key, bool snapshot) {
	FdbCApi::FDBFuture* f = api->transactionGet(tr, key.begin(), key.size(), snapshot);

	return toThreadFuture<Optional<Value>>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		FdbCApi::fdb_bool_t present;
		const uint8_t* value;
		int valueLength;
		FdbCApi::fdb_error_t error = api->futureGetValue(f, &present, &value, &valueLength);
		ASSERT(!error);
		if (present) {
			// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
			return Optional<Value>(Value(ValueRef(value, valueLength), Arena()));
		} else {
			return Optional<Value>();
		}
	});
}

ThreadFuture<Key> DLTransaction::getKey(const KeySelectorRef& key, bool snapshot) {
	FdbCApi::FDBFuture* f =
	    api->transactionGetKey(tr, key.getKey().begin(), key.getKey().size(), key.orEqual, key.offset, snapshot);

	return toThreadFuture<Key>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const uint8_t* key;
		int keyLength;
		FdbCApi::fdb_error_t error = api->futureGetKey(f, &key, &keyLength);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Key(KeyRef(key, keyLength), Arena());
	});
}

ThreadFuture<RangeResult> DLTransaction::getRange(const KeySelectorRef& begin,
                                                  const KeySelectorRef& end,
                                                  int limit,
                                                  bool snapshot,
                                                  bool reverse) {
	return getRange(begin, end, GetRangeLimits(limit), snapshot, reverse);
}

ThreadFuture<RangeResult> DLTransaction::getRange(const KeySelectorRef& begin,
                                                  const KeySelectorRef& end,
                                                  GetRangeLimits limits,
                                                  bool snapshot,
                                                  bool reverse) {
	FdbCApi::FDBFuture* f = api->transactionGetRange(tr,
	                                                 begin.getKey().begin(),
	                                                 begin.getKey().size(),
	                                                 begin.orEqual,
	                                                 begin.offset,
	                                                 end.getKey().begin(),
	                                                 end.getKey().size(),
	                                                 end.orEqual,
	                                                 end.offset,
	                                                 limits.rows,
	                                                 limits.bytes,
	                                                 FDB_STREAMING_MODE_EXACT,
	                                                 0,
	                                                 snapshot,
	                                                 reverse);
	return toThreadFuture<RangeResult>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const FdbCApi::FDBKeyValue* kvs;
		int count;
		FdbCApi::fdb_bool_t more;
		FdbCApi::fdb_error_t error = api->futureGetKeyValueArray(f, &kvs, &count, &more);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return RangeResult(RangeResultRef(VectorRef<KeyValueRef>((KeyValueRef*)kvs, count), more), Arena());
	});
}

ThreadFuture<RangeResult> DLTransaction::getRange(const KeyRangeRef& keys, int limit, bool snapshot, bool reverse) {
	return getRange(
	    firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), GetRangeLimits(limit), snapshot, reverse);
}

ThreadFuture<RangeResult> DLTransaction::getRange(const KeyRangeRef& keys,
                                                  GetRangeLimits limits,
                                                  bool snapshot,
                                                  bool reverse) {
	return getRange(firstGreaterOrEqual(keys.begin), firstGreaterOrEqual(keys.end), limits, snapshot, reverse);
}

ThreadFuture<MappedRangeResult> DLTransaction::getMappedRange(const KeySelectorRef& begin,
                                                              const KeySelectorRef& end,
                                                              const StringRef& mapper,
                                                              GetRangeLimits limits,
                                                              bool snapshot,
                                                              bool reverse) {
	FdbCApi::FDBFuture* f = api->transactionGetMappedRange(tr,
	                                                       begin.getKey().begin(),
	                                                       begin.getKey().size(),
	                                                       begin.orEqual,
	                                                       begin.offset,
	                                                       end.getKey().begin(),
	                                                       end.getKey().size(),
	                                                       end.orEqual,
	                                                       end.offset,
	                                                       mapper.begin(),
	                                                       mapper.size(),
	                                                       limits.rows,
	                                                       limits.bytes,
	                                                       FDB_STREAMING_MODE_EXACT,
	                                                       0,
	                                                       snapshot,
	                                                       reverse);
	return toThreadFuture<MappedRangeResult>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const FdbCApi::FDBMappedKeyValue* kvms;
		int count;
		FdbCApi::fdb_bool_t more;
		FdbCApi::fdb_error_t error = api->futureGetMappedKeyValueArray(f, &kvms, &count, &more);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return MappedRangeResult(
		    MappedRangeResultRef(VectorRef<MappedKeyValueRef>((MappedKeyValueRef*)kvms, count), more), Arena());
	});
}

ThreadFuture<Standalone<VectorRef<const char*>>> DLTransaction::getAddressesForKey(const KeyRef& key) {
	FdbCApi::FDBFuture* f = api->transactionGetAddressesForKey(tr, key.begin(), key.size());

	return toThreadFuture<Standalone<VectorRef<const char*>>>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const char** addresses;
		int count;
		FdbCApi::fdb_error_t error = api->futureGetStringArray(f, &addresses, &count);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Standalone<VectorRef<const char*>>(VectorRef<const char*>(addresses, count), Arena());
	});
}

ThreadFuture<Standalone<StringRef>> DLTransaction::getVersionstamp() {
	if (!api->transactionGetVersionstamp) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->transactionGetVersionstamp(tr);

	return toThreadFuture<Standalone<StringRef>>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const uint8_t* str;
		int strLength;
		FdbCApi::fdb_error_t error = api->futureGetKey(f, &str, &strLength);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Standalone<StringRef>(StringRef(str, strLength), Arena());
	});
}

ThreadFuture<int64_t> DLTransaction::getEstimatedRangeSizeBytes(const KeyRangeRef& keys) {
	if (!api->transactionGetEstimatedRangeSizeBytes) {
		return unsupported_operation();
	}
	FdbCApi::FDBFuture* f = api->transactionGetEstimatedRangeSizeBytes(
	    tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size());

	return toThreadFuture<int64_t>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		int64_t sampledSize;
		FdbCApi::fdb_error_t error = api->futureGetInt64(f, &sampledSize);
		ASSERT(!error);
		return sampledSize;
	});
}

ThreadFuture<Standalone<VectorRef<KeyRef>>> DLTransaction::getRangeSplitPoints(const KeyRangeRef& range,
                                                                               int64_t chunkSize) {
	if (!api->transactionGetRangeSplitPoints) {
		return unsupported_operation();
	}
	FdbCApi::FDBFuture* f = api->transactionGetRangeSplitPoints(
	    tr, range.begin.begin(), range.begin.size(), range.end.begin(), range.end.size(), chunkSize);

	return toThreadFuture<Standalone<VectorRef<KeyRef>>>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const FdbCApi::FDBKey* splitKeys;
		int keysArrayLength;
		FdbCApi::fdb_error_t error = api->futureGetKeyArray(f, &splitKeys, &keysArrayLength);
		ASSERT(!error);
		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Standalone<VectorRef<KeyRef>>(VectorRef<KeyRef>((KeyRef*)splitKeys, keysArrayLength), Arena());
	});
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> DLTransaction::getBlobGranuleRanges(const KeyRangeRef& keyRange,
                                                                                     int rangeLimit) {
	if (!api->transactionGetBlobGranuleRanges) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->transactionGetBlobGranuleRanges(
	    tr, keyRange.begin.begin(), keyRange.begin.size(), keyRange.end.begin(), keyRange.end.size(), rangeLimit);
	return toThreadFuture<Standalone<VectorRef<KeyRangeRef>>>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const FdbCApi::FDBKeyRange* keyRanges;
		int keyRangesLength;
		FdbCApi::fdb_error_t error = api->futureGetKeyRangeArray(f, &keyRanges, &keyRangesLength);
		ASSERT(!error);
		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Standalone<VectorRef<KeyRangeRef>>(VectorRef<KeyRangeRef>((KeyRangeRef*)keyRanges, keyRangesLength),
		                                          Arena());
	});
}

ThreadResult<RangeResult> DLTransaction::readBlobGranules(const KeyRangeRef& keyRange,
                                                          Version beginVersion,
                                                          Optional<Version> readVersion,
                                                          ReadBlobGranuleContext granuleContext) {
	return unsupported_operation();
}

ThreadFuture<Standalone<VectorRef<BlobGranuleChunkRef>>> DLTransaction::readBlobGranulesStart(
    const KeyRangeRef& keyRange,
    Version beginVersion,
    Optional<Version> readVersion,
    Version* readVersionOut) {
	if (!api->transactionReadBlobGranulesStart) {
		return unsupported_operation();
	}

	int64_t rv = readVersion.present() ? readVersion.get() : latestVersion;

	FdbCApi::FDBFuture* f = api->transactionReadBlobGranulesStart(tr,
	                                                              keyRange.begin.begin(),
	                                                              keyRange.begin.size(),
	                                                              keyRange.end.begin(),
	                                                              keyRange.end.size(),
	                                                              beginVersion,
	                                                              rv,
	                                                              readVersionOut);

	return ThreadFuture<Standalone<VectorRef<BlobGranuleChunkRef>>>(
	    (ThreadSingleAssignmentVar<Standalone<VectorRef<BlobGranuleChunkRef>>>*)(f));
};

ThreadResult<RangeResult> DLTransaction::readBlobGranulesFinish(
    ThreadFuture<Standalone<VectorRef<BlobGranuleChunkRef>>> startFuture,
    const KeyRangeRef& keyRange,
    Version beginVersion,
    Version readVersion,
    ReadBlobGranuleContext granuleContext) {
	if (!api->transactionReadBlobGranulesFinish) {
		return unsupported_operation();
	}

	// convert back to fdb future for API
	FdbCApi::FDBFuture* f = (FdbCApi::FDBFuture*)(startFuture.extractPtr());

	// FIXME: better way to convert here?
	FdbCApi::FDBReadBlobGranuleContext context;
	context.userContext = granuleContext.userContext;
	context.start_load_f = granuleContext.start_load_f;
	context.get_load_f = granuleContext.get_load_f;
	context.free_load_f = granuleContext.free_load_f;
	context.debugNoMaterialize = granuleContext.debugNoMaterialize;
	context.granuleParallelism = granuleContext.granuleParallelism;

	FdbCApi::FDBResult* r = api->transactionReadBlobGranulesFinish(tr,
	                                                               f,
	                                                               keyRange.begin.begin(),
	                                                               keyRange.begin.size(),
	                                                               keyRange.end.begin(),
	                                                               keyRange.end.size(),
	                                                               beginVersion,
	                                                               readVersion,
	                                                               &context);

	return ThreadResult<RangeResult>((ThreadSingleAssignmentVar<RangeResult>*)(r));
};

ThreadFuture<Standalone<VectorRef<BlobGranuleSummaryRef>>>
DLTransaction::summarizeBlobGranules(const KeyRangeRef& keyRange, Optional<Version> summaryVersion, int rangeLimit) {
	if (!api->transactionSummarizeBlobGranules) {
		return unsupported_operation();
	}

	int64_t sv = summaryVersion.present() ? summaryVersion.get() : latestVersion;

	FdbCApi::FDBFuture* f = api->transactionSummarizeBlobGranules(
	    tr, keyRange.begin.begin(), keyRange.begin.size(), keyRange.end.begin(), keyRange.end.size(), sv, rangeLimit);

	return toThreadFuture<Standalone<VectorRef<BlobGranuleSummaryRef>>>(
	    api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		    const FdbCApi::FDBGranuleSummary* summaries;
		    int summariesLength;
		    FdbCApi::fdb_error_t error = api->futureGetGranuleSummaryArray(f, &summaries, &summariesLength);
		    ASSERT(!error);
		    // The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		    return Standalone<VectorRef<BlobGranuleSummaryRef>>(
		        VectorRef<BlobGranuleSummaryRef>((BlobGranuleSummaryRef*)summaries, summariesLength), Arena());
	    });
}

void DLTransaction::addReadConflictRange(const KeyRangeRef& keys) {
	throwIfError(api->transactionAddConflictRange(
	    tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size(), FDB_CONFLICT_RANGE_TYPE_READ));
}

void DLTransaction::atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) {
	api->transactionAtomicOp(
	    tr, key.begin(), key.size(), value.begin(), value.size(), static_cast<FDBMutationType>(operationType));
}

void DLTransaction::set(const KeyRef& key, const ValueRef& value) {
	api->transactionSet(tr, key.begin(), key.size(), value.begin(), value.size());
}

void DLTransaction::clear(const KeyRef& begin, const KeyRef& end) {
	api->transactionClearRange(tr, begin.begin(), begin.size(), end.begin(), end.size());
}

void DLTransaction::clear(const KeyRangeRef& range) {
	api->transactionClearRange(tr, range.begin.begin(), range.begin.size(), range.end.begin(), range.end.size());
}

void DLTransaction::clear(const KeyRef& key) {
	api->transactionClear(tr, key.begin(), key.size());
}

ThreadFuture<Void> DLTransaction::watch(const KeyRef& key) {
	FdbCApi::FDBFuture* f = api->transactionWatch(tr, key.begin(), key.size());

	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) { return Void(); });
}

void DLTransaction::addWriteConflictRange(const KeyRangeRef& keys) {
	throwIfError(api->transactionAddConflictRange(
	    tr, keys.begin.begin(), keys.begin.size(), keys.end.begin(), keys.end.size(), FDB_CONFLICT_RANGE_TYPE_WRITE));
}

ThreadFuture<Void> DLTransaction::commit() {
	FdbCApi::FDBFuture* f = api->transactionCommit(tr);

	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) { return Void(); });
}

Version DLTransaction::getCommittedVersion() {
	int64_t version;
	throwIfError(api->transactionGetCommittedVersion(tr, &version));
	return version;
}

ThreadFuture<double> DLTransaction::getTagThrottledDuration() {
	if (!api->transactionGetTagThrottledDuration) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->transactionGetTagThrottledDuration(tr);
	return toThreadFuture<double>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		double duration;
		FdbCApi::fdb_error_t error = api->futureGetDouble(f, &duration);
		ASSERT(!error);
		return duration;
	});
}

ThreadFuture<int64_t> DLTransaction::getTotalCost() {
	if (!api->transactionGetTotalCost) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->transactionGetTotalCost(tr);
	return toThreadFuture<int64_t>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		int64_t size = 0;
		FdbCApi::fdb_error_t error = api->futureGetInt64(f, &size);
		ASSERT(!error);
		return size;
	});
}

ThreadFuture<int64_t> DLTransaction::getApproximateSize() {
	if (!api->transactionGetApproximateSize) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->transactionGetApproximateSize(tr);
	return toThreadFuture<int64_t>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		int64_t size = 0;
		FdbCApi::fdb_error_t error = api->futureGetInt64(f, &size);
		ASSERT(!error);
		return size;
	});
}

void DLTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	throwIfError(api->transactionSetOption(tr,
	                                       static_cast<FDBTransactionOption>(option),
	                                       value.present() ? value.get().begin() : nullptr,
	                                       value.present() ? value.get().size() : 0));
}

ThreadFuture<Void> DLTransaction::onError(Error const& e) {
	FdbCApi::FDBFuture* f = api->transactionOnError(tr, e.code());

	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) { return Void(); });
}

void DLTransaction::reset() {
	api->transactionReset(tr);
}

void DLTransaction::debugTrace(BaseTraceEvent&& event) {
	event.detail("CommitResult", "Deferred logging unsupported").log();
};

void DLTransaction::debugPrint(std::string const& message) {
	fmt::print("[Deferred logging unsupported] {}\n", message);
}

ThreadFuture<VersionVector> DLTransaction::getVersionVector() {
	return VersionVector(); // not implemented
}

// DLTenant
Reference<ITransaction> DLTenant::createTransaction() {
	ASSERT(api->tenantCreateTransaction != nullptr);

	FdbCApi::FDBTransaction* tr;
	api->tenantCreateTransaction(tenant, &tr);
	return Reference<ITransaction>(new DLTransaction(api, tr));
}

ThreadFuture<int64_t> DLTenant::getId() {
	if (!api->tenantGetId) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->tenantGetId(tenant);

	return toThreadFuture<int64_t>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		int64_t res = 0;
		FdbCApi::fdb_error_t error = api->futureGetInt64(f, &res);
		ASSERT(!error);
		return res;
	});
}

ThreadFuture<Key> DLTenant::purgeBlobGranules(const KeyRangeRef& keyRange, Version purgeVersion, bool force) {
	if (!api->tenantPurgeBlobGranules) {
		return unsupported_operation();
	}
	FdbCApi::FDBFuture* f = api->tenantPurgeBlobGranules(tenant,
	                                                     keyRange.begin.begin(),
	                                                     keyRange.begin.size(),
	                                                     keyRange.end.begin(),
	                                                     keyRange.end.size(),
	                                                     purgeVersion,
	                                                     force);

	return toThreadFuture<Key>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const uint8_t* key;
		int keyLength;
		FdbCApi::fdb_error_t error = api->futureGetKey(f, &key, &keyLength);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Key(KeyRef(key, keyLength), Arena());
	});
}

ThreadFuture<Void> DLTenant::waitPurgeGranulesComplete(const KeyRef& purgeKey) {
	if (!api->tenantWaitPurgeGranulesComplete) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->tenantWaitPurgeGranulesComplete(tenant, purgeKey.begin(), purgeKey.size());
	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) { return Void(); });
}

ThreadFuture<bool> DLTenant::blobbifyRange(const KeyRangeRef& keyRange) {
	if (!api->tenantBlobbifyRange) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->tenantBlobbifyRange(
	    tenant, keyRange.begin.begin(), keyRange.begin.size(), keyRange.end.begin(), keyRange.end.size());

	return toThreadFuture<bool>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		FdbCApi::fdb_bool_t ret = false;
		ASSERT(!api->futureGetBool(f, &ret));
		return ret;
	});
}

ThreadFuture<bool> DLTenant::blobbifyRangeBlocking(const KeyRangeRef& keyRange) {
	if (!api->tenantBlobbifyRangeBlocking) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->tenantBlobbifyRangeBlocking(
	    tenant, keyRange.begin.begin(), keyRange.begin.size(), keyRange.end.begin(), keyRange.end.size());

	return toThreadFuture<bool>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		FdbCApi::fdb_bool_t ret = false;
		ASSERT(!api->futureGetBool(f, &ret));
		return ret;
	});
}

ThreadFuture<bool> DLTenant::unblobbifyRange(const KeyRangeRef& keyRange) {
	if (!api->tenantUnblobbifyRange) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->tenantUnblobbifyRange(
	    tenant, keyRange.begin.begin(), keyRange.begin.size(), keyRange.end.begin(), keyRange.end.size());

	return toThreadFuture<bool>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		FdbCApi::fdb_bool_t ret = false;
		ASSERT(!api->futureGetBool(f, &ret));
		return ret;
	});
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> DLTenant::listBlobbifiedRanges(const KeyRangeRef& keyRange,
                                                                                int rangeLimit) {
	if (!api->tenantListBlobbifiedRanges) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->tenantListBlobbifiedRanges(
	    tenant, keyRange.begin.begin(), keyRange.begin.size(), keyRange.end.begin(), keyRange.end.size(), rangeLimit);

	return toThreadFuture<Standalone<VectorRef<KeyRangeRef>>>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const FdbCApi::FDBKeyRange* keyRanges;
		int keyRangesLength;
		FdbCApi::fdb_error_t error = api->futureGetKeyRangeArray(f, &keyRanges, &keyRangesLength);
		ASSERT(!error);
		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed.
		return Standalone<VectorRef<KeyRangeRef>>(VectorRef<KeyRangeRef>((KeyRangeRef*)keyRanges, keyRangesLength),
		                                          Arena());
	});
}

ThreadFuture<Version> DLTenant::verifyBlobRange(const KeyRangeRef& keyRange, Optional<Version> version) {
	if (!api->tenantVerifyBlobRange) {
		return unsupported_operation();
	}

	Version readVersion = version.present() ? version.get() : latestVersion;

	FdbCApi::FDBFuture* f = api->tenantVerifyBlobRange(
	    tenant, keyRange.begin.begin(), keyRange.begin.size(), keyRange.end.begin(), keyRange.end.size(), readVersion);

	return toThreadFuture<Version>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		Version version = invalidVersion;
		ASSERT(!api->futureGetInt64(f, &version));
		return version;
	});
}

ThreadFuture<bool> DLTenant::flushBlobRange(const KeyRangeRef& keyRange, bool compact, Optional<Version> version) {
	if (!api->tenantFlushBlobRange) {
		return unsupported_operation();
	}

	Version readVersion = version.present() ? version.get() : latestVersion;

	FdbCApi::FDBFuture* f = api->tenantFlushBlobRange(tenant,
	                                                  keyRange.begin.begin(),
	                                                  keyRange.begin.size(),
	                                                  keyRange.end.begin(),
	                                                  keyRange.end.size(),
	                                                  compact,
	                                                  readVersion);

	return toThreadFuture<bool>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		FdbCApi::fdb_bool_t ret = false;
		ASSERT(!api->futureGetBool(f, &ret));
		return ret;
	});
}

// DLDatabase
DLDatabase::DLDatabase(Reference<FdbCApi> api, ThreadFuture<FdbCApi::FDBDatabase*> dbFuture) : api(api), db(nullptr) {
	addref();
	ready = mapThreadFuture<FdbCApi::FDBDatabase*, Void>(dbFuture, [this](ErrorOr<FdbCApi::FDBDatabase*> db) {
		if (db.isError()) {
			delref();
			return ErrorOr<Void>(db.getError());
		}

		this->db = db.get();
		delref();
		return ErrorOr<Void>(Void());
	});
}

ThreadFuture<Void> DLDatabase::onReady() {
	return ready;
}

Reference<ITenant> DLDatabase::openTenant(TenantNameRef tenantName) {
	if (!api->databaseOpenTenant) {
		throw unsupported_operation();
	}

	FdbCApi::FDBTenant* tenant;
	throwIfError(api->databaseOpenTenant(db, tenantName.begin(), tenantName.size(), &tenant));
	return makeReference<DLTenant>(api, tenant);
}

Reference<ITransaction> DLDatabase::createTransaction() {
	FdbCApi::FDBTransaction* tr;
	throwIfError(api->databaseCreateTransaction(db, &tr));
	return Reference<ITransaction>(new DLTransaction(api, tr));
}

void DLDatabase::setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	throwIfError(api->databaseSetOption(db,
	                                    static_cast<FDBDatabaseOption>(option),
	                                    value.present() ? value.get().begin() : nullptr,
	                                    value.present() ? value.get().size() : 0));
}

ThreadFuture<int64_t> DLDatabase::rebootWorker(const StringRef& address, bool check, int duration) {
	if (!api->databaseRebootWorker) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->databaseRebootWorker(db, address.begin(), address.size(), check, duration);
	return toThreadFuture<int64_t>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		int64_t res;
		FdbCApi::fdb_error_t error = api->futureGetInt64(f, &res);
		ASSERT(!error);
		return res;
	});
}

ThreadFuture<Void> DLDatabase::forceRecoveryWithDataLoss(const StringRef& dcid) {
	if (!api->databaseForceRecoveryWithDataLoss) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->databaseForceRecoveryWithDataLoss(db, dcid.begin(), dcid.size());
	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) { return Void(); });
}

ThreadFuture<Void> DLDatabase::createSnapshot(const StringRef& uid, const StringRef& snapshot_command) {
	if (!api->databaseCreateSnapshot) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f =
	    api->databaseCreateSnapshot(db, uid.begin(), uid.size(), snapshot_command.begin(), snapshot_command.size());
	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) { return Void(); });
}

ThreadFuture<DatabaseSharedState*> DLDatabase::createSharedState() {
	if (!api->databaseCreateSharedState) {
		return unsupported_operation();
	}
	FdbCApi::FDBFuture* f = api->databaseCreateSharedState(db);
	return toThreadFuture<DatabaseSharedState*>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		DatabaseSharedState* res;
		FdbCApi::fdb_error_t error = api->futureGetSharedState(f, &res);
		ASSERT(!error);
		return res;
	});
}

void DLDatabase::setSharedState(DatabaseSharedState* p) {
	if (!api->databaseSetSharedState) {
		throw unsupported_operation();
	}
	api->databaseSetSharedState(db, p);
}

// Get network thread busyness
double DLDatabase::getMainThreadBusyness() {
	if (api->databaseGetMainThreadBusyness != nullptr) {
		return api->databaseGetMainThreadBusyness(db);
	}

	return 0;
}

// Returns the protocol version reported by the coordinator this client is connected to
// If an expected version is given, the future won't return until the protocol version is different than expected
// Note: this will never return if the server is running a protocol from FDB 5.0 or older
ThreadFuture<ProtocolVersion> DLDatabase::getServerProtocol(Optional<ProtocolVersion> expectedVersion) {
	ASSERT(api->databaseGetServerProtocol != nullptr);

	uint64_t expected = expectedVersion.map(&ProtocolVersion::version).orDefault(0);
	FdbCApi::FDBFuture* f = api->databaseGetServerProtocol(db, expected);
	return toThreadFuture<ProtocolVersion>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		uint64_t pv;
		FdbCApi::fdb_error_t error = api->futureGetUInt64(f, &pv);
		ASSERT(!error);
		return ProtocolVersion(pv);
	});
}

ThreadFuture<Key> DLDatabase::purgeBlobGranules(const KeyRangeRef& keyRange, Version purgeVersion, bool force) {
	if (!api->databasePurgeBlobGranules) {
		return unsupported_operation();
	}
	FdbCApi::FDBFuture* f = api->databasePurgeBlobGranules(db,
	                                                       keyRange.begin.begin(),
	                                                       keyRange.begin.size(),
	                                                       keyRange.end.begin(),
	                                                       keyRange.end.size(),
	                                                       purgeVersion,
	                                                       force);

	return toThreadFuture<Key>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const uint8_t* key;
		int keyLength;
		FdbCApi::fdb_error_t error = api->futureGetKey(f, &key, &keyLength);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Key(KeyRef(key, keyLength), Arena());
	});
}

ThreadFuture<Void> DLDatabase::waitPurgeGranulesComplete(const KeyRef& purgeKey) {
	if (!api->databaseWaitPurgeGranulesComplete) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->databaseWaitPurgeGranulesComplete(db, purgeKey.begin(), purgeKey.size());
	return toThreadFuture<Void>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) { return Void(); });
}

ThreadFuture<bool> DLDatabase::blobbifyRange(const KeyRangeRef& keyRange) {
	if (!api->databaseBlobbifyRange) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->databaseBlobbifyRange(
	    db, keyRange.begin.begin(), keyRange.begin.size(), keyRange.end.begin(), keyRange.end.size());

	return toThreadFuture<bool>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		FdbCApi::fdb_bool_t ret = false;
		ASSERT(!api->futureGetBool(f, &ret));
		return ret;
	});
}

ThreadFuture<bool> DLDatabase::blobbifyRangeBlocking(const KeyRangeRef& keyRange) {
	if (!api->databaseBlobbifyRangeBlocking) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->databaseBlobbifyRangeBlocking(
	    db, keyRange.begin.begin(), keyRange.begin.size(), keyRange.end.begin(), keyRange.end.size());

	return toThreadFuture<bool>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		FdbCApi::fdb_bool_t ret = false;
		ASSERT(!api->futureGetBool(f, &ret));
		return ret;
	});
}

ThreadFuture<bool> DLDatabase::unblobbifyRange(const KeyRangeRef& keyRange) {
	if (!api->databaseUnblobbifyRange) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->databaseUnblobbifyRange(
	    db, keyRange.begin.begin(), keyRange.begin.size(), keyRange.end.begin(), keyRange.end.size());

	return toThreadFuture<bool>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		FdbCApi::fdb_bool_t ret = false;
		ASSERT(!api->futureGetBool(f, &ret));
		return ret;
	});
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> DLDatabase::listBlobbifiedRanges(const KeyRangeRef& keyRange,
                                                                                  int rangeLimit) {
	if (!api->databaseListBlobbifiedRanges) {
		return unsupported_operation();
	}

	FdbCApi::FDBFuture* f = api->databaseListBlobbifiedRanges(
	    db, keyRange.begin.begin(), keyRange.begin.size(), keyRange.end.begin(), keyRange.end.size(), rangeLimit);

	return toThreadFuture<Standalone<VectorRef<KeyRangeRef>>>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const FdbCApi::FDBKeyRange* keyRanges;
		int keyRangesLength;
		FdbCApi::fdb_error_t error = api->futureGetKeyRangeArray(f, &keyRanges, &keyRangesLength);
		ASSERT(!error);
		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed.
		return Standalone<VectorRef<KeyRangeRef>>(VectorRef<KeyRangeRef>((KeyRangeRef*)keyRanges, keyRangesLength),
		                                          Arena());
	});
}

ThreadFuture<Version> DLDatabase::verifyBlobRange(const KeyRangeRef& keyRange, Optional<Version> version) {
	if (!api->databaseVerifyBlobRange) {
		return unsupported_operation();
	}

	Version readVersion = version.present() ? version.get() : latestVersion;

	FdbCApi::FDBFuture* f = api->databaseVerifyBlobRange(
	    db, keyRange.begin.begin(), keyRange.begin.size(), keyRange.end.begin(), keyRange.end.size(), readVersion);

	return toThreadFuture<Version>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		Version version = invalidVersion;
		ASSERT(!api->futureGetInt64(f, &version));
		return version;
	});
}

ThreadFuture<bool> DLDatabase::flushBlobRange(const KeyRangeRef& keyRange, bool compact, Optional<Version> version) {
	if (!api->databaseFlushBlobRange) {
		return unsupported_operation();
	}

	Version readVersion = version.present() ? version.get() : latestVersion;

	FdbCApi::FDBFuture* f = api->databaseFlushBlobRange(db,
	                                                    keyRange.begin.begin(),
	                                                    keyRange.begin.size(),
	                                                    keyRange.end.begin(),
	                                                    keyRange.end.size(),
	                                                    compact,
	                                                    readVersion);

	return toThreadFuture<bool>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		FdbCApi::fdb_bool_t ret = false;
		ASSERT(!api->futureGetBool(f, &ret));
		return ret;
	});
}

ThreadFuture<Standalone<StringRef>> DLDatabase::getClientStatus() {
	if (!api->databaseGetClientStatus) {
		return unsupported_operation();
	}
	FdbCApi::FDBFuture* f = api->databaseGetClientStatus(db);
	return toThreadFuture<Standalone<StringRef>>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		const uint8_t* str;
		int strLength;
		FdbCApi::fdb_error_t error = api->futureGetKey(f, &str, &strLength);
		ASSERT(!error);

		// The memory for this is stored in the FDBFuture and is released when the future gets destroyed
		return Standalone<StringRef>(StringRef(str, strLength), Arena());
	});
}

// DLApi

// Loads the specified function from a dynamic library
//
// fp - The function pointer where the loaded function will be stored
// lib - The dynamic library where the function is loaded from
// libPath - The path of the dynamic library (used for logging)
// functionName - The function to load
// requireFunction - Determines the behavior if the function is not present. If true, an error is thrown. If false,
//                   the function pointer will be set to nullptr.
template <class T>
void loadClientFunction(T* fp, void* lib, std::string libPath, const char* functionName, bool requireFunction) {
	*(void**)(fp) = loadFunction(lib, functionName);
	if (*fp == nullptr && requireFunction) {
		TraceEvent(SevError, "ErrorLoadingFunction").detail("LibraryPath", libPath).detail("Function", functionName);
		throw api_function_missing();
	}
}

DLApi::DLApi(std::string fdbCPath, bool unlinkOnLoad)
  : fdbCPath(fdbCPath), api(new FdbCApi()), unlinkOnLoad(unlinkOnLoad), networkSetup(false) {}

// Loads client API functions (definitions are in FdbCApi struct)
void DLApi::init() {
	if (isLibraryLoaded(fdbCPath.c_str())) {
		throw external_client_already_loaded();
	}

	void* lib = loadLibrary(fdbCPath.c_str());
	if (lib == nullptr) {
		TraceEvent(SevError, "ErrorLoadingExternalClientLibrary").detail("LibraryPath", fdbCPath);
		throw platform_error();
	}
	if (unlinkOnLoad) {
		int err = unlink(fdbCPath.c_str());
		if (err) {
			TraceEvent(SevError, "ErrorUnlinkingTempClientLibraryFile").GetLastError().detail("LibraryPath", fdbCPath);
			throw platform_error();
		}
	}

	loadClientFunction(&api->selectApiVersion, lib, fdbCPath, "fdb_select_api_version_impl", headerVersion >= 0);
	loadClientFunction(&api->getClientVersion, lib, fdbCPath, "fdb_get_client_version", headerVersion >= 410);
	loadClientFunction(&api->useFutureProtocolVersion,
	                   lib,
	                   fdbCPath,
	                   "fdb_use_future_protocol_version",
	                   headerVersion >= ApiVersion::withFutureProtocolVersionApi().version());
	loadClientFunction(&api->setNetworkOption, lib, fdbCPath, "fdb_network_set_option", headerVersion >= 0);
	loadClientFunction(&api->setupNetwork, lib, fdbCPath, "fdb_setup_network", headerVersion >= 0);
	loadClientFunction(&api->runNetwork, lib, fdbCPath, "fdb_run_network", headerVersion >= 0);
	loadClientFunction(&api->stopNetwork, lib, fdbCPath, "fdb_stop_network", headerVersion >= 0);
	loadClientFunction(&api->createDatabase, lib, fdbCPath, "fdb_create_database", headerVersion >= 610);
	loadClientFunction(&api->createDatabaseFromConnectionString,
	                   lib,
	                   fdbCPath,
	                   "fdb_create_database_from_connection_string",
	                   headerVersion >= ApiVersion::withCreateDBFromConnString().version());

	loadClientFunction(&api->databaseOpenTenant, lib, fdbCPath, "fdb_database_open_tenant", headerVersion >= 710);
	loadClientFunction(
	    &api->databaseCreateSharedState, lib, fdbCPath, "fdb_database_create_shared_state", headerVersion >= 710);
	loadClientFunction(
	    &api->databaseSetSharedState, lib, fdbCPath, "fdb_database_set_shared_state", headerVersion >= 710);
	loadClientFunction(
	    &api->databaseCreateTransaction, lib, fdbCPath, "fdb_database_create_transaction", headerVersion >= 0);
	loadClientFunction(&api->databaseSetOption, lib, fdbCPath, "fdb_database_set_option", headerVersion >= 0);
	loadClientFunction(&api->databaseGetMainThreadBusyness,
	                   lib,
	                   fdbCPath,
	                   "fdb_database_get_main_thread_busyness",
	                   headerVersion >= 700);
	loadClientFunction(
	    &api->databaseGetServerProtocol, lib, fdbCPath, "fdb_database_get_server_protocol", headerVersion >= 700);
	loadClientFunction(&api->databaseDestroy, lib, fdbCPath, "fdb_database_destroy", headerVersion >= 0);
	loadClientFunction(&api->databaseRebootWorker, lib, fdbCPath, "fdb_database_reboot_worker", headerVersion >= 700);
	loadClientFunction(&api->databaseForceRecoveryWithDataLoss,
	                   lib,
	                   fdbCPath,
	                   "fdb_database_force_recovery_with_data_loss",
	                   headerVersion >= 700);
	loadClientFunction(
	    &api->databaseCreateSnapshot, lib, fdbCPath, "fdb_database_create_snapshot", headerVersion >= 700);
	loadClientFunction(
	    &api->databasePurgeBlobGranules, lib, fdbCPath, "fdb_database_purge_blob_granules", headerVersion >= 710);
	loadClientFunction(&api->databaseWaitPurgeGranulesComplete,
	                   lib,
	                   fdbCPath,
	                   "fdb_database_wait_purge_granules_complete",
	                   headerVersion >= 710);
	loadClientFunction(&api->databaseBlobbifyRange,
	                   lib,
	                   fdbCPath,
	                   "fdb_database_blobbify_range",
	                   headerVersion >= ApiVersion::withBlobRangeApi().version());
	loadClientFunction(&api->databaseBlobbifyRangeBlocking,
	                   lib,
	                   fdbCPath,
	                   "fdb_database_blobbify_range_blocking",
	                   headerVersion >= ApiVersion::withTenantBlobRangeApi().version());
	loadClientFunction(&api->databaseUnblobbifyRange,
	                   lib,
	                   fdbCPath,
	                   "fdb_database_unblobbify_range",
	                   headerVersion >= ApiVersion::withBlobRangeApi().version());
	loadClientFunction(&api->databaseListBlobbifiedRanges,
	                   lib,
	                   fdbCPath,
	                   "fdb_database_list_blobbified_ranges",
	                   headerVersion >= ApiVersion::withBlobRangeApi().version());
	loadClientFunction(&api->databaseVerifyBlobRange,
	                   lib,
	                   fdbCPath,
	                   "fdb_database_verify_blob_range",
	                   headerVersion >= ApiVersion::withBlobRangeApi().version());
	loadClientFunction(&api->databaseFlushBlobRange,
	                   lib,
	                   fdbCPath,
	                   "fdb_database_flush_blob_range",
	                   headerVersion >= ApiVersion::withTenantBlobRangeApi().version());
	loadClientFunction(&api->databaseGetClientStatus,
	                   lib,
	                   fdbCPath,
	                   "fdb_database_get_client_status",
	                   headerVersion >= ApiVersion::withGetClientStatus().version());
	loadClientFunction(
	    &api->tenantCreateTransaction, lib, fdbCPath, "fdb_tenant_create_transaction", headerVersion >= 710);
	loadClientFunction(&api->tenantPurgeBlobGranules,
	                   lib,
	                   fdbCPath,
	                   "fdb_tenant_purge_blob_granules",
	                   headerVersion >= ApiVersion::withTenantBlobRangeApi().version());
	loadClientFunction(&api->tenantWaitPurgeGranulesComplete,
	                   lib,
	                   fdbCPath,
	                   "fdb_tenant_wait_purge_granules_complete",
	                   headerVersion >= ApiVersion::withTenantBlobRangeApi().version());
	loadClientFunction(&api->tenantBlobbifyRange,
	                   lib,
	                   fdbCPath,
	                   "fdb_tenant_blobbify_range",
	                   headerVersion >= ApiVersion::withTenantBlobRangeApi().version());
	loadClientFunction(&api->tenantBlobbifyRangeBlocking,
	                   lib,
	                   fdbCPath,
	                   "fdb_tenant_blobbify_range_blocking",
	                   headerVersion >= ApiVersion::withTenantBlobRangeApi().version());
	loadClientFunction(&api->tenantUnblobbifyRange,
	                   lib,
	                   fdbCPath,
	                   "fdb_tenant_unblobbify_range",
	                   headerVersion >= ApiVersion::withTenantBlobRangeApi().version());
	loadClientFunction(&api->tenantListBlobbifiedRanges,
	                   lib,
	                   fdbCPath,
	                   "fdb_tenant_list_blobbified_ranges",
	                   headerVersion >= ApiVersion::withTenantBlobRangeApi().version());
	loadClientFunction(&api->tenantVerifyBlobRange,
	                   lib,
	                   fdbCPath,
	                   "fdb_tenant_verify_blob_range",
	                   headerVersion >= ApiVersion::withTenantBlobRangeApi().version());
	loadClientFunction(&api->tenantFlushBlobRange,
	                   lib,
	                   fdbCPath,
	                   "fdb_tenant_flush_blob_range",
	                   headerVersion >= ApiVersion::withTenantBlobRangeApi().version());
	loadClientFunction(&api->tenantGetId,
	                   lib,
	                   fdbCPath,
	                   "fdb_tenant_get_id",
	                   headerVersion >= ApiVersion::withTenantGetId().version());
	loadClientFunction(&api->tenantDestroy, lib, fdbCPath, "fdb_tenant_destroy", headerVersion >= 710);

	loadClientFunction(&api->transactionSetOption, lib, fdbCPath, "fdb_transaction_set_option", headerVersion >= 0);
	loadClientFunction(&api->transactionDestroy, lib, fdbCPath, "fdb_transaction_destroy", headerVersion >= 0);
	loadClientFunction(
	    &api->transactionSetReadVersion, lib, fdbCPath, "fdb_transaction_set_read_version", headerVersion >= 0);
	loadClientFunction(
	    &api->transactionGetReadVersion, lib, fdbCPath, "fdb_transaction_get_read_version", headerVersion >= 0);
	loadClientFunction(&api->transactionGet, lib, fdbCPath, "fdb_transaction_get", headerVersion >= 0);
	loadClientFunction(&api->transactionGetKey, lib, fdbCPath, "fdb_transaction_get_key", headerVersion >= 0);
	loadClientFunction(&api->transactionGetAddressesForKey,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_get_addresses_for_key",
	                   headerVersion >= 0);
	loadClientFunction(&api->transactionGetRange, lib, fdbCPath, "fdb_transaction_get_range", headerVersion >= 0);
	loadClientFunction(
	    &api->transactionGetMappedRange, lib, fdbCPath, "fdb_transaction_get_mapped_range", headerVersion >= 710);
	loadClientFunction(
	    &api->transactionGetVersionstamp, lib, fdbCPath, "fdb_transaction_get_versionstamp", headerVersion >= 410);
	loadClientFunction(&api->transactionSet, lib, fdbCPath, "fdb_transaction_set", headerVersion >= 0);
	loadClientFunction(&api->transactionClear, lib, fdbCPath, "fdb_transaction_clear", headerVersion >= 0);
	loadClientFunction(&api->transactionClearRange, lib, fdbCPath, "fdb_transaction_clear_range", headerVersion >= 0);
	loadClientFunction(&api->transactionAtomicOp, lib, fdbCPath, "fdb_transaction_atomic_op", headerVersion >= 0);
	loadClientFunction(&api->transactionCommit, lib, fdbCPath, "fdb_transaction_commit", headerVersion >= 0);
	loadClientFunction(&api->transactionGetCommittedVersion,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_get_committed_version",
	                   headerVersion >= 0);
	loadClientFunction(&api->transactionGetTagThrottledDuration,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_get_tag_throttled_duration",
	                   headerVersion >= ApiVersion::withGetTagThrottledDuration().version());
	loadClientFunction(&api->transactionGetTotalCost,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_get_total_cost",
	                   headerVersion >= ApiVersion::withGetTotalCost().version());
	loadClientFunction(&api->transactionGetApproximateSize,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_get_approximate_size",
	                   headerVersion >= 620);
	loadClientFunction(&api->transactionWatch, lib, fdbCPath, "fdb_transaction_watch", headerVersion >= 0);
	loadClientFunction(&api->transactionOnError, lib, fdbCPath, "fdb_transaction_on_error", headerVersion >= 0);
	loadClientFunction(&api->transactionReset, lib, fdbCPath, "fdb_transaction_reset", headerVersion >= 0);
	loadClientFunction(&api->transactionCancel, lib, fdbCPath, "fdb_transaction_cancel", headerVersion >= 0);
	loadClientFunction(
	    &api->transactionAddConflictRange, lib, fdbCPath, "fdb_transaction_add_conflict_range", headerVersion >= 0);
	loadClientFunction(&api->transactionGetEstimatedRangeSizeBytes,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_get_estimated_range_size_bytes",
	                   headerVersion >= 630);
	loadClientFunction(&api->transactionGetRangeSplitPoints,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_get_range_split_points",
	                   headerVersion >= 700);

	loadClientFunction(&api->transactionGetBlobGranuleRanges,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_get_blob_granule_ranges",
	                   headerVersion >= 710);
	loadClientFunction(
	    &api->transactionReadBlobGranules, lib, fdbCPath, "fdb_transaction_read_blob_granules", headerVersion >= 710);
	loadClientFunction(&api->transactionReadBlobGranulesStart,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_read_blob_granules_start",
	                   headerVersion >= ApiVersion::withBlobRangeApi().version());
	loadClientFunction(&api->transactionReadBlobGranulesFinish,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_read_blob_granules_finish",
	                   headerVersion >= ApiVersion::withBlobRangeApi().version());
	loadClientFunction(&api->transactionSummarizeBlobGranules,
	                   lib,
	                   fdbCPath,
	                   "fdb_transaction_summarize_blob_granules",
	                   headerVersion >= ApiVersion::withBlobRangeApi().version());
	loadClientFunction(&api->futureGetDouble,
	                   lib,
	                   fdbCPath,
	                   "fdb_future_get_double",
	                   headerVersion >= ApiVersion::withFutureGetDouble().version());
	loadClientFunction(&api->futureGetInt64,
	                   lib,
	                   fdbCPath,
	                   headerVersion >= 620 ? "fdb_future_get_int64" : "fdb_future_get_version",
	                   headerVersion >= 0);
	loadClientFunction(&api->futureGetBool,
	                   lib,
	                   fdbCPath,
	                   "fdb_future_get_bool",
	                   headerVersion >= ApiVersion::withFutureGetBool().version());
	loadClientFunction(&api->futureGetUInt64, lib, fdbCPath, "fdb_future_get_uint64", headerVersion >= 700);
	loadClientFunction(&api->futureGetError, lib, fdbCPath, "fdb_future_get_error", headerVersion >= 0);
	loadClientFunction(&api->futureGetKey, lib, fdbCPath, "fdb_future_get_key", headerVersion >= 0);
	loadClientFunction(&api->futureGetValue, lib, fdbCPath, "fdb_future_get_value", headerVersion >= 0);
	loadClientFunction(&api->futureGetStringArray, lib, fdbCPath, "fdb_future_get_string_array", headerVersion >= 0);
	loadClientFunction(
	    &api->futureGetKeyRangeArray, lib, fdbCPath, "fdb_future_get_keyrange_array", headerVersion >= 710);
	loadClientFunction(&api->futureGetKeyArray, lib, fdbCPath, "fdb_future_get_key_array", headerVersion >= 700);
	loadClientFunction(
	    &api->futureGetKeyValueArray, lib, fdbCPath, "fdb_future_get_keyvalue_array", headerVersion >= 0);
	loadClientFunction(
	    &api->futureGetMappedKeyValueArray, lib, fdbCPath, "fdb_future_get_mappedkeyvalue_array", headerVersion >= 710);
	loadClientFunction(&api->futureGetGranuleSummaryArray,
	                   lib,
	                   fdbCPath,
	                   "fdb_future_get_granule_summary_array",
	                   headerVersion >= ApiVersion::withBlobRangeApi().version());
	loadClientFunction(&api->futureGetSharedState, lib, fdbCPath, "fdb_future_get_shared_state", headerVersion >= 710);
	loadClientFunction(&api->futureSetCallback, lib, fdbCPath, "fdb_future_set_callback", headerVersion >= 0);
	loadClientFunction(&api->futureCancel, lib, fdbCPath, "fdb_future_cancel", headerVersion >= 0);
	loadClientFunction(&api->futureDestroy, lib, fdbCPath, "fdb_future_destroy", headerVersion >= 0);

	loadClientFunction(
	    &api->resultGetKeyValueArray, lib, fdbCPath, "fdb_result_get_keyvalue_array", headerVersion >= 710);
	loadClientFunction(&api->resultDestroy, lib, fdbCPath, "fdb_result_destroy", headerVersion >= 710);

	loadClientFunction(&api->futureGetDatabase, lib, fdbCPath, "fdb_future_get_database", headerVersion < 610);
	loadClientFunction(&api->createCluster, lib, fdbCPath, "fdb_create_cluster", headerVersion < 610);
	loadClientFunction(&api->clusterCreateDatabase, lib, fdbCPath, "fdb_cluster_create_database", headerVersion < 610);
	loadClientFunction(&api->clusterDestroy, lib, fdbCPath, "fdb_cluster_destroy", headerVersion < 610);
	loadClientFunction(&api->futureGetCluster, lib, fdbCPath, "fdb_future_get_cluster", headerVersion < 610);
	loadClientFunction(&api->retrieveBuildID, lib, fdbCPath, "fdb_retrieve_build_id", headerVersion >= 720);
}

void DLApi::selectApiVersion(int apiVersion) {
	// External clients must support at least this version
	// Versions newer than what we understand are rejected in the C bindings
	headerVersion = std::max(apiVersion, 400);
	// auto externalClient = false;
	// TIMER_START(1);
	init();
	// TIMER_END(1, DLApi::selectApiVersion::init());

	// TIMER_START(2);
	throwIfError(api->selectApiVersion(apiVersion, headerVersion));
	throwIfError(api->setNetworkOption(static_cast<FDBNetworkOption>(FDBNetworkOptions::EXTERNAL_CLIENT), nullptr, 0));
	// TIMER_END(2, DLApi::selectApiVersion::throwIfError);
}

const char* DLApi::getClientVersion() {
	if (!api->getClientVersion) {
		return "unknown";
	}

	return api->getClientVersion();
}

void DLApi::useFutureProtocolVersion() {
	if (!api->useFutureProtocolVersion) {
		return;
	}

	api->useFutureProtocolVersion();
}

void DLApi::setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	throwIfError(api->setNetworkOption(static_cast<FDBNetworkOption>(option),
	                                   value.present() ? value.get().begin() : nullptr,
	                                   value.present() ? value.get().size() : 0));
}

void DLApi::setupNetwork() {
	networkSetup = true;
	throwIfError(api->setupNetwork());
}

void DLApi::runNetwork() {
	auto e = api->runNetwork();

	for (auto& hook : threadCompletionHooks) {
		try {
			hook.first(hook.second);
		} catch (Error& e) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(e);
		} catch (std::exception& e) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(unknown_error()).detail("RootException", e.what());
		} catch (...) {
			TraceEvent(SevError, "NetworkShutdownHookError").error(unknown_error());
		}
	}

	throwIfError(e);
}

void DLApi::stopNetwork() {
	if (networkSetup) {
		throwIfError(api->stopNetwork());
	}
}

const uint8_t* DLApi::retrieveBuildID() {
	return api->retrieveBuildID();
}

Reference<IDatabase> DLApi::createDatabase609(const char* clusterFilePath) {
	FdbCApi::FDBFuture* f = api->createCluster(clusterFilePath);

	auto clusterFuture = toThreadFuture<FdbCApi::FDBCluster*>(api, f, [](FdbCApi::FDBFuture* f, FdbCApi* api) {
		FdbCApi::FDBCluster* cluster;
		api->futureGetCluster(f, &cluster);
		return cluster;
	});

	Reference<FdbCApi> innerApi = api;
	auto dbFuture = flatMapThreadFuture<FdbCApi::FDBCluster*, FdbCApi::FDBDatabase*>(
	    clusterFuture, [innerApi](ErrorOr<FdbCApi::FDBCluster*> cluster) {
		    if (cluster.isError()) {
			    return ErrorOr<ThreadFuture<FdbCApi::FDBDatabase*>>(cluster.getError());
		    }

		    auto innerDbFuture =
		        toThreadFuture<FdbCApi::FDBDatabase*>(innerApi,
		                                              innerApi->clusterCreateDatabase(cluster.get(), (uint8_t*)"DB", 2),
		                                              [](FdbCApi::FDBFuture* f, FdbCApi* api) {
			                                              FdbCApi::FDBDatabase* db;
			                                              api->futureGetDatabase(f, &db);
			                                              return db;
		                                              });

		    return ErrorOr<ThreadFuture<FdbCApi::FDBDatabase*>>(
		        mapThreadFuture<FdbCApi::FDBDatabase*, FdbCApi::FDBDatabase*>(
		            innerDbFuture, [cluster, innerApi](ErrorOr<FdbCApi::FDBDatabase*> db) {
			            innerApi->clusterDestroy(cluster.get());
			            return db;
		            }));
	    });

	return makeReference<DLDatabase>(api, dbFuture);
}

Reference<IDatabase> DLApi::createDatabase(const char* clusterFilePath) {
	if (headerVersion >= 610) {
		FdbCApi::FDBDatabase* db;
		throwIfError(api->createDatabase(clusterFilePath, &db));
		return Reference<IDatabase>(new DLDatabase(api, db));
	} else {
		return DLApi::createDatabase609(clusterFilePath);
	}
}

Reference<IDatabase> DLApi::createDatabaseFromConnectionString(const char* connectionString) {
	if (api->createDatabaseFromConnectionString == nullptr) {
		throw unsupported_operation();
	}

	FdbCApi::FDBDatabase* db;
	throwIfError(api->createDatabaseFromConnectionString(connectionString, &db));
	return Reference<IDatabase>(new DLDatabase(api, db));
}

void DLApi::addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) {
	MutexHolder holder(lock);
	threadCompletionHooks.emplace_back(hook, hookParameter);
}

// MultiVersionTransaction
MultiVersionTransaction::MultiVersionTransaction(Reference<MultiVersionDatabase> db,
                                                 Optional<Reference<MultiVersionTenant>> tenant,
                                                 UniqueOrderedOptionList<FDBTransactionOptions> defaultOptions)
  : db(db), tenant(tenant), startTime(timer_monotonic()), timeoutTsav(new ThreadSingleAssignmentVar<Void>()) {
	setDefaultOptions(defaultOptions);
	updateTransaction(false);
}

void MultiVersionTransaction::setDefaultOptions(UniqueOrderedOptionList<FDBTransactionOptions> options) {
	MutexHolder holder(db->dbState->optionLock);
	std::copy(options.begin(), options.end(), std::back_inserter(persistentOptions));
}

void MultiVersionTransaction::updateTransaction(bool setPersistentOptions) {
	TransactionInfo newTr;
	if (tenant.present()) {
		ASSERT(tenant.get());
		auto currentTenant = tenant.get()->tenantState->tenantVar->get();
		if (currentTenant.value) {
			newTr.transaction = currentTenant.value->createTransaction();
		}
		newTr.onChange = currentTenant.onChange;
	} else {
		auto currentDb = db->dbState->dbVar->get();
		if (currentDb.value) {
			newTr.transaction = currentDb.value->createTransaction();
		}
		newTr.onChange = currentDb.onChange;
	}

	// When called from the constructor or from reset(), all persistent options are database options and therefore
	// already set on newTr.transaction if it got created successfully. If newTr.transaction could not be created (i.e.,
	// because no database with a matching version is present), the local timeout set in setTimeout() applies, so we
	// need to set it.
	if (setPersistentOptions || !newTr.transaction) {
		Optional<StringRef> timeout;
		for (auto const& option : persistentOptions) {
			if (option.first == FDBTransactionOptions::TIMEOUT) {
				timeout = option.second.castTo<StringRef>();
			} else if (newTr.transaction) {
				newTr.transaction->setOption(option.first, option.second.castTo<StringRef>());
			}
		}
		if (newTr.transaction) {
			for (auto const& option : sensitivePersistentOptions) {
				newTr.transaction->setOption(option.first, option.second.castTo<StringRef>());
			}
		}

		// Setting a timeout can immediately cause a transaction to fail. The only timeout
		// that matters is the one most recently set, so we ignore any earlier set timeouts
		// that might inadvertently fail the transaction.
		if (timeout.present()) {
			if (newTr.transaction) {
				newTr.transaction->setOption(FDBTransactionOptions::TIMEOUT, timeout);
				resetTimeout();
			} else {
				setTimeout(timeout);
			}
		}
	}
	lock.enter();
	transaction = newTr;
	lock.leave();
}

MultiVersionTransaction::TransactionInfo MultiVersionTransaction::getTransaction() {
	lock.enter();
	MultiVersionTransaction::TransactionInfo currentTr(transaction);
	lock.leave();

	return currentTr;
}

void MultiVersionTransaction::cancel() {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->cancel();
	}
}

void MultiVersionTransaction::setVersion(Version v) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->setVersion(v);
	}
}

template <class T, class... Args>
ThreadFuture<T> MultiVersionTransaction::executeOperation(ThreadFuture<T> (ITransaction::*func)(Args...),
                                                          Args&&... args) {
	auto tr = getTransaction();
	if (tr.transaction) {
		auto f = (tr.transaction.getPtr()->*func)(std::forward<Args>(args)...);
		return abortableFuture(f, tr.onChange);
	}

	// If database initialization failed, return the initialization error
	auto dbError = db->dbState->getInitializationError();
	if (dbError.isError()) {
		return ThreadFuture<T>(dbError.getError());
	}

	// Wait for the database to be initialized
	return abortableFuture(makeTimeout<T>(), tr.onChange);
}

ThreadFuture<Version> MultiVersionTransaction::getReadVersion() {
	return executeOperation(&ITransaction::getReadVersion);
}

ThreadFuture<Optional<Value>> MultiVersionTransaction::get(const KeyRef& key, bool snapshot) {
	return executeOperation(&ITransaction::get, key, std::forward<bool>(snapshot));
}

ThreadFuture<Key> MultiVersionTransaction::getKey(const KeySelectorRef& key, bool snapshot) {
	return executeOperation(&ITransaction::getKey, key, std::forward<bool>(snapshot));
}

ThreadFuture<RangeResult> MultiVersionTransaction::getRange(const KeySelectorRef& begin,
                                                            const KeySelectorRef& end,
                                                            int limit,
                                                            bool snapshot,
                                                            bool reverse) {
	return executeOperation<RangeResult>(&ITransaction::getRange,
	                                     begin,
	                                     end,
	                                     std::forward<int>(limit),
	                                     std::forward<bool>(snapshot),
	                                     std::forward<bool>(reverse));
}

ThreadFuture<RangeResult> MultiVersionTransaction::getRange(const KeySelectorRef& begin,
                                                            const KeySelectorRef& end,
                                                            GetRangeLimits limits,
                                                            bool snapshot,
                                                            bool reverse) {
	return executeOperation<RangeResult>(&ITransaction::getRange,
	                                     begin,
	                                     end,
	                                     std::forward<GetRangeLimits>(limits),
	                                     std::forward<bool>(snapshot),
	                                     std::forward<bool>(reverse));
}

ThreadFuture<RangeResult> MultiVersionTransaction::getRange(const KeyRangeRef& keys,
                                                            int limit,
                                                            bool snapshot,
                                                            bool reverse) {
	return executeOperation<RangeResult>(&ITransaction::getRange,
	                                     keys,
	                                     std::forward<int>(limit),
	                                     std::forward<bool>(snapshot),
	                                     std::forward<bool>(reverse));
}

ThreadFuture<RangeResult> MultiVersionTransaction::getRange(const KeyRangeRef& keys,
                                                            GetRangeLimits limits,
                                                            bool snapshot,
                                                            bool reverse) {
	return executeOperation<RangeResult>(&ITransaction::getRange,
	                                     keys,
	                                     std::forward<GetRangeLimits>(limits),
	                                     std::forward<bool>(snapshot),
	                                     std::forward<bool>(reverse));
}

ThreadFuture<MappedRangeResult> MultiVersionTransaction::getMappedRange(const KeySelectorRef& begin,
                                                                        const KeySelectorRef& end,
                                                                        const StringRef& mapper,
                                                                        GetRangeLimits limits,
                                                                        bool snapshot,
                                                                        bool reverse) {
	return executeOperation(&ITransaction::getMappedRange,
	                        begin,
	                        end,
	                        mapper,
	                        std::forward<GetRangeLimits>(limits),
	                        std::forward<bool>(snapshot),
	                        std::forward<bool>(reverse));
}

ThreadFuture<Standalone<StringRef>> MultiVersionTransaction::getVersionstamp() {
	return executeOperation(&ITransaction::getVersionstamp);
}

ThreadFuture<Standalone<VectorRef<const char*>>> MultiVersionTransaction::getAddressesForKey(const KeyRef& key) {
	return executeOperation(&ITransaction::getAddressesForKey, key);
}

void MultiVersionTransaction::addReadConflictRange(const KeyRangeRef& keys) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->addReadConflictRange(keys);
	}
}

ThreadFuture<int64_t> MultiVersionTransaction::getEstimatedRangeSizeBytes(const KeyRangeRef& keys) {
	return executeOperation(&ITransaction::getEstimatedRangeSizeBytes, keys);
}

ThreadFuture<Standalone<VectorRef<KeyRef>>> MultiVersionTransaction::getRangeSplitPoints(const KeyRangeRef& range,
                                                                                         int64_t chunkSize) {
	return executeOperation(&ITransaction::getRangeSplitPoints, range, std::forward<int64_t>(chunkSize));
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> MultiVersionTransaction::getBlobGranuleRanges(
    const KeyRangeRef& keyRange,
    int rangeLimit) {
	return executeOperation(&ITransaction::getBlobGranuleRanges, keyRange, std::forward<int>(rangeLimit));
}

ThreadResult<RangeResult> MultiVersionTransaction::readBlobGranules(const KeyRangeRef& keyRange,
                                                                    Version beginVersion,
                                                                    Optional<Version> readVersion,
                                                                    ReadBlobGranuleContext granuleContext) {
	// FIXME: prevent from calling this from another main thread?
	auto tr = getTransaction();
	if (tr.transaction) {
		Version readVersionOut;
		auto f = tr.transaction->readBlobGranulesStart(keyRange, beginVersion, readVersion, &readVersionOut);
		auto abortableF = abortableFuture(f, tr.onChange);
		abortableF.blockUntilReadyCheckOnMainThread();
		if (abortableF.isError()) {
			return ThreadResult<RangeResult>(abortableF.getError());
		}
		if (granuleContext.debugNoMaterialize) {
			return ThreadResult<RangeResult>(blob_granule_not_materialized());
		}
		return tr.transaction->readBlobGranulesFinish(
		    abortableF, keyRange, beginVersion, readVersionOut, granuleContext);
	} else {
		return abortableTimeoutResult<RangeResult>(tr.onChange);
	}
}

ThreadFuture<Standalone<VectorRef<BlobGranuleChunkRef>>> MultiVersionTransaction::readBlobGranulesStart(
    const KeyRangeRef& keyRange,
    Version beginVersion,
    Optional<Version> readVersion,
    Version* readVersionOut) {
	return executeOperation(&ITransaction::readBlobGranulesStart,
	                        keyRange,
	                        std::forward<Version>(beginVersion),
	                        std::forward<Optional<Version>>(readVersion),
	                        std::forward<Version*>(readVersionOut));
}

ThreadResult<RangeResult> MultiVersionTransaction::readBlobGranulesFinish(
    ThreadFuture<Standalone<VectorRef<BlobGranuleChunkRef>>> startFuture,
    const KeyRangeRef& keyRange,
    Version beginVersion,
    Version readVersion,
    ReadBlobGranuleContext granuleContext) {
	// can't call this directly
	return ThreadResult<RangeResult>(unsupported_operation());
}

ThreadFuture<Standalone<VectorRef<BlobGranuleSummaryRef>>> MultiVersionTransaction::summarizeBlobGranules(
    const KeyRangeRef& keyRange,
    Optional<Version> summaryVersion,
    int rangeLimit) {
	return executeOperation(&ITransaction::summarizeBlobGranules,
	                        keyRange,
	                        std::forward<Optional<Version>>(summaryVersion),
	                        std::forward<int>(rangeLimit));
}

void MultiVersionTransaction::atomicOp(const KeyRef& key, const ValueRef& value, uint32_t operationType) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->atomicOp(key, value, operationType);
	}
}

void MultiVersionTransaction::set(const KeyRef& key, const ValueRef& value) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->set(key, value);
	}
}

void MultiVersionTransaction::clear(const KeyRef& begin, const KeyRef& end) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->clear(begin, end);
	}
}

void MultiVersionTransaction::clear(const KeyRangeRef& range) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->clear(range);
	}
}

void MultiVersionTransaction::clear(const KeyRef& key) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->clear(key);
	}
}

ThreadFuture<Void> MultiVersionTransaction::watch(const KeyRef& key) {
	return executeOperation(&ITransaction::watch, key);
}

void MultiVersionTransaction::addWriteConflictRange(const KeyRangeRef& keys) {
	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->addWriteConflictRange(keys);
	}
}

ThreadFuture<Void> MultiVersionTransaction::commit() {
	return executeOperation(&ITransaction::commit);
}

Version MultiVersionTransaction::getCommittedVersion() {
	auto tr = getTransaction();
	if (tr.transaction) {
		return tr.transaction->getCommittedVersion();
	}
	return invalidVersion;
}

ThreadFuture<VersionVector> MultiVersionTransaction::getVersionVector() {
	auto tr = getTransaction();
	if (tr.transaction) {
		return tr.transaction->getVersionVector();
	}

	return VersionVector();
}

ThreadFuture<SpanContext> MultiVersionTransaction::getSpanContext() {
	auto tr = getTransaction();
	if (tr.transaction) {
		return tr.transaction->getSpanContext();
	}

	return SpanContext();
}

ThreadFuture<double> MultiVersionTransaction::getTagThrottledDuration() {
	auto tr = getTransaction();
	auto f = tr.transaction ? tr.transaction->getTagThrottledDuration() : makeTimeout<double>();
	return abortableFuture(f, tr.onChange);
}

ThreadFuture<int64_t> MultiVersionTransaction::getTotalCost() {
	return executeOperation(&ITransaction::getTotalCost);
}

ThreadFuture<int64_t> MultiVersionTransaction::getApproximateSize() {
	return executeOperation(&ITransaction::getApproximateSize);
}

void MultiVersionTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	auto itr = FDBTransactionOptions::optionInfo.find(option);
	if (itr == FDBTransactionOptions::optionInfo.end()) {
		TraceEvent("UnknownTransactionOption").detail("Option", option);
		throw invalid_option();
	}

	if (MultiVersionApi::api->getApiVersion().hasPersistentOptions() && itr->second.persistent) {
		if (itr->second.sensitive)
			sensitivePersistentOptions.emplace_back(option, value.castTo<WipedString>());
		else
			persistentOptions.emplace_back(option, value.castTo<Standalone<StringRef>>());
	}

	auto tr = getTransaction();
	if (tr.transaction) {
		tr.transaction->setOption(option, value);
	} else if (itr->first == FDBTransactionOptions::TIMEOUT) {
		setTimeout(value);
	}
}

ThreadFuture<Void> MultiVersionTransaction::onError(Error const& e) {
	if (e.code() == error_code_cluster_version_changed) {
		updateTransaction(true);
		return ThreadFuture<Void>(Void());
	} else {
		auto f = executeOperation(&ITransaction::onError, e);
		return flatMapThreadFuture<Void, Void>(f, [this](ErrorOr<Void> ready) {
			if (ready.isError() && ready.getError().code() == error_code_cluster_version_changed) {
				// In case of a cluster version change, upgrade (or downgrade) the transaction
				// and let it to be retried independently of the original error
				updateTransaction(true);
				return ErrorOr<ThreadFuture<Void>>(Void());
			}
			// In all other cases forward the result of the inner onError call
			if (ready.isError()) {
				return ErrorOr<ThreadFuture<Void>>(ready.getError());
			} else {
				return ErrorOr<ThreadFuture<Void>>(Void());
			}
		});
	}
}

Optional<TenantName> MultiVersionTransaction::getTenant() {
	if (tenant.present()) {
		return tenant.get()->tenantState->tenantName;
	} else {
		return Optional<TenantName>();
	}
}

// Waits for the specified duration and signals the assignment variable with a timed out error
// This will be canceled if a new timeout is set, in which case the tsav will not be signaled.
ACTOR Future<Void> timeoutImpl(Reference<ThreadSingleAssignmentVar<Void>> tsav, double duration) {
	state double endTime = now() + duration;
	while (now() < endTime) {
		wait(delayUntil(std::min(endTime + 0.0001, now() + CLIENT_KNOBS->TRANSACTION_TIMEOUT_DELAY_INTERVAL)));
	}

	tsav->trySendError(transaction_timed_out());
	return Void();
}

namespace {

void validateOptionValuePresent(Optional<StringRef> value) {
	if (!value.present()) {
		throw invalid_option_value();
	}
}

int64_t extractIntOption(Optional<StringRef> value, int64_t minValue, int64_t maxValue) {
	validateOptionValuePresent(value);
	if (value.get().size() != 8) {
		throw invalid_option_value();
	}

	int64_t passed = *((int64_t*)(value.get().begin()));
	if (passed > maxValue || passed < minValue) {
		throw invalid_option_value();
	}

	return passed;
}

} // namespace

// Configure a timeout based on the options set for this transaction. This timeout only applies
// if we don't have an underlying database object to connect with.
void MultiVersionTransaction::setTimeout(Optional<StringRef> value) {
	double timeoutDuration = extractIntOption(value, 0, std::numeric_limits<int>::max()) / 1000.0;

	ThreadFuture<Void> prevTimeout;
	double transactionStartTime = startTime;

	{ // lock scope
		ThreadSpinLockHolder holder(timeoutLock);
		prevTimeout = currentTimeout;

		if (timeoutDuration > 0) {
			Reference<ThreadSingleAssignmentVar<Void>> tsav = timeoutTsav;
			ThreadFuture<Void> newTimeout = onMainThread([transactionStartTime, tsav, timeoutDuration]() {
				return timeoutImpl(tsav, timeoutDuration - std::max(0.0, now() - transactionStartTime));
			});
			currentTimeout = newTimeout;
		} else {
			currentTimeout = ThreadFuture<Void>();
		}
	}

	// Cancel the previous timeout now that we have a new one. This means that changing the timeout
	// affects in-flight operations, which is consistent with the behavior in RYW.
	if (prevTimeout.isValid()) {
		prevTimeout.cancel();
	}
}

// Removes timeout if set. This timeout only applies if we don't have an underlying database object to connect with.
void MultiVersionTransaction::resetTimeout() {
	ThreadFuture<Void> prevTimeout;
	{ // lock scope
		ThreadSpinLockHolder holder(timeoutLock);
		prevTimeout = currentTimeout;
		currentTimeout = ThreadFuture<Void>();
	}
	if (prevTimeout.isValid()) {
		prevTimeout.cancel();
	}
}
// Creates a ThreadFuture<T> that will signal an error if the transaction times out.
template <class T>
ThreadFuture<T> MultiVersionTransaction::makeTimeout() {
	ThreadFuture<Void> f;

	{ // lock scope
		ThreadSpinLockHolder holder(timeoutLock);

		// Our ThreadFuture holds a reference to this TSAV,
		// but the ThreadFuture does not increment the ref count
		timeoutTsav->addref();
		f = ThreadFuture<Void>(timeoutTsav.getPtr());
	}

	// When our timeoutTsav gets set, map it to the appropriate type
	return mapThreadFuture<Void, T>(f, [](ErrorOr<Void> v) {
		ASSERT(v.isError());
		return ErrorOr<T>(v.getError());
	});
}

template <class T>
ThreadResult<T> MultiVersionTransaction::abortableTimeoutResult(ThreadFuture<Void> abortSignal) {
	// If database initialization failed, return the initialization error
	auto dbError = db->dbState->getInitializationError();
	if (dbError.isError()) {
		return ThreadResult<T>(dbError.getError());
	}
	ThreadFuture<T> abortable = abortableFuture(makeTimeout<T>(), abortSignal);
	abortable.blockUntilReadyCheckOnMainThread();
	return ThreadResult<T>((ThreadSingleAssignmentVar<T>*)abortable.extractPtr());
}

void MultiVersionTransaction::reset() {
	persistentOptions.clear();
	sensitivePersistentOptions.clear();

	// Reset the timeout state
	Reference<ThreadSingleAssignmentVar<Void>> prevTimeoutTsav;
	ThreadFuture<Void> prevTimeout;
	startTime = timer_monotonic();

	{ // lock scope
		ThreadSpinLockHolder holder(timeoutLock);

		prevTimeoutTsav = timeoutTsav;
		timeoutTsav = makeReference<ThreadSingleAssignmentVar<Void>>();

		prevTimeout = currentTimeout;
		currentTimeout = ThreadFuture<Void>();
	}

	// Cancel any outstanding operations if they don't have an underlying transaction object to cancel them
	prevTimeoutTsav->trySendError(transaction_cancelled());
	if (prevTimeout.isValid()) {
		prevTimeout.cancel();
	}

	setDefaultOptions(db->dbState->transactionDefaultOptions);
	updateTransaction(false);
}

MultiVersionTransaction::~MultiVersionTransaction() {
	timeoutTsav->trySendError(transaction_cancelled());
	if (currentTimeout.isValid()) {
		currentTimeout.cancel();
	}
}

bool MultiVersionTransaction::isValid() {
	auto tr = getTransaction();
	return tr.transaction.isValid();
}

void MultiVersionTransaction::debugTrace(BaseTraceEvent&& event) {
	auto tr = getTransaction();
	tr.transaction->debugTrace(std::move(event));
}

void MultiVersionTransaction::debugPrint(std::string const& message) {
	auto tr = getTransaction();
	tr.transaction->debugPrint(message);
}

// MultiVersionTenant
MultiVersionTenant::MultiVersionTenant(Reference<MultiVersionDatabase> db, TenantNameRef tenantName)
  : tenantState(makeReference<TenantState>(db, tenantName)) {}

MultiVersionTenant::~MultiVersionTenant() {
	tenantState->close();
}

Reference<ITransaction> MultiVersionTenant::createTransaction() {
	return Reference<ITransaction>(new MultiVersionTransaction(tenantState->db,
	                                                           Reference<MultiVersionTenant>::addRef(this),
	                                                           tenantState->db->dbState->transactionDefaultOptions));
}

template <class T, class... Args>
ThreadFuture<T> MultiVersionTenant::executeOperation(ThreadFuture<T> (ITenant::*func)(Args...), Args&&... args) {
	auto tenantDb = tenantState->tenantVar->get();
	if (tenantDb.value) {
		auto f = (tenantDb.value.getPtr()->*func)(std::forward<Args>(args)...);
		return abortableFuture(f, tenantDb.onChange);
	}

	// If database initialization failed, return the initialization error
	auto dbError = tenantState->db->dbState->getInitializationError();
	if (dbError.isError()) {
		return ThreadFuture<T>(dbError.getError());
	}

	// Wait for the database to be initialized
	return abortableFuture(ThreadFuture<T>(Never()), tenantDb.onChange);
}

ThreadFuture<int64_t> MultiVersionTenant::getId() {
	return executeOperation(&ITenant::getId);
}

ThreadFuture<Key> MultiVersionTenant::purgeBlobGranules(const KeyRangeRef& keyRange, Version purgeVersion, bool force) {
	return executeOperation(
	    &ITenant::purgeBlobGranules, keyRange, std::forward<Version>(purgeVersion), std::forward<bool>(force));
}

ThreadFuture<Void> MultiVersionTenant::waitPurgeGranulesComplete(const KeyRef& purgeKey) {
	return executeOperation(&ITenant::waitPurgeGranulesComplete, purgeKey);
}

ThreadFuture<bool> MultiVersionTenant::blobbifyRange(const KeyRangeRef& keyRange) {
	return executeOperation(&ITenant::blobbifyRange, keyRange);
}

ThreadFuture<bool> MultiVersionTenant::blobbifyRangeBlocking(const KeyRangeRef& keyRange) {
	return executeOperation(&ITenant::blobbifyRangeBlocking, keyRange);
}

ThreadFuture<bool> MultiVersionTenant::unblobbifyRange(const KeyRangeRef& keyRange) {
	return executeOperation(&ITenant::unblobbifyRange, keyRange);
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> MultiVersionTenant::listBlobbifiedRanges(const KeyRangeRef& keyRange,
                                                                                          int rangeLimit) {
	return executeOperation(&ITenant::listBlobbifiedRanges, keyRange, std::forward<int>(rangeLimit));
}

ThreadFuture<Version> MultiVersionTenant::verifyBlobRange(const KeyRangeRef& keyRange, Optional<Version> version) {
	return executeOperation(&ITenant::verifyBlobRange, keyRange, std::forward<Optional<Version>>(version));
}

ThreadFuture<bool> MultiVersionTenant::flushBlobRange(const KeyRangeRef& keyRange,
                                                      bool compact,
                                                      Optional<Version> version) {
	return executeOperation(
	    &ITenant::flushBlobRange, keyRange, std::forward<bool>(compact), std::forward<Optional<Version>>(version));
}

MultiVersionTenant::TenantState::TenantState(Reference<MultiVersionDatabase> db, TenantNameRef tenantName)
  : tenantVar(new ThreadSafeAsyncVar<Reference<ITenant>>(Reference<ITenant>(nullptr))), tenantName(tenantName), db(db),
    closed(false) {
	updateTenant();
}

// Creates a new underlying tenant object whenever the database connection changes. This change is signaled
// to open transactions via an AsyncVar.
void MultiVersionTenant::TenantState::updateTenant() {
	Reference<ITenant> tenant;
	auto currentDb = db->dbState->dbVar->get();
	if (currentDb.value) {
		tenant = currentDb.value->openTenant(tenantName);
	} else {
		tenant = Reference<ITenant>(nullptr);
	}

	tenantVar->set(tenant, /* triggerIfSame */ !tenant.isValid());

	Reference<TenantState> self = Reference<TenantState>::addRef(this);

	MutexHolder holder(tenantLock);
	if (closed) {
		return;
	}

	tenantUpdater = mapThreadFuture<Void, Void>(currentDb.onChange, [self](ErrorOr<Void> result) {
		if (!result.isError()) {
			self->updateTenant();
		}
		return result;
	});
}

void MultiVersionTenant::TenantState::close() {
	MutexHolder holder(tenantLock);
	closed = true;
	if (tenantUpdater.isValid()) {
		tenantUpdater.cancel();
	}
}

// MultiVersionDatabase
MultiVersionDatabase::MultiVersionDatabase(MultiVersionApi* api,
                                           int threadIdx,
                                           ClusterConnectionRecord const& connectionRecord,
                                           Reference<IDatabase> db,
                                           Reference<IDatabase> versionMonitorDb,
                                           bool openConnectors)
  : dbState(new DatabaseState(connectionRecord, versionMonitorDb)) {
	dbState->setDatabase(db);
	if (openConnectors) {
		if (!api->localClientDisabled) {
			dbState->addClient(api->getLocalClient());
		}

		api->runOnExternalClients(threadIdx, [this](Reference<ClientInfo> client) { dbState->addClient(client); });

		api->runOnExternalClientsAllThreads([&connectionRecord](Reference<ClientInfo> client) {
			// This creates a database to initialize some client state on the external library.
			// We only do this on 6.2+ clients to avoid some bugs associated with older versions.
			// This deletes the new database immediately to discard its connections.
			//
			// Simultaneous attempts to create a database could result in us running this initialization
			// code in multiple threads simultaneously. It is necessary that each attempt have a chance
			// to run this initialization in case the other fails, and it's safe to run them in parallel.
			if (client->protocolVersion.hasCloseUnusedConnection() && !client->initialized) {
				try {
					Reference<IDatabase> newDb = connectionRecord.createDatabase(client->api);
					client->initialized = true;
				} catch (Error& e) {
					// This connection is not initialized. It is still possible to connect with it,
					// but we may not see trace logs from this client until a successful connection
					// is established.
					TraceEvent(SevWarnAlways, "FailedToInitializeExternalClient")
					    .error(e)
					    .detail("LibraryPath", client->libPath)
					    .detail("ConnectionRecord", connectionRecord);
				}
			}
		});

		// For clients older than 6.2 we create and maintain our database connection
		api->runOnExternalClients(threadIdx, [this, &connectionRecord](Reference<ClientInfo> client) {
			if (!client->protocolVersion.hasCloseUnusedConnection()) {
				try {
					dbState->legacyDatabaseConnections[client->protocolVersion] =
					    connectionRecord.createDatabase(client->api);
				} catch (Error& e) {
					// This connection is discarded
					TraceEvent(SevWarnAlways, "FailedToCreateLegacyDatabaseConnection")
					    .error(e)
					    .detail("LibraryPath", client->libPath)
					    .detail("ConnectionRecord", connectionRecord);
				}
			}
		});

		Reference<DatabaseState> dbStateRef = dbState;
		onMainThreadVoid([dbStateRef]() { dbStateRef->protocolVersionMonitor = dbStateRef->monitorProtocolVersion(); });
	}
}

MultiVersionDatabase::~MultiVersionDatabase() {
	dbState->close();
}

// Create a MultiVersionDatabase that wraps an already created IDatabase object
// For internal use in testing
Reference<IDatabase> MultiVersionDatabase::debugCreateFromExistingDatabase(Reference<IDatabase> db) {
	return Reference<IDatabase>(new MultiVersionDatabase(
	    MultiVersionApi::api, 0, ClusterConnectionRecord::fromConnectionString(""), db, db, false));
}

Reference<ITenant> MultiVersionDatabase::openTenant(TenantNameRef tenantName) {
	return makeReference<MultiVersionTenant>(Reference<MultiVersionDatabase>::addRef(this), tenantName);
}

Reference<ITransaction> MultiVersionDatabase::createTransaction() {
	return Reference<ITransaction>(new MultiVersionTransaction(Reference<MultiVersionDatabase>::addRef(this),
	                                                           Optional<Reference<MultiVersionTenant>>(),
	                                                           dbState->transactionDefaultOptions));
}

void MultiVersionDatabase::setOption(FDBDatabaseOptions::Option option, Optional<StringRef> value) {
	MutexHolder holder(dbState->optionLock);

	auto itr = FDBDatabaseOptions::optionInfo.find(option);
	if (itr == FDBDatabaseOptions::optionInfo.end()) {
		TraceEvent("UnknownDatabaseOption").detail("Option", option);
		throw invalid_option();
	}
	if (itr->first == FDBDatabaseOptions::USE_CONFIG_DATABASE) {
		dbState->isConfigDB = true;
	}

	int defaultFor = itr->second.defaultFor;
	if (defaultFor >= 0) {
		ASSERT(FDBTransactionOptions::optionInfo.find((FDBTransactionOptions::Option)defaultFor) !=
		       FDBTransactionOptions::optionInfo.end());
		dbState->transactionDefaultOptions.addOption((FDBTransactionOptions::Option)defaultFor,
		                                             value.castTo<Standalone<StringRef>>());
	}

	dbState->options.emplace_back(option, value.castTo<Standalone<StringRef>>());

	if (dbState->db) {
		dbState->db->setOption(option, value);
	}
}

ThreadFuture<int64_t> MultiVersionDatabase::rebootWorker(const StringRef& address, bool check, int duration) {
	if (dbState->db) {
		return dbState->db->rebootWorker(address, check, duration);
	}
	return false;
}

template <class T, class... Args>
ThreadFuture<T> MultiVersionDatabase::executeOperation(ThreadFuture<T> (IDatabase::*func)(Args...), Args&&... args) {
	auto db = dbState->dbVar->get();
	if (db.value) {
		auto f = (db.value.getPtr()->*func)(std::forward<Args>(args)...);
		return abortableFuture(f, db.onChange);
	}

	// If database initialization failed, return the initialization error
	auto dbError = dbState->getInitializationError();
	if (dbError.isError()) {
		return ThreadFuture<T>(dbError.getError());
	}

	// Wait for the database to be initialized
	return abortableFuture(ThreadFuture<T>(Never()), db.onChange);
}

ThreadFuture<Void> MultiVersionDatabase::forceRecoveryWithDataLoss(const StringRef& dcid) {
	return executeOperation(&IDatabase::forceRecoveryWithDataLoss, dcid);
}

ThreadFuture<Void> MultiVersionDatabase::createSnapshot(const StringRef& uid, const StringRef& snapshot_command) {
	return executeOperation(&IDatabase::createSnapshot, uid, snapshot_command);
}

ThreadFuture<DatabaseSharedState*> MultiVersionDatabase::createSharedState() {
	return executeOperation(&IDatabase::createSharedState);
}

void MultiVersionDatabase::setSharedState(DatabaseSharedState* p) {
	if (dbState->db) {
		dbState->db->setSharedState(p);
	}
}

// Get network thread busyness
// Return the busyness for the main thread. When using external clients, take the larger of the local client
// and the external client's busyness.
double MultiVersionDatabase::getMainThreadBusyness() {
	ASSERT(g_network);

	double localClientBusyness = g_network->networkInfo.metrics.networkBusyness;
	if (dbState->db) {
		return std::max(dbState->db->getMainThreadBusyness(), localClientBusyness);
	}

	return localClientBusyness;
}

ThreadFuture<Key> MultiVersionDatabase::purgeBlobGranules(const KeyRangeRef& keyRange,
                                                          Version purgeVersion,
                                                          bool force) {
	return executeOperation(
	    &IDatabase::purgeBlobGranules, keyRange, std::forward<Version>(purgeVersion), std::forward<bool>(force));
}

ThreadFuture<Void> MultiVersionDatabase::waitPurgeGranulesComplete(const KeyRef& purgeKey) {
	return executeOperation(&IDatabase::waitPurgeGranulesComplete, purgeKey);
}

ThreadFuture<bool> MultiVersionDatabase::blobbifyRange(const KeyRangeRef& keyRange) {
	return executeOperation(&IDatabase::blobbifyRange, keyRange);
}

ThreadFuture<bool> MultiVersionDatabase::blobbifyRangeBlocking(const KeyRangeRef& keyRange) {
	return executeOperation(&IDatabase::blobbifyRangeBlocking, keyRange);
}

ThreadFuture<bool> MultiVersionDatabase::unblobbifyRange(const KeyRangeRef& keyRange) {
	return executeOperation(&IDatabase::unblobbifyRange, keyRange);
}

ThreadFuture<Standalone<VectorRef<KeyRangeRef>>> MultiVersionDatabase::listBlobbifiedRanges(const KeyRangeRef& keyRange,
                                                                                            int rangeLimit) {
	return executeOperation(&IDatabase::listBlobbifiedRanges, keyRange, std::forward<int>(rangeLimit));
}

ThreadFuture<Version> MultiVersionDatabase::verifyBlobRange(const KeyRangeRef& keyRange, Optional<Version> version) {
	return executeOperation(&IDatabase::verifyBlobRange, keyRange, std::forward<Optional<Version>>(version));
}

ThreadFuture<bool> MultiVersionDatabase::flushBlobRange(const KeyRangeRef& keyRange,
                                                        bool compact,
                                                        Optional<Version> version) {
	return executeOperation(
	    &IDatabase::flushBlobRange, keyRange, std::forward<bool>(compact), std::forward<Optional<Version>>(version));
}

// Returns the protocol version reported by the coordinator this client is connected to
// If an expected version is given, the future won't return until the protocol version is different than expected
// Note: this will never return if the server is running a protocol from FDB 5.0 or older
ThreadFuture<ProtocolVersion> MultiVersionDatabase::getServerProtocol(Optional<ProtocolVersion> expectedVersion) {
	return dbState->versionMonitorDb->getServerProtocol(expectedVersion);
}

ThreadFuture<Standalone<StringRef>> MultiVersionDatabase::getClientStatus() {
	auto stateRef = dbState;
	auto db = stateRef->dbVar->get();
	if (!db.value.isValid()) {
		db.value = stateRef->versionMonitorDb;
	}
	if (!db.value.isValid()) {
		return onMainThread([stateRef] { return Future<Standalone<StringRef>>(stateRef->getClientStatus(""_sr)); });
	} else {
		// If a database is created first retrieve its status
		auto f = db.value->getClientStatus();
		auto statusFuture = abortableFuture(f, db.onChange);
		return flatMapThreadFuture<Standalone<StringRef>, Standalone<StringRef>>(
		    statusFuture, [stateRef](ErrorOr<Standalone<StringRef>> dbContextStatus) {
			    return onMainThread([stateRef, dbContextStatus] {
				    return Future<Standalone<StringRef>>(stateRef->getClientStatus(dbContextStatus));
			    });
		    });
	}
}

MultiVersionDatabase::DatabaseState::DatabaseState(ClusterConnectionRecord const& connectionRecord,
                                                   Reference<IDatabase> versionMonitorDb)
  : dbVar(new ThreadSafeAsyncVar<Reference<IDatabase>>(Reference<IDatabase>(nullptr))),
    connectionRecord(connectionRecord), versionMonitorDb(versionMonitorDb),
    initializationState(InitializationState::INITIALIZING), isConfigDB(false) {}

void MultiVersionDatabase::DatabaseState::setDatabase(Reference<IDatabase> db) {
	if (db.isValid()) {
		initializationState = InitializationState::CREATED;
	}
	this->db = db;
	dbVar->set(db, true);
}

ErrorOr<Void> MultiVersionDatabase::DatabaseState::getInitializationError() {
	InitializationState st = initializationState.load();
	switch (st) {
	case InitializationState::INCOMPATIBLE:
		return MultiVersionApi::api->failIncompatibleClient ? ErrorOr<Void>(incompatible_client())
		                                                    : ErrorOr<Void>(Void());
	case InitializationState::INITIALIZATION_FAILED:
		return ErrorOr<Void>(initializationError);
	default:
		return ErrorOr<Void>(Void());
	}
}

// Adds a client (local or externally loaded) that can be used to connect to the cluster
void MultiVersionDatabase::DatabaseState::addClient(Reference<ClientInfo> client) {
	ProtocolVersion baseVersion = client->protocolVersion.normalizedVersion();
	auto [itr, inserted] = clients.insert({ baseVersion, client });
	if (!inserted) {
		// SOMEDAY: prefer client with higher release version if protocol versions are compatible
		Reference<ClientInfo> keptClient = itr->second;
		Reference<ClientInfo> discardedClient = client;
		if (client->canReplace(itr->second)) {
			std::swap(keptClient, discardedClient);
			clients[baseVersion] = client;
		}

		discardedClient->failed = true;
		TraceEvent(SevWarn, "DuplicateClientVersion")
		    .detail("Keeping", keptClient->libPath)
		    .detail("KeptProtocolVersion", keptClient->protocolVersion)
		    .detail("Disabling", discardedClient->libPath)
		    .detail("DisabledProtocolVersion", discardedClient->protocolVersion);

		MultiVersionApi::api->updateSupportedVersions();
	}

	if (!client->protocolVersion.hasInexpensiveMultiVersionClient() && !client->failed) {
		TraceEvent("AddingLegacyVersionMonitor")
		    .detail("LibPath", client->libPath)
		    .detail("ProtocolVersion", client->protocolVersion);

		legacyVersionMonitors.emplace_back(new LegacyVersionMonitor(client));
	}
}

// Watch the cluster protocol version for changes and update the database state when it does.
// Must be called from the main thread
ThreadFuture<Void> MultiVersionDatabase::DatabaseState::monitorProtocolVersion() {
	startLegacyVersionMonitors();

	Optional<ProtocolVersion> expected = dbProtocolVersion;
	ThreadFuture<ProtocolVersion> f = versionMonitorDb->getServerProtocol(dbProtocolVersion);

	Reference<DatabaseState> self = Reference<DatabaseState>::addRef(this);
	return mapThreadFuture<ProtocolVersion, Void>(f, [self, expected](ErrorOr<ProtocolVersion> cv) {
		if (self->initializationState == InitializationState::CLOSED) {
			return ErrorOr<Void>(Void());
		}

		ProtocolVersion clusterVersion;
		if (cv.isError()) {
			if (cv.getError().code() == error_code_operation_cancelled) {
				return ErrorOr<Void>(cv.getError());
			}

			TraceEvent("ErrorGettingClusterProtocolVersion")
			    .error(cv.getError())
			    .detail("ExpectedProtocolVersion", expected);

			if (self->initializationState == InitializationState::INITIALIZING) {
				// A failure to retrieve the protocol error is a fatal error, such as invalid or
				// missing cluster file. There is no point of retrying on it.
				// Mark the database as failed and abort pending futures
				self->initializationError = cv.getError();
				self->initializationState = InitializationState::INITIALIZATION_FAILED;
				self->dbVar->set(Reference<IDatabase>(), true);
			}
		} else {
			clusterVersion = cv.get();
			onMainThreadVoid([self, clusterVersion]() { self->protocolVersionChanged(clusterVersion); });
		}
		return ErrorOr<Void>(Void());
	});
}

// Called when a change to the protocol version of the cluster has been detected.
// Must be called from the main thread
void MultiVersionDatabase::DatabaseState::protocolVersionChanged(ProtocolVersion protocolVersion) {
	if (initializationState == InitializationState::CLOSED) {
		return;
	}

	// If the protocol version changed but is still compatible, update our local version but keep the
	// same connection
	if (dbProtocolVersion.present() &&
	    protocolVersion.normalizedVersion() == dbProtocolVersion.get().normalizedVersion()) {
		dbProtocolVersion = protocolVersion;

		ASSERT(protocolVersionMonitor.isValid());
		protocolVersionMonitor.cancel();
		protocolVersionMonitor = monitorProtocolVersion();
		return;
	}

	// The protocol version has changed to a different, incompatible version
	TraceEvent("ProtocolVersionChanged")
	    .detail("NewProtocolVersion", protocolVersion)
	    .detail("OldProtocolVersion", dbProtocolVersion);
	// When the protocol version changes, clear the corresponding entry in the shared state map
	// so it can be re-initialized. Only do so if there was a valid previous protocol version.
	if (dbProtocolVersion.present() && MultiVersionApi::api->getApiVersion().hasClusterSharedStateMap()) {
		MultiVersionApi::api->clearClusterSharedStateMapEntry(clusterId, dbProtocolVersion.get());
	}

	dbProtocolVersion = protocolVersion;

	auto itr = clients.find(protocolVersion.normalizedVersion());
	if (itr == clients.end()) {
		// We don't have a client matching the current protocol
		initializationState = InitializationState::INCOMPATIBLE;
		updateDatabase(Reference<IDatabase>(), Reference<ClientInfo>());
		return;
	}

	// A compatible client found, use it for creating a new database connection
	auto& client = itr->second;
	TraceEvent("CreatingDatabaseOnClient")
	    .detail("LibraryPath", client->libPath)
	    .detail("Failed", client->failed)
	    .detail("External", client->external);

	Reference<IDatabase> newDb;
	try {
		newDb = connectionRecord.createDatabase(client->api);
	} catch (Error& e) {
		// Create error currently does not return any error except for network not initialized,
		// which cannot happen at this point
		ASSERT(false);
	}

	if (client->external && !MultiVersionApi::api->getApiVersion().hasInlineUpdateDatabase()) {
		// Old API versions return a future when creating the database, so we need to wait for it
		Reference<DatabaseState> self = Reference<DatabaseState>::addRef(this);
		dbReady = mapThreadFuture<Void, Void>(
		    newDb.castTo<DLDatabase>()->onReady(), [self, newDb, client](ErrorOr<Void> ready) {
			    if (!ready.isError()) {
				    onMainThreadVoid([self, newDb, client]() { self->updateDatabase(newDb, client); });
			    } else {
				    onMainThreadVoid(
				        [self, client]() { self->updateDatabase(Reference<IDatabase>(), Reference<ClientInfo>()); });
			    }
			    return ready;
		    });
	} else {
		updateDatabase(newDb, client);
	}
}

// Replaces the active database connection with a new one. Must be called from the main thread.
void MultiVersionDatabase::DatabaseState::updateDatabase(Reference<IDatabase> newDb, Reference<ClientInfo> client) {
	if (initializationState == InitializationState::CLOSED) {
		return;
	}

	// Reapply database options on the new database
	if (newDb) {
		optionLock.enter();
		for (auto option : options) {
			try {
				// In practice, this will set a deferred error instead of throwing. If that happens, the database
				// will be unusable (attempts to use it will throw errors).
				newDb->setOption(option.first, option.second.castTo<StringRef>());
			} catch (Error& e) {
				optionLock.leave();

				// If we can't set all of the options on a cluster, we abandon the client
				TraceEvent(SevError, "ClusterVersionChangeOptionError")
				    .error(e)
				    .detail("Option", option.first)
				    .detail("OptionValue", option.second)
				    .detail("LibPath", client->libPath);
				client->failed = true;
				MultiVersionApi::api->updateSupportedVersions();
				newDb = Reference<IDatabase>();
				break;
			}
		}
		optionLock.leave();
	}

	// Use the new database for monitoring the version changes, if it supports version monitoring
	if (newDb && dbProtocolVersion.get().hasStableInterfaces()) {
		versionMonitorDb = newDb;
	} else {
		// We don't have a database connection, so use the local client to monitor the protocol version
		// Also for older clients that don't have an API to get the protocol version, we have to monitor it locally
		try {
			versionMonitorDb = connectionRecord.createDatabase(MultiVersionApi::api->getLocalClient()->api);
		} catch (Error& e) {
			// We can't create a new database to monitor the cluster version. This means we will continue using
			// the previous one, which should hopefully continue to work.
			TraceEvent(SevWarnAlways, "FailedToCreateDatabaseForVersionMonitoring")
			    .error(e)
			    .detail("ConnectionRecord", connectionRecord);
		}
	}

	// Verify the database has the necessary functionality to update the shared
	// state. Avoid updating the shared state if the database is a
	// configuration database, because a configuration database does not have
	// access to typical system keys and does not need to be updated.
	if (newDb && MultiVersionApi::api->getApiVersion().hasClusterSharedStateMap() && !isConfigDB) {
		Future<std::string> updateResult =
		    MultiVersionApi::api->updateClusterSharedStateMap(connectionRecord, dbProtocolVersion.get(), newDb);
		sharedStateUpdater = map(errorOr(updateResult), [this, newDb](ErrorOr<std::string> result) {
			if (result.present()) {
				clusterId = result.get();
				TraceEvent("ClusterSharedStateUpdated")
				    .detail("ClusterId", result.get())
				    .detail("ProtocolVersion", dbProtocolVersion.get());
			} else {
				TraceEvent(SevWarnAlways, "ClusterSharedStateUpdateError")
				    .error(result.getError())
				    .detail("ConnectionRecord", connectionRecord)
				    .detail("ProtocolVersion", dbProtocolVersion.get());
			}
			setDatabase(newDb);
			return Void();
		});
	} else {
		setDatabase(newDb);
	}

	ASSERT(protocolVersionMonitor.isValid());
	protocolVersionMonitor.cancel();
	protocolVersionMonitor = monitorProtocolVersion();
}

// Starts version monitors for old client versions that don't support connect packet monitoring (<= 5.0).
// Must be called from the main thread
void MultiVersionDatabase::DatabaseState::startLegacyVersionMonitors() {
	for (auto itr = legacyVersionMonitors.begin(); itr != legacyVersionMonitors.end(); ++itr) {
		while (itr != legacyVersionMonitors.end() && (*itr)->client->failed) {
			(*itr)->close();
			itr = legacyVersionMonitors.erase(itr);
		}
		if (itr != legacyVersionMonitors.end() &&
		    (!dbProtocolVersion.present() || (*itr)->client->protocolVersion != dbProtocolVersion.get())) {
			(*itr)->startConnectionMonitor(Reference<DatabaseState>::addRef(this));
		}
	}
}

// Cleans up state for the legacy version monitors to break reference cycles
void MultiVersionDatabase::DatabaseState::close() {
	Reference<DatabaseState> self = Reference<DatabaseState>::addRef(this);
	onMainThreadVoid([self]() {
		self->initializationState = InitializationState::CLOSED;
		if (self->protocolVersionMonitor.isValid()) {
			self->protocolVersionMonitor.cancel();
		}
		for (auto monitor : self->legacyVersionMonitors) {
			monitor->close();
		}

		self->legacyVersionMonitors.clear();
	});
}

namespace {

const char* initializationStateToString(MultiVersionDatabase::InitializationState initState) {
	switch (initState) {
	case MultiVersionDatabase::InitializationState::INITIALIZING:
		return "initializing";
	case MultiVersionDatabase::InitializationState::INITIALIZATION_FAILED:
		return "initialization_failed";
	case MultiVersionDatabase::InitializationState::CREATED:
		return "created";
	case MultiVersionDatabase::InitializationState::INCOMPATIBLE:
		return "incompatible";
	case MultiVersionDatabase::InitializationState::CLOSED:
		return "closed";
	default:
		ASSERT(false);
		return "invalid_state";
	}
}

} // namespace

//
// Generates the client-side status report for the Multi-Version Database
//
// The parameter dbContextStatus contains the status report generated by
// the wrapped Native Database (from external or local client), which is then
// embedded within the status report of the Multi-Version Database
//
// The overall report schema is as follows:
// { "Healthy": <overall health status, true or false>,
//   "InitializationState": <initialization state of the Multi-Version Database>,
//   "InitializationError": <initialization error code, present if initialization failed>,
//   "ProtocolVersion" : <determined protocol version of the cluster, present if determined>,
//   "ConnectionRecord" : <connection file name or connection string>,
//   "DatabaseStatus" : <Native Database status report, present if successfully retrieved>,
//   "ErrorRetrievingDatabaseStatus" : <error code of retrieving status of the Native Database, present if failed>,
//   "AvailableClients" : [
//      { "ProtocolVersion" : <protocol version of the client>,
//        "ReleaseVersion" : <release version of the client>,
//        "ThreadIndex" : <the index of the client thread serving this database>
//      },
//      ...
//   ]
// }
//
//
Standalone<StringRef> MultiVersionDatabase::DatabaseState::getClientStatus(
    ErrorOr<Standalone<StringRef>> dbContextStatus) {
	json_spirit::mObject statusObj;
	statusObj["InitializationState"] = initializationStateToString(initializationState);
	if (initializationState == InitializationState::INITIALIZATION_FAILED) {
		statusObj["InitializationError"] = initializationError.code();
	}
	json_spirit::mArray clientArr;
	for (auto [protocolVersion, client] : this->clients) {
		json_spirit::mObject clientDesc;
		clientDesc["ProtocolVersion"] = format("%llx", client->protocolVersion.version());
		clientDesc["ReleaseVersion"] = client->releaseVersion;
		clientDesc["ThreadIndex"] = client->threadIndex;
		clientArr.push_back(clientDesc);
	}
	statusObj["AvailableClients"] = clientArr;
	statusObj["ConnectionRecord"] = connectionRecord.toString();
	if (dbProtocolVersion.present()) {
		statusObj["ProtocolVersion"] = format("%llx", dbProtocolVersion.get().version());
	}
	bool dbContextHealthy = false;
	if (initializationState != InitializationState::INITIALIZATION_FAILED) {
		if (dbContextStatus.isError()) {
			statusObj["ErrorRetrievingDatabaseStatus"] = dbContextStatus.getError().code();
		} else {
			json_spirit::mValue dbContextStatusVal;
			json_spirit::read_string(dbContextStatus.get().toString(), dbContextStatusVal);
			statusObj["DatabaseStatus"] = dbContextStatusVal;
			auto& dbContextStatusObj = dbContextStatusVal.get_obj();
			auto healthyIter = dbContextStatusObj.find("Healthy");
			if (healthyIter != dbContextStatusObj.end() && healthyIter->second.type() == json_spirit::bool_type) {
				dbContextHealthy = healthyIter->second.get_bool();
			}
		}
	}
	statusObj["Healthy"] = initializationState == InitializationState::CREATED && dbContextHealthy;
	return StringRef(json_spirit::write_string(json_spirit::mValue(statusObj)));
}

// Starts the connection monitor by creating a database object at an old version.
// Must be called from the main thread
void MultiVersionDatabase::LegacyVersionMonitor::startConnectionMonitor(
    Reference<MultiVersionDatabase::DatabaseState> dbState) {
	if (!monitorRunning) {
		monitorRunning = true;

		auto itr = dbState->legacyDatabaseConnections.find(client->protocolVersion);
		ASSERT(itr != dbState->legacyDatabaseConnections.end());

		db = itr->second;
		tr = Reference<ITransaction>();

		TraceEvent("StartingLegacyVersionMonitor").detail("ProtocolVersion", client->protocolVersion);
		Reference<LegacyVersionMonitor> self = Reference<LegacyVersionMonitor>::addRef(this);
		versionMonitor =
		    mapThreadFuture<Void, Void>(db.castTo<DLDatabase>()->onReady(), [self, dbState](ErrorOr<Void> ready) {
			    onMainThreadVoid([self, ready, dbState]() {
				    if (ready.isError()) {
					    if (ready.getError().code() != error_code_operation_cancelled) {
						    TraceEvent(SevError, "FailedToOpenDatabaseOnClient")
						        .error(ready.getError())
						        .detail("LibPath", self->client->libPath);

						    self->client->failed = true;
						    MultiVersionApi::api->updateSupportedVersions();
					    }
				    } else {
					    self->runGrvProbe(dbState);
				    }
			    });

			    return ready;
		    });
	}
}

// Runs a GRV probe on the cluster to determine if the client version is compatible with the cluster.
// Must be called from main thread
void MultiVersionDatabase::LegacyVersionMonitor::runGrvProbe(Reference<MultiVersionDatabase::DatabaseState> dbState) {
	tr = db->createTransaction();
	Reference<LegacyVersionMonitor> self = Reference<LegacyVersionMonitor>::addRef(this);
	versionMonitor = mapThreadFuture<Version, Void>(tr->getReadVersion(), [self, dbState](ErrorOr<Version> v) {
		// If the version attempt returns an error, we regard that as a connection (except operation_cancelled)
		if (!v.isError() || v.getError().code() != error_code_operation_cancelled) {
			onMainThreadVoid([self, dbState]() {
				self->monitorRunning = false;
				dbState->protocolVersionChanged(self->client->protocolVersion);
			});
		}

		return v.map([](Version v) { return Void(); });
	});
}

void MultiVersionDatabase::LegacyVersionMonitor::close() {
	if (versionMonitor.isValid()) {
		versionMonitor.cancel();
	}
}

// MultiVersionApi
void MultiVersionApi::runOnExternalClientsAllThreads(std::function<void(Reference<ClientInfo>)> func,
                                                     bool runOnFailedClients,
                                                     bool failOnError) {
	for (int i = 0; i < threadCount; i++) {
		runOnExternalClients(i, func, runOnFailedClients, failOnError);
	}
}

void MultiVersionApi::runOnExternalClientsThreadRange(std::function<void(Reference<ClientInfo>)> func,
                                                      int startIdx,
                                                      int endIdx,
                                                      bool runOnFailedClients,
                                                      bool failOnError) {
	for (int i = startIdx; i < std::min(endIdx, threadCount); i++) {
		runOnExternalClients(i, func, runOnFailedClients, failOnError);
	}
}

// runOnFailedClients should be used cautiously. Some failed clients may not have successfully loaded all symbols.
void MultiVersionApi::runOnExternalClients(int threadIdx,
                                           std::function<void(Reference<ClientInfo>)> func,
                                           bool runOnFailedClients,
                                           bool failOnError) {
	bool newFailure = false;

	auto c = externalClients.begin();
	while (c != externalClients.end()) {
		auto client = c->second[threadIdx];
		try {
			if (!client->failed || runOnFailedClients) { // TODO: Should we ignore some failures?
				func(client);
			}
		} catch (Error& e) {
			if (e.code() == error_code_external_client_already_loaded) {
				TraceEvent(SevInfo, "ExternalClientAlreadyLoaded").error(e).detail("LibPath", c->first);
				c = externalClients.erase(c);
				continue;
			} else {
				TraceEvent(SevWarnAlways, "ExternalClientFailure").error(e).detail("LibPath", c->first);
				client->failed = true;
				newFailure = true;
				if (failOnError) {
					throw e;
				}
			}
		}

		++c;
	}

	if (newFailure) {
		updateSupportedVersions();
	}
}

bool MultiVersionApi::hasNonFailedExternalClients() {
	bool validClientFound = false;
	runOnExternalClientsAllThreads([&validClientFound](auto client) {
		if (!client->failed) {
			validClientFound = true;
		}
	});
	return validClientFound;
}

Reference<ClientInfo> MultiVersionApi::getLocalClient() {
	return localClient;
}

void MultiVersionApi::selectApiVersion(int apiVersion) {
	ApiVersion newApiVersion(apiVersion);
	if (!localClient) {
		localClient = makeReference<ClientInfo>(getLocalClientAPI());
		ASSERT(localClient);
	}

	if (this->apiVersion.isValid() && this->apiVersion != newApiVersion) {
		throw api_version_already_set();
	}

	localClient->api->selectApiVersion(apiVersion);
	this->apiVersion = newApiVersion;
}

const char* MultiVersionApi::getClientVersion() {
	return localClient->api->getClientVersion();
}

void MultiVersionApi::useFutureProtocolVersion() {
	localClient->api->useFutureProtocolVersion();
}

namespace {
void validateOption(Optional<StringRef> value, bool canBePresent, bool canBeAbsent, bool canBeEmpty = true) {
	ASSERT(canBePresent || canBeAbsent);

	if (!canBePresent && value.present() && (!canBeEmpty || value.get().size() > 0)) {
		throw invalid_option_value();
	}
	if (!canBeAbsent && (!value.present() || (!canBeEmpty && value.get().size() == 0))) {
		throw invalid_option_value();
	}
}

} // namespace

void MultiVersionApi::disableMultiVersionClientApi() {
	MutexHolder holder(lock);
	if (networkStartSetup || localClientDisabled || disableBypass) {
		throw invalid_option();
	}

	bypassMultiClientApi = true;
}

void MultiVersionApi::setCallbacksOnExternalThreads() {
	MutexHolder holder(lock);
	if (networkStartSetup) {
		throw invalid_option();
	}

	callbackOnMainThread = false;
}
void MultiVersionApi::addExternalLibrary(std::string path, bool useFutureVersion) {
	std::string filename = basename(path);

	if (filename.empty() || !fileExists(path)) {
		TraceEvent("ExternalClientNotFound").detail("LibraryPath", filename);
		throw file_not_found();
	}

	MutexHolder holder(lock);
	if (networkStartSetup) {
		throw invalid_option(); // SOMEDAY: it might be good to allow clients to be added after the network is setup
	}

	// external libraries always run on their own thread; ensure we allocate at least one thread to run this
	// library.
	threadCount = std::max(threadCount, 1);

	if (externalClientDescriptions.count(filename) == 0) {
		TraceEvent("AddingExternalClient").detail("LibraryPath", filename).detail("UseFutureVersion", useFutureVersion);
		externalClientDescriptions.emplace(std::make_pair(filename, ClientDesc(path, true, useFutureVersion)));
	}
}

void MultiVersionApi::addExternalLibraryDirectory(std::string path) {
	TraceEvent("AddingExternalClientDirectory").detail("Directory", path);
	std::vector<std::string> files = platform::listFiles(path, DYNAMIC_LIB_EXT);

	MutexHolder holder(lock);
	if (networkStartSetup) {
		throw invalid_option(); // SOMEDAY: it might be good to allow clients to be added after the network is setup
	}

	// external libraries always run on their own thread; ensure we allocate at least one thread to run this
	// library.
	threadCount = std::max(threadCount, 1);

	for (auto filename : files) {
		std::string lib = abspath(joinPath(path, filename));
		if (externalClientDescriptions.count(filename) == 0) {
			TraceEvent("AddingExternalClient").detail("LibraryPath", filename);
			externalClientDescriptions.emplace(std::make_pair(filename, ClientDesc(lib, true, false)));
		}
	}
}

#if defined(__unixish__)
std::vector<std::pair<std::string, bool>> MultiVersionApi::copyExternalLibraryPerThread(std::string path,
                                                                                        std::string buildId) {

	ASSERT_GE(threadCount, 1);
	// Copy library for each thread configured per version if the copies do not already exist
	std::vector<std::pair<std::string, bool>> paths;

	std::string filename = basename(path);

	for (int ii = 1; ii < threadCount; ++ii) {

		constexpr int MAX_TMP_NAME_LENGTH = PATH_MAX + 12;
		char cpName[MAX_TMP_NAME_LENGTH];
		snprintf(cpName, MAX_TMP_NAME_LENGTH, "%s/%s-%d-%s", tmpDir.c_str(), filename.c_str(), ii, buildId.c_str());

		if (!(std::filesystem::exists(cpName))) {

			int cpFd = open(cpName, O_RDWR | O_CREAT, 0755);
			int fd;

			if ((fd = open(path.c_str(), O_RDONLY)) == -1) {
				TraceEvent("ExternalClientNotFound").detail("LibraryPath", path);
				throw file_not_found();
			}

			TraceEvent("CopyingExternalClient")
			    .detail("FileName", filename)
			    .detail("LibraryPath", path)
			    .detail("CopyPath", cpName);

			constexpr size_t buf_sz = 4096;
			char buf[buf_sz];
			while (1) {
				ssize_t readCount = read(fd, buf, buf_sz);
				if (readCount == 0) {
					// eof
					break;
				}
				if (readCount == -1) {
					TraceEvent(SevError, "ExternalClientCopyFailedReadError")
					    .GetLastError()
					    .detail("LibraryPath", path);
					throw platform_error();
				}
				ssize_t written = 0;
				while (written != readCount) {
					ssize_t writeCount = write(cpFd, buf + written, readCount - written);
					if (writeCount == -1) {
						TraceEvent(SevError, "ExternalClientCopyFailedWriteError")
						    .GetLastError()
						    .detail("LibraryPath", path);
						throw platform_error();
					}
					written += writeCount;
				}
			}

			close(fd);
			close(cpFd);
		}
		paths.push_back({ cpName, false }); // use + no not delete copies of the library.
	}

	return paths;
}
#else // if defined (__unixish__)
std::vector<std::pair<std::string, bool>> MultiVersionApi::copyExternalLibraryPerThread(std::string path) {
	if (threadCount > 1) {
		TraceEvent(SevError, "MultipleClientThreadsUnsupportedOnWindows").log();
		throw unsupported_operation();
	}
	std::vector<std::pair<std::string, bool>> paths;
	paths.push_back({ path, false });
	return paths;
}
#endif // if defined (__unixish__)

void MultiVersionApi::disableLocalClient() {
	MutexHolder holder(lock);
	if (networkStartSetup || bypassMultiClientApi) {
		throw invalid_option();
	}
	threadCount = std::max(threadCount, 1);
	localClientDisabled = true;
}

void MultiVersionApi::setSupportedClientVersions(Standalone<StringRef> versions) {
	MutexHolder holder(lock);
	ASSERT(networkSetup);

	// This option must be set on the main thread because it modifies structures that can be used concurrently by
	// the main thread
	onMainThreadVoid([this, versions]() {
		localClient->api->setNetworkOption(FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS, versions);
	});

	if (!bypassMultiClientApi) {
		runOnExternalClientsAllThreads([versions](Reference<ClientInfo> client) {
			client->api->setNetworkOption(FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS, versions);
		});
	}
}

void MultiVersionApi::setNetworkOption(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	if (option != FDBNetworkOptions::EXTERNAL_CLIENT &&
	    !externalClient) { // This is the first option set for external clients
		loadEnvironmentVariableNetworkOptions();
	}

	setNetworkOptionInternal(option, value);
}

void MultiVersionApi::setNetworkOptionInternal(FDBNetworkOptions::Option option, Optional<StringRef> value) {
	bool forwardOption = false;

	auto itr = FDBNetworkOptions::optionInfo.find(option);
	if (itr != FDBNetworkOptions::optionInfo.end()) {
		TraceEvent("SetNetworkOption").detail("Option", itr->second.name);
	} else {
		TraceEvent("UnknownNetworkOption").detail("Option", option);
		throw invalid_option();
	}

	if (option == FDBNetworkOptions::DISABLE_MULTI_VERSION_CLIENT_API) {
		validateOption(value, false, true);
		disableMultiVersionClientApi();
	} else if (option == FDBNetworkOptions::CALLBACKS_ON_EXTERNAL_THREADS) {
		validateOption(value, false, true);
		setCallbacksOnExternalThreads();
	} else if (option == FDBNetworkOptions::EXTERNAL_CLIENT_LIBRARY) {
		validateOption(value, true, false, false);
		addExternalLibrary(abspath(value.get().toString()), false);
	} else if (option == FDBNetworkOptions::EXTERNAL_CLIENT_DIRECTORY) {
		validateOption(value, true, false, false);
		addExternalLibraryDirectory(value.get().toString());
	} else if (option == FDBNetworkOptions::DISABLE_LOCAL_CLIENT) {
		validateOption(value, false, true);
		disableLocalClient();
	} else if (option == FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS) {
		ASSERT(value.present());
		setSupportedClientVersions(value.get());
	} else if (option == FDBNetworkOptions::EXTERNAL_CLIENT) {
		MutexHolder holder(lock);
		ASSERT(!value.present() && !networkStartSetup);
		externalClient = true;
		bypassMultiClientApi = true;
		forwardOption = true;
	} else if (option == FDBNetworkOptions::DISABLE_CLIENT_BYPASS) {
		MutexHolder holder(lock);
		ASSERT(!networkStartSetup);
		if (bypassMultiClientApi) {
			throw invalid_option();
		}
		disableBypass = true;
	} else if (option == FDBNetworkOptions::CLIENT_THREADS_PER_VERSION) {
		MutexHolder holder(lock);
		validateOption(value, true, false, false);
		if (networkStartSetup) {
			throw invalid_option();
		}
#if defined(__unixish__)
		threadCount = extractIntOption(value, 1, 1024);
#else
		// multiple client threads are not supported on windows.
		threadCount = extractIntOption(value, 1, 1);
#endif
	} else if (option == FDBNetworkOptions::CLIENT_TMP_DIR) {
		validateOption(value, true, false, false);
		tmpDir = abspath(value.get().toString());
	} else if (option == FDBNetworkOptions::FUTURE_VERSION_CLIENT_LIBRARY) {
		validateOption(value, true, false, false);
		addExternalLibrary(abspath(value.get().toString()), true);
	} else if (option == FDBNetworkOptions::TRACE_FILE_IDENTIFIER) {
		validateOption(value, true, false, true);
		traceFileIdentifier = value.get().toString();
		{
			MutexHolder holder(lock);
			// Forward the option unmodified only to the the local client and let it validate it.
			// While for external clients the trace file identifiers are determined in setupNetwork
			localClient->api->setNetworkOption(option, value);
		}
	} else if (option == FDBNetworkOptions::TRACE_SHARE_AMONG_CLIENT_THREADS) {
		validateOption(value, false, true);
		traceShareBaseNameAmongThreads = true;
	} else if (option == FDBNetworkOptions::IGNORE_EXTERNAL_CLIENT_FAILURES) {
		validateOption(value, false, true);
		ignoreExternalClientFailures = true;
	} else if (option == FDBNetworkOptions::FAIL_INCOMPATIBLE_CLIENT) {
		validateOption(value, false, true);
		failIncompatibleClient = true;
	} else if (option == FDBNetworkOptions::RETAIN_CLIENT_LIBRARY_COPIES) {
		validateOption(value, false, true);
		retainClientLibCopies = true;
	} else {
		forwardOption = true;
	}

	if (forwardOption) {
		MutexHolder holder(lock);
		localClient->api->setNetworkOption(option, value);

		if (!bypassMultiClientApi) {
			if (networkSetup) {
				runOnExternalClientsAllThreads(
				    [option, value](Reference<ClientInfo> client) { client->api->setNetworkOption(option, value); });
			} else {
				options.emplace_back(option, value.castTo<Standalone<StringRef>>());
			}
		}
	}
}

std::string getBuildIdString(const uint8_t* build_id) {
	// https://gist.github.com/miguelmota/4fc9b46cf21111af5fa613555c14de92
	const int BUILD_ID_LENGTH = 20;
	std::ostringstream ss_build_id;
	ss_build_id << std::hex << std::setfill('0');
	for (ElfW(Word) i = 0; i < BUILD_ID_LENGTH; i++) {
		ss_build_id << std::hex << std::setw(2) << static_cast<int>(build_id[i]);
	};
	return ss_build_id.str();
}

void MultiVersionApi::setupNetwork() {

	TIMER_START(1);

	// function to initialize external clients
	auto extInitFunc = [this](Reference<ClientInfo> client) {
		// TIMER_START(1);
		TraceEvent("InitializingExternalClient").detail("LibraryPath", client->libPath);
		client->api->selectApiVersion(apiVersion.version());
		if (client->useFutureVersion) {
			client->api->useFutureProtocolVersion();
		}
		client->loadVersion();
		// TIMER_END(1, extInitFunc);
	};

	try {
		if (!externalClient) {
			loadEnvironmentVariableNetworkOptions();
		}

		uint64_t transportId = 0;
		{ // lock scope
			MutexHolder holder(lock);
			if (networkStartSetup) {
				throw network_already_setup();
			}

			if (threadCount > 1) {
				disableLocalClient();
			}

			networkStartSetup = true;

			if (externalClientDescriptions.empty() && localClientDisabled) {
				TraceEvent(SevWarn, "CannotSetupNetwork")
				    .detail("Reason", "Local client is disabled and no external clients configured");

				throw no_external_client_provided();
			}

			if (externalClientDescriptions.empty() && !disableBypass) {
				bypassMultiClientApi = true; // SOMEDAY: we won't be able to set this option once it becomes
				                             // possible to add clients after setupNetwork is called
			}

			if (!bypassMultiClientApi) {
				transportId =
				    (uint64_t(uint32_t(platform::getRandomSeed())) << 32) ^ uint32_t(platform::getRandomSeed());
				if (transportId <= 1)
					transportId += 2;
				localClient->api->setNetworkOption(FDBNetworkOptions::EXTERNAL_CLIENT_TRANSPORT_ID,
				                                   std::to_string(transportId));
			}
			TIMER_START(2);
			localClient->api->setupNetwork();
			TIMER_END(2, Setting up local network);

			if (!apiVersion.hasFailOnExternalClientErrors()) {
				ignoreExternalClientFailures = true;
			}

			assert(externalClients.empty());

			// TIMER_START(3);
			for (auto i : externalClientDescriptions) {
				std::string path = i.second.libPath;
				std::string filename = basename(path);
				bool useFutureVersion = i.second.useFutureVersion;

				if (externalClients.count(filename) == 0) {
					externalClients[filename] = {};
					externalClients[filename].push_back((Reference<ClientInfo>(
					    new ClientInfo(new DLApi(path, false /*unlink on load*/), path, useFutureVersion, 0))));
				}
			}
			// TIMER_END(3, Retrieving first external clients);

			TIMER_START(4);
			// initialize the first external thread for each library
			runOnExternalClients(0, extInitFunc, false, !ignoreExternalClientFailures);
			TIMER_END(4, Init first external clients(mostly loading library functions));

			TIMER_START(5);
			// generate copies of the first external thread for each library
			for (auto i : externalClients) {
				std::string path = i.second[0]->libPath;
				std::string filename = basename(path);
				bool useFutureVersion = i.second[0]->useFutureVersion;

				auto libCopies = copyExternalLibraryPerThread(
				    path, getBuildIdString(dynamic_cast<DLApi*>(externalClients[filename][0]->api)->retrieveBuildID()));

				for (int idx = 0; idx < libCopies.size(); ++idx) {
					externalClients[filename].push_back(Reference<ClientInfo>(new ClientInfo(
					    new DLApi(libCopies[idx].first, false /*unlink on load*/), path, useFutureVersion, idx)));
				}
			}
			TIMER_END(5, External client copy loop);
		}
		// TIMER_START(11);
		localClient->loadVersion();
		// TIMER_END(11, localClient->loadVersion());

		if (bypassMultiClientApi) {
			networkSetup = true;
		} else {
			TIMER_START(6);
			runOnExternalClientsThreadRange(extInitFunc, 1, threadCount, false, !ignoreExternalClientFailures);
			TIMER_END(6, Init remaining external clients(mostly loading library functions));

			std::string baseTraceFileId;
			if (apiVersion.hasTraceFileIdentifier()) {
				// TRACE_FILE_IDENTIFIER option is supported since 6.3
				baseTraceFileId = traceFileIdentifier.empty() ? format("%d", getpid()) : traceFileIdentifier;
			}

			MutexHolder holder(lock);
			TIMER_START(7);
			runOnExternalClientsAllThreads(
			    [this, transportId, baseTraceFileId](Reference<ClientInfo> client) {
				    for (auto option : options) {
					    client->api->setNetworkOption(option.first, option.second.castTo<StringRef>());
				    }
				    client->api->setNetworkOption(FDBNetworkOptions::EXTERNAL_CLIENT_TRANSPORT_ID,
				                                  std::to_string(transportId));
				    if (!baseTraceFileId.empty()) {
					    client->api->setNetworkOption(FDBNetworkOptions::TRACE_FILE_IDENTIFIER,
					                                  traceShareBaseNameAmongThreads
					                                      ? baseTraceFileId
					                                      : client->getTraceFileIdentifier(baseTraceFileId));
				    }
				    client->api->setupNetwork();
			    },
			    false,
			    !ignoreExternalClientFailures);
			TIMER_END(7, Setup External Networks);

			if (localClientDisabled && !hasNonFailedExternalClients()) {
				TraceEvent(SevWarn, "CannotSetupNetwork")
				    .detail("Reason", "Local client is disabled and all external clients failed");
				throw all_external_clients_failed();
			}

			networkSetup = true; // Needs to be guarded by mutex
		}

		options.clear();
		updateSupportedVersions();
	} catch (Error& e) {
		// Make sure all error and warning events are traced
		flushTraceFileVoid();
		throw e;
	}
	TIMER_END(1, MultiVersionApi::setupNetwork);
}

THREAD_FUNC_RETURN runNetworkThread(void* param) {
	try {
		((ClientInfo*)param)->api->runNetwork();
	} catch (Error& e) {
		TraceEvent(SevError, "ExternalRunNetworkError").error(e);
	} catch (std::exception& e) {
		TraceEvent(SevError, "ExternalRunNetworkError").error(unknown_error()).detail("RootException", e.what());
	} catch (...) {
		TraceEvent(SevError, "ExternalRunNetworkError").error(unknown_error());
	}

	TraceEvent("ExternalNetworkThreadTerminating");
	THREAD_RETURN;
}

void MultiVersionApi::runNetwork() {
	lock.enter();
	if (!networkSetup) {
		lock.leave();
		throw network_not_setup();
	}

	lock.leave();

	std::vector<THREAD_HANDLE> handles;
	if (!bypassMultiClientApi) {
		runOnExternalClientsAllThreads([&handles](Reference<ClientInfo> client) {
			ASSERT(client->external);
			std::string threadName = format("fdb-%s-%d", client->releaseVersion.c_str(), client->threadIndex);
			if (threadName.size() > 15) {
				threadName = format("fdb-%s", client->releaseVersion.c_str());
				if (threadName.size() > 15) {
					threadName = "fdb-external";
				}
			}
			handles.push_back(g_network->startThread(&runNetworkThread, client.getPtr(), 0, threadName.c_str()));
		});
	}

	try {
		localClient->api->runNetwork();
	} catch (const Error& e) {
		closeTraceFile();
		throw e;
	}

	for (auto h : handles) {
		waitThread(h);
	}

	TraceEvent("MultiVersionRunNetworkTerminating");
	closeTraceFile();
}

void MultiVersionApi::stopNetwork() {
	lock.enter();
	if (!networkSetup) {
		lock.leave();
		throw network_not_setup();
	}
	lock.leave();

	TraceEvent("MultiVersionStopNetwork");
	localClient->api->stopNetwork();

	if (!bypassMultiClientApi) {
		runOnExternalClientsAllThreads([](Reference<ClientInfo> client) { client->api->stopNetwork(); }, true);
	}
}

void MultiVersionApi::addNetworkThreadCompletionHook(void (*hook)(void*), void* hookParameter) {
	lock.enter();
	if (!networkSetup) {
		lock.leave();
		throw network_not_setup();
	}
	lock.leave();

	localClient->api->addNetworkThreadCompletionHook(hook, hookParameter);

	if (!bypassMultiClientApi) {
		runOnExternalClientsAllThreads([hook, hookParameter](Reference<ClientInfo> client) {
			client->api->addNetworkThreadCompletionHook(hook, hookParameter);
		});
	}
}

// Creates an IDatabase object that represents a connection to the cluster
Reference<IDatabase> MultiVersionApi::createDatabase(ClusterConnectionRecord const& connectionRecord) {
	lock.enter();
	if (!networkSetup) {
		lock.leave();
		throw network_not_setup();
	}
	if (localClientDisabled) {
		ASSERT(!bypassMultiClientApi);

		int threadIdx = nextThread;
		nextThread = (nextThread + 1) % threadCount;
		lock.leave();

		Reference<IDatabase> localDb = connectionRecord.createDatabase(localClient->api);
		return Reference<IDatabase>(
		    new MultiVersionDatabase(this, threadIdx, connectionRecord, Reference<IDatabase>(), localDb));
	}

	lock.leave();

	ASSERT_LE(threadCount, 1);

	Reference<IDatabase> localDb = connectionRecord.createDatabase(localClient->api);
	if (bypassMultiClientApi) {
		return localDb;
	} else {
		return Reference<IDatabase>(
		    new MultiVersionDatabase(this, 0, connectionRecord, Reference<IDatabase>(), localDb));
	}
}

Reference<IDatabase> MultiVersionApi::createDatabase(const char* clusterFilePath) {
	return createDatabase(ClusterConnectionRecord::fromFile(clusterFilePath));
}

Reference<IDatabase> MultiVersionApi::createDatabaseFromConnectionString(const char* connectionString) {
	return createDatabase(ClusterConnectionRecord::fromConnectionString(connectionString));
}

void MultiVersionApi::updateSupportedVersions() {
	if (networkSetup) {
		Standalone<VectorRef<uint8_t>> versionStr;

		// not mutating the client, so just call on one instance of each client version.
		// thread 0 always exists.
		runOnExternalClients(0, [&versionStr](Reference<ClientInfo> client) {
			const char* ver = client->api->getClientVersion();
			versionStr.append(versionStr.arena(), (uint8_t*)ver, (int)strlen(ver));
			versionStr.append(versionStr.arena(), (uint8_t*)";", 1);
		});

		if (!localClient->failed) {
			const char* local = localClient->api->getClientVersion();
			versionStr.append(versionStr.arena(), (uint8_t*)local, (int)strlen(local));
		} else {
			versionStr.resize(versionStr.arena(), std::max(0, versionStr.size() - 1));
		}

		setNetworkOption(FDBNetworkOptions::SUPPORTED_CLIENT_VERSIONS,
		                 StringRef(versionStr.begin(), versionStr.size()));
	}
}

// Must be called from the main thread
ACTOR Future<std::string> updateClusterSharedStateMapImpl(MultiVersionApi* self,
                                                          ClusterConnectionRecord connectionRecord,
                                                          ProtocolVersion dbProtocolVersion,
                                                          Reference<IDatabase> db) {
	// The cluster ID will be the connection record string (either a filename or the connection string itself)
	// in versions before we could read the cluster ID.
	state std::string clusterId = connectionRecord.toString();
	if (CLIENT_KNOBS->CLIENT_ENABLE_USING_CLUSTER_ID_KEY && dbProtocolVersion.hasClusterIdSpecialKey()) {
		state Reference<ITransaction> tr = db->createTransaction();
		loop {
			try {
				state ThreadFuture<Optional<Value>> clusterIdFuture = tr->get("\xff\xff/cluster_id"_sr);
				Optional<Value> clusterIdVal = wait(safeThreadFutureToFuture(clusterIdFuture));
				ASSERT(clusterIdVal.present());
				clusterId = clusterIdVal.get().toString();
				ASSERT(UID::fromString(clusterId).isValid());
				break;
			} catch (Error& e) {
				wait(safeThreadFutureToFuture(tr->onError(e)));
			}
		}
	}

	if (self->clusterSharedStateMap.find(clusterId) == self->clusterSharedStateMap.end()) {
		TraceEvent("CreatingClusterSharedState")
		    .detail("ClusterId", clusterId)
		    .detail("ProtocolVersion", dbProtocolVersion);
		self->clusterSharedStateMap[clusterId] = { db->createSharedState(), dbProtocolVersion };
	} else {
		auto& sharedStateInfo = self->clusterSharedStateMap[clusterId];
		if (sharedStateInfo.protocolVersion != dbProtocolVersion) {
			// This situation should never happen, because we are connecting to the same cluster,
			// so the protocol version must be the same
			TraceEvent(SevError, "ClusterStateProtocolVersionMismatch")
			    .detail("ClusterId", clusterId)
			    .detail("ProtocolVersionExpected", dbProtocolVersion)
			    .detail("ProtocolVersionFound", sharedStateInfo.protocolVersion);
			return clusterId;
		}

		TraceEvent("SettingClusterSharedState")
		    .detail("ClusterId", clusterId)
		    .detail("ProtocolVersion", dbProtocolVersion);

		state ThreadFuture<DatabaseSharedState*> entry = sharedStateInfo.sharedStateFuture;
		DatabaseSharedState* sharedState = wait(safeThreadFutureToFuture(entry));
		db->setSharedState(sharedState);
	}

	return clusterId;
}

// Must be called from the main thread
Future<std::string> MultiVersionApi::updateClusterSharedStateMap(ClusterConnectionRecord const& connectionRecord,
                                                                 ProtocolVersion dbProtocolVersion,
                                                                 Reference<IDatabase> db) {
	return updateClusterSharedStateMapImpl(this, connectionRecord, dbProtocolVersion, db);
}

// Must be called from the main thread
void MultiVersionApi::clearClusterSharedStateMapEntry(std::string clusterId, ProtocolVersion dbProtocolVersion) {
	auto mapEntry = clusterSharedStateMap.find(clusterId);
	// It can be that other database instances on the same cluster are already upgraded and thus
	// have cleared or even created a new shared object entry
	if (mapEntry == clusterSharedStateMap.end()) {
		TraceEvent("ClusterSharedStateMapEntryNotFound").detail("ClusterId", clusterId);
		return;
	}
	auto sharedStateInfo = mapEntry->second;
	if (sharedStateInfo.protocolVersion != dbProtocolVersion) {
		TraceEvent("ClusterSharedStateClearSkipped")
		    .detail("ClusterId", clusterId)
		    .detail("ProtocolVersionExpected", dbProtocolVersion)
		    .detail("ProtocolVersionFound", sharedStateInfo.protocolVersion);
		return;
	}
	auto ssPtr = sharedStateInfo.sharedStateFuture.get();
	ssPtr->delRef(ssPtr);
	clusterSharedStateMap.erase(mapEntry);
	TraceEvent("ClusterSharedStateCleared").detail("ClusterId", clusterId).detail("ProtocolVersion", dbProtocolVersion);
}

std::vector<std::string> parseOptionValues(std::string valueStr) {
	std::string specialCharacters = "\\";
	specialCharacters += ENV_VAR_PATH_SEPARATOR;

	std::vector<std::string> values;

	size_t index = 0;
	size_t nextIndex = 0;
	std::stringstream ss;
	while (true) {
		nextIndex = valueStr.find_first_of(specialCharacters, index);
		char c = nextIndex == valueStr.npos ? ENV_VAR_PATH_SEPARATOR : valueStr[nextIndex];

		if (c == '\\') {
			if (valueStr.size() == nextIndex + 1 || specialCharacters.find(valueStr[nextIndex + 1]) == valueStr.npos) {
				throw invalid_option_value();
			}

			ss << valueStr.substr(index, nextIndex - index);
			ss << valueStr[nextIndex + 1];

			index = nextIndex + 2;
		} else if (c == ENV_VAR_PATH_SEPARATOR) {
			ss << valueStr.substr(index, nextIndex - index);
			values.push_back(ss.str());
			ss.str(std::string());

			if (nextIndex == valueStr.npos) {
				break;
			}
			index = nextIndex + 1;
		} else {
			ASSERT(false);
		}
	}

	return values;
}

// This function sets all environment variable options which have not been set previously by a call to this
// function. If an option has multiple values and setting one of those values failed with an error, then only those
// options which were not successfully set will be set on subsequent calls.
void MultiVersionApi::loadEnvironmentVariableNetworkOptions() {
	if (envOptionsLoaded) {
		return;
	}

	for (auto option : FDBNetworkOptions::optionInfo) {
		if (!option.second.hidden) {
			std::string valueStr;
			try {
				if (platform::getEnvironmentVar(("FDB_NETWORK_OPTION_" + option.second.name).c_str(), valueStr)) {
					FDBOptionInfo::ParamType curParamType = option.second.paramType;
					for (auto value : parseOptionValues(valueStr)) {
						Standalone<StringRef> currentValue;
						int64_t intParamVal;
						if (curParamType == FDBOptionInfo::ParamType::Int) {
							try {
								size_t nextIdx;
								intParamVal = std::stoll(value, &nextIdx);
								if (nextIdx != value.length()) {
									throw invalid_option_value();
								}
							} catch (std::exception e) {
								TraceEvent(SevError, "EnvironmentVariableParseIntegerFailed")
								    .detail("Option", option.second.name)
								    .detail("Value", valueStr)
								    .detail("Error", e.what());
								throw invalid_option_value();
							}
							currentValue = StringRef(reinterpret_cast<uint8_t*>(&intParamVal), 8);
						} else {
							currentValue = StringRef(value);
						}
						{ // lock scope
							MutexHolder holder(lock);
							if (setEnvOptions[option.first].count(currentValue) == 0) {
								setNetworkOptionInternal(option.first, currentValue);
								setEnvOptions[option.first].insert(currentValue);
							}
						}
					}
				}
			} catch (Error& e) {
				TraceEvent(SevError, "EnvironmentVariableNetworkOptionFailed")
				    .error(e)
				    .detail("Option", option.second.name)
				    .detail("Value", valueStr);
				throw environment_variable_network_option_failed();
			}
		}
	}

	MutexHolder holder(lock);
	envOptionsLoaded = true;
}

MultiVersionApi::MultiVersionApi()
  : callbackOnMainThread(true), localClientDisabled(false), networkStartSetup(false), networkSetup(false),
    disableBypass(false), bypassMultiClientApi(false), externalClient(false), ignoreExternalClientFailures(false),
    failIncompatibleClient(false), retainClientLibCopies(false), apiVersion(0), threadCount(0), tmpDir("/tmp"),
    traceShareBaseNameAmongThreads(false), envOptionsLoaded(false) {}

MultiVersionApi* MultiVersionApi::api = new MultiVersionApi();

// ClientInfo
void ClientInfo::loadVersion() {
	std::string version = api->getClientVersion();
	if (version == "unknown") {
		protocolVersion = ProtocolVersion(0);
		releaseVersion = "unknown";
		return;
	}

	Standalone<ClientVersionRef> clientVersion = ClientVersionRef(StringRef(version));

	char* next;
	std::string protocolVersionStr = clientVersion.protocolVersion.toString();
	protocolVersion = ProtocolVersion(strtoull(protocolVersionStr.c_str(), &next, 16));

	ASSERT(protocolVersion.version() != 0 && protocolVersion.version() != ULLONG_MAX);
	ASSERT_EQ(next, &protocolVersionStr[protocolVersionStr.length()]);

	releaseVersion = clientVersion.clientVersion.toString();
}

bool ClientInfo::canReplace(Reference<ClientInfo> other) const {
	if (protocolVersion > other->protocolVersion) {
		return true;
	}

	if (protocolVersion == other->protocolVersion && !external) {
		return true;
	}

	return !protocolVersion.isCompatible(other->protocolVersion);
}

std::string ClientInfo::getTraceFileIdentifier(const std::string& baseIdentifier) {
	std::string versionStr = releaseVersion;
	std::replace(versionStr.begin(), versionStr.end(), '.', '_');
	return format("%s_v%st%d", baseIdentifier.c_str(), versionStr.c_str(), threadIndex);
}

// UNIT TESTS
TEST_CASE("/fdbclient/multiversionclient/EnvironmentVariableParsing") {
	auto vals = parseOptionValues("a");
	ASSERT(vals.size() == 1 && vals[0] == "a");

	vals = parseOptionValues("abcde");
	ASSERT(vals.size() == 1 && vals[0] == "abcde");

	vals = parseOptionValues("");
	ASSERT(vals.size() == 1 && vals[0] == "");

	vals = parseOptionValues("a:b:c:d:e");
	ASSERT(vals.size() == 5 && vals[0] == "a" && vals[1] == "b" && vals[2] == "c" && vals[3] == "d" && vals[4] == "e");

	vals = parseOptionValues("\\\\a\\::\\:b:\\\\");
	ASSERT(vals.size() == 3 && vals[0] == "\\a:" && vals[1] == ":b" && vals[2] == "\\");

	vals = parseOptionValues("abcd:");
	ASSERT(vals.size() == 2 && vals[0] == "abcd" && vals[1] == "");

	vals = parseOptionValues(":abcd");
	ASSERT(vals.size() == 2 && vals[0] == "" && vals[1] == "abcd");

	vals = parseOptionValues(":");
	ASSERT(vals.size() == 2 && vals[0] == "" && vals[1] == "");

	try {
		vals = parseOptionValues("\\x");
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_invalid_option_value);
	}

	return Void();
}

class ValidateFuture final : public ThreadCallback {
public:
	ValidateFuture(ThreadFuture<int> f, ErrorOr<int> expectedValue, std::set<int> legalErrors)
	  : f(f), expectedValue(expectedValue), legalErrors(legalErrors) {}

	bool canFire(int notMadeActive) const override { return true; }

	void fire(const Void& unused, int& userParam) override {
		ASSERT(!f.isError() && !expectedValue.isError() && f.get() == expectedValue.get());
		delete this;
	}

	void error(const Error& e, int& userParam) override {
		ASSERT(legalErrors.count(e.code()) > 0 ||
		       (f.isError() && expectedValue.isError() && f.getError().code() == expectedValue.getError().code()));
		delete this;
	}

private:
	ThreadFuture<int> f;
	ErrorOr<int> expectedValue;
	std::set<int> legalErrors;
};

struct FutureInfo {
	FutureInfo() {
		if (deterministicRandom()->coinflip()) {
			expectedValue = Error(deterministicRandom()->randomInt(1, 100));
		} else {
			expectedValue = deterministicRandom()->randomInt(0, 100);
		}
	}

	FutureInfo(ThreadFuture<int> future, ErrorOr<int> expectedValue, std::set<int> legalErrors = std::set<int>())
	  : future(future), expectedValue(expectedValue), legalErrors(legalErrors) {}

	void validate() {
		int userParam;
		future.callOrSetAsCallback(new ValidateFuture(future, expectedValue, legalErrors), userParam, 0);
	}

	ThreadFuture<int> future;
	ErrorOr<int> expectedValue;
	std::set<int> legalErrors;
	std::vector<THREAD_HANDLE> threads;
};

FutureInfo createVarOnMainThread(bool canBeNever = true) {
	FutureInfo f;

	if (deterministicRandom()->coinflip()) {
		f.future = onMainThread([f, canBeNever]() {
			Future<Void> sleep;
			if (canBeNever && deterministicRandom()->coinflip()) {
				sleep = Never();
			} else {
				sleep = delay(0.1 * deterministicRandom()->random01());
			}

			if (f.expectedValue.isError()) {
				return tagError<int>(sleep, f.expectedValue.getError());
			} else {
				return tag(sleep, f.expectedValue.get());
			}
		});
	} else if (f.expectedValue.isError()) {
		f.future = f.expectedValue.getError();
	} else {
		f.future = f.expectedValue.get();
	}

	return f;
}

THREAD_FUNC setAbort(void* arg) {
	threadSleep(0.1 * deterministicRandom()->random01());
	try {
		((ThreadSingleAssignmentVar<Void>*)arg)->send(Void());
		((ThreadSingleAssignmentVar<Void>*)arg)->delref();
	} catch (Error& e) {
		printf("Caught error in setAbort: %s\n", e.name());
		ASSERT(false);
	}
	THREAD_RETURN;
}

THREAD_FUNC releaseMem(void* arg) {
	threadSleep(0.1 * deterministicRandom()->random01());
	try {
		// Must get for releaseMemory to work
		((ThreadSingleAssignmentVar<int>*)arg)->get();
	} catch (Error&) {
		// Swallow
	}
	try {
		((ThreadSingleAssignmentVar<int>*)arg)->releaseMemory();
	} catch (Error& e) {
		printf("Caught error in releaseMem: %s\n", e.name());
		ASSERT(false);
	}
	THREAD_RETURN;
}

THREAD_FUNC destroy(void* arg) {
	threadSleep(0.1 * deterministicRandom()->random01());
	try {
		((ThreadSingleAssignmentVar<int>*)arg)->cancel();
	} catch (Error& e) {
		printf("Caught error in destroy: %s\n", e.name());
		ASSERT(false);
	}
	THREAD_RETURN;
}

THREAD_FUNC cancel(void* arg) {
	threadSleep(0.1 * deterministicRandom()->random01());
	try {
		((ThreadSingleAssignmentVar<int>*)arg)->addref();
		destroy(arg);
	} catch (Error& e) {
		printf("Caught error in cancel: %s\n", e.name());
		ASSERT(false);
	}
	THREAD_RETURN;
}

ACTOR Future<Void> checkUndestroyedFutures(std::vector<ThreadSingleAssignmentVar<int>*> undestroyed) {
	state int fNum;
	state ThreadSingleAssignmentVar<int>* f;
	state double start = now();

	for (fNum = 0; fNum < undestroyed.size(); ++fNum) {
		f = undestroyed[fNum];

		while (!f->isReady() && start + 5 >= now()) {
			wait(delay(1.0));
		}

		ASSERT(f->isReady());
	}

	wait(delay(1.0));

	for (fNum = 0; fNum < undestroyed.size(); ++fNum) {
		f = undestroyed[fNum];

		ASSERT_EQ(f->debugGetReferenceCount(), 1);
		ASSERT(f->isReady());

		f->cancel();
	}

	return Void();
}

// Common code for tests of single assignment vars. Tests both correctness and thread safety.
// T should be a class that has a static method with the following signature:
//
//     static FutureInfo createThreadFuture(FutureInfo f);
//
// See AbortableTest for an example T type
template <class T>
THREAD_FUNC runSingleAssignmentVarTest(void* arg) {
	noUnseed = true;

// This test intentionally leaks memory
#ifdef ADDRESS_SANITIZER
	__lsan::ScopedDisabler disableLeakChecks;
#endif

	volatile bool* done = (volatile bool*)arg;
	try {
		for (int i = 0; i < 25; ++i) {
			FutureInfo f = createVarOnMainThread(false);
			FutureInfo tf = T::createThreadFuture(f);
			tf.validate();

			tf.future.extractPtr(); // leaks
			for (auto t : tf.threads) {
				waitThread(t);
			}
		}

		for (int numRuns = 0; numRuns < 25; ++numRuns) {
			std::vector<ThreadSingleAssignmentVar<int>*> undestroyed;
			std::vector<THREAD_HANDLE> threads;
			for (int i = 0; i < 10; ++i) {
				FutureInfo f = createVarOnMainThread();
				f.legalErrors.insert(error_code_operation_cancelled);

				FutureInfo tf = T::createThreadFuture(f);
				for (auto t : tf.threads) {
					threads.push_back(t);
				}

				tf.legalErrors.insert(error_code_operation_cancelled);
				tf.validate();

				auto tfp = tf.future.extractPtr();

				if (deterministicRandom()->coinflip()) {
					if (deterministicRandom()->coinflip()) {
						threads.push_back(g_network->startThread(releaseMem, tfp, 0, "fdb-release-mem"));
					}
					threads.push_back(g_network->startThread(cancel, tfp, 0, "fdb-cancel"));
					undestroyed.push_back((ThreadSingleAssignmentVar<int>*)tfp);
				} else {
					threads.push_back(g_network->startThread(destroy, tfp, 0, "fdb-destroy"));
				}
			}

			for (auto t : threads) {
				waitThread(t);
			}

			ThreadFuture<Void> checkUndestroyed =
			    onMainThread([undestroyed]() { return checkUndestroyedFutures(undestroyed); });

			checkUndestroyed.blockUntilReady();
		}

		onMainThreadVoid([done]() { *done = true; });
	} catch (Error& e) {
		printf("Caught error in test: %s\n", e.name());
		*done = true;
		ASSERT(false);
	}

	THREAD_RETURN;
}

struct AbortableTest {
	static FutureInfo createThreadFuture(FutureInfo f) {
		ThreadSingleAssignmentVar<Void>* abort = new ThreadSingleAssignmentVar<Void>();
		abort->addref(); // this leaks if abort is never set

		auto newFuture =
		    FutureInfo(abortableFuture(f.future, ThreadFuture<Void>(abort)), f.expectedValue, f.legalErrors);

		if (!abort->isReady() && deterministicRandom()->coinflip()) {
			ASSERT_EQ(abort->status, ThreadSingleAssignmentVarBase::Unset);
			newFuture.threads.push_back(g_network->startThread(setAbort, abort, 0, "fdb-abort"));
		}

		newFuture.legalErrors.insert(error_code_cluster_version_changed);
		return newFuture;
	}
};

TEST_CASE("fdbclient/multiversionclient/AbortableSingleAssignmentVar") {
	state volatile bool done = false;
	state THREAD_HANDLE thread = g_network->startThread(runSingleAssignmentVarTest<AbortableTest>, (void*)&done);

	while (!done) {
		wait(delay(1.0));
	}

	waitThread(thread);

	return Void();
}

class CAPICallback final : public ThreadCallback {
public:
	CAPICallback(void (*callbackf)(FdbCApi::FDBFuture*, void*), FdbCApi::FDBFuture* f, void* userdata)
	  : callbackf(callbackf), f(f), userdata(userdata) {}

	bool canFire(int notMadeActive) const override { return true; }
	void fire(const Void& unused, int& userParam) override {
		(*callbackf)(f, userdata);
		delete this;
	}
	void error(const Error& e, int& userParam) override {
		(*callbackf)(f, userdata);
		delete this;
	}

private:
	void (*callbackf)(FdbCApi::FDBFuture*, void*);
	FdbCApi::FDBFuture* f;
	void* userdata;
};

struct DLTest {
	static FutureInfo createThreadFuture(FutureInfo f) {
		return FutureInfo(
		    toThreadFuture<int>(getApi(),
		                        (FdbCApi::FDBFuture*)f.future.extractPtr(),
		                        [](FdbCApi::FDBFuture* f, FdbCApi* api) {
			                        ASSERT_GE(((ThreadSingleAssignmentVar<int>*)f)->debugGetReferenceCount(), 1);
			                        return ((ThreadSingleAssignmentVar<int>*)f)->get();
		                        }),
		    f.expectedValue,
		    f.legalErrors);
	}

	static Reference<FdbCApi> getApi() {
		static Reference<FdbCApi> api;
		if (!api) {
			api = makeReference<FdbCApi>();

			// Functions needed for DLSingleAssignmentVar
			api->futureSetCallback = [](FdbCApi::FDBFuture* f, FdbCApi::FDBCallback callback, void* callbackParameter) {
				try {
					CAPICallback* cb = new CAPICallback(callback, f, callbackParameter);
					int ignore;
					((ThreadSingleAssignmentVarBase*)f)->callOrSetAsCallback(cb, ignore, 0);
					return FdbCApi::fdb_error_t(error_code_success);
				} catch (Error& e) {
					return FdbCApi::fdb_error_t(e.code());
				}
			};
			api->futureCancel = [](FdbCApi::FDBFuture* f) {
				((ThreadSingleAssignmentVarBase*)f)->addref();
				((ThreadSingleAssignmentVarBase*)f)->cancel();
			};
			api->futureGetError = [](FdbCApi::FDBFuture* f) {
				return FdbCApi::fdb_error_t(((ThreadSingleAssignmentVarBase*)f)->getErrorCode());
			};
			api->futureDestroy = [](FdbCApi::FDBFuture* f) { ((ThreadSingleAssignmentVarBase*)f)->cancel(); };
		}

		return api;
	}
};

TEST_CASE("fdbclient/multiversionclient/DLSingleAssignmentVar") {
	state volatile bool done = false;

	MultiVersionApi::api->callbackOnMainThread = true;
	state THREAD_HANDLE thread = g_network->startThread(runSingleAssignmentVarTest<DLTest>, (void*)&done);

	while (!done) {
		wait(delay(1.0));
	}

	waitThread(thread);

	done = false;
	MultiVersionApi::api->callbackOnMainThread = false;
	thread = g_network->startThread(runSingleAssignmentVarTest<DLTest>, (void*)&done);

	while (!done) {
		wait(delay(1.0));
	}

	waitThread(thread);

	return Void();
}

struct MapTest {
	static FutureInfo createThreadFuture(FutureInfo f) {
		FutureInfo newFuture;
		newFuture.legalErrors = f.legalErrors;
		newFuture.future = mapThreadFuture<int, int>(f.future, [f, newFuture](ErrorOr<int> v) {
			if (v.isError()) {
				ASSERT(f.legalErrors.count(v.getError().code()) > 0 ||
				       (f.expectedValue.isError() && f.expectedValue.getError().code() == v.getError().code()));
			} else {
				ASSERT(!f.expectedValue.isError() && f.expectedValue.get() == v.get());
			}

			return newFuture.expectedValue;
		});

		return newFuture;
	}
};

TEST_CASE("fdbclient/multiversionclient/MapSingleAssignmentVar") {
	state volatile bool done = false;
	state THREAD_HANDLE thread = g_network->startThread(runSingleAssignmentVarTest<MapTest>, (void*)&done);

	while (!done) {
		wait(delay(1.0));
	}

	waitThread(thread);

	return Void();
}

struct FlatMapTest {
	static FutureInfo createThreadFuture(FutureInfo f) {
		FutureInfo mapFuture = createVarOnMainThread();

		return FutureInfo(
		    flatMapThreadFuture<int, int>(
		        f.future,
		        [f, mapFuture](ErrorOr<int> v) {
			        if (v.isError()) {
				        ASSERT(f.legalErrors.count(v.getError().code()) > 0 ||
				               (f.expectedValue.isError() && f.expectedValue.getError().code() == v.getError().code()));
			        } else {
				        ASSERT(!f.expectedValue.isError() && f.expectedValue.get() == v.get());
			        }

			        if (mapFuture.expectedValue.isError() && deterministicRandom()->coinflip()) {
				        return ErrorOr<ThreadFuture<int>>(mapFuture.expectedValue.getError());
			        } else {
				        return ErrorOr<ThreadFuture<int>>(mapFuture.future);
			        }
		        }),
		    mapFuture.expectedValue,
		    f.legalErrors);
	}
};

TEST_CASE("fdbclient/multiversionclient/FlatMapSingleAssignmentVar") {
	state volatile bool done = false;
	state THREAD_HANDLE thread = g_network->startThread(runSingleAssignmentVarTest<FlatMapTest>, (void*)&done);

	while (!done) {
		wait(delay(1.0));
	}

	waitThread(thread);

	return Void();
}
