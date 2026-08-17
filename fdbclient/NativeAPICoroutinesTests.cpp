/*
 * NativeAPICoroutinesTests.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbrpc/FailureMonitor.h"
#include "flow/Coroutines.h"
#include "flow/UnitTest.h"

#include <initializer_list>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using SplitMetricsResult = Optional<Standalone<VectorRef<KeyRef>>>;
using WaitForCommittedVersionSignature = Future<Version> (*)(Database const&, Version const&, SpanContext const&);
using SplitStorageMetricsSignature = Future<SplitMetricsResult> (*)(std::vector<KeyRangeLocationInfo> const&,
                                                                    KeyRange const&,
                                                                    StorageMetrics const&,
                                                                    StorageMetrics const&,
                                                                    Optional<int> const&);

static_assert(std::is_same_v<decltype(&waitForCommittedVersion), WaitForCommittedVersionSignature>);
static_assert(std::is_same_v<decltype(&splitStorageMetricsWithLocations), SplitStorageMetricsSignature>);

constexpr double requestTimeout = 5.0;

Future<SplitMetricsRequest> nextSplitRequest(FutureStream<SplitMetricsRequest> requests) {
	co_return co_await requests;
}

StorageServerInterface makeSplitMetricsTestInterface() {
	StorageServerInterface interface;
	interface.initEndpoints();
	return interface;
}

class LocalEndpointHealth : NonCopyable {
public:
	explicit LocalEndpointHealth(NetworkAddress address)
	  : monitor(IFailureMonitor::failureMonitor()), address(address), previousStatus(monitor.getState(address)) {
		monitor.setStatus(address, FailureStatus(false));
	}

	~LocalEndpointHealth() { monitor.setStatus(address, previousStatus); }

private:
	IFailureMonitor& monitor;
	const NetworkAddress address;
	const FailureStatus previousStatus;
};

class SplitMetricsTestServer {
public:
	explicit SplitMetricsTestServer(KeyRange range)
	  : interface(makeSplitMetricsTestInterface()),
	    endpointHealth(interface.splitMetrics.getEndpoint().getPrimaryAddress()), range(std::move(range)) {
		locations = makeReference<LocationInfo>(std::vector<Reference<ReferencedInterface<StorageServerInterface>>>{
		    makeReference<ReferencedInterface<StorageServerInterface>>(interface) });
	}

	KeyRangeLocationInfo location() const { return KeyRangeLocationInfo(range, locations); }

	Future<SplitMetricsRequest> nextRequest() const {
		return timeoutError(
		    nextSplitRequest(interface.splitMetrics.getFuture()), requestTimeout, TaskPriority::DataDistribution);
	}

	bool hasPendingRequests() const { return interface.splitMetrics.getFuture().isReady(); }

private:
	StorageServerInterface interface;
	LocalEndpointHealth endpointHealth;
	KeyRange range;
	Reference<LocationInfo> locations;
};

StorageMetrics testMetrics(int64_t bytes) {
	StorageMetrics metrics;
	metrics.bytes = bytes;
	metrics.bytesWrittenPerKSecond = bytes + 1;
	metrics.iosPerKSecond = bytes + 2;
	metrics.bytesReadPerKSecond = bytes + 3;
	metrics.opsReadPerKSecond = bytes + 4;
	return metrics;
}

void checkSplitRequest(SplitMetricsRequest const& request,
                       KeyRangeRef keys,
                       StorageMetrics const& limit,
                       StorageMetrics const& used,
                       StorageMetrics const& estimated,
                       bool isLastShard,
                       Optional<int> minSplitBytes) {
	ASSERT(request.keys == keys);
	ASSERT(request.limits == limit);
	ASSERT(request.used == used);
	ASSERT(request.estimated == estimated);
	ASSERT(request.isLastShard == isLastShard);
	ASSERT(request.minSplitBytes == minSplitBytes);
}

void sendSplitReply(SplitMetricsRequest const& request,
                    std::initializer_list<KeyRef> splits,
                    StorageMetrics const& used,
                    bool more) {
	SplitMetricsReply reply;
	for (KeyRef key : splits) {
		reply.splits.push_back_deep(reply.splits.arena(), key);
	}
	reply.used = used;
	reply.more = more;
	request.reply.send(std::move(reply));
}

Future<Standalone<VectorRef<KeyRef>>> collectPaginatedSplits() {
	SplitMetricsTestServer first(KeyRangeRef("a"_sr, "m"_sr));
	SplitMetricsTestServer second(KeyRangeRef("m"_sr, "z"_sr));
	const StorageMetrics limit = testMetrics(100);
	const StorageMetrics estimated = testMetrics(350);
	const StorageMetrics firstUsed = testMetrics(11);
	const StorageMetrics secondUsed = testMetrics(17);
	const Optional<int> minSplitBytes = 7;
	Future<SplitMetricsResult> pending;
	{
		std::vector<KeyRangeLocationInfo> locations{ first.location(), second.location() };
		KeyRange keys = KeyRangeRef("a"_sr, "z"_sr);
		StorageMetrics callerLimit = limit;
		StorageMetrics callerEstimated = estimated;
		Optional<int> callerMinSplitBytes = minSplitBytes;
		pending = splitStorageMetricsWithLocations(locations, keys, callerLimit, callerEstimated, callerMinSplitBytes);
		ASSERT(!pending.isReady());
		// The exported const-reference entry point must own its arguments before the first suspension.
		locations.clear();
		keys = KeyRange();
		callerLimit = StorageMetrics();
		callerEstimated = StorageMetrics();
		callerMinSplitBytes.reset();
	}

	{
		SplitMetricsRequest request = co_await first.nextRequest();
		checkSplitRequest(
		    request, KeyRangeRef("a"_sr, "m"_sr), limit, StorageMetrics(), estimated, false, minSplitBytes);
		sendSplitReply(request, { "c"_sr, "f"_sr }, firstUsed, true);
	}
	{
		SplitMetricsRequest request = co_await first.nextRequest();
		checkSplitRequest(request, KeyRangeRef("f"_sr, "m"_sr), limit, firstUsed, estimated, false, minSplitBytes);
		sendSplitReply(request, { "j"_sr }, secondUsed, false);
	}
	{
		SplitMetricsRequest request = co_await second.nextRequest();
		checkSplitRequest(request, KeyRangeRef("m"_sr, "z"_sr), limit, secondUsed, estimated, true, minSplitBytes);
		sendSplitReply(request, { "t"_sr, "w"_sr }, limit, false);
	}

	SplitMetricsResult result = co_await timeoutError(pending, requestTimeout, TaskPriority::DataDistribution);
	ASSERT(result.present());
	ASSERT(!first.hasPendingRequests());
	ASSERT(!second.hasPendingRequests());
	co_return std::move(result.get());
}

Future<Void> checkRetryableSplitError(Error error) {
	SplitMetricsTestServer server(KeyRangeRef("a"_sr, "z"_sr));
	Future<SplitMetricsResult> pending = splitStorageMetricsWithLocations(
	    { server.location() }, KeyRangeRef("a"_sr, "z"_sr), testMetrics(100), testMetrics(350), Optional<int>(7));
	SplitMetricsRequest request = co_await server.nextRequest();
	request.reply.sendError(error);
	SplitMetricsResult result = co_await timeoutError(pending, requestTimeout, TaskPriority::DataDistribution);
	ASSERT(!result.present());
	ASSERT(!server.hasPendingRequests());
}

} // namespace

TEST_CASE("/fdbclient/NativeAPI/splitStorageMetrics/paginationAndOwnership") {
	Standalone<VectorRef<KeyRef>> result = co_await collectPaginatedSplits();
	const std::vector<KeyRef> expected{ "a"_sr, "c"_sr, "f"_sr, "j"_sr, "t"_sr, "w"_sr, "z"_sr };
	ASSERT(result.size() == expected.size());
	for (int i = 0; i < result.size(); ++i) {
		ASSERT(result[i] == expected[i]);
	}
}

TEST_CASE("/fdbclient/NativeAPI/splitStorageMetrics/wrongShardServer") {
	return checkRetryableSplitError(wrong_shard_server());
}

TEST_CASE("/fdbclient/NativeAPI/splitStorageMetrics/allAlternativesFailed") {
	return checkRetryableSplitError(all_alternatives_failed());
}

TEST_CASE("/fdbclient/NativeAPI/splitStorageMetrics/cancellation") {
	SplitMetricsTestServer server(KeyRangeRef("a"_sr, "z"_sr));
	Future<SplitMetricsResult> pending = splitStorageMetricsWithLocations(
	    { server.location() }, KeyRangeRef("a"_sr, "z"_sr), testMetrics(100), testMetrics(350), Optional<int>(7));
	SplitMetricsRequest request = co_await server.nextRequest();
	ASSERT(!pending.isReady());
	pending.cancel();
	ASSERT(pending.isReady() && pending.isError() && pending.getError().code() == error_code_actor_cancelled);
	if (!request.reply.isSet()) {
		sendSplitReply(request, { "m"_sr }, testMetrics(11), true);
	}
	co_await delay(0.0, TaskPriority::DataDistribution);
	ASSERT(!server.hasPendingRequests());
}

void forceLinkNativeAPICoroutinesTests() {}
