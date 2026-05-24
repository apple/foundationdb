/*
 * NativeCdc.cpp
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

#include <utility>
#include <vector>

#include "fdbclient/NativeCdc.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/tester/workloads.h"

struct NativeCdcWorkload : TestWorkload {
	static constexpr auto NAME = "NativeCdc";

	explicit NativeCdcWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return run(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<std::pair<Tag, Version>> getPersistedRoute(Database cx, CDCStreamId streamId) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> keys = co_await tr.get(cdcStreamKeyFor(streamId));
				Optional<Value> minVersion = co_await tr.get(cdcMinVersionKeyFor(streamId));
				RangeResult history = co_await tr.getRange(cdcTagHistoryRangeFor(streamId), 2);
				ASSERT(keys.present());
				ASSERT(minVersion.present());
				ASSERT(history.size() == 1);
				const auto [historyStreamId, historyVersion, tag] = decodeCDCTagHistoryKey(history[0].key);
				ASSERT(historyStreamId == streamId);
				ASSERT(historyVersion == decodeCDCMinVersionValue(minVersion.get()));
				co_return std::make_pair(tag, historyVersion);
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Version> getPersistedMinVersion(Database cx, CDCStreamId streamId) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> minVersion = co_await tr.get(cdcMinVersionKeyFor(streamId));
				ASSERT(minVersion.present());
				co_return decodeCDCMinVersionValue(minVersion.get());
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> run(Database cx) {
		const Key firstName = "native-cdc-first"_sr;
		const Key secondName = "native-cdc-second"_sr;
		const KeyRange firstRange(KeyRangeRef("a"_sr, "m"_sr));
		const KeyRange conflictingRange(KeyRangeRef("a"_sr, "z"_sr));
		const KeyRange secondRange(KeyRangeRef("g"_sr, "z"_sr));

		const CDCStreamId firstId = co_await registerNativeCdcStream(cx, firstName, firstRange);
		ASSERT(co_await registerNativeCdcStream(cx, firstName, firstRange) == firstId);

		bool conflictingDuplicateRejected = false;
		try {
			co_await registerNativeCdcStream(cx, firstName, conflictingRange);
		} catch (Error& e) {
			if (e.code() == error_code_client_invalid_operation) {
				conflictingDuplicateRejected = true;
			} else {
				throw;
			}
		}
		ASSERT(conflictingDuplicateRejected);

		const auto firstRoute = co_await getPersistedRoute(cx, firstId);
		ASSERT(firstRoute.first.locality == tagLocalityCDC);

		std::vector<NativeCdcStreamInfo> streams = co_await listNativeCdcStreams(cx);
		ASSERT(streams.size() == 1);
		ASSERT(streams[0].name == firstName);
		ASSERT(streams[0].streamId == firstId);
		ASSERT(streams[0].keys == firstRange);
		ASSERT(streams[0].minVersion == firstRoute.second);

		const Version firstConsumedThrough = firstRoute.second + 5;
		const Version firstAckMinVersion = firstConsumedThrough + 1;
		ASSERT(co_await acknowledgeNativeCdcStream(cx, firstId, firstConsumedThrough) == firstAckMinVersion);
		ASSERT(co_await acknowledgeNativeCdcStream(cx, firstId, firstRoute.second) == firstAckMinVersion);
		streams = co_await listNativeCdcStreams(cx);
		ASSERT(streams.size() == 1);
		ASSERT(streams[0].minVersion == firstAckMinVersion);

		co_await removeNativeCdcStream(cx, firstName);
		ASSERT((co_await listNativeCdcStreams(cx)).empty());
		const Version retiredConsumedThrough = firstConsumedThrough + 5;
		const Version retiredAckMinVersion = retiredConsumedThrough + 1;
		ASSERT(co_await acknowledgeNativeCdcStream(cx, firstId, retiredConsumedThrough) == retiredAckMinVersion);
		ASSERT(co_await getPersistedMinVersion(cx, firstId) == retiredAckMinVersion);

		const CDCStreamId secondId = co_await registerNativeCdcStream(cx, secondName, secondRange);
		const auto secondRoute = co_await getPersistedRoute(cx, secondId);
		ASSERT(secondId > firstId);
		ASSERT(secondRoute.first != firstRoute.first);

		co_await removeNativeCdcStream(cx, secondName);
	}
};

WorkloadFactory<NativeCdcWorkload> NativeCdcWorkloadFactory;
