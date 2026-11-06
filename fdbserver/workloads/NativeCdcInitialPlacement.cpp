/*
 * NativeCdcInitialPlacement.cpp
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

#include <set>
#include <utility>
#include <vector>

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeCdc.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/NativeCdcMetadata.h"
#include "fdbserver/tester/workloads.h"
#include "flow/CodeProbe.h"

class NativeCdcInitialPlacementWorkload : public TestWorkload {
	const Key coldName = "native-cdc-placement/cold"_sr;
	const Key hotName = "native-cdc-placement/hot"_sr;
	const Key duplicateName = "native-cdc-placement/duplicate"_sr;
	const Key placedName = "native-cdc-placement/placed"_sr;
	const Key coldKey = "native-cdc-placement/data/cold"_sr;
	const Key hotKey = "native-cdc-placement/data/hot"_sr;
	const Key placedKey = "native-cdc-placement/data/placed"_sr;
	const double operationTimeout;

	static Future<Version> writeValue(Database cx, Key key, Value value) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.set(key, value);
				co_await tr.commit();
				co_return tr.getCommittedVersion();
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	static Future<Void> produceHotWrites(Database cx, Key key) {
		int sequence = 0;
		while (true) {
			const Value value = StringRef(std::string(32768, 'a' + (++sequence % 26)));
			co_await writeValue(cx, key, value);
			co_await delay(0.05);
		}
	}

	static Future<Tag> readTag(Database cx, CDCStreamId streamId) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				const RangeResult history = co_await tr.getRange(cdcTagHistoryRangeFor(streamId), 2);
				ASSERT_EQ(history.size(), 1);
				ASSERT(!history.more);
				ASSERT(history.front().value.empty());
				co_return decodeCDCTagHistoryKey(history.front().key).tag;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	static Future<Optional<Version>> usableLoadVersion(Transaction* tr, Tag coldTag, Tag hotTag) {
		const Value generation = (co_await tr->get(cdcProxyAssignmentChangeKey)).orDefault(Value());
		const Version version = co_await tr->getReadVersion();
		const Optional<Value> coldValue = co_await tr->get(cdcTagLoadKeyFor(coldTag));
		const Optional<Value> hotValue = co_await tr->get(cdcTagLoadKeyFor(hotTag));
		if (!coldValue.present() || !hotValue.present()) {
			co_return Optional<Version>();
		}
		const auto cold = decodeCDCTagLoadValue(coldValue.get());
		const auto hot = decodeCDCTagLoadValue(hotValue.get());
		for (const auto& sample : { cold, hot }) {
			if (sample.assignmentChange != generation || sample.sampleVersion < 0 || sample.sampleVersion > version ||
			    sample.validThrough < version || sample.bytesWrittenPerKSecond < 0) {
				co_return Optional<Version>();
			}
		}
		co_return hot.bytesWrittenPerKSecond > cold.bytesWrittenPerKSecond ? Optional<Version>(hot.sampleVersion)
		                                                                   : Optional<Version>();
	}

	Future<CDCStreamId> registerWithLoad(Database cx, Tag coldTag, Tag hotTag) {
		const std::vector<KeyRange> ranges{ KeyRange(KeyRangeRef(placedKey, keyAfter(placedKey))) };
		Transaction tr(cx);
		std::set<CDCStreamId> attemptedIds;
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				const Optional<Value> existing = co_await tr.get(cdcStreamNameKeyFor(placedName));
				if (existing.present()) {
					// A successful but ambiguous commit invalidates its own sample generation.
					const CDCStreamId streamId = decodeCDCStreamNameValue(existing.get());
					ASSERT(attemptedIds.contains(streamId));
					co_return streamId;
				}
				// Guard the actual registration snapshot: a separate readiness check can expire on recovery.
				if (!(co_await usableLoadVersion(&tr, coldTag, hotTag)).present() ||
				    cx->clientInfo->get().cdcProxies.empty()) {
					tr.reset();
					co_await delay(0.1);
					continue;
				}
				const auto result = co_await prepareNativeCdcStreamRegistration(
				    &tr, placedName, ranges, cx->clientInfo->get().cdcProxies.front().id());
				ASSERT(result.requiresCommit);
				attemptedIds.insert(result.streamId);
				co_await tr.commit();
				co_return result.streamId;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	Future<Void> verifyLiveMovesDisabled(Database cx,
	                                     std::vector<std::pair<CDCStreamId, Tag>> assignments,
	                                     Tag coldTag,
	                                     Tag hotTag) {
		ASSERT(SERVER_KNOBS->NATIVE_CDC_TAG_BALANCING_ENABLED);
		ASSERT(!SERVER_KNOBS->NATIVE_CDC_LIVE_RETAGGING_ENABLED);
		// Two disjoint hot ranges now share the old cold tag; moving the placed stream would reduce its load.
		Future<Void> firstProducer = produceHotWrites(cx, coldKey);
		Future<Void> secondProducer = produceHotWrites(cx, placedKey);
		Transaction tr(cx);
		Version previousSample = invalidVersion;
		int sampledPasses = 0;
		while (sampledPasses < 3) {
			ASSERT(!firstProducer.isReady());
			ASSERT(!secondProducer.isReady());
			for (const auto& [streamId, tag] : assignments) {
				ASSERT_EQ(co_await readTag(cx, streamId), tag);
			}
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				const Optional<Version> sample = co_await usableLoadVersion(&tr, hotTag, coldTag);
				if (sample.present() && sample.get() > previousSample) {
					previousSample = sample.get();
					++sampledPasses;
				}
				tr.reset();
				co_await delay(SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_INTERVAL);
			} catch (Error& e) {
				error = e;
			}
			if (error.isValid()) {
				co_await tr.onError(error);
			}
		}
		for (const auto& [streamId, tag] : assignments) {
			ASSERT_EQ(co_await readTag(cx, streamId), tag);
		}
		CODE_PROBE(true, "Native CDC placement samples do not enable live retagging");
	}

	Future<Void> run(Database cx) {
		const std::vector<KeyRange> coldRanges{ KeyRange(KeyRangeRef(coldKey, keyAfter(coldKey))) };
		const std::vector<KeyRange> hotRanges{ KeyRange(KeyRangeRef(hotKey, keyAfter(hotKey))) };
		const CDCStreamId cold = co_await registerNativeCdcStreamClient(cx, coldName, coldRanges);
		ASSERT_EQ(cx->clientInfo->get().nativeCdcTagCount, 2);
		const CDCStreamId hot = co_await registerNativeCdcStreamClient(cx, hotName, hotRanges);
		const CDCStreamId duplicate = co_await registerNativeCdcStreamClient(cx, duplicateName, coldRanges);
		const Tag coldTag = co_await readTag(cx, cold);
		const Tag hotTag = co_await readTag(cx, hot);
		ASSERT_NE(coldTag, hotTag);
		ASSERT_EQ(co_await readTag(cx, duplicate), coldTag);
		Future<Void> producer = produceHotWrites(cx, hotKey);
		const CDCStreamId placed = co_await timeoutError(registerWithLoad(cx, coldTag, hotTag), operationTimeout);
		producer.cancel();
		ASSERT_EQ(co_await readTag(cx, placed), coldTag);
		// Existing streams retain their original tag; placement requires no history cutover protocol.
		ASSERT_EQ(co_await readTag(cx, cold), coldTag);
		ASSERT_EQ(co_await readTag(cx, duplicate), coldTag);
		ASSERT_EQ(co_await readTag(cx, hot), hotTag);
		const std::vector<std::pair<CDCStreamId, Tag>> assignments{
			{ cold, coldTag }, { hot, hotTag }, { duplicate, coldTag }, { placed, coldTag }
		};
		co_await timeoutError(verifyLiveMovesDisabled(cx, assignments, coldTag, hotTag), operationTimeout);
		Reference<NativeCdcConsumer> consumer = co_await createNativeCdcConsumer(cx, placedName);
		const Value marker = "placed-stream-delivery"_sr;
		const Version committed = co_await writeValue(cx, placedKey, marker);
		bool found = false;
		while (!found) {
			const CDCConsumeReply reply = co_await timeoutError(consumer->consume(), operationTimeout);
			for (const auto& versioned : reply.mutations) {
				if (versioned.version == committed) {
					ASSERT_EQ(versioned.mutations.size(), 1);
					ASSERT_EQ(versioned.mutations.front().type, MutationRef::SetValue);
					ASSERT_EQ(versioned.mutations.front().param1, placedKey);
					ASSERT_EQ(versioned.mutations.front().param2, marker);
					found = true;
				}
			}
		}
		co_await consumer->acknowledge();
		for (const auto& name : { placedName, duplicateName, hotName, coldName }) {
			co_await removeNativeCdcStreamClient(cx, name);
		}
		CODE_PROBE(true, "Native CDC initial placement favors measured load over stream count");
		TraceEvent("NativeCdcInitialPlacementVerified").detail("PlacedStream", placed).detail("Tag", coldTag);
	}

public:
	static constexpr auto NAME = "NativeCdcInitialPlacement";
	explicit NativeCdcInitialPlacementWorkload(WorkloadContext const& wc)
	  : TestWorkload(wc), operationTimeout(getOption(options, "operationTimeout"_sr, 120.0)) {}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("RandomRangeLock"); }
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		return clientId == 0 ? timeoutError(run(cx), operationTimeout * 2) : Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& metrics) override {}
};

WorkloadFactory<NativeCdcInitialPlacementWorkload> NativeCdcInitialPlacementWorkloadFactory;
