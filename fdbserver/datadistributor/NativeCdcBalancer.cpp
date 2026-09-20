/*
 * NativeCdcBalancer.cpp
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

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <set>
#include <utility>
#include <vector>

#include "NativeCdcBalancer.h"
#include "fdbserver/core/NativeCdcMetadata.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/Knobs.h"
#include "flow/CodeProbe.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

namespace {

bool addNativeCdcLoad(int64_t* total, int64_t value) {
	if (value < 0 || value > std::numeric_limits<int64_t>::max() - *total) {
		return false;
	}
	*total += value;
	return true;
}

class NativeCdcLoadModel {
	struct Segment {
		KeyRange keys;
		std::set<Tag> tags;
		Optional<int64_t> load;
	};

	std::vector<Segment> segments;
	std::map<Tag, int64_t> tagLoads;
	bool complete = false;

	explicit NativeCdcLoadModel(std::vector<NativeCdcTagState> const& states) {
		KeyRangeMap<std::set<Tag>> coveringTags;
		for (const auto& state : states) {
			for (const auto& keys : state.ranges) {
				for (auto range : coveringTags.modify(keys)) {
					range->value().insert(state.assignment.tag);
				}
			}
		}
		for (auto range : coveringTags.ranges()) {
			if (!range.value().empty()) {
				segments.push_back(Segment{ KeyRange(range.range()), range.value(), {} });
			}
		}
	}

public:
	static Optional<NativeCdcLoadModel> create(std::vector<NativeCdcTagState> const& states, int64_t maxEntries) {
		if (maxEntries <= 0) {
			return {};
		}
		if (!states.empty()) {
			// M ranges have at most 2M-1 nonempty segments, each containing at most N tags.
			// Bound coverage memberships before constructing the segment sets.
			const uint64_t maxRanges = static_cast<uint64_t>(maxEntries) / states.size() / 2;
			uint64_t ranges = 0;
			for (const auto& state : states) {
				if (state.ranges.empty() || state.ranges.size() > maxRanges - ranges) {
					return {};
				}
				ranges += state.ranges.size();
			}
		}
		return NativeCdcLoadModel(states);
	}

	size_t segmentCount() const { return segments.size(); }
	KeyRange const& segmentKeys(size_t index) const { return segments[index].keys; }
	std::map<Tag, int64_t> const& loads() const {
		ASSERT(complete);
		return tagLoads;
	}

	bool setSample(size_t index, int64_t load) {
		ASSERT(!complete);
		if (load < 0) {
			return false;
		}
		segments[index].load = load;
		return true;
	}

	bool finishSamples() {
		tagLoads.clear();
		for (const auto& segment : segments) {
			if (!segment.load.present()) {
				return false;
			}
			for (Tag tag : segment.tags) {
				if (!addNativeCdcLoad(&tagLoads[tag], segment.load.get())) {
					return false;
				}
			}
		}
		complete = true;
		return true;
	}
};

Version nativeCdcDurationVersions(double seconds) {
	const long double versions = static_cast<long double>(seconds) * SERVER_KNOBS->VERSIONS_PER_SECOND;
	if (versions >= std::numeric_limits<Version>::max()) {
		return std::numeric_limits<Version>::max();
	}
	return static_cast<Version>(versions);
}

bool validNativeCdcBalancerKnobs() {
	return SERVER_KNOBS->NATIVE_CDC_TAG_MAX_STREAMS > 0 &&
	       SERVER_KNOBS->NATIVE_CDC_TAG_MAX_STREAMS < std::numeric_limits<int>::max() &&
	       SERVER_KNOBS->NATIVE_CDC_TAG_MODEL_MAX_ENTRIES > 0 && SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_CONCURRENCY > 0 &&
	       SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_SHARD_LIMIT > 1 &&
	       std::isfinite(SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_TIMEOUT) &&
	       SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_TIMEOUT > 0 &&
	       std::isfinite(SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_MAX_AGE) &&
	       SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_MAX_AGE > 0 && SERVER_KNOBS->VERSIONS_PER_SECOND > 0;
}

Future<Void> sampleNativeCdcRanges(Database cx, NativeCdcLoadModel* model, size_t* nextSegment) {
	while (*nextSegment < model->segmentCount()) {
		const size_t index = (*nextSegment)++;
		const StorageMetrics metrics =
		    co_await cx->getStorageMetrics(model->segmentKeys(index), SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_SHARD_LIMIT);
		if (!model->setSample(index, metrics.bytesWrittenPerKSecond)) {
			throw operation_failed();
		}
		co_await yield(TaskPriority::DataDistribution);
	}
}

Future<Void> sampleNativeCdcLoads(Database cx, NativeCdcLoadModel* model) {
	size_t nextSegment = 0;
	std::vector<Future<Void>> workers;
	const size_t workerCount =
	    std::min(model->segmentCount(), static_cast<size_t>(SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_CONCURRENCY));
	workers.reserve(workerCount);
	for (size_t i = 0; i < workerCount; ++i) {
		workers.push_back(sampleNativeCdcRanges(cx, model, &nextSegment));
	}
	co_await waitForAll(workers);
}

struct NativeCdcMetadataSnapshot {
	Value assignmentChange;
	Version version;
	std::vector<NativeCdcTagState> streams;
};

class NativeCdcBalancer {
	Database cx;
	MoveKeysLock lock;
	const DDEnabledState* ddEnabledState;

	bool samplingEnabled() const {
		return SERVER_KNOBS->NATIVE_CDC_TAG_BALANCING_ENABLED && cx->clientInfo->get().nativeCdcEnabled;
	}

	Future<Optional<NativeCdcMetadataSnapshot>> readSnapshot() {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				const Optional<Value> change = co_await tr.get(cdcProxyAssignmentChangeKey);
				const Value generation = change.present() ? change.get() : Value();
				Optional<std::vector<NativeCdcTagState>> states =
				    co_await readNativeCdcTagStates(&tr, SERVER_KNOBS->NATIVE_CDC_TAG_MAX_STREAMS);
				if (!states.present()) {
					TraceEvent("NativeCdcTagMetadataUnavailable", lock.myOwner)
					    .detail("StreamLimit", SERVER_KNOBS->NATIVE_CDC_TAG_MAX_STREAMS);
					co_return Optional<NativeCdcMetadataSnapshot>();
				}
				const Version version = co_await tr.getReadVersion();
				co_return Optional<NativeCdcMetadataSnapshot>(
				    NativeCdcMetadataSnapshot{ generation, version, std::move(states.get()) });
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<bool> currentGeneration(Transaction* tr, Value expected, Version sampledAt, Version validThrough) const {
		if (!samplingEnabled()) {
			co_return false;
		}
		const Optional<Value> generation = co_await tr->get(cdcProxyAssignmentChangeKey);
		if ((generation.present() ? generation.get() : Value()) != expected) {
			CODE_PROBE(true, "Native CDC DD discards samples after assignment changes");
			co_return false;
		}
		const Version version = co_await tr->getReadVersion();
		const bool fresh = version >= sampledAt && version <= validThrough;
		co_return fresh;
	}

	Future<bool> publishLoads(NativeCdcMetadataSnapshot const& snapshot,
	                          Version validThrough,
	                          NativeCdcLoadModel const& model) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				if (!(co_await currentGeneration(&tr, snapshot.assignmentChange, snapshot.version, validThrough))) {
					co_return false;
				}
				tr.clear(cdcTagLoadKeys);
				for (const auto& [tag, load] : model.loads()) {
					tr.set(cdcTagLoadKeyFor(tag),
					       cdcTagLoadValue(
					           CDCTagLoadSample{ snapshot.assignmentChange, snapshot.version, validThrough, load }));
				}
				co_await checkMoveKeysLock(&tr, lock, ddEnabledState);
				co_await tr.commit();
				CODE_PROBE(true, "Native CDC DD publishes producer tag throughput samples");
				TraceEvent("NativeCdcTagLoadSampled", lock.myOwner)
				    .detail("Streams", snapshot.streams.size())
				    .detail("Tags", model.loads().size())
				    .detail("Segments", model.segmentCount())
				    .detail("SampleVersion", snapshot.version)
				    .detail("ValidThrough", validThrough);
				co_return true;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> runPass() {
		if (!samplingEnabled()) {
			co_return;
		}
		if (!validNativeCdcBalancerKnobs()) {
			TraceEvent(SevWarn, "NativeCdcTagBalancerInvalidKnobs", lock.myOwner);
			co_return;
		}
		Optional<NativeCdcMetadataSnapshot> snapshot = co_await readSnapshot();
		if (!snapshot.present() || snapshot.get().streams.empty()) {
			co_return;
		}
		const int tagCount = cx->clientInfo->get().nativeCdcTagCount;
		if (tagCount <= 0 || tagCount > static_cast<int>(std::numeric_limits<uint16_t>::max()) + 1) {
			co_return;
		}
		const Version lifetime = nativeCdcDurationVersions(SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_MAX_AGE);
		const Version validThrough = snapshot.get().version > std::numeric_limits<Version>::max() - lifetime
		                                 ? std::numeric_limits<Version>::max()
		                                 : snapshot.get().version + lifetime;
		Optional<NativeCdcLoadModel> boundedModel =
		    NativeCdcLoadModel::create(snapshot.get().streams, SERVER_KNOBS->NATIVE_CDC_TAG_MODEL_MAX_ENTRIES);
		if (!boundedModel.present()) {
			CODE_PROBE(true, "Native CDC DD skips an overlap model exceeding its entry budget");
			TraceEvent("NativeCdcTagModelBudgetExceeded", lock.myOwner)
			    .detail("Streams", snapshot.get().streams.size())
			    .detail("MaxEntries", SERVER_KNOBS->NATIVE_CDC_TAG_MODEL_MAX_ENTRIES);
			co_return;
		}
		NativeCdcLoadModel& model = boundedModel.get();
		// One deadline bounds all segment requests. Partial/failed samples are never published as zero load.
		const Optional<Void> sampled =
		    co_await timeout(sampleNativeCdcLoads(cx, &model), SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_TIMEOUT);
		if (!sampled.present() || !model.finishSamples()) {
			CODE_PROBE(true, "Native CDC DD skips incomplete throughput samples");
			TraceEvent("NativeCdcTagSamplingIncomplete", lock.myOwner).detail("Segments", model.segmentCount());
			co_return;
		}
		co_await publishLoads(snapshot.get(), validThrough, model);
	}

public:
	NativeCdcBalancer(Database cx, MoveKeysLock lock, const DDEnabledState* ddEnabledState)
	  : cx(cx), lock(lock), ddEnabledState(ddEnabledState) {}

	Future<Void> run(Future<Void> initialized) {
		co_await initialized;
		while (true) {
			try {
				co_await runPass();
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled || e.code() == error_code_broken_promise ||
				    e.code() == error_code_movekeys_conflict) {
					throw;
				}
				TraceEvent(SevWarn, "NativeCdcTagBalancerError", lock.myOwner).error(e);
			}
			const double interval = SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_INTERVAL;
			co_await delay(std::isfinite(interval) && interval > 0 ? interval : 30.0, TaskPriority::DataDistribution);
		}
	}
};

NativeCdcTagState nativeCdcPolicyTestStream(CDCStreamId streamId, KeyRange keys, uint16_t tag) {
	return NativeCdcTagState{ streamId, { keys }, CDCTagHistoryEntry(streamId, 100, Tag(tagLocalityCDC, tag)) };
}

TEST_CASE("/NativeCDC/TagBalancing/IncompleteAndZeroSamples") {
	auto model = NativeCdcLoadModel::create({ nativeCdcPolicyTestStream(1, KeyRangeRef("a"_sr, "b"_sr), 0) },
	                                        SERVER_KNOBS->NATIVE_CDC_TAG_MODEL_MAX_ENTRIES)
	                 .get();
	ASSERT(!model.finishSamples());
	ASSERT(!model.setSample(0, -1));
	ASSERT(model.setSample(0, 0));
	ASSERT(model.finishSamples());
	ASSERT_EQ(model.loads().at(Tag(tagLocalityCDC, 0)), 0);
	int64_t total = std::numeric_limits<int64_t>::max() - 1;
	ASSERT(!addNativeCdcLoad(&total, 2));
	ASSERT(addNativeCdcLoad(&total, 1));
	return Void();
}

TEST_CASE("/NativeCDC/TagBalancing/OverlappingTagLoads") {
	auto model = NativeCdcLoadModel::create({ nativeCdcPolicyTestStream(1, KeyRangeRef("a"_sr, "d"_sr), 0),
	                                          nativeCdcPolicyTestStream(2, KeyRangeRef("c"_sr, "f"_sr), 0),
	                                          nativeCdcPolicyTestStream(3, KeyRangeRef("c"_sr, "d"_sr), 1) },
	                                        18)
	                 .get();
	ASSERT_EQ(model.segmentCount(), 3);
	ASSERT(model.setSample(0, 2000));
	ASSERT(model.setSample(1, 3000));
	ASSERT(model.setSample(2, 5000));
	ASSERT(model.finishSamples());
	ASSERT_EQ(model.loads().at(Tag(tagLocalityCDC, 0)), 10000);
	ASSERT_EQ(model.loads().at(Tag(tagLocalityCDC, 1)), 3000);
	return Void();
}

TEST_CASE("/NativeCDC/TagBalancing/ModelEntryBudget") {
	std::vector<NativeCdcTagState> streams{ nativeCdcPolicyTestStream(1, KeyRangeRef("a"_sr, "f"_sr), 0),
		                                    nativeCdcPolicyTestStream(2, KeyRangeRef("b"_sr, "e"_sr), 0),
		                                    nativeCdcPolicyTestStream(3, KeyRangeRef("c"_sr, "d"_sr), 1) };
	// The same stream count needs a larger entry budget once streams cover disjoint range unions.
	ASSERT(NativeCdcLoadModel::create(streams, 18).present());
	for (auto& stream : streams) {
		stream.ranges.emplace_back(KeyRangeRef("x"_sr, "y"_sr));
	}
	ASSERT(!NativeCdcLoadModel::create(streams, 18).present());
	ASSERT(!NativeCdcLoadModel::create(streams, 35).present());
	auto model = NativeCdcLoadModel::create(streams, 36);
	ASSERT(model.present());
	ASSERT_EQ(model.get().segmentCount(), 6);
	for (size_t i = 0; i < model.get().segmentCount(); ++i) {
		ASSERT(model.get().setSample(i, 1000));
	}
	ASSERT(model.get().finishSamples());
	ASSERT_EQ(model.get().loads().at(Tag(tagLocalityCDC, 0)), 6000);
	ASSERT_EQ(model.get().loads().at(Tag(tagLocalityCDC, 1)), 2000);
	ASSERT(!NativeCdcLoadModel::create(streams, 0).present());
	ASSERT(!NativeCdcLoadModel::create(streams, -1).present());
	ASSERT(NativeCdcLoadModel::create(streams, std::numeric_limits<int64_t>::max()).present());
	return Void();
}

} // namespace

Future<Void> nativeCdcBalancer(Database cx,
                               MoveKeysLock lock,
                               const DDEnabledState* ddEnabledState,
                               Future<Void> initialized) {
	NativeCdcBalancer balancer(cx, lock, ddEnabledState);
	co_await balancer.run(initialized);
}

void forceLinkNativeCdcBalancerTests() {}
