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
#include "NativeCdcInternal.h"
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

struct NativeCdcRetagDecision {
	size_t streamIndex;
	Tag destination;
	int64_t sourceBefore;
	int64_t destinationBefore;
	int64_t sourceAfter;
	int64_t destinationAfter;
	int64_t improvement;
};

class NativeCdcLoadModel {
	struct Segment {
		KeyRange keys;
		std::set<size_t> streams;
		std::map<Tag, int> tagCounts;
		Optional<int64_t> load;
	};

	std::vector<NativeCdcTagState> streams;
	std::vector<Segment> segments;
	std::map<Tag, std::set<UID>> tagOwners;
	std::map<Tag, int64_t> tagLoads;
	std::map<Tag, std::vector<int64_t>> tagPrefixes;
	std::vector<int64_t> rangePrefix;
	std::vector<int64_t> removableLoads;
	std::vector<std::pair<size_t, size_t>> streamSegments;
	bool complete = false;

public:
	explicit NativeCdcLoadModel(std::vector<NativeCdcTagState> states) : streams(std::move(states)) {
		KeyRangeMap<std::set<size_t>> coveringStreams;
		for (size_t i = 0; i < streams.size(); ++i) {
			for (auto range : coveringStreams.modify(streams[i].keys)) {
				range->value().insert(i);
			}
			tagOwners[streams[i].assignment.tag].insert(streams[i].proxyId);
		}
		streamSegments.resize(streams.size(), { std::numeric_limits<size_t>::max(), 0 });
		for (auto range : coveringStreams.ranges()) {
			if (range.value().empty()) {
				continue;
			}
			Segment segment{ KeyRange(range.range()), range.value(), {}, {} };
			for (size_t streamIndex : segment.streams) {
				++segment.tagCounts[streams[streamIndex].assignment.tag];
				auto& bounds = streamSegments[streamIndex];
				bounds.first = std::min(bounds.first, segments.size());
				bounds.second = segments.size() + 1;
			}
			segments.push_back(std::move(segment));
		}
	}

	size_t segmentCount() const { return segments.size(); }
	KeyRange const& segmentKeys(size_t index) const { return segments[index].keys; }
	NativeCdcTagState const& stream(size_t index) const { return streams[index]; }
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
		rangePrefix.assign(1, 0);
		removableLoads.assign(streams.size(), 0);
		tagPrefixes.clear();
		tagLoads.clear();
		for (const auto& [tag, owners] : tagOwners) {
			tagPrefixes[tag].push_back(0);
		}
		for (const auto& segment : segments) {
			if (!segment.load.present()) {
				return false;
			}
			int64_t rangeLoad = rangePrefix.back();
			if (!addNativeCdcLoad(&rangeLoad, segment.load.get())) {
				return false;
			}
			rangePrefix.push_back(rangeLoad);
			for (auto& [tag, prefix] : tagPrefixes) {
				int64_t tagLoad = prefix.back();
				if (segment.tagCounts.contains(tag) && !addNativeCdcLoad(&tagLoad, segment.load.get())) {
					return false;
				}
				prefix.push_back(tagLoad);
			}
			for (const size_t streamIndex : segment.streams) {
				const Tag tag = streams[streamIndex].assignment.tag;
				if (segment.tagCounts.at(tag) == 1 &&
				    !addNativeCdcLoad(&removableLoads[streamIndex], segment.load.get())) {
					return false;
				}
			}
		}
		for (const auto& [tag, prefix] : tagPrefixes) {
			tagLoads[tag] = prefix.back();
		}
		complete = true;
		return true;
	}

	Optional<NativeCdcRetagDecision> chooseMove(Version version,
	                                            int tagCount,
	                                            Version cooldown,
	                                            double minRelativeImprovement,
	                                            int64_t minBytesPerSecondImprovement) const {
		ASSERT(complete);
		std::vector<Tag> destinations;
		for (const auto& [tag, owners] : tagOwners) {
			if (tag.id < tagCount) {
				destinations.push_back(tag);
			}
		}
		// Every unused tag has the same predicted cost; only its deterministic tie-break matters.
		for (int tagId = 0; tagId < tagCount; ++tagId) {
			const Tag tag(tagLocalityCDC, static_cast<uint16_t>(tagId));
			if (!tagOwners.contains(tag)) {
				destinations.push_back(tag);
				break;
			}
		}

		Optional<NativeCdcRetagDecision> best;
		for (size_t i = 0; i < streams.size(); ++i) {
			const auto& state = streams[i];
			if (state.pending || version < state.assignment.version || version - state.assignment.version < cooldown) {
				continue;
			}
			const auto [first, end] = streamSegments[i];
			const int64_t streamLoad = rangePrefix[end] - rangePrefix[first];
			const int64_t sourceBefore = tagLoads.at(state.assignment.tag);
			const int64_t sourceAfter = sourceBefore - removableLoads[i];
			for (const Tag destination : destinations) {
				if (destination == state.assignment.tag) {
					continue;
				}
				const auto owners = tagOwners.find(destination);
				if (owners != tagOwners.end() &&
				    (owners->second.size() != 1 || !owners->second.contains(state.proxyId))) {
					continue;
				}
				const auto prefix = tagPrefixes.find(destination);
				const int64_t overlap = prefix == tagPrefixes.end() ? 0 : prefix->second[end] - prefix->second[first];
				const int64_t destinationBefore = prefix == tagPrefixes.end() ? 0 : prefix->second.back();
				int64_t destinationAfter = destinationBefore;
				if (!addNativeCdcLoad(&destinationAfter, streamLoad - overlap)) {
					continue;
				}
				const int64_t before = std::max(sourceBefore, destinationBefore);
				const int64_t after = std::max(sourceAfter, destinationAfter);
				if (after >= before) {
					continue;
				}
				const int64_t improvement = before - after;
				if (static_cast<long double>(improvement) < static_cast<long double>(before) * minRelativeImprovement ||
				    static_cast<long double>(improvement) <
				        static_cast<long double>(minBytesPerSecondImprovement) * 1000) {
					continue;
				}
				if (!best.present() || improvement > best.get().improvement ||
				    (improvement == best.get().improvement &&
				     std::make_pair(state.streamId, destination.id) <
				         std::make_pair(streams[best.get().streamIndex].streamId, best.get().destination.id))) {
					best = NativeCdcRetagDecision{ i,           destination,      sourceBefore, destinationBefore,
						                           sourceAfter, destinationAfter, improvement };
				}
			}
		}
		return best;
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
	       SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_CONCURRENCY > 0 && SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_SHARD_LIMIT > 1 &&
	       std::isfinite(SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_TIMEOUT) &&
	       SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_TIMEOUT > 0 &&
	       std::isfinite(SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_MAX_AGE) &&
	       SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_MAX_AGE > 0 &&
	       std::isfinite(SERVER_KNOBS->NATIVE_CDC_TAG_MOVE_COOLDOWN) &&
	       SERVER_KNOBS->NATIVE_CDC_TAG_MOVE_COOLDOWN >= 0 &&
	       std::isfinite(SERVER_KNOBS->NATIVE_CDC_TAG_MIN_RELATIVE_IMPROVEMENT) &&
	       SERVER_KNOBS->NATIVE_CDC_TAG_MIN_RELATIVE_IMPROVEMENT >= 0 &&
	       SERVER_KNOBS->NATIVE_CDC_TAG_MIN_RELATIVE_IMPROVEMENT <= 1 &&
	       SERVER_KNOBS->NATIVE_CDC_TAG_MIN_BYTES_PER_SECOND_IMPROVEMENT >= 0 && SERVER_KNOBS->VERSIONS_PER_SECOND > 0;
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

class NativeCdcCleanupProgress {
	Optional<Value> assignmentChange;
	Key next = cdcStreamKeys.begin;
	bool cycleComplete = false;
	bool sawPending = false;
	bool rescan = false;

	bool sameGeneration(ValueRef generation) const {
		return assignmentChange.present() && assignmentChange.get() == generation;
	}

public:
	bool needsScan(ValueRef generation) const {
		return !sameGeneration(generation) || !cycleComplete || sawPending || rescan;
	}

	Key begin() const { return cycleComplete ? Key(cdcStreamKeys.begin) : next; }

	void scanned(Value generation, Key nextBegin, bool lastPage, bool pagePending) {
		const bool continuing = assignmentChange.present() && !cycleComplete;
		sawPending = (continuing && sawPending) || pagePending;
		rescan = continuing && (rescan || !sameGeneration(generation));
		assignmentChange = std::move(generation);
		next = std::move(nextBegin);
		cycleComplete = lastPage;
	}

	// Churn requires another traversal, not a restart that can starve later pages.
	void changed() { rescan = true; }
};

class NativeCdcBalancer {
	Database cx;
	MoveKeysLock lock;
	const DDEnabledState* ddEnabledState;
	NativeCdcCleanupProgress cleanupProgress;

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

	Future<bool> finishPendingPage() {
		constexpr int cleanupPageSize = 100;
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				const Optional<Value> change = co_await tr.get(cdcProxyAssignmentChangeKey);
				const Value generation = change.present() ? change.get() : Value();
				if (!cleanupProgress.needsScan(generation)) {
					co_return false;
				}
				const Key begin = cleanupProgress.begin();
				const RangeResult page = co_await tr.getRange(KeyRangeRef(begin, cdcStreamKeys.end), cleanupPageSize);
				std::vector<Future<Optional<NativeCdcTagState>>> reads;
				for (const auto& row : page) {
					reads.push_back(readNativeCdcTagState(&tr, decodeCDCStreamKey(row.key)));
				}
				const std::vector<Optional<NativeCdcTagState>> states = co_await getAll(reads);
				int finished = 0;
				bool pagePending = false;
				for (const auto& state : states) {
					if (!state.present()) {
						// Incomplete ownership/metadata is not evidence that all transitions have drained.
						pagePending = true;
						continue;
					}
					pagePending = pagePending || state.get().pending;
					if (state.get().pending && (co_await finishNativeCdcRetag(&tr, state.get()))) {
						++finished;
					}
				}
				if (finished == 0) {
					cleanupProgress.scanned(generation,
					                        page.more ? keyAfter(page.back().key) : Key(cdcStreamKeys.end),
					                        !page.more,
					                        pagePending);
					co_return false;
				}
				co_await checkMoveKeysLock(&tr, lock, ddEnabledState);
				co_await tr.commit();
				cleanupProgress.scanned(generation,
				                        page.more ? keyAfter(page.back().key) : Key(cdcStreamKeys.end),
				                        !page.more,
				                        pagePending);
				cleanupProgress.changed();
				CODE_PROBE(true, "Native CDC DD finishes acknowledged tag transitions");
				TraceEvent("NativeCdcTagTransitionsFinished", lock.myOwner).detail("Streams", finished);
				co_return true;
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

	Future<Void> applyMove(NativeCdcMetadataSnapshot const& snapshot,
	                       Version validThrough,
	                       NativeCdcTagState const& state,
	                       NativeCdcRetagDecision decision) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				if (!(co_await currentGeneration(&tr, snapshot.assignmentChange, snapshot.version, validThrough)) ||
				    !(co_await retagNativeCdcStream(&tr, state, decision.destination))) {
					co_return;
				}
				co_await checkMoveKeysLock(&tr, lock, ddEnabledState);
				co_await tr.commit();
				CODE_PROBE(true, "Native CDC DD retags a stream using producer throughput");
				TraceEvent("NativeCdcTagRebalanced", lock.myOwner)
				    .detail("StreamId", state.streamId)
				    .detail("SourceTag", state.assignment.tag)
				    .detail("DestinationTag", decision.destination)
				    .detail("SourceBytesPerKSecond", decision.sourceBefore)
				    .detail("DestinationBytesPerKSecond", decision.destinationBefore)
				    .detail("PredictedSourceBytesPerKSecond", decision.sourceAfter)
				    .detail("PredictedDestinationBytesPerKSecond", decision.destinationAfter);
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> runPass() {
		// Cleanup has its own bounded cursor: the admission/sampling envelope must not strand existing history.
		if ((co_await finishPendingPage()) || !samplingEnabled()) {
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
		NativeCdcLoadModel model(snapshot.get().streams);
		// One deadline bounds all segment requests. Partial/failed samples are never published as zero load.
		const Optional<Void> sampled =
		    co_await timeout(sampleNativeCdcLoads(cx, &model), SERVER_KNOBS->NATIVE_CDC_TAG_SAMPLE_TIMEOUT);
		if (!sampled.present() || !model.finishSamples()) {
			CODE_PROBE(true, "Native CDC DD skips incomplete throughput samples");
			TraceEvent("NativeCdcTagSamplingIncomplete", lock.myOwner).detail("Segments", model.segmentCount());
			co_return;
		}
		if (!(co_await publishLoads(snapshot.get(), validThrough, model))) {
			co_return;
		}
		const Optional<NativeCdcRetagDecision> decision =
		    model.chooseMove(snapshot.get().version,
		                     tagCount,
		                     nativeCdcDurationVersions(SERVER_KNOBS->NATIVE_CDC_TAG_MOVE_COOLDOWN),
		                     SERVER_KNOBS->NATIVE_CDC_TAG_MIN_RELATIVE_IMPROVEMENT,
		                     SERVER_KNOBS->NATIVE_CDC_TAG_MIN_BYTES_PER_SECOND_IMPROVEMENT);
		if (decision.present()) {
			co_await applyMove(snapshot.get(), validThrough, model.stream(decision.get().streamIndex), decision.get());
		}
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

NativeCdcTagState nativeCdcPolicyTestStream(CDCStreamId streamId,
                                            KeyRange keys,
                                            uint16_t tag,
                                            UID owner = UID(1, 1),
                                            bool pending = false,
                                            Version assignedAt = 100) {
	return NativeCdcTagState{ streamId,
		                      keys,
		                      cdcTagHistoryKeyFor(streamId, assignedAt, Tag(tagLocalityCDC, tag)),
		                      CDCTagHistoryEntry(streamId, assignedAt, Tag(tagLocalityCDC, tag)),
		                      owner,
		                      assignedAt,
		                      pending };
}

TEST_CASE("/NativeCDC/TagBalancing/DisjointThroughput") {
	NativeCdcLoadModel model({ nativeCdcPolicyTestStream(1, KeyRangeRef("a"_sr, "b"_sr), 0),
	                           nativeCdcPolicyTestStream(2, KeyRangeRef("b"_sr, "c"_sr), 0),
	                           nativeCdcPolicyTestStream(3, KeyRangeRef("x"_sr, "y"_sr), 1) });
	ASSERT_EQ(model.segmentCount(), 3);
	ASSERT(model.setSample(0, 40000000));
	ASSERT(model.setSample(1, 40000000));
	ASSERT(model.setSample(2, 1000000));
	ASSERT(model.finishSamples());
	const auto decision = model.chooseMove(1000, 2, 100, 0.2, 10000);
	ASSERT(decision.present());
	ASSERT_EQ(model.stream(decision.get().streamIndex).streamId, 1);
	ASSERT_EQ(decision.get().destination, Tag(tagLocalityCDC, 1));
	ASSERT_EQ(decision.get().sourceAfter, 40000000);
	ASSERT_EQ(decision.get().destinationAfter, 41000000);
	ASSERT(!model.chooseMove(1000, 2, 100, 0.5, 10000).present());
	ASSERT(!model.chooseMove(1000, 2, 100, 0.2, 40000).present());
	ASSERT(!model.chooseMove(1000, 2, 1000, 0.2, 10000).present());
	return Void();
}

TEST_CASE("/NativeCDC/TagBalancing/Overlap") {
	NativeCdcLoadModel identical({ nativeCdcPolicyTestStream(1, KeyRangeRef("a"_sr, "b"_sr), 0),
	                               nativeCdcPolicyTestStream(2, KeyRangeRef("a"_sr, "b"_sr), 0) });
	ASSERT_EQ(identical.segmentCount(), 1);
	ASSERT(identical.setSample(0, 40000000));
	ASSERT(identical.finishSamples());
	ASSERT_EQ(identical.loads().at(Tag(tagLocalityCDC, 0)), 40000000);
	ASSERT(!identical.chooseMove(1000, 2, 0, 0, 0).present());

	NativeCdcLoadModel partial({ nativeCdcPolicyTestStream(1, KeyRangeRef("a"_sr, "d"_sr), 0),
	                             nativeCdcPolicyTestStream(2, KeyRangeRef("c"_sr, "f"_sr), 0),
	                             nativeCdcPolicyTestStream(3, KeyRangeRef("c"_sr, "d"_sr), 1) });
	ASSERT_EQ(partial.segmentCount(), 3);
	ASSERT(partial.setSample(0, 2000000));
	ASSERT(partial.setSample(1, 3000000));
	ASSERT(partial.setSample(2, 5000000));
	ASSERT(partial.finishSamples());
	const auto decision = partial.chooseMove(1000, 2, 0, 0.1, 1000);
	ASSERT(decision.present());
	ASSERT_EQ(partial.stream(decision.get().streamIndex).streamId, 1);
	ASSERT_EQ(decision.get().sourceAfter, 8000000);
	ASSERT_EQ(decision.get().destinationAfter, 5000000);
	return Void();
}

TEST_CASE("/NativeCDC/TagBalancing/OwnerAndPending") {
	NativeCdcLoadModel model({ nativeCdcPolicyTestStream(1, KeyRangeRef("a"_sr, "b"_sr), 0, UID(1, 1), true),
	                           nativeCdcPolicyTestStream(2, KeyRangeRef("b"_sr, "c"_sr), 0),
	                           nativeCdcPolicyTestStream(3, KeyRangeRef("x"_sr, "y"_sr), 1, UID(2, 2)) });
	ASSERT(model.setSample(0, 40000000));
	ASSERT(model.setSample(1, 40000000));
	ASSERT(model.setSample(2, 1000000));
	ASSERT(model.finishSamples());
	ASSERT(!model.chooseMove(1000, 2, 0, 0.2, 10000).present());
	const auto decision = model.chooseMove(1000, 4, 0, 0.2, 10000);
	ASSERT(decision.present());
	ASSERT_EQ(model.stream(decision.get().streamIndex).streamId, 2);
	ASSERT_EQ(decision.get().destination, Tag(tagLocalityCDC, 2));
	return Void();
}

TEST_CASE("/NativeCDC/TagBalancing/IncompleteAndZeroSamples") {
	NativeCdcLoadModel model({ nativeCdcPolicyTestStream(1, KeyRangeRef("a"_sr, "b"_sr), 0) });
	ASSERT(!model.finishSamples());
	ASSERT(!model.setSample(0, -1));
	ASSERT(model.setSample(0, 0));
	ASSERT(model.finishSamples());
	ASSERT_EQ(model.loads().at(Tag(tagLocalityCDC, 0)), 0);
	ASSERT(!model.chooseMove(1000, 2, 0, 0, 0).present());
	int64_t total = std::numeric_limits<int64_t>::max() - 1;
	ASSERT(!addNativeCdcLoad(&total, 2));
	ASSERT(addNativeCdcLoad(&total, 1));
	return Void();
}

TEST_CASE("/NativeCDC/TagBalancing/CleanupPaging") {
	NativeCdcCleanupProgress progress;
	const Value firstGeneration = "first"_sr;
	const Value secondGeneration = "second"_sr;
	const Key nextPage = keyAfter(cdcStreamKeyFor(100));
	ASSERT(progress.needsScan(firstGeneration));
	ASSERT_EQ(progress.begin(), cdcStreamKeys.begin);
	progress.scanned(firstGeneration, nextPage, false, false);
	ASSERT(progress.needsScan(firstGeneration));
	ASSERT_EQ(progress.begin(), nextPage);
	progress.scanned(firstGeneration, cdcStreamKeys.end, true, true);
	// Acknowledgements do not change the assignment generation, so pending cycles must repeat.
	ASSERT(progress.needsScan(firstGeneration));
	ASSERT_EQ(progress.begin(), cdcStreamKeys.begin);
	progress.scanned(firstGeneration, cdcStreamKeys.end, true, false);
	ASSERT(!progress.needsScan(firstGeneration));
	ASSERT(progress.needsScan(secondGeneration));
	ASSERT_EQ(progress.begin(), cdcStreamKeys.begin);
	progress.changed();
	ASSERT(progress.needsScan(firstGeneration));
	return Void();
}

TEST_CASE("/NativeCDC/TagBalancing/CleanupProgressAcrossChurn") {
	NativeCdcCleanupProgress progress;
	const Value firstGeneration = "first"_sr;
	const Value secondGeneration = "second"_sr;
	const Value thirdGeneration = "third"_sr;
	const Key secondPage = keyAfter(cdcStreamKeyFor(100));
	const Key thirdPage = keyAfter(cdcStreamKeyFor(200));
	progress.scanned(firstGeneration, secondPage, false, true);
	progress.changed(); // A successful cleanup on the first page changes the generation.
	ASSERT(progress.needsScan(secondGeneration));
	ASSERT_EQ(progress.begin(), secondPage);
	progress.scanned(secondGeneration, thirdPage, false, true);
	progress.changed();
	ASSERT(progress.needsScan(thirdGeneration));
	ASSERT_EQ(progress.begin(), thirdPage);
	progress.scanned(thirdGeneration, cdcStreamKeys.end, true, false);
	ASSERT(progress.needsScan(thirdGeneration));
	ASSERT_EQ(progress.begin(), cdcStreamKeys.begin);
	progress.scanned(thirdGeneration, cdcStreamKeys.end, true, false);
	ASSERT(!progress.needsScan(thirdGeneration));
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
