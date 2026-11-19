/*
 * ChangeFeeds.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/serialize.h"
#include <cstring>
#include <limits>

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<std::pair<Standalone<VectorRef<KeyValueRef>>, Version>> readDatabase(Database cx) {
	state Transaction tr(cx);
	loop {
		state Standalone<VectorRef<KeyValueRef>> output;
		state Version readVersion;
		try {
			Version ver = wait(tr.getReadVersion());
			readVersion = ver;

			state PromiseStream<Standalone<RangeResultRef>> results;
			state Future<Void> stream = tr.getRangeStream(results, normalKeys, GetRangeLimits());

			loop {
				Standalone<RangeResultRef> res = waitNext(results.getFuture());
				output.arena().dependsOn(res.arena());
				output.append(output.arena(), res.begin(), res.size());
			}
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				return std::make_pair(output, readVersion);
			}
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Standalone<VectorRef<MutationsAndVersionRef>>> readMutations(Database cx,
                                                                          Key rangeID,
                                                                          Promise<Version> end,
                                                                          Version begin = 0,
                                                                          KeyRange range = normalKeys) {
	state Standalone<VectorRef<MutationsAndVersionRef>> output;
	loop {
		try {
			state Reference<ChangeFeedData> results = makeReference<ChangeFeedData>();
			state Future<Void> stream =
			    cx->getChangeFeedStream(results, rangeID, begin, std::numeric_limits<Version>::max(), range);
			loop {
				choose {
					when(Standalone<VectorRef<MutationsAndVersionRef>> res = waitNext(results->mutations.getFuture())) {
						output.arena().dependsOn(res.arena());
						for (auto& it : res) {
							if (it.mutations.size() == 1 && it.mutations.back().param1 == lastEpochEndPrivateKey) {
								Version rollbackVersion;
								BinaryReader br(it.mutations.back().param2, Unversioned());
								br >> rollbackVersion;
								TraceEvent("ChangeFeedRollback")
								    .detail("Ver", it.version)
								    .detail("RollbackVer", rollbackVersion);
								while (output.size() && output.back().version > rollbackVersion) {
									TraceEvent("ChangeFeedRollbackVer").detail("Ver", output.back().version);
									output.pop_back();
								}
							} else {
								output.push_back(output.arena(), it);
							}
						}
						begin = res.back().version + 1;
					}
					when(wait(end.isSet() ? Future<Void>(Never()) : success(end.getFuture()))) {}
					when(wait(!end.isSet() ? Future<Void>(Never()) : results->whenAtLeast(end.getFuture().get()))) {
						return output;
					}
				}
			}
		} catch (Error& e) {
			throw;
		}
	}
}

Standalone<VectorRef<KeyValueRef>> advanceData(Standalone<VectorRef<KeyValueRef>> source,
                                               Standalone<VectorRef<MutationsAndVersionRef>> mutations,
                                               Version begin,
                                               Version end) {
	StringRef dbgKey = ""_sr;
	std::map<KeyRef, ValueRef> data;
	for (auto& kv : source) {
		if (kv.key == dbgKey)
			TraceEvent("ChangeFeedDbgStart").detail("K", kv.key).detail("V", kv.value);
		data[kv.key] = kv.value;
	}
	for (auto& it : mutations) {
		if (it.version > begin && it.version <= end) {
			for (auto& m : it.mutations) {
				if (m.type == MutationRef::SetValue) {
					if (m.param1 == dbgKey)
						TraceEvent("ChangeFeedDbgSet")
						    .detail("Ver", it.version)
						    .detail("K", m.param1)
						    .detail("V", m.param2);
					data[m.param1] = m.param2;
				} else {
					ASSERT(m.type == MutationRef::ClearRange);
					if (KeyRangeRef(m.param1, m.param2).contains(dbgKey))
						TraceEvent("ChangeFeedDbgClear")
						    .detail("Ver", it.version)
						    .detail("Begin", m.param1)
						    .detail("End", m.param2);
					data.erase(data.lower_bound(m.param1), data.lower_bound(m.param2));
				}
			}
		}
	}
	Standalone<VectorRef<KeyValueRef>> output;
	output.arena().dependsOn(source.arena());
	output.arena().dependsOn(mutations.arena());
	for (auto& kv : data) {
		output.push_back(output.arena(), KeyValueRef(kv.first, kv.second));
	}
	return output;
}

bool compareData(Standalone<VectorRef<KeyValueRef>> source, Standalone<VectorRef<KeyValueRef>> dest) {
	if (source.size() != dest.size()) {
		TraceEvent(SevError, "ChangeFeedSizeMismatch").detail("SrcSize", source.size()).detail("DestSize", dest.size());
	}
	for (int i = 0; i < std::min(source.size(), dest.size()); i++) {
		if (source[i] != dest[i]) {
			TraceEvent("ChangeFeedMutationMismatch")
			    .detail("Index", i)
			    .detail("SrcKey", source[i].key)
			    .detail("DestKey", dest[i].key)
			    .detail("SrcValue", source[i].value)
			    .detail("DestValue", dest[i].value);
			return false;
		}
	}
	return source.size() == dest.size();
}

struct ChangeFeedsWorkload : TestWorkload {
	static constexpr auto NAME = "ChangeFeeds";
	double testDuration;
	bool testStreamReadyTimeout;
	Future<Void> client;

	ChangeFeedsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		testStreamReadyTimeout = getOption(options, "testStreamReadyTimeout"_sr, false);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			if (testStreamReadyTimeout) {
				return timeoutError(checkStreamReadyTimeout(cx->clone()), testDuration);
			}
			client = changeFeedClient(cx->clone(), this);
			return delay(testDuration);
		}
		return Void();
	}
	Future<bool> check(Database const& cx) override {
		client = Future<Void>();
		return true;
	}
	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Version> writeMarker(Database cx, Key key, Value value) {
		state Transaction tr(cx);
		loop {
			try {
				tr.set(key, value);
				wait(tr.commit());
				return tr.getCommittedVersion();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> checkStreamReadyTimeout(Database cx) {
		state Key key = "changeFeedMarker"_sr;
		state KeyRange range = singleKeyRange(key);
		state Key feedID = "stalledReader"_sr;
		wait(updateChangeFeed(cx, feedID, ChangeFeedStatus::CHANGE_FEED_CREATE, range));
		state Transaction tr(cx);
		state Version begin = wait(tr.getReadVersion());
		KeyRangeLocationInfo loc = wait(getKeyLocation_internal(
		    cx, TenantInfo(), key, SpanContext(), {}, UseProvisionalProxies::False, Reverse::False, begin));
		// Local streams bypass the transport byte limit.
		int i = 0;
		while (i < loc.locations->size() && loc.locations->getInterface(i).changeFeedStream.getEndpoint().isLocal()) {
			++i;
		}
		ASSERT(i < loc.locations->size());
		state StorageServerInterface ss = loc.locations->getInterface(i);
		wait(success(ss.changeFeedVersionUpdate.getReply(ChangeFeedVersionUpdateRequest(begin))));
		ChangeFeedStreamRequest req;
		req.id = deterministicRandom()->randomUniqueID();
		req.rangeID = feedID;
		req.range = range;
		req.begin = begin;
		req.end = MAX_VERSION;
		req.canReadPopped = false;
		req.replyBufferSize = 1;
		state ReplyPromiseStream<ChangeFeedStreamReply> stalled = ss.changeFeedStream.getReplyStream(req);
		state Future<ErrorOr<Void>> streamError = errorOr(stalled.onError());
		state Version resume;
		loop {
			ChangeFeedStreamReply reply = waitNext(stalled.getFuture());
			if (reply.atLatestVersion) {
				resume = reply.mutations.back().version + 1;
				break;
			}
		}

		state Version first = wait(writeMarker(cx, key, "first"_sr));
		// Keep the first reply queued and unacknowledged. The second write must then block the sender.
		while (stalled.size() == 0) {
			wait(delay(0.01));
		}
		state Version second = wait(writeMarker(cx, key, "second"_sr));
		ChangeFeedVersionUpdateReply pinned =
		    wait(ss.changeFeedVersionUpdate.getReply(ChangeFeedVersionUpdateRequest(second)));
		ASSERT(pinned.version < second);
		ErrorOr<Void> stopped = wait(streamError);
		ASSERT(stopped.isError() && stopped.getError().code() == error_code_connection_failed);
		ChangeFeedVersionUpdateReply released =
		    wait(ss.changeFeedVersionUpdate.getReply(ChangeFeedVersionUpdateRequest(second)));
		ASSERT(released.version >= second);

		Promise<Version> end;
		end.send(second);
		Standalone<VectorRef<MutationsAndVersionRef>> replay = wait(readMutations(cx, feedID, end, resume, range));
		int count = 0;
		for (const auto& row : replay) {
			for (const auto& m : row.mutations) {
				ASSERT(m.type == MutationRef::SetValue && m.param1 == key);
				ASSERT(row.version == (count == 0 ? first : second));
				ASSERT(m.param2 == (count == 0 ? "first"_sr : "second"_sr));
				++count;
			}
		}
		ASSERT(count == 2);
		return Void();
	}

	ACTOR Future<Void> changeFeedClient(Database cx, ChangeFeedsWorkload* self) {
		// Enable change feeds for a key range
		state Key rangeID = StringRef(deterministicRandom()->randomUniqueID().toString());
		wait(updateChangeFeed(cx, rangeID, ChangeFeedStatus::CHANGE_FEED_CREATE, normalKeys));

		loop {
			state Promise<Version> endVersion;
			state Future<Standalone<VectorRef<MutationsAndVersionRef>>> fMutations =
			    readMutations(cx, rangeID, endVersion);

			wait(delay(deterministicRandom()->random01()));

			state std::pair<Standalone<VectorRef<KeyValueRef>>, Version> firstResults = wait(readDatabase(cx));
			TraceEvent("ChangeFeedReadDB").detail("Ver1", firstResults.second);

			wait(delay(10 * deterministicRandom()->random01()));

			state std::pair<Standalone<VectorRef<KeyValueRef>>, Version> secondResults = wait(readDatabase(cx));
			TraceEvent("ChangeFeedReadDB").detail("Ver2", secondResults.second);
			endVersion.send(secondResults.second + 1);
			Standalone<VectorRef<MutationsAndVersionRef>> mutations = wait(fMutations);
			Standalone<VectorRef<KeyValueRef>> advancedResults =
			    advanceData(firstResults.first, mutations, firstResults.second, secondResults.second);

			if (!compareData(secondResults.first, advancedResults)) {
				TraceEvent(SevError, "ChangeFeedMismatch")
				    .detail("FirstVersion", firstResults.second)
				    .detail("SecondVersion", secondResults.second);
				for (int i = 0; i < secondResults.first.size(); i++) {
					TraceEvent("ChangeFeedBase")
					    .detail("Index", i)
					    .detail("K", secondResults.first[i].key)
					    .detail("V", secondResults.first[i].value);
				}
				for (int i = 0; i < advancedResults.size(); i++) {
					TraceEvent("ChangeFeedAdvanced")
					    .detail("Index", i)
					    .detail("K", advancedResults[i].key)
					    .detail("V", advancedResults[i].value);
				}
				ASSERT(false);
			}

			wait(cx->popChangeFeedMutations(rangeID, secondResults.second));
		}
	}
};

WorkloadFactory<ChangeFeedsWorkload> ChangeFeedsWorkloadFactory;
