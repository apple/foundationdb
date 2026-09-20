/*
 * NativeCdcOrdered.cpp
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
#include <map>
#include <set>
#include <string>
#include <vector>

#include "NativeCdcInternal.h"
#include "NativeCdcOrderedMetadata.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeCdc.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/tester/workloads.h"

class NativeCdcOrderedWorkload : public TestWorkload {
	using Mutations = Standalone<VectorRef<MutationRef>>;
	using Keyspace = std::map<Key, Value>;

	double operationTimeout;
	bool testOwnerReplacement;
	bool completed = false;
	std::map<Version, Mutations> expectedVersions;
	const Key name = "native-cdc-ordered"_sr;

	Key key(StringRef suffix) const { return Key("native-cdc-ordered/data/"_sr.withSuffix(suffix)); }
	KeyRange selectedRange() const { return KeyRange(KeyRangeRef(key("a"_sr), key("z"_sr))); }

	static Value integerValue(uint64_t value) {
		std::string bytes(8, '\0');
		for (int i = 0; i < 8; ++i) {
			bytes[i] = static_cast<char>((value >> (8 * i)) & 0xff);
		}
		return Value(StringRef(bytes));
	}

	static uint64_t integerValue(ValueRef value) {
		ASSERT_EQ(value.size(), 8);
		uint64_t result = 0;
		for (int i = 0; i < 8; ++i) {
			result |= static_cast<uint64_t>(value[i]) << (8 * i);
		}
		return result;
	}

	static void append(Mutations& mutations, MutationRef const& mutation) {
		mutations.push_back_deep(mutations.arena(), mutation);
	}

	Mutations writeMutations(int step) const {
		Mutations mutations;
		if (step == 0) {
			append(mutations, MutationRef(MutationRef::SetValue, key("b"_sr), integerValue(0)));
			append(mutations, MutationRef(MutationRef::SetValue, key("n"_sr), integerValue(0)));
			append(mutations, MutationRef(MutationRef::SetValue, key("x"_sr), "initial-right"_sr));
		} else if (step == 1) {
			append(mutations, MutationRef(MutationRef::SetValue, key("c"_sr), "hot-only"_sr));
		} else {
			ASSERT_EQ(step, 2);
			append(mutations, MutationRef(MutationRef::SetValue, key("b"_sr), "left-before"_sr));
			append(mutations, MutationRef(MutationRef::SetValue, key("n"_sr), "middle-before"_sr));
			append(mutations, MutationRef(MutationRef::SetValue, key("x"_sr), "right-before"_sr));
			append(mutations, MutationRef(MutationRef::ClearRange, key("b"_sr), key("y"_sr)));
			append(mutations, MutationRef(MutationRef::SetValue, key("b"_sr), integerValue(0)));
			append(mutations, MutationRef(MutationRef::AddValue, key("b"_sr), integerValue(1)));
			append(mutations, MutationRef(MutationRef::SetValue, key("n"_sr), integerValue(0)));
			append(mutations, MutationRef(MutationRef::AddValue, key("n"_sr), integerValue(2)));
			append(mutations, MutationRef(MutationRef::SetValue, key("x"_sr), "right-after"_sr));
		}
		return mutations;
	}

	Mutations expectedMutations(Mutations const& writes) const {
		Mutations expected;
		const std::vector<Key> boundaries{ key("a"_sr), key("m"_sr), key("t"_sr), key("z"_sr) };
		for (int partition = 0; partition < 3; ++partition) {
			const KeyRangeRef range(boundaries[partition], boundaries[partition + 1]);
			for (const auto& mutation : writes) {
				if (mutation.type == MutationRef::ClearRange) {
					const KeyRangeRef clipped = range & KeyRangeRef(mutation.param1, mutation.param2);
					if (!clipped.empty()) {
						append(expected, MutationRef(MutationRef::ClearRange, clipped.begin, clipped.end));
					}
				} else if (range.contains(mutation.param1)) {
					append(expected, mutation);
				}
			}
		}
		return expected;
	}

	Future<Version> writeStep(Database cx, int step) {
		const Mutations mutations = writeMutations(step);
		const Key marker(StringRef(format("native-cdc-ordered/ledger/%d", step)));
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				// The marker is outside the selected keyspace and makes a commit-unknown
				// retry return the original version without duplicating the tested writes.
				const Optional<Value> prior = co_await tr.get(marker);
				if (prior.present()) {
					co_return decodeCDCMinVersionValue(prior.get());
				}
				for (const auto& mutation : mutations) {
					if (mutation.type == MutationRef::SetValue) {
						tr.set(mutation.param1, mutation.param2);
					} else if (mutation.type == MutationRef::ClearRange) {
						tr.clear(KeyRangeRef(mutation.param1, mutation.param2));
					} else {
						tr.atomicOp(mutation.param1, mutation.param2, static_cast<MutationRef::Type>(mutation.type));
					}
				}
				tr.atomicOp(marker, cdcVersionstampedMinVersionValue(), MutationRef::SetVersionstampedValue);
				co_await tr.commit();
				co_return tr.getCommittedVersion();
			} catch (Error& caught) {
				error = caught;
			}
			co_await tr.onError(error);
		}
	}

	static void apply(Keyspace& view, MutationRef const& mutation) {
		if (mutation.type == MutationRef::SetValue) {
			view[Key(mutation.param1)] = Value(mutation.param2);
		} else if (mutation.type == MutationRef::ClearRange) {
			auto begin = view.lower_bound(Key(mutation.param1));
			auto end = view.lower_bound(Key(mutation.param2));
			view.erase(begin, end);
		} else {
			ASSERT_EQ(mutation.type, MutationRef::AddValue);
			auto found = view.find(Key(mutation.param1));
			ASSERT(found != view.end());
			found->second = integerValue(integerValue(found->second) + integerValue(mutation.param2));
		}
	}

	Future<Void> consumeThrough(Reference<NativeCdcConsumer> consumer,
	                            Version target,
	                            Keyspace* view,
	                            std::set<Version>* observed,
	                            Optional<Version> replayFrom = {}) {
		Version previous = replayFrom.present() ? replayFrom.get() : consumer->position().lastConsumedVersion;
		while (previous < target) {
			TraceEvent("NativeCdcOrderedConsumeBegin")
			    .detail("Cursor", consumer->position().lastConsumedVersion)
			    .detail("Target", target);
			CDCConsumeReply reply = co_await consumer->consume();
			TraceEvent("NativeCdcOrderedConsumeReply")
			    .detail("Through", reply.lastConsumedVersion)
			    .detail("Versions", reply.mutations.size())
			    .detail("Target", target);
			ASSERT_GT(reply.lastConsumedVersion, previous);
			ASSERT_EQ(consumer->position().lastConsumedVersion, reply.lastConsumedVersion);
			for (const auto& versioned : reply.mutations) {
				ASSERT_GT(versioned.version, previous);
				ASSERT_LE(versioned.version, reply.lastConsumedVersion);
				previous = versioned.version;
				auto expected = expectedVersions.find(versioned.version);
				ASSERT(expected != expectedVersions.end());
				ASSERT(observed->insert(versioned.version).second);
				ASSERT_EQ(versioned.mutations.size(), expected->second.size());
				for (int i = 0; i < versioned.mutations.size(); ++i) {
					const auto& mutation = versioned.mutations[i];
					ASSERT_EQ(mutation.type, expected->second[i].type);
					ASSERT_EQ(mutation.param1, expected->second[i].param1);
					ASSERT_EQ(mutation.param2, expected->second[i].param2);
					apply(*view, mutation);
				}
			}
			previous = reply.lastConsumedVersion;
		}
		ASSERT(observed->contains(target));
	}

	Future<Void> verifyKeyspace(Database cx, Keyspace view) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				const RangeResult rows = co_await tr.getRange(selectedRange(), 100);
				ASSERT(!rows.more);
				ASSERT_EQ(rows.size(), view.size());
				auto expected = view.begin();
				for (const auto& row : rows) {
					ASSERT_EQ(row.key, expected->first);
					ASSERT_EQ(row.value, expected->second);
					++expected;
				}
				co_return;
			} catch (Error& caught) {
				error = caught;
			}
			co_await tr.onError(error);
		}
	}

	Future<std::vector<CDCStreamId>> readPartitions(Database cx, CDCStreamId streamId) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				const Optional<Value> encoded = co_await tr.get(cdcOrderedStreamKeyFor(streamId));
				ASSERT(encoded.present());
				const auto metadata = decodeCDCOrderedStreamValue(encoded.get());
				ASSERT_EQ(metadata.partitions().size(), 3);
				co_return metadata.partitions();
			} catch (Error& caught) {
				error = caught;
			}
			co_await tr.onError(error);
		}
	}

	Future<Void> verifyCommonMinimum(Database cx, std::vector<CDCStreamId> partitions, Version minimum) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				for (const CDCStreamId partition : partitions) {
					const Optional<Value> value = co_await tr.get(cdcMinVersionKeyFor(partition));
					ASSERT(value.present());
					ASSERT_EQ(decodeCDCMinVersionValue(value.get()), minimum);
				}
				co_return;
			} catch (Error& caught) {
				error = caught;
			}
			co_await tr.onError(error);
		}
	}

	Future<Void> verifyStatus(Database cx, CDCStreamId streamId, std::vector<CDCStreamId> partitions, Version minimum) {
		const NativeCdcStatus status = co_await getNativeCdcStatus(cx);
		ASSERT(status.metadataComplete);
		ASSERT_EQ(status.streams.size(), partitions.size() + 1);
		const auto expectedRanges = nativeCdcOrderedPartitionRanges(std::vector<KeyRange>{ selectedRange() },
		                                                            std::vector<Key>{ key("m"_sr), key("t"_sr) });
		std::set<CDCStreamId> found;
		for (const auto& stream : status.streams) {
			ASSERT(found.insert(stream.info.streamId).second);
			ASSERT_EQ(stream.info.name, name);
			ASSERT_EQ(stream.info.minVersion, minimum);
			if (stream.info.streamId == streamId) {
				ASSERT(!stream.orderedParent.present());
				ASSERT(!stream.owner.present());
				ASSERT(stream.partitions == partitions);
				ASSERT(stream.info.ranges == std::vector<KeyRange>{ selectedRange() });
			} else {
				const auto child = std::find(partitions.begin(), partitions.end(), stream.info.streamId);
				ASSERT(child != partitions.end());
				ASSERT(stream.orderedParent.present());
				ASSERT_EQ(stream.orderedParent.get(), streamId);
				ASSERT(stream.partitions.empty());
				ASSERT(stream.info.ranges == expectedRanges[child - partitions.begin()]);
			}
		}
		ASSERT(found.contains(streamId));
		std::set<CDCStreamId> blockers;
		for (const auto& tag : status.tags) {
			for (const CDCStreamId blocker : tag.blockingStreams) {
				ASSERT_NE(blocker, streamId);
				ASSERT(std::find(partitions.begin(), partitions.end(), blocker) != partitions.end());
				blockers.insert(blocker);
			}
		}
		ASSERT(blockers == std::set<CDCStreamId>(partitions.begin(), partitions.end()));
	}

	Future<Void> verifyIndependentAcknowledgementRejected(Database cx,
	                                                      std::vector<CDCStreamId> partitions,
	                                                      Version minimum,
	                                                      Version deliveredThrough) {
		ASSERT_GT(minimum, 0);
		ASSERT_GE(deliveredThrough, minimum);
		for (const CDCStreamId partition : partitions) {
			const Version duplicateMinimum =
			    co_await acknowledgeNativeCdcStream(cx, partition, minimum - 1, minimum - 1);
			ASSERT_EQ(duplicateMinimum, minimum);
			bool rejected = false;
			try {
				// Even proven delivered data cannot advance one child outside the group's atomic acknowledgement.
				co_await acknowledgeNativeCdcStream(cx, partition, deliveredThrough, deliveredThrough);
			} catch (Error& error) {
				if (error.code() == error_code_actor_cancelled) {
					throw;
				}
				ASSERT_EQ(error.code(), error_code_client_invalid_operation);
				rejected = true;
			}
			ASSERT(rejected);
			co_await verifyCommonMinimum(cx, partitions, minimum);
		}
	}

	Future<Void> waitForProxies(Database cx) {
		while (cx->clientInfo->get().cdcProxies.size() < 2) {
			co_await cx->clientInfo->onChange();
		}
	}

	Future<CDCProxyInterface> waitForOwners(Database cx,
	                                        std::vector<CDCStreamId> partitions,
	                                        Optional<UID> retiredOwner = {}) {
		while (true) {
			Future<Void> changed = cx->clientInfo->onChange();
			std::set<UID> owners;
			Optional<CDCProxyInterface> first;
			bool complete = true;
			const auto& info = cx->clientInfo->get();
			for (const CDCStreamId partition : partitions) {
				const auto assigned = info.streamToCDCProxyId.find(partition);
				if (assigned == info.streamToCDCProxyId.end() ||
				    (retiredOwner.present() && assigned->second == retiredOwner.get())) {
					complete = false;
					break;
				}
				const auto proxy =
				    std::find_if(info.cdcProxies.begin(), info.cdcProxies.end(), [&](const auto& candidate) {
					    return candidate.id() == assigned->second;
				    });
				if (proxy == info.cdcProxies.end()) {
					complete = false;
					break;
				}
				owners.insert(proxy->id());
				if (!first.present()) {
					first = *proxy;
				}
			}
			if (complete) {
				ASSERT_EQ(owners.size(), 2);
				co_return first.get();
			}
			co_await changed;
		}
	}

	Future<Void> replaceOwner(Database cx, CDCProxyInterface owner, std::vector<CDCStreamId> partitions) {
		while (std::find(cx->clientInfo->get().cdcProxies.begin(), cx->clientInfo->get().cdcProxies.end(), owner) !=
		       cx->clientInfo->get().cdcProxies.end()) {
			const ErrorOr<Void> halted = co_await owner.haltForTesting.tryGetReply(HaltCDCProxyRequest());
			if (halted.present()) {
				break;
			}
			co_await delay(0.1);
		}
		co_await waitForOwners(cx, partitions, owner.id());
	}

	Future<Void> waitForCleanup(Database cx, CDCStreamId streamId, std::vector<CDCStreamId> partitions) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				ASSERT(!(co_await tr.get(cdcStreamNameKeyFor(name))).present());
				ASSERT(!(co_await tr.get(cdcOrderedStreamKeyFor(streamId))).present());
				for (const CDCStreamId partition : partitions) {
					ASSERT(!(co_await tr.get(cdcStreamKeyFor(partition))).present());
					ASSERT(!(co_await tr.get(cdcMinVersionKeyFor(partition))).present());
					ASSERT(!(co_await tr.get(cdcOrderedParentKeyFor(partition))).present());
					ASSERT((co_await tr.getRange(cdcTagHistoryRangeFor(partition), 1)).empty());
					ASSERT((co_await tr.getRange(cdcProxyRangeFor(partition), 1)).empty());
				}
				const RangeResult markers = co_await tr.getRange(cdcRetiredTagPopKeys, 1);
				const RangeResult versions = co_await tr.getRange(cdcRetiredTagPopVersionKeys, 1);
				if (markers.empty() && versions.empty()) {
					co_return;
				}
				tr.reset();
				co_await delay(0.1);
				continue;
			} catch (Error& caught) {
				error = caught;
			}
			co_await tr.onError(error);
		}
	}

	static Future<Void> expectConsumerError(Reference<NativeCdcConsumer> consumer, int code, bool acknowledge = false) {
		bool failed = false;
		try {
			if (acknowledge) {
				co_await consumer->acknowledge();
			} else {
				co_await consumer->consume();
			}
		} catch (Error& error) {
			if (error.code() == error_code_actor_cancelled) {
				throw;
			}
			ASSERT_EQ(error.code(), code);
			failed = true;
		}
		ASSERT(failed);
	}

	static Future<Void> expectRegistrationCollision(Future<CDCStreamId> registration) {
		bool failed = false;
		try {
			co_await registration;
		} catch (Error& error) {
			if (error.code() == error_code_actor_cancelled) {
				throw;
			}
			ASSERT_EQ(error.code(), error_code_client_invalid_operation);
			failed = true;
		}
		ASSERT(failed);
	}

	Future<Void> run(Database cx) {
		auto phase = [](const char* name) { TraceEvent("NativeCdcOrderedPhase").detail("Phase", name); };
		phase("WaitForProxies");
		co_await waitForProxies(cx);
		const std::vector<KeyRange> ranges{ selectedRange() };
		const std::vector<Key> splitPoints{ key("m"_sr), key("t"_sr) };
		phase("OrdinaryRegistration");
		const CDCStreamId ordinaryId = co_await registerNativeCdcStreamClient(cx, name, ranges);
		co_await expectRegistrationCollision(registerNativeCdcOrderedStreamClient(cx, name, ranges, splitPoints));
		ASSERT_EQ(co_await registerNativeCdcStreamClient(cx, name, ranges), ordinaryId);
		co_await removeNativeCdcStreamClient(cx, name);
		phase("OrderedRegistration");
		const CDCStreamId streamId = co_await registerNativeCdcOrderedStreamClient(cx, name, ranges, splitPoints);
		ASSERT_NE(streamId, ordinaryId);
		co_await expectRegistrationCollision(registerNativeCdcStreamClient(cx, name, ranges));
		ASSERT_EQ(co_await registerNativeCdcOrderedStreamClient(cx, name, ranges, splitPoints), streamId);
		const std::vector<CDCStreamId> partitions = co_await readPartitions(cx, streamId);
		phase("WaitForOwners");
		const CDCProxyInterface originalOwner = co_await waitForOwners(cx, partitions);
		const auto listed = co_await listNativeCdcStreamsClient(cx);
		ASSERT_EQ(listed.size(), 1);
		ASSERT_EQ(listed.front().streamId, streamId);
		ASSERT(listed.front().ranges == ranges);
		Reference<NativeCdcConsumer> consumer = co_await createNativeCdcConsumer(cx, name);
		ASSERT_EQ(consumer->position().streamId, streamId);
		Keyspace view;
		std::set<Version> observed;
		phase("InitialConsumption");
		const Version initial = co_await writeStep(cx, 0);
		expectedVersions.emplace(initial, expectedMutations(writeMutations(0)));
		co_await consumeThrough(consumer, initial, &view, &observed);
		co_await verifyKeyspace(cx, view);
		phase("InitialAcknowledgement");
		co_await consumer->acknowledge();
		const CDCCursor checkpoint = consumer->position();
		const Keyspace checkpointView = view;
		co_await verifyCommonMinimum(cx, partitions, checkpoint.lastConsumedVersion + 1);
		phase("Status");
		co_await verifyStatus(cx, streamId, partitions, checkpoint.lastConsumedVersion + 1);

		// Neither middle nor right receives this version. Their certified empty
		// progress must allow the ordered consumer to return the left mutation.
		phase("QuietPartitions");
		const Version quiet = co_await writeStep(cx, 1);
		ASSERT_GT(quiet, checkpoint.lastConsumedVersion);
		expectedVersions.emplace(quiet, expectedMutations(writeMutations(1)));
		co_await consumeThrough(consumer, quiet, &view, &observed);
		co_await verifyKeyspace(cx, view);
		phase("MixedMutations");
		const Version mixed = co_await writeStep(cx, 2);
		ASSERT_GT(mixed, quiet);
		expectedVersions.emplace(mixed, expectedMutations(writeMutations(2)));
		co_await consumeThrough(consumer, mixed, &view, &observed);
		co_await verifyKeyspace(cx, view);
		ASSERT_EQ(observed.size(), 3);
		co_await verifyCommonMinimum(cx, partitions, checkpoint.lastConsumedVersion + 1);
		phase("IndependentAcknowledgement");
		co_await verifyIndependentAcknowledgementRejected(
		    cx, partitions, checkpoint.lastConsumedVersion + 1, consumer->position().lastConsumedVersion);

		// A canceled operation invalidates the aggregate's speculative position.
		// Reusing the same object must replay every unacknowledged complete version.
		phase("CancellationReplay");
		Future<CDCConsumeReply> cancelled = consumer->consume();
		ASSERT(!cancelled.isReady());
		cancelled.cancel();
		view = checkpointView;
		observed.clear();
		co_await consumeThrough(consumer, mixed, &view, &observed, checkpoint.lastConsumedVersion);
		ASSERT_EQ(observed.size(), 2);
		ASSERT(observed.contains(quiet));
		co_await verifyKeyspace(cx, view);
		co_await verifyCommonMinimum(cx, partitions, checkpoint.lastConsumedVersion + 1);

		if (testOwnerReplacement) {
			phase("OwnerReplacement");
			co_await replaceOwner(cx, originalOwner, partitions);
			view = checkpointView;
			observed.clear();
			co_await consumeThrough(consumer, mixed, &view, &observed, checkpoint.lastConsumedVersion);
			ASSERT_EQ(observed.size(), 2);
			ASSERT(observed.contains(quiet));
			co_await verifyKeyspace(cx, view);
			co_await verifyCommonMinimum(cx, partitions, checkpoint.lastConsumedVersion + 1);
		}
		phase("ScalarResume");
		consumer = Reference<NativeCdcConsumer>();
		consumer = resumeNativeCdcConsumer(cx, checkpoint);
		view = checkpointView;
		observed.clear();
		co_await consumeThrough(consumer, mixed, &view, &observed);
		ASSERT_EQ(observed.size(), 2);
		ASSERT(observed.contains(quiet));
		co_await verifyKeyspace(cx, view);
		co_await consumer->acknowledge();
		co_await verifyCommonMinimum(cx, partitions, consumer->position().lastConsumedVersion + 1);
		phase("StaleResume");
		Reference<NativeCdcConsumer> stale = resumeNativeCdcConsumer(cx, checkpoint);
		co_await expectConsumerError(stale, error_code_transaction_too_old);
		phase("Removal");
		co_await removeNativeCdcStreamClient(cx, name);
		co_await expectConsumerError(consumer, error_code_client_invalid_operation);
		co_await expectConsumerError(consumer, error_code_client_invalid_operation, true);
		phase("Cleanup");
		co_await waitForCleanup(cx, streamId, partitions);
		ASSERT((co_await listNativeCdcStreamsClient(cx)).empty());
		phase("Complete");
		completed = true;
	}

	Future<Void> clearFixture(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.clear(KeyRangeRef("native-cdc-ordered/"_sr, "native-cdc-ordered0"_sr));
				co_await tr.commit();
				co_return;
			} catch (Error& caught) {
				error = caught;
			}
			co_await tr.onError(error);
		}
	}

public:
	static constexpr auto NAME = "NativeCdcOrdered";

	explicit NativeCdcOrderedWorkload(WorkloadContext const& context) : TestWorkload(context) {
		operationTimeout = getOption(options, "operationTimeout"_sr, 180.0);
		testOwnerReplacement = getOption(options, "testOwnerReplacement"_sr, true);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("RandomRangeLock"); }
	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			return clearFixture(cx);
		}
		return Void();
	}
	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return timeoutError(run(cx), operationTimeout);
		}
		return Void();
	}
	Future<bool> check(Database const& cx) override { return clientId != 0 || completed; }
	void getMetrics(std::vector<PerfMetric>& metrics) override {}
};

WorkloadFactory<NativeCdcOrderedWorkload> NativeCdcOrderedWorkloadFactory;
