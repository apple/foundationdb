/*
 * ConfigNode.actor.cpp
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

#include <map>

#include "fdbclient/SystemData.h"
#include "fdbserver/ConfigNode.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/OnDemandStore.h"
#include "flow/Arena.h"
#include "flow/genericactors.actor.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

const KeyRef coordinatorsHashKey = "id"_sr;
const KeyRef lastCompactedVersionKey = "lastCompactedVersion"_sr;
const KeyRef currentGenerationKey = "currentGeneration"_sr;
const KeyRef registeredKey = "registered"_sr;
const KeyRef lockedKey = "locked"_sr;
const KeyRangeRef kvKeys = KeyRangeRef("kv/"_sr, "kv0"_sr);
const KeyRangeRef mutationKeys = KeyRangeRef("mutation/"_sr, "mutation0"_sr);
const KeyRangeRef annotationKeys = KeyRangeRef("annotation/"_sr, "annotation0"_sr);

Key versionedAnnotationKey(Version version) {
	ASSERT_GE(version, 0);
	return BinaryWriter::toValue(bigEndian64(version), IncludeVersion()).withPrefix(annotationKeys.begin);
}

Version getVersionFromVersionedAnnotationKey(KeyRef versionedAnnotationKey) {
	return fromBigEndian64(BinaryReader::fromStringRef<uint64_t>(
	    versionedAnnotationKey.removePrefix(annotationKeys.begin), IncludeVersion()));
}

Key versionedMutationKey(Version version, uint32_t index) {
	ASSERT_GE(version, 0);
	BinaryWriter bw(IncludeVersion());
	bw << bigEndian64(version);
	bw << bigEndian32(index);
	return bw.toValue().withPrefix(mutationKeys.begin);
}

Version getVersionFromVersionedMutationKey(KeyRef versionedMutationKey) {
	uint64_t bigEndianResult;
	ASSERT(versionedMutationKey.startsWith(mutationKeys.begin));
	BinaryReader br(versionedMutationKey.removePrefix(mutationKeys.begin), IncludeVersion());
	br >> bigEndianResult;
	return fromBigEndian64(bigEndianResult);
}

template <typename T>
void assertCommitted(RangeResult const& range, VectorRef<T> versionedConfigs, std::function<Version(KeyRef)> fn) {
	if (range.size() == 0) {
		return;
	}
	// Verify every versioned value read from disk (minus the last one which
	// may not be committed on a quorum) exists in the rollforward changes.
	for (auto it = range.begin(); it != std::prev(range.end()); ++it) {
		Version version = fn(it->key);
		auto resultIt = std::find_if(
		    versionedConfigs.begin(), versionedConfigs.end(), [version](T const& o) { return o.version == version; });
		ASSERT(resultIt != versionedConfigs.end());
	}
}

} // namespace

TEST_CASE("/fdbserver/ConfigDB/ConfigNode/Internal/versionedMutationKeys") {
	std::vector<Key> keys;
	for (Version version = 0; version < 1000; ++version) {
		for (int index = 0; index < 5; ++index) {
			keys.push_back(versionedMutationKey(version, index));
		}
	}
	for (int i = 0; i < 5000; ++i) {
		ASSERT(getVersionFromVersionedMutationKey(keys[i]) == i / 5);
	}
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/ConfigNode/Internal/versionedMutationKeyOrdering") {
	Standalone<VectorRef<KeyRef>> keys;
	for (Version version = 0; version < 1000; ++version) {
		for (auto index = 0; index < 5; ++index) {
			keys.push_back_deep(keys.arena(), versionedMutationKey(version, index));
		}
	}
	for (auto index = 0; index < 1000; ++index) {
		keys.push_back_deep(keys.arena(), versionedMutationKey(1000, index));
	}
	ASSERT(std::is_sorted(keys.begin(), keys.end()));
	return Void();
}

class ConfigNodeImpl {
	UID id;
	FlowLock lock;
	OnDemandStore kvStore;
	CounterCollection cc;

	// Follower counters
	Counter compactRequests;
	Counter rollbackRequests;
	Counter rollforwardRequests;
	Counter successfulChangeRequests;
	Counter failedChangeRequests;
	Counter snapshotRequests;
	Counter getCommittedVersionRequests;
	Counter lockRequests;

	// Transaction counters
	Counter successfulCommits;
	Counter failedCommits;
	Counter setMutations;
	Counter clearMutations;
	Counter getValueRequests;
	Counter getGenerationRequests;
	Future<Void> logger;

	ACTOR static Future<CoordinatorsHash> getCoordinatorsHash(ConfigNodeImpl* self) {
		state CoordinatorsHash coordinatorsHash = 0;
		Optional<Value> value = wait(self->kvStore->readValue(coordinatorsHashKey));
		if (value.present()) {
			coordinatorsHash = BinaryReader::fromStringRef<CoordinatorsHash>(value.get(), IncludeVersion());
		} else {
			self->kvStore->set(
			    KeyValueRef(coordinatorsHashKey, BinaryWriter::toValue(coordinatorsHash, IncludeVersion())));
			wait(self->kvStore->commit());
		}
		return coordinatorsHash;
	}

	ACTOR static Future<Optional<CoordinatorsHash>> getLocked(ConfigNodeImpl* self) {
		Optional<Value> value = wait(self->kvStore->readValue(lockedKey));
		if (!value.present()) {
			return Optional<CoordinatorsHash>();
		}
		return BinaryReader::fromStringRef<Optional<CoordinatorsHash>>(value.get(), IncludeVersion());
	}

	ACTOR static Future<ConfigGeneration> getGeneration(ConfigNodeImpl* self) {
		state ConfigGeneration generation;
		Optional<Value> value = wait(self->kvStore->readValue(currentGenerationKey));
		if (value.present()) {
			generation = BinaryReader::fromStringRef<ConfigGeneration>(value.get(), IncludeVersion());
		} else {
			self->kvStore->set(KeyValueRef(currentGenerationKey, BinaryWriter::toValue(generation, IncludeVersion())));
			wait(self->kvStore->commit());
		}
		return generation;
	}

	ACTOR static Future<Version> getLastCompactedVersion(ConfigNodeImpl* self) {
		Optional<Value> value = wait(self->kvStore->readValue(lastCompactedVersionKey));
		state Version lastCompactedVersion = 0;
		if (value.present()) {
			lastCompactedVersion = BinaryReader::fromStringRef<Version>(value.get(), IncludeVersion());
		} else {
			self->kvStore->set(
			    KeyValueRef(lastCompactedVersionKey, BinaryWriter::toValue(lastCompactedVersion, IncludeVersion())));
			wait(self->kvStore->commit());
		}
		return lastCompactedVersion;
	}

	// Returns all commit annotations between for commits with version in [startVersion, endVersion]
	ACTOR static Future<Standalone<VectorRef<VersionedConfigCommitAnnotationRef>>> getAnnotations(ConfigNodeImpl* self,
	                                                                                              Version startVersion,
	                                                                                              Version endVersion) {
		Key startKey = versionedAnnotationKey(startVersion);
		Key endKey = versionedAnnotationKey(endVersion + 1);
		state KeyRangeRef keys(startKey, endKey);
		RangeResult range = wait(self->kvStore->readRange(keys));
		Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> result;
		for (const auto& kv : range) {
			auto version = getVersionFromVersionedAnnotationKey(kv.key);
			ASSERT_LE(version, endVersion);
			auto annotation = BinaryReader::fromStringRef<ConfigCommitAnnotation>(kv.value, IncludeVersion());
			result.emplace_back_deep(result.arena(), version, annotation);
		}
		return result;
	}

	// Returns all mutations with version in [startVersion, endVersion]
	ACTOR static Future<Standalone<VectorRef<VersionedConfigMutationRef>>> getMutations(ConfigNodeImpl* self,
	                                                                                    Version startVersion,
	                                                                                    Version endVersion) {
		Key startKey = versionedMutationKey(startVersion, 0);
		Key endKey = versionedMutationKey(endVersion + 1, 0);
		state KeyRangeRef keys(startKey, endKey);
		RangeResult range = wait(self->kvStore->readRange(keys));
		Standalone<VectorRef<VersionedConfigMutationRef>> result;
		for (const auto& kv : range) {
			auto version = getVersionFromVersionedMutationKey(kv.key);
			ASSERT_LE(version, endVersion);
			auto mutation = ObjectReader::fromStringRef<ConfigMutation>(kv.value, IncludeVersion());
			result.emplace_back_deep(result.arena(), version, mutation);
		}
		return result;
	}

	ACTOR static Future<Void> getChanges(ConfigNodeImpl* self, ConfigFollowerGetChangesRequest req) {
		wait(self->lock.take());
		state FlowLock::Releaser releaser(self->lock);
		Version lastCompactedVersion = wait(getLastCompactedVersion(self));
		if (req.lastSeenVersion < lastCompactedVersion) {
			++self->failedChangeRequests;
			req.reply.sendError(version_already_compacted());
			return Void();
		}
		state Version committedVersion =
		    wait(map(getGeneration(self), [](auto const& gen) { return gen.committedVersion; }));
		if (committedVersion < req.mostRecentVersion) {
			// Handle a very rare case where a ConfigNode loses data between
			// responding with a committed version and responding to the
			// subsequent get changes request.
			CODE_PROBE(true, "ConfigNode data loss occurred on a minority of coordinators", probe::decoration::rare);
			req.reply.sendError(process_behind()); // Reuse the process_behind error
			return Void();
		}
		if (BUGGIFY) {
			wait(delay(deterministicRandom()->random01() * 2));
		}
		state Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations =
		    wait(getMutations(self, req.lastSeenVersion + 1, req.mostRecentVersion));
		state Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> versionedAnnotations =
		    wait(getAnnotations(self, req.lastSeenVersion + 1, req.mostRecentVersion));
		TraceEvent(SevInfo, "ConfigNodeSendingChanges", self->id)
		    .detail("ReqLastSeenVersion", req.lastSeenVersion)
		    .detail("ReqMostRecentVersion", req.mostRecentVersion)
		    .detail("CommittedVersion", committedVersion)
		    .detail("NumMutations", versionedMutations.size())
		    .detail("NumCommits", versionedAnnotations.size());
		++self->successfulChangeRequests;
		req.reply.send(ConfigFollowerGetChangesReply{ versionedMutations, versionedAnnotations });
		return Void();
	}

	// New transactions increment the database's current live version. This effectively serves as a lock, providing
	// serializability
	ACTOR static Future<Void> getNewGeneration(ConfigNodeImpl* self, ConfigTransactionGetGenerationRequest req) {
		state CoordinatorsHash coordinatorsHash = wait(getCoordinatorsHash(self));
		if (req.coordinatorsHash != coordinatorsHash) {
			req.reply.sendError(coordinators_changed());
			return Void();
		}
		state ConfigGeneration generation = wait(getGeneration(self));
		++generation.liveVersion;
		if (req.lastSeenLiveVersion.present()) {
			CODE_PROBE(req.lastSeenLiveVersion.get() >= generation.liveVersion,
			           "Node is lagging behind some other node");
			generation.liveVersion = std::max(generation.liveVersion, req.lastSeenLiveVersion.get() + 1);
		}
		self->kvStore->set(KeyValueRef(currentGenerationKey, BinaryWriter::toValue(generation, IncludeVersion())));
		wait(self->kvStore->commit());
		req.reply.send(ConfigTransactionGetGenerationReply{ generation });
		return Void();
	}

	ACTOR static Future<Void> get(ConfigNodeImpl* self, ConfigTransactionGetRequest req) {
		state Optional<CoordinatorsHash> locked = wait(getLocked(self));
		if (locked.present()) {
			CODE_PROBE(true, "attempting to read from a locked ConfigNode");
			req.reply.sendError(coordinators_changed());
			return Void();
		}
		state CoordinatorsHash coordinatorsHash = wait(getCoordinatorsHash(self));
		if (req.coordinatorsHash != coordinatorsHash) {
			req.reply.sendError(coordinators_changed());
			return Void();
		}
		ConfigGeneration currentGeneration = wait(getGeneration(self));
		if (req.generation != currentGeneration) {
			// TODO: Also send information about highest seen version
			req.reply.sendError(transaction_too_old());
			return Void();
		}
		state Optional<Value> serializedValue =
		    wait(self->kvStore->readValue(BinaryWriter::toValue(req.key, IncludeVersion()).withPrefix(kvKeys.begin)));
		state Optional<KnobValue> value;
		if (serializedValue.present()) {
			value = ObjectReader::fromStringRef<KnobValue>(serializedValue.get(), IncludeVersion());
		}
		Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations =
		    wait(getMutations(self, 0, req.generation.committedVersion));
		for (const auto& versionedMutation : versionedMutations) {
			const auto& mutation = versionedMutation.mutation;
			if (mutation.getKey() == req.key) {
				if (mutation.isSet()) {
					value = mutation.getValue();
				} else {
					value = {};
				}
			}
		}
		req.reply.send(ConfigTransactionGetReply{ value });
		return Void();
	}

	// Retrieve all configuration classes that contain explicitly defined knobs
	// TODO: Currently it is possible that extra configuration classes may be returned, we
	// may want to fix this to clean up the contract
	ACTOR static Future<Void> getConfigClasses(ConfigNodeImpl* self, ConfigTransactionGetConfigClassesRequest req) {
		state Optional<CoordinatorsHash> locked = wait(getLocked(self));
		if (locked.present()) {
			CODE_PROBE(true, "attempting to read config classes from locked ConfigNode", probe::decoration::rare);
			req.reply.sendError(coordinators_changed());
			return Void();
		}
		state CoordinatorsHash coordinatorsHash = wait(getCoordinatorsHash(self));
		if (req.coordinatorsHash != coordinatorsHash) {
			req.reply.sendError(coordinators_changed());
			return Void();
		}
		ConfigGeneration currentGeneration = wait(getGeneration(self));
		if (req.generation != currentGeneration) {
			req.reply.sendError(transaction_too_old());
			return Void();
		}
		state RangeResult snapshot = wait(self->kvStore->readRange(kvKeys));
		state std::set<Key> configClassesSet;
		for (const auto& kv : snapshot) {
			auto configKey =
			    BinaryReader::fromStringRef<ConfigKey>(kv.key.removePrefix(kvKeys.begin), IncludeVersion());
			if (configKey.configClass.present()) {
				configClassesSet.insert(configKey.configClass.get());
			}
		}
		state Version lastCompactedVersion = wait(getLastCompactedVersion(self));
		state Standalone<VectorRef<VersionedConfigMutationRef>> mutations =
		    wait(getMutations(self, lastCompactedVersion + 1, req.generation.committedVersion));
		for (const auto& versionedMutation : mutations) {
			auto configClass = versionedMutation.mutation.getConfigClass();
			if (configClass.present()) {
				configClassesSet.insert(configClass.get());
			}
		}
		Standalone<VectorRef<KeyRef>> configClasses;
		for (const auto& configClass : configClassesSet) {
			configClasses.push_back_deep(configClasses.arena(), configClass);
		}
		req.reply.send(ConfigTransactionGetConfigClassesReply{ configClasses });
		return Void();
	}

	// Retrieve all knobs explicitly defined for the specified configuration class
	ACTOR static Future<Void> getKnobs(ConfigNodeImpl* self, ConfigTransactionGetKnobsRequest req) {
		state Optional<CoordinatorsHash> locked = wait(getLocked(self));
		if (locked.present()) {
			CODE_PROBE(true, "attempting to read knobs from locked ConfigNode", probe::decoration::rare);
			req.reply.sendError(coordinators_changed());
			return Void();
		}
		state CoordinatorsHash coordinatorsHash = wait(getCoordinatorsHash(self));
		if (req.coordinatorsHash != coordinatorsHash) {
			req.reply.sendError(coordinators_changed());
			return Void();
		}

		ConfigGeneration currentGeneration = wait(getGeneration(self));
		if (req.generation != currentGeneration) {
			req.reply.sendError(transaction_too_old());
			return Void();
		}
		// FIXME: Filtering after reading from disk is very inefficient
		state RangeResult snapshot = wait(self->kvStore->readRange(kvKeys));
		state std::set<Key> knobSet;
		for (const auto& kv : snapshot) {
			auto configKey =
			    BinaryReader::fromStringRef<ConfigKey>(kv.key.removePrefix(kvKeys.begin), IncludeVersion());
			if (configKey.configClass.template castTo<Key>() == req.configClass) {
				knobSet.insert(configKey.knobName);
			}
		}
		state Version lastCompactedVersion = wait(getLastCompactedVersion(self));
		state Standalone<VectorRef<VersionedConfigMutationRef>> mutations =
		    wait(getMutations(self, lastCompactedVersion + 1, req.generation.committedVersion));
		for (const auto& versionedMutation : mutations) {
			if (versionedMutation.mutation.getConfigClass().template castTo<Key>() == req.configClass) {
				if (versionedMutation.mutation.isSet()) {
					knobSet.insert(versionedMutation.mutation.getKnobName());
				} else {
					knobSet.erase(versionedMutation.mutation.getKnobName());
				}
			}
		}
		Standalone<VectorRef<KeyRef>> knobNames;
		for (const auto& knobName : knobSet) {
			knobNames.push_back_deep(knobNames.arena(), knobName);
		}
		req.reply.send(ConfigTransactionGetKnobsReply{ knobNames });
		return Void();
	}

	ACTOR static Future<Void> commitMutations(ConfigNodeImpl* self,
	                                          Standalone<VectorRef<VersionedConfigMutationRef>> mutations,
	                                          Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations,
	                                          Version commitVersion,
	                                          Version liveVersion = ::invalidVersion) {
		wait(self->lock.take());
		state FlowLock::Releaser releaser(self->lock);
		Version latestVersion = 0;
		int index = 0;
		for (const auto& mutation : mutations) {
			if (mutation.version > commitVersion) {
				continue;
			}
			// Mutations should be in ascending version order.
			ASSERT_GE(mutation.version, latestVersion);
			if (mutation.version > latestVersion) {
				latestVersion = mutation.version;
				index = 0;
			}
			Key key = versionedMutationKey(mutation.version, index++);
			Value value = ObjectWriter::toValue(mutation.mutation, IncludeVersion());
			if (mutation.mutation.isSet()) {
				TraceEvent("ConfigNodeSetting")
				    .detail("ConfigClass", mutation.mutation.getConfigClass())
				    .detail("KnobName", mutation.mutation.getKnobName())
				    .detail("Value", mutation.mutation.getValue().toString())
				    .detail("Version", mutation.version);
				++self->setMutations;
			} else {
				++self->clearMutations;
			}
			self->kvStore->set(KeyValueRef(key, value));
		}
		for (const auto& annotation : annotations) {
			self->kvStore->set(KeyValueRef(versionedAnnotationKey(annotation.version),
			                               BinaryWriter::toValue(annotation.annotation, IncludeVersion())));
		}
		ConfigGeneration newGeneration = { commitVersion, commitVersion };
		if (liveVersion != ::invalidVersion) {
			newGeneration.liveVersion = liveVersion;
		}
		self->kvStore->set(KeyValueRef(currentGenerationKey, BinaryWriter::toValue(newGeneration, IncludeVersion())));
		wait(self->kvStore->commit());
		++self->successfulCommits;
		return Void();
	}

	ACTOR static Future<Void> commit(ConfigNodeImpl* self, ConfigTransactionCommitRequest req) {
		state Optional<CoordinatorsHash> locked = wait(getLocked(self));
		if (locked.present()) {
			CODE_PROBE(true, "attempting to write to locked ConfigNode");
			req.reply.sendError(coordinators_changed());
			return Void();
		}
		state CoordinatorsHash coordinatorsHash = wait(getCoordinatorsHash(self));
		if (req.coordinatorsHash != coordinatorsHash) {
			req.reply.sendError(coordinators_changed());
			return Void();
		}

		ConfigGeneration currentGeneration = wait(getGeneration(self));
		if (req.generation.committedVersion != currentGeneration.committedVersion) {
			++self->failedCommits;
			req.reply.sendError(commit_unknown_result());
			return Void();
		} else if (req.generation.liveVersion != currentGeneration.liveVersion) {
			++self->failedCommits;
			req.reply.sendError(not_committed());
			return Void();
		}
		Standalone<VectorRef<VersionedConfigMutationRef>> mutations;
		for (const auto& mutation : req.mutations) {
			mutations.emplace_back_deep(mutations.arena(), req.generation.liveVersion, mutation);
		}
		Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations;
		annotations.emplace_back_deep(annotations.arena(), req.generation.liveVersion, req.annotation);

		wait(commitMutations(self, mutations, annotations, req.generation.liveVersion));
		req.reply.send(Void());
		return Void();
	}

	ACTOR static Future<Void> serve(ConfigNodeImpl* self, ConfigTransactionInterface const* cti) {
		loop {
			choose {
				when(ConfigTransactionGetGenerationRequest req = waitNext(cti->getGeneration.getFuture())) {
					++self->getGenerationRequests;
					wait(getNewGeneration(self, req));
				}
				when(ConfigTransactionGetRequest req = waitNext(cti->get.getFuture())) {
					++self->getValueRequests;
					wait(get(self, req));
				}
				when(ConfigTransactionCommitRequest req = waitNext(cti->commit.getFuture())) {
					wait(commit(self, req));
				}
				when(ConfigTransactionGetConfigClassesRequest req = waitNext(cti->getClasses.getFuture())) {
					wait(getConfigClasses(self, req));
				}
				when(ConfigTransactionGetKnobsRequest req = waitNext(cti->getKnobs.getFuture())) {
					wait(getKnobs(self, req));
				}
				when(wait(self->kvStore->getError())) { ASSERT(false); }
			}
		}
	}

	ACTOR static Future<Void> getSnapshotAndChanges(ConfigNodeImpl* self,
	                                                ConfigFollowerGetSnapshotAndChangesRequest req) {
		state ConfigFollowerGetSnapshotAndChangesReply reply;
		RangeResult data = wait(self->kvStore->readRange(kvKeys));
		for (const auto& kv : data) {
			reply
			    .snapshot[BinaryReader::fromStringRef<ConfigKey>(kv.key.removePrefix(kvKeys.begin), IncludeVersion())] =
			    ObjectReader::fromStringRef<KnobValue>(kv.value, IncludeVersion());
		}
		wait(store(reply.snapshotVersion, getLastCompactedVersion(self)));
		if (req.mostRecentVersion < reply.snapshotVersion) {
			// The version in the request can be less than the last compacted
			// version in certain circumstances where the coordinators are
			// being changed and the consumer reads the latest committed
			// version from a majority of ConfigNodes before they have received
			// up to date snapshots. This should be fine, it just means the
			// consumer needs to fetch the latest version and retry its
			// request.
			CODE_PROBE(true, "ConfigNode ahead of consumer", probe::decoration::rare);
			req.reply.sendError(version_already_compacted());
			return Void();
		}
		wait(store(reply.changes, getMutations(self, reply.snapshotVersion + 1, req.mostRecentVersion)));
		wait(store(reply.annotations, getAnnotations(self, reply.snapshotVersion + 1, req.mostRecentVersion)));
		TraceEvent(SevInfo, "ConfigNodeGettingSnapshot", self->id)
		    .detail("SnapshotVersion", reply.snapshotVersion)
		    .detail("SnapshotSize", reply.snapshot.size())
		    .detail("ChangesSize", reply.changes.size())
		    .detail("AnnotationsSize", reply.annotations.size());
		req.reply.send(reply);
		return Void();
	}

	// Apply mutations from the WAL in mutationKeys into the kvKeys key space.
	// Periodic compaction prevents the database from growing too large, and improve read performance.
	// However, commit annotations for compacted mutations are lost
	ACTOR static Future<Void> compact(ConfigNodeImpl* self, ConfigFollowerCompactRequest req) {
		state Version lastCompactedVersion = wait(getLastCompactedVersion(self));
		TraceEvent(SevInfo, "ConfigNodeCompacting", self->id)
		    .detail("Version", req.version)
		    .detail("LastCompacted", lastCompactedVersion);
		if (req.version <= lastCompactedVersion) {
			req.reply.send(Void());
			return Void();
		}
		Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations =
		    wait(getMutations(self, lastCompactedVersion + 1, req.version));
		self->kvStore->clear(
		    KeyRangeRef(versionedMutationKey(lastCompactedVersion + 1, 0), versionedMutationKey(req.version + 1, 0)));
		self->kvStore->clear(
		    KeyRangeRef(versionedAnnotationKey(lastCompactedVersion + 1), versionedAnnotationKey(req.version + 1)));
		for (const auto& versionedMutation : versionedMutations) {
			const auto& version = versionedMutation.version;
			const auto& mutation = versionedMutation.mutation;
			if (version > req.version) {
				break;
			} else {
				TraceEvent(SevDebug, "ConfigNodeCompactionApplyingMutation", self->id)
				    .detail("IsSet", mutation.isSet())
				    .detail("MutationVersion", version)
				    .detail("LastCompactedVersion", lastCompactedVersion)
				    .detail("ReqVersion", req.version);
				auto serializedKey = BinaryWriter::toValue(mutation.getKey(), IncludeVersion());
				if (mutation.isSet()) {
					self->kvStore->set(KeyValueRef(serializedKey.withPrefix(kvKeys.begin),
					                               ObjectWriter::toValue(mutation.getValue(), IncludeVersion())));
				} else {
					self->kvStore->clear(singleKeyRange(serializedKey.withPrefix(kvKeys.begin)));
				}
				lastCompactedVersion = version;
			}
		}
		self->kvStore->set(
		    KeyValueRef(lastCompactedVersionKey, BinaryWriter::toValue(lastCompactedVersion, IncludeVersion())));
		wait(self->kvStore->commit());
		req.reply.send(Void());
		return Void();
	}

	ACTOR static Future<Void> rollforward(ConfigNodeImpl* self, ConfigFollowerRollforwardRequest req) {
		state Version lastCompactedVersion = wait(getLastCompactedVersion(self));
		if (req.lastKnownCommitted < lastCompactedVersion) {
			req.reply.sendError(version_already_compacted());
			return Void();
		}
		state ConfigGeneration currentGeneration = wait(getGeneration(self));
		if (req.lastKnownCommitted != currentGeneration.committedVersion) {
			req.reply.sendError(transaction_too_old());
			return Void();
		}
		TraceEvent("ConfigNodeRollforward", self->id)
		    .detail("RollbackTo", req.rollback)
		    .detail("Target", req.target)
		    .detail("LastKnownCommitted", req.lastKnownCommitted)
		    .detail("Committed", currentGeneration.committedVersion)
		    .detail("CurrentGeneration", currentGeneration.toString())
		    .detail("LastCompactedVersion", lastCompactedVersion)
		    .detail("SpecialZeroQuorum", req.specialZeroQuorum);
		// Rollback to prior known committed version to erase any commits not
		// made on a quorum.
		if (req.rollback.present() && req.rollback.get() < currentGeneration.committedVersion) {
			if (g_network->isSimulated()) {
				RangeResult mutationRange = wait(self->kvStore->readRange(
				    KeyRangeRef(versionedMutationKey(req.rollback.get() + 1, 0),
				                versionedMutationKey(currentGeneration.committedVersion + 1, 0))));
				// assertCommitted(mutationRange, req.mutations, getVersionFromVersionedMutationKey);
				RangeResult annotationRange = wait(self->kvStore->readRange(
				    KeyRangeRef(versionedAnnotationKey(req.rollback.get() + 1),
				                versionedAnnotationKey(currentGeneration.committedVersion + 1))));
				// assertCommitted(annotationRange, req.annotations, getVersionFromVersionedAnnotationKey);
			}
			self->kvStore->clear(KeyRangeRef(versionedMutationKey(req.rollback.get() + 1, 0),
			                                 versionedMutationKey(currentGeneration.committedVersion + 1, 0)));
			self->kvStore->clear(KeyRangeRef(versionedAnnotationKey(req.rollback.get() + 1),
			                                 versionedAnnotationKey(currentGeneration.committedVersion + 1)));

			currentGeneration.committedVersion = req.rollback.get();
			if (req.rollback.get() < lastCompactedVersion) {
				self->kvStore->set(
				    KeyValueRef(lastCompactedVersionKey, BinaryWriter::toValue(req.rollback.get(), IncludeVersion())));
			}
			// The mutation commit loop below should persist the new generation
			// to disk, so we don't need to do it here.
		}
		// Now rollforward by applying all mutations between last known
		// committed version and rollforward version.
		if (req.mutations.size() > 0) {
			ASSERT_GT(req.mutations.size(), 0);
			ASSERT_GT(req.mutations[0].version, currentGeneration.committedVersion);
			wait(commitMutations(self,
			                     req.mutations,
			                     req.annotations,
			                     req.target,
			                     req.specialZeroQuorum ? currentGeneration.liveVersion : ::invalidVersion));
		}

		req.reply.send(Void());
		return Void();
	}

	ACTOR static Future<Void> getCommittedVersion(ConfigNodeImpl* self, ConfigFollowerGetCommittedVersionRequest req) {
		state Version lastCompacted = wait(getLastCompactedVersion(self));
		state ConfigGeneration generation = wait(getGeneration(self));
		bool isRegistered = wait(registered(self));
		req.reply.send(ConfigFollowerGetCommittedVersionReply{
		    isRegistered, lastCompacted, generation.liveVersion, generation.committedVersion });
		return Void();
	}

	// Requires ConfigNodes to register with the ConfigBroadcaster before being
	// allowed to respond to most requests. The ConfigBroadcaster will first
	// ask the ConfigNode whether it is registered (kickstarted by the worker
	// registering with the cluster controller). Then, the ConfigBroadcaster
	// will send the ConfigNode a ready message, containing a snapshot if the
	// ConfigNode is a new coordinator and needs updated state, or empty
	// otherwise.
	ACTOR static Future<Void> serve(ConfigNodeImpl* self, ConfigBroadcastInterface const* cbi, bool infinite) {
		loop {
			// Normally, the ConfigBroadcaster will first send a
			// ConfigBroadcastRegisteredRequest, followed by a
			// ConfigBroadcastReadyRequest. However, if the cluster controller
			// the broadcaster is running on fails in between sending these two
			// messages, the new broadcaster may need to resend its registered
			// request. So we need to listen for either message in a loop to
			// guarantee no pattern of failure will result in a stuck
			// ConfigNode.
			choose {
				when(state ConfigBroadcastRegisteredRequest req = waitNext(cbi->registered.getFuture())) {
					state bool isRegistered = wait(registered(self));
					ConfigGeneration generation = wait(getGeneration(self));
					TraceEvent("ConfigNodeSendingRegisteredReply", self->id)
					    .detail("Generation", generation.toString());
					req.reply.send(ConfigBroadcastRegisteredReply{ isRegistered, generation.committedVersion });
				}
				when(state ConfigBroadcastReadyRequest readyReq = waitNext(cbi->ready.getFuture())) {
					state Optional<CoordinatorsHash> locked = wait(getLocked(self));

					// New ConfigNodes with no previous state should always
					// apply snapshots from the ConfigBroadcaster. Otherwise,
					// the ConfigNode must be part of a new generation to
					// accept a snapshot. An existing ConfigNode that restarts
					// shouldn't apply a snapshot and overwrite its state if
					// the set of coordinators hasn't changed.
					if ((!infinite && !locked.present()) ||
					    (locked.present() && locked.get() != readyReq.coordinatorsHash)) {
						// Apply snapshot if necessary.
						if (readyReq.snapshot.size() > 0) {
							for (const auto& [configKey, knobValue] : readyReq.snapshot) {
								TraceEvent("ConfigNodeSettingFromSnapshot", self->id)
								    .detail("ConfigClass", configKey.configClass)
								    .detail("KnobName", configKey.knobName)
								    .detail("Value", knobValue.toString())
								    .detail("Version", readyReq.snapshotVersion);
								self->kvStore->set(KeyValueRef(
								    BinaryWriter::toValue(configKey, IncludeVersion()).withPrefix(kvKeys.begin),
								    ObjectWriter::toValue(knobValue, IncludeVersion())));
							}
							ConfigGeneration newGeneration = { readyReq.snapshotVersion, readyReq.liveVersion };
							self->kvStore->set(KeyValueRef(currentGenerationKey,
							                               BinaryWriter::toValue(newGeneration, IncludeVersion())));
							// Clear out any mutations to the keys. If these
							// aren't cleared, they will overwrite the
							// snapshotted values when the knobs are read.
							self->kvStore->clear(KeyRangeRef(versionedMutationKey(0, 0),
							                                 versionedMutationKey(readyReq.snapshotVersion + 1, 0)));
							self->kvStore->clear(KeyRangeRef(versionedAnnotationKey(0),
							                                 versionedAnnotationKey(readyReq.snapshotVersion + 1)));

							self->kvStore->set(
							    KeyValueRef(lastCompactedVersionKey,
							                BinaryWriter::toValue(readyReq.snapshotVersion, IncludeVersion())));
						}
						// Make sure freshly up to date ConfigNode isn't
						// locked! This is possible if it was a coordinator in
						// a previous generation.
						self->kvStore->set(KeyValueRef(
						    lockedKey, BinaryWriter::toValue(Optional<CoordinatorsHash>(), IncludeVersion())));
					}
					self->kvStore->set(KeyValueRef(coordinatorsHashKey,
					                               BinaryWriter::toValue(readyReq.coordinatorsHash, IncludeVersion())));
					wait(self->kvStore->commit());

					TraceEvent("ConfigNodeReady", self->id).detail("WasLocked", locked.present());
					readyReq.reply.send(ConfigBroadcastReadyReply{});
					if (!infinite) {
						return Void();
					}
				}
			}
		}
	}

	ACTOR static Future<Void> serveRegistered(ConfigNodeImpl* self, ConfigFollowerInterface const* cfi) {
		loop {
			ConfigFollowerCompactRequest req = waitNext(cfi->compact.getFuture());
			++self->compactRequests;
			wait(compact(self, req));
		}
	}

	// Many of the ConfigNode interfaces need to be served before the
	// ConfigNode is officially registered with the ConfigBroadcaster. This is
	// necessary due to edge cases around coordinator changes. For example, a
	// ConfigNode that loses its coordinator status but then restarts before
	// serving its snapshot to the new coordinators needs to be able to
	// continue serving its snapshot interface when it restarts, even though it
	// is no longer a coordinator.
	ACTOR static Future<Void> serveUnregistered(ConfigNodeImpl* self, ConfigFollowerInterface const* cfi) {
		loop {
			choose {
				when(ConfigFollowerGetSnapshotAndChangesRequest req =
				         waitNext(cfi->getSnapshotAndChanges.getFuture())) {
					++self->snapshotRequests;
					wait(getSnapshotAndChanges(self, req));
				}
				when(ConfigFollowerGetChangesRequest req = waitNext(cfi->getChanges.getFuture())) {
					wait(getChanges(self, req));
				}
				when(ConfigFollowerRollforwardRequest req = waitNext(cfi->rollforward.getFuture())) {
					++self->rollforwardRequests;
					wait(rollforward(self, req));
				}
				when(ConfigFollowerGetCommittedVersionRequest req = waitNext(cfi->getCommittedVersion.getFuture())) {
					++self->getCommittedVersionRequests;
					wait(getCommittedVersion(self, req));
				}
				when(state ConfigFollowerLockRequest req = waitNext(cfi->lock.getFuture())) {
					++self->lockRequests;
					CoordinatorsHash coordinatorsHash = wait(getCoordinatorsHash(self));
					if (coordinatorsHash == 0 || coordinatorsHash == req.coordinatorsHash) {
						TraceEvent("ConfigNodeLocking", self->id).log();
						self->kvStore->set(KeyValueRef(registeredKey, BinaryWriter::toValue(false, IncludeVersion())));
						self->kvStore->set(KeyValueRef(
						    lockedKey,
						    BinaryWriter::toValue(Optional<CoordinatorsHash>(req.coordinatorsHash), IncludeVersion())));
						wait(self->kvStore->commit());
					}
					req.reply.send(Void());
				}
				when(wait(self->kvStore->getError())) { ASSERT(false); }
			}
		}
	}

	ACTOR static Future<Void> serve(ConfigNodeImpl* self,
	                                ConfigBroadcastInterface const* cbi,
	                                ConfigTransactionInterface const* cti,
	                                ConfigFollowerInterface const* cfi) {
		state Future<Void> serveUnregisteredFuture = serveUnregistered(self, cfi);
		wait(serve(self, cbi, false));

		self->kvStore->set(KeyValueRef(registeredKey, BinaryWriter::toValue(true, IncludeVersion())));
		wait(self->kvStore->commit());

		// Shouldn't return (coordinationServer will throw an error if it does).
		wait(serve(self, cbi, true) || serve(self, cti) || serveRegistered(self, cfi) || serveUnregisteredFuture);
		return Void();
	}

	ACTOR static Future<bool> registered(ConfigNodeImpl* self) {
		Optional<Value> value = wait(self->kvStore->readValue(registeredKey));
		return value.present();
	}

public:
	ConfigNodeImpl(std::string const& folder)
	  : id(deterministicRandom()->randomUniqueID()), kvStore(folder, id, "globalconf-"), cc("ConfigNode"),
	    compactRequests("CompactRequests", cc), rollbackRequests("RollbackRequests", cc),
	    rollforwardRequests("RollforwardRequests", cc), successfulChangeRequests("SuccessfulChangeRequests", cc),
	    failedChangeRequests("FailedChangeRequests", cc), snapshotRequests("SnapshotRequests", cc),
	    getCommittedVersionRequests("GetCommittedVersionRequests", cc), lockRequests("LockRequests", cc),
	    successfulCommits("SuccessfulCommits", cc), failedCommits("FailedCommits", cc),
	    setMutations("SetMutations", cc), clearMutations("ClearMutations", cc),
	    getValueRequests("GetValueRequests", cc), getGenerationRequests("GetGenerationRequests", cc) {
		logger = cc.traceCounters("ConfigNodeMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, "ConfigNode");
		TraceEvent(SevInfo, "StartingConfigNode", id).detail("KVStoreAlreadyExists", kvStore.exists());
	}

	Future<Void> serve(ConfigBroadcastInterface const& cbi,
	                   ConfigTransactionInterface const& cti,
	                   ConfigFollowerInterface const& cfi) {
		return serve(this, &cbi, &cti, &cfi);
	}

	Future<Void> serve(ConfigBroadcastInterface const& cbi) { return serve(this, &cbi, true); }

	Future<Void> serve(ConfigTransactionInterface const& cti) { return serve(this, &cti); }

	Future<Void> serve(ConfigFollowerInterface const& cfi) {
		return serveUnregistered(this, &cfi) && serveRegistered(this, &cfi);
	}

	void close() { kvStore.close(); }

	Future<Void> onClosed() { return kvStore.onClosed(); }
};

ConfigNode::ConfigNode(std::string const& folder) : impl(PImpl<ConfigNodeImpl>::create(folder)) {}

ConfigNode::~ConfigNode() = default;

Future<Void> ConfigNode::serve(ConfigBroadcastInterface const& cbi,
                               ConfigTransactionInterface const& cti,
                               ConfigFollowerInterface const& cfi) {
	return impl->serve(cbi, cti, cfi);
}

Future<Void> ConfigNode::serve(ConfigBroadcastInterface const& cbi) {
	return impl->serve(cbi);
}

Future<Void> ConfigNode::serve(ConfigTransactionInterface const& cti) {
	return impl->serve(cti);
}

Future<Void> ConfigNode::serve(ConfigFollowerInterface const& cfi) {
	return impl->serve(cfi);
}

void ConfigNode::close() {
	impl->close();
}

Future<Void> ConfigNode::onClosed() {
	return impl->onClosed();
}
