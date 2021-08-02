/*
 * ConfigNode.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

const KeyRef lastCompactedVersionKey = "lastCompactedVersion"_sr;
const KeyRef currentGenerationKey = "currentGeneration"_sr;
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
	OnDemandStore kvStore;
	CounterCollection cc;

	// Follower counters
	Counter compactRequests;
	Counter successfulChangeRequests;
	Counter failedChangeRequests;
	Counter snapshotRequests;
	Counter getCommittedVersionRequests;

	// Transaction counters
	Counter successfulCommits;
	Counter failedCommits;
	Counter setMutations;
	Counter clearMutations;
	Counter getValueRequests;
	Counter newVersionRequests;
	Future<Void> logger;

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
		Version lastCompactedVersion = wait(getLastCompactedVersion(self));
		if (req.lastSeenVersion < lastCompactedVersion) {
			++self->failedChangeRequests;
			req.reply.sendError(version_already_compacted());
			return Void();
		}
		state Version committedVersion =
		    wait(map(getGeneration(self), [](auto const& gen) { return gen.committedVersion; }));
		state Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations =
		    wait(getMutations(self, req.lastSeenVersion + 1, committedVersion));
		state Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> versionedAnnotations =
		    wait(getAnnotations(self, req.lastSeenVersion + 1, committedVersion));
		TraceEvent(SevDebug, "ConfigDatabaseNodeSendingChanges", self->id)
		    .detail("ReqLastSeenVersion", req.lastSeenVersion)
		    .detail("CommittedVersion", committedVersion)
		    .detail("NumMutations", versionedMutations.size())
		    .detail("NumCommits", versionedAnnotations.size());
		++self->successfulChangeRequests;
		req.reply.send(ConfigFollowerGetChangesReply{ committedVersion, versionedMutations, versionedAnnotations });
		return Void();
	}

	// New transactions increment the database's current live version. This effectively serves as a lock, providing
	// serializability
	ACTOR static Future<Void> getNewGeneration(ConfigNodeImpl* self, ConfigTransactionGetGenerationRequest req) {
		state ConfigGeneration generation = wait(getGeneration(self));
		++generation.liveVersion;
		self->kvStore->set(KeyValueRef(currentGenerationKey, BinaryWriter::toValue(generation, IncludeVersion())));
		wait(self->kvStore->commit());
		req.reply.send(ConfigTransactionGetGenerationReply{ generation });
		return Void();
	}

	ACTOR static Future<Void> get(ConfigNodeImpl* self, ConfigTransactionGetRequest req) {
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

	ACTOR static Future<Void> commit(ConfigNodeImpl* self, ConfigTransactionCommitRequest req) {
		ConfigGeneration currentGeneration = wait(getGeneration(self));
		if (req.generation != currentGeneration) {
			++self->failedCommits;
			req.reply.sendError(transaction_too_old());
			return Void();
		}
		int index = 0;
		for (const auto& mutation : req.mutations) {
			Key key = versionedMutationKey(req.generation.liveVersion, index++);
			Value value = ObjectWriter::toValue(mutation, IncludeVersion());
			if (mutation.isSet()) {
				TraceEvent("ConfigNodeSetting")
				    .detail("ConfigClass", mutation.getConfigClass())
				    .detail("KnobName", mutation.getKnobName())
				    .detail("Value", mutation.getValue().toString())
				    .detail("Version", req.generation.liveVersion);
				++self->setMutations;
			} else {
				++self->clearMutations;
			}
			self->kvStore->set(KeyValueRef(key, value));
		}
		self->kvStore->set(KeyValueRef(versionedAnnotationKey(req.generation.liveVersion),
		                               BinaryWriter::toValue(req.annotation, IncludeVersion())));
		ConfigGeneration newGeneration = { req.generation.liveVersion, req.generation.liveVersion };
		self->kvStore->set(KeyValueRef(currentGenerationKey, BinaryWriter::toValue(newGeneration, IncludeVersion())));
		wait(self->kvStore->commit());
		++self->successfulCommits;
		req.reply.send(Void());
		return Void();
	}

	ACTOR static Future<Void> serve(ConfigNodeImpl* self, ConfigTransactionInterface const* cti) {
		loop {
			choose {
				when(ConfigTransactionGetGenerationRequest req = waitNext(cti->getGeneration.getFuture())) {
					++self->newVersionRequests;
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
		wait(store(reply.changes, getMutations(self, reply.snapshotVersion + 1, req.mostRecentVersion)));
		wait(store(reply.annotations, getAnnotations(self, reply.snapshotVersion + 1, req.mostRecentVersion)));
		TraceEvent(SevDebug, "ConfigDatabaseNodeGettingSnapshot", self->id)
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
		TraceEvent(SevDebug, "ConfigDatabaseNodeCompacting", self->id)
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
				TraceEvent(SevDebug, "ConfigDatabaseNodeCompactionApplyingMutation", self->id)
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

	ACTOR static Future<Void> getCommittedVersion(ConfigNodeImpl* self, ConfigFollowerGetCommittedVersionRequest req) {
		ConfigGeneration generation = wait(getGeneration(self));
		req.reply.send(ConfigFollowerGetCommittedVersionReply{ generation.committedVersion });
		return Void();
	}

	ACTOR static Future<Void> serve(ConfigNodeImpl* self, ConfigFollowerInterface const* cfi) {
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
				when(ConfigFollowerCompactRequest req = waitNext(cfi->compact.getFuture())) {
					++self->compactRequests;
					wait(compact(self, req));
				}
				when(ConfigFollowerGetCommittedVersionRequest req = waitNext(cfi->getCommittedVersion.getFuture())) {
					++self->getCommittedVersionRequests;
					wait(getCommittedVersion(self, req));
				}
				when(wait(self->kvStore->getError())) { ASSERT(false); }
			}
		}
	}

public:
	ConfigNodeImpl(std::string const& folder)
	  : id(deterministicRandom()->randomUniqueID()), kvStore(folder, id, "globalconf-"), cc("ConfigDatabaseNode"),
	    compactRequests("CompactRequests", cc), successfulChangeRequests("SuccessfulChangeRequests", cc),
	    failedChangeRequests("FailedChangeRequests", cc), snapshotRequests("SnapshotRequests", cc),
	    getCommittedVersionRequests("GetCommittedVersionRequests", cc), successfulCommits("SuccessfulCommits", cc),
	    failedCommits("FailedCommits", cc), setMutations("SetMutations", cc), clearMutations("ClearMutations", cc),
	    getValueRequests("GetValueRequests", cc), newVersionRequests("NewVersionRequests", cc) {
		logger = traceCounters(
		    "ConfigDatabaseNodeMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "ConfigDatabaseNode");
		TraceEvent(SevDebug, "StartingConfigNode", id).detail("KVStoreAlreadyExists", kvStore.exists());
	}

	Future<Void> serve(ConfigTransactionInterface const& cti) { return serve(this, &cti); }

	Future<Void> serve(ConfigFollowerInterface const& cfi) { return serve(this, &cfi); }
};

ConfigNode::ConfigNode(std::string const& folder) : impl(PImpl<ConfigNodeImpl>::create(folder)) {}

ConfigNode::~ConfigNode() = default;

Future<Void> ConfigNode::serve(ConfigTransactionInterface const& cti) {
	return impl->serve(cti);
}

Future<Void> ConfigNode::serve(ConfigFollowerInterface const& cfi) {
	return impl->serve(cfi);
}
