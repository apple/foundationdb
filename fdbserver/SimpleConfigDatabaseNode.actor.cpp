/*
 * SimpleConfigDatabaseNode.actor.cpp
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

#include "fdbserver/SimpleConfigDatabaseNode.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/OnDemandStore.h"
#include "flow/Arena.h"
#include "flow/genericactors.actor.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

const KeyRef lastCompactedVersionKey = "lastCompactedVersion"_sr;
const KeyRef liveTransactionVersionKey = "liveTransactionVersion"_sr;
const KeyRef committedVersionKey = "committedVersion"_sr;
const KeyRangeRef kvKeys = KeyRangeRef("kv/"_sr, "kv0"_sr);
const KeyRangeRef mutationKeys = KeyRangeRef("mutation/"_sr, "mutation0"_sr);

Key versionedMutationKey(Version version, uint32_t index) {
	ASSERT(version >= 0);
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

} //namespace

TEST_CASE("/fdbserver/ConfigDB/SimpleConfigDatabaseNode/Internal/versionedMutationKeys") {
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

TEST_CASE("/fdbserver/ConfigDB/SimpleConfigDatabaseNode/Internal/versionedMutationKeyOrdering") {
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

class SimpleConfigDatabaseNodeImpl {
	UID id;
	// TODO: Listen for errors
	OnDemandStore kvStore;
	std::map<std::string, std::string> config;

	CounterCollection cc;

	// Follower counters
	Counter compactRequests;
	Counter successfulChangeRequests;
	Counter failedChangeRequests;
	Counter snapshotRequests;

	// Transaction counters
	Counter successfulCommits;
	Counter failedCommits;
	Counter setMutations;
	Counter clearMutations;
	Counter getValueRequests;
	Counter newVersionRequests;
	Future<Void> logger;

	ACTOR static Future<Version> getLiveTransactionVersion(SimpleConfigDatabaseNodeImpl *self) {
		Optional<Value> value = wait(self->kvStore->readValue(liveTransactionVersionKey));
		state Version liveTransactionVersion = 0;
		if (value.present()) {
			liveTransactionVersion = BinaryReader::fromStringRef<Version>(value.get(), IncludeVersion());
		} else {
			self->kvStore->set(KeyValueRef(liveTransactionVersionKey, BinaryWriter::toValue(liveTransactionVersion, IncludeVersion())));
			wait(self->kvStore->commit());
		}
		return liveTransactionVersion;
	}

	ACTOR static Future<Version> getCommittedVersion(SimpleConfigDatabaseNodeImpl *self) {
		Optional<Value> value = wait(self->kvStore->readValue(committedVersionKey));
		state Version committedVersion = 0;
		if (value.present()) {
			committedVersion = BinaryReader::fromStringRef<Version>(value.get(), IncludeVersion());
		} else {
			self->kvStore->set(KeyValueRef(committedVersionKey, BinaryWriter::toValue(committedVersion, IncludeVersion())));
			wait(self->kvStore->commit());
		}
		return committedVersion;
	}

	ACTOR static Future<Version> getLastCompactedVersion(SimpleConfigDatabaseNodeImpl* self) {
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

	ACTOR static Future<Standalone<VectorRef<VersionedConfigMutationRef>>>
	getMutations(SimpleConfigDatabaseNodeImpl* self, Version startVersion, Version endVersion) {
		Key startVersionKey = versionedMutationKey(startVersion, 0);
		state KeyRangeRef keys(startVersionKey, mutationKeys.end);
		Standalone<RangeResultRef> range = wait(self->kvStore->readRange(keys));
		Standalone<VectorRef<VersionedConfigMutationRef>> result;
		for (const auto &kv : range) {
			auto version = getVersionFromVersionedMutationKey(kv.key);
			if (version > endVersion) {
				break;
			}
			auto mutation = BinaryReader::fromStringRef<Standalone<ConfigMutationRef>>(kv.value, IncludeVersion());
			result.emplace_back_deep(result.arena(), version, mutation);
		}
		return result;
	}

	ACTOR static Future<Void> getChanges(SimpleConfigDatabaseNodeImpl *self, ConfigFollowerGetChangesRequest req) {
		Version lastCompactedVersion = wait(getLastCompactedVersion(self));
		if (req.lastSeenVersion < lastCompactedVersion) {
			++self->failedChangeRequests;
			req.reply.sendError(version_already_compacted());
			return Void();
		}
		state Version committedVersion = wait(getCommittedVersion(self));
		Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations =
		    wait(getMutations(self, req.lastSeenVersion + 1, committedVersion));
		TraceEvent(SevDebug, "ConfigDatabaseNodeSendingChanges")
		    .detail("ReqLastSeenVersion", req.lastSeenVersion)
		    .detail("CommittedVersion", committedVersion)
		    .detail("NumMutations", versionedMutations.size());
		++self->successfulChangeRequests;
		req.reply.send(ConfigFollowerGetChangesReply{ committedVersion, versionedMutations });
		return Void();
	}

	ACTOR static Future<Void> getNewVersion(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionGetVersionRequest req) {
		state Version currentVersion = wait(getLiveTransactionVersion(self));
		self->kvStore->set(KeyValueRef(liveTransactionVersionKey, BinaryWriter::toValue(++currentVersion, IncludeVersion())));
		wait(self->kvStore->commit());
		req.reply.send(ConfigTransactionGetVersionReply(currentVersion));
		return Void();
	}

	ACTOR static Future<Void> get(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionGetRequest req) {
		Version currentVersion = wait(getLiveTransactionVersion(self));
		if (req.version != currentVersion) {
			req.reply.sendError(transaction_too_old());
			return Void();
		}
		state Optional<Value> value =
		    wait(self->kvStore->readValue(BinaryWriter::toValue(req.key, IncludeVersion()).withPrefix(kvKeys.begin)));
		Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations = wait(getMutations(self, 0, req.version));
		for (const auto &versionedMutation : versionedMutations) {
			const auto &mutation = versionedMutation.mutation;
			if (mutation.getKey() == req.key) {
				value = mutation.getValue().castTo<Value>();
			}
		}
		req.reply.send(ConfigTransactionGetReply(value));
		return Void();
	}

	/*
	ACTOR static Future<Void> getRange(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionGetRangeRequest req) {
	    wait(self->globalLock.take());
	    state FlowLock::Releaser releaser(self->globalLock);
	    Version currentVersion = wait(getLiveTransactionVersion(self));
	    if (req.version != currentVersion) {
	        req.reply.sendError(transaction_too_old());
	        return Void();
	    }
	    state Standalone<RangeResultRef> range = wait(self->kvStore->readRange(req.keys.withPrefix(kvKeys.begin)));
	    // FIXME: Inefficient
	    for (auto& kv : range) {
	        ASSERT(kv.key.startsWith(kvKeys.begin));
	        kv.key = kv.key.removePrefix(kvKeys.begin);
	    }
	    Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations = wait(getMutations(self, 0, req.version));
	    for (const auto& versionedMutation : versionedMutations) {
	        const auto& mutation = versionedMutation.mutation;
	        if (mutation.isSet()) {
	            // FIXME: This is very inefficient
	            Standalone<RangeResultRef> newRange;
	            bool added = false;
	            for (auto& kv : range) {
	                if (kv.key > mutation.param1 && !added) {
	                    newRange.push_back_deep(newRange.arena(), KeyValueRef(mutation.param1, mutation.param2));
	                    added = true;
	                } else if (kv.key == mutation.param1) {
	                    kv.value = mutation.param2;
	                    added = true;
	                }
	                newRange.push_back_deep(newRange.arena(), kv);
	            }
	            if (!added) {
	                newRange.push_back_deep(newRange.arena(), KeyValueRef(mutation.param1, mutation.param2));
	            }
	            range = std::move(newRange);
	        } else {
	            // FIXME: This is very inefficient
	            Standalone<RangeResultRef> newRange;
	            for (const auto& kv : range) {
	                if (kv.key == mutation.getKey()) {
	                    newRange.push_back_deep(newRange.arena(), kv);
	                }
	            }
	            range = std::move(newRange);
	        }
	    }
	    req.reply.send(ConfigTransactionGetRangeReply(range));
	    return Void();
	}
	*/

	ACTOR static Future<Void> commit(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionCommitRequest req) {
		Version currentVersion = wait(getLiveTransactionVersion(self));
		if (req.version != currentVersion) {
			++self->failedCommits;
			req.reply.sendError(transaction_too_old());
			return Void();
		}
		int index = 0;
		for (const auto &mutation : req.mutations) {
			Key key = versionedMutationKey(req.version, index++);
			Value value = BinaryWriter::toValue(mutation, IncludeVersion());
			if (mutation.isSet()) {
				++self->setMutations;
			} else {
				++self->clearMutations;
			}
			self->kvStore->set(KeyValueRef(key, value));
		}
		self->kvStore->set(KeyValueRef(committedVersionKey, BinaryWriter::toValue(req.version, IncludeVersion())));
		wait(self->kvStore->commit());
		++self->successfulCommits;
		req.reply.send(Void());
		return Void();
	}

	ACTOR static Future<Void> serve(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionInterface const* cti) {
		loop {
			//wait(traceQueuedMutations(self));
			choose {
				when(ConfigTransactionGetVersionRequest req = waitNext(cti->getVersion.getFuture())) {
					++self->newVersionRequests;
					wait(getNewVersion(self, req));
				}
				when(ConfigTransactionGetRequest req = waitNext(cti->get.getFuture())) {
					++self->getValueRequests;
					wait(get(self, req));
				}
				when(ConfigTransactionCommitRequest req = waitNext(cti->commit.getFuture())) {
					wait(commit(self, req));
				}
				when(ConfigTransactionGetRangeRequest req = waitNext(cti->getRange.getFuture())) {
					// FIXME: Fix and reenable
					// wait(getRange(self, req));
				}
			}
		}
	}

	ACTOR static Future<Void> getSnapshotAndChanges(SimpleConfigDatabaseNodeImpl* self,
	                                                ConfigFollowerGetSnapshotAndChangesRequest req) {
		state ConfigFollowerGetSnapshotAndChangesReply reply;
		Standalone<RangeResultRef> data = wait(self->kvStore->readRange(kvKeys));
		for (const auto& kv : data) {
			reply
			    .snapshot[BinaryReader::fromStringRef<ConfigKey>(kv.key.removePrefix(kvKeys.begin), IncludeVersion())] =
			    kv.value;
		}
		wait(store(reply.snapshotVersion, getLastCompactedVersion(self)));
		wait(store(reply.changesVersion, getCommittedVersion(self)));
		wait(store(reply.changes, getMutations(self, reply.snapshotVersion + 1, reply.changesVersion)));
		TraceEvent(SevDebug, "ConfigDatabaseNodeGettingSnapshot")
		    .detail("SnapshotVersion", reply.snapshotVersion)
		    .detail("ChangesVersion", reply.changesVersion)
		    .detail("SnapshotSize", reply.snapshot.size())
		    .detail("ChangesSize", reply.changes.size());
		req.reply.send(reply);
		return Void();
	}

	ACTOR static Future<Void> compact(SimpleConfigDatabaseNodeImpl* self, ConfigFollowerCompactRequest req) {
		state Version lastCompactedVersion = wait(getLastCompactedVersion(self));
		TraceEvent(SevDebug, "ConfigDatabaseNodeCompacting")
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
		for (const auto& versionedMutation : versionedMutations) {
			const auto& version = versionedMutation.version;
			const auto& mutation = versionedMutation.mutation;
			if (version > req.version) {
				break;
			} else {
				TraceEvent(SevDebug, "ConfigDatabaseNodeCompactionApplyingMutation")
				    .detail("IsSet", mutation.isSet())
				    .detail("MutationVersion", version)
				    .detail("LastCompactedVersion", lastCompactedVersion)
				    .detail("ReqVersion", req.version);
				auto serializedKey = BinaryWriter::toValue(mutation.getKey(), IncludeVersion());
				if (mutation.isSet()) {
					self->kvStore->set(KeyValueRef(serializedKey.withPrefix(kvKeys.begin), mutation.getValue().get()));
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

	ACTOR static Future<Void> serve(SimpleConfigDatabaseNodeImpl* self, ConfigFollowerInterface const* cfi) {
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
			}
		}
	}

public:
	SimpleConfigDatabaseNodeImpl(std::string const& folder, Optional<UID> testID)
	  : id(testID.present() ? testID.get() : deterministicRandom()->randomUniqueID()),
	    kvStore(folder, id, "globalconf-" + (testID.present() ? id.toString() : "")), cc("ConfigDatabaseNode"),
	    compactRequests("CompactRequests", cc), successfulChangeRequests("SuccessfulChangeRequests", cc),
	    failedChangeRequests("FailedChangeRequests", cc), snapshotRequests("SnapshotRequests", cc),
	    successfulCommits("SuccessfulCommits", cc), failedCommits("FailedCommits", cc),
	    setMutations("SetMutations", cc), clearMutations("ClearMutations", cc),
	    getValueRequests("GetValueRequests", cc), newVersionRequests("NewVersionRequests", cc) {
		logger = traceCounters(
		    "ConfigDatabaseNodeMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "ConfigDatabaseNode");
	}

	Future<Void> serve(ConfigTransactionInterface const& cti) { return serve(this, &cti); }

	Future<Void> serve(ConfigFollowerInterface const& cfi) { return serve(this, &cfi); }
};

SimpleConfigDatabaseNode::SimpleConfigDatabaseNode(std::string const& folder, Optional<UID> testID)
  : impl(std::make_unique<SimpleConfigDatabaseNodeImpl>(folder, testID)) {}

SimpleConfigDatabaseNode::~SimpleConfigDatabaseNode() = default;

Future<Void> SimpleConfigDatabaseNode::serve(ConfigTransactionInterface const& cti) {
	return impl->serve(cti);
}

Future<Void> SimpleConfigDatabaseNode::serve(ConfigFollowerInterface const& cfi) {
	return impl->serve(cfi);
}
