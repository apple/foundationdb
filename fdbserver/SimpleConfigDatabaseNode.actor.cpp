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

#include "fdbserver/IConfigDatabaseNode.h"
#include "fdbserver/IKeyValueStore.h"
#include "flow/Arena.h"
#include "flow/genericactors.actor.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

const KeyRef liveTransactionVersionKey = LiteralStringRef("liveTransactionVersion");
const KeyRef committedVersionKey = LiteralStringRef("committedVersion");
const KeyRangeRef kvKeys = KeyRangeRef(LiteralStringRef("kv/"), LiteralStringRef("kv0"));
const KeyRangeRef mutationKeys = KeyRangeRef(LiteralStringRef("mutation/"), LiteralStringRef("mutation0"));

// FIXME: negative versions break ordering
Key versionedMutationKey(Version version, int index) {
	ASSERT(version >= 0);
	BinaryWriter bw(IncludeVersion());
	bw << bigEndian64(version);
	bw << bigEndian32(index);
	return bw.toValue().withPrefix(mutationKeys.begin);
}

Version getVersionFromVersionedMutationKey(KeyRef versionedMutationKey) {
	Version result;
	BinaryReader br(versionedMutationKey, IncludeVersion());
	br >> result;
	return result;
}

} //namespace

TEST_CASE("/fdbserver/SimpleConfigDatabaseNode/versionedMutationKeyOrdering") {
	std::vector<Key> keys;
	for (Version version = 0; version < 1000; ++version) {
		for (int index = 0; index < 5; ++index) {
			keys.push_back(versionedMutationKey(version, index));
		}
	}
	for (int index = 0; index < 1000; ++index) {
		keys.push_back(versionedMutationKey(1000, index));
	}
	ASSERT(std::is_sorted(keys.begin(), keys.end()));
	return Void();
}

class SimpleConfigDatabaseNodeImpl {
	IKeyValueStore* kvStore; // FIXME: Prevent leak
	std::map<std::string, std::string> config;
	ActorCollection actors{ false };
	FlowLock globalLock;

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

	ACTOR static Future<Standalone<VectorRef<VersionedMutationRef>>> getMutations(SimpleConfigDatabaseNodeImpl *self, Version startVersion, Version endVersion) {
		Key startVersionKey = versionedMutationKey(startVersion, 0);
		state KeyRangeRef keys(startVersionKey, mutationKeys.end);
		Standalone<RangeResultRef> range = wait(self->kvStore->readRange(keys));
		Standalone<VectorRef<VersionedMutationRef>> result;
		for (const auto &kv : range) {
			auto version = BinaryReader::fromStringRef<Version>(kv.key.removePrefix(mutationKeys.begin), IncludeVersion());
			if (version > endVersion) {
				break;
			}
			auto mutation = BinaryReader::fromStringRef<Standalone<MutationRef>>(kv.value, IncludeVersion());
			result.emplace_back_deep(result.arena(), version, mutation);
		}
		return result;
	}

	ACTOR static Future<Void> getChanges(SimpleConfigDatabaseNodeImpl *self, ConfigFollowerGetChangesRequest req) {
		state Version committedVersion = wait(getCommittedVersion(self));
		Standalone<VectorRef<VersionedMutationRef>> mutations = wait(getMutations(self, req.lastSeenVersion+1, committedVersion));
		req.reply.send(ConfigFollowerGetChangesReply{committedVersion, mutations});
		return Void();
	}

	ACTOR static Future<Void> getCommittedVersion(SimpleConfigDatabaseNodeImpl *self, ConfigFollowerGetVersionRequest req) {
		Version committedVersion = wait(getCommittedVersion(self));
		req.reply.send(ConfigFollowerGetVersionReply(committedVersion));
		return Void();
	}

	ACTOR static Future<Void> getNewVersion(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionGetVersionRequest req) {
		wait(self->globalLock.take());
		state FlowLock::Releaser releaser(self->globalLock);
		state Version currentVersion = wait(getLiveTransactionVersion(self));
		self->kvStore->set(KeyValueRef(liveTransactionVersionKey, BinaryWriter::toValue(++currentVersion, IncludeVersion())));
		wait(self->kvStore->commit());
		req.reply.send(ConfigTransactionGetVersionReply(currentVersion));
		return Void();
	}

	ACTOR static Future<Void> get(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionGetRequest req) {
		wait(self->globalLock.take());
		state FlowLock::Releaser releaser(self->globalLock);
		Version currentVersion = wait(getLiveTransactionVersion(self));
		if (req.version != currentVersion) {
			req.reply.sendError(transaction_too_old());
			return Void();
		}
		state Optional<Value> value = wait(self->kvStore->readValue(req.key.withPrefix(kvKeys.begin)));
		Standalone<VectorRef<VersionedMutationRef>> versionedMutations = wait(getMutations(self, 0, req.version));
		for (const auto &versionedMutation : versionedMutations) {
			const auto &mutation = versionedMutation.mutation;
			if (mutation.type == MutationRef::Type::SetValue) {
				if (mutation.param1 == req.key) {
					value = mutation.param2;
				}
			} else if (mutation.type == MutationRef::Type::ClearRange) {
				if (mutation.param1 <= req.key && req.key <= mutation.param2) {
					value = Optional<Value>{};
				}
			} else {
				ASSERT(false);
			}
		}
		req.reply.send(ConfigTransactionGetReply(value));
		return Void();
	}

	ACTOR static Future<Void> commit(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionCommitRequest req) {
		wait(self->globalLock.take());
		state FlowLock::Releaser releaser(self->globalLock);
		Version currentVersion = wait(getLiveTransactionVersion(self));
		if (req.version != currentVersion) {
			req.reply.sendError(transaction_too_old());
			return Void();
		}
		int index = 0;
		for (const auto &mutation : req.mutations) {
			Key key = versionedMutationKey(req.version, index++);
			Value value = BinaryWriter::toValue(mutation, IncludeVersion());
			self->kvStore->set(KeyValueRef(key, value));
		}
		self->kvStore->set(KeyValueRef(committedVersionKey, BinaryWriter::toValue(req.version, IncludeVersion())));
		wait(self->kvStore->commit());
		req.reply.send(Void());
		return Void();
	}

	ACTOR static Future<Void> traceQueuedMutations(SimpleConfigDatabaseNodeImpl *self) {
		state Version currentVersion = wait(getCommittedVersion(self));
		Standalone<VectorRef<VersionedMutationRef>> versionedMutations = wait(getMutations(self, 0, currentVersion));
		TraceEvent te("SimpleConfigNodeQueuedMutations");
		te.detail("Size", versionedMutations.size());
		te.detail("CommittedVersion", currentVersion);
		int index = 0;
		for (const auto &versionedMutation : versionedMutations) {
			te.detail(format("Version%d", index), versionedMutation.version);
			te.detail(format("Mutation%d", index), versionedMutation.mutation.type);
			te.detail(format("FirstParam%d", index), versionedMutation.mutation.param1);
			te.detail(format("SecondParam%d", index), versionedMutation.mutation.param2);
			++index;
		}
		return Void();
	}

	ACTOR static Future<Void> serve(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionInterface* cti) {
		loop {
			//wait(traceQueuedMutations(self));
			choose {
				when(ConfigTransactionGetVersionRequest req = waitNext(cti->getVersion.getFuture())) {
					self->actors.add(getNewVersion(self, req));
				}
				when(ConfigTransactionGetRequest req = waitNext(cti->get.getFuture())) {
					self->actors.add(get(self, req));
				}
				when(ConfigTransactionCommitRequest req = waitNext(cti->commit.getFuture())) {
					self->actors.add(commit(self, req));
				}
				when(wait(self->actors.getResult())) { ASSERT(false); }
			}
		}
	}

	ACTOR static Future<Void> getFullDatabase(SimpleConfigDatabaseNodeImpl* self,
	                                          ConfigFollowerGetFullDatabaseRequest req) {
		state ConfigFollowerGetFullDatabaseReply reply;
		Standalone<RangeResultRef> data = wait(self->kvStore->readRange(kvKeys));
		for (const auto& kv : data) {
			reply.database[kv.key] = kv.value;
		}
		Standalone<VectorRef<VersionedMutationRef>> versionedMutations =
		    wait(getMutations(self, 0, req.version));
		for (const auto& versionedMutation : versionedMutations) {
			const auto& mutation = versionedMutation.mutation;
			if (mutation.type == MutationRef::SetValue) {
				reply.database[mutation.param1] = mutation.param2;
			} else if (mutation.type == MutationRef::ClearRange) {
				reply.database.erase(reply.database.find(mutation.param1), reply.database.find(mutation.param2));
			}
		}
		req.reply.send(reply);
		return Void();
	}

	ACTOR static Future<Void> serve(SimpleConfigDatabaseNodeImpl* self, ConfigFollowerInterface* cfi) {
		loop {
			choose {
				when(ConfigFollowerGetVersionRequest req = waitNext(cfi->getVersion.getFuture())) {
					self->actors.add(getCommittedVersion(self, req));
				}
				when(ConfigFollowerGetFullDatabaseRequest req = waitNext(cfi->getFullDatabase.getFuture())) {
					self->actors.add(getFullDatabase(self, req));
				}
				when(ConfigFollowerGetChangesRequest req = waitNext(cfi->getChanges.getFuture())) {
					self->actors.add(getChanges(self, req));
				}
				when(ConfigFollowerCompactRequest req = waitNext(cfi->compact.getFuture())) {
					// TODO: Implement
					req.reply.send(Void());
				}
				when(wait(self->actors.getResult())) { ASSERT(false); }
			}
		}
	}

public:
	SimpleConfigDatabaseNodeImpl(std::string const& dataFolder) {
		platform::createDirectory(dataFolder);
		kvStore = keyValueStoreMemory(joinPath(dataFolder, "globalconf-"), UID{}, 500e6);
	}

	Future<Void> serve(ConfigTransactionInterface& cti) { return serve(this, &cti); }

	Future<Void> serve(ConfigFollowerInterface& cfi) { return serve(this, &cfi); }
};

SimpleConfigDatabaseNode::SimpleConfigDatabaseNode(std::string const& dataFolder)
  : impl(std::make_unique<SimpleConfigDatabaseNodeImpl>(dataFolder)) {}

SimpleConfigDatabaseNode::~SimpleConfigDatabaseNode() = default;

Future<Void> SimpleConfigDatabaseNode::serve(ConfigTransactionInterface& cti) {
	return impl->serve(cti);
}

Future<Void> SimpleConfigDatabaseNode::serve(ConfigFollowerInterface& cfi) {
	return impl->serve(cfi);
}
