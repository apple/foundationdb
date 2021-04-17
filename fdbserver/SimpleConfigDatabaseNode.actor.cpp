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

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

const KeyRef versionKey = LiteralStringRef("version");
const KeyRangeRef kvKeys = KeyRangeRef(LiteralStringRef("kv/"), LiteralStringRef("kv0"));
const KeyRangeRef mutationKeys = KeyRangeRef(LiteralStringRef("mutation/"), LiteralStringRef("mutation0"));

struct VersionedMutationRef {
	Version version;
	MutationRef mutation;

	VersionedMutationRef()=default; // FIXME Undefined memory?
	explicit VersionedMutationRef(Arena &arena, Version version, MutationRef mutation) : version(version), mutation(arena, mutation) {}
};

Key versionedMutationKey(Version version, int index) {
	BinaryWriter bw(IncludeVersion());
	bw << version;
	bw << index;
	return bw.toValue().withPrefix(mutationKeys.begin);
}

} //namespace

class SimpleConfigDatabaseNodeImpl {
	IKeyValueStore* kvStore; // FIXME: Prevent leak
	std::map<std::string, std::string> config;
	ActorCollection actors{ false };
	FlowLock globalLock;

	ACTOR static Future<Version> getCurrentVersion(SimpleConfigDatabaseNodeImpl *self) {
		Optional<Value> value = wait(self->kvStore->readValue(versionKey));
		state Version version = 0;
		if (value.present()) {
			BinaryReader br(value.get(), IncludeVersion());
			br >> version;
		} else {
			BinaryWriter bw(IncludeVersion());
			bw << version;
			self->kvStore->set(KeyValueRef(versionKey, bw.toValue()));
			wait(self->kvStore->commit());
		}
		return version;
	}

	ACTOR static Future<Standalone<VectorRef<VersionedMutationRef>>> getMutations(SimpleConfigDatabaseNodeImpl *self, int startVersion, int endVersion) {
		Value serializedStartVersion = BinaryWriter::toValue(startVersion, IncludeVersion());
		KeyRangeRef keys(serializedStartVersion.withPrefix(mutationKeys.begin), mutationKeys.end);
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

	ACTOR static Future<Void> getCurrentVersion(SimpleConfigDatabaseNodeImpl *self, ConfigFollowerGetVersionRequest req) {
		Version version = wait(getCurrentVersion(self));
		req.reply.send(ConfigFollowerGetVersionReply(version));
		return Void();
	}

	ACTOR static Future<Void> getNewVersion(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionGetVersionRequest req) {
		wait(self->globalLock.take());
		state FlowLock::Releaser releaser(self->globalLock);
		state Version currentVersion = wait(getCurrentVersion(self));
		self->kvStore->set(KeyValueRef(versionKey, BinaryWriter::toValue(++currentVersion, IncludeVersion())));
		wait(self->kvStore->commit());
		req.reply.send(ConfigTransactionGetVersionReply(currentVersion));
		return Void();
	}

	ACTOR static Future<Void> get(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionGetRequest req) {
		wait(self->globalLock.take());
		state FlowLock::Releaser releaser(self->globalLock);
		Version currentVersion = wait(getCurrentVersion(self));
		if (req.version != currentVersion) {
			req.reply.sendError(transaction_too_old());
			return Void();
		}
		state Optional<Value> value = wait(self->kvStore->readValue(req.key.withPrefix(kvKeys.begin)));
		Standalone<VectorRef<VersionedMutationRef>> versionedMutations = wait(getMutations(self, -1, req.version));
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
		Version currentVersion = wait(getCurrentVersion(self));
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
		wait(self->kvStore->commit());
		req.reply.send(Void());
		return Void();
	}

	ACTOR static Future<Void> serve(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionInterface* cti) {
		loop {
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

	ACTOR static Future<Void> serve(SimpleConfigDatabaseNodeImpl* self, ConfigFollowerInterface* cfi) {
		loop {
			choose {
				when(ConfigFollowerGetVersionRequest req = waitNext(cfi->getVersion.getFuture())) {
					self->actors.add(getCurrentVersion(self, req));
				}
				when(ConfigFollowerGetFullDatabaseRequest req = waitNext(cfi->getFullDatabase.getFuture())) {
					// TODO: Implement
					continue;
				}
				when(ConfigFollowerGetChangesRequest req = waitNext(cfi->getChanges.getFuture())) {
					// TODO: Implement
					continue;
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
