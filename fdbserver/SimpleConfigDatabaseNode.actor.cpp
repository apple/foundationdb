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
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

static const KeyRef versionKey = LiteralStringRef("version");
static const KeyRangeRef configKeys = KeyRangeRef(LiteralStringRef("config/"), LiteralStringRef("config0"));

class SimpleConfigDatabaseNodeImpl {
	IKeyValueStore* kvStore; // FIXME: Prevent leak
	std::map<std::string, std::string> config;
	Version currentVersion;
	ActorCollection actors{ false };
	FlowLock globalLock;

	ACTOR static Future<Void> getVersion(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionGetVersionRequest req) {
		wait(self->globalLock.take());
		state FlowLock::Releaser releaser(self->globalLock);
		++self->currentVersion;
		BinaryWriter bw(IncludeVersion());
		bw << self->currentVersion;
		self->kvStore->set(KeyValueRef(versionKey, bw.toValue()));
		wait(self->kvStore->commit());
		req.reply.send(self->currentVersion);
		return Void();
	}

	ACTOR static Future<Void> get(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionGetRequest req) {
		wait(self->globalLock.take());
		state FlowLock::Releaser releaser(self->globalLock);
		if (req.version != self->currentVersion) {
			req.reply.sendError(transaction_too_old());
			return Void();
		}
		auto it = self->config.find(req.key.toString());
		if (it == self->config.end()) {
			req.reply.send(ConfigTransactionGetReply());
		} else {
			req.reply.send(ConfigTransactionGetReply(Value(it->second)));
		}
		return Void();
	}

	ACTOR static Future<Void> commit(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionCommitRequest req) {
		wait(self->globalLock.take());
		state FlowLock::Releaser releaser(self->globalLock);
		if (req.version != self->currentVersion) {
			req.reply.sendError(transaction_too_old());
			return Void();
		}
		state int index = 0;
		for (; index < req.mutations.size(); ++index) {
			const auto& mutation = req.mutations[index];
			if (mutation.type == MutationRef::SetValue) {
				self->config[mutation.param1.toString()] = mutation.param2.toString();
				self->kvStore->set(KeyValueRef(mutation.param1, mutation.param2));
			} else if (mutation.type == MutationRef::ClearRange) {
				self->config.erase(self->config.find(mutation.param1.toString()),
				                   self->config.find(mutation.param2.toString()));
				self->kvStore->clear(KeyRangeRef(mutation.param1, mutation.param2));
			} else {
				ASSERT(false);
			}
			++index;
		}
		wait(self->kvStore->commit());
		req.reply.send(Void());
		return Void();
	}

	ACTOR static Future<Void> readKVStoreIntoMemory(SimpleConfigDatabaseNodeImpl* self) {
		wait(self->kvStore->init());
		state Optional<Value> onDiskVersion = wait(self->kvStore->readValue(versionKey));
		if (!onDiskVersion.present()) {
			// Brand new database
			self->currentVersion = 0;
			BinaryWriter wr(IncludeVersion());
			wr << self->currentVersion;
			self->kvStore->set(KeyValueRef(versionKey, wr.toValue()));
			wait(self->kvStore->commit());
			return Void();
		}
		BinaryReader br(onDiskVersion.get(), IncludeVersion());
		br >> self->currentVersion;
		Standalone<RangeResultRef> data = wait(self->kvStore->readRange(configKeys));
		ASSERT(!data.more); // TODO: Support larger amounts of data?
		for (auto const& kv : data) {
			self->config[kv.key.toString()] = kv.value.toString();
		}
		return Void();
	}

	ACTOR static Future<Void> serve(SimpleConfigDatabaseNodeImpl* self, ConfigTransactionInterface* cti) {
		wait(readKVStoreIntoMemory(self));
		loop {
			choose {
				when(ConfigTransactionGetVersionRequest req = waitNext(cti->getVersion.getFuture())) {
					self->actors.add(getVersion(self, req));
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
		// TODO: Implement
		TraceEvent("HERE");
		wait(Never());
		return Void();
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
