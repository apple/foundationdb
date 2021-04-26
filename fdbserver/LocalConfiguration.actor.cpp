/*
 * LocalConfiguration.actor.cpp
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

#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/LocalConfiguration.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

const KeyRef configClassesKey = "configClasses"_sr;
const KeyRef lastSeenVersionKey = "lastSeenVersion"_sr;
const KeyRangeRef knobOverrideKeys = KeyRangeRef("knobOverride/"_sr, "knobOverride0"_sr);

} // namespace

class LocalConfigurationImpl {
	IKeyValueStore* kvStore; // FIXME: fix leaks?
	ConfigClassSet configClasses;
	Version lastSeenVersion { 0 };
	Future<Void> initFuture;
	TestKnobs knobs;
	std::map<Key, Value> manuallyOverriddenKnobs;
	std::map<Key, Value> configDatabaseOverriddenKnobs;
	TestKnobs defaultKnobs;

	ACTOR static Future<Void> saveConfigClasses(LocalConfigurationImpl* self) {
		self->kvStore->set(KeyValueRef(configClassesKey, BinaryWriter::toValue(self->configClasses, IncludeVersion())));
		wait(self->kvStore->commit());
		return Void();
	}

	ACTOR static Future<Void> clearKVStore(LocalConfigurationImpl *self) {
		self->kvStore->clear(singleKeyRange(configClassesKey));
		self->kvStore->clear(knobOverrideKeys);
		wait(self->kvStore->commit());
		return Void();
	}

	ACTOR static Future<Void> getLastSeenVersion(LocalConfigurationImpl *self) {
		state Optional<Value> lastSeenVersionValue = wait(self->kvStore->readValue(lastSeenVersionKey));
		if (!lastSeenVersionValue.present()) {
			self->lastSeenVersion = 0;
			self->kvStore->set(KeyValueRef(lastSeenVersionKey, BinaryWriter::toValue(self->lastSeenVersion, IncludeVersion())));
			wait(self->kvStore->commit());
			return Void();
		}
		self->lastSeenVersion = BinaryReader::fromStringRef<Version>(lastSeenVersionValue.get(), IncludeVersion());
		return Void();
	}

	ACTOR static Future<Void> init(LocalConfigurationImpl* self) {
		self->defaultKnobs.initialize();
		wait(self->kvStore->init());
		wait(getLastSeenVersion(self));
		state Optional<Value> storedConfigClassesValue = wait(self->kvStore->readValue(configClassesKey));
		if (!storedConfigClassesValue.present()) {
			wait(saveConfigClasses(self));
			self->updateInMemoryKnobs();
			return Void();
		}
		state ConfigClassSet storedConfigClasses = BinaryReader::fromStringRef<ConfigClassSet>(storedConfigClassesValue.get(), IncludeVersion());
		if (storedConfigClasses != self->configClasses) {
			// All local information is outdated
			wait(clearKVStore(self));
			wait(saveConfigClasses(self));
			self->updateInMemoryKnobs();
			return Void();
		}
		Standalone<RangeResultRef> range = wait(self->kvStore->readRange(knobOverrideKeys));
		for (const auto &kv : range) {
			self->configDatabaseOverriddenKnobs[kv.key] = kv.value;
		}
		self->updateInMemoryKnobs();
		return Void();
	}

	void updateInMemoryKnobs() {
		knobs = defaultKnobs;
		for (const auto& [knobName, knobValue] : configDatabaseOverriddenKnobs) {
			// TODO: Fail gracefully
			ASSERT(knobs.setKnob(knobName.toString(), knobValue.toString()));
		}
		for (const auto& [knobName, knobValue] : manuallyOverriddenKnobs) {
			// TODO: Fail gracefully
			ASSERT(knobs.setKnob(knobName.toString(), knobValue.toString()));
		}
		// Must reinitialize in order to update dependent knobs
		knobs.initialize();
	}

	ACTOR static Future<Void> applyKnobUpdates(LocalConfigurationImpl *self, ConfigFollowerGetFullDatabaseReply reply) {
		self->kvStore->clear(knobOverrideKeys);
		for (const auto &[k, v] : reply.database) {
			self->kvStore->set(KeyValueRef(k.withPrefix(knobOverrideKeys.begin), v));
			self->configDatabaseOverriddenKnobs[k] = v;
		}
		self->kvStore->set(KeyValueRef(lastSeenVersionKey, BinaryWriter::toValue(self->lastSeenVersion, IncludeVersion())));
		wait(self->kvStore->commit());
		self->updateInMemoryKnobs();
		return Void();
	}

	ACTOR static Future<Void> applyKnobUpdates(LocalConfigurationImpl *self, ConfigFollowerGetChangesReply reply) {
		for (const auto &versionedMutation : reply.versionedMutations) {
			const auto &mutation = versionedMutation.mutation;
			if (mutation.type == MutationRef::SetValue) {
				self->kvStore->set(KeyValueRef(mutation.param1.withPrefix(knobOverrideKeys.begin), mutation.param2));
				self->configDatabaseOverriddenKnobs[mutation.param1] = mutation.param2;
			} else if (mutation.type == MutationRef::ClearRange) {
				self->kvStore->clear(KeyRangeRef(mutation.param1.withPrefix(knobOverrideKeys.begin), mutation.param2.withPrefix(knobOverrideKeys.begin)));
				self->configDatabaseOverriddenKnobs.erase(
				    self->configDatabaseOverriddenKnobs.lower_bound(mutation.param1),
				    self->configDatabaseOverriddenKnobs.lower_bound(mutation.param2));
			} else {
				ASSERT(false);
			}
		}
		self->lastSeenVersion = reply.mostRecentVersion;
		self->kvStore->set(KeyValueRef(lastSeenVersionKey, BinaryWriter::toValue(reply.mostRecentVersion, IncludeVersion())));
		wait(self->kvStore->commit());
		self->updateInMemoryKnobs();
		return Void();
	}

	ACTOR static Future<Void> fetchChanges(LocalConfigurationImpl *self, ConfigFollowerInterface broadcaster) {
		try {
			ConfigFollowerGetChangesReply changesReply = wait(broadcaster.getChanges.getReply(ConfigFollowerGetChangesRequest{ self->lastSeenVersion, self->configClasses }));
			// TODO: Avoid applying if there are no updates
			wait(applyKnobUpdates(self, changesReply));
		} catch (Error &e) {
			if (e.code() == error_code_version_already_compacted) {
				ConfigFollowerGetVersionReply versionReply = wait(broadcaster.getVersion.getReply(ConfigFollowerGetVersionRequest{}));
				self->lastSeenVersion = versionReply.version;
				ConfigFollowerGetFullDatabaseReply fullDBReply = wait(broadcaster.getFullDatabase.getReply(ConfigFollowerGetFullDatabaseRequest{ self->lastSeenVersion, self->configClasses}));
				// TODO: Avoid applying if there are no updates
				wait(applyKnobUpdates(self, fullDBReply));
			} else {
				throw e;
			}
		}
		return Void();
	}

	ACTOR static Future<Void> consume(LocalConfigurationImpl* self,
	                                  Reference<AsyncVar<ServerDBInfo> const> serverDBInfo) {
		wait(self->initFuture);
		state Future<ConfigFollowerGetChangesReply> getChangesReply = Never();
		loop {
			auto broadcaster = serverDBInfo->get().configBroadcaster;
			choose {
				when(wait(serverDBInfo->onChange())) {}
				when(wait(fetchChanges(self, serverDBInfo->get().configBroadcaster))) {
					wait(delay(0.5)); // TODO: Make knob?
				}
			}
		}
	}

public:
	LocalConfigurationImpl(ConfigClassSet const& configClasses,
	                       std::string const& dataFolder,
	                       std::map<Key, Value>&& manuallyOverriddenKnobs)
	  : configClasses(configClasses), manuallyOverriddenKnobs(std::move(manuallyOverriddenKnobs)) {
		platform::createDirectory(dataFolder);
		kvStore = keyValueStoreMemory(joinPath(dataFolder, "localconf-"), UID{}, 500e6);
	}

	Future<Void> init() {
		ASSERT(!initFuture.isValid());
		initFuture = init(this);
		return initFuture;
	}

	TestKnobs const &getKnobs() const {
		ASSERT(initFuture.isReady());
		return knobs;
	}

	Future<Void> consume(Reference<AsyncVar<ServerDBInfo> const> const& serverDBInfo) {
		return consume(this, serverDBInfo);
	}
};

LocalConfiguration::LocalConfiguration(ConfigClassSet const& configClasses,
                                       std::string const& dataFolder,
                                       std::map<Key, Value>&& manuallyOverriddenKnobs)
  : impl(std::make_unique<LocalConfigurationImpl>(configClasses, dataFolder, std::move(manuallyOverriddenKnobs))) {}

LocalConfiguration::~LocalConfiguration() = default;

Future<Void> LocalConfiguration::init() {
	return impl->init();
}

TestKnobs const &LocalConfiguration::getKnobs() const {
	return impl->getKnobs();
}

Future<Void> LocalConfiguration::consume(Reference<AsyncVar<ServerDBInfo> const> const& serverDBInfo) {
	return impl->consume(serverDBInfo);
}

#define init(knob, value) initKnob(knob, value, #knob)

void TestKnobs::initialize() {
	init(TEST, 0);
}
