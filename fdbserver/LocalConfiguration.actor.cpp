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

#include "fdbclient/Knobs.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/LocalConfiguration.h"
#include "flow/Knobs.h"
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
	FlowKnobs flowKnobs;
	ClientKnobs clientKnobs;
	ServerKnobs serverKnobs;
	TestKnobs testKnobs;
	std::map<Key, Value> manuallyOverriddenKnobs;
	std::map<ConfigKey, Value> configDatabaseOverriddenKnobs;

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
			auto configKey = BinaryReader::fromStringRef<ConfigKey>(kv.key, IncludeVersion());
			self->configDatabaseOverriddenKnobs[configKey] = kv.value;
		}
		self->updateInMemoryKnobs();
		return Void();
	}

	bool updateKnob(Key knobName, Value knobValue) {
		return (flowKnobs.setKnob(knobName.toString(), knobValue.toString()) ||
		        clientKnobs.setKnob(knobName.toString(), knobValue.toString()) ||
		        serverKnobs.setKnob(knobName.toString(), knobValue.toString()) ||
		        testKnobs.setKnob(knobName.toString(), knobValue.toString()));
	}

	void initializeKnobs() {
		flowKnobs.initialize();
		clientKnobs.initialize();
		serverKnobs.initialize();
		testKnobs.initialize();
	}

	void resetKnobs() {
		flowKnobs.reset();
		clientKnobs.initialize();
		serverKnobs.initialize();
		testKnobs.initialize();
	}

	void updateInMemoryKnobs() {
		resetKnobs();
		for (const auto& [configKey, knobValue] : configDatabaseOverriddenKnobs) {
			// TODO: Order updates by config class
			// Assert here because we should be validating on the client
			ASSERT(updateKnob(configKey.knobName, knobValue));
		}
		for (const auto& [knobName, knobValue] : manuallyOverriddenKnobs) {
			if (!updateKnob(knobName, knobValue)) {
				fprintf(stderr, "WARNING: Unrecognized knob option '%s'\n", knobName.toString().c_str());
				TraceEvent(SevWarnAlways, "UnrecognizedKnobOption").detail("Knob", printable(knobName));
			}
		}
		// Must reinitialize in order to update dependent knobs
		initializeKnobs();
	}

	ACTOR static Future<Void> applyKnobUpdates(LocalConfigurationImpl *self, ConfigFollowerGetFullDatabaseReply reply) {
		self->kvStore->clear(knobOverrideKeys);
		for (const auto& [configKey, knobValue] : reply.database) {
			self->configDatabaseOverriddenKnobs[configKey] = knobValue;
		}
		self->kvStore->set(KeyValueRef(lastSeenVersionKey, BinaryWriter::toValue(self->lastSeenVersion, IncludeVersion())));
		wait(self->kvStore->commit());
		self->updateInMemoryKnobs();
		return Void();
	}

	ACTOR static Future<Void> applyKnobUpdates(LocalConfigurationImpl *self, ConfigFollowerGetChangesReply reply) {
		for (const auto &versionedMutation : reply.versionedMutations) {
			const auto &mutation = versionedMutation.mutation;
			auto serializedKey = BinaryWriter::toValue(mutation.getKey(), IncludeVersion());
			if (mutation.isSet()) {
				self->kvStore->set(KeyValueRef(serializedKey.withPrefix(knobOverrideKeys.begin), mutation.getValue()));
				self->configDatabaseOverriddenKnobs[mutation.getKey()] = mutation.getValue();
			} else {
				self->kvStore->clear(singleKeyRange(serializedKey.withPrefix(knobOverrideKeys.begin)));
				self->configDatabaseOverriddenKnobs.erase(mutation.getKey());
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

	ACTOR static Future<Void> monitorBroadcaster(Reference<AsyncVar<ServerDBInfo> const> serverDBInfo,
	                                             Reference<AsyncVar<ConfigFollowerInterface>> broadcaster) {
		loop {
			wait(serverDBInfo->onChange());
			broadcaster->set(serverDBInfo->get().configBroadcaster);
		}
	}

	ACTOR static Future<Void> consume(LocalConfigurationImpl* self,
	                                  Reference<AsyncVar<ServerDBInfo> const> serverDBInfo) {
		wait(self->initFuture);
		state Future<ConfigFollowerGetChangesReply> getChangesReply = Never();
		state Reference<AsyncVar<ConfigFollowerInterface>> broadcaster =
		    makeReference<AsyncVar<ConfigFollowerInterface>>(serverDBInfo->get().configBroadcaster);
		state Future<Void> monitor = monitorBroadcaster(serverDBInfo, broadcaster);
		loop {
			choose {
				when(wait(broadcaster->onChange())) {}
				when(wait(brokenPromiseToNever(fetchChanges(self, broadcaster->get())))) {
					wait(delay(5.0)); // TODO: Make knob?
				}
				when(wait(monitor)) { ASSERT(false); }
			}
		}
	}

public:
	LocalConfigurationImpl(ConfigClassSet const& configClasses,
	                       std::string const& dataFolder,
	                       std::map<Key, Value>&& manuallyOverriddenKnobs,
	                       UID id)
	  : configClasses(configClasses), manuallyOverriddenKnobs(std::move(manuallyOverriddenKnobs)) {
		platform::createDirectory(dataFolder);
		kvStore = keyValueStoreMemory(joinPath(dataFolder, "localconf-" + id.toString()), id, 500e6);
	}

	Future<Void> init() {
		ASSERT(!initFuture.isValid());
		initFuture = init(this);
		return initFuture;
	}

	FlowKnobs const& getFlowKnobs() const {
		ASSERT(initFuture.isReady());
		return flowKnobs;
	}

	ClientKnobs const& getClientKnobs() const {
		ASSERT(initFuture.isReady());
		return clientKnobs;
	}

	ServerKnobs const& getServerKnobs() const {
		ASSERT(initFuture.isReady());
		return serverKnobs;
	}

	TestKnobs const& getTestKnobs() const {
		ASSERT(initFuture.isReady());
		return testKnobs;
	}

	Future<Void> consume(Reference<AsyncVar<ServerDBInfo> const> const& serverDBInfo) {
		return consume(this, serverDBInfo);
	}
};

LocalConfiguration::LocalConfiguration(ConfigClassSet const& configClasses,
                                       std::string const& dataFolder,
                                       std::map<Key, Value>&& manuallyOverriddenKnobs,
                                       UID id)
  : impl(std::make_unique<LocalConfigurationImpl>(configClasses, dataFolder, std::move(manuallyOverriddenKnobs), id)) {}

LocalConfiguration::~LocalConfiguration() = default;

Future<Void> LocalConfiguration::init() {
	return impl->init();
}

FlowKnobs const& LocalConfiguration::getFlowKnobs() const {
	return impl->getFlowKnobs();
}

ClientKnobs const& LocalConfiguration::getClientKnobs() const {
	return impl->getClientKnobs();
}

ServerKnobs const& LocalConfiguration::getServerKnobs() const {
	return impl->getServerKnobs();
}

TestKnobs const& LocalConfiguration::getTestKnobs() const {
	return impl->getTestKnobs();
}

Future<Void> LocalConfiguration::consume(Reference<AsyncVar<ServerDBInfo> const> const& serverDBInfo) {
	return impl->consume(serverDBInfo);
}

#define init(knob, value) initKnob(knob, value, #knob)

void TestKnobs::initialize() {
	init(TEST, 0);
}
