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

const KeyRef configPathKey = "configPath"_sr;
const KeyRef lastSeenVersionKey = "lastSeenVersion"_sr;
const KeyRangeRef knobOverrideKeys = KeyRangeRef("knobOverride/"_sr, "knobOverride0"_sr);

bool updateSingleKnob(Key knobName, Value knobValue) {
	return false;
}

template <class K, class... Rest>
bool updateSingleKnob(Key knobName, Value knobValue, K& k, Rest&... rest) {
	if (k.setKnob(knobName.toString(), knobValue.toString())) {
		return true;
	} else {
		return updateSingleKnob(knobName, knobValue, rest...);
	}
}

class ConfigKnobOverrides {
	Standalone<VectorRef<KeyRef>> configPath;
	std::map<Key, std::map<Key, Value>> configClassToKnobToValue;

public:
	ConfigKnobOverrides() = default;
	ConfigKnobOverrides(std::string const& paramString) {
		// TODO: Validate string
		// FIXME: Fix this implementation
		/*
		auto b = paramString.begin();
		while (b != paramString.end()) {
		    auto e = std::find(b, paramString.end(), '/');
		    configPath.emplace_back_deep(configPath.arena(), reinterpret_cast<uint8_t const *>(&(*b)), e-b);
		    if (e != paramString.end()) {
		        b = e+1;
		    }
		}
		*/
	}
	ConfigClassSet getConfigClassSet() const { return ConfigClassSet(configPath); }
	void set(KeyRef configClass, KeyRef knobName, ValueRef value) {
		configClassToKnobToValue[configClass][knobName] = value;
	}
	void remove(KeyRef configClass, KeyRef knobName) { configClassToKnobToValue[configClass].erase(knobName); }

	template <class... KS>
	void update(KS&... knobCollections) const {
		for (const auto& configClass : configPath) {
			const auto& knobToValue = configClassToKnobToValue.find(configClass);
			if (knobToValue != configClassToKnobToValue.end()) {
				for (const auto& [knobName, knobValue] : knobToValue->second) {
					// Assert here because we should be validating on the client
					ASSERT(updateSingleKnob(knobName, knobValue, knobCollections...));
				}
			}
		}
	}

	bool hasSameConfigPath(ConfigKnobOverrides const& other) const { return configPath == other.configPath; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, configPath);
	}
};

class ManualKnobOverrides {
	std::map<Key, Value> overrides;

public:
	ManualKnobOverrides(std::map<Key, Value>&& overrides) : overrides(std::move(overrides)) {}

	template <class... KS>
	void update(KS&... knobCollections) const {
		for (const auto& [knobName, knobValue] : overrides) {
			if (!updateSingleKnob(knobName, knobValue, knobCollections...)) {
				fprintf(stderr, "WARNING: Unrecognized knob option '%s'\n", knobName.toString().c_str());
				TraceEvent(SevWarnAlways, "UnrecognizedKnobOption").detail("Knob", printable(knobName));
			}
		}
	}
};

} // namespace

class LocalConfigurationImpl {
	IKeyValueStore* kvStore; // FIXME: fix leaks?
	Version lastSeenVersion { 0 };
	Future<Void> initFuture;
	FlowKnobs flowKnobs;
	ClientKnobs clientKnobs;
	ServerKnobs serverKnobs;
	TestKnobs testKnobs;
	ManualKnobOverrides manualKnobOverrides;
	ConfigKnobOverrides configKnobOverrides;

	ACTOR static Future<Void> saveConfigPath(LocalConfigurationImpl* self) {
		self->kvStore->set(
		    KeyValueRef(configPathKey, BinaryWriter::toValue(self->configKnobOverrides, IncludeVersion())));
		wait(self->kvStore->commit());
		return Void();
	}

	ACTOR static Future<Void> clearKVStore(LocalConfigurationImpl *self) {
		self->kvStore->clear(singleKeyRange(configPathKey));
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
		state Optional<Value> storedConfigPathValue = wait(self->kvStore->readValue(configPathKey));
		if (!storedConfigPathValue.present()) {
			wait(saveConfigPath(self));
			self->updateInMemoryKnobs();
			return Void();
		}
		state ConfigKnobOverrides storedConfigPath =
		    BinaryReader::fromStringRef<ConfigKnobOverrides>(storedConfigPathValue.get(), IncludeVersion());
		if (!storedConfigPath.hasSameConfigPath(self->configKnobOverrides)) {
			// All local information is outdated
			wait(clearKVStore(self));
			wait(saveConfigPath(self));
			self->updateInMemoryKnobs();
			return Void();
		}
		Standalone<RangeResultRef> range = wait(self->kvStore->readRange(knobOverrideKeys));
		for (const auto &kv : range) {
			auto configKey = BinaryReader::fromStringRef<ConfigKey>(kv.key, IncludeVersion());
			self->configKnobOverrides.set(configKey.configClass, configKey.knobName, kv.value);
		}
		self->updateInMemoryKnobs();
		return Void();
	}

	void initializeKnobs() {
		flowKnobs.initialize();
		clientKnobs.initialize();
		serverKnobs.initialize();
		testKnobs.initialize();
	}

	void resetKnobs() {
		flowKnobs.reset();
		clientKnobs.reset();
		serverKnobs.reset();
		testKnobs.reset();
	}

	void updateInMemoryKnobs() {
		resetKnobs();
		configKnobOverrides.update(flowKnobs, clientKnobs, serverKnobs, testKnobs);
		manualKnobOverrides.update(flowKnobs, clientKnobs, serverKnobs, testKnobs);
		// Must reinitialize in order to update dependent knobs
		initializeKnobs();
	}

	ACTOR static Future<Void> applyKnobUpdates(LocalConfigurationImpl *self, ConfigFollowerGetFullDatabaseReply reply) {
		self->kvStore->clear(knobOverrideKeys);
		for (const auto& [configKey, knobValue] : reply.database) {
			self->configKnobOverrides.set(configKey.configClass, configKey.knobName, knobValue);
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
				self->configKnobOverrides.set(mutation.getConfigClass(), mutation.getKnobName(), mutation.getValue());
			} else {
				self->kvStore->clear(singleKeyRange(serializedKey.withPrefix(knobOverrideKeys.begin)));
				self->configKnobOverrides.remove(mutation.getConfigClass(), mutation.getKnobName());
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
			ConfigFollowerGetChangesReply changesReply =
			    wait(broadcaster.getChanges.getReply(ConfigFollowerGetChangesRequest{
			        self->lastSeenVersion, self->configKnobOverrides.getConfigClassSet() }));
			// TODO: Avoid applying if there are no updates
			wait(applyKnobUpdates(self, changesReply));
		} catch (Error &e) {
			if (e.code() == error_code_version_already_compacted) {
				ConfigFollowerGetVersionReply versionReply = wait(broadcaster.getVersion.getReply(ConfigFollowerGetVersionRequest{}));
				self->lastSeenVersion = versionReply.version;
				ConfigFollowerGetFullDatabaseReply fullDBReply =
				    wait(broadcaster.getFullDatabase.getReply(ConfigFollowerGetFullDatabaseRequest{
				        self->lastSeenVersion, self->configKnobOverrides.getConfigClassSet() }));
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
	LocalConfigurationImpl(std::string const& configPath,
	                       std::string const& dataFolder,
	                       std::map<Key, Value>&& manualKnobOverrides,
	                       UID id)
	  : configKnobOverrides(configPath), manualKnobOverrides(std::move(manualKnobOverrides)) {
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

LocalConfiguration::LocalConfiguration(std::string const& configPath,
                                       std::string const& dataFolder,
                                       std::map<Key, Value>&& manualKnobOverrides,
                                       UID id)
  : impl(std::make_unique<LocalConfigurationImpl>(configPath, dataFolder, std::move(manualKnobOverrides), id)) {}

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

void TestKnobs::reset() {
	explicitlySetKnobs.clear();
	initialize();
}
