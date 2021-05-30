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
#include "fdbrpc/Stats.h"
#include "fdbserver/ConfigBroadcastFollowerInterface.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LocalConfiguration.h"
#include "fdbserver/OnDemandStore.h"
#include "flow/Knobs.h"
#include "flow/UnitTest.h"

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

KeyRef stringToKeyRef(std::string const& s) {
	return StringRef(reinterpret_cast<uint8_t const*>(s.c_str()), s.size());
}

class ConfigKnobOverrides {
	Standalone<VectorRef<KeyRef>> configPath;
	std::map<Optional<Key>, std::map<Key, Value>> configClassToKnobToValue;

public:
	ConfigKnobOverrides() = default;
	explicit ConfigKnobOverrides(std::string const& paramString) {
		configClassToKnobToValue[{}] = {};
		if (std::all_of(paramString.begin(), paramString.end(), [](char c) {
			    return isalpha(c) || isdigit(c) || c == '/' || c == '-';
		    })) {
			StringRef s = stringToKeyRef(paramString);
			while (s.size()) {
				configPath.push_back_deep(configPath.arena(), s.eat("/"_sr));
				configClassToKnobToValue[configPath.back()] = {};
			}
		} else {
			fprintf(stderr, "WARNING: Invalid configuration path: `%s'\n", paramString.c_str());
		}
	}
	ConfigClassSet getConfigClassSet() const { return ConfigClassSet(configPath); }
	void set(Optional<KeyRef> configClass, KeyRef knobName, ValueRef value) {
		configClassToKnobToValue[configClass.castTo<Key>()][knobName] = value;
	}
	void remove(Optional<KeyRef> configClass, KeyRef knobName) {
		configClassToKnobToValue[configClass.castTo<Key>()].erase(knobName);
	}

	template <class... KS>
	void update(KS&... knobCollections) const {
		// Apply global overrides
		const auto& knobToValue = configClassToKnobToValue.at({});
		for (const auto& [knobName, knobValue] : knobToValue) {
			updateSingleKnob(knobName, knobValue, knobCollections...);
		}

		// Apply specific overrides
		for (const auto& configClass : configPath) {
			const auto& knobToValue = configClassToKnobToValue.at(configClass);
			for (const auto& [knobName, knobValue] : knobToValue) {
				updateSingleKnob(knobName, knobValue, knobCollections...);
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
	explicit ManualKnobOverrides(std::map<std::string, std::string> const& overrides) {
		for (const auto& [knobName, knobValue] : overrides) {
			this->overrides[stringToKeyRef(knobName)] = stringToKeyRef(knobValue);
		}
	}

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
	UID id;
	// TODO: Listen for errors
	OnDemandStore kvStore;
	Future<Void> initFuture;
	FlowKnobs flowKnobs;
	ClientKnobs clientKnobs;
	ServerKnobs serverKnobs;
	TestKnobs testKnobs;
	Version lastSeenVersion{ 0 };
	ManualKnobOverrides manualKnobOverrides;
	ConfigKnobOverrides configKnobOverrides;

	CounterCollection cc;
	Counter broadcasterChanges;
	Counter snapshots;
	Counter changeRequestsFetched;
	Counter mutations;
	Future<Void> logger;

	ACTOR static Future<Void> saveConfigPath(LocalConfigurationImpl* self) {
		self->kvStore->set(
		    KeyValueRef(configPathKey, BinaryWriter::toValue(self->configKnobOverrides, IncludeVersion())));
		wait(self->kvStore->commit());
		return Void();
	}

	ACTOR static Future<Void> clearKVStore(LocalConfigurationImpl* self) {
		self->kvStore->clear(singleKeyRange(configPathKey));
		self->kvStore->clear(knobOverrideKeys);
		wait(self->kvStore->commit());
		return Void();
	}

	ACTOR static Future<Version> getLastSeenVersion(LocalConfigurationImpl* self) {
		state Version result = 0;
		state Optional<Value> lastSeenVersionValue = wait(self->kvStore->readValue(lastSeenVersionKey));
		if (!lastSeenVersionValue.present()) {
			self->kvStore->set(KeyValueRef(lastSeenVersionKey, BinaryWriter::toValue(result, IncludeVersion())));
			wait(self->kvStore->commit());
		} else {
			result = BinaryReader::fromStringRef<Version>(lastSeenVersionValue.get(), IncludeVersion());
		}
		return result;
	}

	ACTOR static Future<Void> initialize(LocalConfigurationImpl* self) {
		state Version lastSeenVersion = wait(getLastSeenVersion(self));
		state Optional<Value> storedConfigPathValue = wait(self->kvStore->readValue(configPathKey));
		if (!storedConfigPathValue.present()) {
			wait(saveConfigPath(self));
			self->updateInMemoryState(lastSeenVersion);
			return Void();
		}
		state ConfigKnobOverrides storedConfigPath =
		    BinaryReader::fromStringRef<ConfigKnobOverrides>(storedConfigPathValue.get(), IncludeVersion());
		if (!storedConfigPath.hasSameConfigPath(self->configKnobOverrides)) {
			// All local information is outdated
			wait(clearKVStore(self));
			wait(saveConfigPath(self));
			self->updateInMemoryState(lastSeenVersion);
			return Void();
		}
		Standalone<RangeResultRef> range = wait(self->kvStore->readRange(knobOverrideKeys));
		for (const auto& kv : range) {
			auto configKey =
			    BinaryReader::fromStringRef<ConfigKey>(kv.key.removePrefix(knobOverrideKeys.begin), IncludeVersion());
			self->configKnobOverrides.set(configKey.configClass, configKey.knobName, kv.value);
		}
		self->updateInMemoryState(lastSeenVersion);
		return Void();
	}

	void initializeKnobs(bool randomize = false, bool isSimulated = false) {
		flowKnobs.initialize(randomize, isSimulated);
		clientKnobs.initialize(randomize);
		serverKnobs.initialize(randomize, &clientKnobs, isSimulated);
		testKnobs.initialize();
	}

	void resetKnobs(bool randomize = false, bool isSimulated = false) {
		flowKnobs.reset(randomize, isSimulated);
		clientKnobs.reset(randomize);
		serverKnobs.reset(randomize, &clientKnobs, isSimulated);
		testKnobs.reset();
	}

	void updateInMemoryState(Version lastSeenVersion) {
		this->lastSeenVersion = lastSeenVersion;
		resetKnobs();
		configKnobOverrides.update(flowKnobs, clientKnobs, serverKnobs, testKnobs);
		manualKnobOverrides.update(flowKnobs, clientKnobs, serverKnobs, testKnobs);
		// Must reinitialize in order to update dependent knobs
		initializeKnobs();
	}

	ACTOR static Future<Void> setSnapshot(LocalConfigurationImpl* self,
	                                      std::map<ConfigKey, Value> snapshot,
	                                      Version snapshotVersion) {
		// TODO: Concurrency control?
		ASSERT(self->initFuture.isValid() && self->initFuture.isReady());
		++self->snapshots;
		self->kvStore->clear(knobOverrideKeys);
		for (const auto& [configKey, knobValue] : snapshot) {
			self->configKnobOverrides.set(configKey.configClass, configKey.knobName, knobValue);
			self->kvStore->set(KeyValueRef(
			    BinaryWriter::toValue(configKey, IncludeVersion()).withPrefix(knobOverrideKeys.begin), knobValue));
		}
		ASSERT_GE(snapshotVersion, self->lastSeenVersion);
		self->kvStore->set(KeyValueRef(lastSeenVersionKey, BinaryWriter::toValue(snapshotVersion, IncludeVersion())));
		wait(self->kvStore->commit());
		self->updateInMemoryState(snapshotVersion);
		return Void();
	}

	ACTOR static Future<Void> addChanges(LocalConfigurationImpl* self,
	                                     Standalone<VectorRef<VersionedConfigMutationRef>> changes,
	                                     Version mostRecentVersion) {
		// TODO: Concurrency control?
		ASSERT(self->initFuture.isValid() && self->initFuture.isReady());
		++self->changeRequestsFetched;
		for (const auto& versionedMutation : changes) {
			++self->mutations;
			const auto& mutation = versionedMutation.mutation;
			auto serializedKey = BinaryWriter::toValue(mutation.getKey(), IncludeVersion());
			if (mutation.isSet()) {
				self->kvStore->set(
				    KeyValueRef(serializedKey.withPrefix(knobOverrideKeys.begin), mutation.getValue().get()));
				self->configKnobOverrides.set(
				    mutation.getConfigClass(), mutation.getKnobName(), mutation.getValue().get());
			} else {
				self->kvStore->clear(singleKeyRange(serializedKey.withPrefix(knobOverrideKeys.begin)));
				self->configKnobOverrides.remove(mutation.getConfigClass(), mutation.getKnobName());
			}
		}
		self->kvStore->set(KeyValueRef(lastSeenVersionKey, BinaryWriter::toValue(mostRecentVersion, IncludeVersion())));
		wait(self->kvStore->commit());
		self->updateInMemoryState(mostRecentVersion);
		return Void();
	}

	ACTOR static Future<Void> consumeInternal(LocalConfigurationImpl* self,
	                                          ConfigBroadcastFollowerInterface broadcaster) {
		loop {
			try {
				state ConfigFollowerGetChangesReply changesReply =
				    wait(broadcaster.getChanges.getReply(ConfigBroadcastFollowerGetChangesRequest{
				        self->lastSeenVersion, self->configKnobOverrides.getConfigClassSet() }));
				TraceEvent(SevDebug, "LocalConfigGotChanges", self->id); // TODO: Add more fields
				wait(self->addChanges(changesReply.changes, changesReply.mostRecentVersion));
			} catch (Error& e) {
				if (e.code() == error_code_version_already_compacted) {
					// TODO: Collect stats
					state ConfigBroadcastFollowerGetSnapshotReply snapshotReply = wait(broadcaster.getSnapshot.getReply(
					    ConfigBroadcastFollowerGetSnapshotRequest{ self->configKnobOverrides.getConfigClassSet() }));
					ASSERT_GT(snapshotReply.version, self->lastSeenVersion);
					wait(setSnapshot(self, std::move(snapshotReply.snapshot), snapshotReply.version));
				} else {
					throw e;
				}
			}
			wait(yield()); // Necessary to not immediately trigger retry?
		}
	}

	ACTOR static Future<Void> consume(
	    LocalConfigurationImpl* self,
	    Reference<IDependentAsyncVar<ConfigBroadcastFollowerInterface> const> broadcaster) {
		ASSERT(self->initFuture.isValid() && self->initFuture.isReady());
		loop {
			choose {
				when(wait(brokenPromiseToNever(consumeInternal(self, broadcaster->get())))) { ASSERT(false); }
				when(wait(broadcaster->onChange())) { ++self->broadcasterChanges; }
				when(wait(self->kvStore->getError())) { ASSERT(false); }
			}
		}
	}

public:
	LocalConfigurationImpl(std::string const& dataFolder,
	                       std::string const& configPath,
	                       std::map<std::string, std::string> const& manualKnobOverrides,
	                       Optional<UID> testID)
	  : id(testID.present() ? testID.get() : deterministicRandom()->randomUniqueID()),
	    kvStore(dataFolder, id, "localconf-" + (testID.present() ? id.toString() : "")), cc("LocalConfiguration"),
	    broadcasterChanges("BroadcasterChanges", cc), snapshots("Snapshots", cc),
	    changeRequestsFetched("ChangeRequestsFetched", cc), mutations("Mutations", cc), configKnobOverrides(configPath),
	    manualKnobOverrides(manualKnobOverrides) {
		logger = traceCounters(
		    "LocalConfigurationMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "LocalConfigurationMetrics");
	}

	Future<Void> initialize() {
		ASSERT(!initFuture.isValid());
		initFuture = initialize(this);
		return initFuture;
	}

	Future<Void> addChanges(Standalone<VectorRef<VersionedConfigMutationRef>> changes, Version mostRecentVersion) {
		return addChanges(this, changes, mostRecentVersion);
	}

	FlowKnobs const& getFlowKnobs() const {
		ASSERT(initFuture.isValid() && initFuture.isReady());
		return flowKnobs;
	}

	ClientKnobs const& getClientKnobs() const {
		ASSERT(initFuture.isValid() && initFuture.isReady());
		return clientKnobs;
	}

	ServerKnobs const& getServerKnobs() const {
		ASSERT(initFuture.isValid() && initFuture.isReady());
		return serverKnobs;
	}

	TestKnobs const& getTestKnobs() const {
		ASSERT(initFuture.isValid() && initFuture.isReady());
		return testKnobs;
	}

	Future<Void> consume(Reference<IDependentAsyncVar<ConfigBroadcastFollowerInterface> const> const& broadcaster) {
		return consume(this, broadcaster);
	}

	UID getID() const { return id; }
};

LocalConfiguration::LocalConfiguration(std::string const& dataFolder,
                                       std::string const& configPath,
                                       std::map<std::string, std::string> const& manualKnobOverrides,
                                       Optional<UID> testID)
  : impl(std::make_unique<LocalConfigurationImpl>(dataFolder, configPath, manualKnobOverrides, testID)) {}

LocalConfiguration::LocalConfiguration(LocalConfiguration&&) = default;

LocalConfiguration& LocalConfiguration::operator=(LocalConfiguration&&) = default;

LocalConfiguration::~LocalConfiguration() = default;

Future<Void> LocalConfiguration::initialize() {
	return impl->initialize();
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

Future<Void> LocalConfiguration::consume(
    Reference<IDependentAsyncVar<ConfigBroadcastFollowerInterface> const> const& broadcaster) {
	return impl->consume(broadcaster);
}

Future<Void> LocalConfiguration::addChanges(Standalone<VectorRef<VersionedConfigMutationRef>> changes,
                                            Version mostRecentVersion) {
	return impl->addChanges(changes, mostRecentVersion);
}

UID LocalConfiguration::getID() const {
	return impl->getID();
}

#define init(knob, value) initKnob(knob, value, #knob)

TestKnobs::TestKnobs() {
	initialize();
}

void TestKnobs::initialize() {
	init(TEST_LONG, 0);
	init(TEST_INT, 0);
	init(TEST_DOUBLE, 0.0);
	init(TEST_BOOL, false);
	init(TEST_STRING, "");
}

bool TestKnobs::operator==(TestKnobs const& rhs) const {
	return (TEST_LONG == rhs.TEST_LONG) && (TEST_INT == rhs.TEST_INT) && (TEST_DOUBLE == rhs.TEST_DOUBLE) &&
	       (TEST_BOOL == rhs.TEST_BOOL) && (TEST_STRING == rhs.TEST_STRING);
}

bool TestKnobs::operator!=(TestKnobs const& rhs) const {
	return !(*this == rhs);
}

namespace {

class TestKnobs2 : public Knobs<TestKnobs2> {
public:
	int64_t TEST2_LONG;
	int TEST2_INT;
	double TEST2_DOUBLE;
	bool TEST2_BOOL;
	std::string TEST2_STRING;

	void initialize() {
		init(TEST2_LONG, 0);
		init(TEST2_INT, 0);
		init(TEST2_DOUBLE, 0.0);
		init(TEST2_BOOL, false);
		init(TEST2_STRING, "");
	}

	TestKnobs2() { initialize(); }
};

} // namespace

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Internal/updateSingleKnob") {
	TestKnobs k1;
	TestKnobs2 k2;
	updateSingleKnob("test_long"_sr, "5"_sr, k1, k2);
	updateSingleKnob("test_int"_sr, "5"_sr, k1, k2);
	updateSingleKnob("test_double"_sr, "5.0"_sr, k1, k2);
	updateSingleKnob("test_bool"_sr, "true"_sr, k1, k2);
	updateSingleKnob("test_string"_sr, "5"_sr, k1, k2);
	updateSingleKnob("test2_long"_sr, "10"_sr, k1, k2);
	updateSingleKnob("test2_int"_sr, "10"_sr, k1, k2);
	updateSingleKnob("test2_double"_sr, "10.0"_sr, k1, k2);
	updateSingleKnob("test2_bool"_sr, "true"_sr, k1, k2);
	updateSingleKnob("test2_string"_sr, "10"_sr, k1, k2);
	ASSERT_EQ(k1.TEST_LONG, 5);
	ASSERT_EQ(k1.TEST_INT, 5);
	ASSERT_EQ(k1.TEST_DOUBLE, 5.0);
	ASSERT(k1.TEST_BOOL);
	ASSERT(k1.TEST_STRING == "5");
	ASSERT_EQ(k2.TEST2_LONG, 10);
	ASSERT_EQ(k2.TEST2_INT, 10);
	ASSERT_EQ(k2.TEST2_DOUBLE, 10.0);
	ASSERT(k2.TEST2_BOOL);
	ASSERT(k2.TEST2_STRING == "10");
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Internal/ManualKnobOverrides") {
	TestKnobs k1;
	TestKnobs2 k2;
	std::map<std::string, std::string> m = { { "test_int", "5" }, { "test2_int", "10" } };
	ManualKnobOverrides manualKnobOverrides(std::move(m));
	manualKnobOverrides.update(k1, k2);
	ASSERT(k1.TEST_INT == 5);
	ASSERT(k2.TEST2_INT == 10);
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Internal/GlobalKnobOverride") {
	TestKnobs k1;
	ConfigKnobOverrides configKnobOverrides("class-A");
	configKnobOverrides.set({}, "test_int"_sr, "5"_sr);
	configKnobOverrides.update(k1);
	ASSERT_EQ(k1.TEST_INT, 5);
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Internal/ConfigKnobOverrides") {
	TestKnobs k1;
	TestKnobs2 k2;
	ConfigKnobOverrides configKnobOverrides("class-A/class-B");
	configKnobOverrides.update(k1, k2);
	ASSERT(k1.TEST_INT == 0);
	ASSERT(k2.TEST2_INT == 0);
	configKnobOverrides.set("class-B"_sr, "test_int"_sr, "7"_sr);
	configKnobOverrides.set("class-A"_sr, "test_int"_sr, "5"_sr);
	configKnobOverrides.set("class-A"_sr, "test2_int"_sr, "10"_sr);
	configKnobOverrides.update(k1, k2);
	ASSERT(k1.TEST_INT == 7);
	ASSERT(k2.TEST2_INT == 10);
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Internal/InvalidKnobName") {
	TestKnobs k1;
	ConfigKnobOverrides configKnobOverrides("class-A");
	// Noop with a SevWarnAlways trace
	configKnobOverrides.set("class-A", "thisknobdoesnotexist"_sr, "5"_sr);
	configKnobOverrides.update(k1);
	return Void();
}
