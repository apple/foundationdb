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
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LocalConfiguration.h"
#include "fdbserver/SimpleConfigConsumer.h"
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

class ConfigKnobOverrides {
	Standalone<VectorRef<KeyRef>> configPath;
	std::map<Key, std::map<Key, Value>> configClassToKnobToValue;

public:
	ConfigKnobOverrides() = default;
	explicit ConfigKnobOverrides(std::string const& paramString) {
		// TODO: Validate string
		StringRef s(reinterpret_cast<uint8_t const*>(paramString.c_str()), paramString.size());
		while (s.size()) {
			configPath.push_back_deep(configPath.arena(), s.eat("/"_sr));
			configClassToKnobToValue[configPath.back()] = {};
		}
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
			ASSERT(knobToValue != configClassToKnobToValue.end());
			for (const auto& [knobName, knobValue] : knobToValue->second) {
				// Assert here because we should be validating on the client
				ASSERT(updateSingleKnob(knobName, knobValue, knobCollections...));
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
	template <class Overrides>
	explicit ManualKnobOverrides(Overrides&& overrides) : overrides(std::forward<Overrides>(overrides)) {}

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

class LocalConfigurationImpl : public NonCopyable {
	IKeyValueStore* kvStore{ nullptr };
	Future<Void> initFuture;
	FlowKnobs flowKnobs;
	ClientKnobs clientKnobs;
	ServerKnobs serverKnobs;
	TestKnobs testKnobs;
	Version lastSeenVersion{ 0 };
	ManualKnobOverrides manualKnobOverrides;
	ConfigKnobOverrides configKnobOverrides;
	ActorCollection actors{ false };

	CounterCollection cc;
	Counter broadcasterChanges;
	Counter snapshots;
	Counter changeRequestsFetched;
	Counter mutations;
	Future<Void> logger;
	UID id;

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

	ACTOR static Future<Void> initialize(LocalConfigurationImpl* self, std::string dataFolder, UID id) {
		platform::createDirectory(dataFolder);
		self->id = id;
		self->kvStore = keyValueStoreMemory(joinPath(dataFolder, "localconf-" + id.toString()), id, 500e6);
		self->logger = traceCounters("LocalConfigurationMetrics",
		                             id,
		                             SERVER_KNOBS->WORKER_LOGGING_INTERVAL,
		                             &self->cc,
		                             "LocalConfigurationMetrics");
		wait(self->kvStore->init());
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
		for (const auto &kv : range) {
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

	void resetKnobs() {
		flowKnobs.reset();
		clientKnobs.reset();
		serverKnobs.reset(&clientKnobs);
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

	ACTOR static Future<Void> addVersionedMutations(
	    LocalConfigurationImpl* self,
	    Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations,
	    Version mostRecentVersion) {
		// TODO: Concurrency control?
		++self->changeRequestsFetched;
		for (const auto& versionedMutation : versionedMutations) {
			++self->mutations;
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
		self->kvStore->set(KeyValueRef(lastSeenVersionKey, BinaryWriter::toValue(mostRecentVersion, IncludeVersion())));
		wait(self->kvStore->commit());
		self->updateInMemoryState(mostRecentVersion);
		return Void();
	}

	ACTOR static Future<Void> consumeLoopIteration(
	    LocalConfiguration* self,
	    LocalConfigurationImpl* impl,
	    Reference<IDependentAsyncVar<ConfigFollowerInterface> const> broadcaster) {
		state SimpleConfigConsumer consumer(broadcaster->get(),
		                                    impl->configKnobOverrides.getConfigClassSet(),
		                                    impl->lastSeenVersion,
		                                    deterministicRandom()->randomUniqueID());
		TraceEvent(SevDebug, "LocalConfigurationStartingConsumer", impl->id)
		    .detail("Consumer", consumer.getID())
		    .detail("Broadcaster", broadcaster->get().id());
		choose {
			when(wait(broadcaster->onChange())) { ++impl->broadcasterChanges; }
			when(wait(brokenPromiseToNever(consumer.consume(*self)))) { ASSERT(false); }
			when(wait(impl->actors.getResult())) { ASSERT(false); }
		}
		return Void();
	}

	ACTOR static Future<Void> consume(LocalConfiguration* self,
	                                  LocalConfigurationImpl* impl,
	                                  Reference<IDependentAsyncVar<ConfigFollowerInterface> const> broadcaster) {
		wait(impl->initFuture);
		loop { wait(consumeLoopIteration(self, impl, broadcaster)); }
	}

public:
	template <class ManualKnobOverrides>
	LocalConfigurationImpl(std::string const& configPath, ManualKnobOverrides&& manualKnobOverrides)
	  : cc("LocalConfiguration"), broadcasterChanges("BroadcasterChanges", cc), snapshots("Snapshots", cc),
	    changeRequestsFetched("ChangeRequestsFetched", cc), mutations("Mutations", cc), configKnobOverrides(configPath),
	    manualKnobOverrides(std::forward<ManualKnobOverrides>(manualKnobOverrides)) {}

	~LocalConfigurationImpl() {
		if (kvStore) {
			kvStore->close();
		}
	}

	Future<Void> initialize(std::string const& dataFolder, UID id) {
		ASSERT(!initFuture.isValid());
		initFuture = initialize(this, dataFolder, id);
		return initFuture;
	}

	Future<Void> setSnapshot(std::map<ConfigKey, Value> const& snapshot, Version lastCompactedVersion) {
		return setSnapshot(this, snapshot, lastCompactedVersion);
	}

	Future<Void> addVersionedMutations(Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations,
	                                   Version mostRecentVersion) {
		return addVersionedMutations(this, versionedMutations, mostRecentVersion);
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

	Future<Void> consume(LocalConfiguration& self,
	                     Reference<IDependentAsyncVar<ConfigFollowerInterface> const> const& broadcaster) {
		return consume(&self, this, broadcaster);
	}

	UID getID() const { return id; }
};

LocalConfiguration::LocalConfiguration(std::string const& configPath, std::map<Key, Value>&& manualKnobOverrides)
  : impl(std::make_unique<LocalConfigurationImpl>(configPath, std::move(manualKnobOverrides))) {}

LocalConfiguration::LocalConfiguration(std::string const& configPath, std::map<Key, Value> const& manualKnobOverrides)
  : impl(std::make_unique<LocalConfigurationImpl>(configPath, manualKnobOverrides)) {}

LocalConfiguration::~LocalConfiguration() = default;

Future<Void> LocalConfiguration::initialize(std::string const& dataFolder, UID id) {
	return impl->initialize(dataFolder, id);
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
    Reference<IDependentAsyncVar<ConfigFollowerInterface> const> const& broadcaster) {
	return impl->consume(*this, broadcaster);
}

Future<Void> LocalConfiguration::setSnapshot(std::map<ConfigKey, Value> const& snapshot, Version lastCompactedVersion) {
	return impl->setSnapshot(snapshot, lastCompactedVersion);
}

Future<Void> LocalConfiguration::addVersionedMutations(
    Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations,
    Version mostRecentVersion) {
	return impl->addVersionedMutations(versionedMutations, mostRecentVersion);
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

void TestKnobs::reset() {
	explicitlySetKnobs.clear();
	initialize();
}

bool TestKnobs::operator==(TestKnobs const& rhs) const {
	return (TEST_LONG == rhs.TEST_LONG) && (TEST_INT == rhs.TEST_INT) && (TEST_DOUBLE == rhs.TEST_DOUBLE) &&
	       (TEST_BOOL == rhs.TEST_BOOL) && (TEST_STRING == rhs.TEST_STRING);
}

bool TestKnobs::operator!=(TestKnobs const& rhs) const {
	return !(*this == rhs);
}

namespace {

class TestKnobs2 : public Knobs {
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

	void reset() {
		explicitlySetKnobs.clear();
		initialize();
	}
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
	ASSERT(k1.TEST_LONG == 5);
	ASSERT(k1.TEST_INT == 5);
	ASSERT(k1.TEST_DOUBLE == 5.0);
	ASSERT(k1.TEST_BOOL);
	ASSERT(k1.TEST_STRING == "5");
	ASSERT(k2.TEST2_LONG == 10);
	ASSERT(k2.TEST2_INT == 10);
	ASSERT(k2.TEST2_DOUBLE == 10.0);
	ASSERT(k2.TEST2_BOOL);
	ASSERT(k2.TEST2_STRING == "10");
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Internal/ManualKnobOverrides") {
	TestKnobs k1;
	TestKnobs2 k2;
	std::map<Key, Value> m;
	m["test_int"_sr] = "5"_sr;
	m["test2_int"_sr] = "10"_sr;
	ManualKnobOverrides manualKnobOverrides(std::move(m));
	manualKnobOverrides.update(k1, k2);
	ASSERT(k1.TEST_INT == 5);
	ASSERT(k2.TEST2_INT == 10);
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
