/*
 * ConfigBroadcaster.actor.cpp
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

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/SimpleConfigTransaction.h"
#include "fdbserver/ConfigBroadcaster.h"
#include "fdbserver/IConfigDatabaseNode.h"
#include "fdbserver/LocalConfiguration.h"
#include "fdbclient/Tuple.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

Key encodeConfigKey(Optional<KeyRef> configClass, KeyRef knobName) {
	Tuple tuple;
	if (configClass.present()) {
		tuple.append(configClass.get());
	} else {
		tuple.appendNull();
	}
	tuple << knobName;
	return tuple.pack();
}

void appendVersionedMutation(Standalone<VectorRef<VersionedConfigMutationRef>>& versionedMutations,
                             Version version,
                             Optional<KeyRef> configClass,
                             KeyRef knobName,
                             Optional<ValueRef> knobValue) {
	auto mutation = ConfigMutationRef::createConfigMutation(encodeConfigKey(configClass, knobName), knobValue);
	versionedMutations.emplace_back_deep(versionedMutations.arena(), version, mutation);
}

Value longToValue(int64_t v) {
	auto s = format("%ld", v);
	return StringRef(reinterpret_cast<uint8_t const*>(s.c_str()), s.size());
}

Future<Void> addTestClearMutations(SimpleConfigTransaction& tr, Version version /* TODO: shouldn't need this */) {
	tr.fullReset();
	auto configKeyA = encodeConfigKey("class-A"_sr, "test_long"_sr);
	auto configKeyB = encodeConfigKey("class-B"_sr, "test_long"_sr);
	tr.clear(configKeyA);
	tr.clear(configKeyB);
	return tr.commit();
}

Future<Void> addTestGlobalSetMutation(SimpleConfigTransaction& tr, int64_t value) {
	tr.fullReset();
	auto configKey = encodeConfigKey({}, "test_long"_sr);
	tr.set(configKey, longToValue(value));
	return tr.commit();
}

Future<Void> addTestSetMutations(SimpleConfigTransaction& tr, int64_t value) {
	tr.fullReset();
	auto configKeyA = encodeConfigKey("class-A"_sr, "test_long"_sr);
	auto configKeyB = encodeConfigKey("class-B"_sr, "test_long"_sr);
	tr.set(configKeyA, longToValue(value));
	tr.set(configKeyB, longToValue(value * 10));
	return tr.commit();
}

template <class WriteTo>
Future<Void> addTestClearMutations(WriteTo& writeTo, Version version) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	appendVersionedMutation(versionedMutations, version, "class-A"_sr, "test_long"_sr, {});
	appendVersionedMutation(versionedMutations, version, "class-B"_sr, "test_long"_sr, {});
	return writeTo.addVersionedMutations(versionedMutations, version);
}

template <class WriteTo>
Future<Void> addTestGlobalSetMutation(WriteTo& writeTo, int64_t value) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	Version version = value;
	appendVersionedMutation(versionedMutations, version, "class-A"_sr, "test_long"_sr, longToValue(value));
	return writeTo.addVersionedMutations(versionedMutations, version);
}

template <class WriteTo>
Future<Void> addTestSetMutations(WriteTo& writeTo, int64_t value) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	Version version = value;
	appendVersionedMutation(versionedMutations, version, "class-A"_sr, "test_long"_sr, longToValue(value));
	appendVersionedMutation(versionedMutations, version, "class-B"_sr, "test_long"_sr, longToValue(value * 10));
	return writeTo.addVersionedMutations(versionedMutations, version);
}

ACTOR Future<Void> readConfigState(SimpleConfigTransaction* tr,
                                   Optional<int64_t> expected,
                                   bool immediate /* TODO : remove this */) {
	tr->fullReset();
	state Key configKeyA = encodeConfigKey("class-A"_sr, "test_long"_sr);
	state Key configKeyB = encodeConfigKey("class-B"_sr, "test_long"_sr);
	state Optional<Value> valueA = wait(tr->get(configKeyA));
	state Optional<Value> valueB = wait(tr->get(configKeyB));
	if (expected.present()) {
		ASSERT(valueA.get() == longToValue(expected.get()));
		ASSERT(valueB.get() == longToValue(expected.get() * 10));
	} else {
		ASSERT(!valueA.present());
		ASSERT(!valueB.present());
	}
	return Void();
}

ACTOR Future<Void> readConfigState(LocalConfiguration const* localConfiguration,
                                   Optional<int64_t> expected,
                                   bool immediate) {
	if (immediate) {
		if (expected.present()) {
			ASSERT_EQ(localConfiguration->getTestKnobs().TEST_LONG, expected.get());
		} else {
			ASSERT_EQ(localConfiguration->getTestKnobs().TEST_LONG, 0);
		}
		return Void();
	} else {
		loop {
			if (localConfiguration->getTestKnobs().TEST_LONG == (expected.present() ? expected.get() : 0)) {
				return Void();
			}
			wait(delayJittered(0.1));
		}
	}
}

template <class ConfigStore>
Future<Void> addTestGlobalSetMutation(ConfigStore& configStore, Version& lastWrittenVersion, ValueRef value) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	++lastWrittenVersion;
	appendVersionedMutation(versionedMutations, lastWrittenVersion, {}, "test_long"_sr, value);
	return configStore.addVersionedMutations(versionedMutations, lastWrittenVersion);
}

class LocalConfigEnvironment {
	LocalConfiguration localConfiguration;
	UID id;

public:
	LocalConfigEnvironment(std::string const& configPath, std::map<Key, Value> const& manualKnobOverrides)
	  : localConfiguration(configPath, manualKnobOverrides), id(deterministicRandom()->randomUniqueID()) {}

	Future<Void> setup() { return localConfiguration.initialize("./", id); }

	Future<Void> restart(std::string const& newConfigPath) {
		localConfiguration = LocalConfiguration(newConfigPath, {});
		return setup();
	}

	template <class TestType, class... Args>
	Future<Void> run(Args&&... args) {
		return TestType::run(localConfiguration, localConfiguration, std::forward<Args>(args)...);
	}
};

class BroadcasterToLocalConfigEnvironment {
	class DummyConfigSource {
		ConfigFollowerInterface cfi;
		ACTOR static Future<Void> serve(DummyConfigSource* self) {
			loop {
				choose {
					when(ConfigFollowerGetVersionRequest req = waitNext(self->cfi.getVersion.getFuture())) {
						req.reply.send(0);
					}
					when(ConfigFollowerGetSnapshotRequest req = waitNext(self->cfi.getSnapshot.getFuture())) {
						req.reply.send(ConfigFollowerGetSnapshotReply{});
					}
				}
			}
		}

	public:
		Future<Void> serve() { return serve(this); }
		ConfigFollowerInterface const& getInterface() { return cfi; }
	} dummyConfigSource;
	ConfigBroadcaster broadcaster;
	Reference<AsyncVar<ConfigFollowerInterface>> cfi;
	LocalConfiguration localConfiguration;
	ActorCollection actors{ false };

	ACTOR static Future<Void> setup(BroadcasterToLocalConfigEnvironment* self) {
		wait(self->localConfiguration.initialize("./", deterministicRandom()->randomUniqueID()));
		self->actors.add(self->dummyConfigSource.serve());
		self->actors.add(self->broadcaster.serve(self->cfi->get()));
		self->actors.add(
		    self->localConfiguration.consume(IDependentAsyncVar<ConfigFollowerInterface>::create(self->cfi)));
		return Void();
	}

public:
	BroadcasterToLocalConfigEnvironment()
	  : broadcaster(dummyConfigSource.getInterface(), deterministicRandom()->randomUniqueID()),
	    cfi(makeReference<AsyncVar<ConfigFollowerInterface>>()), localConfiguration("class-A", {}) {}

	Future<Void> setup() { return setup(this); }

	template <class TestType, class... Args>
	Future<Void> run(Args&&... args) {
		return waitOrError(TestType::run(broadcaster, localConfiguration, std::forward<Args>(args)...),
		                   actors.getResult());
	}
};

class TransactionEnvironment {
	ConfigTransactionInterface cti;
	SimpleConfigTransaction tr;
	SimpleConfigDatabaseNode node;
	ActorCollection actors{ false };

	ACTOR static Future<Void> setup(TransactionEnvironment* self) {
		wait(self->node.initialize("./", deterministicRandom()->randomUniqueID()));
		self->actors.add(self->node.serve(self->cti));
		return Void();
	}

public:
	TransactionEnvironment() : tr(cti) {}

	Future<Void> setup() { return setup(this); }

	template <class TestType, class... Args>
	Future<Void> run(Args&&... args) {
		return waitOrError(TestType::run(tr, tr, std::forward<Args>(args)...), actors.getResult());
	}
};

class TransactionToLocalConfigEnvironment {
	ConfigTransactionInterface cti;
	Reference<AsyncVar<ConfigFollowerInterface>> cfi;
	SimpleConfigTransaction tr;
	SimpleConfigDatabaseNode node;
	LocalConfiguration localConfiguration;
	ActorCollection actors{ false };

	ACTOR Future<Void> setup(TransactionToLocalConfigEnvironment* self) {
		wait(self->localConfiguration.initialize("./", deterministicRandom()->randomUniqueID()));
		wait(self->node.initialize("./", deterministicRandom()->randomUniqueID()));
		self->actors.add(self->node.serve(self->cti));
		self->actors.add(self->node.serve(self->cfi->get()));
		self->actors.add(
		    self->localConfiguration.consume(IDependentAsyncVar<ConfigFollowerInterface>::create(self->cfi)));
		return Void();
	}

public:
	TransactionToLocalConfigEnvironment()
	  : cfi(makeReference<AsyncVar<ConfigFollowerInterface>>()), tr(cti), localConfiguration("class-A", {}) {}

	Future<Void> setup() { return setup(this); }

	template <class TestType, class... Args>
	Future<Void> run(Args&&... args) {
		return waitOrError(TestType::run(tr, localConfiguration, std::forward<Args>(args)...), actors.getResult());
	}
};

class TestSet {
public:
	template <class WriteTo, class ReadFrom>
	static Future<Void> run(WriteTo& writeTo, ReadFrom& readFrom, int64_t expected, bool immediate) {
		std::function<Future<Void>(Void const&)> check = [&readFrom, expected, immediate](Void const&) {
			return readConfigState(&readFrom, expected, immediate);
		};
		return addTestSetMutations(writeTo, 1) >>= check;
	}
};

class TestClear {
public:
	template <class WriteTo, class ReadFrom>
	static Future<Void> run(WriteTo& writeTo, ReadFrom& readFrom, bool immediate) {
		std::function<Future<Void>(Void const&)> clear = [&writeTo](Void const&) {
			return addTestClearMutations(writeTo, 2);
		};
		std::function<Future<Void>(Void const&)> check = [&readFrom, immediate](Void const&) {
			return readConfigState(&readFrom, {}, immediate);
		};
		return (addTestSetMutations(writeTo, 1) >>= clear) >>= check;
	}
};

class TestGlobal {
public:
	template <class WriteTo>
	static Future<Void> run(WriteTo& writeTo, LocalConfiguration const& readFrom, bool immediate) {
		std::function<Future<Void>(Void const&)> globalWrite = [&writeTo](Void const&) {
			return addTestGlobalSetMutation(writeTo, 2);
		};
		std::function<Future<Void>(Void const&)> check = [&readFrom, immediate](Void const&) {
			return readConfigState(&readFrom, 2, immediate);
		};
		return (addTestSetMutations(writeTo, 1) >>= globalWrite) >>= check;
	}
};

} // namespace

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Set") {
	state LocalConfigEnvironment environment("class-A", std::map<Key, Value>{});
	wait(environment.setup());
	wait(environment.run<TestSet>(1, true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Clear") {
	state LocalConfigEnvironment environment("class-A", std::map<Key, Value>{});
	wait(environment.setup());
	wait(environment.run<TestClear>(true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/ManualOverride") {
	state LocalConfigEnvironment environment("class-A", std::map<Key, Value>{ { "test_long"_sr, "1000"_sr } });
	wait(environment.setup());
	wait(environment.run<TestSet>(1000, true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/ConflictingOverride") {
	state LocalConfigEnvironment environment("class-A/class-B", std::map<Key, Value>{});
	wait(environment.setup());
	wait(environment.run<TestSet>(10, true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Global") {
	state LocalConfigEnvironment environment("class-A", std::map<Key, Value>{});
	wait(environment.setup());
	wait(environment.run<TestGlobal>(true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Set") {
	state BroadcasterToLocalConfigEnvironment environment;
	wait(environment.setup());
	wait(environment.run<TestSet>(1, false));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Clear") {
	state BroadcasterToLocalConfigEnvironment environment;
	wait(environment.setup());
	wait(environment.run<TestClear>(false));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Global") {
	state BroadcasterToLocalConfigEnvironment environment;
	wait(environment.setup());
	wait(environment.run<TestGlobal>(false));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Set") {
	state TransactionEnvironment environment;
	wait(environment.setup());
	wait(environment.run<TestSet>(1, true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Clear") {
	state TransactionEnvironment environment;
	wait(environment.setup());
	wait(environment.run<TestClear>(true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/Set") {
	state TransactionToLocalConfigEnvironment environment;
	wait(environment.setup());
	wait(environment.run<TestSet>(1, false));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/Clear") {
	state TransactionToLocalConfigEnvironment environment;
	wait(environment.setup());
	wait(environment.run<TestClear>(false));
	return Void();
}
