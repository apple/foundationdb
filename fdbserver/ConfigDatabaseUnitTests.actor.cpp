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

Future<Void> addTestClearMutations(SimpleConfigTransaction& tr) {
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

ACTOR Future<Void> readConfigState(SimpleConfigTransaction* tr, Optional<int64_t> expected) {
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

template <bool immediate>
Future<Void> readConfigState(LocalConfiguration const* localConfiguration, int64_t expected) {
	if constexpr (immediate) {
		ASSERT_EQ(localConfiguration->getTestKnobs().TEST_LONG, expected);
		return Void();
	} else {
		return waitUntil(
		    [localConfiguration, expected] { return localConfiguration->getTestKnobs().TEST_LONG == expected; });
	}
}

template <class ConfigStore>
Future<Void> addTestGlobalSetMutation(ConfigStore& configStore, Version& lastWrittenVersion, ValueRef value) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	++lastWrittenVersion;
	appendVersionedMutation(versionedMutations, lastWrittenVersion, {}, "test_long"_sr, value);
	return configStore.addVersionedMutations(versionedMutations, lastWrittenVersion);
}

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
};

ACTOR template <class F>
Future<Void> runLocalConfigEnvironment(std::string configPath, std::map<Key, Value> manualKnobOverrides, F f) {
	state LocalConfiguration localConfiguration(configPath, manualKnobOverrides);
	state Version lastWrittenVersion = 0;
	wait(localConfiguration.initialize("./", deterministicRandom()->randomUniqueID()));
	wait(f(localConfiguration));
	return Void();
}

ACTOR template <class F>
Future<Void> runBroadcasterToLocalConfigEnvironment(F f) {
	state DummyConfigSource dummyConfigSource;
	state ConfigBroadcaster broadcaster(dummyConfigSource.getInterface(), deterministicRandom()->randomUniqueID());
	state Reference<AsyncVar<ConfigFollowerInterface>> cfi = makeReference<AsyncVar<ConfigFollowerInterface>>();
	state LocalConfiguration localConfigurationA("class-A", {});
	state LocalConfiguration localConfigurationB("class-B", {});
	state ActorCollection actors(false);
	wait(localConfigurationA.initialize("./", deterministicRandom()->randomUniqueID()));
	wait(localConfigurationB.initialize("./", deterministicRandom()->randomUniqueID()));
	actors.add(dummyConfigSource.serve());
	actors.add(broadcaster.serve(cfi->get()));
	actors.add(localConfigurationA.consume(IDependentAsyncVar<ConfigFollowerInterface>::create(cfi)));
	actors.add(localConfigurationB.consume(IDependentAsyncVar<ConfigFollowerInterface>::create(cfi)));
	wait(waitOrError(f(broadcaster, localConfigurationA, localConfigurationB), actors.getResult()));
	return Void();
}

ACTOR template <class F>
Future<Void> runTransactionToLocalConfigEnvironment(F f) {
	state ConfigTransactionInterface cti;
	state Reference<AsyncVar<ConfigFollowerInterface>> cfi = makeReference<AsyncVar<ConfigFollowerInterface>>();
	state SimpleConfigTransaction tr(cti);
	state SimpleConfigDatabaseNode node;
	state ActorCollection actors(false);
	state LocalConfiguration localConfigurationA("class-A", {});
	state LocalConfiguration localConfigurationB("class-B", {});
	wait(localConfigurationA.initialize("./", deterministicRandom()->randomUniqueID()));
	wait(localConfigurationB.initialize("./", deterministicRandom()->randomUniqueID()));
	wait(node.initialize("./", deterministicRandom()->randomUniqueID()));
	actors.add(node.serve(cti));
	actors.add(node.serve(cfi->get()));
	actors.add(localConfigurationA.consume(IDependentAsyncVar<ConfigFollowerInterface>::create(cfi)));
	actors.add(localConfigurationB.consume(IDependentAsyncVar<ConfigFollowerInterface>::create(cfi)));
	wait(waitOrError(f(tr, localConfigurationA, localConfigurationB), actors.getResult()));
	return Void();
}

ACTOR template <class F>
Future<Void> runTransactionEnvironment(F f) {
	state ConfigTransactionInterface cti;
	state SimpleConfigTransaction tr(cti);
	state SimpleConfigDatabaseNode node;
	state ActorCollection actors(false);
	wait(node.initialize("./", deterministicRandom()->randomUniqueID()));
	actors.add(node.serve(cti));
	wait(waitOrError(f(tr), actors.getResult()));
	return Void();
}

ACTOR template <class F>
Future<Void> waitUntil(F isReady) {
	loop {
		if (isReady()) {
			return Void();
		}
		wait(delayJittered(0.1));
	}
}

} // namespace

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/ManualOverride") {
	wait(runLocalConfigEnvironment("class-A", { { "test_long"_sr, "1000"_sr } }, [](auto& conf) {
		std::function<Future<Void>(Void const&)> check = [&conf](Void const&) {
			return readConfigState<true>(&conf, 1000);
		};
		return addTestSetMutations(conf, 1) >>= check;
	}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/ConflictingOverride") {
	wait(runLocalConfigEnvironment("class-A/class-B", {}, [](auto& conf) {
		std::function<Future<Void>(Void const&)> check = [&conf](Void const&) {
			return readConfigState<true>(&conf, 10);
		};
		return addTestSetMutations(conf, 1) >>= check;
	}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Clear") {
	wait(runLocalConfigEnvironment("class-A/class-B", {}, [](auto& conf) {
		std::function<Future<Void>(Void const&)> clear = [&conf](Void const&) {
			return addTestClearMutations(conf, 2);
		};
		std::function<Future<Void>(Void const&)> check = [&conf](Void const&) {
			return readConfigState<true>(&conf, 0);
		};
		return (addTestSetMutations(conf, 1) >>= clear) >>= check;
	}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Global") {
	wait(runLocalConfigEnvironment("class-A", {}, [](auto& conf) {
		std::function<Future<Void>(Void const&)> setGlobal = [&conf](Void const&) {
			return addTestGlobalSetMutation(conf, 2);
		};
		std::function<Future<Void>(Void const&)> check = [&conf](Void const&) {
			return readConfigState<true>(&conf, 2);
		};
		return (addTestSetMutations(conf, 1) >>= setGlobal) >>= check;
	}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Set") {
	wait(runBroadcasterToLocalConfigEnvironment([](auto& broadcaster, auto& confA, auto& confB) {
		std::function<Future<Void>(Void const&)> check = [&confA, &confB](Void const&) {
			return readConfigState<false>(&confA, 1) && readConfigState<false>(&confB, 10);
		};
		return addTestSetMutations(broadcaster, 1) >>= check;
	}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Clear") {
	wait(runBroadcasterToLocalConfigEnvironment([](auto& broadcaster, auto& confA, auto& confB) {
		std::function<Future<Void>(Void const&)> clear = [&broadcaster](Void const&) {
			return addTestClearMutations(broadcaster, 2);
		};
		std::function<Future<Void>(Void const&)> check = [&confA, &confB](Void const&) {
			return readConfigState<false>(&confA, 0) && readConfigState<false>(&confB, 0);
		};
		return (addTestSetMutations(broadcaster, 1) >>= clear) >>= check;
	}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Set") {
	wait(runTransactionEnvironment([](auto& tr) {
		std::function<Future<Void>(Void const&)> check = [&tr](Void const&) { return readConfigState(&tr, 1); };
		return addTestSetMutations(tr, 1) >>= check;
	}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Clear") {
	wait(runTransactionEnvironment([](auto& tr) {
		std::function<Future<Void>(Void const&)> clear = [&tr](Void const&) { return addTestClearMutations(tr); };
		std::function<Future<Void>(Void const&)> check = [&tr](Void const&) { return readConfigState(&tr, {}); };
		return (addTestSetMutations(tr, 1) >>= clear) >>= check;
	}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/Set") {
	wait(runTransactionToLocalConfigEnvironment([](auto& tr, auto const& confA, auto const& confB) {
		std::function<Future<Void>(Void const&)> check = [&confA, &confB](Void const&) {
			return readConfigState<false>(&confA, 1) && readConfigState<false>(&confB, 10);
		};
		return addTestSetMutations(tr, 1) >>= check;
	}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/Clear") {
	wait(runTransactionToLocalConfigEnvironment([](auto& tr, auto const& confA, auto const& confB) {
		std::function<Future<Void>(Void const&)> clear = [&tr](Void const&) { return addTestClearMutations(tr); };
		std::function<Future<Void>(Void const&)> check = [&confA, &confB](Void const&) {
			return readConfigState<false>(&confA, 0) && readConfigState<false>(&confB, 0);
		};
		return (addTestSetMutations(tr, 1) >>= clear) >>= check;
	}));
	return Void();
}
