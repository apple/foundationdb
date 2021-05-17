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

Future<Void> addTestClearMutation(SimpleConfigTransaction& tr, Optional<KeyRef> configClass) {
	tr.fullReset();
	auto configKey = encodeConfigKey(configClass, "test_long"_sr);
	tr.clear(configKey);
	return tr.commit();
}

Future<Void> addTestSetMutation(SimpleConfigTransaction& tr, Optional<KeyRef> configClass, int64_t value) {
	tr.fullReset();
	auto configKey = encodeConfigKey(configClass, "test_long"_sr);
	tr.set(configKey, longToValue(value));
	return tr.commit();
}

template <class WriteTo>
Future<Void> addTestClearMutation(WriteTo& writeTo, Optional<KeyRef> configKey, Version version) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	appendVersionedMutation(versionedMutations, version, configKey, "test_long"_sr, {});
	return writeTo.addVersionedMutations(versionedMutations, version);
}

template <class WriteTo>
Future<Void> addTestSetMutation(WriteTo& writeTo, Optional<KeyRef> configClass, int64_t value, Version version) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	appendVersionedMutation(versionedMutations, version, configClass, "test_long"_sr, longToValue(value));
	return writeTo.addVersionedMutations(versionedMutations, version);
}

ACTOR Future<Void> checkImmediate(SimpleConfigTransaction* tr,
                                  Optional<KeyRef> configClass,
                                  Optional<int64_t> expected) {
	tr->fullReset();
	state Key configKey = encodeConfigKey(configClass, "test_long"_sr);
	state Optional<Value> value = wait(tr->get(configKey));
	if (expected.present()) {
		ASSERT(value.get() == longToValue(expected.get()));
	} else {
		ASSERT(!value.present());
	}
	return Void();
}

void checkImmediate(LocalConfiguration const& localConfiguration, Optional<int64_t> expected) {
	if (expected.present()) {
		ASSERT_EQ(localConfiguration.getTestKnobs().TEST_LONG, expected.get());
	} else {
		ASSERT_EQ(localConfiguration.getTestKnobs().TEST_LONG, 0);
	}
}

ACTOR Future<Void> checkEventually(LocalConfiguration const* localConfiguration, Optional<int64_t> expected) {
	loop {
		if (localConfiguration->getTestKnobs().TEST_LONG == (expected.present() ? expected.get() : 0)) {
			return Void();
		}
		wait(delayJittered(0.1));
	}
}

class LocalConfigEnvironment {
	LocalConfiguration localConfiguration;
	UID id;
	Version lastWrittenVersion{ 0 };

public:
	LocalConfigEnvironment(std::string const& configPath, std::map<Key, Value> const& manualKnobOverrides)
	  : localConfiguration(configPath, manualKnobOverrides), id(deterministicRandom()->randomUniqueID()) {}
	Future<Void> setup() { return localConfiguration.initialize("./", id); }
	Future<Void> restart(std::string const& newConfigPath) {
		localConfiguration = LocalConfiguration(newConfigPath, {});
		return setup();
	}
	Future<Void> getError() const { return Never(); }
	Future<Void> clear(Optional<KeyRef> configClass) {
		return addTestClearMutation(localConfiguration, configClass, ++lastWrittenVersion);
	}
	Future<Void> set(Optional<KeyRef> configClass, int64_t value) {
		return addTestSetMutation(localConfiguration, configClass, value, ++lastWrittenVersion);
	}
	void check(Optional<int64_t> value) const { checkImmediate(localConfiguration, value); }
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
	Version lastWrittenVersion{ 0 };
	Future<Void> broadcastServer;
	ActorCollection actors{ false };

	ACTOR static Future<Void> setup(BroadcasterToLocalConfigEnvironment* self) {
		wait(self->localConfiguration.initialize("./", deterministicRandom()->randomUniqueID()));
		self->actors.add(self->dummyConfigSource.serve());
		self->broadcastServer = self->broadcaster.serve(self->cfi->get());
		self->actors.add(
		    self->localConfiguration.consume(IDependentAsyncVar<ConfigFollowerInterface>::create(self->cfi)));
		return Void();
	}

	ACTOR static Future<Void> compact(BroadcasterToLocalConfigEnvironment* self) {
		ConfigFollowerGetVersionReply reply =
		    wait(self->cfi->get().getVersion.getReply(ConfigFollowerGetVersionRequest{}));
		wait(self->cfi->get().compact.getReply(ConfigFollowerCompactRequest{ reply.version }));
		return Void();
	}

public:
	BroadcasterToLocalConfigEnvironment(std::string const& configPath)
	  : broadcaster(dummyConfigSource.getInterface(), deterministicRandom()->randomUniqueID()),
	    cfi(makeReference<AsyncVar<ConfigFollowerInterface>>()), localConfiguration(configPath, {}) {}

	Future<Void> setup() { return setup(this); }

	Future<Void> set(Optional<KeyRef> configClass, int64_t value) {
		return addTestSetMutation(localConfiguration, configClass, value, ++lastWrittenVersion);
	}
	Future<Void> clear(Optional<KeyRef> configClass) {
		return addTestClearMutation(localConfiguration, configClass, ++lastWrittenVersion);
	}
	Future<Void> check(Optional<int64_t> value) const { return checkEventually(&localConfiguration, value); }
	void changeBroadcaster() {
		cfi->set(ConfigFollowerInterface());
		broadcaster = ConfigBroadcaster(dummyConfigSource.getInterface(), deterministicRandom()->randomUniqueID());
		broadcastServer = broadcaster.serve(cfi->get());
	}
	Future<Void> compact() { return compact(this); }

	Future<Void> getError() const { return actors.getResult() || broadcastServer; }
};

class TransactionEnvironment {
	ConfigTransactionInterface cti;
	SimpleConfigTransaction tr;
	Reference<IConfigDatabaseNode> node;
	Future<Void> server;
	UID id;

	ACTOR static Future<Void> setup(TransactionEnvironment* self) {
		wait(self->node->initialize("./", self->id));
		self->server = self->node->serve(self->cti);
		return Void();
	}

public:
	TransactionEnvironment()
	  : tr(cti), node(makeReference<SimpleConfigDatabaseNode>()), id(deterministicRandom()->randomUniqueID()) {}

	Future<Void> setup() { return setup(this); }

	Future<Void> restart() {
		server.cancel();
		node = makeReference<SimpleConfigDatabaseNode>();
		return setup();
	}

	Future<Void> set(Optional<KeyRef> configClass, int64_t value) { return addTestSetMutation(tr, configClass, value); }
	Future<Void> clear(Optional<KeyRef> configClass) { return addTestClearMutation(tr, configClass); }
	Future<Void> check(Optional<KeyRef> configClass, Optional<int64_t> expected) {
		return checkImmediate(&tr, configClass, expected);
	}
	Future<Void> getError() const { return server; }
};

class TransactionToLocalConfigEnvironment {
	ConfigTransactionInterface cti;
	Reference<AsyncVar<ConfigFollowerInterface>> cfi;
	SimpleConfigTransaction tr;
	Reference<IConfigDatabaseNode> node;
	UID nodeID;
	Future<Void> ctiServer;
	Future<Void> cfiServer;
	LocalConfiguration localConfiguration;
	Future<Void> consumer;

	ACTOR static Future<Void> setup(TransactionToLocalConfigEnvironment* self) {
		wait(self->localConfiguration.initialize("./", deterministicRandom()->randomUniqueID()));
		wait(setupNode(self));
		self->consumer =
		    self->localConfiguration.consume(IDependentAsyncVar<ConfigFollowerInterface>::create(self->cfi));
		return Void();
	}

	ACTOR static Future<Void> setupNode(TransactionToLocalConfigEnvironment* self) {
		wait(self->node->initialize("./", self->nodeID));
		self->ctiServer = self->node->serve(self->cti);
		self->cfiServer = self->node->serve(self->cfi->get());
		return Void();
	}

public:
	TransactionToLocalConfigEnvironment(std::string const& configPath)
	  : cfi(makeReference<AsyncVar<ConfigFollowerInterface>>()), tr(cti),
	    node(makeReference<SimpleConfigDatabaseNode>()), nodeID(deterministicRandom()->randomUniqueID()),
	    localConfiguration(configPath, {}) {}

	Future<Void> setup() { return setup(this); }

	Future<Void> restartNode() {
		cfiServer.cancel();
		ctiServer.cancel();
		node = makeReference<SimpleConfigDatabaseNode>();
		return setupNode(this);
	}

	Future<Void> set(Optional<KeyRef> configClass, int64_t value) { return addTestSetMutation(tr, configClass, value); }
	Future<Void> clear(Optional<KeyRef> configClass) { return addTestClearMutation(tr, configClass); }
	Future<Void> check(Optional<int64_t> value) { return checkEventually(&localConfiguration, value); }
	Future<Void> getError() const { return ctiServer || cfiServer || consumer; }
};

template <class Env, class... Args>
Future<Void> set(Env& env, Args&&... args) {
	return waitOrError(env.set(std::forward<Args>(args)...), env.getError());
}
template <class Env, class... Args>
Future<Void> clear(Env& env, Args&&... args) {
	return waitOrError(env.clear(std::forward<Args>(args)...), env.getError());
}
template <class Env, class... Args>
Future<Void> check(Env& env, Args&&... args) {
	return waitOrError(env.check(std::forward<Args>(args)...), env.getError());
}

} // namespace

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Set") {
	state LocalConfigEnvironment env("class-A", {});
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	env.check(1);
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Restart") {
	state LocalConfigEnvironment env("class-A/class-B", {});
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(env.restart("class-A/class-B"));
	env.check(1);
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/RestartFresh") {
	state LocalConfigEnvironment env("class-A/class-B", {});
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(env.restart("class-B/class-A"));
	env.check(Optional<int64_t>{});
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Clear") {
	state LocalConfigEnvironment env("class-A", {});
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(clear(env, "class-A"_sr));
	env.check(Optional<int64_t>{});
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/GlobalSet") {
	state LocalConfigEnvironment env("class-A", {});
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(set(env, Optional<KeyRef>{}, 10));
	env.check(10);
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/ConflictingOverrides") {
	state LocalConfigEnvironment env("class-A/class-B", {});
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(set(env, "class-B"_sr, 10));
	env.check(10);
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Manual") {
	state LocalConfigEnvironment env("class-A", { { "test_long"_sr, "1000"_sr } });
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	env.check(1000);
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Set") {
	state BroadcasterToLocalConfigEnvironment env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(check(env, 1));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Clear") {
	state BroadcasterToLocalConfigEnvironment env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(clear(env, "class-A"_sr));
	wait(check(env, Optional<int64_t>{}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Ignore") {
	state BroadcasterToLocalConfigEnvironment env("class-A");
	wait(env.setup());
	wait(set(env, "class-B"_sr, 1));
	choose {
		when(wait(delay(5))) {}
		when(wait(check(env, 1))) { ASSERT(false); }
	}
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/GlobalSet") {
	state BroadcasterToLocalConfigEnvironment env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(set(env, Optional<KeyRef>{}, 10));
	wait(check(env, 10));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/ChangeBroadcaster") {
	state BroadcasterToLocalConfigEnvironment env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(check(env, 1));
	env.changeBroadcaster();
	wait(set(env, "class-A"_sr, 2));
	wait(check(env, 2));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Compact") {
	state BroadcasterToLocalConfigEnvironment env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(env.compact());
	wait(check(env, 1));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/Set") {
	state TransactionToLocalConfigEnvironment env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(check(env, 1));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/Clear") {
	state TransactionToLocalConfigEnvironment env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(clear(env, "class-A"_sr));
	wait(check(env, Optional<int64_t>{}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/GlobalSet") {
	state TransactionToLocalConfigEnvironment env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(set(env, Optional<KeyRef>{}, 10));
	wait(check(env, 10));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/RestartNode") {
	state TransactionToLocalConfigEnvironment env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(env.restartNode());
	wait(check(env, 1));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Set") {
	state TransactionEnvironment env;
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(check(env, "class-A"_sr, 1));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Clear") {
	state TransactionEnvironment env;
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(clear(env, "class-A"_sr));
	wait(check(env, "class-A"_sr, Optional<int64_t>{}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Restart") {
	state TransactionEnvironment env;
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(env.restart());
	wait(check(env, "class-A"_sr, 1));
	return Void();
}
