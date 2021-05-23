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
#include "fdbclient/IConfigTransaction.h"
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

Future<Void> addTestClearMutation(IConfigTransaction& tr, Optional<KeyRef> configClass) {
	tr.fullReset();
	auto configKey = encodeConfigKey(configClass, "test_long"_sr);
	tr.clear(configKey);
	return tr.commit();
}

Future<Void> addTestClearMutation(LocalConfiguration& localConfiguration, Optional<KeyRef> configKey, Version version) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	appendVersionedMutation(versionedMutations, version, configKey, "test_long"_sr, {});
	return localConfiguration.addChanges(versionedMutations, version);
}

void addTestClearMutation(ConfigBroadcaster& broadcaster, Optional<KeyRef> configClass, Version version) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	appendVersionedMutation(versionedMutations, version, configClass, "test_long"_sr, {});
	broadcaster.applyChanges(versionedMutations, version);
}

Future<Void> addTestSetMutation(IConfigTransaction& tr, Optional<KeyRef> configClass, int64_t value) {
	tr.fullReset();
	auto configKey = encodeConfigKey(configClass, "test_long"_sr);
	tr.set(configKey, longToValue(value));
	return tr.commit();
}

Future<Void> addTestSetMutation(LocalConfiguration& localConfiguration,
                                Optional<KeyRef> configClass,
                                int64_t value,
                                Version version) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	appendVersionedMutation(versionedMutations, version, configClass, "test_long"_sr, longToValue(value));
	return localConfiguration.addChanges(versionedMutations, version);
}

void addTestSetMutation(ConfigBroadcaster& broadcaster, Optional<KeyRef> configClass, int64_t value, Version version) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	appendVersionedMutation(versionedMutations, version, configClass, "test_long"_sr, longToValue(value));
	broadcaster.applyChanges(versionedMutations, version);
}

ACTOR Future<Void> checkImmediate(IConfigTransaction* tr, Optional<KeyRef> configClass, Optional<int64_t> expected) {
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

Future<Void> checkImmediate(IConfigTransaction& tr, Optional<KeyRef> configClass, Optional<int64_t> expected) {
	return checkImmediate(&tr, configClass, expected);
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
	LocalConfigEnvironment(std::string const& configPath,
	                       std::map<std::string, std::string> const& manualKnobOverrides = {})
	  : localConfiguration(configPath, manualKnobOverrides), id(deterministicRandom()->randomUniqueID()) {}
	Future<Void> setup() { return localConfiguration.initialize("./", id); }
	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
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
	ConfigBroadcaster broadcaster;
	Reference<AsyncVar<ConfigBroadcastFollowerInterface>> cbfi;
	LocalConfiguration localConfiguration;
	Version lastWrittenVersion{ 0 };
	Future<Void> broadcastServer;
	Future<Void> configConsumer;
	UID localConfigID;

	ACTOR static Future<Void> setup(BroadcasterToLocalConfigEnvironment* self) {
		self->broadcastServer = self->broadcaster.serve(self->cbfi->get());
		wait(setupLocalConfig(self));
		return Void();
	}

	ACTOR static Future<Void> setupLocalConfig(BroadcasterToLocalConfigEnvironment* self) {
		wait(self->localConfiguration.initialize("./", self->localConfigID));
		self->configConsumer =
		    self->localConfiguration.consume(IDependentAsyncVar<ConfigBroadcastFollowerInterface>::create(self->cbfi));
		return Void();
	}

public:
	BroadcasterToLocalConfigEnvironment(std::string const& configPath)
	  : broadcaster(ConfigFollowerInterface{}), cbfi(makeReference<AsyncVar<ConfigBroadcastFollowerInterface>>()),
	    localConfigID(deterministicRandom()->randomUniqueID()), localConfiguration(configPath, {}) {}

	Future<Void> setup() { return setup(this); }

	void set(Optional<KeyRef> configClass, int64_t value) {
		addTestSetMutation(broadcaster, configClass, value, ++lastWrittenVersion);
	}

	void clear(Optional<KeyRef> configClass) { addTestClearMutation(broadcaster, configClass, ++lastWrittenVersion); }

	Future<Void> check(Optional<int64_t> value) const { return checkEventually(&localConfiguration, value); }

	void changeBroadcaster() {
		broadcastServer.cancel();
		cbfi->set(ConfigBroadcastFollowerInterface{});
		broadcastServer = broadcaster.serve(cbfi->get());
	}

	Future<Void> restartLocalConfig(std::string const& configPath) {
		localConfiguration = LocalConfiguration(configPath, {});
		return setupLocalConfig(this);
	}

	void compact() { broadcaster.compact(); }

	Future<Void> getError() const { return configConsumer || broadcastServer; }
};

class TransactionEnvironment {
	ConfigTransactionInterface cti;
	Reference<IConfigTransaction> tr;
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
	  : tr(IConfigTransaction::createSimple(cti)), node(IConfigDatabaseNode::createSimple()),
	    id(deterministicRandom()->randomUniqueID()) {}

	Future<Void> setup() { return setup(this); }

	Future<Void> restartNode() {
		server.cancel();
		node = IConfigDatabaseNode::createSimple();
		return setup();
	}

	Future<Void> set(Optional<KeyRef> configClass, int64_t value) {
		return addTestSetMutation(*tr, configClass, value);
	}
	Future<Void> clear(Optional<KeyRef> configClass) { return addTestClearMutation(*tr, configClass); }
	Future<Void> check(Optional<KeyRef> configClass, Optional<int64_t> expected) {
		return checkImmediate(*tr, configClass, expected);
	}
	Future<Void> getError() const { return server; }
};

class TransactionToLocalConfigEnvironment {
	ConfigTransactionInterface cti;
	ConfigFollowerInterface nodeToBroadcaster;
	Reference<IConfigTransaction> tr;
	Reference<AsyncVar<ConfigBroadcastFollowerInterface>> broadcasterToLocalConfig;
	Reference<IConfigDatabaseNode> node;
	UID nodeID;
	UID localConfigID;
	Future<Void> ctiServer;
	Future<Void> cfiServer;
	LocalConfiguration localConfiguration;
	ConfigBroadcaster broadcaster;
	Future<Void> broadcastServer;
	Future<Void> localConfigConsumer;

	ACTOR static Future<Void> setup(TransactionToLocalConfigEnvironment* self) {
		wait(setupNode(self));
		wait(setupLocalConfig(self));
		self->broadcastServer = self->broadcaster.serve(self->broadcasterToLocalConfig->get());
		return Void();
	}

	ACTOR static Future<Void> setupLocalConfig(TransactionToLocalConfigEnvironment* self) {
		wait(self->localConfiguration.initialize("./", self->localConfigID));
		self->localConfigConsumer = self->localConfiguration.consume(
		    IDependentAsyncVar<ConfigBroadcastFollowerInterface>::create(self->broadcasterToLocalConfig));
		return Void();
	}

	ACTOR static Future<Void> setupNode(TransactionToLocalConfigEnvironment* self) {
		wait(self->node->initialize("./", self->nodeID));
		self->ctiServer = self->node->serve(self->cti);
		self->cfiServer = self->node->serve(self->nodeToBroadcaster);
		return Void();
	}

public:
	TransactionToLocalConfigEnvironment(std::string const& configPath)
	  : broadcasterToLocalConfig(makeReference<AsyncVar<ConfigBroadcastFollowerInterface>>()),
	    node(IConfigDatabaseNode::createSimple()), tr(IConfigTransaction::createSimple(cti)),
	    nodeID(deterministicRandom()->randomUniqueID()), localConfigID(deterministicRandom()->randomUniqueID()),
	    localConfiguration(configPath, {}), broadcaster(nodeToBroadcaster) {}

	Future<Void> setup() { return setup(this); }

	Future<Void> restartNode() {
		cfiServer.cancel();
		ctiServer.cancel();
		node = IConfigDatabaseNode::createSimple();
		return setupNode(this);
	}

	void changeBroadcaster() {
		broadcastServer.cancel();
		broadcasterToLocalConfig->set(ConfigBroadcastFollowerInterface{});
		broadcastServer = broadcaster.serve(broadcasterToLocalConfig->get());
	}

	Future<Void> restartLocalConfig(std::string const& configPath) {
		localConfiguration = LocalConfiguration(configPath, {});
		return setupLocalConfig(this);
	}

	Future<Void> set(Optional<KeyRef> configClass, int64_t value) {
		return addTestSetMutation(*tr, configClass, value);
	}
	Future<Void> clear(Optional<KeyRef> configClass) { return addTestClearMutation(*tr, configClass); }
	Future<Void> check(Optional<int64_t> value) { return checkEventually(&localConfiguration, value); }
	Future<Void> getError() const { return ctiServer || cfiServer || localConfigConsumer || broadcastServer; }
};

template <class Env, class... Args>
Future<Void> set(Env& env, Args&&... args) {
	return waitOrError(env.set(std::forward<Args>(args)...), env.getError());
}
template <class... Args>
Future<Void> set(BroadcasterToLocalConfigEnvironment& env, Args&&... args) {
	env.set(std::forward<Args>(args)...);
	return Void();
}
template <class Env, class... Args>
Future<Void> clear(Env& env, Args&&... args) {
	return waitOrError(env.clear(std::forward<Args>(args)...), env.getError());
}
template <class... Args>
Future<Void> clear(BroadcasterToLocalConfigEnvironment& env, Args&&... args) {
	env.clear(std::forward<Args>(args)...);
	return Void();
}
template <class Env, class... Args>
Future<Void> check(Env& env, Args&&... args) {
	return waitOrError(env.check(std::forward<Args>(args)...), env.getError());
}
template <class... Args>
Future<Void> check(LocalConfigEnvironment& env, Args&&... args) {
	env.check(std::forward<Args>(args)...);
	return Void();
}

ACTOR template <class Env>
Future<Void> testRestartLocalConfig() {
	state Env env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(check(env, 1));
	wait(env.restartLocalConfig("class-A"));
	wait(check(env, 1));
	wait(set(env, "class-A"_sr, 2));
	wait(check(env, 2));
	return Void();
}

ACTOR template <class Env>
Future<Void> testRestartLocalConfigAndChangeClass() {
	state Env env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(check(env, 1));
	wait(env.restartLocalConfig("class-B"));
	wait(check(env, 0));
	wait(set(env, "class-B"_sr, 2));
	wait(check(env, 2));
	return Void();
}

ACTOR template <class Env>
Future<Void> testSet() {
	state LocalConfigEnvironment env("class-A", {});
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(check(env, 1));
	return Void();
}

ACTOR template <class Env>
Future<Void> testClear() {
	state LocalConfigEnvironment env("class-A", {});
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(clear(env, "class-A"_sr));
	wait(check(env, Optional<int64_t>{}));
	return Void();
}

ACTOR template <class Env>
Future<Void> testGlobalSet() {
	state Env env("class-A");
	wait(env.setup());
	wait(set(env, Optional<KeyRef>{}, 1));
	env.check(1);
	wait(set(env, "class-A"_sr, 10));
	env.check(10);
	return Void();
}

ACTOR template <class Env>
Future<Void> testIgnore() {
	state Env env("class-A");
	wait(env.setup());
	wait(set(env, "class-B"_sr, 1));
	choose {
		when(wait(delay(5))) {}
		when(wait(check(env, 1))) { ASSERT(false); }
	}
	return Void();
}

ACTOR template <class Env>
Future<Void> testCompact() {
	state Env env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	env.compact();
	wait(check(env, 1));
	return Void();
}

ACTOR template <class Env>
Future<Void> testChangeBroadcaster() {
	state Env env("class-A");
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(check(env, 1));
	env.changeBroadcaster();
	wait(set(env, "class-A"_sr, 2));
	wait(check(env, 2));
	return Void();
}

} // namespace

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Set") {
	wait(testSet<LocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Restart") {
	wait(testRestartLocalConfig<LocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/RestartFresh") {
	wait(testRestartLocalConfigAndChangeClass<LocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Clear") {
	wait(testClear<LocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/GlobalSet") {
	wait(testGlobalSet<LocalConfigEnvironment>());
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
	state LocalConfigEnvironment env("class-A", { { "test_long", "1000" } });
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	env.check(1000);
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Set") {
	wait(testSet<BroadcasterToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Clear") {
	wait(testClear<BroadcasterToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Ignore") {
	wait(testIgnore<BroadcasterToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/GlobalSet") {
	wait(testGlobalSet<BroadcasterToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/ChangeBroadcaster") {
	wait(testChangeBroadcaster<BroadcasterToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/RestartLocalConfig") {
	wait(testRestartLocalConfig<BroadcasterToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/RestartLocalConfigAndChangeClass") {
	wait(testRestartLocalConfigAndChangeClass<BroadcasterToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Compact") {
	wait(testCompact<BroadcasterToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/Set") {
	wait(testSet<TransactionToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/Clear") {
	wait(testClear<TransactionToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/GlobalSet") {
	wait(testGlobalSet<TransactionToLocalConfigEnvironment>());
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

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/ChangeBroadcaster") {
	wait(testChangeBroadcaster<TransactionToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/RestartLocalConfig") {
	wait(testRestartLocalConfig<TransactionToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/RestartLocalConfigAndChangeClass") {
	wait(testRestartLocalConfigAndChangeClass<TransactionToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Set") {
	wait(testSet<TransactionEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Clear") {
	wait(testClear<TransactionEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Restart") {
	state TransactionEnvironment env;
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(env.restartNode());
	wait(check(env, "class-A"_sr, 1));
	return Void();
}
