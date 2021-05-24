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

// TODO: Use newly public ConfigMutationRef constructor
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

class WriteToTransactionEnvironment {
	UID id;
	ConfigTransactionInterface cti;
	ConfigFollowerInterface cfi;
	Reference<IConfigDatabaseNode> node;
	Future<Void> ctiServer;
	Future<Void> cfiServer;
	Version lastWrittenVersion{ 0 };

	ACTOR static Future<Void> set(WriteToTransactionEnvironment* self, Optional<KeyRef> configClass, int64_t value) {
		state Reference<IConfigTransaction> tr = IConfigTransaction::createSimple(self->cti);
		auto configKey = encodeConfigKey(configClass, "test_long"_sr);
		tr->set(configKey, longToValue(value));
		wait(tr->commit());
		self->lastWrittenVersion = tr->getCommittedVersion();
		return Void();
	}

	ACTOR static Future<Void> clear(WriteToTransactionEnvironment* self, Optional<KeyRef> configClass) {
		state Reference<IConfigTransaction> tr = IConfigTransaction::createSimple(self->cti);
		auto configKey = encodeConfigKey(configClass, "test_long"_sr);
		tr->clear(configKey);
		wait(tr->commit());
		self->lastWrittenVersion = tr->getCommittedVersion();
		return Void();
	}

	void setup() {
		ctiServer = node->serve(cti);
		cfiServer = node->serve(cfi);
	}

public:
	WriteToTransactionEnvironment()
	  : id(deterministicRandom()->randomUniqueID()), node(IConfigDatabaseNode::createSimple("./", id)) {
		setup();
	}

	Future<Void> set(Optional<KeyRef> configClass, int64_t value) { return set(this, configClass, value); }

	Future<Void> clear(Optional<KeyRef> configClass) { return clear(this, configClass); }

	Future<Void> compact() { return cfi.compact.getReply(ConfigFollowerCompactRequest{ lastWrittenVersion }); }

	void restartNode() {
		cfiServer.cancel();
		ctiServer.cancel();
		node = IConfigDatabaseNode::createSimple("./", id);
		setup();
	}

	ConfigTransactionInterface getTransactionInterface() const { return cti; }

	ConfigFollowerInterface getFollowerInterface() const { return cfi; }

	Future<Void> getError() const { return cfiServer || ctiServer; }
};

class ReadFromLocalConfigEnvironment {
	UID id;
	LocalConfiguration localConfiguration;
	Reference<IDependentAsyncVar<ConfigBroadcastFollowerInterface> const> cbfi;
	Future<Void> consumer;

	ACTOR static Future<Void> checkEventually(LocalConfiguration const* localConfiguration,
	                                          Optional<int64_t> expected) {
		loop {
			if (localConfiguration->getTestKnobs().TEST_LONG == (expected.present() ? expected.get() : 0)) {
				return Void();
			}
			wait(delayJittered(0.1));
		}
	}

	ACTOR static Future<Void> setup(ReadFromLocalConfigEnvironment* self) {
		wait(self->localConfiguration.initialize());
		if (self->cbfi) {
			self->consumer = self->localConfiguration.consume(self->cbfi);
		}
		return Void();
	}

public:
	ReadFromLocalConfigEnvironment(std::string const& configPath,
	                               std::map<std::string, std::string> const& manualKnobOverrides)
	  : id(deterministicRandom()->randomUniqueID()), localConfiguration("./", configPath, manualKnobOverrides, id),
	    consumer(Never()) {}

	Future<Void> setup() { return setup(this); }

	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
		localConfiguration = LocalConfiguration("./", newConfigPath, {}, id);
		return setup();
	}

	void connectToBroadcaster(Reference<IDependentAsyncVar<ConfigBroadcastFollowerInterface> const> const& cbfi) {
		ASSERT(!this->cbfi);
		this->cbfi = cbfi;
		consumer = localConfiguration.consume(cbfi);
	}

	void checkImmediate(Optional<int64_t> expected) const {
		if (expected.present()) {
			ASSERT_EQ(localConfiguration.getTestKnobs().TEST_LONG, expected.get());
		} else {
			ASSERT_EQ(localConfiguration.getTestKnobs().TEST_LONG, 0);
		}
	}

	Future<Void> checkEventually(Optional<int64_t> expected) const {
		return checkEventually(&localConfiguration, expected);
	}

	LocalConfiguration& getMutableLocalConfiguration() { return localConfiguration; }

	Future<Void> getError() const { return consumer; }
};

class LocalConfigEnvironment {
	ReadFromLocalConfigEnvironment readFrom;
	Version lastWrittenVersion{ 0 };

	Future<Void> addMutation(Optional<KeyRef> configClass, Optional<ValueRef> value) {
		Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
		appendVersionedMutation(versionedMutations, ++lastWrittenVersion, configClass, "test_long"_sr, value);
		return readFrom.getMutableLocalConfiguration().addChanges(versionedMutations, lastWrittenVersion);
	}

public:
	LocalConfigEnvironment(std::string const& configPath,
	                       std::map<std::string, std::string> const& manualKnobOverrides = {})
	  : readFrom(configPath, manualKnobOverrides) {}
	Future<Void> setup() { return readFrom.setup(); }
	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
		return readFrom.restartLocalConfig(newConfigPath);
	}
	Future<Void> getError() const { return Never(); }
	Future<Void> clear(Optional<KeyRef> configClass) { return addMutation(configClass, {}); }
	Future<Void> set(Optional<KeyRef> configClass, int64_t value) {
		return addMutation(configClass, longToValue(value));
	}
	void check(Optional<int64_t> value) const { return readFrom.checkImmediate(value); }
};

class BroadcasterToLocalConfigEnvironment {
	ReadFromLocalConfigEnvironment readFrom;
	Reference<AsyncVar<ConfigBroadcastFollowerInterface>> cbfi;
	ConfigBroadcaster broadcaster;
	Version lastWrittenVersion{ 0 };
	Future<Void> broadcastServer;

	ACTOR static Future<Void> setup(BroadcasterToLocalConfigEnvironment* self) {
		wait(self->readFrom.setup());
		self->readFrom.connectToBroadcaster(IDependentAsyncVar<ConfigBroadcastFollowerInterface>::create(self->cbfi));
		self->broadcastServer = self->broadcaster.serve(self->cbfi->get());
		return Void();
	}

	void addMutation(Optional<KeyRef> configClass, Optional<ValueRef> value) {
		Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
		appendVersionedMutation(versionedMutations, ++lastWrittenVersion, configClass, "test_long"_sr, value);
		broadcaster.applyChanges(versionedMutations, lastWrittenVersion, {});
	}

public:
	BroadcasterToLocalConfigEnvironment(std::string const& configPath)
	  : broadcaster(ConfigFollowerInterface{}), cbfi(makeReference<AsyncVar<ConfigBroadcastFollowerInterface>>()),
	    readFrom(configPath, {}) {}

	Future<Void> setup() { return setup(this); }

	void set(Optional<KeyRef> configClass, int64_t value) { addMutation(configClass, longToValue(value)); }

	void clear(Optional<KeyRef> configClass) { addMutation(configClass, {}); }

	Future<Void> check(Optional<int64_t> value) const { return readFrom.checkEventually(value); }

	void changeBroadcaster() {
		broadcastServer.cancel();
		cbfi->set(ConfigBroadcastFollowerInterface{});
		broadcastServer = broadcaster.serve(cbfi->get());
	}

	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
		return readFrom.restartLocalConfig(newConfigPath);
	}

	void compact() { broadcaster.compact(lastWrittenVersion); }

	Future<Void> getError() const { return readFrom.getError() || broadcastServer; }
};

class TransactionEnvironment {
	WriteToTransactionEnvironment writeTo;

	ACTOR static Future<Void> check(TransactionEnvironment* self,
	                                Optional<KeyRef> configClass,
	                                Optional<int64_t> expected) {
		state Reference<IConfigTransaction> tr =
		    IConfigTransaction::createSimple(self->writeTo.getTransactionInterface());
		state Key configKey = encodeConfigKey(configClass, "test_long"_sr);
		state Optional<Value> value = wait(tr->get(configKey));
		if (expected.present()) {
			ASSERT(value.get() == longToValue(expected.get()));
		} else {
			ASSERT(!value.present());
		}
		return Void();
	}

public:
	// TODO: Remove this?
	Future<Void> setup() { return Void(); }

	void restartNode() { writeTo.restartNode(); }
	Future<Void> set(Optional<KeyRef> configClass, int64_t value) { return writeTo.set(configClass, value); }
	Future<Void> clear(Optional<KeyRef> configClass) { return writeTo.clear(configClass); }
	Future<Void> check(Optional<KeyRef> configClass, Optional<int64_t> expected) {
		return check(this, configClass, expected);
	}
	Future<Void> compact() { return writeTo.compact(); }
	Future<Void> getError() const { return writeTo.getError(); }
};

class TransactionToLocalConfigEnvironment {
	WriteToTransactionEnvironment writeTo;
	ReadFromLocalConfigEnvironment readFrom;
	Reference<AsyncVar<ConfigBroadcastFollowerInterface>> cbfi;
	ConfigBroadcaster broadcaster;
	Future<Void> broadcastServer;

	ACTOR static Future<Void> setup(TransactionToLocalConfigEnvironment* self) {
		wait(self->readFrom.setup());
		self->readFrom.connectToBroadcaster(IDependentAsyncVar<ConfigBroadcastFollowerInterface>::create(self->cbfi));
		self->broadcastServer = self->broadcaster.serve(self->cbfi->get());
		return Void();
	}

public:
	TransactionToLocalConfigEnvironment(std::string const& configPath)
	  : readFrom(configPath, {}), broadcaster(writeTo.getFollowerInterface()),
	    cbfi(makeReference<AsyncVar<ConfigBroadcastFollowerInterface>>()) {}

	Future<Void> setup() { return setup(this); }

	void restartNode() { writeTo.restartNode(); }

	void changeBroadcaster() {
		broadcastServer.cancel();
		cbfi->set(ConfigBroadcastFollowerInterface{});
		broadcastServer = broadcaster.serve(cbfi->get());
	}

	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
		return readFrom.restartLocalConfig(newConfigPath);
	}

	Future<Void> compact() { return writeTo.compact(); }

	Future<Void> set(Optional<KeyRef> configClass, int64_t value) { return writeTo.set(configClass, value); }
	Future<Void> clear(Optional<KeyRef> configClass) { return writeTo.clear(configClass); }
	Future<Void> check(Optional<int64_t> value) const { return readFrom.checkEventually(value); }
	Future<Void> getError() const { return writeTo.getError() || readFrom.getError() || broadcastServer; }
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
template <class Env>
Future<Void> compact(Env& env) {
	return waitOrError(env.compact(), env.getError());
}
Future<Void> compact(BroadcasterToLocalConfigEnvironment& env) {
	env.compact();
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
	wait(compact(env));
	wait(check(env, 1));
	wait(set(env, "class-A"_sr, 2));
	wait(check(env, 2));
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

// TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/ChangeBroadcaster") {
//	wait(testChangeBroadcaster<BroadcasterToLocalConfigEnvironment>());
//	return Void();
//}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/RestartLocalConfig") {
	wait(testRestartLocalConfig<BroadcasterToLocalConfigEnvironment>());
	return Void();
}

// TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/RestartLocalConfigAndChangeClass") {
//	wait(testRestartLocalConfigAndChangeClass<BroadcasterToLocalConfigEnvironment>());
//	return Void();
//}

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
	env.restartNode();
	wait(check(env, 1));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/ChangeBroadcaster") {
	wait(testChangeBroadcaster<TransactionToLocalConfigEnvironment>());
	return Void();
}

// TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/RestartLocalConfig") {
//	wait(testRestartLocalConfig<TransactionToLocalConfigEnvironment>());
//	return Void();
//}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/RestartLocalConfigAndChangeClass") {
	wait(testRestartLocalConfigAndChangeClass<TransactionToLocalConfigEnvironment>());
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/CompactNode") {
	wait(testCompact<TransactionToLocalConfigEnvironment>());
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
	env.restartNode();
	wait(check(env, "class-A"_sr, 1));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/CompactNode") {
	state TransactionEnvironment env;
	wait(env.setup());
	wait(set(env, "class-A"_sr, 1));
	wait(env.compact());
	wait(env.check("class-A"_sr, 1));
	wait(set(env, "class-A"_sr, 2));
	wait(env.check("class-A"_sr, 2));
	return Void();
}
