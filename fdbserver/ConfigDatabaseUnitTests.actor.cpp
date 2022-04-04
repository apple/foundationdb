/*
 * ConfigDatabaseUnitTests.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/TestKnobCollection.h"
#include "fdbserver/ConfigBroadcaster.h"
#include "fdbserver/ConfigNode.h"
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
                             Optional<KnobValueRef> knobValue) {
	auto configKey = ConfigKeyRef(configClass, knobName);
	auto mutation = ConfigMutationRef(configKey, knobValue);
	versionedMutations.emplace_back_deep(versionedMutations.arena(), version, mutation);
}

class WriteToTransactionEnvironment {
	std::string dataDir;
	ConfigTransactionInterface cti;
	ConfigFollowerInterface cfi;
	Reference<ConfigNode> node;
	Future<Void> ctiServer;
	Future<Void> cfiServer;
	Version lastWrittenVersion{ 0 };

	static Value longToValue(int64_t v) {
		auto s = format("%lld", v);
		return StringRef(reinterpret_cast<uint8_t const*>(s.c_str()), s.size());
	}

	ACTOR static Future<Void> set(WriteToTransactionEnvironment* self,
	                              Optional<KeyRef> configClass,
	                              KeyRef knobName,
	                              int64_t value) {
		state Reference<IConfigTransaction> tr = IConfigTransaction::createTestSimple(self->cti);
		auto configKey = encodeConfigKey(configClass, knobName);
		tr->set(configKey, longToValue(value));
		wait(tr->commit());
		self->lastWrittenVersion = tr->getCommittedVersion();
		return Void();
	}

	ACTOR static Future<Void> clear(WriteToTransactionEnvironment* self,
	                                Optional<KeyRef> configClass,
	                                KeyRef knobName) {
		state Reference<IConfigTransaction> tr = IConfigTransaction::createTestSimple(self->cti);
		auto configKey = encodeConfigKey(configClass, knobName);
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
	WriteToTransactionEnvironment(std::string const& dataDir)
	  : dataDir(dataDir), node(makeReference<ConfigNode>(dataDir)) {
		platform::eraseDirectoryRecursive(dataDir);
		setup();
	}

	Future<Void> set(Optional<KeyRef> configClass, KeyRef knobName, int64_t value) {
		return set(this, configClass, knobName, value);
	}

	Future<Void> clear(Optional<KeyRef> configClass, KeyRef knobName) { return clear(this, configClass, knobName); }

	Future<Void> compact() { return cfi.compact.getReply(ConfigFollowerCompactRequest{ lastWrittenVersion }); }

	Future<Void> rollforward(Optional<Version> rollback,
	                         Version lastKnownCommitted,
	                         Version target,
	                         Standalone<VectorRef<VersionedConfigMutationRef>> mutations,
	                         Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations) {
		return cfi.rollforward.getReply(
		    ConfigFollowerRollforwardRequest{ rollback, lastKnownCommitted, target, mutations, annotations });
	}

	void restartNode() {
		cfiServer.cancel();
		ctiServer.cancel();
		node = makeReference<ConfigNode>(dataDir);
		setup();
	}

	ConfigTransactionInterface getTransactionInterface() const { return cti; }

	ConfigFollowerInterface getFollowerInterface() const { return cfi; }

	void close() const { node->close(); }

	Future<Void> onClosed() const { return node->onClosed(); }

	Future<Void> getError() const { return cfiServer || ctiServer; }
};

class ReadFromLocalConfigEnvironment {
	UID id;
	std::string dataDir;
	Reference<LocalConfiguration> localConfiguration;
	Reference<AsyncVar<ConfigBroadcastInterface> const> cbi;
	Future<Void> consumer;

	ACTOR template <class T, class V, class E>
	static Future<Void> checkEventually(Reference<LocalConfiguration const> localConfiguration,
	                                    V T::*member,
	                                    Optional<E> expected) {
		state double lastMismatchTime = now();
		loop {
			if (localConfiguration->getTestKnobs().*member == expected.orDefault(0)) {
				return Void();
			}
			if (now() > lastMismatchTime + 1.0) {
				TraceEvent(SevWarn, "CheckEventuallyStillChecking")
				    .detail("Expected", expected.present() ? expected.get() : 0)
				    .detail("TestMember", localConfiguration->getTestKnobs().*member);
				lastMismatchTime = now();
			}
			wait(delayJittered(0.1));
		}
	}

	ACTOR static Future<Void> setup(ReadFromLocalConfigEnvironment* self) {
		wait(self->localConfiguration->initialize());
		if (self->cbi) {
			// LocalConfiguration runs in a loop waiting for messages from the
			// broadcaster. These unit tests use the same
			// ConfigBroadcastInterface across restarts, so when "killing" the
			// old LocalConfiguration, it's necessary to make sure it is
			// completely stopped before starting the second config. This
			// prevents two actors trying to listen for the same message on the
			// same interface, causing lots of issues!
			self->consumer.cancel();
			self->consumer = self->localConfiguration->consume(self->cbi->get());
		}
		return Void();
	}

public:
	ReadFromLocalConfigEnvironment(std::string const& dataDir,
	                               std::string const& configPath,
	                               std::map<std::string, std::string> const& manualKnobOverrides)
	  : dataDir(dataDir),
	    localConfiguration(makeReference<LocalConfiguration>(dataDir, configPath, manualKnobOverrides, IsTest::True)),
	    consumer(Never()) {}

	Future<Void> setup() { return setup(this); }

	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
		std::map<std::string, std::string> manualKnobOverrides = {};
		localConfiguration =
		    makeReference<LocalConfiguration>(dataDir, newConfigPath, manualKnobOverrides, IsTest::True);
		return setup();
	}

	void connectToBroadcaster(Reference<AsyncVar<ConfigBroadcastInterface> const> const& cbi) {
		this->cbi = cbi;
		consumer = localConfiguration->consume(cbi->get());
	}

	template <class T, class V, class E>
	void checkImmediate(V T::*member, Optional<E> expected) const {
		if (expected.present()) {
			ASSERT_EQ(localConfiguration->getTestKnobs().*member, expected.get());
		} else {
			ASSERT_EQ(localConfiguration->getTestKnobs().*member, 0);
		}
	}

	template <class T, class V, class E>
	Future<Void> checkEventually(V T::*member, Optional<E> expected) const {
		return checkEventually(localConfiguration, member, expected);
	}

	LocalConfiguration& getMutableLocalConfiguration() { return *localConfiguration; }

	void close() const { localConfiguration->close(); }
	Future<Void> onClosed() const { return localConfiguration->onClosed(); }

	Future<Void> getError() const { return consumer; }

	Version lastSeenVersion() { return localConfiguration->lastSeenVersion(); }

	ConfigClassSet configClassSet() { return localConfiguration->configClassSet(); }
};

class LocalConfigEnvironment {
	ReadFromLocalConfigEnvironment readFrom;
	Version lastWrittenVersion{ 0 };

	Future<Void> addMutation(Optional<KeyRef> configClass, KeyRef knobName, Optional<KnobValueRef> value) {
		Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
		appendVersionedMutation(versionedMutations, ++lastWrittenVersion, configClass, knobName, value);
		return readFrom.getMutableLocalConfiguration().addChanges(versionedMutations, lastWrittenVersion);
	}

public:
	LocalConfigEnvironment(std::string const& dataDir,
	                       std::string const& configPath,
	                       std::map<std::string, std::string> const& manualKnobOverrides = {})
	  : readFrom(dataDir, configPath, manualKnobOverrides) {}
	Future<Void> setup(ConfigClassSet const& configClassSet) { return readFrom.setup(); }
	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
		return readFrom.restartLocalConfig(newConfigPath);
	}
	Future<Void> getError() const { return Never(); }
	Future<Void> clear(Optional<KeyRef> configClass, KeyRef knobName) { return addMutation(configClass, knobName, {}); }
	Future<Void> set(Optional<KeyRef> configClass, KeyRef knobName, int64_t value) {
		auto knobValue = KnobValueRef::create(value);
		return addMutation(configClass, knobName, knobValue.contents());
	}
	template <class T, class V, class E>
	void check(V T::*member, Optional<E> value) const {
		return readFrom.checkImmediate(member, value);
	}
};

class BroadcasterToLocalConfigEnvironment {
	ReadFromLocalConfigEnvironment readFrom;
	Reference<AsyncVar<ConfigBroadcastInterface>> cbi;
	ConfigBroadcaster broadcaster;
	Version lastWrittenVersion{ 0 };
	Future<Void> broadcastServer;
	Promise<Void> workerFailure;
	Future<Void> workerFailed_;

	ACTOR static Future<Void> setup(BroadcasterToLocalConfigEnvironment* self, ConfigClassSet configClassSet) {
		wait(self->readFrom.setup());
		self->cbi = makeReference<AsyncVar<ConfigBroadcastInterface>>();
		self->readFrom.connectToBroadcaster(self->cbi);
		self->broadcastServer = self->broadcaster.registerNode(
		    WorkerInterface(), 0, configClassSet, self->workerFailure.getFuture(), self->cbi->get());
		return Void();
	}

	void addMutation(Optional<KeyRef> configClass, KeyRef knobName, KnobValueRef value) {
		Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
		appendVersionedMutation(versionedMutations, ++lastWrittenVersion, configClass, knobName, value);
		broadcaster.applyChanges(versionedMutations, lastWrittenVersion, {}, {});
	}

public:
	BroadcasterToLocalConfigEnvironment(std::string const& dataDir, std::string const& configPath)
	  : readFrom(dataDir, configPath, {}), cbi(makeReference<AsyncVar<ConfigBroadcastInterface>>()),
	    broadcaster(ConfigFollowerInterface{}) {}

	Future<Void> setup(ConfigClassSet const& configClassSet) { return setup(this, configClassSet); }

	void set(Optional<KeyRef> configClass, KeyRef knobName, int64_t value) {
		auto knobValue = KnobValueRef::create(value);
		addMutation(configClass, knobName, knobValue.contents());
	}

	void clear(Optional<KeyRef> configClass, KeyRef knobName) { addMutation(configClass, knobName, {}); }

	template <class T, class V, class E>
	Future<Void> check(V T::*member, Optional<E> value) const {
		return readFrom.checkEventually(member, value);
	}

	void changeBroadcaster() {
		broadcastServer.cancel();
		cbi->set(ConfigBroadcastInterface{});
		readFrom.connectToBroadcaster(cbi);
		broadcastServer = broadcaster.registerNode(WorkerInterface(),
		                                           readFrom.lastSeenVersion(),
		                                           readFrom.configClassSet(),
		                                           workerFailure.getFuture(),
		                                           cbi->get());
	}

	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
		return readFrom.restartLocalConfig(newConfigPath);
	}

	void killLocalConfig() {
		workerFailed_ = broadcaster.getClientFailure(cbi->get().id());
		workerFailure.send(Void());
	}

	Future<Void> workerFailed() {
		ASSERT(workerFailed_.isValid());
		return workerFailed_;
	}

	void compact() { broadcaster.compact(lastWrittenVersion); }

	void close() const { readFrom.close(); }

	Future<Void> onClosed() const { return readFrom.onClosed(); }

	Future<Void> getError() const { return readFrom.getError() || broadcaster.getError(); }
};

class TransactionEnvironment {
	WriteToTransactionEnvironment writeTo;

	ACTOR static Future<Void> check(TransactionEnvironment* self,
	                                Optional<KeyRef> configClass,
	                                KeyRef knobName,
	                                Optional<int64_t> expected) {
		state Reference<IConfigTransaction> tr =
		    IConfigTransaction::createTestSimple(self->writeTo.getTransactionInterface());
		state Key configKey = encodeConfigKey(configClass, knobName);
		state Optional<Value> value = wait(tr->get(configKey));
		if (expected.present()) {
			ASSERT_EQ(BinaryReader::fromStringRef<int64_t>(value.get(), Unversioned()), expected.get());
		} else {
			ASSERT(!value.present());
		}
		return Void();
	}

	ACTOR static Future<Standalone<VectorRef<KeyRef>>> getConfigClasses(TransactionEnvironment* self) {
		state Reference<IConfigTransaction> tr =
		    IConfigTransaction::createTestSimple(self->writeTo.getTransactionInterface());
		state KeySelector begin = firstGreaterOrEqual(configClassKeys.begin);
		state KeySelector end = firstGreaterOrEqual(configClassKeys.end);
		RangeResult range = wait(tr->getRange(begin, end, 1000));
		Standalone<VectorRef<KeyRef>> result;
		for (const auto& kv : range) {
			result.push_back_deep(result.arena(), kv.key);
			ASSERT(kv.value == ""_sr);
		}
		return result;
	}

	ACTOR static Future<Standalone<VectorRef<KeyRef>>> getKnobNames(TransactionEnvironment* self,
	                                                                Optional<KeyRef> configClass) {
		state Reference<IConfigTransaction> tr =
		    IConfigTransaction::createTestSimple(self->writeTo.getTransactionInterface());
		state KeyRange keys = globalConfigKnobKeys;
		if (configClass.present()) {
			keys = singleKeyRange(configClass.get().withPrefix(configKnobKeys.begin));
		}
		KeySelector begin = firstGreaterOrEqual(keys.begin);
		KeySelector end = firstGreaterOrEqual(keys.end);
		RangeResult range = wait(tr->getRange(begin, end, 1000));
		Standalone<VectorRef<KeyRef>> result;
		for (const auto& kv : range) {
			result.push_back_deep(result.arena(), kv.key);
			ASSERT(kv.value == ""_sr);
		}
		return result;
	}

	ACTOR static Future<Void> badRangeRead(TransactionEnvironment* self) {
		state Reference<IConfigTransaction> tr =
		    IConfigTransaction::createTestSimple(self->writeTo.getTransactionInterface());
		KeySelector begin = firstGreaterOrEqual(normalKeys.begin);
		KeySelector end = firstGreaterOrEqual(normalKeys.end);
		wait(success(tr->getRange(begin, end, 1000)));
		return Void();
	}

public:
	TransactionEnvironment(std::string const& dataDir) : writeTo(dataDir) {}

	Future<Void> setup() { return Void(); }

	void restartNode() { writeTo.restartNode(); }
	template <class T>
	Future<Void> set(Optional<KeyRef> configClass, KeyRef knobName, T value) {
		return writeTo.set(configClass, knobName, value);
	}
	Future<Void> clear(Optional<KeyRef> configClass, KeyRef knobName) { return writeTo.clear(configClass, knobName); }
	Future<Void> check(Optional<KeyRef> configClass, KeyRef knobName, Optional<int64_t> expected) {
		return check(this, configClass, knobName, expected);
	}
	Future<Void> badRangeRead() { return badRangeRead(this); }

	Future<Standalone<VectorRef<KeyRef>>> getConfigClasses() { return getConfigClasses(this); }
	Future<Standalone<VectorRef<KeyRef>>> getKnobNames(Optional<KeyRef> configClass) {
		return getKnobNames(this, configClass);
	}

	Future<Void> compact() { return writeTo.compact(); }
	Future<Void> rollforward(Optional<Version> rollback,
	                         Version lastKnownCommitted,
	                         Version target,
	                         Standalone<VectorRef<VersionedConfigMutationRef>> mutations,
	                         Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations) {
		return writeTo.rollforward(rollback, lastKnownCommitted, target, mutations, annotations);
	}
	Future<Void> getError() const { return writeTo.getError(); }
};

class TransactionToLocalConfigEnvironment {
	WriteToTransactionEnvironment writeTo;
	ReadFromLocalConfigEnvironment readFrom;
	Reference<AsyncVar<ConfigBroadcastInterface>> cbi;
	ConfigBroadcaster broadcaster;
	Future<Void> broadcastServer;
	Promise<Void> workerFailure;
	Future<Void> workerFailed_;

	ACTOR static Future<Void> setup(TransactionToLocalConfigEnvironment* self, ConfigClassSet configClassSet) {
		wait(self->readFrom.setup());
		self->cbi = makeReference<AsyncVar<ConfigBroadcastInterface>>();
		self->readFrom.connectToBroadcaster(self->cbi);
		self->broadcastServer = self->broadcaster.registerNode(
		    WorkerInterface(), 0, configClassSet, self->workerFailure.getFuture(), self->cbi->get());
		return Void();
	}

public:
	TransactionToLocalConfigEnvironment(std::string const& dataDir, std::string const& configPath)
	  : writeTo(dataDir), readFrom(dataDir, configPath, {}), cbi(makeReference<AsyncVar<ConfigBroadcastInterface>>()),
	    broadcaster(writeTo.getFollowerInterface()) {}

	Future<Void> setup(ConfigClassSet const& configClassSet) { return setup(this, configClassSet); }

	void restartNode() { writeTo.restartNode(); }

	void changeBroadcaster() {
		broadcastServer.cancel();
		cbi->set(ConfigBroadcastInterface{});
		readFrom.connectToBroadcaster(cbi);
		broadcastServer = broadcaster.registerNode(WorkerInterface(),
		                                           readFrom.lastSeenVersion(),
		                                           readFrom.configClassSet(),
		                                           workerFailure.getFuture(),
		                                           cbi->get());
	}

	Future<Void> restartLocalConfig(std::string const& newConfigPath) {
		return readFrom.restartLocalConfig(newConfigPath);
	}

	void killLocalConfig() {
		workerFailed_ = broadcaster.getClientFailure(cbi->get().id());
		workerFailure.send(Void());
	}

	Future<Void> workerFailed() {
		ASSERT(workerFailed_.isValid());
		return workerFailed_;
	}

	Future<Void> compact() { return writeTo.compact(); }

	template <class T>
	Future<Void> set(Optional<KeyRef> configClass, KeyRef knobName, T const& value) {
		return writeTo.set(configClass, knobName, value);
	}
	Future<Void> clear(Optional<KeyRef> configClass, KeyRef knobName) { return writeTo.clear(configClass, knobName); }
	template <class T, class V, class E>
	Future<Void> check(V T::*member, Optional<E> value) const {
		return readFrom.checkEventually(member, value);
	}
	void close() const {
		writeTo.close();
		readFrom.close();
	}
	Future<Void> onClosed() const { return writeTo.onClosed() && readFrom.onClosed(); }
	Future<Void> getError() const { return writeTo.getError() || readFrom.getError() || broadcaster.getError(); }
};

// These functions give a common interface to all environments, to improve code reuse
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
template <class Env, class... Args>
Future<Void> rollforward(Env& env, Args&&... args) {
	return waitOrError(env.rollforward(std::forward<Args>(args)...), env.getError());
}

ACTOR template <class Env>
Future<Void> testRestartLocalConfig(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr })));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 1 }));
	wait(env.restartLocalConfig("class-A"));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 1 }));
	wait(set(env, "class-A"_sr, "test_long"_sr, 2));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 2 }));
	return Void();
}

ACTOR template <class Env>
Future<Void> testRestartLocalConfigAndChangeClass(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr, "class-B"_sr })));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 1 }));
	wait(env.restartLocalConfig("class-B"));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 0 }));
	wait(set(env, "class-B"_sr, "test_long"_sr, int64_t{ 2 }));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 2 }));
	return Void();
}

ACTOR template <class Env>
Future<Void> testNewLocalConfigAfterCompaction(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr })));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 1 }));
	wait(compact(env));
	// Erase the data dir to simulate a new worker joining the system after
	// compaction.
	platform::eraseDirectoryRecursive(params.getDataDir());
	platform::createDirectory(params.getDataDir());
	wait(env.restartLocalConfig("class-A"));
	// Reregister worker with broadcaster.
	env.changeBroadcaster();
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 1 }));
	wait(set(env, "class-A"_sr, "test_long"_sr, 2));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 2 }));
	return Void();
}

ACTOR template <class Env>
Future<Void> testKillWorker(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr })));
	env.killLocalConfig();
	// Make sure broadcaster detects worker death in a timely manner.
	wait(timeoutError(env.workerFailed(), 3));
	Future<Void> closed = env.onClosed();
	env.close();
	wait(closed);
	return Void();
}

ACTOR template <class Env>
Future<Void> testSet(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr })));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 1 }));
	return Void();
}

ACTOR template <class Env>
Future<Void> testAtomicSet(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr })));
	state bool restarted = false;
	try {
		wait(set(env, "class-A"_sr, "test_atomic_long"_sr, int64_t{ 1 }));
	} catch (Error& e) {
		ASSERT(e.code() == error_code_local_config_changed);
		restarted = true;
	}
	ASSERT(restarted);
	wait(env.restartLocalConfig("class-A"));
	wait(check(env, &TestKnobs::TEST_ATOMIC_LONG, Optional<int64_t>{ 1 }));
	return Void();
}

ACTOR template <class Env>
Future<Void> testClear(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr })));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	wait(clear(env, "class-A"_sr, "test_long"_sr));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{}));
	return Void();
}

ACTOR template <class Env>
Future<Void> testAtomicClear(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr })));
	state bool restarted = false;
	try {
		wait(set(env, "class-A"_sr, "test_atomic_long"_sr, int64_t{ 1 }));
	} catch (Error& e) {
		ASSERT(e.code() == error_code_local_config_changed);
		restarted = true;
	}
	ASSERT(restarted);
	restarted = false;
	try {
		wait(clear(env, "class-A"_sr, "test_atomic_long"_sr));
	} catch (Error& e) {
		ASSERT(e.code() == error_code_local_config_changed);
		restarted = true;
	}
	ASSERT(restarted);
	wait(check(env, &TestKnobs::TEST_ATOMIC_LONG, Optional<int64_t>{}));
	return Void();
}

ACTOR template <class Env>
Future<Void> testGlobalSet(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr })));
	wait(set(env, Optional<KeyRef>{}, "test_long"_sr, int64_t{ 1 }));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 1 }));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 10 }));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 10 }));
	return Void();
}

ACTOR template <class Env>
Future<Void> testIgnore(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr, "class-B"_sr })));
	wait(set(env, "class-B"_sr, "test_long"_sr, int64_t{ 1 }));
	choose {
		when(wait(delay(5))) {}
		when(wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 1 }))) { ASSERT(false); }
	}
	return Void();
}

ACTOR template <class Env>
Future<Void> testCompact(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr })));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	wait(compact(env));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 1 }));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 2 }));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 2 }));
	return Void();
}

ACTOR template <class Env>
Future<Void> testChangeBroadcaster(UnitTestParameters params) {
	state Env env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr })));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 1 }));
	env.changeBroadcaster();
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 2 }));
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 2 }));
	return Void();
}

bool matches(Standalone<VectorRef<KeyRef>> const& vec, std::set<Key> const& compareTo) {
	std::set<Key> s;
	for (const auto& value : vec) {
		s.insert(value);
	}
	return (s == compareTo);
}

ACTOR Future<Void> testGetConfigClasses(UnitTestParameters params, bool doCompact) {
	state TransactionEnvironment env(params.getDataDir());
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	wait(set(env, "class-B"_sr, "test_long"_sr, int64_t{ 1 }));
	if (doCompact) {
		wait(compact(env));
	}
	Standalone<VectorRef<KeyRef>> configClasses = wait(env.getConfigClasses());
	ASSERT(matches(configClasses, { "class-A"_sr, "class-B"_sr }));
	return Void();
}

ACTOR Future<Void> testGetKnobs(UnitTestParameters params, bool global, bool doCompact) {
	state TransactionEnvironment env(params.getDataDir());
	state Optional<Key> configClass;
	if (!global) {
		configClass = "class-A"_sr;
	}
	wait(set(env, configClass.castTo<KeyRef>(), "test_long"_sr, int64_t{ 1 }));
	wait(set(env, configClass.castTo<KeyRef>(), "test_int"_sr, int{ 2 }));
	wait(set(env, "class-B"_sr, "test_double"_sr, double{ 3.0 })); // ignored
	if (doCompact) {
		wait(compact(env));
	}
	Standalone<VectorRef<KeyRef>> knobNames =
	    wait(waitOrError(env.getKnobNames(configClass.castTo<KeyRef>()), env.getError()));
	ASSERT(matches(knobNames, { "test_long"_sr, "test_int"_sr }));
	return Void();
}

} // namespace

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Set") {
	wait(testSet<LocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/AtomicSet") {
	wait(testAtomicSet<LocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Restart") {
	wait(testRestartLocalConfig<LocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/RestartFresh") {
	wait(testRestartLocalConfigAndChangeClass<LocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Clear") {
	wait(testClear<LocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/AtomicClear") {
	wait(testAtomicClear<LocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/GlobalSet") {
	wait(testGlobalSet<LocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/ConflictingOverrides") {
	state LocalConfigEnvironment env(params.getDataDir(), "class-A/class-B", {});
	wait(env.setup(ConfigClassSet({ "class-A"_sr, "class-B"_sr })));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	wait(set(env, "class-B"_sr, "test_long"_sr, int64_t{ 10 }));
	env.check(&TestKnobs::TEST_LONG, Optional<int64_t>{ 10 });
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Manual") {
	state LocalConfigEnvironment env(params.getDataDir(), "class-A", { { "test_long", "1000" } });
	wait(env.setup(ConfigClassSet({ "class-A"_sr })));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	env.check(&TestKnobs::TEST_LONG, Optional<int64_t>{ 1000 });
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Set") {
	wait(testSet<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Clear") {
	wait(testClear<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Ignore") {
	wait(testIgnore<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/GlobalSet") {
	wait(testGlobalSet<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/ChangeBroadcaster") {
	wait(testChangeBroadcaster<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/RestartLocalConfig") {
	wait(testRestartLocalConfig<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/RestartLocalConfigAndChangeClass") {
	wait(testRestartLocalConfigAndChangeClass<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/Compact") {
	wait(testCompact<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/RestartLocalConfigurationAfterCompaction") {
	wait(testNewLocalConfigAfterCompaction<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/BroadcasterToLocalConfig/KillWorker") {
	wait(testKillWorker<BroadcasterToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/Set") {
	wait(testSet<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/Clear") {
	wait(testClear<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/GlobalSet") {
	wait(testGlobalSet<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/RestartNode") {
	state TransactionToLocalConfigEnvironment env(params.getDataDir(), "class-A");
	wait(env.setup(ConfigClassSet({ "class-A"_sr })));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	env.restartNode();
	wait(check(env, &TestKnobs::TEST_LONG, Optional<int64_t>{ 1 }));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/ChangeBroadcaster") {
	wait(testChangeBroadcaster<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/RestartLocalConfigAndChangeClass") {
	wait(testRestartLocalConfigAndChangeClass<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/CompactNode") {
	wait(testCompact<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/RestartLocalConfigurationAfterCompaction") {
	wait(testNewLocalConfigAfterCompaction<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/TransactionToLocalConfig/KillWorker") {
	wait(testKillWorker<TransactionToLocalConfigEnvironment>(params));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Set") {
	state TransactionEnvironment env(params.getDataDir());
	wait(env.setup());
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	wait(check(env, "class-A"_sr, "test_long"_sr, Optional<int64_t>{ 1 }));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Clear") {
	state TransactionEnvironment env(params.getDataDir());
	wait(env.setup());
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	wait(clear(env, "class-A"_sr, "test_long"_sr));
	wait(check(env, "class-A"_sr, "test_long"_sr, Optional<int64_t>{}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Restart") {
	state TransactionEnvironment env(params.getDataDir());
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	env.restartNode();
	wait(check(env, "class-A"_sr, "test_long"_sr, Optional<int64_t>{ 1 }));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/CompactNode") {
	state TransactionEnvironment env(params.getDataDir());
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	wait(compact(env));
	wait(check(env, "class-A"_sr, "test_long"_sr, Optional<int64_t>{ 1 }));
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 2 }));
	wait(check(env, "class-A"_sr, "test_long"_sr, Optional<int64_t>{ 2 }));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/Rollforward") {
	state TransactionEnvironment env(params.getDataDir());
	Standalone<VectorRef<VersionedConfigMutationRef>> mutations;
	appendVersionedMutation(
	    mutations, 1, "class-A"_sr, "test_long_v1"_sr, KnobValueRef::create(int64_t{ 1 }).contents());
	appendVersionedMutation(
	    mutations, 2, "class-B"_sr, "test_long_v2"_sr, KnobValueRef::create(int64_t{ 2 }).contents());
	Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations;
	annotations.emplace_back_deep(annotations.arena(), 1, ConfigCommitAnnotationRef{ "unit_test"_sr, now() });
	annotations.emplace_back_deep(annotations.arena(), 2, ConfigCommitAnnotationRef{ "unit_test"_sr, now() });
	wait(rollforward(env, Optional<Version>{}, 0, 2, mutations, annotations));
	wait(check(env, "class-A"_sr, "test_long_v1"_sr, Optional<int64_t>{ 1 }));
	wait(check(env, "class-B"_sr, "test_long_v2"_sr, Optional<int64_t>{ 2 }));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/RollforwardWithExistingMutation") {
	state TransactionEnvironment env(params.getDataDir());
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	Standalone<VectorRef<VersionedConfigMutationRef>> mutations;
	appendVersionedMutation(
	    mutations, 2, "class-A"_sr, "test_long_v2"_sr, KnobValueRef::create(int64_t{ 2 }).contents());
	appendVersionedMutation(
	    mutations, 3, "class-A"_sr, "test_long_v3"_sr, KnobValueRef::create(int64_t{ 3 }).contents());
	Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations;
	annotations.emplace_back_deep(annotations.arena(), 2, ConfigCommitAnnotationRef{ "unit_test"_sr, now() });
	annotations.emplace_back_deep(annotations.arena(), 3, ConfigCommitAnnotationRef{ "unit_test"_sr, now() });
	wait(rollforward(env, Optional<Version>{}, 1, 3, mutations, annotations));
	wait(check(env, "class-A"_sr, "test_long"_sr, Optional<int64_t>{ 1 }));
	wait(check(env, "class-A"_sr, "test_long_v2"_sr, Optional<int64_t>{ 2 }));
	wait(check(env, "class-A"_sr, "test_long_v3"_sr, Optional<int64_t>{ 3 }));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/RollforwardWithInvalidMutation") {
	state TransactionEnvironment env(params.getDataDir());
	Standalone<VectorRef<VersionedConfigMutationRef>> mutations;
	appendVersionedMutation(
	    mutations, 1, "class-A"_sr, "test_long_v1"_sr, KnobValueRef::create(int64_t{ 1 }).contents());
	appendVersionedMutation(
	    mutations, 10, "class-A"_sr, "test_long_v10"_sr, KnobValueRef::create(int64_t{ 2 }).contents());
	Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations;
	annotations.emplace_back_deep(annotations.arena(), 1, ConfigCommitAnnotationRef{ "unit_test"_sr, now() });
	wait(rollforward(env, Optional<Version>{}, 0, 5, mutations, annotations));
	wait(check(env, "class-A"_sr, "test_long_v1"_sr, Optional<int64_t>{ 1 }));
	wait(check(env, "class-A"_sr, "test_long_v10"_sr, Optional<int64_t>{}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/RollbackThenRollforward") {
	state TransactionEnvironment env(params.getDataDir());
	wait(set(env, "class-A"_sr, "test_long"_sr, int64_t{ 1 }));
	Standalone<VectorRef<VersionedConfigMutationRef>> mutations;
	appendVersionedMutation(
	    mutations, 1, "class-B"_sr, "test_long_v1"_sr, KnobValueRef::create(int64_t{ 2 }).contents());
	Standalone<VectorRef<VersionedConfigCommitAnnotationRef>> annotations;
	annotations.emplace_back_deep(annotations.arena(), 1, ConfigCommitAnnotationRef{ "unit_test"_sr, now() });
	wait(rollforward(env, 0, 1, 1, mutations, annotations));
	wait(check(env, "class-A"_sr, "test_long"_sr, Optional<int64_t>{}));
	wait(check(env, "class-B"_sr, "test_long_v1"_sr, Optional<int64_t>{ 2 }));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/GetConfigClasses") {
	wait(testGetConfigClasses(params, false));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/CompactThenGetConfigClasses") {
	wait(testGetConfigClasses(params, true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/GetKnobs") {
	wait(testGetKnobs(params, false, false));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/CompactThenGetKnobs") {
	wait(testGetKnobs(params, false, true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/GetGlobalKnobs") {
	wait(testGetKnobs(params, true, false));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/CompactThenGetGlobalKnobs") {
	wait(testGetKnobs(params, true, true));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/Transaction/BadRangeRead") {
	state TransactionEnvironment env(params.getDataDir());
	try {
		wait(env.badRangeRead() || env.getError());
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_invalid_config_db_range_read);
	}
	return Void();
}
