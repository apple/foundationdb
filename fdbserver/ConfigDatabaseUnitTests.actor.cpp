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
#include "fdbserver/ConfigBroadcaster.h"
#include "fdbserver/LocalConfiguration.h"
#include "fdbclient/Tuple.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

ACTOR template <class ConfigStore>
Future<Void> setTestSnapshot(ConfigStore* configStore, Version* version) {
	std::map<ConfigKey, Value> snapshot = {
		{ ConfigKeyRef("class-A"_sr, "test_int"_sr), "1"_sr },
		{ ConfigKeyRef("class-B"_sr, "test_int"_sr), "2"_sr },
		{ ConfigKeyRef("class-C"_sr, "test_int"_sr), "3"_sr },
		{ ConfigKeyRef("class-A"_sr, "test_string"_sr), "x"_sr },
	};
	wait(configStore->setSnapshot(std::move(snapshot), ++(*version)));
	return Void();
}

void appendVersionedMutation(Standalone<VectorRef<VersionedConfigMutationRef>>& versionedMutations,
                             Version version,
                             KeyRef configClass,
                             KeyRef knobName,
                             ValueRef knobValue) {
	Tuple tuple;
	tuple << configClass;
	tuple << knobName;
	auto mutation = ConfigMutationRef::createConfigMutation(tuple.pack(), knobValue);
	versionedMutations.emplace_back_deep(versionedMutations.arena(), version, mutation);
}

ACTOR template <class ConfigStore>
Future<Void> addTestUpdates(ConfigStore* configStore, Version* version) {
	Standalone<VectorRef<VersionedConfigMutationRef>> versionedMutations;
	++(*version);
	appendVersionedMutation(versionedMutations, *version, "class-A"_sr, "test_bool"_sr, "true"_sr);
	appendVersionedMutation(versionedMutations, *version, "class-B"_sr, "test_long"_sr, "100"_sr);
	appendVersionedMutation(versionedMutations, *version, "class-C"_sr, "test_double"_sr, "1.0"_sr);
	appendVersionedMutation(versionedMutations, *version, "class-A"_sr, "test_int"_sr, "10"_sr);
	wait(configStore->addVersionedMutations(versionedMutations, *version));
	return Void();
}

ACTOR template <class ConfigStore>
Future<Void> runTestUpdates(ConfigStore* configStore, Version* version) {
	wait(setTestSnapshot(configStore, version));
	wait(addTestUpdates(configStore, version));
	// TODO: Clean up on-disk state
	return Void();
}

ACTOR Future<Void> runFirstLocalConfiguration(std::string configPath, UID uid) {
	state LocalConfiguration localConfiguration(configPath, "./", {}, uid);
	state Version version = 1;
	wait(localConfiguration.initialize());
	wait(runTestUpdates(&localConfiguration, &version));
	ASSERT(localConfiguration.getTestKnobs().TEST_INT == 2);
	ASSERT(localConfiguration.getTestKnobs().TEST_BOOL);
	ASSERT(localConfiguration.getTestKnobs().TEST_STRING == "x");
	return Void();
}

ACTOR template <class F>
Future<Void> runSecondLocalConfiguration(std::string configPath, UID uid, F validate) {
	state LocalConfiguration localConfiguration(configPath, "./", {}, uid);
	wait(localConfiguration.initialize());
	validate(localConfiguration.getTestKnobs());
	return Void();
}

} // namespace

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Simple") {
	wait(runFirstLocalConfiguration("class-A/class-B", deterministicRandom()->randomUniqueID()));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/Restart") {
	state UID uid = deterministicRandom()->randomUniqueID();
	wait(runFirstLocalConfiguration("class-A/class-B", uid));
	wait(runSecondLocalConfiguration("class-A/class-B", uid, [](TestKnobs const& testKnobs) {
		ASSERT(testKnobs.TEST_INT == 2);
		ASSERT(testKnobs.TEST_BOOL);
		ASSERT(testKnobs.TEST_STRING == "x");
		ASSERT(testKnobs.TEST_DOUBLE == 0.0);
		ASSERT(testKnobs.TEST_LONG == 100);
	}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/LocalConfiguration/FreshRestart") {
	state UID uid = deterministicRandom()->randomUniqueID();
	wait(runFirstLocalConfiguration("class-A/class-B", uid));
	wait(runSecondLocalConfiguration("class-B/class-A", uid, [](TestKnobs const& testKnobs) {
		ASSERT(testKnobs.TEST_INT == 0);
		ASSERT(!testKnobs.TEST_BOOL);
		ASSERT(testKnobs.TEST_STRING == "");
		ASSERT(testKnobs.TEST_DOUBLE == 0.0);
		ASSERT(testKnobs.TEST_LONG == 0);
	}));
	return Void();
}

TEST_CASE("/fdbserver/ConfigDB/ConfigBroadcaster/Simple") {
	state ConfigBroadcaster broadcaster(ConfigFollowerInterface{});
	state Reference<IDependentAsyncVar<ConfigFollowerInterface>> cfi =
	    IDependentAsyncVar<ConfigFollowerInterface>::create(makeReference<AsyncVar<ConfigFollowerInterface>>());
	state LocalConfiguration localConfiguration("class-A/class-B", "./", {}, deterministicRandom()->randomUniqueID());
	state Version version = 1;
	state ActorCollection actors(false);
	wait(localConfiguration.initialize());
	actors.add(broadcaster.serve(cfi->get()));
	actors.add(localConfiguration.consume(cfi));
	choose {
		when(wait(runTestUpdates(&broadcaster, &version))) {}
		when(wait(actors.getResult())) { ASSERT(false); }
	}
	// TODO: Check that local configuration received updates
	return Void();
}
