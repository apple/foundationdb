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

#include "fdbserver/ConfigFollowerInterface.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/LocalConfiguration.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

const KeyRef configClassesKey = "configClasses"_sr;
const KeyRangeRef updateKeys = KeyRangeRef("updates/"_sr, "updates0"_sr);

} // namespace

class LocalConfigurationImpl {
	IKeyValueStore* kvStore; // FIXME: fix leaks?
	ConfigClassSet configClasses;
	Future<Void> initFuture;
	Reference<AsyncVar<ConfigFollowerInterface>> broadcaster;
	TestKnobs testKnobs;

	ACTOR static Future<Void> saveConfigClasses(LocalConfigurationImpl* self) {
		self->kvStore->set(KeyValueRef(configClassesKey, BinaryWriter::toValue(self->configClasses, IncludeVersion())));
		wait(self->kvStore->commit());
		return Void();
	}

	ACTOR static Future<Void> clearKVStore(LocalConfigurationImpl *self) {
		self->kvStore->clear(singleKeyRange(configClassesKey));
		self->kvStore->clear(updateKeys);
		wait(self->kvStore->commit());
		return Void();
	}

	ACTOR static Future<Void> init(LocalConfigurationImpl* self) {
		self->testKnobs.initialize();
		wait(self->kvStore->init());
		state Optional<Value> storedConfigClassesValue = wait(self->kvStore->readValue(configClassesKey));
		if (!storedConfigClassesValue.present()) {
			wait(saveConfigClasses(self));
			return Void();
		}
		state ConfigClassSet storedConfigClasses = BinaryReader::fromStringRef<ConfigClassSet>(storedConfigClassesValue.get(), IncludeVersion());
		if (storedConfigClasses != self->configClasses) {
			// All local information is outdated
			wait(clearKVStore(self));
			wait(saveConfigClasses(self));
			return Void();
		}
		Standalone<RangeResultRef> range = wait(self->kvStore->readRange(updateKeys));
		for (const auto &kv : range) {
			// TODO: Fail gracefully
			ASSERT(self->testKnobs.setKnob(kv.key.toString(), kv.value.toString()));
		}
		return Void();
	}

	ACTOR static Future<Void> consume(LocalConfigurationImpl *self, Reference<AsyncVar<ConfigFollowerInterface>> broadcaster) {
		wait(self->initFuture);
		// TODO: Implement
		wait(Never());
		return Void();
	}

public:
	LocalConfigurationImpl(ConfigClassSet const& configClasses, std::string const& dataFolder) : configClasses(configClasses) {
		platform::createDirectory(dataFolder);
		kvStore = keyValueStoreMemory(joinPath(dataFolder, "localconf-"), UID{}, 500e6);
	}

	Future<Void> init() {
		ASSERT(!initFuture.isValid());
		initFuture = init(this);
		return initFuture;
	}

	TestKnobs const &getKnobs() const {
		ASSERT(initFuture.isReady());
		return testKnobs;
	}

	Future<Void> consume(Reference<AsyncVar<ConfigFollowerInterface>> broadcaster) {
		return consume(this, broadcaster);
	}
};

LocalConfiguration::LocalConfiguration(ConfigClassSet const &configClasses, std::string const &dataFolder) :
	impl(std::make_unique<LocalConfigurationImpl>(configClasses, dataFolder)) {}

LocalConfiguration::~LocalConfiguration() = default;

Future<Void> LocalConfiguration::init() {
	return impl->init();
}

TestKnobs const &LocalConfiguration::getKnobs() const {
	return impl->getKnobs();
}

Future<Void> LocalConfiguration::consume(Reference<AsyncVar<ConfigFollowerInterface>> broadcaster) {
	return impl->consume(broadcaster);
}

#define init(knob, value) initKnob(knob, value, #knob)

void TestKnobs::initialize() {
	init(TEST, 0);
}
