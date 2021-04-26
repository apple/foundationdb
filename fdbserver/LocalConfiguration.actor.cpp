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

#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/LocalConfiguration.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

const KeyRef configClassesKey = "configClasses"_sr;
const KeyRef lastSeenVersionKey = "lastSeenVersion"_sr;
const KeyRangeRef updateKeys = KeyRangeRef("updates/"_sr, "updates0"_sr);

} // namespace

class LocalConfigurationImpl {
	IKeyValueStore* kvStore; // FIXME: fix leaks?
	ConfigClassSet configClasses;
	Version lastSeenVersion { 0 };
	Future<Void> initFuture;
	Reference<AsyncVar<ServerDBInfo> const> serverDBInfo;
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

	ACTOR static Future<Void> getLastSeenVersion(LocalConfigurationImpl *self) {
		state Optional<Value> lastSeenVersionValue = wait(self->kvStore->readValue(lastSeenVersionKey));
		if (!lastSeenVersionValue.present()) {
			self->lastSeenVersion = 0;
			self->kvStore->set(KeyValueRef(lastSeenVersionKey, BinaryWriter::toValue(self->lastSeenVersion, IncludeVersion())));
			wait(self->kvStore->commit());
			return Void();
		}
		self->lastSeenVersion = BinaryReader::fromStringRef<Version>(lastSeenVersionValue.get(), IncludeVersion());
		return Void();
	}

	ACTOR static Future<Void> init(LocalConfigurationImpl* self) {
		self->testKnobs.initialize();
		wait(self->kvStore->init());
		wait(getLastSeenVersion(self));
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

	ACTOR static Future<Void> consume(LocalConfigurationImpl* self) {
		wait(self->initFuture);
		state Future<Void> timeout = Void();
		state Future<ConfigFollowerGetChangesReply> getChangesReply;
		loop {
			choose {
				when(wait(self->serverDBInfo->onChange())) { timeout = Void(); }
				when(wait(timeout)) {
					getChangesReply = self->serverDBInfo->get().configFollowerInterface.get().getChanges.getReply(
					    ConfigFollowerGetChangesRequest{ self->lastSeenVersion, {} });
					timeout = Future<Void>{};
				}
				when(ConfigFollowerGetChangesReply reply = wait(getChangesReply)) {
					// TODO: Handle reply
					timeout = delay(0.5); // TODO: Make knob?
				}
			}
		}
	}

public:
	LocalConfigurationImpl(ConfigClassSet const& configClasses,
	                       std::string const& dataFolder,
	                       Reference<AsyncVar<ServerDBInfo> const> const& serverDBInfo)
	  : configClasses(configClasses), serverDBInfo(serverDBInfo) {
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

	Future<Void> consume() { return consume(this); }
};

LocalConfiguration::LocalConfiguration(ConfigClassSet const& configClasses,
                                       std::string const& dataFolder,
                                       Reference<AsyncVar<ServerDBInfo> const> const& serverDBInfo)
  : impl(std::make_unique<LocalConfigurationImpl>(configClasses, dataFolder, serverDBInfo)) {}

LocalConfiguration::~LocalConfiguration() = default;

Future<Void> LocalConfiguration::init() {
	return impl->init();
}

TestKnobs const &LocalConfiguration::getKnobs() const {
	return impl->getKnobs();
}

Future<Void> LocalConfiguration::consume() {
	return impl->consume();
}

#define init(knob, value) initKnob(knob, value, #knob)

void TestKnobs::initialize() {
	init(TEST, 0);
}
