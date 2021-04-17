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
#include "fdbserver/LocalConfiguration.h"
#include "flow/actorcompiler.h" // This must be the last #include.

static const KeyRef pathKey = LiteralStringRef("path");
static const KeyRef updatesKey = LiteralStringRef("updates");

class LocalConfigurationImpl {
	IKeyValueStore* kvStore; // FIXME: fix leaks?
	ConfigPath configPath;
	Optional<KnobUpdates> updates;
	Future<Void> init;
	bool read{ false };
	ConfigFollowerInterface consumer;

	ACTOR static Future<Void> initialize(LocalConfigurationImpl* self) {
		wait(self->kvStore->init());
		Optional<Value> path = wait(self->kvStore->readValue(pathKey));
		if (!path.present()) {
			return Void();
		}
		ConfigPath durablePath;
		BinaryReader br(path.get(), IncludeVersion());
		br >> durablePath;
		if (durablePath == self->configPath) {
			Optional<Value> updatesValue = wait(self->kvStore->readValue(updatesKey));
			ASSERT(updatesValue.present()); // FIXME: Possible race condition?
			BinaryReader br(updatesValue.get(), IncludeVersion());
			KnobUpdates updates;
			br >> updates;
			self->updates = updates;
		}
		return Void();
	}

	ACTOR static Future<Optional<KnobUpdates>> getUpdates(LocalConfigurationImpl* self) {
		ASSERT(false);
		wait(self->init);
		self->read = true;
		return self->updates;
	}

	ACTOR static Future<Void> saveUpdates(LocalConfigurationImpl* self, KnobUpdates updates) {
		{
			BinaryWriter bw(IncludeVersion());
			bw << self->configPath;
			self->kvStore->set(KeyValueRef(pathKey, bw.toValue()));
		}
		{
			BinaryWriter bw(IncludeVersion());
			bw << updates;
			self->kvStore->set(KeyValueRef(updatesKey, bw.toValue()));
		}
		wait(self->kvStore->commit());
		return Void();
	}

public:
	LocalConfigurationImpl(ConfigPath const& configPath, std::string const& dataFolder) : configPath(configPath) {
		platform::createDirectory(dataFolder);
		kvStore = keyValueStoreMemory(joinPath(dataFolder, "localconf-"), UID{}, 500e6);
		init = (initialize(this));
	}

	Future<Optional<KnobUpdates>> getUpdates() { return getUpdates(this); }

	Future<Void> saveUpdates(KnobUpdates const& updates) { return saveUpdates(this, updates); }
};

LocalConfiguration::LocalConfiguration(ConfigPath const& path, std::string const& dataFolder)
  : impl(std::make_unique<LocalConfigurationImpl>(path, dataFolder)) {}

LocalConfiguration::~LocalConfiguration() = default;

Future<Optional<KnobUpdates>> LocalConfiguration::getUpdates() {
	return impl->getUpdates();
}

Future<Void> LocalConfiguration::saveUpdates(KnobUpdates const& updates) {
	return impl->saveUpdates(updates);
}
