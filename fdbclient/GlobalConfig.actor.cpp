/*
 * GlobalConfig.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/GlobalConfig.actor.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

GlobalConfig::GlobalConfig() : lastUpdate(0) {}

void GlobalConfig::create(DatabaseContext* cx, Reference<AsyncVar<ClientDBInfo>> dbInfo) {
	auto config = new GlobalConfig{}; // TODO: memory leak?
	config->cx = Database(cx);
	g_network->setGlobal(INetwork::enGlobalConfig, config);
	config->_updater = updater(config, dbInfo);
}

GlobalConfig& GlobalConfig::globalConfig() {
	void* res = g_network->global(INetwork::enGlobalConfig);
	ASSERT(res);
	return *reinterpret_cast<GlobalConfig*>(res);
}

const std::any GlobalConfig::get(StringRef name) {
	auto it = data.find(name);
	if (it == data.end()) {
		return nullptr;
	}
	return it->second;
}

Future<Void> GlobalConfig::onInitialized() {
	return initialized.getFuture();
}

void GlobalConfig::insert(KeyRef key, ValueRef value) {
	Tuple t = Tuple::unpack(value);
	// TODO: Add more Tuple types
	if (t.getType(0) == Tuple::ElementType::UTF8) {
		data[key] = t.getString(0);
	} else if (t.getType(0) == Tuple::ElementType::INT) {
		data[key] = t.getInt(0);
	}
}
