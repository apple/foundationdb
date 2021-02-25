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
#include "fdbclient/SystemData.h"
#include "fdbclient/Tuple.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h"  // This must be the last #include.

const KeyRef fdbClientInfoTxnSampleRate = LiteralStringRef("config/fdbClientInfo/client_txn_sample_rate");
const KeyRef fdbClientInfoTxnSizeLimit = LiteralStringRef("config/fdbClientInfo/client_txn_size_limit");

const KeyRef transactionTagSampleRate = LiteralStringRef("config/transactionTagSampleRate");
const KeyRef transactionTagSampleCost = LiteralStringRef("config/transactionTagSampleCost");

GlobalConfig::GlobalConfig() : lastUpdate(0) {}

void GlobalConfig::create(DatabaseContext* cx, Reference<AsyncVar<ClientDBInfo>> dbInfo) {
	if (g_network->global(INetwork::enGlobalConfig) == nullptr) {
		auto config = new GlobalConfig{};
		config->cx = Database(cx);
		g_network->setGlobal(INetwork::enGlobalConfig, config);
		config->_updater = updater(config, dbInfo);
	}
}

GlobalConfig& GlobalConfig::globalConfig() {
	void* res = g_network->global(INetwork::enGlobalConfig);
	ASSERT(res);
	return *reinterpret_cast<GlobalConfig*>(res);
}

const ConfigValue GlobalConfig::get(KeyRef name) {
	auto it = data.find(name);
	if (it == data.end()) {
		return ConfigValue{ Arena(), std::any{} };
	}
	return it->second;
}

const std::map<KeyRef, ConfigValue> GlobalConfig::get(KeyRangeRef range) {
	std::map<KeyRef, ConfigValue> results;
	for (const auto& [key, value] : data) {
		if (range.contains(key)) {
			results[key] = value;
		}
	}
	return results;
}

Future<Void> GlobalConfig::onInitialized() {
	return initialized.getFuture();
}

void GlobalConfig::insert(KeyRef key, ValueRef value) {
	Arena arena(1);
	KeyRef stableKey = KeyRef(arena, key);
	try {
		Tuple t = Tuple::unpack(value);
		if (t.getType(0) == Tuple::ElementType::UTF8) {
			data[stableKey] = ConfigValue{ arena, StringRef(arena, t.getString(0).contents()) };
		} else if (t.getType(0) == Tuple::ElementType::INT) {
			data[stableKey] = ConfigValue{ arena, t.getInt(0) };
		} else if (t.getType(0) == Tuple::ElementType::FLOAT) {
			data[stableKey] = ConfigValue{ arena, t.getFloat(0) };
		} else if (t.getType(0) == Tuple::ElementType::DOUBLE) {
			data[stableKey] = ConfigValue{ arena, t.getDouble(0) };
		} else {
			ASSERT(false);
		}
	} catch (Error& e) {
		TraceEvent("GlobalConfigTupleError").detail("What", e.what());
	}
}

void GlobalConfig::erase(KeyRef key) {
	erase(KeyRangeRef(key, keyAfter(key)));
}

void GlobalConfig::erase(KeyRangeRef range) {
	auto it = data.begin();
	while (it != data.end()) {
		if (range.contains(it->first)) {
			it = data.erase(it);
		} else {
			++it;
		}
	}
}

ACTOR Future<Void> GlobalConfig::refresh(GlobalConfig* self) {
	Transaction tr(self->cx);
	Standalone<RangeResultRef> result = wait(tr.getRange(globalConfigDataKeys, CLIENT_KNOBS->TOO_MANY));
	for (const auto& kv : result) {
		KeyRef systemKey = kv.key.removePrefix(globalConfigKeysPrefix);
		self->insert(systemKey, kv.value);
	}
	return Void();
}

ACTOR Future<Void> GlobalConfig::updater(GlobalConfig* self, Reference<AsyncVar<ClientDBInfo>> dbInfo) {
	wait(self->refresh(self));
	self->initialized.send(Void());

	loop {
		try {
			wait(dbInfo->onChange());

			auto& history = dbInfo->get().history;
			if (history.size() == 0 || (self->lastUpdate < history[0].first && self->lastUpdate != 0)) {
				// This process missed too many global configuration
				// history updates or the protocol version changed, so it
				// must re-read the entire configuration range.
				wait(self->refresh(self));
				if (dbInfo->get().history.size() > 0) {
					self->lastUpdate = dbInfo->get().history.back().contents().first;
				}
			} else {
				// Apply history in order, from lowest version to highest
				// version. Mutation history should already be stored in
				// ascending version order.
				for (int i = 0; i < history.size(); ++i) {
					const std::pair<Version, VectorRef<MutationRef>>& pair = history[i].contents();

					Version version = pair.first;
					if (version <= self->lastUpdate) {
						continue;  // already applied this mutation
					}

					const VectorRef<MutationRef>& mutations = pair.second;
					for (const auto& mutation : mutations) {
						if (mutation.type == MutationRef::SetValue) {
							self->insert(mutation.param1, mutation.param2);
						} else if (mutation.type == MutationRef::ClearRange) {
							self->erase(KeyRangeRef(mutation.param1, mutation.param2));
						} else {
							ASSERT(false);
						}
					}

					ASSERT(version > self->lastUpdate);
					self->lastUpdate = version;
				}
			}
		} catch (Error& e) {
			throw;
		}
	}
}
