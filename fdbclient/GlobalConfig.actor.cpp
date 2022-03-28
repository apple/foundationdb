/*
 * GlobalConfig.actor.cpp
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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tuple.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

const KeyRef fdbClientInfoTxnSampleRate = LiteralStringRef("config/fdb_client_info/client_txn_sample_rate");
const KeyRef fdbClientInfoTxnSizeLimit = LiteralStringRef("config/fdb_client_info/client_txn_size_limit");

const KeyRef transactionTagSampleRate = LiteralStringRef("config/transaction_tag_sample_rate");
const KeyRef transactionTagSampleCost = LiteralStringRef("config/transaction_tag_sample_cost");

const KeyRef samplingFrequency = LiteralStringRef("visibility/sampling/frequency");
const KeyRef samplingWindow = LiteralStringRef("visibility/sampling/window");

GlobalConfig::GlobalConfig(Database& cx) : cx(cx), lastUpdate(0) {}

GlobalConfig& GlobalConfig::globalConfig() {
	void* res = g_network->global(INetwork::enGlobalConfig);
	ASSERT(res);
	return *reinterpret_cast<GlobalConfig*>(res);
}

Key GlobalConfig::prefixedKey(KeyRef key) {
	return key.withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::GLOBALCONFIG).begin);
}

const Reference<ConfigValue> GlobalConfig::get(KeyRef name) {
	auto it = data.find(name);
	if (it == data.end()) {
		return Reference<ConfigValue>();
	}
	return it->second;
}

const std::map<KeyRef, Reference<ConfigValue>> GlobalConfig::get(KeyRangeRef range) {
	std::map<KeyRef, Reference<ConfigValue>> results;
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

Future<Void> GlobalConfig::onChange() {
	return configChanged.onTrigger();
}

void GlobalConfig::trigger(KeyRef key, std::function<void(std::optional<std::any>)> fn) {
	callbacks.emplace(key, std::move(fn));
}

void GlobalConfig::insert(KeyRef key, ValueRef value) {
	// TraceEvent(SevInfo, "GlobalConfig_Insert").detail("Key", key).detail("Value", value);
	data.erase(key);

	Arena arena(key.expectedSize() + value.expectedSize());
	KeyRef stableKey = KeyRef(arena, key);
	try {
		std::any any;
		Tuple t = Tuple::unpack(value);
		if (t.getType(0) == Tuple::ElementType::UTF8) {
			any = StringRef(arena, t.getString(0).contents());
		} else if (t.getType(0) == Tuple::ElementType::INT) {
			any = t.getInt(0);
		} else if (t.getType(0) == Tuple::ElementType::BOOL) {
			any = t.getBool(0);
		} else if (t.getType(0) == Tuple::ElementType::FLOAT) {
			any = t.getFloat(0);
		} else if (t.getType(0) == Tuple::ElementType::DOUBLE) {
			any = t.getDouble(0);
		} else {
			ASSERT(false);
		}
		data[stableKey] = makeReference<ConfigValue>(std::move(arena), std::move(any));

		if (callbacks.find(stableKey) != callbacks.end()) {
			callbacks[stableKey](data[stableKey]->value);
		}
	} catch (Error& e) {
		TraceEvent(SevWarn, "GlobalConfigTupleParseError").detail("What", e.what());
	}
}

void GlobalConfig::erase(Key key) {
	erase(KeyRangeRef(key, keyAfter(key)));
}

void GlobalConfig::erase(KeyRangeRef range) {
	// TraceEvent(SevInfo, "GlobalConfig_Erase").detail("Range", range);
	auto it = data.begin();
	while (it != data.end()) {
		if (range.contains(it->first)) {
			if (callbacks.find(it->first) != callbacks.end()) {
				callbacks[it->first](std::nullopt);
			}
			it = data.erase(it);
		} else {
			++it;
		}
	}
}

// Older FDB versions used different keys for client profiling data. This
// function performs a one-time migration of data in these keys to the new
// global configuration key space.
ACTOR Future<Void> GlobalConfig::migrate(GlobalConfig* self) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

	state Key migratedKey("\xff\x02/fdbClientInfo/migrated/"_sr);
	state Optional<Value> migrated = wait(tr->get(migratedKey));
	if (migrated.present()) {
		// Already performed migration.
		return Void();
	}

	state Optional<Value> sampleRate = wait(tr->get(Key("\xff\x02/fdbClientInfo/client_txn_sample_rate/"_sr)));
	state Optional<Value> sizeLimit = wait(tr->get(Key("\xff\x02/fdbClientInfo/client_txn_size_limit/"_sr)));

	try {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		// The value doesn't matter too much, as long as the key is set.
		tr->set(migratedKey.contents(), "1"_sr);
		if (sampleRate.present()) {
			const double sampleRateDbl =
			    BinaryReader::fromStringRef<double>(sampleRate.get().contents(), Unversioned());
			Tuple rate = Tuple().appendDouble(sampleRateDbl);
			tr->set(GlobalConfig::prefixedKey(fdbClientInfoTxnSampleRate), rate.pack());
		}
		if (sizeLimit.present()) {
			const int64_t sizeLimitInt =
			    BinaryReader::fromStringRef<int64_t>(sizeLimit.get().contents(), Unversioned());
			Tuple size = Tuple().append(sizeLimitInt);
			tr->set(GlobalConfig::prefixedKey(fdbClientInfoTxnSizeLimit), size.pack());
		}

		wait(tr->commit());
	} catch (Error& e) {
		// If multiple fdbserver processes are started at once, they will all
		// attempt this migration at the same time, sometimes resulting in
		// aborts due to conflicts. Purposefully avoid retrying, making this
		// migration best-effort.
		TraceEvent(SevInfo, "GlobalConfig_MigrationError").detail("What", e.what());
	}

	return Void();
}

// Updates local copy of global configuration by reading the entire key-range
// from storage.
ACTOR Future<Void> GlobalConfig::refresh(GlobalConfig* self) {
	// TraceEvent trace(SevInfo, "GlobalConfig_Refresh");
	self->erase(KeyRangeRef(""_sr, "\xff"_sr));

	Transaction tr(self->cx);
	tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	RangeResult result = wait(tr.getRange(globalConfigDataKeys, CLIENT_KNOBS->TOO_MANY));
	for (const auto& kv : result) {
		KeyRef systemKey = kv.key.removePrefix(globalConfigKeysPrefix);
		self->insert(systemKey, kv.value);
	}
	return Void();
}

// Applies updates to the local copy of the global configuration when this
// process receives an updated history.
ACTOR Future<Void> GlobalConfig::updater(GlobalConfig* self, const ClientDBInfo* dbInfo) {
	wait(self->cx->onConnected());
	wait(self->migrate(self));

	wait(self->refresh(self));
	self->initialized.send(Void());

	loop {
		try {
			wait(self->dbInfoChanged.onTrigger());

			auto& history = dbInfo->history;
			if (history.size() == 0) {
				continue;
			}

			if (self->lastUpdate < history[0].version) {
				// This process missed too many global configuration
				// history updates or the protocol version changed, so it
				// must re-read the entire configuration range.
				wait(self->refresh(self));
				if (dbInfo->history.size() > 0) {
					self->lastUpdate = dbInfo->history.back().version;
				}
			} else {
				// Apply history in order, from lowest version to highest
				// version. Mutation history should already be stored in
				// ascending version order.
				for (const auto& vh : history) {
					if (vh.version <= self->lastUpdate) {
						continue; // already applied this mutation
					}

					for (const auto& mutation : vh.mutations.contents()) {
						if (mutation.type == MutationRef::SetValue) {
							self->insert(mutation.param1, mutation.param2);
						} else if (mutation.type == MutationRef::ClearRange) {
							self->erase(KeyRangeRef(mutation.param1, mutation.param2));
						} else {
							ASSERT(false);
						}
					}

					ASSERT(vh.version > self->lastUpdate);
					self->lastUpdate = vh.version;
				}
			}

			self->configChanged.trigger();
		} catch (Error& e) {
			throw;
		}
	}
}
