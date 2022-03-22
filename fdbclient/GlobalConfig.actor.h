/*
 * GlobalConfig.actor.h
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

#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_GLOBALCONFIG_ACTOR_G_H)
#define FDBCLIENT_GLOBALCONFIG_ACTOR_G_H
#include "fdbclient/GlobalConfig.actor.g.h"
#elif !defined(FDBCLIENT_GLOBALCONFIG_ACTOR_H)
#define FDBCLIENT_GLOBALCONFIG_ACTOR_H

#include <any>
#include <map>
#include <optional>
#include <type_traits>
#include <unordered_map>

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/GlobalConfig.h"
#include "fdbclient/ReadYourWrites.h"

#include "flow/actorcompiler.h" // has to be last include

// The global configuration is a series of typed key-value pairs synced to all
// nodes (server and client) in an FDB cluster in an eventually consistent
// manner. Only small key-value pairs should be stored in global configuration;
// an excessive amount of data can cause synchronization slowness.

// Keys
extern const KeyRef fdbClientInfoTxnSampleRate;
extern const KeyRef fdbClientInfoTxnSizeLimit;

extern const KeyRef transactionTagSampleRate;
extern const KeyRef transactionTagSampleCost;

extern const KeyRef samplingFrequency;
extern const KeyRef samplingWindow;

// Structure used to hold the values stored by global configuration. The arena
// is used as memory to store both the key and the value (the value is only
// stored in the arena if it is an object; primitives are just copied).
struct ConfigValue : ReferenceCounted<ConfigValue> {
	Arena arena;
	std::any value;

	ConfigValue() {}
	ConfigValue(Arena&& a, std::any&& v) : arena(a), value(v) {}
};

class GlobalConfig : NonCopyable {
public:
	// Creates a GlobalConfig singleton, accessed by calling
	// GlobalConfig::globalConfig(). This function requires a database object
	// to allow global configuration to run transactions on the database, and
	// an AsyncVar object to watch for changes on. The ClientDBInfo pointer
	// should point to a ClientDBInfo object which will contain the updated
	// global configuration history when the given AsyncVar changes. This
	// function should be called whenever the database object changes, in order
	// to allow global configuration to run transactions on the latest
	// database.
	template <class T>
	static void create(Database& cx, Reference<AsyncVar<T> const> db, const ClientDBInfo* dbInfo) {
		if (g_network->global(INetwork::enGlobalConfig) == nullptr) {
			auto config = new GlobalConfig{ cx };
			g_network->setGlobal(INetwork::enGlobalConfig, config);
			config->_updater = updater(config, dbInfo);
			// Bind changes in `db` to the `dbInfoChanged` AsyncTrigger.
			// TODO: Change AsyncTrigger to a Reference
			forward(db, std::addressof(config->dbInfoChanged));
		} else {
			GlobalConfig* config = reinterpret_cast<GlobalConfig*>(g_network->global(INetwork::enGlobalConfig));
			config->cx = cx;
		}
	}

	// Returns a reference to the global GlobalConfig object. Clients should
	// call this function whenever they need to read a value out of the global
	// configuration.
	static GlobalConfig& globalConfig();

	// Use this function to turn a global configuration key defined above into
	// the full path needed to set the value in the database.
	//
	// For example, given "config/a", returns "\xff\xff/global_config/config/a".
	static Key prefixedKey(KeyRef key);

	// Get a value from the framework. Values are returned as a ConfigValue
	// reference which also contains the arena holding the object. As long as
	// the caller keeps the ConfigValue reference, the value is guaranteed to
	// be readable. An empty reference is returned if the value does not exist.
	const Reference<ConfigValue> get(KeyRef name);
	const std::map<KeyRef, Reference<ConfigValue>> get(KeyRangeRef range);

	// For arithmetic value types, returns a copy of the value for the given
	// key, or the supplied default value if the framework does not know about
	// the key.
	template <typename T, typename std::enable_if<std::is_arithmetic<T>{}, bool>::type = true>
	const T get(KeyRef name, T defaultVal) {
		try {
			auto configValue = get(name);
			if (configValue.isValid()) {
				if (configValue->value.has_value()) {
					return std::any_cast<T>(configValue->value);
				}
			}

			return defaultVal;
		} catch (Error& e) {
			throw;
		}
	}

	// Trying to write into the global configuration keyspace? To write data,
	// submit a transaction to \xff\xff/global_config/<your-key> with
	// <your-value> encoded using the FDB tuple typecodes. Use the helper
	// function `prefixedKey` to correctly prefix your global configuration
	// key.

	// Triggers the returned future when the global configuration singleton has
	// been created and is ready.
	Future<Void> onInitialized();

	// Triggers the returned future when any key-value pair in the global
	// configuration changes.
	Future<Void> onChange();

	// Calls \ref fn when the value associated with \ref key is changed. \ref
	// key should be one of the string literals defined at the top of
	// GlobalConfig.actor.cpp, to ensure memory validity. \ref fn is passed the
	// updated value for the key, or an empty optional if the key has been
	// cleared. If the value is an allocated object, its memory remains in the
	// control of global configuration.
	void trigger(KeyRef key, std::function<void(std::optional<std::any>)> fn);

private:
	GlobalConfig(Database& cx);

	// The functions below only affect the local copy of the global
	// configuration keyspace! To insert or remove values across all nodes you
	// must use a transaction (see the note above).

	// Inserts the given key-value pair into the local copy of the global
	// configuration keyspace, overwriting the old key-value pair if it exists.
	// `value` must be encoded using the FDB tuple typecodes.
	void insert(KeyRef key, ValueRef value);
	// Removes the given key (and associated value) from the local copy of the
	// global configuration keyspace.
	void erase(Key key);
	// Removes the given key range (and associated values) from the local copy
	// of the global configuration keyspace.
	void erase(KeyRangeRef range);

	ACTOR static Future<Void> migrate(GlobalConfig* self);
	ACTOR static Future<Void> refresh(GlobalConfig* self);
	ACTOR static Future<Void> updater(GlobalConfig* self, const ClientDBInfo* dbInfo);

	Database cx;
	AsyncTrigger dbInfoChanged;
	Future<Void> _updater;
	Promise<Void> initialized;
	AsyncTrigger configChanged;
	std::unordered_map<StringRef, Reference<ConfigValue>> data;
	Version lastUpdate;
	// The key should be a global config string literal key (see the top of this class).
	std::unordered_map<KeyRef, std::function<void(std::optional<std::any>)>> callbacks;
};

#endif
