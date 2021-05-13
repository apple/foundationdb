/*
 * GlobalConfig.actor.h
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

#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_GLOBALCONFIG_ACTOR_G_H)
#define FDBCLIENT_GLOBALCONFIG_ACTOR_G_H
#include "fdbclient/GlobalConfig.actor.g.h"
#elif !defined(FDBCLIENT_GLOBALCONFIG_ACTOR_H)
#define FDBCLIENT_GLOBALCONFIG_ACTOR_H

#include <any>
#include <map>
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
	// Creates a GlobalConfig singleton, accessed by calling GlobalConfig().
	// This function should only be called once by each process (however, it is
	// idempotent and calling it multiple times will have no effect).
	static void create(DatabaseContext* cx, Reference<AsyncVar<ClientDBInfo>> dbInfo);

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

private:
	GlobalConfig();

	// The functions below only affect the local copy of the global
	// configuration keyspace! To insert or remove values across all nodes you
	// must use a transaction (see the note above).

	// Inserts the given key-value pair into the local copy of the global
	// configuration keyspace, overwriting the old key-value pair if it exists.
	// `value` must be encoded using the FDB tuple typecodes.
	void insert(KeyRef key, ValueRef value);
	// Removes the given key (and associated value) from the local copy of the
	// global configuration keyspace.
	void erase(KeyRef key);
	// Removes the given key range (and associated values) from the local copy
	// of the global configuration keyspace.
	void erase(KeyRangeRef range);

	ACTOR static Future<Void> migrate(GlobalConfig* self);
	ACTOR static Future<Void> refresh(GlobalConfig* self);
	ACTOR static Future<Void> updater(GlobalConfig* self, Reference<AsyncVar<ClientDBInfo>> dbInfo);

	Database cx;
	Future<Void> _updater;
	Promise<Void> initialized;
	std::unordered_map<StringRef, Reference<ConfigValue>> data;
	Version lastUpdate;
};

#endif
