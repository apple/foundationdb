/*
 * ConfigKnobs.h
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

#pragma once

#include <string>

#include "fdbclient/FDBTypes.h"

struct ConfigKeyRef {
	// Empty config class means the update is global
	Optional<KeyRef> configClass;
	KeyRef knobName;

	ConfigKeyRef() = default;
	explicit ConfigKeyRef(Optional<KeyRef> configClass, KeyRef knobName)
	  : configClass(configClass), knobName(knobName) {}
	explicit ConfigKeyRef(Arena& arena, Optional<KeyRef> configClass, KeyRef knobName)
	  : configClass(arena, configClass), knobName(arena, knobName) {}
	explicit ConfigKeyRef(Arena& arena, ConfigKeyRef const& rhs) : ConfigKeyRef(arena, rhs.configClass, rhs.knobName) {}

	static Standalone<ConfigKeyRef> decodeKey(KeyRef const&);

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, configClass, knobName);
	}

	bool operator==(ConfigKeyRef const& rhs) const {
		return (configClass == rhs.configClass) && (knobName == rhs.knobName);
	}
	bool operator!=(ConfigKeyRef const& rhs) const { return !(*this == rhs); }
	size_t expectedSize() const { return configClass.expectedSize() + knobName.expectedSize(); }
};
using ConfigKey = Standalone<ConfigKeyRef>;

inline bool operator<(ConfigKeyRef const& lhs, ConfigKeyRef const& rhs) {
	if (lhs.configClass != rhs.configClass) {
		return lhs.configClass < rhs.configClass;
	} else {
		return lhs.knobName < rhs.knobName;
	}
}

class ConfigMutationRef {
	ConfigKeyRef key;
	Optional<ValueRef> value;
public:
	ConfigMutationRef() = default;

	ConfigMutationRef(ConfigKeyRef key, Optional<ValueRef> value) : key(key), value(value) {}

	ConfigKeyRef getKey() const { return key; }

	Optional<KeyRef> getConfigClass() const { return key.configClass; }

	KeyRef getKnobName() const { return key.knobName; }

	Optional<ValueRef> getValue() const { return value; }

	ConfigMutationRef(Arena& arena, ConfigMutationRef const& rhs) : key(arena, rhs.key), value(arena, rhs.value) {}

	bool isSet() const { return value.present(); }

	static Standalone<ConfigMutationRef> createConfigMutation(KeyRef encodedKey, Optional<ValueRef> value) {
		auto key = ConfigKeyRef::decodeKey(encodedKey);
		return ConfigMutationRef(key, value);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, value);
	}

	size_t expectedSize() const { return key.expectedSize() + value.expectedSize(); }
};
using ConfigMutation = Standalone<ConfigMutationRef>;

struct ConfigCommitAnnotationRef {
	KeyRef description;
	double timestamp{ 0.0 };

	ConfigCommitAnnotationRef() = default;
	explicit ConfigCommitAnnotationRef(KeyRef description, double timestamp)
	  : description(description), timestamp(timestamp) {}
	explicit ConfigCommitAnnotationRef(Arena& arena, ConfigCommitAnnotationRef& rhs)
	  : description(arena, rhs.description), timestamp(rhs.timestamp) {}

	size_t expectedSize() const { return description.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, timestamp, description);
	}
};
using ConfigCommitAnnotation = Standalone<ConfigCommitAnnotationRef>;
