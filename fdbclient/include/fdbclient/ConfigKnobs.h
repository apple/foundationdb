/*
 * ConfigKnobs.h
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

#include <string>
#include <variant>

#include "fdbclient/FDBTypes.h"

/*
 * KnobValueRefs are stored in the configuration database, and in local configuration files. They are created from
 * ParsedKnobValue objects, so it is assumed that the value type is correct for the corresponding knob name
 */
class KnobValueRef {
	std::variant<int, double, int64_t, bool, ValueRef> value;
	template <class T>
	explicit KnobValueRef(T const& v) : value(std::in_place_type<T>, v) {}

	explicit KnobValueRef(Arena& arena, ValueRef const& v) : value(std::in_place_type<ValueRef>, arena, v) {}

	struct CreatorFunc {
		Standalone<KnobValueRef> operator()(NoKnobFound) const;
		Standalone<KnobValueRef> operator()(int) const;
		Standalone<KnobValueRef> operator()(double) const;
		Standalone<KnobValueRef> operator()(int64_t) const;
		Standalone<KnobValueRef> operator()(bool) const;
		Standalone<KnobValueRef> operator()(std::string const& v) const;
	};

	struct ToValueFunc {
		Value operator()(int) const;
		Value operator()(int64_t) const;
		Value operator()(bool) const;
		Value operator()(ValueRef) const;
		Value operator()(double) const;
	};

public:
	static constexpr FileIdentifier file_identifier = 9297109;

	template <class T>
	static Value toValue(T const& v) {
		return ToValueFunc{}(v);
	}

	KnobValueRef() = default;

	explicit KnobValueRef(Arena& arena, KnobValueRef const& rhs) : value(rhs.value) {
		if (std::holds_alternative<ValueRef>(value)) {
			value = ValueRef(arena, std::get<ValueRef>(value));
		}
	}

	static Standalone<KnobValueRef> create(ParsedKnobValue const& v);

	size_t expectedSize() const {
		return std::holds_alternative<KeyRef>(value) ? std::get<KeyRef>(value).expectedSize() : 0;
	}

	Value toValue() const { return std::visit(ToValueFunc{}, value); }

	// Returns true if and only if the knob was successfully found and set
	bool visitSetKnob(std::string const& knobName, Knobs& knobs) const;

	std::string toString() const;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};

using KnobValue = Standalone<KnobValueRef>;

/*
 * In the configuration database, each key contains a configuration class (or no configuration class, in the case of
 * global updates), and a knob name
 */
struct ConfigKeyRef {
	static constexpr FileIdentifier file_identifier = 5918726;

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

/*
 * Only set and point clear configuration database mutations are currently permitted.
 */
class ConfigMutationRef {
	ConfigKeyRef key;
	// Empty value means this is a clear mutation
	Optional<KnobValueRef> value;

public:
	static constexpr FileIdentifier file_identifier = 7219528;

	ConfigMutationRef() = default;

	explicit ConfigMutationRef(Arena& arena, ConfigKeyRef key, Optional<KnobValueRef> value)
	  : key(arena, key), value(arena, value) {}

	explicit ConfigMutationRef(ConfigKeyRef key, Optional<KnobValueRef> value) : key(key), value(value) {}

	ConfigKeyRef getKey() const { return key; }

	Optional<KeyRef> getConfigClass() const { return key.configClass; }

	KeyRef getKnobName() const { return key.knobName; }

	KnobValueRef getValue() const { return value.get(); }

	ConfigMutationRef(Arena& arena, ConfigMutationRef const& rhs) : key(arena, rhs.key), value(arena, rhs.value) {}

	bool isSet() const { return value.present(); }

	static Standalone<ConfigMutationRef> createConfigMutation(KeyRef encodedKey, KnobValueRef value) {
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

/*
 * Each configuration database commit is annotated with:
 *   - A description (set manually by the client)
 *   - A commit timestamp (automatically generated at commit time)
 */
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

ConfigDBType configDBTypeFromString(std::string const&);
std::string configDBTypeToString(ConfigDBType);
