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
#include <variant>

#include "fdbclient/FDBTypes.h"

class KnobValueRef {
	std::variant<int, double, int64_t, bool, ValueRef> value;
	template <class T>
	explicit KnobValueRef(T const& v) : value(std::in_place_type<T>, v) {}

	explicit KnobValueRef(Arena& arena, ValueRef const& v) : value(std::in_place_type<ValueRef>, arena, v) {}

	struct CreatorFunc {
		Standalone<KnobValueRef> operator()(NoKnobFound const&) const {
			ASSERT(false);
			return {};
		}
		template <class T>
		Standalone<KnobValueRef> operator()(T const& v) const {
			return KnobValueRef(v);
		}
		Standalone<KnobValueRef> operator()(std::string const& v) const {
			Standalone<KnobValueRef> knobValue;
			return KnobValueRef(ValueRef(reinterpret_cast<uint8_t const*>(v.c_str()), v.size()));
		}
	};

	struct ToStringFunc {
		std::string operator()(int v) const { return format("%d", v); }
		std::string operator()(int64_t v) const { return format("%ld", v); }
		std::string operator()(bool v) const { return format("%d", v); }
		std::string operator()(ValueRef v) const { return v.toString(); }
		std::string operator()(double const& v) const { return format("%lf", v); }
	};

	template <class KnobsType>
	class SetKnobFunc {
		KnobsType* knobs;
		std::string const* knobName;

	public:
		SetKnobFunc(KnobsType& knobs, std::string const& knobName) : knobs(&knobs), knobName(&knobName) {}
		template <class T>
		bool operator()(T const& v) const {
			return knobs->setKnob(*knobName, v);
		}
		bool operator()(StringRef const& v) const { return knobs->setKnob(*knobName, v.toString()); }
	};

public:
	static constexpr FileIdentifier file_identifier = 9297109;

	KnobValueRef() = default;

	explicit KnobValueRef(Arena& arena, KnobValueRef const& rhs) : value(rhs.value) {
		if (std::holds_alternative<ValueRef>(value)) {
			value = ValueRef(arena, std::get<ValueRef>(value));
		}
	}

	static Standalone<KnobValueRef> create(ParsedKnobValue const& v) { return std::visit(CreatorFunc{}, v); }

	size_t expectedSize() const {
		return std::holds_alternative<KeyRef>(value) ? std::get<KeyRef>(value).expectedSize() : 0;
	}

	template <class KnobsType>
	bool setKnob(KnobsType& knobs, KeyRef knobName) {
		return std::visit(value, [&knobs, knobName](auto& v) { return knobs.setKnob(knobName, v); });
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}

	std::string toString() const { return std::visit(ToStringFunc{}, value); }

	Value toValue() const {
		auto s = toString();
		return ValueRef(reinterpret_cast<uint8_t const*>(s.c_str()), s.size());
	}

	bool setKnob(std::string const& knobName, Knobs& knobs) const {
		return std::visit(SetKnobFunc<Knobs>{ knobs, knobName }, value);
	}
};

using KnobValue = Standalone<KnobValueRef>;

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
