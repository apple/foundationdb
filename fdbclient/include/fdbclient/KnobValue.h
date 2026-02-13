/*
 * KnobValue.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "flow/Knobs.h"

// KnobValueRef stores a parsed knob value with its concrete type.
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
		return std::holds_alternative<ValueRef>(value) ? std::get<ValueRef>(value).expectedSize() : 0;
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
