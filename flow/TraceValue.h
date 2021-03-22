/*
 * TraceValue.h
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

#ifndef __FLOW_TRACE_VALUE__
#define __FLOW_TRACE_VALUE__
#pragma once

#include <string>

struct TraceBool {
	constexpr static FileIdentifier file_identifier = 7918345;
	bool value;

	TraceBool(bool value = false) : value(value) {}

	std::string toString() const;

	static constexpr size_t size() { return 1; }
	static constexpr void truncate(int) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};

struct TraceString final {
	constexpr static FileIdentifier file_identifier = 8923844;
	std::string value;

	TraceString(std::string const& value = "") : value(value) {}
	TraceString(std::string&& value) : value(std::move(value)) {}
	std::string const& toString() const& { return value; }
	std::string toString() && { return std::move(value); }
	size_t size() const { return value.size(); }
	void truncate(int maxFieldLength);

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};

struct TraceNumeric final {
	constexpr static FileIdentifier file_identifier = 285900351;
	std::string value;

	TraceNumeric(std::string const& value = "") : value(value) {}
	TraceNumeric(std::string&& value) : value(std::move(value)) {}
	std::string toString() const& { return value; }
	std::string toString() && { return std::move(value); }

	size_t size() const { return value.size(); }
	static constexpr void truncate(int) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};

struct TraceCounter {
	constexpr static FileIdentifier file_identifier = 19843497;
	double rate;
	double roughness;
	int64_t value;

	TraceCounter() : rate(0.0), roughness(0.0), value(0) {}
	TraceCounter(double rate, double roughness, int64_t value) : rate(rate), roughness(roughness), value(value) {}

	std::string toString() const;
	size_t size() const {
		return toString().size();
	}
	static constexpr void truncate(int) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, rate, roughness, value);
	}
};

struct TraceVector {
	constexpr static FileIdentifier file_identifier = 6925899;
	int maxFieldLength{ -1 };
	std::vector<struct TraceValue> values;

	TraceVector() = default;
	void push_back(TraceValue&&);
	size_t size() const;
	void truncate(int maxFieldLength);
	std::string toString() const;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, maxFieldLength, values);
	}
};

struct TraceValue {
	std::variant<TraceString, TraceBool, TraceCounter, TraceNumeric, TraceVector> value;
	template <class T, class... Args>
	explicit TraceValue(std::in_place_type_t<T> typeId, Args&&... args) : value(typeId, std::forward<Args>(args)...) {}

public:
	constexpr static FileIdentifier file_identifier = 2947802;
	TraceValue(std::string const& value = "");
	TraceValue(std::string&& value);

	template <class T, class... Args>
	static TraceValue create(Args&&... args) {
		return TraceValue(std::in_place_type<T>, std::forward<Args>(args)...);
	}
	template <class T>
	T const& get() const& {
		ASSERT(std::holds_alternative<T>(value));
		return std::get<T>(value);
	}
	template <class T>
	T& get() & {
		ASSERT(std::holds_alternative<T>(value));
		return std::get<T>(value);
	}
	template <class T>
	T&& get() && {
		ASSERT(std::holds_alternative<T>(value));
		return std::get<T>(std::move(value));
	}

	std::string toString() const&;
	std::string toString() &&;

	template <class Formatter>
	std::string format(Formatter const& f) const {
		return std::visit(f, value);
	}

	size_t size() const;
	void truncate(int maxFieldLength);

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};

#endif
