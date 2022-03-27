/*
 * ConfigKnobs.cpp
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

#include "fdbclient/ConfigKnobs.h"
#include "fdbclient/Tuple.h"
#include "flow/UnitTest.h"

ConfigKey ConfigKeyRef::decodeKey(KeyRef const& key) {
	Tuple tuple;
	try {
		tuple = Tuple::unpack(key);
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "FailedToUnpackConfigKey").error(e).detail("Key", printable(key));
		throw invalid_config_db_key();
	}
	if (tuple.size() != 2) {
		throw invalid_config_db_key();
	}
	if (tuple.getType(0) == Tuple::NULL_TYPE) {
		return ConfigKeyRef({}, tuple.getString(1));
	} else {
		if (tuple.getType(0) != Tuple::BYTES || tuple.getType(1) != Tuple::BYTES) {
			throw invalid_config_db_key();
		}
		return ConfigKeyRef(tuple.getString(0), tuple.getString(1));
	}
}

Value KnobValueRef::ToValueFunc::operator()(int v) const {
	return BinaryWriter::toValue(v, Unversioned());
}
Value KnobValueRef::ToValueFunc::operator()(int64_t v) const {
	return BinaryWriter::toValue(v, Unversioned());
}
Value KnobValueRef::ToValueFunc::operator()(bool v) const {
	return BinaryWriter::toValue(v, Unversioned());
}
Value KnobValueRef::ToValueFunc::operator()(ValueRef v) const {
	return v;
}
Value KnobValueRef::ToValueFunc::operator()(double v) const {
	return BinaryWriter::toValue(v, Unversioned());
}

KnobValue KnobValueRef::CreatorFunc::operator()(NoKnobFound) const {
	ASSERT(false);
	return {};
}
KnobValue KnobValueRef::CreatorFunc::operator()(int v) const {
	return KnobValueRef(v);
}
KnobValue KnobValueRef::CreatorFunc::operator()(double v) const {
	return KnobValueRef(v);
}
KnobValue KnobValueRef::CreatorFunc::operator()(int64_t v) const {
	return KnobValueRef(v);
}
KnobValue KnobValueRef::CreatorFunc::operator()(bool v) const {
	return KnobValueRef(v);
}
KnobValue KnobValueRef::CreatorFunc::operator()(std::string const& v) const {
	return KnobValueRef(ValueRef(reinterpret_cast<uint8_t const*>(v.c_str()), v.size()));
}

namespace {

class SetKnobFunc {
	Knobs* knobs;
	std::string const* knobName;

public:
	SetKnobFunc(Knobs& knobs, std::string const& knobName) : knobs(&knobs), knobName(&knobName) {}
	template <class T>
	bool operator()(T const& v) const {
		return knobs->setKnob(*knobName, v);
	}
	bool operator()(StringRef const& v) const { return knobs->setKnob(*knobName, v.toString()); }
};

struct ToStringFunc {
	std::string operator()(int v) const { return format("int:%d", v); }
	std::string operator()(int64_t v) const { return format("int64_t:%lld", v); }
	std::string operator()(bool v) const { return format("bool:%d", v); }
	std::string operator()(ValueRef v) const { return "string:" + v.toString(); }
	std::string operator()(double v) const { return format("double:%lf", v); }
};

} // namespace

KnobValue KnobValueRef::create(ParsedKnobValue const& v) {
	return std::visit(CreatorFunc{}, v);
}

bool KnobValueRef::visitSetKnob(std::string const& knobName, Knobs& knobs) const {
	return std::visit(SetKnobFunc{ knobs, knobName }, value);
}

std::string KnobValueRef::toString() const {
	return std::visit(ToStringFunc{}, value);
}

ConfigDBType configDBTypeFromString(std::string const& str) {
	if (str == "disabled") {
		return ConfigDBType::DISABLED;
	} else if (str == "simple") {
		return ConfigDBType::SIMPLE;
	} else if (str == "paxos") {
		return ConfigDBType::PAXOS;
	} else {
		TraceEvent(SevWarnAlways, "InvalidConfigDBString");
		return ConfigDBType::DISABLED;
	}
}

std::string configDBTypeToString(ConfigDBType configDBType) {
	switch (configDBType) {
	case ConfigDBType::DISABLED:
		return "disabled";
	case ConfigDBType::SIMPLE:
		return "simple";
	case ConfigDBType::PAXOS:
		return "paxos";
	default:
		ASSERT(false);
		return "";
	}
}

TEST_CASE("/fdbclient/ConfigDB/ConfigKey/EncodeDecode") {
	Tuple tuple;
	tuple << "class-A"_sr
	      << "test_long"_sr;
	auto packed = tuple.pack();
	auto unpacked = ConfigKeyRef::decodeKey(packed);
	ASSERT(unpacked.configClass.get() == "class-A"_sr);
	ASSERT(unpacked.knobName == "test_long"_sr);
	return Void();
}

namespace {

void decodeFailureTest(KeyRef key) {
	try {
		ConfigKeyRef::decodeKey(key);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_invalid_config_db_key);
		return;
	}
	ASSERT(false);
}

} // namespace

TEST_CASE("/fdbclient/ConfigDB/ConfigKey/DecodeFailure") {
	{
		Tuple tuple;
		tuple << "s1"_sr
		      << "s2"_sr
		      << "s3"_sr;
		decodeFailureTest(tuple.pack());
	}
	{
		Tuple tuple;
		tuple << "s1"_sr << 5;
		decodeFailureTest(tuple.pack());
	}
	decodeFailureTest("non-tuple-key"_sr);
	return Void();
}
