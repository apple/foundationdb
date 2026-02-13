/*
 * KnobValue.cpp
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

#include "fdbclient/KnobValue.h"
#include "fdbclient/Tuple.h"
#include "flow/flow.h"
#include "flow/Trace.h"

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

Value KnobValueRef::ToValueFunc::operator()(int v) const {
	return Tuple::makeTuple(v).pack();
}
Value KnobValueRef::ToValueFunc::operator()(int64_t v) const {
	return Tuple::makeTuple(v).pack();
}
Value KnobValueRef::ToValueFunc::operator()(bool v) const {
	return Tuple::makeTuple(v).pack();
}
Value KnobValueRef::ToValueFunc::operator()(ValueRef v) const {
	return Tuple::makeTuple(v).pack();
}
Value KnobValueRef::ToValueFunc::operator()(double v) const {
	return Tuple::makeTuple(v).pack();
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

KnobValue KnobValueRef::create(ParsedKnobValue const& v) {
	return std::visit(CreatorFunc{}, v);
}

bool KnobValueRef::visitSetKnob(std::string const& knobName, Knobs& knobs) const {
	return std::visit(SetKnobFunc{ knobs, knobName }, value);
}

std::string KnobValueRef::toString() const {
	return std::visit(ToStringFunc{}, value);
}
