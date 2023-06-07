/*
 * BooleanParam.h
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

class BooleanParam {
	bool value;

public:
	explicit constexpr BooleanParam(bool value) : value(value) {}
	constexpr operator bool() const { return value; }
	constexpr void set(bool value) { this->value = value; }
};

// Declares a boolean parametr with the desired name. This declaration can be nested inside of a namespace or another
// class. This macro should not be used directly unless this boolean parameter is going to be defined as a nested class.
#define FDB_DECLARE_BOOLEAN_PARAM(ParamName)                                                                           \
	class ParamName : public BooleanParam {                                                                            \
	public:                                                                                                            \
		explicit constexpr ParamName(bool value) : BooleanParam(value) {}                                              \
		static ParamName const True;                                                                                   \
		static ParamName const False;                                                                                  \
	}

// Defines the static members True and False of a boolean parameter.
// This macro should not be used directly unless this boolean parameter is going to be defined as a nested class.
// For a nested class, it is necessary to specify the fully qualified name for the boolean param.
#define FDB_DEFINE_BOOLEAN_PARAM(ParamName)                                                                            \
	inline ParamName const ParamName::True = ParamName(true);                                                          \
	inline ParamName const ParamName::False = ParamName(false)

// Declares and defines a boolean parameter. Use this macro unless this parameter will be nested inside of another
// class. In that case, declare the boolean parameter inside of the class using FDB_DECLARE_BOOLEAN_PARAM and define it
// outside the class using FDB_DEFINE_BOOLEAN_PARAM with a fully-qualified name.
#define FDB_BOOLEAN_PARAM(ParamName)                                                                                   \
	FDB_DECLARE_BOOLEAN_PARAM(ParamName);                                                                              \
	FDB_DEFINE_BOOLEAN_PARAM(ParamName)
