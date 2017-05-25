/*
 * FdbOptions.h
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


#ifndef FDB_NODE_FDB_OPTIONS_H
#define FDB_NODE_FDB_OPTIONS_H

#define ADD_OPTION(scope, name, value, type) AddOption(scope, name, value, type)

#include "Version.h"

#include <foundationdb/fdb_c.h>
#include <node.h>
#include <string>
#include <map>

struct Parameter {
	Parameter() : isNull(true), errorCode(0) { }
	Parameter(std::string param) : param(param), isNull(false), errorCode(0) { }
	Parameter(fdb_error_t errorCode) : isNull(false), errorCode(errorCode) { }

	std::string param;
	bool isNull;
	fdb_error_t errorCode;

	uint8_t const* getValue() { return isNull ? NULL : (uint8_t const*)param.c_str(); }
	int getLength() { return isNull ? 0 : (int)param.size(); }
};

struct ScopeInfo {
	std::string templateClassName;
	v8::Handle<v8::Value> (*optionFunction) (const v8::Arguments &args);

	ScopeInfo() { }
	ScopeInfo(std::string templateClassName, v8::Handle<v8::Value> (*optionFunction) (const v8::Arguments &args)) {
		this->templateClassName = templateClassName;
		this->optionFunction = optionFunction;
	}
};

class FdbOptions : node::ObjectWrap {
	public:
		static void Init();

		enum ParameterType {
			None,
			Int,
			String,
			Bytes
		};

		enum Scope {
			NetworkOption,
			ClusterOption,
			DatabaseOption,
			TransactionOption,
			StreamingMode,
			MutationType,
			ConflictRangeType
		};

		static v8::Handle<v8::Value> CreateOptions(Scope scope, v8::Handle<v8::Value> source = v8::Null());
		static v8::Handle<v8::Value> CreateEnum(Scope scope);

		static Parameter GetOptionParameter(const v8::Arguments &args, Scope scope, int optionValue, int index = 0);

		v8::Persistent<v8::Value> GetSource() {
			return source;
		}

	private:
		static v8::Handle<v8::Value> New(const v8::Arguments &args);
		static v8::Handle<v8::Value> NewInstance(v8::Persistent<v8::FunctionTemplate> optionsTemplate, v8::Handle<v8::Value> source);

		FdbOptions();

		static void InitOptionsTemplate(v8::Persistent<v8::FunctionTemplate> &tpl, const char *className);
		static void InitOptions();

		static void AddOption(Scope scope, std::string name, int value, ParameterType type);
		static void WeakCallback(v8::Persistent<v8::Value> value, void *data);

		static std::string ToJavaScriptName(std::string optionName, bool isSetter);

		static std::map<Scope, ScopeInfo> scopeInfo;
		static std::map<Scope, v8::Persistent<v8::FunctionTemplate>> optionTemplates;
		static std::map<Scope, std::map<int, ParameterType>> parameterTypes;

		v8::Persistent<v8::Value> source;
};

#endif
