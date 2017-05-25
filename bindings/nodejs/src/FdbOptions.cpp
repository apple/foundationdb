/*
 * FdbOptions.cpp
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


#include "FdbOptions.h"
#include "Cluster.h"
#include "Database.h"
#include "Transaction.h"
#include "FdbError.h"

#include <algorithm>
#include <node_buffer.h>

#define INVALID_OPTION_VALUE_ERROR_CODE (fdb_error_t)2006

using namespace v8;
using namespace node;

std::map<FdbOptions::Scope, Persistent<FunctionTemplate>> FdbOptions::optionTemplates;
std::map<FdbOptions::Scope, ScopeInfo> FdbOptions::scopeInfo;
std::map<FdbOptions::Scope, std::map<int, FdbOptions::ParameterType>> FdbOptions::parameterTypes;

FdbOptions::FdbOptions() { }

void FdbOptions::InitOptionsTemplate(Persistent<FunctionTemplate> &tpl, const char *className) {
	tpl = Persistent<FunctionTemplate>::New(FunctionTemplate::New(New));
	tpl->SetClassName(String::NewSymbol(className));
	tpl->InstanceTemplate()->SetInternalFieldCount(1);
}

void FdbOptions::AddOption(Scope scope, std::string name, int value, ParameterType type) {
	if(scope == NetworkOption || scope == ClusterOption || scope == DatabaseOption || scope == TransactionOption || scope == MutationType) {
		bool isSetter = scope != MutationType;
		optionTemplates[scope]->PrototypeTemplate()->Set(v8::String::NewSymbol(ToJavaScriptName(name, isSetter).c_str()),
														 v8::FunctionTemplate::New(scopeInfo[scope].optionFunction, v8::Integer::New(value))->GetFunction());
		parameterTypes[scope][value] = type;
	}
	else if(scope == StreamingMode) {
		optionTemplates[scope]->PrototypeTemplate()->Set(v8::String::NewSymbol(ToJavaScriptName(name, false).c_str()), v8::Integer::New(value));
	}
	else if(scope == ConflictRangeType) {
		//Conflict range type enum is not exposed to JS code
	}
}

Handle<Value> FdbOptions::New(const Arguments &args) {
	FdbOptions *options = new FdbOptions();
	options->Wrap(args.Holder());

	return args.Holder();
}

void FdbOptions::WeakCallback(Persistent<Value> value, void *data) { }

Handle<Value> FdbOptions::NewInstance(Persistent<FunctionTemplate> optionsTemplate, Handle<Value> source) {
	HandleScope scope;

	Local<Object> instance = optionsTemplate->GetFunction()->NewInstance();

	FdbOptions *optionsObj = ObjectWrap::Unwrap<FdbOptions>(instance);
	optionsObj->source = Persistent<Value>::New(source);
	optionsObj->source.MakeWeak(optionsObj, WeakCallback);

	return scope.Close(instance);
}

Handle<Value> FdbOptions::CreateOptions(Scope scope, Handle<Value> source) {
	return NewInstance(optionTemplates[scope], source);
}

Handle<Value> FdbOptions::CreateEnum(Scope scope) {
	return optionTemplates[scope]->GetFunction()->NewInstance();
}

Parameter GetStringParameter(const Arguments &args, int index) {
	if(args.Length() <= index || (!Buffer::HasInstance(args[index]) && !args[index]->IsString()))
		return INVALID_OPTION_VALUE_ERROR_CODE;
	else if(args[index]->IsString()) {
		String::Utf8Value val(args[index]);
		return std::string(*val, val.length());
	}
	else
		return std::string(Buffer::Data(args[index]->ToObject()), Buffer::Length(args[index]->ToObject()));
};

Parameter FdbOptions::GetOptionParameter(const Arguments &args, Scope scope, int optionValue, int index) {
	if(args.Length() > index) {
		int64_t val;
		switch(parameterTypes[scope][optionValue]) {
			case FdbOptions::String:
				return GetStringParameter(args, index);

			case FdbOptions::Bytes:
				if(!Buffer::HasInstance(args[index]))
					return INVALID_OPTION_VALUE_ERROR_CODE;

				return std::string(Buffer::Data(args[index]->ToObject()), Buffer::Length(args[index]->ToObject()));

			case FdbOptions::Int:
				if(!args[index]->IsNumber())
					return INVALID_OPTION_VALUE_ERROR_CODE;
				val = args[index]->IntegerValue();
				return std::string((const char*)&val, 8);


			case FdbOptions::None:
				return Parameter();
		}
	}

	return Parameter();
}

v8::Handle<v8::Value> SetNetworkOption(const Arguments &args) {
	FDBNetworkOption op = (FDBNetworkOption)args.Data()->Uint32Value();

	Parameter param = FdbOptions::GetOptionParameter(args, FdbOptions::NetworkOption, op);
	fdb_error_t errorCode = param.errorCode;
	if(errorCode == 0)
		errorCode = fdb_network_set_option(op, param.getValue(), param.getLength());

	if(errorCode)
		return ThrowException(FdbError::NewInstance(errorCode, fdb_get_error(errorCode)));

	return Null();
}

v8::Handle<v8::Value> SetClusterOption(const Arguments &args) {
	FdbOptions *options = ObjectWrap::Unwrap<FdbOptions>(args.Holder());
	Cluster *cluster = ObjectWrap::Unwrap<Cluster>(options->GetSource()->ToObject());
	FDBClusterOption op = (FDBClusterOption)args.Data()->Uint32Value();

	Parameter param = FdbOptions::GetOptionParameter(args, FdbOptions::ClusterOption, op);
	fdb_error_t errorCode = param.errorCode;
	if(errorCode == 0)
		errorCode = fdb_cluster_set_option(cluster->GetCluster(), op, param.getValue(), param.getLength());

	if(errorCode)
		return ThrowException(FdbError::NewInstance(errorCode, fdb_get_error(errorCode)));

	return Null();
}

v8::Handle<v8::Value> SetDatabaseOption(const Arguments &args) {
	FdbOptions *options = ObjectWrap::Unwrap<FdbOptions>(args.Holder());
	Database *db = ObjectWrap::Unwrap<Database>(options->GetSource()->ToObject());
	FDBDatabaseOption op = (FDBDatabaseOption)args.Data()->Uint32Value();

	Parameter param = FdbOptions::GetOptionParameter(args, FdbOptions::DatabaseOption, op);
	fdb_error_t errorCode = param.errorCode;
	if(errorCode == 0)
		errorCode = fdb_database_set_option(db->GetDatabase(), op, param.getValue(), param.getLength());

	if(errorCode)
		return ThrowException(FdbError::NewInstance(errorCode, fdb_get_error(errorCode)));

	return Null();
}

v8::Handle<v8::Value> SetTransactionOption(const Arguments &args) {
	FdbOptions *options = ObjectWrap::Unwrap<FdbOptions>(args.Holder());
	Transaction *tr = ObjectWrap::Unwrap<Transaction>(options->GetSource()->ToObject());
	FDBTransactionOption op = (FDBTransactionOption)args.Data()->Uint32Value();

	Parameter param = FdbOptions::GetOptionParameter(args, FdbOptions::TransactionOption, op);
	fdb_error_t errorCode = param.errorCode;
	if(errorCode == 0)
		errorCode = fdb_transaction_set_option(tr->GetTransaction(), op, param.getValue(), param.getLength());

	if(errorCode)
		return ThrowException(FdbError::NewInstance(errorCode, fdb_get_error(errorCode)));

	return Null();
}

v8::Handle<v8::Value> CallAtomicOperation(const Arguments &args) {
	Transaction *tr = ObjectWrap::Unwrap<Transaction>(args.Holder());
	Parameter key = GetStringParameter(args, 0);
	Parameter value = GetStringParameter(args, 1);

	fdb_error_t errorCode = key.errorCode > 0 ? key.errorCode : value.errorCode;
	if(errorCode > 0)
		return ThrowException(FdbError::NewInstance(errorCode, fdb_get_error(errorCode)));

	fdb_transaction_atomic_op(tr->GetTransaction(), key.getValue(), key.getLength(), value.getValue(), value.getLength(), (FDBMutationType)args.Data()->Uint32Value());

	return Null();
}

//Converts names using underscores as word separators to camel case (but preserves existing capitalization, if present). If isSetter, prepends the word 'set' to each name
std::string FdbOptions::ToJavaScriptName(std::string optionName, bool isSetter) {
	if(isSetter)
		optionName = "set_" + optionName;

	size_t start = 0;
	while(start < optionName.size()) {
		if(start != 0)
			optionName[start] = ::toupper(optionName[start]);
		size_t index = optionName.find_first_of('_', start);
		if(index == std::string::npos)
			break;

		optionName.erase(optionName.begin() + index);

		start = index;
	}

	return optionName;
}

void FdbOptions::Init() {
	scopeInfo[NetworkOption] = ScopeInfo("FdbNetworkOptions", SetNetworkOption);
	scopeInfo[ClusterOption] = ScopeInfo("FdbClusterOptions", SetClusterOption);
	scopeInfo[DatabaseOption] = ScopeInfo("FdbDatabaseOptions", SetDatabaseOption);
	scopeInfo[TransactionOption] = ScopeInfo("FdbTransactionOptions", SetTransactionOption);
	scopeInfo[StreamingMode] = ScopeInfo("FdbStreamingMode", NULL);
	scopeInfo[MutationType] = ScopeInfo("AtomicOperations", CallAtomicOperation);
	//scopeInfo[ConflictRangeType] = ScopeInfo("ConflictRangeType", NULL);

	for(auto itr = scopeInfo.begin(); itr != scopeInfo.end(); ++itr)
		InitOptionsTemplate(optionTemplates[itr->first], itr->second.templateClassName.c_str());

	InitOptions();
}
