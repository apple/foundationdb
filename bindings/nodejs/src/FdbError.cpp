/*
 * FdbError.cpp
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


#include <node.h>
#include "FdbError.h"

using namespace v8;
using namespace node;

static Persistent<Object> module;

void FdbError::Init( Handle<Object> module ) {
	::module = Persistent<Object>::New( module );
}

Handle<Value> FdbError::NewInstance(fdb_error_t code, const char *description) {
	HandleScope scope;

	Local<Value> constructor = module->Get( String::NewSymbol("FDBError") );
	Local<Object> instance;
	if (!constructor.IsEmpty() && constructor->IsFunction()) {
		Local<Value> constructorArgs[] = { String::New(description), Integer::New(code) };
		instance = Local<Function>::Cast(constructor)->NewInstance(2, constructorArgs);
	} else {
		// We can't find the (javascript) FDBError class, so construct and throw *something*
		instance = Exception::Error(String::New("FDBError class not found.  Unable to deliver error."))->ToObject();
	}

	return scope.Close(instance);
}
