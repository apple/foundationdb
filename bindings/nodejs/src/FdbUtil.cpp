/*
 * FdbUtil.cpp
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
#include <node_buffer.h>
#include "FdbUtil.h"

using namespace v8;

Handle<Value> ToFloat(const Arguments &args) {
    HandleScope scope;

    if (args.Length() != 1) {
        return ThrowException(Exception::TypeError(String::NewSymbol("Wrong number of arguments (must be exactly 1)")));
    }

    if (!args[0]->IsNumber()) {
        return ThrowException(Exception::TypeError(String::NewSymbol("Argument is not a Number")));
    }

    float value = (float)args[0]->NumberValue();
    Handle<Value> jsValue = Number::New(value);

    return scope.Close(jsValue);
}
