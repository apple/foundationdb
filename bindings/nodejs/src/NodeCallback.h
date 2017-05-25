/*
 * NodeCallback.h
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


#ifndef FDB_NODE_NODE_CALLBACK_H
#define FDB_NODE_NODE_CALLBACK_H

#include "FdbError.h"

#include <v8.h>
#include <cstdlib>
#include <stdio.h>
#include <string.h>
#include <node.h>
#include <node_buffer.h>
#include <node_version.h>
#include <foundationdb/fdb_c.h>

#if NODE_VERSION_AT_LEAST(0, 7, 9)
#else
#error Node version too old
#endif

using namespace std;
using namespace v8;
using namespace node;

struct NodeCallback {

public:
	NodeCallback(FDBFuture *future, Persistent<Function> cbFunc) : future(future), cbFunc(cbFunc), refCount(1) {
		uv_async_init(uv_default_loop(), &handle, &NodeCallback::nodeThreadCallback);
		uv_ref((uv_handle_t*)&handle);
		handle.data = this;
	}

	void start() {
		if (fdb_future_set_callback(future, &NodeCallback::futureReadyCallback, this)) {
			fprintf(stderr, "fdb_future_set_callback failed.\n");
			abort();
		}
	}

	virtual ~NodeCallback() {
		cbFunc.Dispose();
		fdb_future_destroy(future);
	}

	void addRef() {
		++refCount;
	}

	void delRef() {
		if(--refCount == 0) {
			delete this;
		}
	}

	FDBFuture* getFuture() {
		return future;
	}

private:
	void close() {
		uv_close((uv_handle_t*)&handle, &NodeCallback::closeCallback);
	}

	static void closeCallback(uv_handle_s *handle) {
		NodeCallback *nc = (NodeCallback*)((uv_async_t*)handle)->data;
		nc->delRef();
	}

	static void futureReadyCallback(FDBFuture *f, void *ptr) {
		NodeCallback *nc = (NodeCallback*)ptr;
		uv_async_send(&nc->handle);
	}

	static void nodeThreadCallback(uv_async_t *handle, int status) {
		HandleScope scope;

		NodeCallback *nc = (NodeCallback*)handle->data;
		FDBFuture *future = nc->future;

		uv_unref((uv_handle_t*)handle);

		Handle<Value> jsError;
		Handle<Value> jsValue;

		fdb_error_t errorCode;
		jsValue = nc->extractValue(future, errorCode);
		if (errorCode == 0)
			jsError = Null();
		else
			jsError = FdbError::NewInstance(errorCode, fdb_get_error(errorCode));

		Handle<Value> args[2] = { jsError, jsValue };

		v8::TryCatch ex;
		nc->cbFunc->Call(Context::GetCurrent()->Global(), 2, args);

		if(ex.HasCaught())
			fprintf(stderr, "\n%s\n", *String::AsciiValue(ex.StackTrace()->ToString()));

		nc->close();
	}

	FDBFuture* future;
	uv_async_t handle;
	Persistent<Function> cbFunc;
	int refCount;

protected:
	virtual Handle<Value> extractValue(FDBFuture* future, fdb_error_t& outErr) = 0;

	static Handle<Value> makeBuffer(const char *arr, int length) {
		HandleScope scope;

		Buffer *buf = Buffer::New(length);
		Local<Object> slowBufferHandle = Local<Object>::New( buf->handle_ );  // Else the buffer, which has only a weak handle to itself, could be freed by GC in one of the below calls...
		memcpy(Buffer::Data(buf), (const char*)arr, length);

		Local<Object> globalObj = Context::GetCurrent()->Global();
		Local<Function> bufferConstructor = Local<Function>::Cast(globalObj->Get(String::NewSymbol("Buffer")));

		Handle<Value> constructorArgs[3] = { slowBufferHandle, Integer::New(length), Integer::New(0) };
		Handle<Object> actualBuffer = bufferConstructor->NewInstance(3, constructorArgs);

		return scope.Close(actualBuffer);
	}
};

#endif
