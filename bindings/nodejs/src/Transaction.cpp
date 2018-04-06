/*
 * Transaction.cpp
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
#include <iostream>
#include <string>
#include <cstring>
#include <vector>
#include <node_buffer.h>
#include <node_version.h>

#include "Transaction.h"
#include "NodeCallback.h"
#include "FdbError.h"
#include "FdbOptions.h"

using namespace v8;
using namespace std;
using namespace node;

// Transaction Implementation
Transaction::Transaction() { };

Transaction::~Transaction() {
	fdb_transaction_destroy(tr);
};

Persistent<Function> Transaction::constructor;

struct NodeValueCallback : NodeCallback {

	NodeValueCallback(FDBFuture *future, Persistent<Function> cbFunc) : NodeCallback(future, cbFunc) { }

	virtual Handle<Value> extractValue(FDBFuture* future, fdb_error_t& outErr) {
		HandleScope scope;

		const char *value;
		int valueLength;
		int valuePresent;

		outErr = fdb_future_get_value(future, &valuePresent, (const uint8_t**)&value, &valueLength);
		if (outErr) return scope.Close(Undefined());

		Handle<Value> jsValue;

		if(!valuePresent)
			jsValue = Null();
		else
			jsValue = makeBuffer(value, valueLength);

		return scope.Close(jsValue);
	}
};

struct NodeKeyCallback : NodeCallback {

	NodeKeyCallback(FDBFuture *future, Persistent<Function> cbFunc) : NodeCallback(future, cbFunc) { }

	virtual Handle<Value> extractValue(FDBFuture* future, fdb_error_t& outErr) {
		HandleScope scope;

		const char *key;
		int keyLength;

		outErr = fdb_future_get_key(future, (const uint8_t**)&key, &keyLength);
		if (outErr) return scope.Close(Undefined());

		Handle<Value> jsValue = makeBuffer(key, keyLength);

		return scope.Close(jsValue);
	}
};

struct NodeVoidCallback : NodeCallback {

	NodeVoidCallback(FDBFuture *future, Persistent<Function> cbFunc) : NodeCallback(future, cbFunc) { }

	virtual Handle<Value> extractValue(FDBFuture* future, fdb_error_t& outErr) {
		outErr = fdb_future_get_error(future);
		return Undefined();
	}
};

struct NodeKeyValueCallback : NodeCallback {

	NodeKeyValueCallback(FDBFuture *future, Persistent<Function> cbFunc) : NodeCallback(future, cbFunc) { }

	virtual Handle<Value> extractValue(FDBFuture* future, fdb_error_t& outErr) {
		HandleScope scope;

		const FDBKeyValue *kv;
		int len;
		fdb_bool_t more;

		outErr = fdb_future_get_keyvalue_array(future, &kv, &len, &more);
		if (outErr) return scope.Close(Undefined());

		/*
		 * Constructing a JavaScript array of KeyValue objects:
		 *  {
		 *  	key: "some key",
		 *  	value: "some value"
		 *  }
		 *
		 */

		Handle<Object> returnObj = Object::New();
		Handle<Array> jsValueArray = Array::New(len);

		Handle<String> keySymbol = String::NewSymbol("key");
		Handle<String> valueSymbol = String::NewSymbol("value");

		for(int i = 0; i < len; i++) {
			Local<Object> jsKeyValue = Object::New();

			Handle<Value> jsKeyBuffer = makeBuffer((const char*)kv[i].key, kv[i].key_length);
			Handle<Value> jsValueBuffer = makeBuffer((const char*)kv[i].value, kv[i].value_length);

			jsKeyValue->Set(keySymbol, jsKeyBuffer);
			jsKeyValue->Set(valueSymbol, jsValueBuffer);
			jsValueArray->Set(Number::New(i), jsKeyValue);
		}

		returnObj->Set(String::NewSymbol("array"), jsValueArray);
		if(more)
			returnObj->Set(String::NewSymbol("more"), Number::New(1));

		return scope.Close(returnObj);
	}
};

struct NodeVersionCallback : NodeCallback {

	NodeVersionCallback(FDBFuture *future, Persistent<Function> cbFunc) : NodeCallback(future, cbFunc) { }

	virtual Handle<Value> extractValue(FDBFuture* future, fdb_error_t& outErr) {
		HandleScope scope;

		int64_t version;

		outErr = fdb_future_get_version(future, &version);
		if (outErr) return scope.Close(Undefined());

		//SOMEDAY: This limits the version to 53-bits.  Do something different here?
		Handle<Value> jsValue = Number::New((double)version);

		return scope.Close(jsValue);
	}
};

struct NodeStringArrayCallback : NodeCallback {

	NodeStringArrayCallback(FDBFuture *future, Persistent<Function> cbFunc) : NodeCallback(future, cbFunc) { }

	virtual Handle<Value> extractValue(FDBFuture *future, fdb_error_t& outErr) {
		HandleScope scope;

		const char **strings;
		int stringCount;

		outErr = fdb_future_get_string_array(future, &strings, &stringCount);
		if (outErr) return scope.Close(Undefined());

		Handle<Array> jsArray = Array::New(stringCount);
		for(int i = 0; i < stringCount; i++)
			jsArray->Set(Number::New(i), makeBuffer(strings[i], (int)strlen(strings[i])));

		return scope.Close(jsArray);
	}
};

struct StringParams {
	uint8_t *str;
	int len;

	/*
	 *  String arguments always have to be buffers to
	 *  preserve bytes. Otherwise, stuff gets converted
	 *  to UTF-8.
	 */
	StringParams(Handle<Value> keyVal) {
		str = (uint8_t*)(Buffer::Data(keyVal->ToObject()));
		len = (int)Buffer::Length(keyVal->ToObject());
	}
};

FDBTransaction* Transaction::GetTransactionFromArgs(const Arguments &args) {
	return node::ObjectWrap::Unwrap<Transaction>(args.Holder())->tr;
}

Persistent<Function> Transaction::GetCallback(Handle<Value> funcVal) {
	return Persistent<Function>::New(Handle<Function>(Function::Cast(*funcVal)));
}

Handle<Value> Transaction::Set(const Arguments &args){
	StringParams key(args[0]);
	StringParams val(args[1]);
	fdb_transaction_set(GetTransactionFromArgs(args), key.str, key.len, val.str, val.len);

	return Null();
}

Handle<Value> Transaction::Commit(const Arguments &args) {
	FDBFuture *f = fdb_transaction_commit(GetTransactionFromArgs(args));
	(new NodeVoidCallback(f, GetCallback(args[0])))->start();
	return Null();
}

Handle<Value> Transaction::Clear(const Arguments &args) {
	StringParams key(args[0]);
	fdb_transaction_clear(GetTransactionFromArgs(args), key.str, key.len);

	return Null();
}

/*
 * ClearRange takes two key strings.
 */
Handle<Value> Transaction::ClearRange(const Arguments &args) {
	StringParams begin(args[0]);
	StringParams end(args[1]);
	fdb_transaction_clear_range(GetTransactionFromArgs(args), begin.str, begin.len, end.str, end.len);

	return Null();
}

/*
 * This function takes a KeySelector and returns a future.
 */
Handle<Value> Transaction::GetKey(const Arguments &args) {
	StringParams key(args[0]);
	int selectorOrEqual = args[1]->Int32Value();
	int selectorOffset = args[2]->Int32Value();
	bool snapshot = args[3]->BooleanValue();

	FDBFuture *f = fdb_transaction_get_key(GetTransactionFromArgs(args), key.str, key.len, (fdb_bool_t)selectorOrEqual, selectorOffset, snapshot);
	(new NodeKeyCallback(f, GetCallback(args[4])))->start();
	return Null();
}

Handle<Value> Transaction::Get(const Arguments &args) {
	StringParams key(args[0]);
	bool snapshot = args[1]->BooleanValue();

	FDBFuture *f = fdb_transaction_get(GetTransactionFromArgs(args), key.str, key.len, snapshot);
	(new NodeValueCallback(f, GetCallback(args[2])))->start();
	return Null();
}

Handle<Value> Transaction::GetRange(const Arguments &args) {
	StringParams start(args[0]);
	int startOrEqual = args[1]->Int32Value();
	int startOffset = args[2]->Int32Value();

	StringParams end(args[3]);
	int endOrEqual = args[4]->Int32Value();
	int endOffset = args[5]->Int32Value();

	int limit = args[6]->Int32Value();
	FDBStreamingMode mode = (FDBStreamingMode)args[7]->Int32Value();
	int iteration = args[8]->Int32Value();
	bool snapshot = args[9]->BooleanValue();
	bool reverse = args[10]->BooleanValue();

	FDBFuture *f = fdb_transaction_get_range(GetTransactionFromArgs(args), start.str, start.len, (fdb_bool_t)startOrEqual, startOffset,
												end.str, end.len, (fdb_bool_t)endOrEqual, endOffset, limit, 0, mode, iteration, snapshot, reverse);

	(new NodeKeyValueCallback(f, GetCallback(args[11])))->start();
	return Null();
}

Handle<Value> Transaction::Watch(const Arguments &args) {
	HandleScope scope;

	Transaction *trPtr = node::ObjectWrap::Unwrap<Transaction>(args.Holder());

	uint8_t *keyStr = (uint8_t*)(Buffer::Data(args[0]->ToObject()));
	int keyLen = (int)Buffer::Length(args[0]->ToObject());

	Persistent<Function> cb = Persistent<Function>::New(Handle<Function>(Function::Cast(*args[1])));

	FDBFuture *f = fdb_transaction_watch(trPtr->tr, keyStr, keyLen);
	NodeVoidCallback *callback = new NodeVoidCallback(f, cb);
	Handle<Value> watch = Watch::NewInstance(callback);

	callback->start();
	return scope.Close(watch);
}

Handle<Value> Transaction::AddConflictRange(const Arguments &args, FDBConflictRangeType type) {
	StringParams start(args[0]);
	StringParams end(args[1]);

	fdb_error_t errorCode = fdb_transaction_add_conflict_range(GetTransactionFromArgs(args), start.str, start.len, end.str, end.len, type);

	if(errorCode != 0) {
		ThrowException(FdbError::NewInstance(errorCode, fdb_get_error(errorCode)));
		return Undefined();
	}

	return Null();
}

Handle<Value> Transaction::AddReadConflictRange(const Arguments &args) {
	return AddConflictRange(args, FDB_CONFLICT_RANGE_TYPE_READ);
}

Handle<Value> Transaction::AddWriteConflictRange(const Arguments &args) {
	return AddConflictRange(args, FDB_CONFLICT_RANGE_TYPE_WRITE);
}

Handle<Value> Transaction::OnError(const Arguments &args) {
	fdb_error_t errorCode = args[0]->Int32Value();
	FDBFuture *f = fdb_transaction_on_error(GetTransactionFromArgs(args), errorCode);
	(new NodeVoidCallback(f, GetCallback(args[1])))->start();
	return Null();
}

Handle<Value> Transaction::Reset(const Arguments &args) {
	fdb_transaction_reset(GetTransactionFromArgs(args));
	return Null();
}

Handle<Value> Transaction::SetReadVersion(const Arguments &args) {
	int64_t version = args[0]->IntegerValue();
	fdb_transaction_set_read_version(GetTransactionFromArgs(args), version);
	return Null();
}

Handle<Value> Transaction::GetReadVersion(const Arguments &args) {
	FDBFuture *f = fdb_transaction_get_read_version(GetTransactionFromArgs(args));
	(new NodeVersionCallback(f, GetCallback(args[0])))->start();
	return Null();
}

Handle<Value> Transaction::GetCommittedVersion(const Arguments &args) {
	HandleScope scope;

	int64_t version;
	fdb_error_t errorCode = fdb_transaction_get_committed_version(GetTransactionFromArgs(args), &version);

	if(errorCode != 0) {
		ThrowException(FdbError::NewInstance(errorCode, fdb_get_error(errorCode)));
		return scope.Close(Undefined());
	}

	return scope.Close(Number::New((double)version));
}

Handle<Value> Transaction::GetVersionstamp(const Arguments &args) {
	FDBFuture *f = fdb_transaction_get_versionstamp(GetTransactionFromArgs(args));
	(new NodeKeyCallback(f, GetCallback(args[0])))->start();
	return Null();
}

Handle<Value> Transaction::Cancel(const Arguments &args) {
	fdb_transaction_cancel(GetTransactionFromArgs(args));
	return Null();
}

Handle<Value> Transaction::GetAddressesForKey(const Arguments &args) {
	StringParams key(args[0]);

	FDBFuture *f = fdb_transaction_get_addresses_for_key(GetTransactionFromArgs(args), key.str, key.len);
	(new NodeStringArrayCallback(f, GetCallback(args[1])))->start();
	return Null();
}

Handle<Value> Transaction::New(const Arguments &args) {
	Transaction *tr = new Transaction();
	tr->Wrap(args.Holder());

	return args.Holder();
}

Handle<Value> Transaction::NewInstance(FDBTransaction *ptr) {
	HandleScope scope;

	Local<Object> instance = constructor->NewInstance();

	Transaction *trObj = ObjectWrap::Unwrap<Transaction>(instance);
	trObj->tr = ptr;

	instance->Set(String::NewSymbol("options"), FdbOptions::CreateOptions(FdbOptions::TransactionOption, instance));

	return scope.Close(instance);
}

void Transaction::Init() {
	Local<FunctionTemplate> tpl = FunctionTemplate::New(New);

	tpl->SetClassName(String::NewSymbol("Transaction"));
	tpl->InstanceTemplate()->SetInternalFieldCount(1);

	tpl->PrototypeTemplate()->Set(String::NewSymbol("get"), FunctionTemplate::New(Get)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("getRange"), FunctionTemplate::New(GetRange)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("getKey"), FunctionTemplate::New(GetKey)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("watch"), FunctionTemplate::New(Watch)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("set"), FunctionTemplate::New(Set)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("commit"), FunctionTemplate::New(Commit)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("clear"), FunctionTemplate::New(Clear)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("clearRange"), FunctionTemplate::New(ClearRange)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("addReadConflictRange"), FunctionTemplate::New(AddReadConflictRange)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("addWriteConflictRange"), FunctionTemplate::New(AddWriteConflictRange)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("onError"), FunctionTemplate::New(OnError)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("reset"), FunctionTemplate::New(Reset)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("getReadVersion"), FunctionTemplate::New(GetReadVersion)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("setReadVersion"), FunctionTemplate::New(SetReadVersion)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("getCommittedVersion"), FunctionTemplate::New(GetCommittedVersion)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("getVersionstamp"), FunctionTemplate::New(GetVersionstamp)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("cancel"), FunctionTemplate::New(Cancel)->GetFunction());
	tpl->PrototypeTemplate()->Set(String::NewSymbol("getAddressesForKey"), FunctionTemplate::New(GetAddressesForKey)->GetFunction());

	constructor = Persistent<Function>::New(tpl->GetFunction());
}

// Watch implementation
Watch::Watch() : callback(NULL) { };

Watch::~Watch() {
	if(callback) {
		if(callback->getFuture())
			fdb_future_cancel(callback->getFuture());

		callback->delRef();
	}
};

Persistent<Function> Watch::constructor;

Handle<Value> Watch::NewInstance(NodeCallback *callback) {
	HandleScope scope;

	Local<Object> instance = constructor->NewInstance();

	Watch *watchObj = ObjectWrap::Unwrap<Watch>(instance);
	watchObj->callback = callback;
	callback->addRef();

	return scope.Close(instance);
}

Handle<Value> Watch::New(const Arguments &args) {
	Watch *c = new Watch();
	c->Wrap(args.Holder());

	return args.Holder();
}

Handle<Value> Watch::Cancel(const Arguments &args) {
	NodeCallback *callback = node::ObjectWrap::Unwrap<Watch>(args.Holder())->callback;

	if(callback && callback->getFuture())
		fdb_future_cancel(callback->getFuture());

	return Null();
}

void Watch::Init() {
	Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
	tpl->SetClassName(String::NewSymbol("Watch"));
	tpl->InstanceTemplate()->SetInternalFieldCount(1);

	tpl->PrototypeTemplate()->Set(String::NewSymbol("cancel"), FunctionTemplate::New(Cancel)->GetFunction());

	constructor = Persistent<Function>::New(tpl->GetFunction());
}
