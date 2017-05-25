/*
 * Transaction.h
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


#ifndef FDB_NODE_TRANSACTION_H
#define FDB_NODE_TRANSACTION_H

#include "Version.h"

#include <foundationdb/fdb_c.h>
#include <node.h>

#include "NodeCallback.h"

class Transaction: public node::ObjectWrap {
	public:
		static void Init();
		static v8::Handle<v8::Value> NewInstance(FDBTransaction *ptr);
		static v8::Handle<v8::Value> New(const v8::Arguments &args);

		static v8::Handle<v8::Value> Get(const v8::Arguments &args);
		static v8::Handle<v8::Value> GetKey(const v8::Arguments &args);
		static v8::Handle<v8::Value> Set(const v8::Arguments &args);
		static v8::Handle<v8::Value> Commit(const v8::Arguments &args);
		static v8::Handle<v8::Value> Clear(const v8::Arguments &args);
		static v8::Handle<v8::Value> ClearRange(const v8::Arguments &args);
		static v8::Handle<v8::Value> GetRange(const v8::Arguments &args);
		static v8::Handle<v8::Value> Watch(const v8::Arguments &args);

		static v8::Handle<v8::Value> AddConflictRange(const v8::Arguments &args, FDBConflictRangeType type);
		static v8::Handle<v8::Value> AddReadConflictRange(const v8::Arguments &args);
		static v8::Handle<v8::Value> AddWriteConflictRange(const v8::Arguments &args);

		static v8::Handle<v8::Value> OnError(const v8::Arguments &args);
		static v8::Handle<v8::Value> Reset(const v8::Arguments &args);

		static v8::Handle<v8::Value> SetReadVersion(const v8::Arguments &args);
		static v8::Handle<v8::Value> GetReadVersion(const v8::Arguments &args);
		static v8::Handle<v8::Value> GetCommittedVersion(const v8::Arguments &args);
		static v8::Handle<v8::Value> GetVersionstamp(const v8::Arguments &args);

		static v8::Handle<v8::Value> Cancel(const v8::Arguments &args);

		static v8::Handle<v8::Value> GetAddressesForKey(const v8::Arguments &args);

		FDBTransaction* GetTransaction() { return tr; }
	private:
		Transaction();
		~Transaction();

		static v8::Persistent<v8::Function> constructor;
		FDBTransaction *tr;

		static FDBTransaction* GetTransactionFromArgs(const v8::Arguments &args);
		static v8::Persistent<v8::Function> GetCallback(const v8::Handle<v8::Value> funcVal);
};

class Watch : public node::ObjectWrap {
	public:
		static void Init();

		static v8::Handle<v8::Value> NewInstance(NodeCallback *callback);
		static v8::Handle<v8::Value> New(const v8::Arguments &args);

		static v8::Handle<v8::Value> Cancel(const v8::Arguments &args);

	private:
		Watch();
		~Watch();

		static v8::Persistent<v8::Function> constructor;
		NodeCallback *callback;
};

#endif
