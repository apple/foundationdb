/*
 * Database.cpp
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

#include "Database.h"
#include "FdbOptions.h"
#include "NodeCallback.h"

using namespace v8;
using namespace std;

Database::Database() { };

Database::~Database() {
	fdb_database_destroy(db);
};

Persistent<Function> Database::constructor;

void Database::Init() {
	Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
	tpl->SetClassName(String::NewSymbol("Database"));
	tpl->InstanceTemplate()->SetInternalFieldCount(1);

	tpl->PrototypeTemplate()->Set(String::NewSymbol("createTransaction"), FunctionTemplate::New(CreateTransaction)->GetFunction());

	constructor = Persistent<Function>::New(tpl->GetFunction());
}

Handle<v8::Value> Database::CreateTransaction(const v8::Arguments &args) {
	HandleScope scope;

	Database *dbPtr = node::ObjectWrap::Unwrap<Database>(args.Holder());
	FDBDatabase *db = dbPtr->db;
	FDBTransaction *tr;
	fdb_error_t err = fdb_database_create_transaction(db, &tr);
	if (err) {
		ThrowException(FdbError::NewInstance(err, fdb_get_error(err)));
		return scope.Close(Undefined());
	}

	return scope.Close(Transaction::NewInstance(tr));
}

Handle<Value> Database::New(const Arguments &args) {
	HandleScope scope;

	Database *db = new Database();
	db->Wrap(args.Holder());

	return scope.Close(args.Holder());
}

Handle<Value> Database::NewInstance(FDBDatabase *ptr) {
	HandleScope scope;

	Local<Object> instance = constructor->NewInstance(0, NULL);
	Database *dbObj = ObjectWrap::Unwrap<Database>(instance);
	dbObj->db = ptr;

	instance->Set(String::NewSymbol("options"), FdbOptions::CreateOptions(FdbOptions::DatabaseOption, instance));

	return scope.Close(instance);
}
