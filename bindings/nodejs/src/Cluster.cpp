/*
 * Cluster.cpp
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
#include <node_version.h>

#include "Cluster.h"
#include "Database.h"
#include "FdbOptions.h"
#include "NodeCallback.h"

using namespace v8;
using namespace std;

Cluster::Cluster() { }
Cluster::~Cluster() {
	fdb_cluster_destroy(cluster);
}

Persistent<Function> Cluster::constructor;

Handle<Value> Cluster::OpenDatabase(const Arguments &args) {
	HandleScope scope;

	Cluster *clusterPtr = ObjectWrap::Unwrap<Cluster>(args.Holder());

	const char *dbName = "DB";
	FDBFuture *f = fdb_cluster_create_database(clusterPtr->cluster, (uint8_t*)dbName, (int)strlen(dbName));

	fdb_error_t errorCode = fdb_future_block_until_ready(f);

	FDBDatabase *database;
	if(errorCode == 0)
		errorCode = fdb_future_get_database(f, &database);

	if(errorCode != 0)
		return ThrowException(FdbError::NewInstance(errorCode, fdb_get_error(errorCode)));

	Handle<Value> jsValue = Database::NewInstance(database);
	return scope.Close(jsValue);
}

void Cluster::Init() {
	HandleScope scope;

	Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
	tpl->InstanceTemplate()->SetInternalFieldCount(1);
	tpl->SetClassName(String::NewSymbol("Cluster"));

	tpl->PrototypeTemplate()->Set(String::NewSymbol("openDatabase"), FunctionTemplate::New(OpenDatabase)->GetFunction());

	constructor = Persistent<Function>::New(tpl->GetFunction());
}

Handle<Value> Cluster::New(const Arguments &args) {
	HandleScope scope;

	Cluster *c = new Cluster();
	c->Wrap(args.Holder());

	return scope.Close(args.Holder());
}

Handle<Value> Cluster::NewInstance(FDBCluster *ptr) {
	HandleScope scope;

	Local<Object> instance = constructor->NewInstance(0, NULL);

	Cluster *clusterObj = ObjectWrap::Unwrap<Cluster>(instance);
	clusterObj->cluster = ptr;

	instance->Set(String::NewSymbol("options"), FdbOptions::CreateOptions(FdbOptions::ClusterOption, instance));

	return scope.Close(instance);
}
