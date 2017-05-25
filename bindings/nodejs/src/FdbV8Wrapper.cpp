/*
 * FdbV8Wrapper.cpp
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


#include <string>
#include "node.h"
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <node_version.h>

#include "Database.h"
#include "NodeCallback.h"
#include "Cluster.h"
#include "Version.h"
#include "FdbError.h"
#include "FdbOptions.h"

uv_thread_t fdbThread;

using namespace v8;
using namespace std;

bool networkStarted = false;

Handle<Value> ApiVersion(const Arguments &args) {
	int apiVersion = args[0]->Int32Value();
	fdb_error_t errorCode = fdb_select_api_version(apiVersion);

	if(errorCode != 0) {
		if(errorCode == 2203) {
			int maxSupportedVersion = fdb_get_max_api_version();

			ostringstream errorStr;
			if(FDB_API_VERSION > maxSupportedVersion) {
				errorStr << "This version of the FoundationDB Node.js binding is not supported by the installed FoundationDB "
						 << "C library. The binding requires a library that supports API version " << FDB_API_VERSION
						 << ", but the installed library supports a maximum version of " << maxSupportedVersion << ".";
			}
			else {
				errorStr << "API version " << apiVersion << " is not supported by the installed FoundationDB C library.";
			}

			return ThrowException(FdbError::NewInstance(errorCode, errorStr.str().c_str()));
		}

		return ThrowException(FdbError::NewInstance(errorCode, fdb_get_error(errorCode)));
	}

	return Null();
}

static void networkThread(void *arg) {
	fdb_error_t errorCode = fdb_run_network();
	if(errorCode != 0)
		fprintf(stderr, "Unhandled error in FoundationDB network thread: %s (%d)\n", fdb_get_error(errorCode), errorCode);
}

static Handle<Value> runNetwork() {
	fdb_error_t errorCode = fdb_setup_network();

	if(errorCode != 0)
		return ThrowException(FdbError::NewInstance(errorCode, fdb_get_error(errorCode)));

	uv_thread_create(&fdbThread, networkThread, NULL);  // FIXME: Return code?

	return Null();
}

Handle<Value> CreateCluster(const Arguments &args) {
	HandleScope scope;

	FDBFuture *f = fdb_create_cluster(*String::AsciiValue(args[0]->ToString()));
	fdb_error_t errorCode = fdb_future_block_until_ready(f);

	FDBCluster *cluster;
	if(errorCode == 0)
		errorCode = fdb_future_get_cluster(f, &cluster);

	if(errorCode != 0)
		return ThrowException(FdbError::NewInstance(errorCode, fdb_get_error(errorCode)));

	Handle<Value> jsValue = Local<Value>::New(Cluster::NewInstance(cluster));
	return scope.Close(jsValue);
}

Handle<Value> StartNetwork(const Arguments &args) {
	if(!networkStarted) {
		networkStarted = true;
		return runNetwork();
	}

	return Null();
}

Handle<Value> StopNetwork(const Arguments &args) {
	fdb_error_t errorCode = fdb_stop_network();

	if(errorCode != 0)
		return ThrowException(FdbError::NewInstance(errorCode, fdb_get_error(errorCode)));

	uv_thread_join(&fdbThread);

	//This line forces garbage collection.  Useful for doing valgrind tests
	//while(!V8::IdleNotification());

	return Null();
}

void init(Handle<Object> target){
	FdbError::Init( target );
	Database::Init();
	Transaction::Init();
	Cluster::Init();
	FdbOptions::Init();
	Watch::Init();

	target->Set(String::NewSymbol("apiVersion"), FunctionTemplate::New(ApiVersion)->GetFunction());
	target->Set(String::NewSymbol("createCluster"), FunctionTemplate::New(CreateCluster)->GetFunction());
	target->Set(String::NewSymbol("startNetwork"), FunctionTemplate::New(StartNetwork)->GetFunction());
	target->Set(String::NewSymbol("stopNetwork"), FunctionTemplate::New(StopNetwork)->GetFunction());
	target->Set(String::NewSymbol("options"), FdbOptions::CreateOptions(FdbOptions::NetworkOption));
	target->Set(String::NewSymbol("streamingMode"), FdbOptions::CreateEnum(FdbOptions::StreamingMode));
	target->Set(String::NewSymbol("atomic"), FdbOptions::CreateOptions(FdbOptions::MutationType));
}

#if NODE_VERSION_AT_LEAST(0, 8, 0)
NODE_MODULE(fdblib, init);
#else
#error "Node.js versions before v0.8.0 are not supported"
#endif
