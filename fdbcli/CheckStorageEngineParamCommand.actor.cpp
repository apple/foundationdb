/*
 * CheckStorageEngineParamCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbcli/FlowLineNoise.h"
#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/ManagementAPI.actor.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> checkStorageEngineParamCommandActor(Reference<IDatabase> db,
                                                       Reference<ITransaction> tr,
                                                       Database localDb,
                                                       std::vector<StringRef> tokens,
                                                       LineNoise* linenoise,
                                                       Future<Void> warn) {
	state bool result = true;
	if (tokens.size() == 1) {
		// this will print the current storage engine parameters
		state ThreadFuture<Optional<Value>> paramsF =
		    tr->get("\xff\xff/configuration/storage_engine_params/storage_engine"_sr);
		Optional<Value> params = wait(safeThreadFutureToFuture(paramsF));
		printf("Storage engine parameters: \n%s\n", params.present() ? params.get().toString().c_str() : "null");
	} else {
		printUsage(tokens[0]);
		return false;
	}
	return result;
}

CommandFactory configureStorageEngineFactory(
    "check-storage-engine",
    CommandHelp("check-storage-engine",
                "Check storage servers' paramters, only supported for certain storage engine type",
                "If the storage engine type is not supported, it will give not_implemented error"));

} // namespace fdb_cli
