/*
 * ConfigureCommand.actor.cpp
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

ACTOR Future<bool> configureStorageEngineCommandActor(Reference<IDatabase> db,
                                                      Database localDb,
                                                      std::vector<StringRef> tokens,
                                                      LineNoise* linenoise,
                                                      Future<Void> warn) {
	bool result = true;
    if (tokens.size() == 1) {
        printf("\nTesting configure-storage-engine command\n");
    } else {
        printUsage(tokens[0]);
		return false;
    }
	return result;
}

void configureStorageEngineGenerator(const char* text,
                                     const char* line,
                                     std::vector<std::string>& lc,
                                     std::vector<StringRef> const& tokens) {
	const char* opts[] = { "ssd-2", nullptr };
	arrayGenerator(text, line, opts, lc);
}

CommandFactory configureStorageEngineFactory("configure-storage-engine",
                                             CommandHelp("configure-storage-engine <engine_name>[:param1=val1]",
                                                         "short description",
                                                         "long description"),
                                             &configureStorageEngineGenerator);

} // namespace fdb_cli