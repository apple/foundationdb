/*
 * ForceRecoveryWithDataLossCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/IClientApi.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> repairSystemDataCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	wait(safeThreadFutureToFuture(db->repairSystemData()));
	return true;
}

CommandFactory repairSystemDataFactory(
    "repair_system_data",
    CommandHelp("repair_system_data",
                "Instruct the database to repair system data",
                "This command will cause the database to start a recovery and reconstruct system keyspace.\n"));
} // namespace fdb_cli
