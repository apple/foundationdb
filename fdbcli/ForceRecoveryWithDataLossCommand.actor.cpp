/*
 * ForceRecoveryWithDataLossCommand.actor.cpp
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

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/IClientApi.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> forceRecoveryWithDataLossCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	if (tokens.size() != 2) {
		printUsage(tokens[0]);
		return false;
	}
	wait(safeThreadFutureToFuture(db->forceRecoveryWithDataLoss(tokens[1])));
	return true;
}

CommandFactory forceRecoveryWithDataLossFactory(
    "force_recovery_with_data_loss",
    CommandHelp("force_recovery_with_data_loss <DCID>",
                "Force the database to recover into DCID",
                "A forced recovery will cause the database to lose the most recently committed mutations. The "
                "amount of mutations that will be lost depends on how far behind the remote datacenter is. This "
                "command will change the region configuration to have a positive priority for the chosen DCID, and "
                "a negative priority for all other DCIDs. This command will set usable_regions to 1. If the "
                "database has already recovered, this command does nothing.\n"));
} // namespace fdb_cli
