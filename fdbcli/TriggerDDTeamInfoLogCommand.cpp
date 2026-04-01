/*
 * TriggerDDTeamInfoLogCommand.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbcli/fdbcli.h"

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/SystemData.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
namespace fdb_cli {

Future<bool> triggerddteaminfologCommandActor(Reference<IDatabase> db) {
	Reference<ITransaction> tr = db->createTransaction();
	while (true) {
		Error err;
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			std::string v = deterministicRandom()->randomUniqueID().toString();
			tr->set(triggerDDTeamInfoPrintKey, v);
			co_await safeThreadFutureToFuture(tr->commit());
			printf("Triggered team info logging in data distribution.\n");
			co_return true;
		} catch (Error& e) {
			err = e;
		}
		co_await safeThreadFutureToFuture(tr->onError(err));
	}
}

CommandFactory triggerddteaminfologFactory(
    "triggerddteaminfolog",
    CommandHelp("triggerddteaminfolog",
                "trigger the data distributor teams logging",
                "Trigger the data distributor to log detailed information about its teams."));

} // namespace fdb_cli
