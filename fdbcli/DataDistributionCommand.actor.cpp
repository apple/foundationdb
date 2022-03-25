/*
 * DataDistributionCommand.actor.cpp
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

#include "boost/lexical_cast.hpp"

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/Knobs.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

ACTOR Future<Void> setDDMode(Reference<IDatabase> db, int mode) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			tr->set(fdb_cli::ddModeSpecialKey, boost::lexical_cast<std::string>(mode));
			if (mode) {
				// set DDMode to 1 will enable all disabled parts, for instance the SS failure monitors.
				// hold the returned standalone object's memory
				state ThreadFuture<RangeResult> resultFuture =
				    tr->getRange(fdb_cli::maintenanceSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
				RangeResult res = wait(safeThreadFutureToFuture(resultFuture));
				ASSERT(res.size() <= 1);
				if (res.size() == 1 && res[0].key == fdb_cli::ignoreSSFailureSpecialKey) {
					// only clear the key if it is currently being used to disable all SS failure data movement
					tr->clear(fdb_cli::maintenanceSpecialKeyRange);
				}
				tr->clear(fdb_cli::ddIgnoreRebalanceSpecialKey);
			}
			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			TraceEvent("SetDDModeRetrying").error(e);
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<Void> setDDIgnoreRebalanceSwitch(Reference<IDatabase> db, int DDIgnoreOption) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			if (DDIgnoreOption > 0) {
				Optional<Value> v = wait(safeThreadFutureToFuture(tr->get(fdb_cli::ddIgnoreRebalanceSpecialKey)));
				if (v.present() && v.get().size() > 0) {
					int oldValue = BinaryReader::fromStringRef<int>(v.get(), Unversioned());
					tr->set(fdb_cli::ddIgnoreRebalanceSpecialKey,
					        BinaryWriter::toValue(DDIgnoreOption & oldValue, Unversioned()));
				} else {
					tr->set(fdb_cli::ddIgnoreRebalanceSpecialKey, BinaryWriter::toValue(DDIgnoreOption, Unversioned()));
				}
			} else {
				tr->clear(fdb_cli::ddIgnoreRebalanceSpecialKey);
			}
			wait(safeThreadFutureToFuture(tr->commit()));
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

} // namespace

namespace fdb_cli {

const KeyRef ddModeSpecialKey = LiteralStringRef("\xff\xff/management/data_distribution/mode");
const KeyRef ddIgnoreRebalanceSpecialKey = LiteralStringRef("\xff\xff/management/data_distribution/rebalance_ignored");
constexpr auto usage =
    "Usage: datadistribution <on|off|disable <ssfailure|rebalance|rebalance_disk|rebalance_read>|enable "
    "<ssfailure|rebalance|rebalance_disk|rebalance_read>>\n";
ACTOR Future<bool> dataDistributionCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	state bool result = true;
	if (tokens.size() != 2 && tokens.size() != 3) {
		printf(usage);
		result = false;
	} else {
		if (tokencmp(tokens[1], "on")) {
			wait(success(setDDMode(db, 1)));
			printf("Data distribution is turned on.\n");
		} else if (tokencmp(tokens[1], "off")) {
			wait(success(setDDMode(db, 0)));
			printf("Data distribution is turned off.\n");
		} else if (tokencmp(tokens[1], "disable")) {
			if (tokencmp(tokens[2], "ssfailure")) {
				wait(success((setHealthyZone(db, LiteralStringRef("IgnoreSSFailures"), 0))));
				printf("Data distribution is disabled for storage server failures.\n");
			} else if (tokencmp(tokens[2], "rebalance")) {
				wait(setDDIgnoreRebalanceSwitch(db, DDIgnore::REBALANCE_DISK | DDIgnore::REBALANCE_READ));
				printf("Data distribution is disabled for rebalance.\n");
			} else if (tokencmp(tokens[2], "rebalance_disk")) {
				wait(setDDIgnoreRebalanceSwitch(db, DDIgnore::REBALANCE_DISK));
				printf("Data distribution is disabled for rebalance_disk.\n");
			} else if (tokencmp(tokens[2], "rebalance_read")) {
				wait(setDDIgnoreRebalanceSwitch(db, DDIgnore::REBALANCE_READ));
				printf("Data distribution is disabled for rebalance_read.\n");
			} else {
				printf(usage);
				result = false;
			}
		} else if (tokencmp(tokens[1], "enable")) {
			if (tokencmp(tokens[2], "ssfailure")) {
				wait(success((clearHealthyZone(db, false, true))));
				printf("Data distribution is enabled for storage server failures.\n");
			} else if (tokencmp(tokens[2], "rebalance")) {
				wait(setDDIgnoreRebalanceSwitch(db, 0));
				printf("Data distribution is enabled for rebalance.\n");
			} else if (tokencmp(tokens[2], "rebalance_disk")) {
				wait(setDDIgnoreRebalanceSwitch(db, ~DDIgnore::REBALANCE_DISK));
				printf("Data distribution is enabled for rebalance_disk.\n");
			} else if (tokencmp(tokens[2], "rebalance_read")) {
				wait(setDDIgnoreRebalanceSwitch(db, ~DDIgnore::REBALANCE_READ));
				printf("Data distribution is enabled for rebalance_read.\n");
			} else {
				printf(usage);
				result = false;
			}
		} else {
			printf(usage);
			result = false;
		}
	}
	return result;
}

// hidden commands, no help text for now
CommandFactory dataDistributionFactory("datadistribution");
} // namespace fdb_cli
