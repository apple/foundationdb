/*
 * MaintenanceCommand.actor.cpp
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

#include <cinttypes>

#include "boost/lexical_cast.hpp"
#include "contrib/fmt-8.1.1/include/fmt/format.h"

#include "fdbcli/fdbcli.actor.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/IClientApi.h"

#include "fdbclient/Knobs.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

// print zoneId under maintenance, only one is possible at the same time
ACTOR Future<Void> printHealthyZone(Reference<IDatabase> db) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			// We need to keep the future as the returned standalone is not guaranteed to manage its memory when
			// using an external client, but the ThreadFuture holds a reference to the memory
			state ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::maintenanceSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult res = wait(safeThreadFutureToFuture(resultFuture));
			ASSERT(res.size() <= 1);
			if (res.size() == 1 && res[0].key == fdb_cli::ignoreSSFailureSpecialKey) {
				printf("Data distribution has been disabled for all storage server failures in this cluster and thus "
				       "maintenance mode is not active.\n");
			} else if (!res.size() || boost::lexical_cast<double>(res[0].value.toString()) <= 0) {
				printf("No ongoing maintenance.\n");
			} else {
				std::string zoneId = res[0].key.removePrefix(fdb_cli::maintenanceSpecialKeyRange.begin).toString();
				int64_t seconds = static_cast<int64_t>(boost::lexical_cast<double>(res[0].value.toString()));
				fmt::print("Maintenance for zone {0} will continue for {1} seconds.\n", zoneId, seconds);
			}
			return Void();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

} // namespace

namespace fdb_cli {

const KeyRangeRef maintenanceSpecialKeyRange = KeyRangeRef(LiteralStringRef("\xff\xff/management/maintenance/"),
                                                           LiteralStringRef("\xff\xff/management/maintenance0"));
// The special key, if present, means data distribution is disabled for storage failures;
const KeyRef ignoreSSFailureSpecialKey = LiteralStringRef("\xff\xff/management/maintenance/IgnoreSSFailures");

// add a zone to maintenance and specify the maintenance duration
ACTOR Future<bool> setHealthyZone(Reference<IDatabase> db, StringRef zoneId, double seconds, bool printWarning) {
	state Reference<ITransaction> tr = db->createTransaction();
	TraceEvent("SetHealthyZone").detail("Zone", zoneId).detail("DurationSeconds", seconds);
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			// hold the returned standalone object's memory
			state ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::maintenanceSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult res = wait(safeThreadFutureToFuture(resultFuture));
			ASSERT(res.size() <= 1);
			if (res.size() == 1 && res[0].key == fdb_cli::ignoreSSFailureSpecialKey) {
				if (printWarning) {
					fprintf(stderr,
					        "ERROR: Maintenance mode cannot be used while data distribution is disabled for storage "
					        "server failures. Use 'datadistribution on' to reenable data distribution.\n");
				}
				return false;
			}
			tr->set(fdb_cli::maintenanceSpecialKeyRange.begin.withSuffix(zoneId),
			        boost::lexical_cast<std::string>(seconds));
			wait(safeThreadFutureToFuture(tr->commit()));
			return true;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// clear ongoing maintenance, let clearSSFailureZoneString = true to enable data distribution for storage
ACTOR Future<bool> clearHealthyZone(Reference<IDatabase> db, bool printWarning, bool clearSSFailureZoneString) {
	state Reference<ITransaction> tr = db->createTransaction();
	TraceEvent("ClearHealthyZone").detail("ClearSSFailureZoneString", clearSSFailureZoneString);
	loop {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		try {
			// hold the returned standalone object's memory
			state ThreadFuture<RangeResult> resultFuture =
			    tr->getRange(fdb_cli::maintenanceSpecialKeyRange, CLIENT_KNOBS->TOO_MANY);
			RangeResult res = wait(safeThreadFutureToFuture(resultFuture));
			ASSERT(res.size() <= 1);
			if (!clearSSFailureZoneString && res.size() == 1 && res[0].key == fdb_cli::ignoreSSFailureSpecialKey) {
				if (printWarning) {
					fprintf(stderr,
					        "ERROR: Maintenance mode cannot be used while data distribution is disabled for storage "
					        "server failures. Use 'datadistribution on' to reenable data distribution.\n");
				}
				return false;
			}

			tr->clear(fdb_cli::maintenanceSpecialKeyRange);
			wait(safeThreadFutureToFuture(tr->commit()));
			return true;
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<bool> maintenanceCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens) {
	state bool result = true;
	if (tokens.size() == 1) {
		wait(printHealthyZone(db));
	} else if (tokens.size() == 2 && tokencmp(tokens[1], "off")) {
		bool clearResult = wait(clearHealthyZone(db, true));
		result = clearResult;
	} else if (tokens.size() == 4 && tokencmp(tokens[1], "on")) {
		double seconds;
		int n = 0;
		auto secondsStr = tokens[3].toString();
		if (sscanf(secondsStr.c_str(), "%lf%n", &seconds, &n) != 1 || n != secondsStr.size()) {
			printUsage(tokens[0]);
			result = false;
		} else {
			bool setResult = wait(setHealthyZone(db, tokens[2], seconds, true));
			result = setResult;
		}
	} else {
		printUsage(tokens[0]);
		result = false;
	}
	return result;
}

CommandFactory maintenanceFactory(
    "maintenance",
    CommandHelp(
        "maintenance [on|off] [ZONEID] [SECONDS]",
        "mark a zone for maintenance",
        "Calling this command with `on' prevents data distribution from moving data away from the processes with the "
        "specified ZONEID. Data distribution will automatically be turned back on for ZONEID after the specified "
        "SECONDS have elapsed, or after a storage server with a different ZONEID fails. Only one ZONEID can be marked "
        "for maintenance. Calling this command with no arguments will display any ongoing maintenance. Calling this "
        "command with `off' will disable maintenance.\n"));
} // namespace fdb_cli
