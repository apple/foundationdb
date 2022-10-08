/*
 * ConsistencyScanCommand.actor.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/IClientApi.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "fdbclient/ConsistencyScanInterface.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> consistencyScanCommandActor(Database db, std::vector<StringRef> tokens) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	// Here we do not proceed in a try-catch loop since the transaction is always supposed to succeed.
	// If not, the outer loop catch block(fdbcli.actor.cpp) will handle the error and print out the error message
	state int usageError = 0;
	state ConsistencyScanInfo csInfo = ConsistencyScanInfo();
	tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

	// Get the exisiting consistencyScanInfo object if present
	state Optional<Value> consistencyScanInfo = wait(ConsistencyScanInfo::getInfo(tr));
	wait(tr->commit());
	if (consistencyScanInfo.present())
		csInfo = ObjectReader::fromStringRef<ConsistencyScanInfo>(consistencyScanInfo.get(), IncludeVersion());
	tr->reset();

	if (tokens.size() == 1) {
		printf("Consistency Scan Info: %s\n", csInfo.toString().c_str());
	} else if ((tokens.size() == 2) && tokencmp(tokens[1], "off")) {
		csInfo.consistency_scan_enabled = false;
		wait(ConsistencyScanInfo::setInfo(tr, csInfo));
		wait(tr->commit());
	} else if ((tokencmp(tokens[1], "on") && tokens.size() > 2)) {
		csInfo.consistency_scan_enabled = true;
		state std::vector<StringRef>::iterator t;
		for (t = tokens.begin() + 2; t != tokens.end(); ++t) {
			if (tokencmp(t->toString(), "restart")) {
				if (++t != tokens.end()) {
					if (tokencmp(t->toString(), "0")) {
						csInfo.restart = false;
					} else if (tokencmp(t->toString(), "1")) {
						csInfo.restart = true;
					} else {
						usageError = 1;
					}
				} else {
					usageError = 1;
				}
			} else if (tokencmp(t->toString(), "maxRate")) {
				if (++t != tokens.end()) {
					char* end;
					csInfo.max_rate = std::strtod(t->toString().data(), &end);
					if (!std::isspace(*end) && (*end != '\0')) {
						fprintf(stderr, "ERROR: %s failed to parse.\n", t->toString().c_str());
						return false;
					}
				} else {
					usageError = 1;
				}
			} else if (tokencmp(t->toString(), "targetInterval")) {
				if (++t != tokens.end()) {
					char* end;
					csInfo.target_interval = std::strtod(t->toString().data(), &end);
					if (!std::isspace(*end) && (*end != '\0')) {
						fprintf(stderr, "ERROR: %s failed to parse.\n", t->toString().c_str());
						return false;
					}
				} else {
					usageError = 1;
				}
			} else {
				usageError = 1;
			}
		}

		if (!usageError) {
			wait(ConsistencyScanInfo::setInfo(tr, csInfo));
			wait(tr->commit());
		}
	} else {
		usageError = 1;
	}

	if (usageError) {
		printUsage(tokens[0]);
		return false;
	}
	return true;
}

CommandFactory consistencyScanFactory(
    "consistencyscan",
    CommandHelp("consistencyscan <on|off> <restart 0|1> <maxRate val> <targetInterval val>",
                "enables or disables consistency scan",
                "Calling this command with `on' enables the consistency scan process to run the scan with given "
                "arguments and `off' will halt the scan. "
                "Calling this command with no arguments will display if consistency scan is currently enabled.\n"));

} // namespace fdb_cli