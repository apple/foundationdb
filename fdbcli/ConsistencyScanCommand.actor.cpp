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
#include "fdbserver/ConsistencyScanInterface.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

// const KeyRef consistencyCheckSpecialKey = LiteralStringRef("\xff\xff/management/consistency_check_suspended");

ACTOR Future<bool> consistencyScanCommandActor(Database db, std::vector<StringRef> tokens) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	// Here we do not proceed in a try-catch loop since the transaction is always supposed to succeed.
	// If not, the outer loop catch block(fdbcli.actor.cpp) will handle the error and print out the error message
	state int usageError = 0;
	tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
	if (tokens.size() == 1) {
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		state Optional<Value> consistencyScanInfo = wait(ConsistencyScanInfo::getInfo(tr));
		wait(tr->commit());
		if (consistencyScanInfo.present()) {
			printf("Consistency Scan Info: %s\n",
			       ObjectReader::fromStringRef<ConsistencyScanInfo>(consistencyScanInfo.get(), IncludeVersion())
			           .toString()
			           .c_str());
		}
	} else if ((tokens.size() == 2) && tokencmp(tokens[1], "off")) {
		ConsistencyScanInfo csInfo = ConsistencyScanInfo();
		wait(ConsistencyScanInfo::setInfo(tr, csInfo));
	} else if ((tokencmp(tokens[1], "on") && tokens.size() == 8)) {
		ConsistencyScanInfo csInfo = ConsistencyScanInfo();
		csInfo.consistency_scan_enabled = true;
		if (tokencmp(tokens[2], "restart")) {
			if (tokencmp(tokens[3], "0")) {
				csInfo.restart = false;
			} else if (tokencmp(tokens[3], "1")) {
				csInfo.restart = true;
			} else
				usageError = 1;
		} else {
			usageError = 1;
		}

		if (tokencmp(tokens[4], "maxRate")) {
			char* end;
			csInfo.max_rate = std::strtod((const char*)tokens[5].begin(), &end);
			if (!std::isspace(*end)) {
				fprintf(stderr, "ERROR: %s failed to parse.\n", printable(tokens[5]).c_str());
				return false;
			}
		} else {
			usageError = 1;
		}

		if (tokencmp(tokens[6], "targetInterval")) {
			char* end;
			csInfo.target_interval = std::strtod((const char*)tokens[7].begin(), &end);
			if (*end != '\0') {
				fprintf(stderr, "ERROR: %s failed to parse.\n", printable(tokens[7]).c_str());
				return false;
			}
		} else {
			usageError = 1;
		}
		// tr->set(consistencyScanInfoKey, csInfo);
		wait(ConsistencyScanInfo::setInfo(tr, csInfo));
		wait(tr->commit());
		// TODO: remove (printing for debug purpose)
		tr->reset();
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		state Optional<Value> consistencyScanInfoGet = wait(ConsistencyScanInfo::getInfo(tr));
		wait(tr->commit());
		if (consistencyScanInfoGet.present()) {
			printf("Consistency Scan Info: %s\n",
			       ObjectReader::fromStringRef<ConsistencyScanInfo>(consistencyScanInfoGet.get(), IncludeVersion())
			           .toString()
			           .c_str());
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
    CommandHelp("consistencyscan [on|off] <ARGS>",
                "enables or disables consistency scan",
                "Calling this command with `on' enables the consistency scan processe to run the scan with given "
                "arguments and `off' will halt the scan. "
                "Calling this command with no arguments will display if consistency scan is currently enabled.\n"));

} // namespace fdb_cli
