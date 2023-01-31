/*
 * GetAuditStatusCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.actor.h"
#include "fdbclient/IClientApi.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<bool> getAuditStatusCommandActor(Database cx, std::vector<StringRef> tokens) {
	if (tokens.size() != 4) {
		printUsage(tokens[0]);
		return false;
	}

	AuditType type = AuditType::Invalid;
	if (tokencmp(tokens[1], "ha")) {
		type = AuditType::ValidateHA;
	} else {
		printUsage(tokens[0]);
		return false;
	}

	if (tokencmp(tokens[2], "id")) {
		const UID id = UID::fromString(tokens[3].toString());
		AuditStorageState res = wait(getAuditState(cx, type, id));
		printf("Audit result is:\n%s", res.toString().c_str());
	} else if (tokencmp(tokens[2], "recent")) {
		const int count = std::stoi(tokens[3].toString());
		std::vector<AuditStorageState> res = wait(getLatestAuditStates(cx, type, count));
		for (const auto& it : res) {
			printf("Audit result is:\n%s\n", it.toString().c_str());
		}
	}
	return true;
}

CommandFactory getAuditStatusFactory(
    "get_audit_status",
    CommandHelp("get_audit_status <Type> <id|recent> [ARGs]",
                "Retrieve audit storage status",
                "To fetch audit status via ID: `get_audit_status [Type] id [ID]'\n"
                "To fetch status of most recent audit: `get_audit_status [Type] recent [Count]'\n"
                "Only 'ha' `Type' is supported currently. If specified, `Count' is how many\n"
                "rows to audit. If not specified, check all rows in audit.\n"
                "Results have the following format:\n"
                "  `[ID]: 000000000001000000000000, [Range]:  - 0xff, [Type]: 1, [Phase]: 2'\n"
                "where `Type' is `1' for `ha' and `Phase' is `2' for `Complete'.\n"
                "Phase can be `Invalid=0', `Running=1', `Complete=2', `Error=3', or `Failed=4'.\n"
                "See also `audit_storage' command."));
} // namespace fdb_cli
