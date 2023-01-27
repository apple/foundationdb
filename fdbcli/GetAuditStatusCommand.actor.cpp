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
	if (tokens.size() < 3 || tokens.size() > 4) {
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
		if (tokens.size() != 4) {
			printUsage(tokens[0]);
			return false;
		}
		const UID id = UID::fromString(tokens[3].toString());
		AuditStorageState res = wait(getAuditState(cx, type, id));
		printf("Audit result is:\n%s", res.toString().c_str());
	} else if (tokencmp(tokens[2], "recent")) {
		int count = 5;
		if (tokens.size() == 4) {
			count = std::stoi(tokens[3].toString());
		}
		std::vector<AuditStorageState> res = wait(getLatestAuditStates(cx, type, count));
		for (const auto& it : res) {
			printf("Audit result is:\n%s\n", it.toString().c_str());
		}
	}
	return true;
}

CommandFactory getAuditStatusFactory(
    "get_audit_status",
    CommandHelp("get_audit_status <ha> <id|recent> [ARGs]",
                "Retrieve audit storage results of the specific type",
                "Fetch audit result with an ID: get_audit_status [Type] id [ID];\n"
                "Fetch most recent audit results: get_audit_status [Type] recent [Count].\n"));
} // namespace fdb_cli
