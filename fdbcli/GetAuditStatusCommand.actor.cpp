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
	if (tokens.size() < 2 || tokens.size() > 4) {
		printUsage(tokens[0]);
		return false;
	}

	AuditType type = AuditType::Invalid;
	if (tokencmp(tokens[1], "ha")) {
		type = AuditType::ValidateHA;
	} else if (tokencmp(tokens[1], "replica")) {
		type = AuditType::ValidateReplica;
	} else if (tokencmp(tokens[1], "locationmetadata")) {
		type = AuditType::ValidateLocationMetadata;
	} else if (tokencmp(tokens[1], "ssshard")) {
		type = AuditType::ValidateStorageServerShard;
	} else if (tokencmp(tokens[1], "checkmigration")) {
		type = AuditType::CheckMigrationStatus;
	} else {
		printUsage(tokens[0]);
		return false;
	}

	if (tokens.size() == 2) {
		ASSERT(type == AuditType::CheckMigrationStatus);
		std::string res = wait(checkMigrationProgress(cx));
		printf("\n%s", res.c_str());
	} else if (tokencmp(tokens[2], "id")) {
		if (tokens.size() != 4) {
			printUsage(tokens[0]);
			return false;
		}
		const UID id = UID::fromString(tokens[3].toString());
		AuditStorageState res = wait(getAuditState(cx, type, id));
		printf("Audit result is:\n%s", res.toStringForCLI().c_str());
	} else if (tokencmp(tokens[2], "recent")) {
		int count = CLIENT_KNOBS->TOO_MANY;
		if (tokens.size() == 4) {
			count = std::stoi(tokens[3].toString());
		}
		std::vector<AuditStorageState> res = wait(getAuditStates(cx, type, /*newFirst=*/true, count));
		for (const auto& it : res) {
			printf("Audit result is:\n%s\n", it.toString().c_str());
		}
	} else {
		printUsage(tokens[0]);
		return false;
	}

	return true;
}

CommandFactory getAuditStatusFactory(
    "get_audit_status",
    CommandHelp("get_audit_status [ha|replica|locationmetadata|ssshard|checkmigration] [id|recent] [ARGs]",
                "Retrieve audit storage status",
                "To fetch audit status via ID: `get_audit_status [Type] id [ID]'\n"
                "To fetch status of most recent audit: `get_audit_status [Type] recent [Count]'\n"
                "Supported types include: 'ha', `replica`, `locationmetadata`, `ssshard`, \n"
                "and `checkmigration`. If specified, `Count' is how many\n"
                "rows to audit. If not specified, check all rows in audit.\n"
                "get_audit_status checkmigration prints out the number of data shards and physical shards."
                "Results have the following format: if not `checkmigration`\n"
                "  `[ID]: 000000000001000000000000, [Range]:  - 0xff, [Type]: 1, [Phase]: 2'\n"
                "where `Type' is `1' for `ha' and `Phase' is `2' for `Complete'.\n"
                "Phase can be `Invalid=0', `Running=1', `Complete=2', `Error=3', or `Failed=4'.\n"
                "See also `audit_storage' command."));
} // namespace fdb_cli
