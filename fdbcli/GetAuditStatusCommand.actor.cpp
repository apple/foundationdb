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

ACTOR Future<Void> getAuditProgress(Database cx, AuditType auditType, UID auditId, KeyRange auditRange) {
	if (auditType == AuditType::ValidateStorageServerShard) {
		printf("ssshard not supported yet\n");
	}
	state KeyRange rangeToRead = auditRange;
	state Key rangeToReadBegin = auditRange.begin;
	state int retryCount = 0;
	state int64_t finishCount = 0;
	state int64_t unfinishedCount = 0;
	while (rangeToReadBegin < auditRange.end) {
		loop {
			try {
				rangeToRead = KeyRangeRef(rangeToReadBegin, auditRange.end);
				state std::vector<AuditStorageState> auditStates =
				    wait(getAuditStateByRange(cx, auditType, auditId, rangeToRead));
				for (int i = 0; i < auditStates.size(); i++) {
					AuditPhase phase = auditStates[i].getPhase();
					if (phase == AuditPhase::Invalid) {
						printf("( Unfinished ) %s\n", auditStates[i].range.toString().c_str());
						++unfinishedCount;
					} else if (phase == AuditPhase::Error) {
						printf("( Error   ) %s\n", auditStates[i].range.toString().c_str());
						++finishCount;
					} else {
						++finishCount;
						continue;
					}
				}
				rangeToReadBegin = auditStates.back().range.end;
				break;
			} catch (Error& e) {
				if (retryCount > 30) {
					printf("Imcomplete check\n");
					return Void();
				}
				wait(delay(0.5));
				retryCount++;
			}
		}
	}
	printf("Finished range count: %ld\n", finishCount);
	printf("Unfinished range count: %ld\n", unfinishedCount);
	return Void();
}

ACTOR Future<bool> getAuditStatusCommandActor(Database cx, std::vector<StringRef> tokens) {
	if (tokens.size() < 2 || tokens.size() > 5) {
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
	} else if (tokencmp(tokens[2], "progress")) {
		if (tokens.size() != 4) {
			printUsage(tokens[0]);
			return false;
		}
		const UID id = UID::fromString(tokens[3].toString());
		state AuditStorageState res = wait(getAuditState(cx, type, id));
		if (res.getPhase() == AuditPhase::Running) {
			wait(getAuditProgress(cx, res.getType(), res.id, res.range));
		} else {
			printf("Already complete\n");
		}
	} else if (tokencmp(tokens[2], "recent")) {
		int count = CLIENT_KNOBS->TOO_MANY;
		if (tokens.size() == 4) {
			count = std::stoi(tokens[3].toString());
		}
		std::vector<AuditStorageState> res = wait(getAuditStates(cx, type, /*newFirst=*/true, count));
		for (const auto& it : res) {
			printf("Audit result is:\n%s\n", it.toString().c_str());
		}
	} else if (tokencmp(tokens[2], "phase")) {
		AuditPhase phase = stringToAuditPhase(tokens[3].toString());
		if (phase == AuditPhase::Invalid) {
			printUsage(tokens[0]);
			return false;
		}
		int count = CLIENT_KNOBS->TOO_MANY;
		if (tokens.size() == 5) {
			count = std::stoi(tokens[4].toString());
		}
		std::vector<AuditStorageState> res = wait(getAuditStates(cx, type, /*newFirst=*/true, count, phase));
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
    CommandHelp("get_audit_status [ha|replica|locationmetadata|ssshard] [id|recent|phase|progress] [ARGs]",
                "Retrieve audit storage status",
                "To fetch audit status via ID: `get_audit_status [Type] id [ID]'\n"
                "To fetch status of most recent audit: `get_audit_status [Type] recent [Count]'\n"
                "To fetch status of audits in a specific phase: `get_audit_status [Type] phase "
                "[running|complete|failed|error] count'\n"
                "To fetch audit progress via ID: `get_audit_status [Type] progress [ID]'\n"
                "Supported types include: 'ha', `replica`, `locationmetadata`, `ssshard`. \n"
                "If specified, `Count' is how many rows to audit.\n"
                "If not specified, check all rows in audit.\n"
                "Phase can be `Invalid=0', `Running=1', `Complete=2', `Error=3', or `Failed=4'.\n"
                "See also `audit_storage' command."));
} // namespace fdb_cli
