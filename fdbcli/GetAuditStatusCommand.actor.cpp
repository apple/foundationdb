/*
 * GetAuditStatusCommand.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include <fmt/core.h>

#include "fdbcli/fdbcli.actor.h"
#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.actor.h"
#include "fdbclient/IClientApi.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<Void> getAuditProgressByRange(Database cx, AuditType auditType, UID auditId, KeyRange auditRange) {
	state KeyRange rangeToRead = auditRange;
	state Key rangeToReadBegin = auditRange.begin;
	state int retryCount = 0;
	state int64_t finishCount = 0;
	while (rangeToReadBegin < auditRange.end) {
		loop {
			try {
				rangeToRead = KeyRangeRef(rangeToReadBegin, auditRange.end);
				state std::vector<AuditStorageState> auditStates =
				    wait(getAuditStateByRange(cx, auditType, auditId, rangeToRead));
				for (int i = 0; i < auditStates.size(); i++) {
					AuditPhase phase = auditStates[i].getPhase();
					if (phase == AuditPhase::Invalid) {
						fmt::println("( Ongoing ) {}", auditStates[i].range.toString());
					} else if (phase == AuditPhase::Error) {
						fmt::println("( Error   ) {}", auditStates[i].range.toString());
						++finishCount;
					} else {
						++finishCount;
						continue;
					}
				}
				rangeToReadBegin = auditStates.back().range.end;
				break;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw e;
				}
				if (retryCount > 30) {
					fmt::println("Incomplete check");
					return Void();
				}
				wait(delay(0.5));
				retryCount++;
			}
		}
	}
	fmt::println("Finished range count: {}", finishCount);
	return Void();
}

ACTOR Future<std::vector<StorageServerInterface>> getStorageServers(Database cx) {
	state Transaction tr(cx);
	loop {
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			RangeResult serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);
			std::vector<StorageServerInterface> servers;
			servers.reserve(serverList.size());
			for (int i = 0; i < serverList.size(); i++)
				servers.push_back(decodeServerListValue(serverList[i].value));
			return servers;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<AuditPhase> getAuditProgressByServer(Database cx,
                                                  AuditType auditType,
                                                  UID auditId,
                                                  KeyRange auditRange,
                                                  UID serverId) {
	state KeyRange rangeToRead = auditRange;
	state Key rangeToReadBegin = auditRange.begin;
	state int retryCount = 0;
	while (rangeToReadBegin < auditRange.end) {
		loop {
			try {
				rangeToRead = KeyRangeRef(rangeToReadBegin, auditRange.end);
				state std::vector<AuditStorageState> auditStates =
				    wait(getAuditStateByServer(cx, auditType, auditId, serverId, rangeToRead));
				for (int i = 0; i < auditStates.size(); i++) {
					AuditPhase phase = auditStates[i].getPhase();
					if (phase == AuditPhase::Invalid) {
						return AuditPhase::Running;
					} else if (phase == AuditPhase::Error) {
						return AuditPhase::Error;
					}
				}
				rangeToReadBegin = auditStates.back().range.end;
				break;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw e;
				}
				if (retryCount > 30) {
					return AuditPhase::Invalid;
				}
				wait(delay(0.5));
				retryCount++;
			}
		}
	}
	return AuditPhase::Complete;
}

ACTOR Future<Void> getAuditProgress(Database cx, AuditType auditType, UID auditId, KeyRange auditRange) {
	if (auditType == AuditType::ValidateHA || auditType == AuditType::ValidateReplica ||
	    auditType == AuditType::ValidateLocationMetadata) {
		wait(getAuditProgressByRange(cx, auditType, auditId, auditRange));
	} else if (auditType == AuditType::ValidateStorageServerShard) {
		state std::vector<Future<Void>> fs;
		state std::unordered_map<UID, bool> res;
		state std::vector<StorageServerInterface> interfs = wait(getStorageServers(cx));
		state int i = 0;
		state int numCompleteServers = 0;
		state int numOngoingServers = 0;
		state int numErrorServers = 0;
		state int numTSSes = 0;
		for (; i < interfs.size(); i++) {
			if (interfs[i].isTss()) {
				numTSSes++;
				continue; // SSShard audit does not test TSS
			}
			AuditPhase serverPhase = wait(getAuditProgressByServer(cx, auditType, auditId, allKeys, interfs[i].id()));
			if (serverPhase == AuditPhase::Running) {
				numOngoingServers++;
			} else if (serverPhase == AuditPhase::Complete) {
				numCompleteServers++;
			} else if (serverPhase == AuditPhase::Error) {
				numErrorServers++;
			} else if (serverPhase == AuditPhase::Invalid) {
				fmt::println("SS {} partial progress fetched", interfs[i].id().toString());
			}
		}
		fmt::println("CompleteServers: {}", numCompleteServers);
		fmt::println("OngoingServers: {}", numOngoingServers);
		fmt::println("ErrorServers: {}", numErrorServers);
		fmt::println("IgnoredTSSes: {}", numTSSes);
	} else {
		fmt::println("AuditType not implemented");
	}
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
		fmt::println("Audit result is:\n{}", res.toString());
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
			fmt::println("Already complete");
		}
	} else if (tokencmp(tokens[2], "recent")) {
		int count = CLIENT_KNOBS->TOO_MANY;
		if (tokens.size() == 4) {
			count = std::stoi(tokens[3].toString());
		}
		std::vector<AuditStorageState> res = wait(getAuditStates(cx, type, /*newFirst=*/true, count));
		for (const auto& it : res) {
			fmt::println("Audit result is:\n{}", it.toString());
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
			fmt::println("Audit result is:\n{}", it.toString());
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
