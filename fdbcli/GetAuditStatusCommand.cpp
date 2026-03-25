/*
 * GetAuditStatusCommand.cpp
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

#include <fmt/core.h>

#include "fdbcli/fdbcli.h"
#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.h"
#include "fdbclient/IClientApi.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"

namespace fdb_cli {

Future<Void> getAuditProgressByRange(Database cx, AuditType auditType, UID auditId, KeyRange auditRange) {
	KeyRange rangeToRead = auditRange;
	Key rangeToReadBegin = auditRange.begin;
	int retryCount = 0;
	int64_t finishCount = 0;
	while (rangeToReadBegin < auditRange.end) {
		while (true) {
			Error err;
			try {
				rangeToRead = KeyRangeRef(rangeToReadBegin, auditRange.end);
				std::vector<AuditStorageState> auditStates =
				    co_await getAuditStateByRange(cx, auditType, auditId, rangeToRead);
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
				err = e;
			}
			if (err.code() == error_code_actor_cancelled) {
				throw err;
			}
			if (retryCount > 30) {
				fmt::println("Incomplete check");
				co_return;
			}
			co_await delay(0.5);
			retryCount++;
		}
	}
	fmt::println("Finished range count: {}", finishCount);
}

Future<std::vector<StorageServerInterface>> getStorageServers(Database cx) {
	Transaction tr(cx);
	while (true) {
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		Error err;
		try {
			RangeResult serverList = co_await tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
			ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);
			std::vector<StorageServerInterface> servers;
			servers.reserve(serverList.size());
			for (int i = 0; i < serverList.size(); i++)
				servers.push_back(decodeServerListValue(serverList[i].value));
			co_return servers;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<AuditPhase> getAuditProgressByServer(Database cx,
                                            AuditType auditType,
                                            UID auditId,
                                            KeyRange auditRange,
                                            UID serverId) {
	KeyRange rangeToRead = auditRange;
	Key rangeToReadBegin = auditRange.begin;
	int retryCount = 0;
	while (rangeToReadBegin < auditRange.end) {
		while (true) {
			Error err;
			try {
				rangeToRead = KeyRangeRef(rangeToReadBegin, auditRange.end);
				std::vector<AuditStorageState> auditStates =
				    co_await getAuditStateByServer(cx, auditType, auditId, serverId, rangeToRead);
				for (int i = 0; i < auditStates.size(); i++) {
					AuditPhase phase = auditStates[i].getPhase();
					if (phase == AuditPhase::Invalid) {
						co_return AuditPhase::Running;
					} else if (phase == AuditPhase::Error) {
						co_return AuditPhase::Error;
					}
				}
				rangeToReadBegin = auditStates.back().range.end;
				break;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_actor_cancelled) {
				throw err;
			}
			if (retryCount > 30) {
				co_return AuditPhase::Invalid;
			}
			co_await delay(0.5);
			retryCount++;
		}
	}
	co_return AuditPhase::Complete;
}

Future<Void> getAuditProgress(Database cx, AuditType auditType, UID auditId, KeyRange auditRange) {
	if (auditType == AuditType::ValidateHA || auditType == AuditType::ValidateReplica ||
	    auditType == AuditType::ValidateLocationMetadata || auditType == AuditType::ValidateRestore) {
		co_await getAuditProgressByRange(cx, auditType, auditId, auditRange);
	} else if (auditType == AuditType::ValidateStorageServerShard) {
		std::vector<Future<Void>> fs;
		std::unordered_map<UID, bool> res;
		std::vector<StorageServerInterface> const& interfs = co_await getStorageServers(cx);
		int i = 0;
		int numCompleteServers = 0;
		int numOngoingServers = 0;
		int numErrorServers = 0;
		int numTSSes = 0;
		for (; i < interfs.size(); i++) {
			if (interfs[i].isTss()) {
				numTSSes++;
				continue; // SSShard audit does not test TSS
			}
			AuditPhase serverPhase =
			    co_await getAuditProgressByServer(cx, auditType, auditId, allKeys, interfs[i].id());
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
}

Future<bool> getAuditStatusCommandActor(Database cx, std::vector<StringRef> tokens) {
	if (tokens.size() < 2 || tokens.size() > 5) {
		printUsage(tokens[0]);
		co_return false;
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
	} else if (tokencmp(tokens[1], "validate_restore")) {
		type = AuditType::ValidateRestore;
	} else {
		printUsage(tokens[0]);
		co_return false;
	}

	if (tokencmp(tokens[2], "id")) {
		if (tokens.size() != 4) {
			printUsage(tokens[0]);
			co_return false;
		}
		const UID id = UID::fromString(tokens[3].toString());
		AuditStorageState res = co_await getAuditState(cx, type, id);
		fmt::println("Audit result is:\n{}", res.toString());
	} else if (tokencmp(tokens[2], "progress")) {
		if (tokens.size() != 4) {
			printUsage(tokens[0]);
			co_return false;
		}
		const UID id = UID::fromString(tokens[3].toString());
		AuditStorageState res = co_await getAuditState(cx, type, id);
		if (res.getPhase() == AuditPhase::Running) {
			co_await getAuditProgress(cx, res.getType(), res.id, res.range);
		} else {
			fmt::println("Already complete");
		}
	} else if (tokencmp(tokens[2], "recent")) {
		int count = CLIENT_KNOBS->TOO_MANY;
		if (tokens.size() == 4) {
			count = std::stoi(tokens[3].toString());
		}
		std::vector<AuditStorageState> const& res = co_await getAuditStates(cx, type, /*newFirst=*/true, count);
		for (const auto& it : res) {
			fmt::println("Audit result is:\n{}", it.toString());
		}
	} else if (tokencmp(tokens[2], "phase")) {
		AuditPhase phase = stringToAuditPhase(tokens[3].toString());
		if (phase == AuditPhase::Invalid) {
			printUsage(tokens[0]);
			co_return false;
		}
		int count = CLIENT_KNOBS->TOO_MANY;
		if (tokens.size() == 5) {
			count = std::stoi(tokens[4].toString());
		}
		std::vector<AuditStorageState> const& res = co_await getAuditStates(cx, type, /*newFirst=*/true, count, phase);
		for (const auto& it : res) {
			fmt::println("Audit result is:\n{}", it.toString());
		}
	} else {
		printUsage(tokens[0]);
		co_return false;
	}

	co_return true;
}

CommandFactory getAuditStatusFactory(
    "get_audit_status",
    CommandHelp(
        "get_audit_status [ha|replica|locationmetadata|ssshard|validate_restore] [id|recent|phase|progress] [ARGs]",
        "Retrieve audit storage status",
        "To fetch audit status via ID: `get_audit_status [Type] id [ID]'\n"
        "To fetch status of most recent audit: `get_audit_status [Type] recent [Count]'\n"
        "To fetch status of audits in a specific phase: `get_audit_status [Type] phase "
        "[running|complete|failed|error] count'\n"
        "To fetch audit progress via ID: `get_audit_status [Type] progress [ID]'\n"
        "Supported types include: 'ha', `replica`, `locationmetadata`, `ssshard`, `validate_restore`. \n"
        "If specified, `Count' is how many rows to audit.\n"
        "If not specified, check all rows in audit.\n"
        "Phase can be `Invalid=0', `Running=1', `Complete=2', `Error=3', or `Failed=4'.\n"
        "See also `audit_storage' command."));
} // namespace fdb_cli
