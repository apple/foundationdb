/*
 * AuditStorageCommand.actor.cpp
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

#include "fdbclient/IClientApi.h"

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/Audit.h"

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

ACTOR Future<UID> auditStorageCommandActor(Reference<IClusterConnectionRecord> clusterFile,
                                           std::vector<StringRef> tokens) {
	if (tokens.size() < 2) {
		printUsage(tokens[0]);
		return UID();
	}

	state UID resAuditId;
	if (tokencmp(tokens[1], "cancel")) {
		if (tokens.size() != 4) {
			printUsage(tokens[0]);
			return UID();
		}
		AuditType type = AuditType::Invalid;
		if (tokencmp(tokens[2], "ha")) {
			type = AuditType::ValidateHA;
		} else if (tokencmp(tokens[2], "replica")) {
			type = AuditType::ValidateReplica;
		} else if (tokencmp(tokens[2], "locationmetadata")) {
			type = AuditType::ValidateLocationMetadata;
		} else if (tokencmp(tokens[2], "ssshard")) {
			type = AuditType::ValidateStorageServerShard;
		} else {
			printUsage(tokens[0]);
			return UID();
		}
		const UID auditId = UID::fromString(tokens[3].toString());
		UID cancelledAuditId = wait(cancelAuditStorage(clusterFile, type, auditId, /*timeoutSeconds=*/60));
		resAuditId = cancelledAuditId;

	} else if (tokencmp(tokens[1], "schedule")) {
		if (tokens.size() != 6) {
			printUsage(tokens[0]);
			return UID();
		}
		AuditType type = AuditType::Invalid;
		if (tokencmp(tokens[2], "ha")) {
			type = AuditType::ValidateHA;
		} else if (tokencmp(tokens[2], "replica")) {
			type = AuditType::ValidateReplica;
		} else if (tokencmp(tokens[2], "locationmetadata")) {
			type = AuditType::ValidateLocationMetadata;
		} else if (tokencmp(tokens[2], "ssshard")) {
			type = AuditType::ValidateStorageServerShard;
		} else {
			printUsage(tokens[0]);
			return UID();
		}
		Key begin = tokens[3];
		Key end = tokens[4];
		if (end > allKeys.end) {
			end = allKeys.end;
		}
		const double periodHours = std::stod(tokens[5].toString());
		if (periodHours <= 0) {
			printUsage(tokens[0]);
			return UID();
		}
		wait(
		    schedulePeriodAuditStorage(clusterFile, KeyRangeRef(begin, end), type, periodHours, /*timeoutSeconds=*/60));
		resAuditId = UID();

	} else if (tokencmp(tokens[1], "cancelschedule")) {
		if (tokens.size() != 3) {
			printUsage(tokens[0]);
			return UID();
		}
		AuditType type = AuditType::Invalid;
		if (tokencmp(tokens[2], "ha")) {
			type = AuditType::ValidateHA;
		} else if (tokencmp(tokens[2], "replica")) {
			type = AuditType::ValidateReplica;
		} else if (tokencmp(tokens[2], "locationmetadata")) {
			type = AuditType::ValidateLocationMetadata;
		} else if (tokencmp(tokens[2], "ssshard")) {
			type = AuditType::ValidateStorageServerShard;
		} else {
			printUsage(tokens[0]);
			return UID();
		}
		wait(cancelSchedulePeriodAuditStorage(clusterFile, type, /*timeoutSeconds=*/60));
		resAuditId = UID();

	} else {
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
			return UID();
		}

		Key begin = allKeys.begin, end = allKeys.end;
		if (tokens.size() == 3) {
			begin = tokens[2];
		} else if (tokens.size() == 4) {
			begin = tokens[2];
			end = tokens[3];
		} else {
			printUsage(tokens[0]);
			return UID();
		}
		if (end > allKeys.end) {
			end = allKeys.end;
		}

		UID startedAuditId = wait(auditStorage(clusterFile, KeyRangeRef(begin, end), type, /*timeoutSeconds=*/60));
		resAuditId = startedAuditId;
	}
	return resAuditId;
}

CommandFactory auditStorageFactory(
    "audit_storage",
    CommandHelp("audit_storage <Type> [BeginKey EndKey]",
                "Start an audit storage",
                "Specify audit `Type' (only `ha' and `replica` and `locationmetadata` and "
                "`ssshard` `Type' is supported currently), and\n"
                "optionally a sub-range with `BeginKey' and `EndKey'.\n"
                "For example, to audit the full key range: `audit_storage ha'\n"
                "To audit a sub-range only: `audit_storage ha 0xa 0xb'\n"
                "Returns an audit `ID'. See also `get_audit_status' command.\n"
                "To cancel an audit: audit_storage cancel auditType auditId"));
} // namespace fdb_cli
