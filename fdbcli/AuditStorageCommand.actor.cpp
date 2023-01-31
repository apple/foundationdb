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

	AuditType type = AuditType::Invalid;
	if (tokencmp(tokens[1], "ha")) {
		type = AuditType::ValidateHA;
	} else {
		printUsage(tokens[0]);
		return UID();
	}

	Key begin, end;
	if (tokens.size() == 2) {
		begin = allKeys.begin;
		end = allKeys.end;
	} else if (tokens.size() == 3) {
		begin = tokens[2];
	} else if (tokens.size() == 4) {
		begin = tokens[2];
		end = tokens[3];
	} else {
		printUsage(tokens[0]);
		return UID();
	}

	UID auditId = wait(auditStorage(clusterFile, KeyRangeRef(begin, end), type, true));
	return auditId;
}

CommandFactory auditStorageFactory("audit_storage",
                                   CommandHelp("audit_storage <Type> [BeginKey EndKey]",
                                               "Start an audit storage",
                                               "Specify audit `Type' (only `ha' `Type' is supported currently), and\n"
                                               "optionally a sub-range with `BeginKey' and `EndKey'.\n"
                                               "For example, to audit the full key range: `audit_storage ha'\n"
                                               "To audit a sub-range only: `audit_storage ha 0xa 0xb'\n"
                                               "Returns an audit `ID'. See also `get_audit_status' command.\n"));
} // namespace fdb_cli
