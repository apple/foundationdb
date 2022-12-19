/*
 * ForceRecoveryWithDataLossCommand.actor.cpp
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
	if (tokens.size() < 3) {
		printUsage(tokens[0]);
		return UID();
	}

	UID auditId = wait(auditStorage(clusterFile, KeyRangeRef(tokens[2], tokens[3]), AuditType::ValidateHA, true));
	return auditId;
}

CommandFactory auditStorageFactory("audit_storage",
                                   CommandHelp("audit_storage <BeginKey> <EndKey>",
                                               "Start an audit storage",
                                               "Trigger an audit storage, the auditID is returned.\n"));
} // namespace fdb_cli
