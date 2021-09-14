/*
 * Util.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/Status.h"

#include "flow/Arena.h"

#include "flow/ThreadHelper.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

bool tokencmp(StringRef token, const char* command) {
	if (token.size() != strlen(command))
		return false;

	return !memcmp(token.begin(), command, token.size());
}

void printUsage(StringRef command) {
	const auto& helpMap = CommandFactory::commands();
	auto i = helpMap.find(command.toString());
	if (i != helpMap.end())
		printf("Usage: %s\n", i->second.usage.c_str());
	else
		fprintf(stderr, "ERROR: Unknown command `%s'\n", command.toString().c_str());
}

ACTOR Future<std::string> getSpecialKeysFailureErrorMessage(Reference<ITransaction> tr) {
	// hold the returned standalone object's memory
	state ThreadFuture<Optional<Value>> errorMsgF = tr->get(fdb_cli::errorMsgSpecialKey);
	Optional<Value> errorMsg = wait(safeThreadFutureToFuture(errorMsgF));
	// Error message should be present
	ASSERT(errorMsg.present());
	// Read the json string
	auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
	// verify schema
	auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
	std::string errorStr;
	ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
	// return the error message
	return valueObj["message"].get_str();
}

ACTOR Future<Void> verifyAndAddInterface(std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface,
                                         Reference<FlowLock> connectLock,
                                         KeyValue kv) {
	wait(connectLock->take());
	state FlowLock::Releaser releaser(*connectLock);
	state ClientWorkerInterface workerInterf;
	try {
		// the interface is back-ward compatible, thus if parsing failed, it needs to upgrade cli version
		workerInterf = BinaryReader::fromStringRef<ClientWorkerInterface>(kv.value, IncludeVersion());
	} catch (Error& e) {
		fprintf(stderr, "Error: %s; CLI version is too old, please update to use a newer version\n", e.what());
		return Void();
	}
	state ClientLeaderRegInterface leaderInterf(workerInterf.address());
	choose {
		when(Optional<LeaderInfo> rep =
		         wait(brokenPromiseToNever(leaderInterf.getLeader.getReply(GetLeaderRequest())))) {
			StringRef ip_port =
			    (kv.key.endsWith(LiteralStringRef(":tls")) ? kv.key.removeSuffix(LiteralStringRef(":tls")) : kv.key)
			        .removePrefix(LiteralStringRef("\xff\xff/worker_interfaces/"));
			(*address_interface)[ip_port] = std::make_pair(kv.value, leaderInterf);

			if (workerInterf.reboot.getEndpoint().addresses.secondaryAddress.present()) {
				Key full_ip_port2 =
				    StringRef(workerInterf.reboot.getEndpoint().addresses.secondaryAddress.get().toString());
				StringRef ip_port2 = full_ip_port2.endsWith(LiteralStringRef(":tls"))
				                         ? full_ip_port2.removeSuffix(LiteralStringRef(":tls"))
				                         : full_ip_port2;
				(*address_interface)[ip_port2] = std::make_pair(kv.value, leaderInterf);
			}
		}
		when(wait(delay(CLIENT_KNOBS->CLI_CONNECT_TIMEOUT))) {}
	}
	return Void();
}

ACTOR Future<Void> getWorkerInterfaces(Reference<ITransaction> tr,
                                       std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface) {
	// Hold the reference to the standalone's memory
	state ThreadFuture<RangeResult> kvsFuture = tr->getRange(
	    KeyRangeRef(LiteralStringRef("\xff\xff/worker_interfaces/"), LiteralStringRef("\xff\xff/worker_interfaces0")),
	    CLIENT_KNOBS->TOO_MANY);
	RangeResult kvs = wait(safeThreadFutureToFuture(kvsFuture));
	ASSERT(!kvs.more);
	auto connectLock = makeReference<FlowLock>(CLIENT_KNOBS->CLI_CONNECT_PARALLELISM);
	std::vector<Future<Void>> addInterfs;
	for (auto it : kvs) {
		addInterfs.push_back(verifyAndAddInterface(address_interface, connectLock, it));
	}
	wait(waitForAll(addInterfs));
	return Void();
}

} // namespace fdb_cli
