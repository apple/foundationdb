/*
 * Util.actor.cpp
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

void addInterfacesFromKVs(RangeResult& kvs,
                          std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface) {
	for (const auto& kv : kvs) {
		ClientWorkerInterface workerInterf;
		try {
			// the interface is back-ward compatible, thus if parsing failed, it needs to upgrade cli version
			workerInterf = BinaryReader::fromStringRef<ClientWorkerInterface>(kv.value, IncludeVersion());
		} catch (Error& e) {
			fprintf(stderr, "Error: %s; CLI version is too old, please update to use a newer version\n", e.what());
			return;
		}
		ClientLeaderRegInterface leaderInterf(workerInterf.address());
		StringRef ip_port = (kv.key.endsWith(":tls"_sr) ? kv.key.removeSuffix(":tls"_sr) : kv.key)
		                        .removePrefix("\xff\xff/worker_interfaces/"_sr);
		(*address_interface)[ip_port] = std::make_pair(kv.value, leaderInterf);

		if (workerInterf.reboot.getEndpoint().addresses.secondaryAddress.present()) {
			Key full_ip_port2 =
			    StringRef(workerInterf.reboot.getEndpoint().addresses.secondaryAddress.get().toString());
			StringRef ip_port2 =
			    full_ip_port2.endsWith(":tls"_sr) ? full_ip_port2.removeSuffix(":tls"_sr) : full_ip_port2;
			(*address_interface)[ip_port2] = std::make_pair(kv.value, leaderInterf);
		}
	}
}

ACTOR Future<Void> getWorkerInterfaces(Reference<ITransaction> tr,
                                       std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface,
                                       bool verify) {
	if (verify) {
		tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		tr->set(workerInterfacesVerifyOptionSpecialKey, ValueRef());
	}
	// Hold the reference to the standalone's memory
	state ThreadFuture<RangeResult> kvsFuture = tr->getRange(
	    KeyRangeRef("\xff\xff/worker_interfaces/"_sr, "\xff\xff/worker_interfaces0"_sr), CLIENT_KNOBS->TOO_MANY);
	state RangeResult kvs = wait(safeThreadFutureToFuture(kvsFuture));
	ASSERT(!kvs.more);
	if (verify) {
		// remove the option if set
		tr->clear(workerInterfacesVerifyOptionSpecialKey);
	}
	addInterfacesFromKVs(kvs, address_interface);
	return Void();
}

ACTOR Future<bool> getWorkers(Reference<IDatabase> db, std::vector<ProcessData>* workers) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state ThreadFuture<RangeResult> processClasses = tr->getRange(processClassKeys, CLIENT_KNOBS->TOO_MANY);
			state ThreadFuture<RangeResult> processData = tr->getRange(workerListKeys, CLIENT_KNOBS->TOO_MANY);

			wait(success(safeThreadFutureToFuture(processClasses)) && success(safeThreadFutureToFuture(processData)));
			ASSERT(!processClasses.get().more && processClasses.get().size() < CLIENT_KNOBS->TOO_MANY);
			ASSERT(!processData.get().more && processData.get().size() < CLIENT_KNOBS->TOO_MANY);

			state std::map<Optional<Standalone<StringRef>>, ProcessClass> id_class;
			state int i;
			for (i = 0; i < processClasses.get().size(); i++) {
				try {
					id_class[decodeProcessClassKey(processClasses.get()[i].key)] =
					    decodeProcessClassValue(processClasses.get()[i].value);
				} catch (Error& e) {
					fprintf(stderr, "Error: %s; Client version is too old, please use a newer version\n", e.what());
					return false;
				}
			}

			for (i = 0; i < processData.get().size(); i++) {
				ProcessData data = decodeWorkerListValue(processData.get()[i].value);
				ProcessClass processClass = id_class[data.locality.processId()];

				if (processClass.classSource() == ProcessClass::DBSource ||
				    data.processClass.classType() == ProcessClass::UnsetClass)
					data.processClass = processClass;

				if (data.processClass.classType() != ProcessClass::TesterClass)
					workers->push_back(data);
			}

			return true;
		} catch (Error& e) {
			TraceEvent(SevWarn, "GetWorkersError").error(e);
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

ACTOR Future<Void> getStorageServerInterfaces(Reference<IDatabase> db,
                                              std::map<std::string, StorageServerInterface>* interfaces) {
	state Reference<ITransaction> tr = db->createTransaction();
	loop {
		interfaces->clear();
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			state ThreadFuture<RangeResult> serverListF = tr->getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
			wait(success(safeThreadFutureToFuture(serverListF)));
			ASSERT(!serverListF.get().more);
			ASSERT_LT(serverListF.get().size(), CLIENT_KNOBS->TOO_MANY);
			RangeResult serverList = serverListF.get();
			// decode server interfaces
			for (int i = 0; i < serverList.size(); i++) {
				auto ssi = decodeServerListValue(serverList[i].value);
				(*interfaces)[ssi.address().toString()] = ssi;
			}
			return Void();
		} catch (Error& e) {
			TraceEvent(SevWarn, "GetStorageServerInterfacesError").error(e);
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

} // namespace fdb_cli
