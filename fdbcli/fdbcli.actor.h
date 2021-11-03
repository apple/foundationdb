/*
 * fdbcli.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated
// version of this file.  In intellisense use the source version.
#if defined(NO_INTELLISENSE) && !defined(FDBCLI_FDBCLI_ACTOR_G_H)
#define FDBCLI_FDBCLI_ACTOR_G_H
#include "fdbcli/fdbcli.actor.g.h"
#elif !defined(FDBCLI_FDBCLI_ACTOR_H)
#define FDBCLI_FDBCLI_ACTOR_H

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/IClientApi.h"
#include "fdbclient/StatusClient.h"
#include "flow/Arena.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace fdb_cli {

struct CommandHelp {
	std::string usage;
	std::string short_desc;
	std::string long_desc;
	CommandHelp() {}
	CommandHelp(const char* u, const char* s, const char* l) : usage(u), short_desc(s), long_desc(l) {}
};

struct CommandFactory {
	CommandFactory(const char* name, CommandHelp help) { commands()[name] = help; }
	CommandFactory(const char* name) { hiddenCommands().insert(name); }
	static std::map<std::string, CommandHelp>& commands() {
		static std::map<std::string, CommandHelp> helpMap;
		return helpMap;
	}
	static std::set<std::string>& hiddenCommands() {
		static std::set<std::string> commands;
		return commands;
	}
};

// Special keys used by fdbcli commands
// advanceversion
extern const KeyRef advanceVersionSpecialKey;
// consistencycheck
extern const KeyRef consistencyCheckSpecialKey;
// datadistribution
extern const KeyRef ddModeSpecialKey;
extern const KeyRef ddIgnoreRebalanceSpecialKey;
// maintenance
extern const KeyRangeRef maintenanceSpecialKeyRange;
extern const KeyRef ignoreSSFailureSpecialKey;
// setclass
extern const KeyRangeRef processClassSourceSpecialKeyRange;
extern const KeyRangeRef processClassTypeSpecialKeyRange;
// Other special keys
inline const KeyRef errorMsgSpecialKey = LiteralStringRef("\xff\xff/error_message");
// help functions (Copied from fdbcli.actor.cpp)
// decode worker interfaces
ACTOR Future<Void> addInterface(std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface,
                                Reference<FlowLock> connectLock,
                                KeyValue kv);

// compare StringRef with the given c string
bool tokencmp(StringRef token, const char* command);
// print the usage of the specified command
void printUsage(StringRef command);
// Pre: tr failed with special_keys_api_failure error
// Read the error message special key and return the message
ACTOR Future<std::string> getSpecialKeysFailureErrorMessage(Reference<ITransaction> tr);
// Using \xff\xff/worker_interfaces/ special key, get all worker interfaces
ACTOR Future<Void> getWorkerInterfaces(Reference<ITransaction> tr,
                                       std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface);
// Deserialize \xff\xff/worker_interfaces/<address>:=<ClientInterface> k-v pair and verify by a RPC call
ACTOR Future<Void> verifyAndAddInterface(std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface,
                                         Reference<FlowLock> connectLock,
                                         KeyValue kv);
// print cluster status info
void printStatus(StatusObjectReader statusObj,
                 StatusClient::StatusLevel level,
                 bool displayDatabaseAvailable = true,
                 bool hideErrorMessages = false);

// All fdbcli commands (alphabetically)
// advanceversion command
ACTOR Future<bool> advanceVersionCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// cache_range command
ACTOR Future<bool> cacheRangeCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// consistency command
ACTOR Future<bool> consistencyCheckCommandActor(Reference<ITransaction> tr,
                                                std::vector<StringRef> tokens,
                                                bool intrans);
// datadistribution command
ACTOR Future<bool> dataDistributionCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// expensive_data_check command
ACTOR Future<bool> expensiveDataCheckCommandActor(
    Reference<IDatabase> db,
    Reference<ITransaction> tr,
    std::vector<StringRef> tokens,
    std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface);
// force_recovery_with_data_loss command
ACTOR Future<bool> forceRecoveryWithDataLossCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// kill command
ACTOR Future<bool> killCommandActor(Reference<IDatabase> db,
                                    Reference<ITransaction> tr,
                                    std::vector<StringRef> tokens,
                                    std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface);
// maintenance command
ACTOR Future<bool> setHealthyZone(Reference<IDatabase> db, StringRef zoneId, double seconds, bool printWarning = false);
ACTOR Future<bool> clearHealthyZone(Reference<IDatabase> db,
                                    bool printWarning = false,
                                    bool clearSSFailureZoneString = false);
ACTOR Future<bool> maintenanceCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// profile command
ACTOR Future<bool> profileCommandActor(Reference<ITransaction> tr, std::vector<StringRef> tokens, bool intrans);
// setclass command
ACTOR Future<bool> setClassCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// snapshot command
ACTOR Future<bool> snapshotCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// status command
ACTOR Future<bool> statusCommandActor(Reference<IDatabase> db,
                                      Database localDb,
                                      std::vector<StringRef> tokens,
                                      bool isExecMode = false);
// suspend command
ACTOR Future<bool> suspendCommandActor(Reference<IDatabase> db,
                                       Reference<ITransaction> tr,
                                       std::vector<StringRef> tokens,
                                       std::map<Key, std::pair<Value, ClientLeaderRegInterface>>* address_interface);
// throttle command
ACTOR Future<bool> throttleCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);
// triggerteaminfolog command
ACTOR Future<Void> triggerddteaminfologCommandActor(Reference<IDatabase> db);
// tssq command
ACTOR Future<bool> tssqCommandActor(Reference<IDatabase> db, std::vector<StringRef> tokens);

} // namespace fdb_cli

#include "flow/unactorcompiler.h"
#endif
