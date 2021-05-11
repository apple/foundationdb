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

#include "fdbclient/IClientApi.h"
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

// consistencycheck
extern const KeyRef consistencyCheckSpecialKey;

// help functions (Copied from fdbcli.actor.cpp)

// compare StringRef with the given c string
bool tokencmp(StringRef token, const char* command);
// print the usage of the specified command
void printUsage(StringRef command);

// All fdbcli commands (alphabetically)
// consistency command
ACTOR Future<bool> consistencyCheckCommandActor(Reference<ITransaction> tr, std::vector<StringRef> tokens);

} // namespace fdb_cli

#include "flow/unactorcompiler.h"
#endif
