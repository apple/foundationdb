/*
 * FDBExecHelper.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_EXEC_HELPER_ACTOR_G_H)
#define FDBSERVER_EXEC_HELPER_ACTOR_G_H
#include "fdbserver/FDBExecHelper.actor.g.h"
#elif !defined(FDBSERVER_EXEC_HELPER_ACTOR_H)
#define FDBSERVER_EXEC_HELPER_ACTOR_H

#include <string>
#include <vector>
#include <map>
#include "flow/Arena.h"
#include "flow/flow.h"
#include "fdbclient/FDBTypes.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// execute/snapshot command takes two arguments: <param1> <param2>
// param1 - represents the command type/name
// param2 - takes a binary path followed by a set of arguments in the following
// format <binary-path>:<key1=val1>,<key2=val2>...
// this class will abstract the format and give functions to get various pieces
// of information
class ExecCmdValueString {
public: // ctor & dtor
	ExecCmdValueString() {}
	explicit ExecCmdValueString(StringRef cmdValueString);

public: // interfaces
	StringRef getBinaryPath() const;
	VectorRef<StringRef> getBinaryArgs() const;
	void setCmdValueString(StringRef cmdValueString);
	StringRef getCmdValueString(void) const;

public: // helper functions
	void dbgPrint() const;

private: // functions
	void parseCmdValue();

private: // data
	Standalone<StringRef> cmdValueString;
	Standalone<VectorRef<StringRef>> binaryArgs;
	StringRef binaryPath;
};

// FIXME: move this function to a common location
// spawns a process pointed by `binPath` and the arguments provided at `paramList`,
// if the process spawned takes more than `maxWaitTime` then it will be killed
// if isSync is set to true then the process will be synchronously executed
// if async and in simulator then delay spawning the process to max of maxSimDelayTime
ACTOR Future<int> spawnProcess(std::string binPath,
                               std::vector<std::string> paramList,
                               double maxWaitTime,
                               bool isSync,
                               double maxSimDelayTime);

// helper to run all the work related to running the exec command
ACTOR Future<int> execHelper(ExecCmdValueString* execArg, UID snapUID, std::string folder, std::string role);

// set the data version for the specified storage server UID
void setDataVersion(UID uid, Version version);
// set the data durable version for the specified storage server UID
void setDataDurableVersion(UID uid, Version version);
// print the version info all the storages servers on this node
void printStorageVersionInfo();

#include "flow/unactorcompiler.h"
#endif
